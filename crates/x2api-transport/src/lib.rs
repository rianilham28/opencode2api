//! # x2api-transport — upstream egress
//!
//! One warm `reqwest::Client` per proxy session ("lane").
//!
//! Why a pool instead of a client: reqwest bakes proxy configuration into the
//! `Client`, so a proxy identity cannot vary per request. A rotating exit IP
//! therefore means either a new client per call (a fresh CONNECT + TLS
//! handshake on every request — the cost this module exists to delete) or a
//! set of long-lived clients, each pinned to a sticky session, rotated on a
//! timer while they are idle. This is the second one.
//!
//! Vendor-shaped facts, and where each lives:
//! - WHERE the session id goes in the proxy URL is the vendor's business, and
//!   for every vendor seen so far it is somewhere inside the username
//!   (`…-session-{session}-ttl-60`, `…-network-eco-sid-{session}-ttl-60`), so
//!   a URL template is enough and it stays CONFIG. `ProxyVendor` is the seam
//!   for a vendor that cannot be spelled as a URL at all (a session header, a
//!   port pool) — code, then, like every other vendor quirk in this template.
//! - The TTL UNIT is per-vendor too (one vendor's `ttl-60` is minutes,
//!   another's is seconds). The config carries one authoritative
//!   `session_ttl_secs`; the template renders it as `{ttl_seconds}` or
//!   `{ttl_minutes}`, whichever that vendor spells. Rotation always uses the
//!   seconds. One number, so the URL and the timer cannot drift apart.
//! - The session id ALPHABET is per-vendor (observed: 6–7 digits), so
//!   `session_format` mints digits by default and hex/alnum on request.
//!
//! Credentials: a proxy URL contains a password. It is composed on demand and
//! never stored, logged, or attached to an error — a lane identifies itself by
//! vendor and session id only.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use serde::Deserialize;
use serde_json::Value;
use tokio::task::JoinHandle;
use x2api_kit::backoff::splitmix64;
use x2api_kit::config::ServerConfig;
use x2api_kit::errors::ProviderError;

mod http;

/// The clock a pool measures lane age on, and why it is a seam at all.
///
/// The lane lifecycle — rotate before expiry, retire what is idle, and the
/// idleness that tells those two apart — is decided entirely by elapsed time,
/// and none of it could be tested while the code read `Instant::now()`
/// inline: the `std` clock only moves on its own, so no test could place a
/// lane on the far side of `rotate_at_pct`.
///
/// The alternative considered and rejected: `tokio::time::Instant`, which
/// `tokio::time::pause()` makes advance-able. The reason is the timers, not
/// the pausing. `reqwest`'s connect and read timeouts are `tokio::time` sleeps,
/// so under a paused clock tokio auto-advances whenever every task is parked on
/// one of them — the instant a prewarm waits on loopback I/O, the clock jumps
/// to that timer's deadline, and the lane under test ages by however far that
/// deadline sits. The lifecycle and the network would then be advancing the
/// same clock, and the warm contracts could only be exercised by stubbing the
/// network out. A per-pool offset keeps the wall clock live, so those timeouts
/// still mean seconds and the same tests still talk to a real socket.
///
/// What is here instead: one atomic offset per pool, shared by every lane it
/// mints. Production always reads zero, so the only cost on a request is one
/// relaxed atomic load and a saturating subtract — no virtual call, no clock
/// type parameter, and nothing to monomorphise. Each `Transport` owns its own
/// clock, so a test that moves one pool's clock cannot perturb another pool's
/// lanes running beside it.
#[derive(Clone)]
pub struct Clock {
    /// Nanos added to the real clock. Zero everywhere in production.
    shift: Arc<AtomicU64>,
}

impl Clock {
    fn new() -> Self {
        Self {
            shift: Arc::new(AtomicU64::new(0)),
        }
    }

    /// This pool's notion of now.
    #[inline]
    pub fn now(&self) -> Instant {
        Instant::now() + Duration::from_nanos(self.shift.load(Ordering::Relaxed))
    }

    /// How long ago `then` was, on this pool's clock. `Instant::elapsed`
    /// cannot be used for it: that reads the real clock and would ignore the
    /// offset entirely, which is the bug this type exists to make impossible.
    #[inline]
    pub fn age_of(&self, then: Instant) -> Duration {
        self.now().saturating_duration_since(then)
    }

    /// Move this pool's clock forward. The hidden affordance a lifecycle test
    /// needs and production never calls: everything minted before it now looks
    /// `by` older, so a lane can be placed past its rotate point or its idle
    /// horizon without the test waiting out a real lifetime.
    #[doc(hidden)]
    pub fn advance(&self, by: Duration) {
        self.shift
            .fetch_add(by.as_nanos() as u64, Ordering::Relaxed);
    }
}

/// Metric names for the egress pool (this crate owns this family).
pub mod names {
    pub const ROTATIONS: &str = "x2api_proxy_lane_rotations_total";
    pub const PREWARMS: &str = "x2api_proxy_prewarm_total";
    pub const FAILURES: &str = "x2api_proxy_lane_failures_total";
    pub const HEALTHY: &str = "x2api_proxy_lanes_healthy";
    pub const LANES: &str = "x2api_proxy_lanes";
    pub const DESIRED: &str = "x2api_proxy_lanes_desired";
    pub const RETIREMENTS: &str = "x2api_proxy_lane_retirements_total";
    pub const MINT_FAILURES: &str = "x2api_proxy_lane_mint_failures_total";
    pub const SHED: &str = "x2api_proxy_lane_shed_total";
    pub const OLDEST_AGE: &str = "x2api_proxy_lane_oldest_age_seconds";
    pub const MAINTAIN_LAST_RUN: &str = "x2api_proxy_maintain_last_run_timestamp_seconds";
    pub const ROTATION_FAILURES: &str = "x2api_proxy_lane_rotation_failures_total";
    /// Header-wait timeouts seen through a lane. Recorded, never counted
    /// against the lane: see `Lane::note_slow`.
    pub const SLOW: &str = "x2api_proxy_lane_timeouts_total";
}

/// Describe the egress metric family at recorder installation.
pub fn describe_metrics() {
    metrics::describe_counter!(names::ROTATIONS, "Proxy lane rotations by vendor");
    metrics::describe_counter!(names::PREWARMS, "Proxy lane prewarm probes by vendor");
    metrics::describe_counter!(names::FAILURES, "Proxy lane transport failures by vendor");
    metrics::describe_gauge!(names::HEALTHY, "Proxy lanes by vendor and health state");
    metrics::describe_gauge!(names::LANES, "Proxy lanes held by vendor");
    metrics::describe_gauge!(names::DESIRED, "Proxy lanes desired by demand");
    metrics::describe_counter!(
        names::RETIREMENTS,
        "Proxy lane retirements by vendor and reason (idle|surplus|spent)"
    );
    metrics::describe_counter!(names::MINT_FAILURES, "Proxy lane mint failures by vendor");
    metrics::describe_counter!(
        names::SHED,
        "Fail-closed pool sheds (vendor=all for aggregate no-capacity sheds)"
    );
    metrics::describe_gauge!(names::OLDEST_AGE, "Age of oldest held proxy lane by vendor");
    metrics::describe_gauge!(
        names::MAINTAIN_LAST_RUN,
        "Unix timestamp of last maintenance completion"
    );
    metrics::describe_counter!(
        names::ROTATION_FAILURES,
        "Warm replacement failures by vendor"
    );
    metrics::describe_counter!(
        names::SLOW,
        "Proxy lane response-header timeouts by vendor (vendor slowness, not a lane fault)"
    );
}

/// Consecutive transport failures before a lane is pulled from rotation. One
/// is noise (a vendor drops the odd connection); three in a row is an exit IP
/// that stopped working.
const UNHEALTHY_AFTER: u32 = 3;

const MAINTAIN_TICK_SECS: u64 = 5;
/// Connecting can consume enough of a short session that a nearly expired
/// lane is not useful work. Half the session caps this allowance so the rule
/// still discriminates for the shortest valid proxy TTL.
const LANE_STREAM_ALLOWANCE: Duration = Duration::from_secs(60);

/// First wait after one failed rotation, doubling per consecutive failure up
/// to [`ROTATION_BACKOFF_CAP`]. A vendor outage must not cost a mint and a
/// probe every 5 s: metered egress bills that traffic, and a doomed rotation
/// spends it re-proving what the last probe already proved.
const ROTATION_BACKOFF_BASE: Duration = Duration::from_secs(30);
/// Ceiling on that wait. Past ten minutes a vendor is DOWN rather than
/// degraded, and the pool must keep trying — slowly — rather than give up on
/// it forever and strand the floor lanes.
const ROTATION_BACKOFF_CAP: Duration = Duration::from_secs(600);

// ── configuration ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SessionFormat {
    /// Digits only — what every vendor seen so far issues.
    #[default]
    Digits,
    Hex,
    Alnum,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProxyVendorConfig {
    /// Label for logs and metrics. Never a URL.
    pub name: String,
    /// Proxy URL template. Must contain `{session}`; may contain
    /// `{ttl_seconds}` / `{ttl_minutes}` so the vendor's own unit renders from
    /// the one authoritative TTL below.
    pub url: String,
    /// Ceiling on this vendor's lanes. The pool grows toward it with demand
    /// and shrinks back when traffic stops — it is a maximum, not a garrison.
    pub lanes: usize,
    /// Lanes to hold even when nothing is being sent. 0 (the default) means
    /// an idle proxy costs nothing at all: no sessions, no rotations, no
    /// probes. Raise it to 1 when the very first request's handshake latency
    /// matters more than the standing quota cost of keeping one alive.
    pub min_lanes: usize,
    /// How long this vendor keeps a sticky session, in SECONDS, whatever unit
    /// its URL spells.
    pub session_ttl_secs: u64,
    pub session_format: SessionFormat,
    /// Session id length (digits/chars).
    pub session_len: usize,
    /// Per-vendor override of `prewarm`. A short-TTL vendor rotates often
    /// enough that warming every replacement can outweigh what it saves.
    pub prewarm: Option<bool>,
    /// Connection pools held per lane, overriding `server.upstream_shards`.
    /// One h2 connection carries every stream, so this is the knob that lifts
    /// a peer's `MAX_CONCURRENT_STREAMS` ceiling — all shards keep the same
    /// session, hence the same exit IP.
    pub shards: Option<usize>,
}

impl std::fmt::Debug for ProxyVendorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyVendorConfig")
            .field("name", &self.name)
            .field("url", &"<redacted>")
            .field("lanes", &self.lanes)
            .field("min_lanes", &self.min_lanes)
            .field("session_ttl_secs", &self.session_ttl_secs)
            .field("session_format", &self.session_format)
            .field("session_len", &self.session_len)
            .field("prewarm", &self.prewarm)
            .field("shards", &self.shards)
            .finish()
    }
}

impl Default for ProxyVendorConfig {
    fn default() -> Self {
        Self {
            name: "proxy".into(),
            url: String::new(),
            lanes: 2,
            min_lanes: 0,
            session_ttl_secs: 3_600,
            session_format: SessionFormat::Digits,
            session_len: 7,
            prewarm: None,
            shards: None,
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProxyConfig {
    pub vendors: Vec<ProxyVendorConfig>,
    /// Rotate a lane at this percentage of its TTL, so a session is replaced
    /// while it still works instead of when it has already expired.
    pub rotate_at_pct: u8,
    /// Requests per minute one lane is expected to carry. The pool sizes
    /// itself from observed demand: `ceil(rate / requests_per_lane)`, clamped
    /// between the vendors' `min_lanes` and `lanes`. Lanes exist for exit-IP
    /// diversity rather than throughput (one client already opens as many
    /// connections as it needs), so this is "how much traffic should share an
    /// IP", not a capacity limit.
    pub requests_per_lane: u32,
    /// Retire a lane that has served nothing for this long instead of paying
    /// to rotate it. This is what makes a quiet proxy free: an idle lane is
    /// dropped, and the next request mints a new one — minting is just
    /// configuration, and the handshake it would have pre-paid is one that
    /// request was going to pay anyway.
    pub idle_retire_secs: u64,
    /// Send one cheap request down a new lane before it serves traffic, so
    /// the CONNECT + TLS handshake is spent off the request path.
    ///
    /// The probe is a `HEAD`, deliberately: it proves the tunnel and returns
    /// no body. It is still real traffic through a metered proxy, and it
    /// repeats on every rotation — a vendor with a 60-second session TTL
    /// probes ~75× per lane per hour. Turn it off (globally, or per vendor)
    /// when quota costs more than the first request's handshake.
    pub prewarm: bool,
}

impl std::fmt::Debug for ProxyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyConfig")
            .field("vendors", &self.vendors)
            .field("rotate_at_pct", &self.rotate_at_pct)
            .field("requests_per_lane", &self.requests_per_lane)
            .field("idle_retire_secs", &self.idle_retire_secs)
            .field("prewarm", &self.prewarm)
            .finish()
    }
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            vendors: Vec::new(),
            rotate_at_pct: 80,
            requests_per_lane: 120,
            idle_retire_secs: 300,
            prewarm: true,
        }
    }
}

impl ProxyConfig {
    /// Read the document's own `proxy` section, then env overrides — the same
    /// shape `ServiceConfig::from_doc` uses, and the reason this config does
    /// not live on `ServerConfig`: egress is an OPTIONAL subsystem, and the
    /// shared server struct is not where optional subsystems grow.
    /// `Ok(None)` means direct egress.
    pub fn from_doc(doc: &Value) -> anyhow::Result<Option<Self>> {
        let mut cfg: Option<Self> = match doc.get("proxy") {
            Some(section) if !section.is_null() => Some(
                serde_json::from_value(section.clone()).map_err(|e: serde_json::Error| {
                    let category = match e.classify() {
                        serde_json::error::Category::Data => "data",
                        serde_json::error::Category::Io => "io",
                        serde_json::error::Category::Syntax => "syntax",
                        serde_json::error::Category::Eof => "eof",
                    };
                    // Serde embeds offending values (including credential URLs) in
                    // its Display text, so only category reaches boot logs. from_value
                    // carries no source line, and inventing one would be misleading.
                    anyhow::anyhow!("proxy section invalid ({category})")
                })?,
            ),
            _ => None,
        };
        // Single-vendor quick start: one URL template, everything else
        // defaulted. Multi-vendor pools need the config file.
        if let Ok(url) = std::env::var(x2api_kit::config::ENV_PROXY_URL)
            && !url.is_empty()
        {
            if let Some(vendors) = cfg.as_ref().map(|cfg| cfg.vendors.len()) {
                tracing::warn!(
                    vendors,
                    "X2API_PROXY_URL replaces the configured proxy vendors and pool settings"
                );
            }
            let mut vendor = ProxyVendorConfig {
                name: "env".into(),
                url,
                ..Default::default()
            };
            if let Ok(value) = std::env::var(x2api_kit::config::ENV_PROXY_LANES)
                && !value.is_empty()
            {
                vendor.lanes = value.parse().map_err(|_| {
                    anyhow::anyhow!(
                        "{x} must be an unsigned integer",
                        x = x2api_kit::config::ENV_PROXY_LANES
                    )
                })?;
            }
            if let Ok(value) = std::env::var(x2api_kit::config::ENV_PROXY_SESSION_TTL_SECS)
                && !value.is_empty()
            {
                vendor.session_ttl_secs = value.parse().map_err(|_| {
                    anyhow::anyhow!(
                        "{x} must be an unsigned integer",
                        x = x2api_kit::config::ENV_PROXY_SESSION_TTL_SECS
                    )
                })?;
            }
            cfg = Some(Self {
                vendors: vec![vendor],
                ..Default::default()
            });
        }
        if let Some(cfg) = &cfg {
            cfg.validate()?;
        }
        Ok(cfg)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (10..=95).contains(&self.rotate_at_pct),
            "proxy.rotate_at_pct must be 10..=95 (leave room to rotate before expiry)"
        );
        anyhow::ensure!(
            self.requests_per_lane >= 1,
            "proxy.requests_per_lane >= 1 (0 would demand an unbounded pool)"
        );
        for (i, v) in self.vendors.iter().enumerate() {
            anyhow::ensure!(!v.name.is_empty(), "proxy.vendors[{i}]: name is required");
        }
        // Lane accounting resolves a lane back to its vendor by name. Two
        // vendors sharing a name would collapse onto the first one's floor
        // and bounds, so the label must be a key, not decoration.
        for (i, name) in self.vendors.iter().map(|v| &v.name).enumerate() {
            anyhow::ensure!(
                !self.vendors[..i].iter().any(|prior| &prior.name == name),
                "proxy.vendors[{i}]: duplicate vendor name {name:?}; names identify lanes \
                 and must be unique"
            );
        }
        for (i, v) in self.vendors.iter().enumerate() {
            anyhow::ensure!(
                v.min_lanes <= v.lanes,
                "proxy.vendors[{i}].min_lanes must not exceed lanes"
            );
            anyhow::ensure!(
                v.url.contains("{session}"),
                "proxy.vendors[{i}].url must contain {{session}} — without it every \
                 lane shares one exit IP and rotation does nothing"
            );
            anyhow::ensure!(
                (1..=64).contains(&v.lanes),
                "proxy.vendors[{i}].lanes must be 1..=64 (each lane builds up to 32 clients, \
                 so a larger ceiling makes startup unexpectedly expensive)"
            );
            anyhow::ensure!(
                v.session_ttl_secs >= 60,
                "proxy.vendors[{i}].session_ttl_secs must be at least 60"
            );
            anyhow::ensure!(
                (v.session_ttl_secs as u128) * (100 - u128::from(self.rotate_at_pct)) / 100
                    >= 2 * MAINTAIN_TICK_SECS as u128,
                "proxy.vendors[{i}].session_ttl_secs must leave at least {budget}s before \
                 expiry at rotate_at_pct={pct}% so two {tick}s maintenance ticks can rotate it",
                budget = 2 * MAINTAIN_TICK_SECS,
                pct = self.rotate_at_pct,
                tick = MAINTAIN_TICK_SECS
            );
            anyhow::ensure!(
                (4..=32).contains(&v.session_len),
                "proxy.vendors[{i}].session_len must be 4..=32"
            );
            anyhow::ensure!(
                v.shards.is_none_or(|s| (1..=32).contains(&s)),
                "proxy.vendors[{i}].shards must be 1..=32"
            );
            // A vendor spelling minutes cannot express 90 seconds; rounding
            // down silently would rotate late, which is the one thing this
            // whole module exists to avoid.
            anyhow::ensure!(
                !v.url.contains("{ttl_minutes}") || v.session_ttl_secs % 60 == 0,
                "proxy.vendors[{i}].url renders {{ttl_minutes}}, so session_ttl_secs \
                 must be a whole number of minutes"
            );
        }
        Ok(())
    }
}

// ── the vendor seam ──────────────────────────────────────────────────────

/// How one proxy vendor spells a sticky session. Implement this only for a
/// vendor that cannot be expressed as a URL template (session in a header, a
/// pool of ports); everything seen so far is `TemplateVendor` from config.
pub trait ProxyVendor: Send + Sync + 'static {
    /// Label for logs and metrics — never the URL, which carries a password.
    fn name(&self) -> &'static str;
    /// The proxy URL for a lane holding this session id.
    fn lane_url(&self, session: &str) -> String;
    /// How long the vendor honours that session.
    fn session_ttl(&self) -> Duration;
    /// Mint an id in the alphabet this vendor accepts.
    fn mint_session(&self, entropy: u64) -> String;
    /// Warm a replacement lane before it serves? `None` defers to the pool.
    fn prewarm(&self) -> Option<bool> {
        None
    }
    /// Connection pools to hold per lane; `None` defers to the server knob.
    fn shards(&self) -> Option<usize> {
        None
    }
}

/// The config-driven vendor: substitute `{session}` (and optionally the TTL)
/// into a URL template.
pub struct TemplateVendor {
    name: &'static str,
    template: String,
    ttl: Duration,
    format: SessionFormat,
    len: usize,
    shards: Option<usize>,
    prewarm: Option<bool>,
}

impl TemplateVendor {
    pub fn new(cfg: &ProxyVendorConfig) -> Self {
        Self {
            // Leaked once at boot: metric and log labels are `&'static str`
            // by convention here, and the vendor set is fixed at startup.
            name: Box::leak(cfg.name.clone().into_boxed_str()),
            template: cfg.url.clone(),
            ttl: Duration::from_secs(cfg.session_ttl_secs),
            format: cfg.session_format,
            len: cfg.session_len,
            shards: cfg.shards,
            prewarm: cfg.prewarm,
        }
    }
}

impl ProxyVendor for TemplateVendor {
    fn name(&self) -> &'static str {
        self.name
    }

    fn lane_url(&self, session: &str) -> String {
        let secs = self.ttl.as_secs();
        self.template
            .replace("{session}", session)
            .replace("{ttl_seconds}", &secs.to_string())
            .replace("{ttl_minutes}", &(secs / 60).max(1).to_string())
    }

    fn session_ttl(&self) -> Duration {
        self.ttl
    }

    fn shards(&self) -> Option<usize> {
        self.shards
    }

    fn prewarm(&self) -> Option<bool> {
        self.prewarm
    }

    fn mint_session(&self, entropy: u64) -> String {
        const ALNUM: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let mut out = String::with_capacity(self.len);
        let mut state = entropy | 1;
        for i in 0..self.len {
            state = splitmix64(state ^ (i as u64));
            match self.format {
                // Never lead with 0: vendors that parse the id as a number
                // would fold 0123456 and 123456 into one session.
                SessionFormat::Digits => {
                    let d = (state % 10) as u8;
                    let d = if i == 0 && d == 0 { 1 } else { d };
                    out.push((b'0' + d) as char);
                }
                SessionFormat::Hex => {
                    out.push(char::from_digit((state % 16) as u32, 16).unwrap_or('f'));
                }
                SessionFormat::Alnum => {
                    out.push(ALNUM[(state as usize) % ALNUM.len()] as char);
                }
            }
        }
        out
    }
}

/// Egress is deliberately an enum: a proxy-backed transport has no field from
/// which future code could accidentally mint a direct client.
enum Egress {
    Direct(Arc<Lane>),
    Pool {
        lanes: RwLock<Vec<Arc<Lane>>>,
        /// Per-vendor `(min_lanes, lanes)`, parallel to `vendors`.
        bounds: Vec<(usize, usize)>,
    },
}

type PoolBounds = [(usize, usize)];

// ── lanes ────────────────────────────────────────────────────────────────

/// One warm client pinned to one proxy session. `Debug` prints the lane's
/// identity only — the client it holds was built from a URL carrying a
/// password, and that URL must not survive into a log line.
pub struct Lane {
    /// One client per shard. Same proxy session in every one, so they share
    /// an exit IP and differ only in which connection a request rides.
    clients: Vec<reqwest::Client>,
    shard: AtomicUsize,
    vendor: &'static str,
    session: String,
    minted: Instant,
    /// The pool's clock, shared with every lane it minted. Age is read through
    /// it, never through `Instant::elapsed`, so the offset a lifecycle test
    /// moves is the offset the predicates see.
    clock: Clock,
    /// Seconds since `minted` at the last time this lane completed real work.
    /// Cheap enough to touch on every completion, and it is what tells
    /// maintenance the difference between a lane worth rotating and one worth
    /// dropping.
    last_used_secs: AtomicU64,
    inflight: AtomicUsize,
    ttl: Duration,
    healthy: AtomicBool,
    consecutive_failures: AtomicU32,
    /// Only a pooled lane can be pulled from rotation and retired. Direct
    /// egress has one lane and no replacement, so a health flip there would be
    /// a signal with no mechanism behind it.
    rotatable: bool,
}

impl std::fmt::Debug for Lane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lane")
            .field("vendor", &self.vendor)
            .field("session", &self.session)
            .field("shards", &self.clients.len())
            .field("healthy", &self.is_healthy())
            .field("age_secs", &self.age().as_secs())
            .finish()
    }
}

impl Lane {
    /// The warm client for the next shard. Cloning one is an `Arc` bump, not
    /// a connection.
    pub fn client(&self) -> &reqwest::Client {
        &self.clients[self.next_shard()]
    }

    /// Every shard's client, for a caller that has to reach ALL of them
    /// rather than the next one — a prewarm. `client()` advances the
    /// round-robin, so a loop over it warms a subset.
    pub(crate) fn shard_clients(&self) -> &[reqwest::Client] {
        &self.clients
    }

    fn next_shard(&self) -> usize {
        match self.clients.len() {
            1 => 0,
            n => self.shard.fetch_add(1, Ordering::Relaxed) % n,
        }
    }

    #[cfg(test)]
    fn selected_shard(&self) -> usize {
        self.shard.load(Ordering::Relaxed).saturating_sub(1) % self.clients.len()
    }

    /// How many connection pools this lane spreads its streams over.
    pub fn shards(&self) -> usize {
        self.clients.len()
    }

    pub fn vendor(&self) -> &'static str {
        self.vendor
    }

    /// The session id — the whole identity a log line may carry, because the
    /// URL this lane was built from holds a password.
    pub fn session(&self) -> &str {
        &self.session
    }

    /// A transport-level failure (connect refused, proxy auth rejected, tunnel
    /// closed, body died mid-flight). Report only these: an upstream 429 or
    /// 500 arrived THROUGH a working lane and says nothing about the exit IP.
    ///
    /// A response-header timeout is deliberately NOT one of them. `reqwest`'s
    /// `is_timeout` cannot tell the read timeout (a slow generation) from a
    /// dead one, and the same timeout fired three times against three
    /// different upstream generations retires three healthy exit IPs and pays
    /// for three fresh sessions each time. Classify with [`classify_send`]
    /// and route those to [`Lane::note_slow`].
    pub fn note_failure(&self) {
        let n = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::counter!(names::FAILURES, "vendor" => self.vendor).increment(1);
        if !self.rotatable {
            return;
        }
        if n >= UNHEALTHY_AFTER && self.healthy.swap(false, Ordering::Relaxed) {
            tracing::warn!(
                vendor = self.vendor,
                session = %self.session,
                failures = n,
                "proxy lane unhealthy; pulling it from rotation"
            );
        }
    }

    /// The upstream accepted the connection and then stopped answering. That
    /// is a property of the GENERATION, not of this exit IP: the same lane
    /// serves the next request in milliseconds when the vendor recovers.
    ///
    /// So this counts and labels (`x2api_proxy_lane_timeouts_total{vendor}`)
    /// and touches nothing else — no failure, no health flip, no use clock.
    /// Whether a slow vendor should eventually cost an exit IP is a threshold
    /// policy; one timeout is not evidence, and guessing one here is how a
    /// quiet vendor loses its whole pool.
    pub fn note_slow(&self) {
        metrics::counter!(names::SLOW, "vendor" => self.vendor).increment(1);
    }

    /// A request came back through this lane and its body was delivered. Only
    /// a confirmed completion may resurrect health: a transport error mid-body
    /// must keep the failure count, or a flapping tunnel would keep its exit IP
    /// in rotation.
    pub fn note_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.healthy.store(true, Ordering::Relaxed);
        self.touch();
    }

    /// This lane carried a request, without a verdict on its health. A caller
    /// that only knows a body was consumed uses this; one that knows the
    /// exchange succeeded uses `note_success`.
    pub fn note_used(&self) {
        self.touch();
    }

    /// Record that handoff proved the tunnel reachable without declaring the
    /// request complete; sequential body failures must still accumulate.
    pub fn note_healthy(&self) {
        self.healthy.store(true, Ordering::Relaxed);
    }

    /// Mark a handed-off lane complete before recording its outcome.
    pub fn end_use(&self) {
        // Saturating, not wrapping: an unbalanced end_use must not resurrect
        // a permanently in-flight counter and freeze the lane out of idle
        // retirement forever.
        self.inflight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_sub(1))
            })
            .ok();
    }

    /// Begin tracking a handoff; the owning guard must call `end_use` first.
    pub fn begin_use(&self) {
        self.inflight.fetch_add(1, Ordering::AcqRel);
    }

    fn is_inflight(&self) -> bool {
        self.inflight.load(Ordering::Acquire) != 0
    }

    /// How long this lane has existed, on its pool's clock.
    #[inline]
    fn age(&self) -> Duration {
        self.clock.age_of(self.minted)
    }

    /// Time since this lane last completed real work. Hidden integration-test
    /// affordance; ordinary callers use the success/failure lifecycle instead.
    #[doc(hidden)]
    pub fn idle_age_for_test(&self) -> Duration {
        self.age().saturating_sub(Duration::from_secs(
            self.last_used_secs.load(Ordering::Relaxed),
        ))
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    fn has_handoff_time(&self, floor: Duration) -> bool {
        self.ttl
            .checked_sub(self.age())
            .is_some_and(|remaining| remaining >= floor)
    }

    fn touch(&self) {
        self.last_used_secs
            .store(self.age().as_secs(), Ordering::Relaxed);
    }

    fn handoff_floor(&self, connect_timeout: Duration) -> Duration {
        connect_timeout
            .checked_add(LANE_STREAM_ALLOWANCE)
            .unwrap_or(Duration::MAX)
            .min(self.ttl / 2)
    }

    /// Nothing has taken this lane for `idle` — so rotating it would buy a
    /// session for traffic that is not arriving.
    fn idle_for(&self, idle: Duration) -> bool {
        if self.is_inflight() {
            return false;
        }
        let last = self.last_used_secs.load(Ordering::Relaxed);
        self.age().saturating_sub(Duration::from_secs(last)) >= idle
    }

    fn expired(&self, at_pct: u8) -> bool {
        let budget = self.ttl.mul_f64(f64::from(at_pct) / 100.0);
        self.age() >= budget
    }
}

// ── the pool ─────────────────────────────────────────────────────────────

/// Every egress lane this process has. Providers ask it for a lane per call;
/// it hands back a warm one, round-robin over the healthy set.
pub struct Transport {
    egress: Egress,
    vendors: Vec<Arc<dyn ProxyVendor>>,
    server: ServerConfig,
    rotate_at_pct: u8,
    prewarm: bool,
    cursor: AtomicUsize,
    /// Requests taken since the demand window opened, and when it opened.
    demand: AtomicU32,
    window: RwLock<Instant>,
    /// This pool's clock, shared with every lane it mints. The demand window
    /// is measured on it too, so one offset ages the whole pool coherently.
    clock: Clock,
    requests_per_lane: u32,
    idle_retire: Duration,
    maintenance_started: AtomicBool,
    maintenance_handle: Mutex<Option<JoinHandle<()>>>,
    /// Per-vendor rotation-failure backoff, parallel to `vendors`. Read and
    /// written only from the maintenance pass, so a plain mutex over a small
    /// vector: one lock per tick is cheaper than an atomic per vendor.
    rotation_backoff: Mutex<Vec<RotationBackoff>>,
}

/// One vendor's rotation-failure history: how many replacements have failed
/// in a row, and when the last one did (on the pool's clock).
///
/// The stamp is what makes a test able to say "the pool stopped hammering"
/// with `Clock::advance` instead of sleeping through a real backoff.
#[derive(Clone, Copy, Default)]
struct RotationBackoff {
    consecutive: u32,
    last_failure_at: Option<Instant>,
}

impl RotationBackoff {
    /// How much longer the next attempt must wait, or `None` when one is due.
    ///
    /// Full-jitter over the consecutive count, CLAMPED to the base and cap.
    /// The clamp is the whole point: `full_jitter` is uniform in
    /// `[0, base * 2^n]`, so unclamped it hands back a first window anywhere
    /// from ~0 to 30 s — shorter than the 5 s maintenance tick often enough
    /// that the storm this exists to stop runs unimpeded. Jitter reorders
    /// retries WITHIN `[base, cap]`; it never sets the floor.
    fn wait_for(&self, now: Instant, seed: u64) -> Option<Duration> {
        if self.consecutive == 0 {
            return None;
        }
        // Both stamps come from the same pool clock, whose offset only ever
        // grows, so `now` is never behind the failure. Saturating is the
        // correct form: the subtraction that CAN underflow is the one below,
        // and it is guarded by the comparison rather than evaluated eagerly.
        let since = now.saturating_duration_since(self.last_failure_at?);
        let backoff = x2api_kit::backoff::full_jitter(
            self.consecutive,
            ROTATION_BACKOFF_BASE,
            ROTATION_BACKOFF_CAP,
            seed,
        )
        .clamp(ROTATION_BACKOFF_BASE, ROTATION_BACKOFF_CAP);
        (backoff > since).then(|| backoff - since)
    }

    fn note_failure(&mut self, now: Instant) {
        self.consecutive = self.consecutive.saturating_add(1);
        self.last_failure_at = Some(now);
    }

    fn note_success(&mut self) {
        *self = Self::default();
    }
}

impl Transport {
    fn pool(&self) -> Option<(&RwLock<Vec<Arc<Lane>>>, &PoolBounds)> {
        match &self.egress {
            Egress::Direct(_) => None,
            Egress::Pool { lanes, bounds } => Some((lanes, bounds)),
        }
    }

    fn pool_lanes(&self) -> &RwLock<Vec<Arc<Lane>>> {
        match &self.egress {
            Egress::Direct(_) => unreachable!("direct transport has no pool"),
            Egress::Pool { lanes, .. } => lanes,
        }
    }
    fn publish_pool_metrics(&self, lanes: &[Arc<Lane>]) {
        for vendor in &self.vendors {
            let held = lanes
                .iter()
                .filter(|lane| lane.vendor() == vendor.name())
                .count();
            let healthy = lanes
                .iter()
                .filter(|lane| lane.vendor() == vendor.name() && lane.is_healthy())
                .count();
            metrics::gauge!(names::LANES, "vendor" => vendor.name()).set(held as f64);
            metrics::gauge!(names::HEALTHY, "vendor" => vendor.name(), "state" => "healthy")
                .set(healthy as f64);
            metrics::gauge!(names::HEALTHY, "vendor" => vendor.name(), "state" => "unhealthy")
                .set((held - healthy) as f64);
            metrics::gauge!(names::OLDEST_AGE, "vendor" => vendor.name()).set(
                lanes
                    .iter()
                    .filter(|lane| lane.vendor() == vendor.name())
                    .map(|lane| lane.age().as_secs() as f64)
                    .fold(0.0, f64::max),
            );
        }
    }

    /// No proxy: independent clients from the same per-lane connection profile.
    pub fn direct(server: &ServerConfig) -> Arc<Self> {
        let clock = Clock::new();
        let lane = Arc::new(Lane {
            clients: shard_clients(server, server.upstream_shards),
            shard: AtomicUsize::new(0),
            vendor: "direct",
            session: String::new(),
            minted: clock.now(),
            clock: clock.clone(),
            last_used_secs: AtomicU64::new(0),
            inflight: AtomicUsize::new(0),
            ttl: Duration::MAX,
            healthy: AtomicBool::new(true),
            consecutive_failures: AtomicU32::new(0),
            rotatable: false,
        });
        Arc::new(Self {
            egress: Egress::Direct(lane),
            vendors: Vec::new(),
            server: server.clone(),
            rotate_at_pct: 100,
            prewarm: false,
            cursor: AtomicUsize::new(0),
            demand: AtomicU32::new(0),
            window: RwLock::new(clock.now()),
            clock,
            requests_per_lane: u32::MAX,
            idle_retire: Duration::MAX,
            maintenance_started: AtomicBool::new(false),
            maintenance_handle: Mutex::new(None),
            rotation_backoff: Mutex::new(Vec::new()),
        })
    }

    /// Build every vendor's lanes. Fails loudly on a bad template: a proxy
    /// that silently falls back to direct egress would leak the origin IP,
    /// which for most deployments is the entire reason the proxy is there.
    pub fn with_proxy(server: &ServerConfig, cfg: &ProxyConfig) -> anyhow::Result<Arc<Self>> {
        cfg.validate()?;
        let vendors: Vec<Arc<dyn ProxyVendor>> = cfg
            .vendors
            .iter()
            .map(|v| Arc::new(TemplateVendor::new(v)) as Arc<dyn ProxyVendor>)
            .collect();
        anyhow::ensure!(!vendors.is_empty(), "proxy.vendors is empty");

        let clock = Clock::new();
        let transport = Arc::new(Self {
            egress: Egress::Pool {
                lanes: RwLock::new(Vec::new()),
                bounds: cfg.vendors.iter().map(|v| (v.min_lanes, v.lanes)).collect(),
            },
            vendors,
            server: server.clone(),
            rotate_at_pct: cfg.rotate_at_pct,
            prewarm: cfg.prewarm,
            cursor: AtomicUsize::new(0),
            demand: AtomicU32::new(0),
            window: RwLock::new(clock.now()),
            clock,
            requests_per_lane: cfg.requests_per_lane,
            idle_retire: Duration::from_secs(cfg.idle_retire_secs),
            maintenance_started: AtomicBool::new(false),
            maintenance_handle: Mutex::new(None),
            rotation_backoff: Mutex::new(
                (0..cfg.vendors.len())
                    .map(|_| RotationBackoff::default())
                    .collect(),
            ),
        });

        // Only the floor is minted here. Everything above `min_lanes` is
        // demand's to ask for — a pool that garrisons its ceiling pays
        // rotations and probes for traffic that may never arrive.
        let mut built = Vec::new();
        for (i, v) in cfg.vendors.iter().enumerate() {
            let vendor = transport.vendors[i].clone();
            for lane in 0..v.min_lanes {
                built.push(transport.mint_lane(&vendor, lane as u64)?);
            }
        }
        tracing::info!(
            lanes = built.len(),
            max = cfg.vendors.iter().map(|v| v.lanes).sum::<usize>(),
            "proxy egress pool built"
        );
        *transport.pool_lanes().write() = built;
        let snapshot = transport.pool_lanes().read().clone();
        transport.publish_pool_metrics(&snapshot);
        Ok(transport)
    }

    /// A warm lane, or a 503 the server already knows how to render. Failing
    /// closed is deliberate: with every lane down, the alternative is egress
    /// from the origin IP, which is worse than a retryable error.
    pub fn lane(&self) -> Result<Arc<Lane>, ProviderError> {
        let lanes = match &self.egress {
            Egress::Direct(lane) => return Ok(lane.clone()),
            Egress::Pool { lanes, .. } => lanes,
        };
        self.demand.fetch_add(1, Ordering::Relaxed);
        if let Some(lane) = self.select_lane(&lanes.read()) {
            return Ok(lane);
        }
        // Nothing healthy — including the idle-pool case, where there is
        // simply nothing yet. Mint one now: building a client is
        // configuration, not a connection, so this costs nothing the caller
        // was not about to spend on a handshake anyway.
        self.grow_now()
    }

    fn select_lane(&self, lanes: &[Arc<Lane>]) -> Option<Arc<Lane>> {
        if lanes.is_empty() {
            return None;
        }
        let healthy_count = lanes.iter().filter(|lane| lane.is_healthy()).count();
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        for i in 0..lanes.len() {
            let lane = &lanes[(start.wrapping_add(i)) % lanes.len()];
            if !lane.is_healthy() {
                continue;
            }
            // With alternatives, a nearly-expired lane must not start a
            // long generation. With only one lane, trying beats shedding.
            if healthy_count == 1
                || lane.has_handoff_time(
                    lane.handoff_floor(Duration::from_secs(self.server.connect_timeout_secs)),
                )
            {
                return Some(lane.clone());
            }
        }
        // A stopped maintenance loop must not shed traffic while healthy lanes
        // remain; the lane with the most life left is the least risky fallback.
        lanes
            .iter()
            .filter(|lane| lane.is_healthy())
            .max_by_key(|lane| lane.ttl.saturating_sub(lane.age()))
            .cloned()
    }

    /// Add one lane, under the write lock, for the vendor furthest below its
    /// ceiling. Returns it, or the shed error if every vendor is full of
    /// unhealthy lanes.
    fn grow_now(&self) -> Result<Arc<Lane>, ProviderError> {
        let lanes = self.pool_lanes();
        let mut guard = lanes.write();
        // Another caller may have raced us here.
        if let Some(lane) = self.select_lane(&guard) {
            return Ok(lane);
        }
        let Some((_, bounds)) = self.pool() else {
            return Err(ProviderError::unavailable("proxy pool unavailable").with_retry_after(5));
        };
        let has_room = self.vendors.iter().enumerate().any(|(i, vendor)| {
            let max = bounds.get(i).map_or(0, |(_, max)| *max);
            guard.iter().filter(|l| l.vendor() == vendor.name()).count() < max
        });
        let minted = if has_room {
            self.mint_for_vendor_with_room(&guard)
        } else {
            None
        };
        match minted {
            Some(lane) => {
                guard.push(lane.clone());
                drop(guard);
                let snapshot = self.pool_lanes().read().clone();
                self.publish_pool_metrics(&snapshot);
                Ok(lane)
            }
            None if !has_room => {
                metrics::counter!(names::SHED, "vendor" => "all").increment(1);
                Err(ProviderError::unavailable(
                    "no healthy upstream proxy lane; not falling back to direct egress",
                )
                .with_retry_after(5))
            }
            None => Err(ProviderError::unavailable(
                "proxy lane mint failed; not falling back to direct egress",
            )
            .with_retry_after(5)),
        }
    }

    /// The vendor index with the most headroom under its ceiling, or `None`
    /// when every vendor is full.
    fn vendor_with_most_headroom(&self, current: &[Arc<Lane>]) -> Option<usize> {
        let (_, bounds) = self.pool()?;
        let mut best: Option<(usize, usize)> = None; // (vendor index, headroom)
        for (i, vendor) in self.vendors.iter().enumerate() {
            let max = bounds.get(i).map_or(0, |(_, max)| *max);
            let held = current
                .iter()
                .filter(|lane| lane.vendor() == vendor.name())
                .count();
            let headroom = max.saturating_sub(held);
            if headroom > 0 && best.is_none_or(|(_, best_headroom)| headroom > best_headroom) {
                best = Some((i, headroom));
            }
        }
        best.map(|(idx, _)| idx)
    }

    /// Mint for whichever vendor has the most headroom under its ceiling.
    fn mint_for_vendor_with_room(&self, current: &[Arc<Lane>]) -> Option<Arc<Lane>> {
        let idx = self.vendor_with_most_headroom(current)?;
        self.mint_for(idx, current.len() as u64)
    }

    fn mint_for(&self, idx: usize, salt: u64) -> Option<Arc<Lane>> {
        let vendor = self.vendors.get(idx)?;
        match self.mint_lane(vendor, salt) {
            Ok(lane) => Some(lane),
            Err(e) => {
                metrics::counter!(names::MINT_FAILURES, "vendor" => vendor.name()).increment(1);
                tracing::error!(error = %e, "minting a lane failed");
                None
            }
        }
    }

    /// Could a request be served right now, without sending anything to find
    /// out? True for direct egress, and for a pool that either holds a
    /// healthy lane or still has room to mint one. Deliberately non-mutating:
    /// readiness probes are frequent, and a probe that minted lanes would
    /// keep an idle pool alive at the vendor's expense.
    pub fn is_ready(&self) -> bool {
        let Some((lanes, bounds)) = self.pool() else {
            return true; // direct egress is as ready as the process is
        };
        let held = lanes.read();
        if held.iter().any(|lane| lane.is_healthy()) {
            return true;
        }
        self.vendors.iter().enumerate().any(|(i, vendor)| {
            let max = bounds.get(i).map_or(0, |(_, max)| *max);
            held.iter()
                .filter(|lane| lane.vendor() == vendor.name())
                .count()
                < max
        })
    }

    /// Convenience for call sites that do not track lane health.
    pub fn client(&self) -> Result<reqwest::Client, ProviderError> {
        Ok(self.lane()?.client().clone())
    }

    /// A client AND the lane it came from.
    ///
    /// `client()` drops the `Arc`, and with it every way to report what the
    /// exchange proved: a catalogue fetch on a dead tunnel is invisible to the
    /// pool, which then keeps handing the same broken exit IP to chat traffic.
    /// Short requests need no inflight accounting — this is health attribution
    /// only, and the caller decides when to say so.
    pub fn lane_client(&self) -> Result<(reqwest::Client, Arc<Lane>), ProviderError> {
        let lane = self.lane()?;
        Ok((lane.client().clone(), lane))
    }

    fn replace_if_identity(
        lanes: &mut [Arc<Lane>],
        index: usize,
        old: &Arc<Lane>,
        replacement: Arc<Lane>,
    ) -> bool {
        let Some(slot) = lanes.get_mut(index) else {
            return false;
        };
        if !Arc::ptr_eq(slot, old) {
            return false;
        }
        *slot = replacement;
        true
    }

    fn mint_lane(&self, vendor: &Arc<dyn ProxyVendor>, salt: u64) -> anyhow::Result<Arc<Lane>> {
        let entropy = splitmix64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0)
                ^ salt.wrapping_mul(0x9E37_79B9_7F4A_7C15),
        );
        let session = vendor.mint_session(entropy);
        let url = vendor.lane_url(&session);
        // The URL dies with this scope: `Proxy::all` keeps what it needs, and
        // nothing else in this process ever holds the password again.
        let proxy = reqwest::Proxy::all(&url).map_err(|e| {
            anyhow::anyhow!(
                "proxy vendor {}: url is not a usable proxy ({e}); \
                 percent-encode any @ : / in the credentials",
                vendor.name()
            )
        })?;
        let shards = vendor
            .shards()
            .unwrap_or(self.server.upstream_shards)
            .clamp(1, 32);
        // `Proxy` is not `Clone`, so each shard parses the URL for itself.
        // The string still dies inside this function either way.
        let mut clients = vec![http::build_client_with(&self.server, move |b| {
            b.proxy(proxy)
        })];
        for _ in 1..shards {
            let proxy =
                reqwest::Proxy::all(&url).expect("the same url parsed as a proxy moments ago");
            clients.push(http::build_client_with(&self.server, move |b| {
                b.proxy(proxy)
            }));
        }
        Ok(Arc::new(Lane {
            clients,
            shard: AtomicUsize::new(0),
            vendor: vendor.name(),
            session,
            minted: self.clock.now(),
            clock: self.clock.clone(),
            last_used_secs: AtomicU64::new(0),
            inflight: AtomicUsize::new(0),
            ttl: vendor.session_ttl(),
            healthy: AtomicBool::new(true),
            consecutive_failures: AtomicU32::new(0),
            rotatable: true,
        }))
    }

    /// Replace expired and unhealthy lanes, forever. `probe` is a cheap
    /// upstream URL used to spend the handshake before the lane serves
    /// traffic; without one, the first real request pays it.
    pub fn spawn_maintenance(self: &Arc<Self>, probe: Option<String>) -> bool {
        if self.pool().is_none() {
            return false;
        }
        if self
            .maintenance_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            tracing::info!("proxy lane maintenance already started");
            return false;
        }
        let this = self.clone();
        let worker_this = this.clone();
        let spawn = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::spawn(async move {
                let worker: JoinHandle<()> = tokio::spawn(async move {
                    let mut tick = tokio::time::interval(Duration::from_secs(MAINTAIN_TICK_SECS));
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    tick.tick().await;
                    loop {
                        tick.tick().await;
                        worker_this.maintain(probe.as_deref()).await;
                    }
                });
                if let Err(error) = worker.await {
                    this.maintenance_started.store(false, Ordering::Release);
                    tracing::error!(error = %error, "proxy lane maintenance stopped");
                }
            })
        }));
        // A claim with no task behind it would suppress rotation forever.
        let Ok(handle) = spawn else {
            self.maintenance_started.store(false, Ordering::Release);
            tracing::error!("proxy lane maintenance could not be spawned");
            return false;
        };
        *self.maintenance_handle.lock() = Some(handle);
        true
    }

    async fn maintain(&self, probe: Option<&str>) {
        let Some((lanes, bounds)) = self.pool() else {
            return;
        };

        // 1. What does the last window's traffic justify?
        let rate = self.take_demand_rate();
        let (min_total, max_total) = bounds
            .iter()
            .fold((0, 0), |(a, b), (min, max)| (a + min, b + max));
        let desired = desired_lanes(rate, self.requests_per_lane, min_total, max_total);

        // 2. Drop what nobody is using. An idle lane is retired outright
        //    rather than rotated: rotating it would buy a fresh session for
        //    traffic that is not arriving, and the request that eventually
        //    does arrive can mint its own.
        {
            let mut guard = lanes.write();
            let before = guard.len();
            let mut vendor_held: Vec<usize> = self
                .vendors
                .iter()
                .map(|vendor| guard.iter().filter(|l| l.vendor() == vendor.name()).count())
                .collect();
            let mut kept: Vec<Arc<Lane>> = Vec::with_capacity(before);
            for lane in guard.drain(..) {
                let idle = lane.idle_for(self.idle_retire);
                let spent = !lane.is_healthy() || lane.expired(self.rotate_at_pct);
                let surplus = kept.len() >= desired;
                let vendor_index = self
                    .vendors
                    .iter()
                    .position(|v| v.name() == lane.vendor())
                    .unwrap_or(0);
                let floor = bounds.get(vendor_index).map_or(0, |(min, _)| *min);
                let held_for_vendor = vendor_held[vendor_index];
                if held_for_vendor > floor && (idle || (surplus && spent)) {
                    let reason = if spent {
                        "spent"
                    } else if surplus {
                        "surplus"
                    } else {
                        "idle"
                    };
                    metrics::counter!(names::RETIREMENTS, "vendor" => lane.vendor(), "reason" => reason).increment(1);
                    vendor_held[vendor_index] = vendor_held[vendor_index].saturating_sub(1);
                    tracing::info!(vendor = lane.vendor(), session = %lane.session(), idle, healthy = lane.is_healthy(), "proxy lane retired");
                    continue;
                }
                kept.push(lane);
            }
            *guard = kept;
            let snapshot = guard.clone();
            drop(guard);
            self.publish_pool_metrics(&snapshot);
        }

        let stale: Vec<(usize, Arc<Lane>)> = lanes
            .read()
            .iter()
            .enumerate()
            .filter(|(_, l)| !l.is_healthy() || l.expired(self.rotate_at_pct))
            .map(|(i, l)| (i, l.clone()))
            .collect();
        for (idx, old) in stale {
            let Some(vendor_index) = self.vendors.iter().position(|v| v.name() == old.vendor())
            else {
                continue;
            };
            // A vendor that is down must not cost a mint and a probe on every
            // 5 s tick: the warm will fail exactly as it failed last tick, and
            // metered egress bills both. The lane it would have replaced still
            // serves, so waiting is the only cost.
            if let Some(wait) = self.rotation_wait(vendor_index) {
                tracing::debug!(
                    vendor = old.vendor(),
                    wait_secs = wait.as_secs(),
                    "rotation deferred; this vendor's last attempts failed"
                );
                continue;
            }
            let vendor = self.vendors[vendor_index].clone();
            let replacement = match self.mint_lane(&vendor, idx as u64) {
                Ok(l) => l,
                Err(e) => {
                    metrics::counter!(names::MINT_FAILURES, "vendor" => old.vendor()).increment(1);
                    self.note_rotation_failure(vendor_index);
                    tracing::error!(vendor = old.vendor(), error = %e, "minting a lane failed");
                    continue;
                }
            };
            if !self.warm(&replacement, vendor.as_ref(), probe).await {
                metrics::counter!(names::ROTATION_FAILURES, "vendor" => old.vendor()).increment(1);
                self.note_rotation_failure(vendor_index);
                continue;
            }
            let mut guard = lanes.write();
            if !Self::replace_if_identity(&mut guard, idx, &old, replacement) {
                // A concurrent identity change means another actor already
                // replaced this lane; the warm replacement was skipped, not
                // a failed warm, so the rotation-failure counter stays quiet.
                tracing::warn!(vendor = old.vendor(), retired = %old.session(), "proxy lane changed before warm replacement; discarding replacement");
                continue;
            }
            drop(guard);
            self.note_rotation_success(vendor_index);
            metrics::counter!(names::ROTATIONS, "vendor" => old.vendor()).increment(1);
            tracing::info!(vendor = old.vendor(), retired = %old.session(), age_secs = old.age().as_secs(), healthy = old.is_healthy(), "proxy lane rotated");
        }

        loop {
            if lanes.read().len() >= desired {
                break;
            }
            // The gate comes BEFORE the mint, and that ordering is the whole
            // point: `mint_lane` mints a session id and builds one `reqwest`
            // client per shard, so gating after it would spend exactly what
            // this exists to save, on every tick a down vendor is deferred.
            // Selecting the vendor first is what makes that possible — the
            // selection is pure arithmetic over the pool, and only the mint
            // after it costs anything.
            let held = lanes.read().clone();
            let Some(vendor_index) = self.vendor_with_most_headroom(&held) else {
                break;
            };
            if self.rotation_wait(vendor_index).is_some() {
                break;
            }
            let Some(lane) = self.mint_for(vendor_index, held.len() as u64) else {
                self.note_rotation_failure(vendor_index);
                break;
            };
            let vendor = self.vendors[vendor_index].clone();
            if !self.warm(&lane, vendor.as_ref(), probe).await {
                self.note_rotation_failure(vendor_index);
                break;
            }
            self.note_rotation_success(vendor_index);
            lanes.write().push(lane);
        }
        let snapshot = lanes.read().clone();
        self.publish_pool_metrics(&snapshot);
        metrics::gauge!(names::DESIRED).set(desired as f64);
        metrics::gauge!(names::MAINTAIN_LAST_RUN).set(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as f64,
        );
    }

    /// How long this vendor must wait before another replacement is worth
    /// buying, on the pool's clock. `None` means an attempt is due.
    ///
    /// The seed is per vendor rather than per failure time, so a fleet that
    /// lost the same vendor spreads out instead of resuming in lockstep.
    fn rotation_wait(&self, vendor_index: usize) -> Option<Duration> {
        let state = self.rotation_backoff.lock();
        let entry = state.get(vendor_index)?;
        entry.wait_for(self.clock.now(), splitmix64(vendor_index as u64))
    }

    fn note_rotation_failure(&self, vendor_index: usize) {
        let mut state = self.rotation_backoff.lock();
        if let Some(entry) = state.get_mut(vendor_index) {
            entry.note_failure(self.clock.now());
        }
    }

    fn note_rotation_success(&self, vendor_index: usize) {
        let mut state = self.rotation_backoff.lock();
        if let Some(entry) = state.get_mut(vendor_index) {
            entry.note_success();
        }
    }

    /// Run one maintenance pass without waiting for the background tick.
    /// Hidden integration-test affordance for deterministic lifecycle tests.
    #[doc(hidden)]
    pub async fn maintain_once_for_test(&self, probe: Option<&str>) {
        self.maintain(probe).await;
    }

    /// This pool's clock, so a lifecycle test can age its lanes without
    /// waiting for a real lifetime. Hidden affordance: production never
    /// advances one, and the offset is this pool's alone.
    #[doc(hidden)]
    pub fn clock_for_test(&self) -> Clock {
        self.clock.clone()
    }

    /// Requests per minute observed since the last call, extrapolated from the
    /// window that just closed and then reset.
    fn take_demand_rate(&self) -> u32 {
        let mut window = self.window.write();
        let elapsed = self.clock.age_of(*window).as_secs_f64().max(0.001);
        *window = self.clock.now();
        let count = self.demand.swap(0, Ordering::Relaxed);
        ((f64::from(count) * 60.0) / elapsed).round() as u32
    }

    /// Spend the handshake before the lane serves, when the pool and the
    /// vendor both want that. `false` means the lane is already broken.
    ///
    /// EVERY shard is probed, not one. `client()` hands out the next shard
    /// round-robin, so a single `HEAD` warms one of N connection pools and
    /// the other N-1 still pay their handshake on their first real request —
    /// which is the entire cost a prewarm exists to remove. The trade is
    /// deliberate and priced: this is N probes, and `x2api_proxy_prewarm_total`
    /// counts probes, not lanes, so a metered vendor with `shards: 4` must be
    /// priced at 4× the per-lane figure.
    async fn warm(&self, lane: &Arc<Lane>, vendor: &dyn ProxyVendor, probe: Option<&str>) -> bool {
        let Some(url) = probe.filter(|_| self.wants_prewarm(vendor)) else {
            return true;
        };
        let mut proved = true;
        for client in lane.shard_clients() {
            metrics::counter!(names::PREWARMS, "vendor" => lane.vendor()).increment(1);
            if drain(client.head(url).send().await).await.is_err() {
                // A prewarm that never came back proves nothing: the
                // replacement is not adopted, but this is not the OLD lane's
                // fault, so it is counted under rotation failures and never
                // charged to any lane's health.
                tracing::warn!(
                    vendor = lane.vendor(),
                    session = %lane.session(),
                    "prewarm failed; lane not adopted"
                );
                proved = false;
            }
            // A non-transport error (404, 405, a 4xx body) still proves that
            // shard's tunnel works, which is all a prewarm asks of it.
        }
        proved
    }

    /// The one place that answers "should this vendor be warmed?", so boot
    /// and rotation cannot disagree about the per-vendor opt-out.
    fn wants_prewarm(&self, vendor: &dyn ProxyVendor) -> bool {
        vendor.prewarm().unwrap_or(self.prewarm)
    }

    /// Prewarm every lane once, at boot: one `HEAD` per SHARD, no body.
    /// Returns how many lanes came up.
    ///
    /// A vendor that turned prewarming off is SKIPPED, not failed: its opt-out
    /// is a quota decision, and a skipped probe must not read as a lane that
    /// could not come up. Every shard is probed for the reason `warm` gives —
    /// a lane with N shard clients has N connection pools, and warming one
    /// leaves the rest paying the handshake a prewarm is meant to remove.
    pub async fn prewarm_all(&self, probe: &str) -> usize {
        let Some((lanes, _)) = self.pool() else {
            return 0;
        };
        let all: Vec<Arc<Lane>> = lanes.read().clone();
        let mut warm = 0;
        for lane in all {
            let wants = self
                .vendors
                .iter()
                .find(|v| v.name() == lane.vendor())
                .is_some_and(|v| self.wants_prewarm(v.as_ref()));
            if !wants {
                continue;
            }
            // The SAME fold `warm` uses: a lane is warmed only if EVERY shard
            // came up. One reachable pool out of four is not a warm lane — it
            // is a lane that will fail on whichever requests land on the dead
            // three, and admitting it at boot while refusing it at rotation
            // would give the same word two meanings in one binary.
            let mut proved = true;
            for client in lane.shard_clients() {
                metrics::counter!(names::PREWARMS, "vendor" => lane.vendor()).increment(1);
                match drain(client.head(probe).send().await).await {
                    Ok(_) => {}
                    // A dead tunnel is the only boot-time evidence against
                    // this lane, and it is a real one: nothing came up
                    // through it. Counted ONCE for the lane, not once per
                    // shard — four failing pools are one dead exit IP, and
                    // charging four would retire the lane during a single
                    // boot sweep at `shards: 4`.
                    Err(e) if matches!(classify_send(&e), SendFault::Lane) => {
                        lane.note_failure();
                        tracing::warn!(
                            vendor = lane.vendor(),
                            session = %lane.session(),
                            "lane failed to warm"
                        );
                        proved = false;
                    }
                    // Connected, then silence. Counted and labelled like
                    // every other header timeout: the boot sweep is the one
                    // place a timeout is seen without a provider in the
                    // loop, so leaving it out here would make the new family
                    // read as "timeouts outside boot only". Not proof, so
                    // not a veto either — a slow shard does not condemn a
                    // lane whose other pools answered.
                    Err(e) if matches!(classify_send(&e), SendFault::Slow) => {
                        lane.note_slow();
                        tracing::warn!(
                            vendor = lane.vendor(),
                            session = %lane.session(),
                            "lane warm timed out before headers"
                        );
                    }
                    // Reached the upstream and got a verdict (404, 405, a
                    // 4xx): that shard's tunnel is up, which is all a
                    // prewarm asks of it.
                    Err(_) => {}
                }
            }
            if proved {
                warm += 1;
            }
        }
        tracing::info!(warm, "proxy lanes warmed");
        warm
    }
}

/// Build the egress pool from config. Warming needs a probe URL, and the only
/// thing that knows the upstream's real endpoints is the provider — which
/// needs this transport to exist first. So construction and warming are two
/// calls: `build`, make the provider, then `start`.
pub fn build(server: &ServerConfig, proxy: Option<&ProxyConfig>) -> anyhow::Result<Arc<Transport>> {
    match proxy {
        Some(cfg) => Transport::with_proxy(server, cfg),
        None => Ok(Transport::direct(server)),
    }
}

impl Transport {
    /// Warm the lanes and keep them rotating. `probe` must be an endpoint the
    /// upstream actually serves — ask the provider for it rather than
    /// rebuilding the URL here, or a base that already carries `/v1` yields
    /// `/v1/v1/...` and every probe pays for a 404.
    pub async fn start(self: &Arc<Self>, probe: Option<String>) {
        if self.pool().is_none() || !self.spawn_maintenance(probe.clone()) {
            return;
        }
        // The asymmetry is deliberate and one-directional: `prewarm` is a
        // POOL-WIDE BOOT switch, while per-vendor overrides apply in BOTH
        // directions at rotation time (`warm` defers to the vendor). So a
        // vendor may opt OUT of the boot sweep inside a warming pool (that is
        // the case this filter exists for), but a vendor that opts IN inside
        // a `prewarm: false` pool still gets its rotation warms — a lane
        // being replaced mid-session cannot wait for the next boot.
        if let Some(url) = probe.as_deref()
            && self.prewarm
        {
            self.prewarm_all(url).await;
        }
    }
}

/// How many lanes the observed demand justifies. Pure on purpose: the policy
/// is the part worth pinning, and it should not need a clock or a socket to
/// assert.
///
/// Below one lane's worth of traffic the answer is `min_total` — which is 0
/// by default, so a proxy nobody is using holds no sessions and pays no
/// rotations. This is the whole "don't do that when requests are low" rule,
/// in one line.
fn desired_lanes(
    requests_per_minute: u32,
    requests_per_lane: u32,
    min_total: usize,
    max_total: usize,
) -> usize {
    let per_lane = requests_per_lane.max(1);
    let wanted = requests_per_minute.div_ceil(per_lane) as usize;
    wanted.clamp(min_total, max_total)
}

/// Read a probe response to completion, so the connection it rode goes back
/// to the pool.
///
/// This is the difference between prewarming and pretending to: a `send()`
/// whose body is never consumed leaves `reqwest` unable to reuse that
/// connection, so the first REAL request opens a second one and pays the
/// handshake the probe was supposed to have spent. A `HEAD` should carry no
/// body at all, but a peer that sends one anyway must not silently cost us
/// the whole point of the exercise. (Caught by the bench's lane row: `cold
/// ttfb` stayed at the mock proxy's full handshake even with `prewarm` on.)
async fn drain(sent: Result<reqwest::Response, reqwest::Error>) -> Result<(), reqwest::Error> {
    let mut resp = sent?;
    while resp.chunk().await?.is_some() {}
    Ok(())
}

/// What a failed `send()` says about the lane that carried it.
///
/// `reqwest` flattens three different worlds into one error, and the pool's
/// whole health policy turns on telling them apart: a tunnel that never came
/// up is this exit IP's fault, a connection that came up and then produced no
/// headers before the read timeout is the VENDOR being slow, and everything
/// else (a body that died, a 4xx from the proxy) is a verdict the provider
/// already handles closer to the exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendFault {
    /// Connect refused, DNS failure, proxy auth rejected: retire candidates.
    Lane,
    /// Connected, then silence past the read timeout. Counted and labelled,
    /// never charged to the lane — see [`Lane::note_slow`].
    Slow,
    /// Not a lane verdict.
    Other,
}

/// Classify a `send()` failure for lane attribution.
///
/// `is_connect` is asked FIRST and deliberately: a connect timeout surfaces
/// as both `is_connect` and `is_timeout`, and it is a lane fault — nothing
/// was ever established through this exit IP. Only a timeout that happened
/// after the connection is up reaches the [`SendFault::Slow`] arm.
pub fn classify_send(error: &reqwest::Error) -> SendFault {
    if error.is_connect() {
        SendFault::Lane
    } else if error.is_timeout() {
        SendFault::Slow
    } else {
        SendFault::Other
    }
}

/// Build `n` independent clients with one profile. Separate clients mean
/// separate pools, which is the only way to get more than one connection to a
/// host once HTTP/2 is in play — h2 would otherwise multiplex everything onto
/// a single connection and inherit the peer's stream limit as our ceiling.
fn shard_clients(server: &ServerConfig, shards: usize) -> Vec<reqwest::Client> {
    (0..shards.clamp(1, 32))
        .map(|_| http::build_client(server))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{LazyLock, Mutex, MutexGuard};

    static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn env_lock() -> MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    const PROXY_ENV: [&str; 3] = [
        x2api_kit::config::ENV_PROXY_URL,
        x2api_kit::config::ENV_PROXY_LANES,
        x2api_kit::config::ENV_PROXY_SESSION_TTL_SECS,
    ];

    struct EnvGuard(Vec<(&'static str, Option<std::ffi::OsString>)>);

    impl EnvGuard {
        fn set(values: &[(&'static str, &str)]) -> Self {
            let prior = PROXY_ENV
                .iter()
                .map(|key| (*key, std::env::var_os(key)))
                .collect::<Vec<_>>();
            for key in PROXY_ENV {
                unsafe { std::env::remove_var(key) };
            }
            for (key, value) in values {
                unsafe { std::env::set_var(key, value) };
            }
            Self(prior)
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, prior) in self.0.drain(..) {
                unsafe {
                    if let Some(prior) = prior {
                        std::env::set_var(key, prior);
                    } else {
                        std::env::remove_var(key);
                    }
                }
            }
        }
    }

    #[derive(Clone, Default)]
    struct WarningCapture(std::sync::Arc<Mutex<Vec<String>>>);

    struct WarningVisitor<'a>(&'a Mutex<Vec<String>>);

    impl tracing::field::Visit for WarningVisitor<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(format!("{}={value:?}", field.name()));
        }
    }

    impl tracing::Subscriber for WarningCapture {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.level() == &tracing::Level::WARN
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            event.record(&mut WarningVisitor(&self.0));
        }
        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}
    }

    fn capture_warnings(body: impl FnOnce()) -> Vec<String> {
        let capture = WarningCapture::default();
        let output = capture.0.clone();
        tracing::subscriber::with_default(capture, body);
        output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn vendor_cfg(url: &str) -> ProxyVendorConfig {
        ProxyVendorConfig {
            name: "test".into(),
            url: url.into(),
            lanes: 2,
            min_lanes: 0,
            session_ttl_secs: 3_600,
            ..Default::default()
        }
    }

    fn test_lane(ttl: Duration, session: &str) -> Arc<Lane> {
        let clock = Clock::new();
        Arc::new(Lane {
            clients: vec![http::build_client(&ServerConfig::default())],
            shard: AtomicUsize::new(0),
            vendor: "test",
            session: session.into(),
            minted: clock.now(),
            clock,
            last_used_secs: AtomicU64::new(0),
            inflight: AtomicUsize::new(0),
            ttl,
            healthy: AtomicBool::new(true),
            consecutive_failures: AtomicU32::new(0),
            rotatable: true,
        })
    }

    /// Make a lane look `age` older than it is, on its own clock. A bare lane
    /// has no pool clock to move, so the instant it was stamped with is moved
    /// instead — the same fact, read the same way the predicates read it.
    fn backdate(lane: &mut Arc<Lane>, age: Duration) {
        let minted = lane.clock.now() - age;
        Arc::get_mut(lane)
            .expect("the test holds the only reference")
            .minted = minted;
    }

    /// The shipped example is documentation people copy; if it stops loading,
    /// every "just copy config.example.json" instruction in the README is a
    /// lie. Parses through the real entry point, validation included.
    #[test]
    fn the_example_config_ships_a_loadable_proxy_section() {
        let _lock = env_lock();
        let _env = EnvGuard::set(&[]);
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config.example.json");
        let doc: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let cfg = ProxyConfig::from_doc(&doc)
            .expect("example proxy section is valid")
            .expect("example ships a proxy section");
        assert_eq!(cfg.vendors.len(), 2, "both vendor shapes stay documented");
        // One spells its TTL in seconds, the other in minutes — the pair is
        // the whole reason the template renders both.
        let urls: Vec<&str> = cfg.vendors.iter().map(|v| v.url.as_str()).collect();
        assert!(urls.iter().any(|u| u.contains("{ttl_seconds}")), "{urls:?}");
        assert!(urls.iter().any(|u| u.contains("{ttl_minutes}")), "{urls:?}");
    }

    #[test]
    fn proxy_typos_and_invalid_types_fail_without_echoing_credentials() {
        let _lock = env_lock();
        let _env = EnvGuard::set(&[]);
        for (proxy, secrets) in [
            (
                serde_json::json!({"venders": [{"url": "http://u-{session}:p@host:1"}]}),
                vec!["host"],
            ),
            (
                serde_json::json!({
                    "vendors": [{"url": "http://u-{session}:p@host:1", "prewram": false}]
                }),
                vec!["host"],
            ),
            (
                serde_json::json!({
                    "vendors": ["http://user:unique-s3cretpass@unique-host:1338"]
                }),
                vec!["unique-s3cretpass", "unique-host"],
            ),
        ] {
            let err = ProxyConfig::from_doc(&serde_json::json!({"proxy": proxy})).unwrap_err();
            let rendered = format!("{err:#}");
            assert!(
                rendered.contains("proxy section invalid (data)"),
                "{rendered}"
            );
            for secret in secrets {
                assert!(!rendered.contains(secret), "{rendered}");
            }
        }
    }

    #[test]
    fn config_debug_redacts_proxy_urls() {
        let cfg = ProxyConfig {
            vendors: vec![vendor_cfg("http://operator:s3cret@private-proxy:1338")],
            ..Default::default()
        };
        for rendered in [format!("{cfg:?}"), format!("{:?}", cfg.vendors[0])] {
            assert!(rendered.contains("<redacted>"), "{rendered}");
            assert!(!rendered.contains("s3cret"), "{rendered}");
            assert!(!rendered.contains("private-proxy"), "{rendered}");
        }
    }

    #[test]
    fn rotation_budget_reserves_two_maintenance_ticks() {
        let ttl_at_boundary = 2 * MAINTAIN_TICK_SECS * 100 / 5;
        let mut valid = ProxyConfig {
            rotate_at_pct: 95,
            vendors: vec![ProxyVendorConfig {
                session_ttl_secs: ttl_at_boundary,
                ..vendor_cfg("http://u-{session}:p@host:1")
            }],
            ..Default::default()
        };
        assert!(valid.validate().is_ok(), "exact boundary is valid");
        valid.vendors[0].session_ttl_secs -= 1;
        let err = valid.validate().unwrap_err().to_string();
        assert!(err.contains("proxy.vendors[0].session_ttl_secs"), "{err}");
        assert!(err.contains("two 5s maintenance ticks"), "{err}");
    }

    #[test]
    fn lane_ceiling_bounds_startup_work() {
        let cfg = ProxyConfig {
            vendors: vec![ProxyVendorConfig {
                lanes: 65,
                min_lanes: 0,
                ..vendor_cfg("http://u-{session}:p@host:1")
            }],
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("proxy.vendors[0].lanes must be 1..=64"),
            "{err}"
        );
    }

    #[test]
    fn empty_proxy_env_knobs_are_absent_rather_than_boot_errors() {
        let _lock = env_lock();
        let _env = EnvGuard::set(&[
            (
                x2api_kit::config::ENV_PROXY_URL,
                "http://u-{session}:p@host:1",
            ),
            (x2api_kit::config::ENV_PROXY_LANES, ""),
            (x2api_kit::config::ENV_PROXY_SESSION_TTL_SECS, ""),
        ]);
        let cfg = ProxyConfig::from_doc(&serde_json::json!({}))
            .unwrap()
            .unwrap();
        assert_eq!(cfg.vendors[0].lanes, ProxyVendorConfig::default().lanes);
        assert_eq!(
            cfg.vendors[0].session_ttl_secs,
            ProxyVendorConfig::default().session_ttl_secs
        );
    }

    #[test]
    fn env_replacement_replaces_the_pool_and_warns_about_dropped_settings() {
        let _lock = env_lock();
        let _env = EnvGuard::set(&[
            (
                x2api_kit::config::ENV_PROXY_URL,
                "http://u-{session}:unique-env-s3cret@unique-env-host:1",
            ),
            (x2api_kit::config::ENV_PROXY_LANES, "3"),
            (x2api_kit::config::ENV_PROXY_SESSION_TTL_SECS, "600"),
        ]);
        let doc = serde_json::json!({"proxy": {
            "vendors": [{"name": "document", "url": "http://u-{session}:p@private-proxy:1"}],
            "requests_per_lane": 999,
            "prewarm": false
        }});
        let mut cfg: Option<Option<ProxyConfig>> = None;
        let warnings = capture_warnings(|| {
            cfg = Some(ProxyConfig::from_doc(&doc).unwrap());
        });
        let cfg = cfg.unwrap().unwrap();
        assert_eq!(cfg.vendors.len(), 1);
        assert_eq!(cfg.vendors[0].name, "env");
        assert_eq!(cfg.vendors[0].lanes, 3);
        assert_eq!(cfg.vendors[0].session_ttl_secs, 600);
        assert_eq!(cfg.requests_per_lane, 120);
        assert!(cfg.prewarm);
        let warning = warnings.join(" ");
        assert!(warning.contains("vendors=1"), "{warning}");
        assert!(warning.contains("X2API_PROXY_URL replaces"), "{warning}");
        assert!(!warning.contains("private-proxy"), "{warning}");
        assert!(!warning.contains("unique-env-s3cret"), "{warning}");
        assert!(!warning.contains("unique-env-host"), "{warning}");
    }

    #[test]
    fn env_replacement_warns_when_it_drops_pool_only_document_settings() {
        let _lock = env_lock();
        let _env = EnvGuard::set(&[(
            x2api_kit::config::ENV_PROXY_URL,
            "http://u-{session}:pool-env-s3cret@pool-env-host:1",
        )]);
        let doc = serde_json::json!({"proxy": {"requests_per_lane": 60, "prewarm": false}});
        let mut cfg = None;
        let warnings = capture_warnings(|| {
            cfg = Some(ProxyConfig::from_doc(&doc).unwrap());
        });
        let cfg = cfg.unwrap().unwrap();
        assert_eq!(cfg.vendors.len(), 1);
        assert_eq!(cfg.requests_per_lane, 120);
        assert!(cfg.prewarm);
        let warning = warnings.join(" ");
        assert!(warning.contains("vendors=0"), "{warning}");
        assert!(warning.contains("X2API_PROXY_URL replaces"), "{warning}");
        assert!(!warning.contains("pool-env-s3cret"), "{warning}");
        assert!(!warning.contains("pool-env-host"), "{warning}");
    }

    #[test]
    fn malformed_proxy_env_values_fail_boot_with_the_variable_name_only() {
        let _lock = env_lock();
        for (key, variable, bad) in [
            (
                x2api_kit::config::ENV_PROXY_LANES,
                "X2API_PROXY_LANES",
                "unique-lanes-password@unique-lanes-host",
            ),
            (
                x2api_kit::config::ENV_PROXY_SESSION_TTL_SECS,
                "X2API_PROXY_SESSION_TTL_SECS",
                "unique-ttl-password@unique-ttl-host",
            ),
        ] {
            let _env = EnvGuard::set(&[
                (
                    x2api_kit::config::ENV_PROXY_URL,
                    "http://u-{session}:p@host:1",
                ),
                (key, bad),
            ]);
            let err = ProxyConfig::from_doc(&serde_json::json!({}))
                .unwrap_err()
                .to_string();
            assert!(err.contains(variable), "{err}");
            assert!(!err.contains(bad), "{err}");
        }
    }

    #[test]
    fn lane_debug_contains_no_proxy_url_parts() {
        let cfg = ProxyConfig {
            prewarm: false,
            vendors: vec![vendor_cfg(
                "http://unique-user:unique-s3cret@unique-private-host:1338/{session}",
            )],
            ..Default::default()
        };
        let lane = Transport::with_proxy(&ServerConfig::default(), &cfg)
            .unwrap()
            .lane()
            .unwrap();
        let rendered = format!("{lane:?}");
        for secret in ["unique-s3cret", "unique-private-host", "://"] {
            assert!(!rendered.contains(secret), "{rendered}");
        }
    }

    #[test]
    fn template_renders_session_and_the_vendors_own_ttl_unit() {
        let minutes = TemplateVendor::new(&vendor_cfg(
            "http://u-session-{session}-ttl-{ttl_minutes}:p@host:1338",
        ));
        assert_eq!(
            minutes.lane_url("968574"),
            "http://u-session-968574-ttl-60:p@host:1338"
        );
        let seconds = TemplateVendor::new(&vendor_cfg(
            "http://u-network-eco-sid-{session}-ttl-{ttl_seconds}:p@host:1337",
        ));
        assert_eq!(
            seconds.lane_url("2628056"),
            "http://u-network-eco-sid-2628056-ttl-3600:p@host:1337"
        );
    }

    #[test]
    fn minted_sessions_are_vendor_shaped_and_distinct() {
        let v = TemplateVendor::new(&ProxyVendorConfig {
            session_len: 7,
            ..vendor_cfg("http://u-sid-{session}:p@host:1")
        });
        let a = v.mint_session(1);
        let b = v.mint_session(2);
        assert_eq!(a.len(), 7);
        assert!(a.bytes().all(|c| c.is_ascii_digit()), "{a}");
        assert_ne!(a.as_bytes()[0], b'0', "a numeric id must not lead with 0");
        assert_ne!(a, b, "two lanes must not share a session");
    }

    #[test]
    fn a_template_without_session_is_rejected() {
        let cfg = ProxyConfig {
            vendors: vec![vendor_cfg("http://u:p@host:1338")],
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("{session}"), "got {err}");
    }

    #[test]
    fn duplicate_vendor_names_are_rejected() {
        let cfg = ProxyConfig {
            vendors: vec![
                vendor_cfg("http://u-{session}:p@host:1"),
                vendor_cfg("http://v-{session}:p@host:2"),
            ],
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("duplicate vendor name"), "got {err}");
    }

    #[test]
    fn an_omitted_vendor_name_is_reported_before_any_duplicate() {
        let cfg = ProxyConfig {
            vendors: vec![
                ProxyVendorConfig {
                    name: String::new(),
                    ..vendor_cfg("http://u-{session}:p@host:1")
                },
                ProxyVendorConfig {
                    name: String::new(),
                    ..vendor_cfg("http://v-{session}:p@host:2")
                },
            ],
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("name is required"), "got {err}");
        assert!(!err.contains("duplicate"), "got {err}");
    }

    #[test]
    fn minute_spelling_rejects_a_ttl_that_cannot_be_spelled() {
        let cfg = ProxyConfig {
            vendors: vec![ProxyVendorConfig {
                session_ttl_secs: 90,
                ..vendor_cfg("http://u-{session}-ttl-{ttl_minutes}:p@host:1")
            }],
            ..Default::default()
        };
        assert!(
            cfg.validate().is_err(),
            "90s cannot render as whole minutes"
        );
    }

    /// Sharding must not change a lane's identity: every shard rides the same
    /// session, so the exit IP is one IP no matter how many connections carry
    /// it. (Anything else would silently defeat sticky sessions.)
    #[test]
    fn shards_multiply_connections_not_sessions() {
        let cfg = ProxyConfig {
            vendors: vec![ProxyVendorConfig {
                lanes: 1,
                min_lanes: 1,
                shards: Some(4),
                ..vendor_cfg("http://u-session-{session}:p@127.0.0.1:1")
            }],
            ..Default::default()
        };
        let t = Transport::with_proxy(&ServerConfig::default(), &cfg).unwrap();
        let lane = t.lane().unwrap();
        assert_eq!(lane.shards(), 4);
        let session = lane.session().to_string();
        for _ in 0..8 {
            assert_eq!(t.lane().unwrap().session(), session, "one lane, one IP");
        }
    }

    #[test]
    fn a_lane_defaults_to_the_server_shard_count() {
        let server = ServerConfig {
            upstream_shards: 3,
            ..Default::default()
        };
        let cfg = ProxyConfig {
            vendors: vec![vendor_cfg("http://u-session-{session}:p@127.0.0.1:1")],
            ..Default::default()
        };
        let t = Transport::with_proxy(&server, &cfg).unwrap();
        assert_eq!(t.lane().unwrap().shards(), 3);
    }

    /// The rule the whole adaptive pool exists for: quiet traffic holds no
    /// sessions, so a proxy nobody is using costs nothing.
    #[test]
    fn quiet_traffic_wants_no_lanes_and_busy_traffic_wants_more() {
        // Under one lane's worth of traffic per minute -> the floor (0).
        assert_eq!(desired_lanes(0, 120, 0, 4), 0);
        assert_eq!(desired_lanes(1, 120, 0, 4), 1, "any traffic wants a lane");
        assert_eq!(desired_lanes(119, 120, 0, 4), 1);
        assert_eq!(desired_lanes(121, 120, 0, 4), 2);
        assert_eq!(desired_lanes(10_000, 120, 0, 4), 4, "never past the max");
        // A floor above zero is honoured even in total silence.
        assert_eq!(desired_lanes(0, 120, 2, 4), 2);
    }

    #[test]
    fn an_idle_pool_starts_empty_and_mints_on_the_first_request() {
        let cfg = ProxyConfig {
            vendors: vec![ProxyVendorConfig {
                lanes: 4,
                min_lanes: 0,
                ..vendor_cfg("http://u-session-{session}:p@127.0.0.1:1")
            }],
            ..Default::default()
        };
        let t = Transport::with_proxy(&ServerConfig::default(), &cfg).unwrap();
        assert_eq!(
            t.pool_lanes().read().len(),
            0,
            "nothing minted before anyone asked"
        );
        let lane = t.lane().expect("a request mints its own lane");
        assert_eq!(t.pool_lanes().read().len(), 1);
        // …and the next request reuses it rather than minting again.
        assert_eq!(t.lane().unwrap().session(), lane.session());
        assert_eq!(t.pool_lanes().read().len(), 1);
    }

    #[test]
    fn a_completed_lane_is_not_idle_and_an_untouched_one_is() {
        let cfg = ProxyConfig {
            vendors: vec![ProxyVendorConfig {
                lanes: 1,
                min_lanes: 1,
                ..vendor_cfg("http://u-session-{session}:p@127.0.0.1:1")
            }],
            ..Default::default()
        };
        let t = Transport::with_proxy(&ServerConfig::default(), &cfg).unwrap();
        let lane = t.pool_lanes().read()[0].clone();
        assert!(
            lane.idle_for(Duration::ZERO),
            "never used: idle by any measure"
        );
        lane.note_success();
        assert!(
            !lane.idle_for(Duration::from_secs(1)),
            "a lane completed a moment ago is not idle"
        );
    }

    #[test]
    fn direct_transport_always_yields_a_lane() {
        let t = Transport::direct(&ServerConfig::default());
        assert!(t.lane().is_ok());
        assert_eq!(t.lane().unwrap().vendor(), "direct");
    }

    #[test]
    fn direct_egress_keeps_one_lane_and_round_robins_all_shards() {
        let t = Transport::direct(&ServerConfig {
            upstream_shards: 4,
            ..Default::default()
        });
        let lane = t.lane().unwrap();
        let again = t.lane().unwrap();
        assert!(
            Arc::ptr_eq(&lane, &again),
            "lane() must not allocate per request"
        );
        assert_eq!(lane.shards(), 4);
        let mut selected = Vec::new();
        for _ in 0..4 {
            let _ = lane.client();
            selected.push(lane.selected_shard());
        }
        assert_eq!(
            selected,
            vec![0, 1, 2, 3],
            "direct clients must all be used"
        );
    }

    #[test]
    fn success_records_completion_time_not_only_handoff() {
        let mut lane = test_lane(Duration::from_secs(3_600), "completed");
        backdate(&mut lane, Duration::from_secs(2));
        assert!(lane.idle_for(Duration::from_secs(1)));
        lane.note_success();
        assert!(!lane.idle_for(Duration::from_secs(1)));
    }

    #[test]
    fn in_use_lane_is_not_idle_until_handoff_finishes() {
        let lane = test_lane(Duration::from_secs(3_600), "in-use");

        assert!(lane.idle_for(Duration::ZERO));
        lane.begin_use();
        assert!(!lane.idle_for(Duration::ZERO));

        lane.end_use();
        assert!(lane.idle_for(Duration::ZERO));
        // An unbalanced extra end_use must not wrap the counter into a
        // permanent in-flight flag that freezes the lane out of retirement.
        lane.end_use();
        assert!(
            lane.idle_for(Duration::ZERO),
            "end_use must saturate at zero"
        );
    }

    #[test]
    fn fallback_ranks_healthy_lanes_by_remaining_lifetime() {
        let t = Transport::with_proxy(
            &ServerConfig {
                connect_timeout_secs: 30,
                ..Default::default()
            },
            &ProxyConfig {
                vendors: vec![ProxyVendorConfig {
                    lanes: 2,
                    min_lanes: 2,
                    ..vendor_cfg("http://u-{session}:p@127.0.0.1:1")
                }],
                ..Default::default()
            },
        )
        .unwrap();
        let mut shorter_ttl = test_lane(Duration::from_secs(200), "shorter-ttl");
        backdate(&mut shorter_ttl, Duration::from_secs(150));
        let mut longer_lifetime = test_lane(Duration::from_secs(300), "more-life");
        backdate(&mut longer_lifetime, Duration::from_secs(240));

        // Both healthy (so the primary cursor loop is reachable and its
        // `healthy_count == 1` short-circuit cannot mask the fallback), and
        // both miss their handoff floor, which forces the remaining-life
        // fallback ranking.
        assert!(shorter_ttl.is_healthy() && longer_lifetime.is_healthy());
        assert!(!shorter_ttl.has_handoff_time(shorter_ttl.handoff_floor(Duration::from_secs(30))));
        assert!(
            !longer_lifetime
                .has_handoff_time(longer_lifetime.handoff_floor(Duration::from_secs(30)))
        );
        assert!(Arc::ptr_eq(
            &t.select_lane(&[shorter_ttl, longer_lifetime.clone()])
                .unwrap(),
            &longer_lifetime
        ));
    }

    #[test]
    fn near_expiry_lane_is_skipped_only_when_an_alternative_exists() {
        let t = Transport::with_proxy(
            &ServerConfig {
                connect_timeout_secs: 10,
                ..Default::default()
            },
            &ProxyConfig {
                vendors: vec![ProxyVendorConfig {
                    lanes: 2,
                    min_lanes: 2,
                    ..vendor_cfg("http://u-{session}:p@127.0.0.1:1")
                }],
                ..Default::default()
            },
        )
        .unwrap();
        let mut short = test_lane(Duration::from_secs(60), "short");
        backdate(&mut short, Duration::from_secs(35));
        assert_eq!(
            short.handoff_floor(Duration::from_secs(100)),
            Duration::from_secs(30)
        );
        let fresh = test_lane(Duration::from_secs(60), "fresh");
        assert!(Arc::ptr_eq(
            &t.select_lane(&[short.clone(), fresh.clone()]).unwrap(),
            &fresh
        ));
        assert!(Arc::ptr_eq(
            &t.select_lane(std::slice::from_ref(&short)).unwrap(),
            &short
        ));
    }

    #[test]
    fn acquisition_does_not_count_as_completed_use() {
        let t = Transport::with_proxy(
            &ServerConfig::default(),
            &ProxyConfig {
                vendors: vec![ProxyVendorConfig {
                    lanes: 1,
                    min_lanes: 1,
                    ..vendor_cfg("http://u-{session}:p@127.0.0.1:1")
                }],
                ..Default::default()
            },
        )
        .unwrap();
        let mut lane = t.pool_lanes().write().pop().unwrap();
        backdate(&mut lane, Duration::from_secs(2));
        t.pool_lanes().write().push(lane.clone());
        assert!(lane.idle_age_for_test() >= Duration::from_secs(2));
        let acquired = t.lane().unwrap();
        assert!(Arc::ptr_eq(&acquired, &lane));
        assert!(acquired.idle_age_for_test() >= Duration::from_secs(2));
    }

    #[test]
    fn healthy_handoff_preserves_failures_without_touching_use_age() {
        let mut lane = test_lane(Duration::from_secs(3_600), "healthy-handoff");
        backdate(&mut lane, Duration::from_secs(2));
        for _ in 0..UNHEALTHY_AFTER {
            lane.note_failure();
        }
        assert!(!lane.is_healthy());
        lane.note_healthy();
        assert!(lane.is_healthy());
        assert_eq!(
            lane.consecutive_failures.load(Ordering::Relaxed),
            UNHEALTHY_AFTER
        );
        assert!(lane.idle_age_for_test() >= Duration::from_secs(2));
    }

    /// CONTRACT (W10e): three slow UPSTREAM generations must not retire three
    /// healthy exit IPs.
    ///
    /// `reqwest`'s `is_timeout` covers the 120 s header wait, so the old
    /// `is_connect() || is_timeout()` branch charged every slow generation to
    /// the lane. This is the exact scenario: the lane is working, the vendor
    /// is not answering, and the only honest outcome is a label.
    #[test]
    fn header_timeouts_are_counted_and_never_charged_to_the_lane() {
        use metrics_util::debugging::DebuggingRecorder;
        let mut lane = test_lane(Duration::from_secs(3_600), "slow-vendor");
        backdate(&mut lane, Duration::from_secs(2));
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            for _ in 0..UNHEALTHY_AFTER * 2 {
                lane.note_slow();
            }
        });
        assert!(lane.is_healthy(), "a slow vendor is not a broken exit IP");
        assert_eq!(
            lane.consecutive_failures.load(Ordering::Relaxed),
            0,
            "timeouts must not accumulate toward retirement"
        );
        assert!(
            lane.idle_age_for_test() >= Duration::from_secs(2),
            "a timeout says nothing about how long the lane has been idle"
        );
        let counters = counters_by(&snapshotter);
        assert_eq!(sum(&counters, names::SLOW), (UNHEALTHY_AFTER * 2) as u64);
        assert_eq!(
            sum(&counters, names::FAILURES),
            0,
            "the timeouts went to their own family, not the failure one"
        );
        assert_eq!(
            labelled(&counters, names::SLOW),
            vec![(
                vec![("vendor".to_owned(), "test".to_owned())],
                (UNHEALTHY_AFTER * 2) as u64,
            )]
        );
    }

    /// The doctrine in one function: a connect failure convicts, a
    /// post-connect timeout does not.
    ///
    /// Both errors are produced for real — a refused TCP connection and a
    /// client whose read timeout expires before the peer sends headers —
    /// because the classification reads reqwest's OWN source chain, and a
    /// hand-built stand-in would pin the test's fiction instead of reqwest's
    /// behaviour.
    #[test]
    fn send_failures_classify_by_where_the_exchange_died() {
        use std::time::Duration as D;
        let rt = lifecycle_runtime();
        let refused = rt.block_on(async {
            reqwest::Client::builder()
                .no_proxy()
                .connect_timeout(D::from_secs(1))
                .build()
                .unwrap()
                .get("http://127.0.0.1:1/v1/models")
                .send()
                .await
                .expect_err("nothing listens on port 1")
        });
        assert_eq!(
            classify_send(&refused),
            SendFault::Lane,
            "a tunnel that never came up is this exit IP's fault"
        );

        // A peer that accepts and then says nothing: connected, no headers
        // before the read timeout. That is a slow generation, not a dead lane.
        let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = silent.local_addr().unwrap();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            while let Ok((socket, _)) = silent.accept() {
                held.push(socket);
            }
        });
        let timed_out = rt.block_on(async {
            reqwest::Client::builder()
                .no_proxy()
                .read_timeout(D::from_millis(150))
                .build()
                .unwrap()
                .get(format!("http://{addr}/v1/models"))
                .send()
                .await
                .expect_err("the peer never answers")
        });
        assert!(
            timed_out.is_timeout(),
            "the harness must produce a real timeout, not a different fault"
        );
        assert_eq!(
            classify_send(&timed_out),
            SendFault::Slow,
            "silence after the connection is the generation being slow"
        );
    }

    #[test]
    fn rotation_replacement_checks_lane_identity() {
        let old = test_lane(Duration::from_secs(3_600), "old");
        let replacement = test_lane(Duration::from_secs(3_600), "replacement");
        let mut lanes = vec![old.clone()];
        assert!(Transport::replace_if_identity(
            &mut lanes,
            0,
            &old,
            replacement.clone()
        ));
        assert!(Arc::ptr_eq(&lanes[0], &replacement));
        let current = replacement.clone();
        assert!(!Transport::replace_if_identity(
            &mut lanes,
            0,
            &old,
            old.clone()
        ));
        assert!(Arc::ptr_eq(&lanes[0], &current));
    }

    /// A plain `#[test]`, not `#[tokio::test]`: `with_local_recorder` takes a
    /// sync closure, and the two constraints (a sync scope around a recorded
    /// await, and no runtime nested inside another) have exactly one shape.
    #[test]
    fn concurrent_start_prewarms_and_starts_maintenance_once() {
        let t = Transport::with_proxy(
            &ServerConfig::default(),
            &ProxyConfig {
                prewarm: true,
                vendors: vec![ProxyVendorConfig {
                    lanes: 1,
                    // One floor lane, so the boot sweep has something to
                    // probe and the counter below is a real reading rather
                    // than a zero that would pass however often start ran.
                    min_lanes: 1,
                    ..vendor_cfg("http://u-{session}:p@127.0.0.1:1")
                }],
                ..Default::default()
            },
        )
        .unwrap();
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // The await is inside the `block_on`, and the `block_on` inside the
        // recorder scope. A current-thread runtime keeps both `start` calls
        // on one thread, so the race under test is still the real one.
        metrics::with_local_recorder(&recorder, || {
            lifecycle_runtime().block_on(async {
                let _ = tokio::join!(
                    t.start(Some("http://127.0.0.1:1".into())),
                    t.start(Some("http://127.0.0.1:1".into()))
                );
            });
        });
        // The observable is the probe counter, which is what an operator sees.
        // A private "how many times did start run" field would only be read
        // here, and would prove nothing the metric does not.
        assert_eq!(
            sum(&counters_by(&snapshotter), names::PREWARMS),
            1,
            "two concurrent starts still sweep the pool once"
        );
        assert!(t.maintenance_handle.lock().is_some());
    }

    #[test]
    fn direct_lane_never_claims_rotation_pull() {
        let t = Transport::direct(&ServerConfig::default());
        let lane = t.lane().unwrap();
        for _ in 0..UNHEALTHY_AFTER * 2 {
            lane.note_failure();
        }
        assert!(lane.is_healthy(), "a direct lane cannot be pulled");
        assert!(t.lane().is_ok(), "direct egress always serves");
    }

    #[test]
    fn pooled_lane_health_resets_only_on_confirmed_completion() {
        let mut lane = test_lane(Duration::from_secs(3_600), "pooled");
        backdate(&mut lane, Duration::from_secs(2));
        for _ in 0..UNHEALTHY_AFTER {
            lane.note_failure();
        }
        assert!(!lane.is_healthy());
        lane.note_used();
        assert!(!lane.is_healthy(), "usage alone proves nothing");
        assert_eq!(
            lane.consecutive_failures.load(Ordering::Relaxed),
            UNHEALTHY_AFTER
        );
        assert!(!lane.idle_age_for_test().is_zero());
        lane.note_success();
        assert!(lane.is_healthy());
        assert_eq!(lane.consecutive_failures.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn selected_vendor_mint_failure_counts_but_does_not_shed() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let cfg = ProxyConfig {
                vendors: vec![
                    ProxyVendorConfig {
                        name: "broken".into(),
                        url: "http://user:pass@127.0.0.1:not-a-port/{session}".into(),
                        lanes: 1,
                        ..vendor_cfg("unused")
                    },
                    ProxyVendorConfig {
                        name: "healthy".into(),
                        url: "http://user:pass@127.0.0.1:1/{session}".into(),
                        lanes: 1,
                        ..vendor_cfg("unused")
                    },
                ],
                ..Default::default()
            };
            let transport = Transport::with_proxy(&ServerConfig::default(), &cfg).unwrap();
            assert_eq!(transport.lane().unwrap_err().status, 503);
        });
        let mut mint_failures = Vec::new();
        let mut sheds = Vec::new();
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            let DebugValue::Counter(counter) = value else {
                continue;
            };
            let labels = key
                .key()
                .labels()
                .map(|l| (l.key().to_owned(), l.value().to_owned()))
                .collect::<Vec<_>>();
            if key.key().name() == names::MINT_FAILURES {
                mint_failures.push((labels, counter));
            } else if key.key().name() == names::SHED {
                sheds.push((labels, counter));
            }
        }
        assert_eq!(
            mint_failures,
            vec![(vec![("vendor".into(), "broken".into())], 1)]
        );
        assert!(
            sheds.is_empty(),
            "a selected vendor mint failure is not a capacity shed"
        );
    }

    #[test]
    fn at_ceiling_unhealthy_pool_sheds_for_all_vendors() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let cfg = ProxyConfig {
                vendors: vec![ProxyVendorConfig {
                    name: "vendor".into(),
                    lanes: 2,
                    min_lanes: 2,
                    ..vendor_cfg("http://u-session-{session}:p@127.0.0.1:1")
                }],
                ..Default::default()
            };
            let transport = Transport::with_proxy(&ServerConfig::default(), &cfg).unwrap();
            for lane in transport.pool_lanes().read().iter() {
                for _ in 0..UNHEALTHY_AFTER {
                    lane.note_failure();
                }
            }
            assert_eq!(transport.lane().unwrap_err().status, 503);
        });
        let sheds = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(key, _, _, value)| {
                let DebugValue::Counter(counter) = value else {
                    return None;
                };
                (key.key().name() == names::SHED).then(|| {
                    (
                        key.key()
                            .labels()
                            .map(|l| (l.key().to_owned(), l.value().to_owned()))
                            .collect::<Vec<_>>(),
                        counter,
                    )
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(sheds, vec![(vec![("vendor".into(), "all".into())], 1)]);
    }

    #[test]
    fn maintenance_publishes_pool_state_and_retirement_reason() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        metrics::with_local_recorder(&recorder, || {
            let cfg = ProxyConfig {
                idle_retire_secs: 0,
                prewarm: false,
                vendors: vec![ProxyVendorConfig {
                    name: "vendor".into(),
                    lanes: 1,
                    min_lanes: 0,
                    ..vendor_cfg("http://u-session-{session}:p@127.0.0.1:1")
                }],
                ..Default::default()
            };
            let transport = Transport::with_proxy(&ServerConfig::default(), &cfg).unwrap();
            transport.lane().unwrap();
            rt.block_on(transport.maintain_once_for_test(None));
        });
        let mut gauges = std::collections::BTreeMap::new();
        let mut retirements = Vec::new();
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            let labels = key
                .key()
                .labels()
                .map(|l| (l.key().to_owned(), l.value().to_owned()))
                .collect::<Vec<_>>();
            match value {
                DebugValue::Gauge(gauge) if key.key().name() == names::LANES => {
                    gauges.insert(("lanes", labels), gauge.into_inner());
                }
                DebugValue::Gauge(gauge) if key.key().name() == names::DESIRED => {
                    gauges.insert(("desired", labels), gauge.into_inner());
                }
                DebugValue::Gauge(gauge) if key.key().name() == names::OLDEST_AGE => {
                    gauges.insert(("oldest", labels), gauge.into_inner());
                }
                DebugValue::Gauge(gauge) if key.key().name() == names::MAINTAIN_LAST_RUN => {
                    gauges.insert(("maintain", labels), gauge.into_inner());
                }
                DebugValue::Counter(counter) if key.key().name() == names::RETIREMENTS => {
                    retirements.push((labels, counter));
                }
                _ => {}
            }
        }
        assert_eq!(
            gauges.get(&("lanes", vec![("vendor".into(), "vendor".into())])),
            Some(&1.0)
        );
        assert_eq!(gauges.get(&("desired", vec![])), Some(&1.0));
        assert!(
            gauges
                .get(&("oldest", vec![("vendor".into(), "vendor".into())]))
                .is_some_and(|v| *v >= 0.0)
        );
        assert!(gauges.get(&("maintain", vec![])).is_some_and(|v| *v > 0.0));
        assert_eq!(
            retirements,
            vec![(
                vec![
                    ("vendor".into(), "vendor".into()),
                    ("reason".into(), "idle".into())
                ],
                1
            )]
        );
    }

    #[test]
    fn maintenance_retains_idle_vendor_lanes_at_the_configured_floor() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        metrics::with_local_recorder(&recorder, || {
            let cfg = ProxyConfig {
                idle_retire_secs: 0,
                prewarm: false,
                vendors: vec![ProxyVendorConfig {
                    name: "vendor".into(),
                    lanes: 1,
                    min_lanes: 1,
                    ..vendor_cfg("http://u-session-{session}:p@127.0.0.1:1")
                }],
                ..Default::default()
            };
            let transport = Transport::with_proxy(&ServerConfig::default(), &cfg).unwrap();
            let original_session = transport.pool_lanes().read()[0].session().to_owned();
            for _ in 0..3 {
                rt.block_on(transport.maintain_once_for_test(None));
                let lanes = transport.pool_lanes().read();
                assert_eq!(lanes.len(), 1);
                assert_eq!(lanes[0].session(), original_session);
            }
        });
        let retirements =
            snapshotter
                .snapshot()
                .into_vec()
                .into_iter()
                .filter_map(|(key, _, _, value)| {
                    let DebugValue::Counter(counter) = value else {
                        return None;
                    };
                    (key.key().name() == names::RETIREMENTS).then_some(counter)
                });
        assert_eq!(retirements.sum::<u64>(), 0);
    }

    #[tokio::test]
    async fn maintenance_replaces_unhealthy_floor_lane_when_aggregate_demand_is_already_met() {
        let cfg = ProxyConfig {
            prewarm: false,
            vendors: vec![
                ProxyVendorConfig {
                    name: "vendor-a".into(),
                    lanes: 2,
                    min_lanes: 1,
                    ..vendor_cfg("http://a-session-{session}:p@127.0.0.1:1")
                },
                ProxyVendorConfig {
                    name: "vendor-b".into(),
                    lanes: 2,
                    min_lanes: 0,
                    ..vendor_cfg("http://b-session-{session}:p@127.0.0.1:1")
                },
            ],
            ..Default::default()
        };
        let transport = Transport::with_proxy(&ServerConfig::default(), &cfg).unwrap();
        let spent_session = transport.pool_lanes().read()[0].session().to_owned();
        for _ in 0..UNHEALTHY_AFTER {
            transport.pool_lanes().read()[0].note_failure();
        }
        let aggregate_lane = transport.lane().unwrap();
        assert_eq!(aggregate_lane.vendor(), "vendor-b");
        assert!(aggregate_lane.is_healthy());

        transport.maintain_once_for_test(None).await;

        let lanes = transport.pool_lanes().read();
        assert!(
            lanes.iter().any(|lane| lane.vendor() == "vendor-a"
                && lane.session() != spent_session
                && lane.is_healthy()),
            "the spent vendor floor lane must be gone and replaced by a healthy lane"
        );
        assert!(
            lanes
                .iter()
                .any(|lane| lane.vendor() == "vendor-b" && lane.is_healthy()),
            "the healthy aggregate-demand lane must remain available"
        );
    }

    /// At every configured ceiling, every vendor's lane is unhealthy. (Below the ceiling the pool
    /// mints a replacement instead — an unhealthy lane is a reason to get a
    /// new session, not a reason to fail.)
    #[test]
    fn a_pool_with_no_healthy_lane_sheds_instead_of_leaking_the_origin_ip() {
        let cfg = ProxyConfig {
            vendors: vec![ProxyVendorConfig {
                lanes: 2,
                min_lanes: 2, // at the ceiling: no headroom to mint into
                ..vendor_cfg("http://u-session-{session}:p@127.0.0.1:1")
            }],
            ..Default::default()
        };
        let t = Transport::with_proxy(&ServerConfig::default(), &cfg).unwrap();
        for lane in t.pool_lanes().read().iter() {
            for _ in 0..UNHEALTHY_AFTER {
                lane.note_failure();
            }
        }
        let err = t.lane().unwrap_err();
        assert_eq!(err.status, 503);
        assert_eq!(err.retry_after, Some(5));
        assert!(!err.message.contains(':'), "no credential in the message");
        assert!(matches!(t.egress, Egress::Pool { .. }));
    }

    /// CONTRACT: `is_ready()` answers without minting.
    ///
    /// Readiness is polled frequently and by the load balancer, so a
    /// readiness probe that kept the pool alive would be the cheapest way
    /// there is to spend a session nobody is using — on metered egress, one
    /// per probe interval, forever. It is deliberately non-mutating, and that
    /// is not visible at the socket (minting builds a client; it opens no
    /// connection), so it has to be asserted on the pool itself.
    #[test]
    fn readiness_never_mints_a_lane() {
        let cfg = ProxyConfig {
            vendors: vec![ProxyVendorConfig {
                lanes: 2,
                // No floor and no traffic: the pool starts empty, which is
                // exactly the state where a mutating readiness check would
                // mint to "fix" it.
                min_lanes: 0,
                ..vendor_cfg("http://u-session-{session}:p@127.0.0.1:1")
            }],
            ..Default::default()
        };
        let t = Transport::with_proxy(&ServerConfig::default(), &cfg).unwrap();
        assert_eq!(t.pool_lanes().read().len(), 0, "the pool starts empty");
        for _ in 0..25 {
            assert!(t.is_ready(), "a pool with headroom is ready");
        }
        assert_eq!(
            t.pool_lanes().read().len(),
            0,
            "twenty-five readiness probes bought no sessions"
        );
        assert_eq!(
            t.lane()
                .expect("the first request may mint")
                .session()
                .len(),
            7,
            "and the first real request still finds room to mint"
        );
    }

    #[test]
    fn round_robin_skips_the_unhealthy_and_recovers_on_success() {
        let cfg = ProxyConfig {
            vendors: vec![ProxyVendorConfig {
                lanes: 2,
                min_lanes: 2,
                ..vendor_cfg("http://u-session-{session}:p@127.0.0.1:1")
            }],
            ..Default::default()
        };
        let t = Transport::with_proxy(&ServerConfig::default(), &cfg).unwrap();
        let first = t.pool_lanes().read()[0].clone();
        for _ in 0..UNHEALTHY_AFTER {
            first.note_failure();
        }
        for _ in 0..4 {
            assert_ne!(t.lane().unwrap().session(), first.session());
        }
    }

    // ── the lane lifecycle, pinned ───────────────────────────────────────
    //
    // Four contracts decide what a session costs, and all four are now
    // reachable only because lane age rides `Clock`. Each test below fails if
    // its rule is inverted, and they run on loopback with no network.

    /// One floor lane, a long idle horizon, a probe URL, and a clock the test
    /// owns. TTL and rotate point are spelled so "just under" and "just over"
    // land inside the session's budget, with room for the two idle passes a
    /// busy-configured rotation needs to survive.
    fn lifecycle_cfg(proxy_url: &str, ttl_secs: u64) -> ProxyConfig {
        ProxyConfig {
            rotate_at_pct: 50,
            // Long enough that nothing reaches retirement on age in these
            // tests: an idle RETIREMENT is what the next test pins, and it has
            // to be visible there as a distinct outcome.
            idle_retire_secs: 86_400,
            prewarm: true,
            vendors: vec![ProxyVendorConfig {
                lanes: 2,
                min_lanes: 1,
                session_ttl_secs: ttl_secs,
                ..vendor_cfg(proxy_url)
            }],
            ..Default::default()
        }
    }

    /// The lifecycle tests are plain `#[test]`s, not `#[tokio::test]s`,
    /// because `with_local_recorder` takes a sync closure and the contract
    /// asserts on what the maintenance PASS emits — not on what building the
    /// pool emits. A current-thread runtime keeps the mock proxy and the pass
    /// on one thread, so the hold-and-observe in (d) has a single scheduler.
    fn lifecycle_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime is all these tests need")
    }

    /// Busy enough that the rotation pass has a reason to hold the new lane,
    /// and steady enough that only the rewind is non-deterministic.
    fn lifecycle_server() -> ServerConfig {
        ServerConfig {
            connect_timeout_secs: 1,
            upstream_read_timeout_secs: 2,
            ..Default::default()
        }
    }

    /// One counter from a snapshot: metric name, its labels, its value.
    type Row = (String, Vec<(String, String)>, u64);

    /// Every counter the recorder has seen, from ONE snapshot.
    ///
    /// `Snapshotter::snapshot` DRAINS — it swaps each counter to zero — so a
    /// second call in the same test reports zeros, and any assertion built on
    /// it becomes a tautology that passes whatever the code did. Every
    /// counter assertion in these tests therefore reads this one snapshot.
    fn counters_by(snapshotter: &metrics_util::debugging::Snapshotter) -> Vec<Row> {
        use metrics_util::debugging::DebugValue;
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(key, _, _, value)| {
                let DebugValue::Counter(counter) = value else {
                    return None;
                };
                let labels = key
                    .key()
                    .labels()
                    .map(|l| (l.key().to_owned(), l.value().to_owned()))
                    .collect();
                Some((key.key().name().to_owned(), labels, counter))
            })
            .collect()
    }

    /// The total a counter family reached over the snapshot.
    fn sum(rows: &[Row], name: &str) -> u64 {
        rows.iter()
            .filter(|(metric, _, _)| metric == name)
            .map(|(_, _, count)| count)
            .sum()
    }

    /// One labelled counter family, for a test that must also see the labels.
    fn labelled(rows: &[Row], name: &str) -> Vec<(Vec<(String, String)>, u64)> {
        rows.iter()
            .filter(|(metric, _, _)| metric == name)
            .map(|(_, labels, count)| (labels.clone(), *count))
            .collect()
    }

    /// CONTRACT (a): a lane past `rotate_at_pct` is REPLACED in place, and the
    /// replacement is a different session. Rotating where retirement belongs
    /// would bill a fresh session for every lane that had merely gone quiet;
    /// retiring where rotation belongs would take a working lane away and
    /// leave the pool short.
    #[test]
    fn a_lane_past_the_rotate_point_is_replaced_not_dropped() {
        let rt = lifecycle_runtime();
        let proxy = rt.block_on(mock_proxy());
        proxy.open();
        let cfg = lifecycle_cfg(&proxy.url(), 3_600);
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let probe = proxy.probe_url();
        // The whole scenario runs inside the recorder scope: the metrics a
        // contract asserts on are emitted by the pass, not by the build.
        metrics::with_local_recorder(&recorder, || {
            let transport = Transport::with_proxy(&lifecycle_server(), &cfg).unwrap();
            let clock = transport.clock_for_test();
            let original = transport.pool_lanes().read()[0].clone();

            // Ten seconds short of the rotate point: still inside its budget,
            // so nothing may move.
            clock.advance(Duration::from_secs(1_790));
            rt.block_on(transport.maintain_once_for_test(Some(&probe)));
            assert_eq!(
                transport.pool_lanes().read()[0].session(),
                original.session(),
                "a lane inside its budget is left alone"
            );

            // Past it: same slot, new session, exactly one rotation.
            clock.advance(Duration::from_secs(10));
            rt.block_on(transport.maintain_once_for_test(Some(&probe)));
            let lanes = transport.pool_lanes().read();
            assert_eq!(lanes.len(), 1, "rotation replaces, it does not grow");
            assert_ne!(
                lanes[0].session(),
                original.session(),
                "a fresh session is the whole point of rotating"
            );
            assert!(lanes[0].is_healthy());
        });
        let rows = counters_by(&snapshotter);
        assert_eq!(sum(&rows, names::ROTATIONS), 1);
        assert_eq!(sum(&rows, names::RETIREMENTS), 0);
    }

    /// CONTRACT (b): a replacement that will not prewarm never displaces the
    /// lane it was minted for. An expired-but-working session still routes;
    /// a fresh-but-dead one only costs the next request its exit IP.
    #[test]
    fn a_failed_warm_keeps_the_lane_it_would_have_replaced() {
        let rt = lifecycle_runtime();
        // A proxy that accepts the connection and then never answers it. The
        // sockets are held in a vector rather than dropped: a dropped accept
        // closes the connection before `reqwest` reads a response, which is a
        // different failure with the same symptom.
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let dead_addr = dead.local_addr().unwrap();
        std::thread::spawn(move || {
            let mut unanswerable = Vec::new();
            while let Ok((socket, _)) = dead.accept() {
                unanswerable.push(socket);
            }
        });
        let probe = format!("http://{dead_addr}/warm");
        let cfg = lifecycle_cfg(
            &format!("http://u-session-{{session}}:p@{dead_addr}"),
            3_600,
        );
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // The whole scenario runs inside the recorder scope: the metrics a
        // contract asserts on are emitted by the pass, not by the build.
        metrics::with_local_recorder(&recorder, || {
            let transport = Transport::with_proxy(&lifecycle_server(), &cfg).unwrap();
            let original = transport.pool_lanes().read()[0].clone();
            transport
                .clock_for_test()
                .advance(Duration::from_secs(1_800));
            rt.block_on(transport.maintain_once_for_test(Some(&probe)));

            let lanes = transport.pool_lanes().read();
            assert_eq!(lanes.len(), 1);
            assert_eq!(
                lanes[0].session(),
                original.session(),
                "a warm that failed must not cost the old lane"
            );
            assert!(lanes[0].is_healthy());
        });
        let rows = counters_by(&snapshotter);
        assert_eq!(sum(&rows, names::ROTATIONS), 0);
        assert_eq!(
            sum(&rows, names::ROTATION_FAILURES),
            1,
            "the operator has to see the failed warm"
        );
    }

    /// CONTRACT (c): above the floor, a lane nobody has used since it was
    /// minted is DROPPED, not rotated, and nothing is bought to replace it.
    ///
    /// The conditions are chosen so only the idle rule can fire: the clock
    /// moves past the idle horizon but stops short of the rotate point, so the
    /// lane is idle and not spent. Rotate here instead and this pass would buy
    /// a session for a lane nobody asked for; retire here and it takes the
    /// lane away. Deleting the idle rule entirely must turn this test red,
    /// which is what the config below is for.
    #[test]
    fn an_idle_lane_above_the_floor_is_retired_never_rotated() {
        let rt = lifecycle_runtime();
        let proxy = rt.block_on(mock_proxy());
        proxy.open();
        let mut cfg = lifecycle_cfg(&proxy.url(), 3_600);
        // No floor, because "above the floor" is the whole claim: with one
        // held on a floor of one, retirement could never fire and the test
        // would pass for the wrong reason.
        cfg.vendors[0].min_lanes = 0;
        // The jump below is short enough to stay INSIDE the rotate budget
        // (1_800s at 50% of a 3_600s session), so the lane this test retires is
        // idle and NOT spent. That is what makes the `idle` rule load-bearing:
        // if the lane were also expired, deleting the idle disjunct would still
        // retire it through `surplus && spent` and this pin would pass for the
        // wrong reason.
        cfg.idle_retire_secs = 60;
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let probe = proxy.probe_url();
        // The whole scenario runs inside the recorder scope: the metrics a
        // contract asserts on are emitted by the pass, not by the build.
        metrics::with_local_recorder(&recorder, || {
            let transport = Transport::with_proxy(&lifecycle_server(), &cfg).unwrap();
            // A lane nobody has asked for, so the pool holds one above its
            // floor of zero. Minting it through the pool's own path is what
            // keeps `demand` at zero — a lane that arrived with traffic would
            // justify its own keep-alive.
            let lane = transport
                .mint_lane(&transport.vendors[0], 1)
                .expect("the template vendor mints");
            let held = lane.session().to_owned();
            transport.pool_lanes().write().push(lane);
            assert_eq!(transport.pool_lanes().read().len(), 1);

            // Past the idle horizon, still well inside the rotate budget.
            transport.clock_for_test().advance(Duration::from_secs(120));
            rt.block_on(transport.maintain_once_for_test(Some(&probe)));

            let lanes = transport.pool_lanes().read();
            assert!(
                lanes.is_empty(),
                "an idle lane above the floor is dropped, not paid for"
            );
            assert!(
                !lanes.iter().any(|lane| lane.session() == held),
                "the idle session is gone"
            );
        });
        // One snapshot: the recorder DRAINS, so a second call in this test
        // would report zeros and turn every counter assertion into a
        // tautology.
        let rows = counters_by(&snapshotter);
        assert_eq!(sum(&rows, names::ROTATIONS), 0);
        assert_eq!(
            sum(&rows, names::PREWARMS),
            0,
            "an idle lane is dropped without paying for a handshake"
        );
        // Exactly one lane went, nothing was minted to replace it, and the
        // reason it went is `surplus`: this pool's demand wants nothing, so a
        // lane nobody is using is more than the pool asked for. The label is
        // asserted, not just the count, because it is a distinct operator
        // signal — "we dropped lanes we did not need" reads very differently
        // from "we dropped lanes that had stopped working", and only the
        // counter's `reason` label tells them apart. The `idle` label is
        // pinned by `maintenance_publishes_pool_state_and_retirement_reason`.
        assert_eq!(
            labelled(&rows, names::RETIREMENTS),
            vec![(
                vec![
                    ("vendor".to_owned(), "test".to_owned()),
                    ("reason".to_owned(), "surplus".to_owned()),
                ],
                1,
            )],
            "one surplus retirement, and nothing else"
        );
    }

    /// CONTRACT (c2): the inverse of (c), and the one that catches a
    /// `retire`-for-`rotate` swap on the rotation side.
    ///
    /// Here the pool WANTS more lanes and holds one that is spent but in use:
    /// not idle, and not surplus. Retirement has no business touching it —
    /// dropping it would shrink a pool that is short — so the only correct
    /// outcome is a replacement in its slot. Collapse the rotation pass into
    /// the retirement rule and this lane is simply deleted, the pool shrinks
    /// to nothing, and the next request has to mint from an empty pool.
    #[test]
    fn a_spent_lane_the_pool_still_wants_is_rotated_not_retired() {
        let rt = lifecycle_runtime();
        let proxy = rt.block_on(mock_proxy());
        proxy.open();
        let mut cfg = lifecycle_cfg(&proxy.url(), 3_600);
        cfg.vendors[0].min_lanes = 0;
        let cfg = cfg.clone();
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let probe = proxy.probe_url();
        metrics::with_local_recorder(&recorder, || {
            let transport = Transport::with_proxy(&lifecycle_server(), &cfg).unwrap();
            // Traffic, so the pool is not asking for nothing, and a lane that
            // has just completed work, so it is not idle either.
            let lane = transport.lane().expect("a request mints its own lane");
            lane.note_success();
            let spent = lane.session().to_owned();
            let clock = transport.clock_for_test();

            // Enough requests, inside the same window as the jump below, that
            // the observed rate justifies a lane: without this the pool wants
            // nothing and would retire the lane for surplus, which is correct
            // behaviour and the wrong thing to be testing here.
            for _ in 0..20 {
                let _ = transport.lane();
            }
            clock.advance(Duration::from_secs(1_800));
            rt.block_on(transport.maintain_once_for_test(Some(&probe)));

            let lanes = transport.pool_lanes().read();
            assert_eq!(lanes.len(), 1, "a wanted pool keeps its lane count");
            assert_ne!(
                lanes[0].session(),
                spent,
                "a spent lane is replaced, not deleted"
            );
            assert!(lanes[0].is_healthy());
        });
        let rows = counters_by(&snapshotter);
        assert_eq!(sum(&rows, names::ROTATIONS), 1, "one rotation");
        assert_eq!(
            sum(&rows, names::RETIREMENTS),
            0,
            "retirement must not take a lane the pool is short of"
        );
    }

    /// CONTRACT (d): the prewarm is SPENT, not issued. The probe has to come
    /// back before the replacement takes the old lane's slot, and the pool has
    /// to still be holding the old session at the moment the probe is in
    /// flight. Both are observable at the proxy — hold the warm there, and
    /// the question "what does traffic see while it is open?" has exactly one
    /// answer.
    #[test]
    fn a_replacement_is_warmed_through_the_proxy_before_it_takes_the_slot() {
        let rt = lifecycle_runtime();
        let proxy = rt.block_on(mock_proxy());
        let cfg = lifecycle_cfg(&proxy.url(), 3_600);
        let transport = Transport::with_proxy(&lifecycle_server(), &cfg).unwrap();
        let clock = transport.clock_for_test();
        let probe = proxy.probe_url();
        let original = transport.pool_lanes().read()[0].clone();

        // One rotation, watched from inside: the mock holds the prewarm, and
        // the assertions run while it is held. `join!` is what puts the pass
        // and the observation in one task — holding one while awaiting the
        // other is the only way to see the pool mid-warm.
        clock.advance(Duration::from_secs(1_800));
        rt.block_on(async {
            let (_, ()) = tokio::join!(
                async {
                    let head = tokio::time::timeout(Duration::from_secs(10), proxy.next_head())
                        .await
                        .expect("the replacement is prewarmed through the proxy");
                    // Proxied reqwest sends the absolute form, and the path in it
                    // is the prewarm URL's — one line that proves both that the
                    // probe went out and that it went out through this lane.
                    assert!(
                        head.starts_with("HEAD http://") && head.contains("/warm HTTP/1.1"),
                        "the prewarm is a HEAD down the new lane: {head:?}"
                    );
                    assert_eq!(
                        transport.pool_lanes().read()[0].session(),
                        original.session(),
                        "traffic must not see the replacement while its warm is open"
                    );
                    proxy.open();
                },
                transport.maintain_once_for_test(Some(&probe)),
            );
        });
        let rotated = transport.pool_lanes().read()[0].clone();
        assert_ne!(
            rotated.session(),
            original.session(),
            "the swap follows the warm, never precedes it"
        );

        // The same rule from the other side: hold the NEXT warm, and the pool
        // goes on serving the session that is still on the books.
        proxy.stall_again();
        clock.advance(Duration::from_secs(1_800));
        rt.block_on(async {
            let (_, ()) = tokio::join!(
                async {
                    let head = tokio::time::timeout(Duration::from_secs(10), proxy.next_head())
                        .await
                        .expect("the next replacement is prewarmed too");
                    assert!(
                        head.starts_with("HEAD http://") && head.contains("/warm HTTP/1.1"),
                        "{head:?}"
                    );
                    assert_eq!(
                        transport.pool_lanes().read()[0].session(),
                        rotated.session(),
                        "an unproven lane never takes over"
                    );
                    proxy.open();
                },
                transport.maintain_once_for_test(Some(&probe)),
            );
        });
        assert_ne!(
            transport.pool_lanes().read()[0].session(),
            rotated.session()
        );
        assert_eq!(
            proxy.head_count(),
            2,
            "one prewarm per rotation, and no traffic rode through them"
        );
    }

    /// CONTRACT (e): a vendor that is DOWN must not cost a mint and a probe
    /// on every maintenance tick.
    ///
    /// The shape of the bug is a storm, not a single bad pass: with one
    /// unanswerable proxy, the old code minted and probed a doomed
    /// replacement every 5 s forever, and on metered egress that is the
    /// outage's cost multiplied by the outage's length.
    ///
    /// The schedule is driven off the BACKOFF, not off a magic tick count:
    /// five passes whose total clock advance stays under `ROTATION_BACKOFF_BASE`
    /// must cost exactly nothing extra, and a single pass past that window
    /// must cost exactly one more. That holds whatever the jitter draw is, so
    /// this test cannot be satisfied by a lucky (or unlucky) vendor index.
    #[test]
    fn a_vendor_whose_warm_keeps_failing_stops_being_probed() {
        let rt = lifecycle_runtime();
        // A proxy that accepts the connection and then never answers it. The
        // sockets are held in a vector rather than dropped: a dropped accept
        // closes the connection before `reqwest` reads a response, which is a
        // different failure with the same symptom.
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let dead_addr = dead.local_addr().unwrap();
        std::thread::spawn(move || {
            let mut unanswerable = Vec::new();
            while let Ok((socket, _)) = dead.accept() {
                unanswerable.push(socket);
            }
        });
        let probe = format!("http://{dead_addr}/warm");
        let cfg = lifecycle_cfg(
            &format!("http://u-session-{{session}}:p@{dead_addr}"),
            3_600,
        );
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let transport = Transport::with_proxy(&lifecycle_server(), &cfg).unwrap();
        let clock = transport.clock_for_test();
        // Five 5 s ticks is 25 s: provably inside the 30 s floor the window is
        // clamped to, so "deferred" here is arithmetic rather than a fact
        // about the seed.
        let inside_window = Duration::from_secs(5 * MAINTAIN_TICK_SECS);
        assert!(
            inside_window < ROTATION_BACKOFF_BASE,
            "the deferral schedule must fit under the floor, or it proves nothing"
        );
        metrics::with_local_recorder(&recorder, || {
            // The first pass is the failure itself: a spent floor lane, and
            // one attempt to replace it that does not come back.
            clock.advance(Duration::from_secs(1_800));
            rt.block_on(transport.maintain_once_for_test(Some(&probe)));
            for _ in 0..5 {
                clock.advance(Duration::from_secs(MAINTAIN_TICK_SECS));
                rt.block_on(transport.maintain_once_for_test(Some(&probe)));
            }
            // Time buys an attempt, and only time buys an attempt.
            clock.advance(ROTATION_BACKOFF_BASE);
            rt.block_on(transport.maintain_once_for_test(Some(&probe)));
        });
        // ONE snapshot for the whole run, asserting LIFETIME totals:
        // `Snapshotter::snapshot` drains, so a second read reports zero
        // whatever the passes did.
        let rows = counters_by(&snapshotter);
        assert_eq!(
            sum(&rows, names::PREWARMS),
            2,
            "one probe for the first failure, one for the pass past the window, none in between"
        );
        assert_eq!(
            sum(&rows, names::ROTATION_FAILURES),
            2,
            "and each of those two probes was allowed to be bought and to fail"
        );
        assert_eq!(
            transport.pool_lanes().read().len(),
            1,
            "the old session keeps serving throughout"
        );
    }

    /// CONTRACT (e2): the growth path obeys the same backoff as the rotation
    /// path.
    ///
    /// Rotating a lane is not the only thing that spends a probe. When a
    /// floor lane was retired for being surplus, the pool wanted it back, and
    /// `mint_for_vendor_with_room` + `warm` re-bought it on every tick — the
    /// same storm, one loop over. The lane is put in that state by spending
    /// it and aging it out, not by the request path: `lane()` mints and pushes
    /// without probing, so only maintenance ever warms here.
    #[test]
    fn demand_growth_stops_hammering_a_vendor_whose_warm_keeps_failing() {
        let rt = lifecycle_runtime();
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let dead_addr = dead.local_addr().unwrap();
        std::thread::spawn(move || {
            let mut unanswerable = Vec::new();
            while let Ok((socket, _)) = dead.accept() {
                unanswerable.push(socket);
            }
        });
        let probe = format!("http://{dead_addr}/warm");
        let mut cfg = lifecycle_cfg(
            &format!("http://u-session-{{session}}:p@{dead_addr}"),
            3_600,
        );
        // This test is about the GROWTH loop, so the rotation loop must have
        // nothing to do: no floor lane means the pool holds none, nothing can
        // be stale, and every probe below can only have come from growth. One
        // request per lane keeps `desired_lanes` above what is held, so the
        // growth loop is reached on every pass instead of breaking out at
        // `len >= desired`.
        cfg.vendors[0].min_lanes = 0;
        cfg.requests_per_lane = 1;
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let transport = Transport::with_proxy(&lifecycle_server(), &cfg).unwrap();
        let clock = transport.clock_for_test();
        metrics::with_local_recorder(&recorder, || {
            // Traffic first: the request path mints and pushes a lane WITHOUT
            // probing it (it cannot — a request never warms), so the pool is
            // one short of what demand justifies.
            for _ in 0..4 {
                let _ = transport.lane();
            }
            // The first pass is the failure itself, and it opens the window.
            rt.block_on(transport.maintain_once_for_test(Some(&probe)));
            // Five 5 s ticks is 25 s, provably inside the 30 s floor.
            for _ in 0..5 {
                for _ in 0..4 {
                    let _ = transport.lane();
                }
                clock.advance(Duration::from_secs(MAINTAIN_TICK_SECS));
                rt.block_on(transport.maintain_once_for_test(Some(&probe)));
            }
            // Past the floor, unambiguously rather than exactly on it: the
            // retry must not depend on how `wait_for` treats equality.
            clock.advance(ROTATION_BACKOFF_BASE + Duration::from_secs(1));
            for _ in 0..4 {
                let _ = transport.lane();
            }
            rt.block_on(transport.maintain_once_for_test(Some(&probe)));
        });
        let rows = counters_by(&snapshotter);
        assert_eq!(
            sum(&rows, names::PREWARMS),
            2,
            "seven passes, two probes: the growth path retries on the backoff schedule, not every tick"
        );
    }

    /// The window `ROTATION_BACKOFF_BASE`'s doc promises, pinned directly:
    /// 30 s at the first failure, doubling, capped at ten minutes, for EVERY
    /// vendor index the pool can hold.
    ///
    /// This is the assertion the lifecycle tests above cannot make. They
    /// prove the backoff holds for one pool's one vendor at one seed; a
    /// regression that dropped the clamp would move that seed's window inside
    /// the 5 s tick, and the lifecycle tests would simply learn whatever
    /// schedule the draw produced.
    #[test]
    fn the_rotation_backoff_never_falls_under_its_floor_or_over_its_cap() {
        let at = Instant::now();
        for index in 0..4usize {
            for consecutive in 1..=8u32 {
                let state = RotationBackoff {
                    consecutive,
                    last_failure_at: Some(at),
                };
                let window = state
                    .wait_for(at, splitmix64(index as u64))
                    .unwrap_or_default();
                assert!(
                    window >= ROTATION_BACKOFF_BASE,
                    "vendor {index}, failure {consecutive}: {window:?} is under the 30 s floor, \
                     so the 5 s maintenance tick would keep hammering this vendor"
                );
                assert!(
                    window <= ROTATION_BACKOFF_CAP,
                    "vendor {index}, failure {consecutive}: {window:?} is over the 10 min cap"
                );
            }
        }
        // The doubling the doc also promises. The CEILING doubles, not the
        // draw: jitter stays inside `[base, ceiling]`, so the strongest
        // statement a single seed supports is that a window can reach the cap
        // and can never exceed it.
        let ceiling = |consecutive: u32| {
            ROTATION_BACKOFF_BASE
                .saturating_mul(1u32 << (consecutive - 1).min(16))
                .min(ROTATION_BACKOFF_CAP)
        };
        let window = |consecutive| {
            RotationBackoff {
                consecutive,
                last_failure_at: Some(at),
            }
            .wait_for(at, 7)
            .unwrap_or_default()
        };
        assert!(
            window(1) <= ceiling(1) && window(6) <= ceiling(6),
            "a window may not exceed its doubling ceiling: {:?} {:?}",
            window(1),
            window(6)
        );
        assert_eq!(
            ceiling(40),
            ROTATION_BACKOFF_CAP,
            "and the doubling ceiling is clamped to the cap the doc promises"
        );
    }

    /// CONTRACT (f): a vendor's own `prewarm: false` is honoured at BOOT.
    ///
    /// Rotation asked `warm()` and deferred to the override; boot asked
    /// `prewarm_all()` and did not, so a short-TTL vendor that opted out was
    /// still probed once per lane at every start. The observable is the probe
    /// count across the mock, not a private flag.
    #[tokio::test]
    async fn boot_prewarms_the_vendors_that_asked_for_it_and_skips_the_rest() {
        let proxy = mock_proxy().await;
        proxy.open();
        let transport = Transport::with_proxy(
            &ServerConfig::default(),
            &ProxyConfig {
                prewarm: true,
                vendors: vec![ProxyVendorConfig {
                    name: "opted-out".into(),
                    lanes: 1,
                    min_lanes: 1,
                    prewarm: Some(false),
                    ..vendor_cfg(&proxy.url())
                }],
                ..Default::default()
            },
        )
        .unwrap();
        let warmed = transport.prewarm_all(&proxy.probe_url()).await;
        assert_eq!(warmed, 0, "an opted-out vendor is not a failure to come up");
        assert_eq!(
            proxy.head_count(),
            0,
            "the probe never went out: that is the whole point of the opt-out"
        );

        // The same pool with the override flipped is what makes this a
        // per-vendor rule rather than a dead code path: `prewarm: true` at
        // the pool level and `Some(false)` on one vendor overrides it.
        let proxy2 = mock_proxy().await;
        proxy2.open();
        let transport2 = Transport::with_proxy(
            &ServerConfig::default(),
            &ProxyConfig {
                prewarm: true,
                vendors: vec![ProxyVendorConfig {
                    name: "opted-in".into(),
                    lanes: 1,
                    min_lanes: 1,
                    prewarm: Some(true),
                    ..vendor_cfg(&proxy2.url())
                }],
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(transport2.prewarm_all(&proxy2.probe_url()).await, 1);
        assert_eq!(
            proxy2.head_count(),
            1,
            "the pool-level switch still applies"
        );
    }

    /// CONTRACT (g): a prewarm reaches EVERY shard, not the next one.
    ///
    /// `client()` hands out shards round-robin, so the single `HEAD` it used
    /// to drive warmed one of N connection pools. The other N-1 then pay
    /// their handshake on their first real request — the exact cost the
    /// prewarm exists to remove, and the more connections a lane is
    /// configured with the more of them it left behind.
    ///
    /// The observable is the probe count at the proxy: a lane with two shard
    /// clients must produce two heads, at boot and at rotation alike.
    #[test]
    fn a_sharded_lane_is_warmed_on_every_shard_at_boot_and_on_rotation() {
        let rt = lifecycle_runtime();
        let proxy = rt.block_on(mock_proxy());
        proxy.open();
        let mut cfg = lifecycle_cfg(&proxy.url(), 3_600);
        cfg.vendors[0].shards = Some(2);
        let transport = Transport::with_proxy(&lifecycle_server(), &cfg).unwrap();
        let probe = proxy.probe_url();
        rt.block_on(transport.prewarm_all(&probe));
        assert_eq!(
            proxy.head_count(),
            2,
            "one HEAD per shard: warming one of two pools warms neither"
        );

        // And the same at rotation, where the replacement is a fresh lane
        // with its own fresh shard clients.
        transport
            .clock_for_test()
            .advance(Duration::from_secs(1_800));
        rt.block_on(transport.maintain_once_for_test(Some(&probe)));
        assert_eq!(
            proxy.head_count(),
            4,
            "the replacement is warmed across its shards before it takes the slot"
        );
    }

    // ── a proxy that speaks absolute-form HTTP/1.1, and nothing else ─────
    //
    // Deliberately not a CONNECT proxy: a proxied `reqwest` opens a tunnel
    // per target host, and a forward proxy is the only shape a `HEAD` probe
    // can be answered by one loopback port. The prewarm URL and the vendor
    // template therefore name the same host, so the absolute-form request
    // line carries the prewarm URL itself — a request whose head proves both
    // that the prewarm went out and that it went out on the NEW lane.

    /// One lane's proxy, counting the request heads that cross it and holding
    /// each warm open until the test lets it land.
    struct MockProxy {
        addr: std::net::SocketAddr,
        probe_url: String,
        /// Whether requests are answered at once or held.
        stalled: std::sync::Arc<AtomicBool>,
        /// Bumped by every `open`, so a held request waits for the NEXT one
        /// rather than for a bump it may have already read.
        opened: tokio::sync::watch::Sender<u64>,
        requests: std::sync::Arc<AtomicUsize>,
        heads: tokio::sync::watch::Sender<Option<String>>,
    }

    impl MockProxy {
        fn url(&self) -> String {
            format!("http://u-session-{{session}}:p@{}", self.addr)
        }

        fn probe_url(&self) -> String {
            self.probe_url.clone()
        }

        /// The NEXT request head to cross the proxy. A fresh subscription
        /// starts unseen, so a warm already on the wire is waited for rather
        /// than read back off the channel's initial value.
        async fn next_head(&self) -> String {
            let mut heads = self.heads.subscribe();
            loop {
                heads
                    .changed()
                    .await
                    .expect("the mock proxy outlives every test that watches it");
                if let Some(head) = heads.borrow_and_update().clone() {
                    return head;
                }
            }
        }

        /// Answer everything from now on, including a warm already held. The
        /// flag is cleared first: a request that reads it afterwards waits
        /// nothing, which is the state the test asked for.
        fn open(&self) {
            // The bump is computed before `send`, which takes the watch's write
            // lock: holding a `borrow` across it would deadlock this thread
            // against itself, and every warm would hang.
            let next = *self.opened.borrow() + 1;
            self.stalled.store(false, Ordering::Release);
            let _ = self.opened.send(next);
        }

        /// Hold requests again, for a test that has to watch the next one.
        fn stall_again(&self) {
            self.stalled.store(true, Ordering::Release);
        }

        fn head_count(&self) -> usize {
            self.requests.load(Ordering::Acquire)
        }
    }

    async fn mock_proxy() -> MockProxy {
        use tokio::io::AsyncWriteExt as _;
        let (opened, opened_rx) = tokio::sync::watch::channel(0u64);
        let (heads, _) = tokio::sync::watch::channel(None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stalled = std::sync::Arc::new(AtomicBool::new(true));
        let requests = std::sync::Arc::new(AtomicUsize::new(0));
        // One clone of each for the accept loop, so the mock keeps its own.
        let (written, served_stalls, served_requests) =
            (heads.clone(), stalled.clone(), requests.clone());
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let (written, mut opened) = (written.clone(), opened_rx.clone());
                let (stalled, requests) = (served_stalls.clone(), served_requests.clone());
                tokio::spawn(async move {
                    let Some(head) = read_head(&mut socket).await else {
                        return;
                    };
                    requests.fetch_add(1, Ordering::AcqRel);
                    let _ = written.send(Some(head));
                    // The flag alone decides whether this request is held; the
                    // watch is only the wakeup. Reading a mark first would let
                    // a request that arrives just before `open` compute a mark
                    // that already includes the release, and then wait for a
                    // bump that never comes.
                    while stalled.load(Ordering::Acquire) {
                        if opened.changed().await.is_err() {
                            return;
                        }
                    }
                    // One bodyless 200: a `HEAD` is complete the moment its
                    // head lands.
                    let _ = socket
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                        .await;
                    let _ = socket.flush().await;
                });
            }
        });
        MockProxy {
            addr,
            probe_url: format!("http://{addr}/warm"),
            stalled,
            opened,
            requests,
            heads,
        }
    }

    /// Read through the end of a request head. A prewarm is a `HEAD` with no
    /// body, so the blank line is the whole request.
    async fn read_head(socket: &mut tokio::net::TcpStream) -> Option<String> {
        use tokio::io::AsyncReadExt as _;
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            match socket.read(&mut byte).await {
                Ok(0) | Err(_) => return None,
                Ok(_) => {}
            }
            head.push(byte[0]);
            if head.ends_with(b"\r\n\r\n") {
                return Some(String::from_utf8_lossy(&head).into_owned());
            }
            if head.len() > 8_192 {
                return None;
            }
        }
    }
}
