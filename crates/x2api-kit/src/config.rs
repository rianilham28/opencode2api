//! `ServerConfig`: the format-agnostic server surface, plus the config
//! split that keeps it generic: a new service bin implements its *own*
//! provider config and never grows the shared struct.
//!
//! Loading: optional `.env` file, then optional JSON file (`--config`),
//! `server` section, every field defaulted; then `X2API_*` env overrides win
//! over both. The full document is returned alongside so the bin can pull its
//! `provider` section.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetryConfig {
    /// Total attempts per logical request (1 disables retrying).
    pub max_attempts: u32,
    pub base_ms: u64,
    pub cap_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_ms: 250,
            cap_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvKey {
    Bind,
    Port,
    ExtraBinds,
    MaxInflight,
    DrainSecs,
    LogLevel,
    LogJson,
    LogDir,
    LogPrefix,
    LogRotate,
    LogKeepFiles,
    LogStdout,
    ClientApiKey,
    RetryAttempts,
    UpstreamShards,
    Http2PriorKnowledge,
}

impl EnvKey {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Bind => ENV_BIND,
            Self::Port => ENV_PORT,
            Self::ExtraBinds => ENV_EXTRA_BINDS,
            Self::MaxInflight => ENV_MAX_INFLIGHT,
            Self::DrainSecs => ENV_DRAIN_SECS,
            Self::LogLevel => ENV_LOG_LEVEL,
            Self::LogJson => ENV_LOG_JSON,
            Self::LogDir => ENV_LOG_DIR,
            Self::LogPrefix => ENV_LOG_PREFIX,
            Self::LogRotate => ENV_LOG_ROTATE,
            Self::LogKeepFiles => ENV_LOG_KEEP_FILES,
            Self::LogStdout => ENV_LOG_STDOUT,
            Self::ClientApiKey => ENV_CLIENT_API_KEY,
            Self::RetryAttempts => ENV_RETRY_ATTEMPTS,
            Self::UpstreamShards => ENV_UPSTREAM_SHARDS,
            Self::Http2PriorKnowledge => ENV_HTTP2_PRIOR_KNOWLEDGE,
        }
    }
    const fn secret(self) -> bool {
        matches!(self, Self::ClientApiKey)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvKnob {
    pub key: EnvKey,
}

pub const ENV_OVERRIDES: &[EnvKnob] = &[
    EnvKnob { key: EnvKey::Bind },
    EnvKnob { key: EnvKey::Port },
    EnvKnob {
        key: EnvKey::ExtraBinds,
    },
    EnvKnob {
        key: EnvKey::MaxInflight,
    },
    EnvKnob {
        key: EnvKey::DrainSecs,
    },
    EnvKnob {
        key: EnvKey::LogLevel,
    },
    EnvKnob {
        key: EnvKey::LogJson,
    },
    EnvKnob {
        key: EnvKey::LogDir,
    },
    EnvKnob {
        key: EnvKey::LogPrefix,
    },
    EnvKnob {
        key: EnvKey::LogRotate,
    },
    EnvKnob {
        key: EnvKey::LogKeepFiles,
    },
    EnvKnob {
        key: EnvKey::LogStdout,
    },
    EnvKnob {
        key: EnvKey::ClientApiKey,
    },
    EnvKnob {
        key: EnvKey::RetryAttempts,
    },
    EnvKnob {
        key: EnvKey::UpstreamShards,
    },
    EnvKnob {
        key: EnvKey::Http2PriorKnowledge,
    },
];

/// Environment names consumed outside `ServerConfig::apply_env` by bins and
/// service/transport crates. They share the same documentation drift pin.
pub const EXTRA_ENV_OVERRIDES: &[&str] = &[
    ENV_CONFIG,
    ENV_ENV_FILE,
    ENV_UPSTREAM_URL,
    ENV_UPSTREAM_KEY,
    ENV_UPSTREAM_KEYS,
    ENV_PROXY_URL,
    ENV_PROXY_LANES,
    ENV_PROXY_SESSION_TTL_SECS,
];

/// Curated names used only by tests and ignored live fixtures.
pub const TEST_ENV_NAMES: &[&str] = &["X2API_LIVE_MODEL"];
pub const ENV_CONFIG: &str = "X2API_CONFIG";
pub const ENV_ENV_FILE: &str = "X2API_ENV_FILE";
pub const ENV_BIND: &str = "X2API_BIND";
pub const ENV_PORT: &str = "X2API_PORT";
pub const ENV_EXTRA_BINDS: &str = "X2API_EXTRA_BINDS";
pub const ENV_MAX_INFLIGHT: &str = "X2API_MAX_INFLIGHT";
pub const ENV_DRAIN_SECS: &str = "X2API_DRAIN_SECS";
pub const ENV_LOG_LEVEL: &str = "X2API_LOG_LEVEL";
pub const ENV_LOG_JSON: &str = "X2API_LOG_JSON";
pub const ENV_LOG_DIR: &str = "X2API_LOG_DIR";
pub const ENV_LOG_PREFIX: &str = "X2API_LOG_PREFIX";
pub const ENV_LOG_ROTATE: &str = "X2API_LOG_ROTATE";
pub const ENV_LOG_KEEP_FILES: &str = "X2API_LOG_KEEP_FILES";
pub const ENV_LOG_STDOUT: &str = "X2API_LOG_STDOUT";
pub const ENV_CLIENT_API_KEY: &str = "X2API_CLIENT_API_KEY";
pub const ENV_RETRY_ATTEMPTS: &str = "X2API_RETRY_ATTEMPTS";
pub const ENV_UPSTREAM_URL: &str = "X2API_UPSTREAM_URL";
pub const ENV_UPSTREAM_KEY: &str = "X2API_UPSTREAM_KEY";
pub const ENV_UPSTREAM_KEYS: &str = "X2API_UPSTREAM_KEYS";
pub const ENV_UPSTREAM_SHARDS: &str = "X2API_UPSTREAM_SHARDS";
pub const ENV_HTTP2_PRIOR_KNOWLEDGE: &str = "X2API_HTTP2_PRIOR_KNOWLEDGE";
pub const ENV_PROXY_URL: &str = "X2API_PROXY_URL";
pub const ENV_PROXY_LANES: &str = "X2API_PROXY_LANES";
pub const ENV_PROXY_SESSION_TTL_SECS: &str = "X2API_PROXY_SESSION_TTL_SECS";

/// Each client dialect a deployment can serve, named ONCE so `serves(...)` call
/// sites, `validate`, the router's gating and every enumerating test read the
/// same value — the drift that let a fourth dialect ship while two enumerations
/// still listed three is what this prevents.
///
/// The four are the definitions and the list is built FROM them, not the other
/// way round: a const written as `DIALECT_LABELS[0]` makes reordering the array
/// a silent relabel of every call site that reads it.
pub const DIALECT_CHAT: &str = "chat";
pub const DIALECT_MESSAGES: &str = "messages";
pub const DIALECT_RESPONSES: &str = "responses";
pub const DIALECT_GENERATE_CONTENT: &str = "generate-content";

/// The whole set, in route-registration order. Tests iterate THIS so a fifth
/// dialect is covered the day it is added to the two lists above.
pub const DIALECT_LABELS: &[&str] = &[
    DIALECT_CHAT,
    DIALECT_MESSAGES,
    DIALECT_RESPONSES,
    DIALECT_GENERATE_CONTENT,
];

/// File-rotation period for the `log_dir` appender. Rotation and pruning
/// are by UTC period boundary; the CURRENT period's file is opened eagerly
/// when telemetry initializes, so a bad `log_dir` fails the boot instead of
/// losing logs later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogRotate {
    Minutely,
    Hourly,
    #[default]
    Daily,
    Weekly,
    /// One file, no stamp, no rotation (`<service>.log`).
    Never,
}

/// Env override parses the same spellings as the JSON values.
impl std::str::FromStr for LogRotate {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "minutely" => Ok(Self::Minutely),
            "hourly" => Ok(Self::Hourly),
            "daily" => Ok(Self::Daily),
            "weekly" => Ok(Self::Weekly),
            "never" => Ok(Self::Never),
            other => Err(format!(
                "log_rotate must be minutely|hourly|daily|weekly|never, got {other:?}"
            )),
        }
    }
}

/// `deny_unknown_fields` is the reason a typo cannot survive: without it a
/// misspelled key (`"log_lovel"`, `"max_inflight": "256"`, a leftover
/// `"rate_limit"`) deserialises to the DEFAULT, so the operator believes they
/// configured something the process then silently ignores. The provider side has
/// always rejected this; a shared-knob surface that a fork inherits is worse, not
/// better, without the same check — and the failure mode is invisible on a laptop
/// where every default happens to be fine.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: String,
    pub port: u16,
    /// Additional full `host:port` listeners the SAME router serves on
    /// after the primary bind. Empty (the default) is one socket, as
    /// before. Every address is bound BEFORE serving starts and all share
    /// one drain signal, so a dead extra fails boot loudly rather than
    /// half-starting the process.
    #[serde(default)]
    pub extra_binds: Vec<String>,
    /// Hard wall-clock cap on the PRE-HANDOFF half of a request, including
    /// all retries: the buffered completion, a stream's setup, the fidelity
    /// lane's `relay_raw` call, and catalogue listings. A breach is
    /// pre-handoff by definition, so it answers 504 without cutting anything
    /// committed to the client.
    ///
    /// The POST-HANDOFF half of a stream is governed by `stream_deadline_secs`
    /// instead: once bytes are streaming, the client is committed, so the
    /// relay truncates and withholds its terminator rather than cutting the
    /// connection.
    pub request_timeout_secs: u64,
    /// A single stream may not outlive this; when it fires the relay emits a
    /// truncation frame and withholds `[DONE]`.
    pub stream_deadline_secs: u64,
    /// reqwest connect timeout on the shared client.
    pub connect_timeout_secs: u64,
    /// reqwest read timeout (per-chunk idle bound upstream-side); there is
    /// deliberately NO total reqwest timeout — it would cut long streams.
    pub upstream_read_timeout_secs: u64,
    /// SSE comment interval; 0 disables keepalive.
    pub sse_keepalive_secs: u64,
    pub max_body_bytes: usize,
    /// Admission semaphore size: overload is shed (503 + Retry-After), not
    /// queued into oblivion.
    pub max_inflight: usize,
    /// How long a request may queue for an admission slot before 503.
    pub admission_wait_secs: u64,
    /// Grace period for in-flight requests at shutdown before the server is
    /// force-dropped (two-stage drain: axum's graceful shutdown alone waits
    /// unbounded on streams).
    pub drain_secs: u64,
    /// Exact origins for CORS; empty list = no CORS layer (conservative
    /// default for a server-to-server proxy).
    pub cors_allow_origin: Vec<String>,
    /// If set, every request must carry `Authorization: Bearer <this>`
    /// (constant-time compared). If unset, the proxy is open (LAN/single-
    /// tenant posture — say so in your deployment, not in code).
    pub client_api_key: Option<String>,
    pub retry: RetryConfig,
    /// The base level for EVERY target: one of
    /// `trace|debug|info|warn|error|off`. `RUST_LOG` still wins when set, and is
    /// the place for per-crate tuning (it takes the full directive syntax this
    /// field deliberately does not).
    ///
    /// A blank or unrecognised value is a BOOT ERROR, not a silent choice: the
    /// filter library parses a directive whose level it cannot read as `OFF`
    /// rather than rejecting it, so a typo here — or an unfilled template leaving
    /// `""` — would otherwise boot clean and log nothing at all. `"off"` stays
    /// legal because it is a decision; an empty string is an accident. Verified
    /// against the binary: both `"infoo"` and `""` exit 1 naming the field.
    pub log_level: String,
    pub log_json: bool,
    /// Rotating NDJSON/text file appender directory when set; the console
    /// otherwise. Files: `<service>.<period>.log` in UTC
    /// (`service.2026-09-07.log`).
    pub log_dir: Option<String>,
    /// File-name prefix for those files; defaults to the bin's service name.
    ///
    /// It exists because the program crate is `service` in EVERY deployment
    /// (R1 keeps one process per upstream and R2 renames the `x2api-*`
    /// crates, never `service`), so all of them would otherwise write
    /// `service.<period>.log`. That is harmless while each has its own
    /// `log_dir` and a real hazard when they share one: pruning is
    /// prefix+suffix scoped, so instance A's boot prune culls instance B's
    /// period files, and same-period writes land in the same file.
    pub log_prefix: Option<String>,
    /// Rotation period for those files.
    pub log_rotate: LogRotate,
    /// How many files the appender prunes to (at boot and on rotation).
    /// `None` grows without bound. A restart inside the current period only
    /// appends, so observed retention can dip to `keep - 1`
    /// (tokio-rs/tracing#3496) — to retain m periods, set m + 1. Minimum 2:
    /// with 1, the boot prune (which runs BEFORE the live file is opened)
    /// eats the log it is about to write into. Ignored under
    /// `log_rotate: "never"` — there are no period files to bound, and a
    /// cap there would cull unstamped leftovers it can never replace.
    pub log_keep_files: Option<usize>,
    /// With `log_dir` set, keep mirroring to stdout (default on). The
    /// appender worker swallows every write error — a full disk loses the
    /// file stream in silence — so the supervisor's channel (journald,
    /// docker logs) is the independent signal; never point logrotate at
    /// appender-owned files (it renames the path the writer holds open;
    /// the appender reopens only at its own boundary).
    pub log_stdout: bool,
    /// Client dialects this deployment serves. Empty (the default) means all
    /// of them; naming a subset makes the others answer 404 instead of
    /// existing.
    ///
    /// Why runtime and not a Cargo feature: gating at compile time would put
    /// `cfg` on the `InboundFormat` variants and on every `match` over them
    /// — routes, error envelopes, the relay's sinks — roughly twenty
    /// attributes threaded through the clearest code in the server, to save
    /// compiling about two thousand lines that cost nothing to carry. The
    /// operational need is "do not expose a dialect I do not serve", and that
    /// is a routing decision.
    pub dialects: Vec<String>,
    /// Assume the upstream speaks h2c and skip negotiation. Over TLS this is
    /// unnecessary — ALPN already prefers h2 — and against an HTTP/1.1
    /// plaintext server it breaks every request, so it stays off by default.
    pub http2_prior_knowledge: bool,
    /// Independent connection pools per egress identity. One h2 connection
    /// multiplexes every stream, so a peer's `MAX_CONCURRENT_STREAMS` becomes
    /// this proxy's concurrency ceiling; sharding spreads streams over N
    /// connections (same exit IP, same session) to lift it. 1 is right until
    /// a lane is actually saturated.
    pub upstream_shards: usize,
    /// IANA zone ("Asia/Jakarta") or fixed offset ("+07:00") for log
    /// timestamps; UTC when unset. The zone applies to BOTH layouts — the json
    /// layer renders the same wall-clock string the text layer does, so
    /// UTC-comparable logs mean leaving this unset, not turning `log_json` on.
    pub log_tz: Option<String>,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".into(),
            port: 10080,
            extra_binds: Vec::new(),
            request_timeout_secs: 3_600,
            stream_deadline_secs: 3_600,
            connect_timeout_secs: 10,
            upstream_read_timeout_secs: 120,
            sse_keepalive_secs: 15,
            max_body_bytes: 16 * 1024 * 1024,
            max_inflight: 256,
            admission_wait_secs: 15,
            drain_secs: 25,
            cors_allow_origin: Vec::new(),
            client_api_key: None,
            retry: RetryConfig::default(),
            log_level: "info".into(),
            log_json: false,
            log_dir: None,
            log_prefix: None,
            log_rotate: LogRotate::default(),
            // ~7 daily files; the appender prunes to n-1 on a mid-period
            // restart, so the cap is asked one above the retention we
            // promise. None here would mean "grow until the disk is full",
            // which is not a conservative default for a template.
            log_keep_files: Some(8),
            log_stdout: true,
            log_tz: None,
            dialects: Vec::new(),
            http2_prior_knowledge: false,
            upstream_shards: 1,
        }
    }
}

/// Which env file to read: `X2API_ENV_FILE` when set, else `.env` beside the
/// working directory. Split out from the loading so the choice is testable
/// without touching the process environment.
fn env_file_path(explicit: Option<&str>) -> PathBuf {
    match explicit.filter(|v| !v.is_empty()) {
        Some(p) => PathBuf::from(p),
        None => PathBuf::from(".env"),
    }
}

/// Load `.env` into the process environment, returning the file that was
/// read. Call it FIRST in `main`: every `X2API_*` override below — and the
/// ones `ServiceConfig`/`ProxyConfig` read for themselves — resolves against
/// whatever the environment holds by then.
///
/// Real environment variables always win: a value already exported is never
/// replaced, so a container's injected secret beats a stale `.env` left in
/// the image. Absent file = silence, not an error; `.env` is a developer
/// convenience, and production usually injects the environment directly.
///
/// This is where credentials belong. A proxy URL carries a password, so it
/// has no business in a JSON config that gets committed — `.env` is
/// gitignored, `config.json` is the shape, not the secrets.
///
/// A file that EXISTS but cannot be parsed is a boot failure. The alternative
/// is the silent kind of damage this crate keeps being asked to remove: the
/// loader stops at the bad line, every secret after it is missing, and the
/// process serves 5xx upstream-auth errors for the rest of its life while the
/// operator reads the config as correct.
pub fn load_dotenv() -> anyhow::Result<Option<PathBuf>> {
    load_env_file(&env_file_path(std::env::var(ENV_ENV_FILE).ok().as_deref()))
}

/// `load_dotenv` with the path already decided.
pub fn load_env_file(path: &Path) -> anyhow::Result<Option<PathBuf>> {
    match dotenvy::from_path(path) {
        Ok(()) => Ok(Some(path.to_path_buf())),
        Err(err) if err.not_found() => Ok(None),
        Err(dotenvy::Error::LineParse(line, index)) => {
            // The text is withheld because dotenvy's OWN `Display` for this
            // variant interpolates the offending line — which in this file is a
            // credential — so the arm below must not print `{other}` the way the
            // catch-all does. Two numbers survive instead: the parser reports an
            // INDEX into that text while the operator needs a LINE number, so the
            // line is located by comparing the text back against the file, and
            // the index is labelled as an index only when that comparison finds
            // nothing (a value split across an ending the two disagree on).
            let number = std::fs::read_to_string(path).ok().and_then(|text| {
                text.lines()
                    .position(|candidate| candidate == line)
                    .map(|at| at + 1)
            });
            anyhow::bail!(
                "parsing {}: {} is not a `KEY=value` assignment — the line's text is withheld \
                 because a `.env` line is a credential",
                path.display(),
                match number {
                    Some(at) => format!("line {at}"),
                    None => format!("the line at index {index}"),
                }
            );
        }
        Err(other) => anyhow::bail!("reading {}: {other}", path.display()),
    }
}

/// The `server` section as serde must see it: absent OR null becomes an
/// empty object. Struct-level `#[serde(default)]` fills absent FIELDS from
/// a map but rejects `Value::Null` outright, and a null section is exactly
/// what an operator gets by deleting a file's keys without its braces —
/// while the docs promise the file itself is optional.
fn server_section(doc: &Value) -> Value {
    doc.get("server")
        .filter(|v| !v.is_null())
        .cloned()
        .unwrap_or(Value::Object(serde_json::Map::new()))
}

impl ServerConfig {
    /// Load `<file>.server` + env overrides. Returns the config plus the full
    /// document (empty object when no file) for the bin's `provider` section.
    pub fn load(path: Option<&Path>) -> anyhow::Result<(ServerConfig, Value)> {
        let doc = match path {
            Some(p) => {
                let text = std::fs::read_to_string(p)
                    .map_err(|e| anyhow::anyhow!("reading {}: {e}", p.display()))?;
                let v: Value = serde_json::from_str(&text)
                    .map_err(|e| anyhow::anyhow!("parsing {}: {e}", p.display()))?;
                v
            }
            None => Value::Object(serde_json::Map::new()),
        };
        let mut cfg: ServerConfig = serde_json::from_value(server_section(&doc))
            .map_err(|e| anyhow::anyhow!("server section: {e}"))?;
        cfg.apply_env()?;
        cfg.validate()?;
        Ok((cfg, doc))
    }

    fn apply_env(&mut self) -> anyhow::Result<()> {
        for knob in ENV_OVERRIDES {
            let Some(value) = env_string(knob.key.name()) else {
                continue;
            };
            match knob.key {
                EnvKey::Bind => self.bind = value,
                EnvKey::Port => self.port = parse_knob(knob.key, value)?,
                EnvKey::ExtraBinds => {
                    self.extra_binds = value
                        .split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                        .collect()
                }
                EnvKey::MaxInflight => self.max_inflight = parse_knob(knob.key, value)?,
                EnvKey::DrainSecs => self.drain_secs = parse_knob(knob.key, value)?,
                EnvKey::LogLevel => self.log_level = value,
                EnvKey::LogJson => self.log_json = parse_knob(knob.key, value)?,
                EnvKey::LogDir => self.log_dir = Some(value),
                EnvKey::LogPrefix => self.log_prefix = Some(value),
                EnvKey::LogRotate => self.log_rotate = parse_knob(knob.key, value)?,
                EnvKey::LogKeepFiles => self.log_keep_files = Some(parse_knob(knob.key, value)?),
                EnvKey::LogStdout => self.log_stdout = parse_knob(knob.key, value)?,
                EnvKey::ClientApiKey => self.client_api_key = Some(value),
                EnvKey::RetryAttempts => self.retry.max_attempts = parse_knob(knob.key, value)?,
                EnvKey::UpstreamShards => self.upstream_shards = parse_knob(knob.key, value)?,
                EnvKey::Http2PriorKnowledge => {
                    self.http2_prior_knowledge = parse_knob(knob.key, value)?
                }
            }
        }
        Ok(())
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.port != 0, "port must be non-zero");
        for extra in &self.extra_binds {
            anyhow::ensure!(
                extra.contains(':'),
                "server.extra_binds entry {extra:?} must be a full host:port \
                 (it does not inherit `port` — a typo silently binding the \
                 wrong socket is worse than a boot error)"
            );
        }
        anyhow::ensure!(self.retry.base_ms >= 1, "retry.base_ms >= 1");
        anyhow::ensure!(
            self.retry.cap_ms >= self.retry.base_ms,
            "retry.cap_ms must be >= retry.base_ms"
        );
        anyhow::ensure!(
            (1..=10).contains(&self.retry.max_attempts),
            "retry.max_attempts must be 1..=10"
        );
        anyhow::ensure!(self.max_inflight >= 1, "max_inflight >= 1");
        anyhow::ensure!(
            (1..=32).contains(&self.upstream_shards),
            "upstream_shards must be 1..=32"
        );
        for d in &self.dialects {
            anyhow::ensure!(
                DIALECT_LABELS.contains(&d.as_str()),
                "server.dialects: {d:?} is not a dialect ({})",
                DIALECT_LABELS.join("|")
            );
        }
        anyhow::ensure!(
            self.stream_deadline_secs >= self.sse_keepalive_secs.saturating_add(1)
                || self.sse_keepalive_secs == 0,
            "stream_deadline_secs must exceed the keepalive interval"
        );
        anyhow::ensure!(
            self.log_keep_files.is_none_or(|n| n >= 2),
            "log_keep_files must be unset (unbounded) or >= 2"
        );
        // A prefix that is empty or carries a separator would either widen the
        // prune filter across every file in `log_dir` (the empty-prefix hazard
        // `file_appender` already refuses for the service name) or try to write
        // outside it. Boot failure beats silent cross-instance pruning.
        if let Some(prefix) = &self.log_prefix {
            anyhow::ensure!(
                !prefix.is_empty()
                    && !prefix.contains('/')
                    && prefix.chars().all(|c| !c.is_whitespace()),
                "log_prefix {prefix:?} must be non-empty, contain no path separator, and have no whitespace \
                 (it is a file-name component, not a path)"
            );
        }
        // A typo in the level must fail at LOAD, not at the first log line. The
        // filter parser ACCEPTS garbage as a directive whose level is `OFF`, so
        // `log_level: "infoo"` would otherwise boot a proxy that logs nothing at
        // all. It sits here beside the port and prefix checks so `base_filter`
        // is the last line of defence rather than the only one. `RUST_LOG` is
        // NOT validated here: it takes the directive grammar this field does not.
        anyhow::ensure!(
            !self.log_level.trim().is_empty()
                && <tracing_subscriber::filter::LevelFilter as std::str::FromStr>::from_str(
                    self.log_level.trim(),
                )
                .is_ok(),
            "log_level {:?} is not a level (trace|debug|info|warn|error|off)",
            self.log_level
        );
        if let Some(tz) = &self.log_tz {
            anyhow::ensure!(
                crate::telemetry::resolve_timezone(tz).is_some(),
                "log_tz {tz:?} must be an IANA timezone (Asia/Jakarta) or fixed offset (+07:00)"
            );
        }
        // (Never + cap is legal and the cap is IGNORED in file_appender:
        // the default keep must not turn the plain `"log_rotate": "never"`
        // config into a boot failure over a knob nobody enabled.)
        Ok(())
    }

    /// Does this deployment serve that dialect? An empty list serves all —
    /// the default, so a fork that never thinks about this gets everything.
    pub fn serves(&self, dialect: &str) -> bool {
        self.dialects.is_empty() || self.dialects.iter().any(|d| d == dialect)
    }

    /// Resolve `bind:port` (boot-time only; a blocking DNS lookup here is
    /// acceptable and keeps `bind` usable as a hostname).
    pub fn socket_addr(&self) -> anyhow::Result<std::net::SocketAddr> {
        use std::net::ToSocketAddrs;
        let addrs = (self.bind.as_str(), self.port)
            .to_socket_addrs()
            .map_err(|e| anyhow::anyhow!("resolving {}:{}: {e}", self.bind, self.port))?;
        addrs
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("{}:{} resolved to no address", self.bind, self.port))
    }

    /// Primary + every `extra_binds` entry, resolved in bind order. Extras
    /// get the same resolver, so hostnames work there too; the whole list
    /// is resolved before the first bind so any bad entry fails boot.
    pub fn socket_addrs(&self) -> anyhow::Result<Vec<std::net::SocketAddr>> {
        use std::net::ToSocketAddrs;
        let mut out = vec![self.socket_addr()?];
        for extra in &self.extra_binds {
            let addrs = extra
                .to_socket_addrs()
                .map_err(|e| anyhow::anyhow!("resolving extra bind {extra:?}: {e}"))?;
            out.push(
                addrs.into_iter().next().ok_or_else(|| {
                    anyhow::anyhow!("extra bind {extra:?} resolved to no address")
                })?,
            );
        }
        Ok(out)
    }
}

fn env_string(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

fn parse_knob<T: std::str::FromStr>(key: EnvKey, value: String) -> anyhow::Result<T>
where
    T::Err: std::fmt::Display,
{
    value.parse().map_err(|error| {
        let name = key.name();
        if key.secret() {
            anyhow::anyhow!("{name} has an invalid secret value")
        } else {
            anyhow::anyhow!("{name} has invalid value {value:?}: {error}")
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testenv::{RestoreEnv, lock_env};

    fn readme_env_names() -> std::collections::BTreeSet<String> {
        let readme =
            std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../README.md"))
                .unwrap();
        let section = readme
            .split("<!-- env-overrides:start -->")
            .nth(1)
            .and_then(|rest| rest.split("<!-- env-overrides:end -->").next())
            .expect("README env override markers");
        section
            .split('`')
            .filter(|token| token.starts_with("X2API_"))
            .map(str::to_string)
            .collect()
    }

    fn env_example_names() -> std::collections::BTreeSet<String> {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.env.example");
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| line.trim_start().trim_start_matches('#').trim_start())
            .filter_map(|line| line.strip_prefix("X2API_"))
            .filter_map(|line| {
                line.split_once('=')
                    .map(|(name, _)| format!("X2API_{name}"))
            })
            .collect()
    }
    #[test]
    fn partial_json_lands_on_defaults() {
        let mut doc = Value::Object(serde_json::Map::new());
        doc["server"] = serde_json::json!({"port": 9999, "retry": {"max_attempts": 5}});
        let cfg: ServerConfig =
            serde_json::from_value(doc.get("server").cloned().unwrap()).unwrap();
        assert_eq!(cfg.port, 9999);
        assert_eq!(cfg.retry.max_attempts, 5);
        // sibling retry knobs keep their defaults under a partial object
        assert_eq!(cfg.retry.base_ms, RetryConfig::default().base_ms);
        assert_eq!(cfg.max_body_bytes, 16 * 1024 * 1024);
    }

    #[test]
    fn extra_binds_resolve_after_primary_and_reject_incomplete_forms() {
        let mut cfg = ServerConfig::default();
        let _guard = lock_env();
        // Empty (the default): exactly one address, the primary — single-bind
        // is not a special case here, it is just this case.
        assert_eq!(cfg.socket_addrs().unwrap().len(), 1);
        // A full second address, resolved in order after the primary.
        let all: Vec<&'static str> = ENV_OVERRIDES.iter().map(|knob| knob.key.name()).collect();
        let _restore_all = RestoreEnv::clear(&all);
        cfg.extra_binds = vec!["127.0.0.1:8081".into()];
        cfg.validate().expect("full host:port is valid");
        let addrs = cfg.socket_addrs().unwrap();
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs[1].to_string(), "127.0.0.1:8081");
        assert_eq!(addrs[0].port(), cfg.port, "primary keeps its port");
        // Bare port must NOT inherit — the silent wrong-socket bind is the
        // failure the boot error exists to prevent.
        cfg.extra_binds = vec!["8081".into()];
        assert!(cfg.validate().is_err(), "bare port rejected");
        cfg.extra_binds = vec![];
        // Env override: comma-separated, blanks dropped. Edition-2024
        // requires the unsafe wrapper; no other test in this crate reads
        // these vars, so the process-global write cannot race a peer.
        // SAFETY: sole writer of X2API_EXTRA_BINDS in this test binary.
        let _restore = RestoreEnv::clear(&[ENV_EXTRA_BINDS]);
        unsafe {
            std::env::set_var(ENV_EXTRA_BINDS, "127.0.0.1:8081 , 127.0.0.1:8082,,");
            cfg.apply_env().unwrap();
            std::env::remove_var(ENV_EXTRA_BINDS);
        }
        assert_eq!(
            cfg.socket_addrs().unwrap()[1..],
            [
                "127.0.0.1:8081".parse().unwrap(),
                "127.0.0.1:8082".parse().unwrap()
            ]
        );
    }

    #[test]
    fn env_file_defaults_to_dot_env_and_honours_an_explicit_path() {
        assert_eq!(env_file_path(None), PathBuf::from(".env"));
        assert_eq!(env_file_path(Some("")), PathBuf::from(".env"));
        assert_eq!(
            env_file_path(Some("/etc/x2api/prod.env")),
            PathBuf::from("/etc/x2api/prod.env")
        );
    }

    /// The level is checked at LOAD, so a typo cannot boot a proxy that logs
    /// nothing: the filter parser accepts `"infoo"` and `""` as a directive whose
    /// level is OFF, which is a silent failure with a clean start.
    #[test]
    fn a_blank_or_unrecognised_level_fails_validation() {
        for bad in ["", "   ", "infoo", "10", "INFO!", "info,debug"] {
            let cfg = ServerConfig {
                log_level: bad.into(),
                ..Default::default()
            };
            let err = cfg
                .validate()
                .err()
                .unwrap_or_else(|| panic!("log_level {bad:?} passed validation"));
            assert!(
                err.to_string().contains("log_level"),
                "must name the field: {err}"
            );
        }
        for good in ["trace", "debug", "info", "warn", "error", "off", "INFO"] {
            ServerConfig {
                log_level: good.into(),
                ..Default::default()
            }
            .validate()
            .unwrap_or_else(|e| panic!("log_level {good:?} is a level: {e}"));
        }
    }

    /// The loader is a convenience, not a requirement: production injects the
    /// environment directly and must not be forced to ship a file.
    #[test]
    fn a_missing_env_file_is_silence_not_an_error() {
        let _guard = lock_env();
        assert!(
            load_env_file(Path::new("./definitely-not-here.env"))
                .unwrap()
                .is_none()
        );
    }

    /// A file that EXISTS but cannot be parsed must stop the boot, and the
    /// failure has to name the line without quoting it — the parser's own
    /// message interpolates the text, and in this file the text is a secret.
    /// The key is one nothing reads, so an unexpected success cannot leak a
    /// value into the process environment either.
    #[test]
    fn an_unparseable_env_line_fails_boot_without_revealing_its_text() {
        let _guard = lock_env();
        let _restore = RestoreEnv::clear(&[concat!("X2API_", "ENVTEST_KEY")]);
        let dir = std::env::temp_dir().join(format!("x2api-env-parse-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(".env");
        std::fs::write(
            &path,
            "# comment\nX2API_ENVTEST_KEY=sk-live-super-secret\nthis line is not an assignment\n",
        )
        .unwrap();

        let err = load_env_file(&path).unwrap_err().to_string();
        assert!(err.contains(".env"), "must name the file: {err}");
        assert!(err.contains("line 3"), "must name the line: {err}");
        assert!(
            !err.contains("sk-live-super-secret") && !err.contains("this line is not"),
            "the failure quoted the secret file: {err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_empty_dialect_list_serves_everything() {
        let cfg = ServerConfig::default();
        for d in DIALECT_LABELS {
            assert!(cfg.serves(d), "{d} must be served by default");
        }
    }

    /// The exclusion is asserted against EVERY label but the named one, not a
    /// fixed pair and not "everything past index 0": a fifth dialect added to
    /// `DIALECT_LABELS` is covered the day it exists, and reordering the list
    /// cannot quietly change which dialect is excluded.
    #[test]
    fn a_named_subset_excludes_the_rest() {
        let cfg = ServerConfig {
            dialects: vec![DIALECT_CHAT.into()],
            ..Default::default()
        };
        assert!(
            cfg.serves(DIALECT_CHAT),
            "the named dialect itself must be served"
        );
        for d in DIALECT_LABELS.iter().filter(|d| **d != DIALECT_CHAT) {
            assert!(!cfg.serves(d), "{d} must not be served by [\"chat\"]");
        }
    }

    #[test]
    fn a_typo_in_the_dialect_list_fails_at_boot() {
        // Silently serving nothing would look exactly like a working proxy
        // until the first request.
        let cfg = ServerConfig {
            dialects: vec!["anthropic".into()],
            ..Default::default()
        };
        let e = cfg.validate().unwrap_err().to_string();
        assert!(e.contains("not a dialect"), "got {e}");
    }

    #[test]
    fn rejects_zero_port() {
        let cfg = ServerConfig {
            port: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rotation_knobs_default_reject_and_parse() {
        // absent = bounded-by-default daily, mirroring the console; the
        // env spellings match the JSON ones
        let cfg: ServerConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(cfg.log_rotate, LogRotate::Daily);
        assert_eq!(cfg.log_keep_files, Some(8));
        assert!(cfg.log_stdout);
        assert_eq!("hourly".parse(), Ok(LogRotate::Hourly));
        assert_eq!("NEVER".parse(), Ok(LogRotate::Never));
        assert!("fortnightly".parse::<LogRotate>().is_err());
        // 1 would let the boot prune (which runs before the live file
        // opens) delete the log being written; 0 disables pruning in the
        // appender and hides "unbounded" behind a number — both rejected,
        // None is the one spelling of unbounded
        for keep in [Some(0), Some(1)] {
            let bad = ServerConfig {
                log_keep_files: keep,
                ..Default::default()
            };
            assert!(bad.validate().is_err(), "keep_files {keep:?} must reject");
        }
        // never + cap is LEGAL (the cap is ignored at the appender —
        // rejecting it would make the DEFAULT keep fail a plain `never`
        // config at boot), and unstamped leftovers stay untouched
        let never_capped = ServerConfig {
            log_rotate: LogRotate::Never,
            ..Default::default()
        };
        assert_eq!(never_capped.log_keep_files, Some(8));
        assert!(never_capped.validate().is_ok());
        let unbounded = ServerConfig {
            log_keep_files: None,
            ..Default::default()
        };
        assert!(unbounded.validate().is_ok());
    }

    /// The load path a no-`--config` boot takes: absent and null `server`
    /// sections alike land on defaults (the service crate's own provider
    /// requirements are a separate, deliberate failure downstream).
    #[test]
    fn absent_or_null_server_section_lands_on_defaults() {
        let _guard = lock_env();
        let docs = [
            Value::Object(serde_json::Map::new()),
            serde_json::json!({"server": null}),
        ];
        let names: Vec<&'static str> = ENV_OVERRIDES.iter().map(|knob| knob.key.name()).collect();
        let _restore = RestoreEnv::clear(&names);
        for doc in docs {
            let cfg: ServerConfig = serde_json::from_value(server_section(&doc)).unwrap();
            assert_eq!(cfg.log_rotate, LogRotate::Daily);
        }
        // a real section still passes through untouched
        let table = server_section(&serde_json::json!({"server": {"port": 9000}}));
        assert_eq!(table["port"], 9000);
    }

    /// End-to-end through `load` itself: an absent FILE is defaults on the
    /// template's surface — a service bin's own provider requirements fail
    /// later, deliberately, in its crate.
    #[test]
    fn server_load_of_an_absent_file_lands_on_defaults() {
        let _guard = lock_env();
        let names: Vec<&'static str> = ENV_OVERRIDES.iter().map(|knob| knob.key.name()).collect();
        let _restore = RestoreEnv::clear(&names);
        assert!(ServerConfig::load(None).is_ok());
    }

    #[test]
    fn retry_rejects_unknown_json_keys_instead_of_defaulting_them() {
        let err = serde_json::from_value::<ServerConfig>(serde_json::json!({
            "retry": {"base_ms": 250, "typo": 1}
        }))
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("unknown field") && err.contains("typo"),
            "{err}"
        );
    }

    #[test]
    fn retry_boundaries_are_rejected_with_the_field_named() {
        let cases = [
            (
                RetryConfig {
                    max_attempts: 0,
                    base_ms: 250,
                    cap_ms: 5_000,
                },
                "max_attempts",
            ),
            (
                RetryConfig {
                    max_attempts: 11,
                    base_ms: 250,
                    cap_ms: 5_000,
                },
                "max_attempts",
            ),
            (
                RetryConfig {
                    max_attempts: 3,
                    base_ms: 0,
                    cap_ms: 5_000,
                },
                "base_ms",
            ),
            (
                RetryConfig {
                    max_attempts: 3,
                    base_ms: 300,
                    cap_ms: 299,
                },
                "cap_ms",
            ),
        ];
        for (retry, field) in cases {
            let err = ServerConfig {
                retry,
                ..Default::default()
            }
            .validate()
            .unwrap_err()
            .to_string();
            assert!(
                err.contains(field),
                "{field} boundary must name itself: {err}"
            );
        }
    }

    #[test]
    fn log_tz_accepts_iana_and_fixed_offsets_and_names_bad_values() {
        for good in ["Asia/Jakarta", "UTC", "+07:00", "-05:30"] {
            ServerConfig {
                log_tz: Some(good.into()),
                ..Default::default()
            }
            .validate()
            .unwrap_or_else(|e| panic!("{good:?} is accepted: {e}"));
        }
        let err = ServerConfig {
            log_tz: Some("Mars/Olympus_Mons".into()),
            ..Default::default()
        }
        .validate()
        .unwrap_err()
        .to_string();
        assert!(err.contains("log_tz"), "{err}");
        assert!(
            err.contains("Asia/Jakarta") && err.contains("+07:00"),
            "{err}"
        );
    }

    #[test]
    fn env_parsing_is_strict_without_turning_empty_secrets_on() {
        let _guard = lock_env();
        let names: Vec<&'static str> = ENV_OVERRIDES.iter().map(|knob| knob.key.name()).collect();
        let _restore = RestoreEnv::clear(&names);
        let (err, cfg) = unsafe {
            std::env::set_var(ENV_PORT, "not-a-port");
            let err = ServerConfig::load(None).unwrap_err().to_string();
            std::env::remove_var(ENV_PORT);
            std::env::set_var(ENV_CLIENT_API_KEY, "");
            let (cfg, _) = ServerConfig::load(None).unwrap();
            (err, cfg)
        };
        assert!(
            err.contains(ENV_PORT) && err.contains("not-a-port"),
            "{err}"
        );
        assert!(cfg.client_api_key.is_none());
        drop(_restore);
    }

    #[test]
    fn env_docs_and_literal_consumers_match_registered_inventory() {
        let example = env_example_names();
        let readme = readme_env_names();
        let registered = ENV_OVERRIDES
            .iter()
            .map(|knob| knob.key.name().to_string())
            .chain(EXTRA_ENV_OVERRIDES.iter().map(|name| (*name).to_string()))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            example, readme,
            ".env.example and README env section drifted"
        );
        assert_eq!(example, registered, "documented service env drift");
        assert_eq!(TEST_ENV_NAMES, &["X2API_LIVE_MODEL"]);
    }
}
