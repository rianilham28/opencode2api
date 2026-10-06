//! tracing + metrics bootstrap, shared by every service bin.
//!
//! The crate name — log target, filter default, and file name — is
//! parameterized rather than hardcoded: pass it once in `init`.

use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

use crate::config::{LogRotate, ServerConfig};

/// Held by `main` for the process lifetime; dropping it early loses buffered
/// log lines, so never construct-then-discard in a temp.
pub struct TelemetryGuard {
    _appender: Option<tracing_appender::non_blocking::WorkerGuard>,
}

/// Render wall-clock timestamps in a configured zone. The subscriber trait
/// hands no timestamp to the formatter (0.3.23 signature), so this reads the
/// clock itself — one syscall per *log line*, not per request.
///
/// The layout is RFC 3339 to the millisecond (`…:15.591+07:00`), deliberately
/// NOT jiff's `Display` (`…:15.591324+07:00[+07:00]`): the trailing bracket
/// repeats the offset that is already in the suffix, and microsecond precision
/// is noise next to a `duration_ms` field — 9 bytes per line, every line.
#[derive(Clone)]
struct ZonedTime {
    tz: jiff::tz::TimeZone,
}

impl FormatTime for ZonedTime {
    fn format_time(
        &self,
        w: &mut tracing_subscriber::fmt::format::Writer<'_>,
    ) -> core::fmt::Result {
        // The clone is not avoidable at this API — `Timestamp::to_zoned` takes
        // the zone by value — and `TimeZone`'s own data is refcounted inside
        // jiff, so this is a cheap clone per line, not a reparsed zone.
        let zoned = jiff::Timestamp::now().to_zoned(self.tz.clone());
        write!(w, "{}", zoned.strftime("%Y-%m-%dT%H:%M:%S%.3f%:z"))
    }
}

/// Resolve "Asia/Jakarta" (IANA) or "+07:00" (fixed offset); `None` => UTC.
pub fn resolve_timezone(s: &str) -> Option<jiff::tz::TimeZone> {
    if let Ok(tz) = jiff::tz::TimeZone::get(s) {
        return Some(tz);
    }
    parse_offset(s).map(jiff::tz::TimeZone::fixed)
}

/// `jiff::tz::Offset` implements no `FromStr`, so accept the common forms
/// by hand: `Z`, `+07`, `-05:30`, `+0700`.
fn parse_offset(s: &str) -> Option<jiff::tz::Offset> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("z") {
        return jiff::tz::Offset::from_seconds(0).ok();
    }
    let sign = match s.as_bytes().first() {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => return None,
    };
    let body = &s[1..];
    let (h, m) = if let Some((a, b)) = body.split_once(':') {
        (a.parse::<i32>().ok()?, b.parse::<i32>().ok()?)
    } else if body.len() == 4 {
        (
            body[..2].parse::<i32>().ok()?,
            body[2..].parse::<i32>().ok()?,
        )
    } else {
        (body.parse::<i32>().ok()?, 0)
    };
    if h > 23 || m > 59 {
        return None;
    }
    jiff::tz::Offset::from_seconds(sign * (h * 3600 + m * 60)).ok()
}

/// Build the JSON layer used by shipped telemetry and test capture.
fn json_layer<S, W>(
    writer: W,
    timer: ZonedTime,
) -> impl tracing_subscriber::layer::Layer<S> + Send + Sync + 'static
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    W: for<'writer> tracing_subscriber::fmt::MakeWriter<'writer> + Send + Sync + 'static,
{
    tracing_subscriber::fmt::layer()
        .with_writer(writer)
        .with_ansi(false)
        .with_timer(timer)
        .json()
        .with_span_list(false)
}

/// Build the shipped JSON layout with UTC timestamps for test capture.
#[doc(hidden)]
pub fn capture_json_layer<S, W>(
    writer: W,
) -> impl tracing_subscriber::layer::Layer<S> + Send + Sync + 'static
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    W: for<'writer> tracing_subscriber::fmt::MakeWriter<'writer> + Send + Sync + 'static,
{
    json_layer(
        writer,
        ZonedTime {
            tz: jiff::tz::TimeZone::UTC,
        },
    )
}

/// The level a deployment asked for, and nothing else.
///
/// `RUST_LOG` wins when set (it is the per-target lever, and the only place
/// per-crate tuning belongs). Otherwise `server.log_level` IS the base level.
///
/// It used to be spelled `format!("{service_name}={},info", …)`, which did two
/// wrong things at once: the directive named the BIN's crate while every line
/// this proxy writes carries a LIBRARY target (`opencode2api_server::…`,
/// `opencode2api_kit::…`), so raising the level raised nothing; and the unconditional
/// `,info` base meant `log_level: "warn"` still emitted every INFO line — the
/// knob could neither quiet the per-request record nor reveal a library `debug`,
/// which is the same inert-config class as a `.env` line that silently stops
/// loading. The consequence is now real and worth stating: setting `warn`
/// disables the request span (`PathOnlyMakeSpan` is an `info_span!`), so lines
/// that lean on span attribution lose their `request_id` — the terminal record
/// keeps it, because it carries the field itself.
fn default_filter(cfg: &ServerConfig) -> anyhow::Result<tracing_subscriber::EnvFilter> {
    match std::env::var("RUST_LOG") {
        // The two fields have different grammars on purpose: `log_level` is ONE
        // level word (validated by `base_filter`, so a typo cannot become a quiet
        // `OFF`), while `RUST_LOG` keeps the library's directive syntax —
        // `RUST_LOG=info,opencode2api_kit=debug` is a routine value and routing it
        // through the level parser would refuse to boot the proxy. Errors are
        // reported, not fallen back over: losing the one lever an operator
        // reaches for under load, in silence, is the inert-config class this
        // function exists to remove.
        Ok(raw) => tracing_subscriber::EnvFilter::builder()
            .parse(raw.as_str())
            .map_err(|e| anyhow::anyhow!("RUST_LOG {raw:?} is unusable: {e}")),
        Err(_) => base_filter(&cfg.log_level),
    }
}

/// The base level, from a word, not a directive list.
///
/// `EnvFilter::new` PANICS on garbage and its lenient siblings ACCEPT it: a
/// directive with no recognizable level parses to level `OFF`, so
/// `server.log_level: "infoo"` — or `""` — boots cleanly and logs NOTHING, which
/// is the inert-config class this wave has been removing, wearing a different
/// hat. Validating against `LevelFilter` is what makes a typo a boot error that
/// names the field, the way `base_url`, `log_dir` and `.env` already do;
/// `validate()` rejects it first, and this is the last line of defence for a
/// config built by hand.
fn base_filter(level: &str) -> anyhow::Result<tracing_subscriber::EnvFilter> {
    use std::str::FromStr;
    // Blank first, and separately: `LevelFilter::from_str("")` is ACCEPTED (it
    // maps to OFF), so an empty `log_level` — a dropped value, an unfilled
    // template — would otherwise boot clean and log nothing. `"off"` stays legal:
    // it is a decision, an empty string is an accident.
    anyhow::ensure!(
        !level.trim().is_empty(),
        "server.log_level is empty; it must be one of trace|debug|info|warn|error|off"
    );
    let parsed = tracing_subscriber::filter::LevelFilter::from_str(level.trim()).map_err(|e| {
        anyhow::anyhow!(
            "server.log_level {level:?} is not a level (trace|debug|info|warn|error|off): {e}"
        )
    })?;
    Ok(tracing_subscriber::EnvFilter::builder()
        .with_default_directive(parsed.into())
        .parse_lossy(""))
}

/// Install the global subscriber. `service_name` is the crate name (snake or
/// kebab): the log file's prefix when `server.log_prefix` is unset, so files
/// are `<prefix>.<period>.log` in UTC.
pub fn init(cfg: &ServerConfig, service_name: &str) -> anyhow::Result<TelemetryGuard> {
    let filter = default_filter(cfg)?;
    let tz = cfg
        .log_tz
        .as_deref()
        .and_then(resolve_timezone)
        .unwrap_or(jiff::tz::TimeZone::UTC);
    let timer = ZonedTime { tz };

    // Two concrete initializations instead of writer type-erasure: cheaper to
    // read than fighting `MakeWriter` generics, and the sink choice is fixed
    // at boot anyway. Each layer is BUILT inside its own branch: a layer's
    // subscriber type parameter resolves at its first `.with()` site, so a
    // construction shared between the json and text chains would pin it to
    // one and poison the other.
    //
    // The file is never the only witness: the appender worker drops every
    // write error (ENOSPC included) in silence, so the supervisor channel is
    // mirrored alongside it unless explicitly switched off.
    if let Some(dir) = &cfg.log_dir {
        let appender = file_appender(cfg, dir, service_name)?;
        let (writer, guard) = tracing_appender::non_blocking(appender);
        if cfg.log_json {
            // `display_span_list` defaults to TRUE, which writes the enclosing
            // span context TWICE per record: once as the `"span"` object the
            // correlation spine reads, once as a `"spans"` array holding the
            // very same single span. Measured on two captured lines of the same
            // event, that array alone cost 170 bytes of a 674-byte line — a
            // quarter of every JSON record, for a second copy of data the line
            // already printed. Off here and on every other json site. (The
            // setter is defined only on the json-configured layer, hence its
            // position after `.json()` in each chain.)
            let file = json_layer(writer, timer.clone());
            let mirror = cfg
                .log_stdout
                .then(|| json_layer(std::io::stdout, timer.clone()));
            tracing_subscriber::registry()
                .with(filter)
                .with(file)
                .with(mirror)
                .init();
        } else {
            let file = tracing_subscriber::fmt::layer()
                .with_writer(writer)
                .with_ansi(false)
                .with_timer(timer.clone());
            let mirror = cfg.log_stdout.then(|| {
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stdout)
                    .with_timer(timer.clone())
            });
            tracing_subscriber::registry()
                .with(filter)
                .with(file)
                .with(mirror)
                .init();
        }
        Ok(TelemetryGuard {
            _appender: Some(guard),
        })
    } else {
        if cfg.log_json {
            let console = json_layer(std::io::stdout, timer.clone());
            tracing_subscriber::registry()
                .with(filter)
                .with(console)
                .init();
        } else {
            let console = tracing_subscriber::fmt::layer()
                .with_writer(std::io::stdout)
                .with_timer(timer);
            tracing_subscriber::registry()
                .with(filter)
                .with(console)
                .init();
        }
        Ok(TelemetryGuard { _appender: None })
    }
}

/// Build the rotating appender from `server.log_*`. Separate from `init` so
/// the naming and retention contract is testable without installing a
/// global subscriber.
fn file_appender(
    cfg: &ServerConfig,
    dir: &str,
    service_name: &str,
) -> anyhow::Result<tracing_appender::rolling::RollingFileAppender> {
    use tracing_appender::rolling::{RollingFileAppender, Rotation};
    // The Builder's own default is NEVER — dropping this match would
    // silently disable rotation for a config that asks for a period.
    let rotation = match cfg.log_rotate {
        LogRotate::Minutely => Rotation::MINUTELY,
        LogRotate::Hourly => Rotation::HOURLY,
        LogRotate::Daily => Rotation::DAILY,
        LogRotate::Weekly => Rotation::WEEKLY,
        LogRotate::Never => Rotation::NEVER,
    };
    // Stamp-last names (`svc.2026-09-07.log`) keep `*.log` globs working
    // and match the prefix/suffix filter the appender's own prune pass uses;
    // `never` drops the stamp, leaving `svc.log`. Retention is in-process:
    // `max_log_files` prunes at boot and at rotation, leaving n-1 for the
    // new file (a restart inside a period only appends, so retention can
    // dip to n-1 — tokio-rs/tracing#3496). External logrotate can never sit
    // on these files: it renames the path the writer holds open and the
    // appender reopens only at its own period boundary. A directory holding
    // the older `svc.log.<date>` shape is NOT matched by the prune filter
    // (prefix+suffix based) — operators upgrade once by deleting those.

    // The prefix, not the crate, is what keeps two deployments sharing a
    // `log_dir` from pruning each other's files; `service_name` is the
    // fallback and is the literal `service` in every fork, since R1 keeps one
    // program crate and R2 renames only the `opencode2api-*` crates.
    //
    // An empty prefix collapses to no-prefix upstream: every file in the
    // directory (date-only names, other services' output) becomes a prune
    // candidate. Cheap to refuse, invisible to the happy path — and checked
    // AFTER resolution, because an explicitly configured `""` reaches it the
    // same way `log_dir` + no service name does.
    let prefix = cfg.log_prefix.as_deref().unwrap_or(service_name);
    anyhow::ensure!(!prefix.is_empty(), "log file prefix is empty");
    let mut builder = RollingFileAppender::builder()
        .rotation(rotation)
        .filename_prefix(prefix)
        .filename_suffix("log");
    // The cap bounds PERIOD files; `never` has exactly one and its
    // unstamped name would make every `<service>*.log` leftover a prune
    // candidate with nothing to replace it — so under `never` the knob is
    // ignored here, not rejected at boot (the DEFAULT keep must not fail
    // an operator's plain `"log_rotate": "never"`).
    if !matches!(cfg.log_rotate, LogRotate::Never)
        && let Some(keep) = cfg.log_keep_files
    {
        builder = builder.max_log_files(keep);
    }
    builder
        .build(dir)
        .map_err(|e| anyhow::anyhow!("creating rolling log file in {dir}: {e}"))
}

/// Metric names, defined ONCE, here.
///
/// They live in the crate that installs the recorder rather than the one that
/// serves requests because `install_metrics` must DESCRIBE them at boot, and a
/// description spelled separately from the counter it describes is how a
/// dashboard ends up with a series that has help text but no data (or the
/// reverse). `opencode2api_server::names` re-exports these so handlers still have one
/// obvious import.
pub mod names {
    pub const REQUESTS: &str = "opencode2api_requests_total";
    pub const DURATION: &str = "opencode2api_request_duration_seconds";
    /// Request and first-token latency buckets, including the stream deadline.
    pub const DURATION_BUCKETS: &[f64] = &[
        0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0,
        3600.0,
    ];
    /// Admission wait buckets sized for permit contention rather than LLM latency.
    pub const ADMISSION_QUEUE_BUCKETS: &[f64] = &[0.05, 0.25, 1.0, 5.0, 15.0];
    /// Time spent waiting for an admission permit.
    pub const ADMISSION_QUEUE: &str = "opencode2api_admission_queue_seconds";
    /// First-token latency of counted streams.
    pub const STREAM_FIRST_TOKEN: &str = "opencode2api_stream_first_token_seconds";
    /// Cooldown episodes started by upstream credential, by reason.
    pub const CREDENTIAL_COOLDOWNS: &str = "opencode2api_credential_cooldowns_total";
    pub const STREAMS: &str = "opencode2api_streams_total";
    /// Tokens the UPSTREAM reported, by kind. A proxy in front of a metered
    /// API is asked "what did this cost" before anything else, and the IR
    /// already carries the answer on every path that parses.
    pub const TOKENS: &str = "opencode2api_tokens_total";
    /// Upstream credentials by state (`ready` | `cooling`). A pool is the one
    /// piece of state in this proxy that is invisible in the RESPONSE: a request
    /// that succeeds on the third key looks exactly like one that never failed,
    /// so without this the only way to notice a dead credential is that every
    /// request eventually starts answering 503.
    pub const CREDENTIALS: &str = "opencode2api_credentials";
    /// Media parts the proxy ACCEPTED, by kind (`image` | `file`). Counted on
    /// every folded path, the identity chat dialect included: `messages` are
    /// already `Value`s by the time the pipeline runs, so the walk reads what is
    /// in memory and decodes no payload. The FIDELITY LANE stays uncounted, like
    /// its tokens — its bytes are the vendor's dialect, and parsing them would
    /// give the lane the vendor knowledge it exists to avoid. What this answers
    /// is "is the multimodal traffic I pay per-image vendors for actually
    /// arriving", which a vendor 400 only reports once the request failed.
    ///
    /// Cost, measured rather than reasoned: the walk adds no allocation per
    /// frame (bench `allocs/f` identical with the site present and removed:
    /// 0.16/0.16 relay, 7.20/7.23 messages, 23.21/23.20 gemini) and decodes no
    /// payload. Its µs/frame could not be measured at this sample size — the
    /// no-proxy mock row moved +104% between the two runs, so wall-clock here
    /// is machine noise, and that is the honest statement, not "free".
    pub const MEDIA: &str = "opencode2api_media_total";
}

fn build_prometheus_recorder() -> metrics_exporter_prometheus::PrometheusRecorder {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(names::DURATION.to_owned()),
            names::DURATION_BUCKETS,
        )
        .expect("duration histogram buckets are valid")
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(names::STREAM_FIRST_TOKEN.to_owned()),
            names::DURATION_BUCKETS,
        )
        .expect("first-token histogram buckets are valid")
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(names::ADMISSION_QUEUE.to_owned()),
            names::ADMISSION_QUEUE_BUCKETS,
        )
        .expect("admission histogram buckets are valid")
        .build_recorder()
}
/// Install the global Prometheus recorder. MUST be called once, before
/// serving, and FROM WITHIN a tokio runtime (the upkeep sweep below spawns
/// a task; `tokio::spawn` outside a runtime panics — every opencode2api bin calls
/// this from async `main`, which satisfies both). Handlers record through
/// the global recorder, and a `build_recorder()` that was never installed
/// renders empty.
pub fn install_metrics() -> anyhow::Result<metrics_exporter_prometheus::PrometheusHandle> {
    let recorder = build_prometheus_recorder();
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder)
        .map_err(|_| anyhow::anyhow!("a global metrics recorder was already installed"))?;
    // Describe at boot so series exist pre-observation (dashboards treat
    // empty and absent differently).
    metrics::describe_counter!(
        names::REQUESTS,
        "Requests completed, by endpoint and status"
    );
    metrics::describe_gauge!(
        names::CREDENTIALS,
        "Upstream credentials by state (label: state=ready|cooling)",
    );
    metrics::describe_histogram!(
        names::DURATION,
        "End-to-end request duration (terminal frame for streams)"
    );
    metrics::describe_histogram!(
        names::ADMISSION_QUEUE,
        "Time a request waited for an admission permit"
    );
    metrics::describe_histogram!(
        names::STREAM_FIRST_TOKEN,
        "First-token latency of counted streams"
    );
    metrics::describe_counter!(
        names::CREDENTIAL_COOLDOWNS,
        "Cooldown episodes started on the upstream credential pool, by reason"
    );
    metrics::describe_counter!(
        names::TOKENS,
        "Upstream-reported tokens, by kind (prompt|completion|cached|reasoning|cache_creation|cache_read)"
    );
    metrics::describe_counter!(
        names::MEDIA,
        "Accepted media parts on folded requests, by kind (image|file)"
    );
    // Described like the rest, because it is recorded like the rest
    // (`relay::StreamOutcome`): a family with data and no help text is the
    // failure this block exists to prevent, and `streams` was the one series an
    // operator could see counted in /metrics with nothing saying what its
    // `result` labels meant.
    metrics::describe_counter!(
        names::STREAMS,
        "Stream outcomes, by endpoint, format and result (ok|failed|truncated|panicked|dropped)"
    );
    // Upkeep sweep: `install`/`set_global_recorder` does not run it — without
    // a periodic sweep the histogram bookkeeping grows without bound. Must
    // be called from within a tokio runtime (every opencode2api bin does, from
    // async main).
    let upkeep = handle.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            upkeep.run_upkeep();
        }
    });
    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_histogram_renders_bucket_lines() {
        let recorder = build_prometheus_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            metrics::histogram!(names::DURATION).record(0.01);
            metrics::histogram!(names::STREAM_FIRST_TOKEN).record(0.01);
        });

        let rendered = handle.render();
        assert!(rendered.contains("# TYPE opencode2api_request_duration_seconds histogram"));
        assert!(rendered.contains("# TYPE opencode2api_stream_first_token_seconds histogram"));
        assert!(rendered.lines().any(|line| {
            line.starts_with("opencode2api_request_duration_seconds_bucket{")
                && line.contains("le=\"0.05\"")
        }));
        assert!(rendered.lines().any(|line| {
            line.starts_with("opencode2api_stream_first_token_seconds_bucket{")
                && line.contains("le=\"0.05\"")
        }));
    }

    #[test]
    fn timezone_resolution_covers_both_forms() {
        assert!(resolve_timezone("Asia/Jakarta").is_some());
        assert!(resolve_timezone("+07:00").is_some());
        assert!(resolve_timezone("Mars/Olympus").is_none());
    }

    #[test]
    fn offset_forms_all_parse_with_correct_sign() {
        let o = |secs: i32| jiff::tz::Offset::from_seconds(secs).unwrap();
        assert_eq!(parse_offset("Z").unwrap(), o(0));
        assert_eq!(parse_offset("+07").unwrap(), o(7 * 3600));
        assert_eq!(parse_offset("-05:30").unwrap(), o(-19_800));
        assert_eq!(parse_offset("+0700").unwrap(), o(7 * 3600));
        assert!(parse_offset("+25").is_none());
        assert!(parse_offset("07:00").is_none());
        assert!(parse_offset("").is_none());
    }

    fn temp_scratch(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "opencode2api-telemetry-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn utc_today() -> String {
        jiff::Timestamp::now()
            .to_zoned(jiff::tz::TimeZone::UTC)
            .strftime("%Y-%m-%d")
            .to_string()
    }

    #[test]
    fn daily_appender_names_the_period_stamped_file_and_writes_it() {
        use std::io::Write;
        let dir = temp_scratch("naming");
        let cfg = ServerConfig {
            log_keep_files: None,
            ..Default::default()
        };
        let mut appender = file_appender(&cfg, &dir.display().to_string(), "x2log_name").unwrap();
        appender.write_all(b"hello\n").unwrap();
        drop(appender);
        let written = dir.join(format!("x2log_name.{}.log", utc_today()));
        assert_eq!(std::fs::read_to_string(&written).unwrap(), "hello\n");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `never` + the DEFAULT cap must boot and leave exactly one unstamped
    /// file: the cap is ignored, not a boot failure nobody asked for.
    #[test]
    fn never_rotation_ignores_the_cap_and_leaves_one_unstamped_file() {
        use std::io::Write;
        let dir = temp_scratch("never");
        let cfg = ServerConfig {
            log_rotate: LogRotate::Never,
            ..Default::default()
        };
        let mut appender = file_appender(&cfg, &dir.display().to_string(), "x2log_never").unwrap();
        appender.write_all(b"line\n").unwrap();
        drop(appender);
        assert!(dir.join("x2log_never.log").exists());
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The boot prune is what bounds the directory without an external
    /// rotator: it culls the stalest seed before the live file is opened,
    /// and NEVER reaches the current period's file when keep >= 2.
    #[test]
    fn boot_prune_olds_out_stale_files_not_the_live_one() {
        let dir = temp_scratch("prune");
        let seed = |name: &str| {
            std::fs::write(dir.join(name), b"stale").unwrap();
            // prune sorts by fs birthtime where the platform reports it;
            // coarse (1 s) resolution would tie two same-second creations
            // and break the ordering determinism the assertion needs
            std::thread::sleep(std::time::Duration::from_millis(1100));
        };
        seed("x2log_prune.2020-01-01.log");
        seed("x2log_prune.2020-01-02.log");
        let cfg = ServerConfig {
            log_keep_files: Some(2),
            ..Default::default()
        };
        let _appender = file_appender(&cfg, &dir.display().to_string(), "x2log_prune").unwrap();
        assert!(!dir.join("x2log_prune.2020-01-01.log").exists());
        assert!(dir.join("x2log_prune.2020-01-02.log").exists());
        assert!(
            dir.join(format!("x2log_prune.{}.log", utc_today()))
                .exists()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// What the PRODUCTION layer configuration (json, default span list, an
    /// `info` filter) actually writes for the events `body` emits.
    fn capture_json(body: impl FnOnce()) -> String {
        let dir = temp_scratch("json");
        let appender = tracing_appender::rolling::never(&dir, "out.log");
        let (writer, guard) = tracing_appender::non_blocking(appender);
        // The very layer `init` installs, so these tests pin this project's
        // configuration and not tracing's defaults.
        let layer = capture_json_layer(writer);
        let sub = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("info"))
            .with(layer);
        tracing::subscriber::with_default(sub, body);
        // Dropping the guard is what flushes the worker; reading before that
        // races the line into an empty file.
        drop(guard);
        std::fs::read_to_string(dir.join("out.log")).unwrap_or_default()
    }

    /// The correlation spine is the REQUEST SPAN, not a field threaded through
    /// every call site: the provider's upstream-error line, the retry warning,
    /// and the relay's terminal line all sit inside it and are attributed for
    /// free — but only while the span survives the filter, which is why
    /// `router.rs` builds it at `info`, not `debug`.
    #[test]
    fn an_info_span_attributes_every_json_line_and_a_debug_span_does_not() {
        let attributed = capture_json(|| {
            let span =
                tracing::info_span!("request", request_id = 7u64, path = "/v1/chat/completions");
            let _entered = span.enter();
            tracing::warn!("upstream error");
        });
        assert!(
            attributed.contains("\"span\"") && attributed.contains("\"request_id\":7"),
            "the json layer stopped copying enclosing span fields: {attributed}"
        );

        // The storage half of the same decision: `"span"` carries the context,
        // and `"spans"` would be that identical single span written a second
        // time on every line of every request.
        assert!(
            !attributed.contains("\"spans\""),
            "the duplicate span list is back on every json line: {attributed}"
        );

        let unattributed = capture_json(|| {
            let span = tracing::debug_span!("request", request_id = 7u64);
            let _entered = span.enter();
            tracing::warn!("upstream error");
        });
        assert!(
            !unattributed.contains("request_id"),
            "a debug-level span was attributed at info level — this test exists \
             to prove the request span cannot be demoted: {unattributed}"
        );
    }

    /// `log_level` has to move the LIBRARY targets, because that is where every
    /// line this proxy writes actually comes from. The old spelling
    /// `{service_name}={},info` named the BIN's crate (always `service`) over an
    /// unconditional `info` base, so the knob could neither quiet the
    /// per-request record at `warn` nor reveal a library `debug` — inert in both
    /// directions, which is the same class of bug as a `.env` line that silently
    /// stops loading.
    ///
    /// `base_filter` is asserted directly, not `default_filter`: the latter's
    /// `RUST_LOG` branch reads process state, and neutralising it would mean
    /// deleting a variable this test does not own while every other test in the
    /// binary runs. That `RUST_LOG` beats the config value is EnvFilter's own
    /// documented precedence, not this crate's logic, so it goes untested here.
    #[test]
    fn log_level_gates_library_targets_in_both_directions() {
        let rendered = |level: &str, event: fn()| -> String {
            let dir = temp_scratch(&format!("level-{level}"));
            let appender = tracing_appender::rolling::never(&dir, "out.log");
            let (writer, guard) = tracing_appender::non_blocking(appender);
            let sub = tracing_subscriber::registry()
                .with(base_filter(level).expect("the level under test is valid"))
                .with(tracing_subscriber::fmt::layer().with_writer(writer));
            tracing::subscriber::with_default(sub, event);
            drop(guard);
            std::fs::read_to_string(dir.join("out.log")).unwrap_or_default()
        };
        let info = || {
            tracing::info!(target: "opencode2api_server::relay", "the per-request line");
        };
        let debug = || {
            tracing::debug!(target: "opencode2api_kit::telemetry", "library detail");
        };
        assert!(
            rendered("info", info).contains("the per-request line"),
            "info still emits its own level"
        );
        assert!(
            !rendered("warn", info).contains("the per-request line"),
            "`warn` bought nothing: the knob must quiet the per-request line"
        );
        assert!(
            !rendered("info", debug).contains("library detail"),
            "info must not emit debug"
        );
        assert!(
            rendered("debug", debug).contains("library detail"),
            "`debug` must reveal a LIBRARY target, not just the bin's"
        );
    }

    /// A typo in `server.log_level` is a boot error naming the field.
    /// `EnvFilter::new` — the other constructor — PANICS on the same input, so
    /// using it here would abort the process inside `init`, before a single log
    /// line exists, over a string nobody can see in a stack trace.
    #[test]
    fn an_unusable_level_is_an_error_and_never_a_panic() {
        // `""` is first because it is the SNEAKY one: `LevelFilter::from_str("")`
        // succeeds (it maps to OFF), so without the explicit blank check an empty
        // `server.log_level` — a dropped value, an unfilled template — would boot
        // clean and log nothing. `"off"` stays legal because turning logging off
        // is a decision; a blank is an accident. The rest are the ordinary typo,
        // which `LevelFilter` rejects and `EnvFilter::new` would have panicked on.
        for garbage in ["", "infoo", "info,,", "10"] {
            let err = base_filter(garbage)
                .err()
                .unwrap_or_else(|| panic!("{garbage:?} was accepted as a level"));
            let msg = err.to_string();
            assert!(
                msg.contains("server.log_level"),
                "the error must name the field: {msg}"
            );
        }
    }

    /// `RUST_LOG` keeps the library's DIRECTIVE grammar, and its errors are
    /// reported rather than dropped on the floor for the config value.
    ///
    /// Both halves matter, and they are the two directions of one mistake: a
    /// routine `RUST_LOG=info,opencode2api_kit=debug` must BOOT (running it through the
    /// level parser instead would refuse to start the proxy over a valid value),
    /// while a value the parser truly cannot read must name the variable instead
    /// of quietly deferring to `server.log_level` — the operator who set it is
    /// looking at the log they think they asked for.
    #[test]
    fn rust_log_keeps_directive_syntax_and_reports_what_it_cannot_parse() {
        let _guard = crate::testenv::lock_env();
        let _restore = crate::testenv::RestoreEnv::clear(&["RUST_LOG"]);
        let with = |value: &str| {
            unsafe { std::env::set_var("RUST_LOG", value) };
            default_filter(&ServerConfig::default())
        };

        for usable in [
            "info,opencode2api_kit=debug",
            "trace",
            "warn,[request]=info",
        ] {
            with(usable).unwrap_or_else(|e| panic!("RUST_LOG {usable:?} must boot: {e}"));
        }
        match with("foo=[bad") {
            Ok(_) => panic!("an unparseable RUST_LOG was accepted and used"),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("RUST_LOG"),
                    "the error must name the lever, not the config field: {msg}"
                );
            }
        }
    }

    #[test]
    fn log_prefix_names_the_file_and_the_service_name_is_the_fallback() {
        use std::io::Write;
        let dir = temp_scratch("prefix");
        let plain = ServerConfig::default();
        let mut fallback =
            file_appender(&plain, &dir.display().to_string(), "svc_fallback").unwrap();
        fallback.write_all(b"one\n").unwrap();
        drop(fallback);

        let named = ServerConfig {
            log_prefix: Some("glm_prod".into()),
            ..Default::default()
        };
        let mut prefixed =
            file_appender(&named, &dir.display().to_string(), "svc_fallback").unwrap();
        prefixed.write_all(b"two\n").unwrap();
        drop(prefixed);

        let today = utc_today();
        assert!(dir.join(format!("svc_fallback.{today}.log")).exists());
        assert!(dir.join(format!("glm_prod.{today}.log")).exists());
        assert!(
            std::fs::read_to_string(dir.join(format!("glm_prod.{today}.log")))
                .unwrap()
                .contains("two")
        );

        // An empty configured prefix is refused, not honored: upstream it
        // collapses to no-prefix, which makes every file in the directory a
        // prune candidate.
        let empty = ServerConfig {
            log_prefix: Some(String::new()),
            ..Default::default()
        };
        assert!(file_appender(&empty, &dir.display().to_string(), "svc").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
