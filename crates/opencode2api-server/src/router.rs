//! Router assembly and the serving lifecycle.
//!
//! Middleware order, innermost first (each earns its place):
//! `RequestBodyLimitLayer` -> `DefaultBodyLimit` -> `gateway_failures` (render
//! and account only the two layers' own plain-text 413) -> `catch_panics` (a
//! panicking handler answers 500, never a dropped connection) -> `TraceLayer`
//! -> CORS -> request-id stamping. Panics inside response-body polling after a
//! handler returns remain the stream outcome owner's responsibility.

use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{DefaultBodyLimit, Request};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Uri, header};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post};
use axum::serve::ListenerExt;
use futures_util::FutureExt as _;
use tokio::net::TcpListener;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::{MakeSpan, TraceLayer};
use tracing::Span;

use crate::errors::{self, api_error};
use crate::relay;
use crate::routes;
use crate::{InboundFormat, Pipeline, ServerConfig};

const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

/// Provenance that the owning response site has already emitted its terminal
/// `Done`. Insert this only after that emission; never infer it from dispatch,
/// status, content type, or the fact that an error was rendered.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TerminalRecorded;

/// Attach terminal-accounting provenance to a response whose owning site has
/// already emitted `Done`.
pub(crate) fn mark_accounted(mut response: Response) -> Response {
    response.extensions_mut().insert(TerminalRecorded);
    response
}

/// Full application router: the four inbound-dialect routes plus ops, with the
/// middleware stack and 404/405 envelopes.
pub fn build_router(pipeline: Pipeline) -> Router {
    let cfg = pipeline.cfg.clone();
    let cors = build_cors(&cfg);

    // A dialect this deployment does not serve is not registered at all, so
    // it 404s through the same envelope as any unknown path — rather than
    // being registered and then refusing, which reads to a client as a
    // broken endpoint instead of an absent one.
    let mut router = Router::new();
    if cfg.serves(opencode2api_kit::DIALECT_CHAT) {
        router = router.route(routes::CHAT_PATH, post(routes::chat_completions));
    }
    if cfg.serves(opencode2api_kit::DIALECT_MESSAGES) {
        router = router.route(routes::MESSAGES_PATH, post(routes::messages));
    }
    if cfg.serves(opencode2api_kit::DIALECT_RESPONSES) {
        router = router.route(routes::RESPONSES_PATH, post(routes::responses));
    }
    if cfg.serves(opencode2api_kit::DIALECT_GENERATE_CONTENT) {
        // The model lives in the path (`models/{model}`) and the verb rides on
        // the last segment after `:` (`{model}:generateContent` /
        // `:streamGenerateContent`); one param captures the whole tail and the
        // handler splits it. `/v1beta` is the stable surface (v1alpha also
        // exists upstream; only v1beta is served here). The same pattern also
        // answers GET, which is this dialect's discovery surface — a Gemini SDK
        // that cannot list models cannot select one, so serving the verb without
        // the listing leaves the dialect half-reachable.
        router = router
            .route("/v1beta/models", get(routes::list_models))
            .route(
                "/v1beta/models/{*tail}",
                post(routes::generate_content).get(routes::list_models),
            );
    }

    router
        .route("/v1/models", get(routes::models))
        .route("/health", get(routes::health))
        .route("/ready", get(routes::ready))
        .route("/metrics", get(routes::metrics_route))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        // WHY: the two limit layers are the sole pre-handler rejection path.
        // `gateway_failures` normalizes only responses without the explicit
        // `TerminalRecorded` extension put on sites that already own their Done.
        // Extractor failures remain unmarked and are therefore normalized.
        .layer(DefaultBodyLimit::max(cfg.max_body_bytes))
        .layer(RequestBodyLimitLayer::new(cfg.max_body_bytes))
        .layer(middleware::from_fn(gateway_failures))
        .layer(middleware::from_fn(catch_panics))
        .layer(TraceLayer::new_for_http().make_span_with(PathOnlyMakeSpan))
        .layer(cors)
        .layer(middleware::from_fn(stamp_request_id))
        .with_state(pipeline)
}

fn build_cors(cfg: &ServerConfig) -> CorsLayer {
    // Empty list = CorsLayer::new(), which allows no origin: browser callers
    // need an explicit opt-in in config. Origins are compared as HeaderValues,
    // not header names, so `AllowOrigin::list` (not a HeaderMap keyed by them).
    let cors = CorsLayer::new();
    if cfg.cors_allow_origin.is_empty() {
        return cors;
    }
    let origins: Vec<HeaderValue> = cfg
        .cors_allow_origin
        .iter()
        .filter_map(|origin| origin.parse::<HeaderValue>().ok())
        .collect();
    cors.allow_origin(AllowOrigin::list(origins))
        .allow_headers([
            header::CONTENT_TYPE,
            header::AUTHORIZATION,
            // Each dialect's own credential header, or a browser caller cannot
            // even preflight the request its SDK builds by default.
            HeaderName::from_static("x-api-key"),
            HeaderName::from_static("anthropic-version"),
            HeaderName::from_static("x-goog-api-key"),
        ])
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
}

/// The request span: TWO fields, and the reason each is here.
///
/// `path`, not `uri`. This server accepts a credential in the query string
/// (`?key=`, one of Google's two documented forms), so the query is sensitive
/// data and `DefaultMakeSpan`'s `uri = %request.uri()` — path AND query — would
/// put it on the span. That is not a json-layer concern either: BOTH layouts
/// publish the span context — json as a `"span"` object, the text layout as a
/// `request{…}: ` prefix standing where the target would be — a prefix a log
/// consumer has to cut before the URI itself — so an
/// unredacted URI would leak `?key=` into the SHIPPED default configuration, not
/// merely into one an operator opts into. Redacting at the span is what makes
/// the answer independent of whichever subscriber a deployment chooses; the
/// handler still reads the real query from typed parts, only the span forgets it.
///
/// `request_id`, DERIVED by the same `opencode2api_kit::util::request_id_of` the
/// middleware and handlers use, so the number in this span, in the response
/// header, in `CallContext` and in the retry seed cannot disagree.
///
/// Everything else that used to be here is gone, because a captured line measured
/// 456 bytes and 145 of them were the span: `request_id` and `model` repeated
/// from the event, `dialect` restating `format` under a second name, `path`
/// restating `endpoint`, and `method`/`version` that no one triages. The two that
/// survive answer the only question nothing else can on the panic path
/// (`CatchPanicLayer` reaches no handler, mints no record and reads no headers,
/// so the span is its whole context) — which is also why `path` is kept and the
/// correlation number is.
///
/// The `info` LEVEL is load-bearing in both directions. A span the filter
/// disables attributes nothing, so a `debug_span!` here would silently drop
/// correlation from every line the request produces; and `log_level: "warn"`
/// switches this span off on purpose, taking the span-borne id with it from every
/// layout. The per-request record carries its own copy of those fields precisely
/// so the answer survives that setting.
#[derive(Clone, Debug)]
struct PathOnlyMakeSpan;

impl<B> MakeSpan<B> for PathOnlyMakeSpan {
    fn make_span(&mut self, request: &Request<B>) -> Span {
        tracing::info_span!(
            "request",
            path = %request.uri().path(),
            request_id = opencode2api_kit::util::request_id_of(request.headers()),
        )
    }
}

async fn stamp_request_id(mut req: Request, next: Next) -> Response {
    // Derived from whatever the caller sent, so a client that already runs a
    // correlation spine sees its own id come back and can grep its own trace
    // through this proxy's logs; a fresh mint when it sent nothing, or sent
    // something this proxy does not key on (a UUID). The SAME function runs in
    // the request span, so the header and the logs cannot drift.
    let id = opencode2api_kit::util::request_id_of(req.headers());
    let value = id.to_string();
    if let Ok(v) = HeaderValue::from_str(&value) {
        req.headers_mut().insert(X_REQUEST_ID, v);
    }
    let mut resp = next.run(req).await;
    if !resp.headers().contains_key(&X_REQUEST_ID)
        && let Ok(v) = HeaderValue::from_str(&value)
    {
        resp.headers_mut().insert(X_REQUEST_ID, v);
    }
    resp
}

async fn not_found(headers: HeaderMap) -> Response {
    unmatched(404, "Not Found", &headers)
}

async fn method_not_allowed(headers: HeaderMap, uri: Uri) -> Response {
    let fmt = format_of_path(uri.path());
    let started = Instant::now();
    let rid = opencode2api_kit::util::request_id_of(&headers);
    relay::Done::answer(rid, "unmatched", fmt, 405, started).emit();
    mark_accounted(api_error(405, "Method Not Allowed", fmt))
}

// Responses are marked at their accounting sites, not blanket-marked at dispatch:
// axum's `Bytes` extractor rejection is produced by dispatch but has no Done yet.

/// Normalize only an unmarked 413, which proves the body-limit layers rejected
/// the request before route/fallback dispatch. Marked responses already own
/// their accounting, regardless of whether their vendor body is JSON or plain
/// text, so they pass through byte-for-byte.
async fn gateway_failures(req: Request, next: Next) -> Response {
    let started = Instant::now();
    let rid = opencode2api_kit::util::request_id_of(req.headers());
    let fmt = format_of_path(req.uri().path());
    let endpoint = endpoint_of_path(req.uri().path());
    let mut response = next.run(req).await;
    if response.status() == axum::http::StatusCode::PAYLOAD_TOO_LARGE
        && response.extensions().get::<TerminalRecorded>().is_none()
    {
        relay::Done::answer(rid, endpoint, fmt, 413, started).emit();
        response = api_error(413, "request body too large", fmt);
    }
    response
}

async fn catch_panics(req: Request, next: Next) -> Response {
    let started = Instant::now();
    let rid = opencode2api_kit::util::request_id_of(req.headers());
    let endpoint = endpoint_of_path(req.uri().path());
    let fmt = format_of_path(req.uri().path());
    match AssertUnwindSafe(next.run(req)).catch_unwind().await {
        Ok(response) => response,
        Err(err) => errors::panic_response(err, rid, endpoint, fmt, started),
    }
}

fn format_of_path(path: &str) -> InboundFormat {
    match path {
        routes::MESSAGES_PATH => InboundFormat::Anthropic,
        routes::RESPONSES_PATH => InboundFormat::Responses,
        path if path == "/v1beta/models" || path.starts_with("/v1beta/models/") => {
            InboundFormat::Gemini
        }
        // Chat, models, and genuinely unknown paths retain the chat envelope.
        _ => InboundFormat::OpenAi,
    }
}

/// The ONE endpoint decision for a path: handlers and the fallback call this,
/// so a URL cannot wear two metric labels depending on which layer recorded it.
pub(crate) fn endpoint_of_path(path: &str) -> &'static str {
    if path == routes::CHAT_PATH {
        "chat"
    } else if path == routes::MESSAGES_PATH {
        "messages"
    } else if path == routes::RESPONSES_PATH {
        "responses"
    } else if path == "/v1/models" || path == "/v1beta/models" {
        "models"
    } else if path.starts_with("/v1beta/models/") {
        if path
            .rsplit_once('/')
            .is_some_and(|(_, tail)| tail.contains(':'))
        {
            "generate-content"
        } else {
            "models"
        }
    } else {
        "unmatched"
    }
}

/// A route that does not exist, ACCOUNTED FOR.
///
/// This was the one client-visible refusal with no terminal record at all:
/// absent from `opencode2api_requests_total` and from the log, so a scanner probing
/// `/v1/messages` on a chat-only deployment — or a client pointed at a dialect
/// this deployment deliberately does not serve, which is exactly how the
/// dialect gate makes a surface 404 — left no trace and read as a broken
/// router. One `endpoint="unmatched"` series and one line per event is the
/// entire cost; the envelope stays the chat shape because a caller that
/// reached no route has not declared a dialect we can trust.
fn unmatched(status: u16, message: &str, headers: &HeaderMap) -> Response {
    let fmt = InboundFormat::OpenAi;
    let started = Instant::now();
    let rid = opencode2api_kit::util::request_id_of(headers);
    relay::Done::answer(rid, "unmatched", fmt, status, started).emit();
    mark_accounted(api_error(status, message, fmt))
}

/// Bind, serve, and drain. Call from the bin's `main`: wires OS signals
/// into the shutdown watch and delegates to `serve`.
pub async fn run(pipeline: Pipeline) -> anyhow::Result<()> {
    let cfg = pipeline.cfg.clone();
    let addrs = cfg.socket_addrs()?;
    // Bind ALL before serving: a dead extra bind must fail boot, never
    // leave a half-started process answering on one interface while
    // operators debug the other.
    let mut listeners = Vec::with_capacity(addrs.len());
    for addr in &addrs {
        listeners.push((
            *addr,
            TcpListener::bind(addr)
                .await
                .map_err(|e| anyhow::anyhow!("binding {addr}: {e}"))?,
        ));
    }
    let service = pipeline.provider.name().to_string();
    let router = build_router(pipeline);
    // One record that answers "what is this process actually configured to do"
    // before a single request arrives — the line an operator pastes into an
    // issue. It is deliberately credential-free: a proxy URL is a password,
    // and this boot line is the one record guaranteed to reach wherever logs
    // are collected, so the upstream's URL, the client key, and any vendor
    // session string are reported as PRESENCE, never as values.
    tracing::info!(
        service = service.as_str(),
        version = env!("CARGO_PKG_VERSION"),
        pid = std::process::id(),
        dialects = if cfg.dialects.is_empty() {
            "all".into()
        } else {
            cfg.dialects.join(",")
        },
        binds = ?addrs,
        auth = if cfg.client_api_key.is_some() {
            "configured"
        } else {
            "open"
        },
        max_inflight = cfg.max_inflight,
        admission_wait_secs = cfg.admission_wait_secs,
        max_body_bytes = cfg.max_body_bytes,
        request_timeout_secs = cfg.request_timeout_secs,
        stream_deadline_secs = cfg.stream_deadline_secs,
        sse_keepalive_secs = cfg.sse_keepalive_secs,
        retry_attempts = cfg.retry.max_attempts,
        retry_base_ms = cfg.retry.base_ms,
        retry_cap_ms = cfg.retry.cap_ms,
        upstream_shards = cfg.upstream_shards,
        http2_prior_knowledge = cfg.http2_prior_knowledge,
        drain_secs = cfg.drain_secs,
        log_level = cfg.log_level.as_str(),
        log_json = cfg.log_json,
        log_dir = cfg.log_dir.as_deref().unwrap_or("-"),
        // `-` means "unset", i.e. the prefix is the BIN's own service name,
        // which the router does not have (telemetry::init received it). Naming
        // a guessed default here would let the boot line contradict the file
        // the proxy actually writes.
        log_prefix = cfg.log_prefix.as_deref().unwrap_or("-"),
        log_rotate = ?cfg.log_rotate,
        // `file_appender` IGNORES the cap under `never`, so printing the
        // configured number there would promise a bound that does not exist.
        log_keep_files = match (cfg.log_rotate, cfg.log_keep_files) {
            (opencode2api_kit::config::LogRotate::Never, Some(_)) => "ignored (never)".into(),
            (_, Some(n)) => n.to_string(),
            (_, None) => "unbounded".into(),
        },
        log_stdout = cfg.log_stdout,
        log_tz = cfg.log_tz.as_deref().unwrap_or("UTC"),
        "configuration"
    );
    for (addr, listener) in &listeners {
        tracing::info!(%addr, actual = %listener.local_addr()?, service, "opencode2api listening");
    }
    let (shutdown, _) = tokio::sync::watch::channel(false);
    let signal = shutdown.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = signal.send(true);
    });
    let drain = cfg.drain_secs;
    // One watch, N serves: the drain event reaches every listener in the
    // same tick (that is why the trigger is a watch, not a future — see
    // `serve`), and a failure on ANY socket is the process failure.
    let results = futures_util::future::join_all(listeners.into_iter().map(|(addr, listener)| {
        let shutdown = shutdown.clone();
        let router = router.clone();
        async move {
            serve(listener, router, drain, shutdown)
                .await
                .map_err(|e| anyhow::anyhow!("serving {addr}: {e}"))
        }
    }))
    .await;
    results.into_iter().collect()
}

/// Serve an already-bound listener until `shutdown` flips true: graceful
/// drain first, force at `drain_secs`. The accepted-socket Nagle opt-out
/// lives here too (see tap below), so `run` only binds. The trigger is a
/// watch, not a future, because TWO arms must observe the same event
/// (graceful hook + cap) — two independently registered ctrl_c waiters is
/// the classic way to await one signal in two places and get only one of
/// them. Tests flip the sender mid-stream; that is what makes the drain
/// path reachable at all (it is the one part of the server `oneshot`
/// cannot exercise).
pub async fn serve(
    listener: TcpListener,
    router: Router,
    drain_secs: u64,
    shutdown: tokio::sync::watch::Sender<bool>,
) -> anyhow::Result<()> {
    // axum::serve leaves Nagle on; SSE frames leave a Nagled socket one RTT
    // at a time — TTFT cost paid on every frame. This is the fix.
    let listener = listener.tap_io(|tcp_stream| {
        let _ = tcp_stream.set_nodelay(true);
    });
    let mut graceful_rx = shutdown.subscribe();
    let graceful = async move {
        while !*graceful_rx.borrow_and_update() {
            if graceful_rx.changed().await.is_err() {
                break; // sender dropped: treat as shutdown, not a hang
            }
        }
    };
    let mut force_rx = shutdown.subscribe();
    let force = async move {
        while !*force_rx.borrow_and_update() {
            if force_rx.changed().await.is_err() {
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(drain_secs)).await;
    };
    tokio::select! {
        res = axum::serve(listener, router).with_graceful_shutdown(graceful) => {
            res.map_err(anyhow::Error::from)
        }
        // Two-stage cap: with_graceful_shutdown alone waits unbounded on
        // open streams; past the cap we drop the server, forcing sockets
        // closed so a stuck client cannot pin the process.
        _ = force => {
            tracing::warn!(drain_secs, "drain cap reached; closing remaining connections");
            Ok(())
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
    tracing::info!("shutdown signal received; draining in-flight requests");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;

    /// The span the router builds IS the correlation spine, so the properties the
    /// rest of the design leans on are pinned here rather than at a call site: it
    /// survives the shipped filter (a disabled span has nothing to attribute —
    /// opencode2api-kit's `an_info_span_attributes_every_json_line_and_a_debug_span_does_not`
    /// pins the json behaviour that relies on it), and it carries the CALLER's
    /// id, derived by the same function the middleware and handlers use.
    ///
    /// It also guards the slimming in the direction a refactor would undo.
    /// `record` on a field the span never declared is a SILENT no-op in tracing,
    /// so without the assertion below someone could re-add the `model`, `dialect`,
    /// `status`, `method` and `version` crowd — 145 bytes of every line, measured
    /// — and no test would notice while the record's own copies stopped mattering.
    ///
    /// The path-only redaction rides the same assertion: BOTH layouts publish the
    /// span context, so a query credential reaching the span would ship to
    /// whatever aggregator a deployment points at, in the DEFAULT configuration.
    #[test]
    fn the_request_span_carries_id_and_path_and_nothing_else() {
        let lines = crate::testlog::capture(|| {
            let request = Request::builder()
                .method("POST")
                .uri("/v1/chat/completions?key=super-secret-canary")
                .header("x-request-id", "4242")
                .body(Body::empty())
                .unwrap();
            let span = PathOnlyMakeSpan.make_span(&request);
            assert!(!span.is_disabled(), "a disabled span attributes nothing");
            let _entered = span.enter();
            // Ignored silently, which is the point: these are the cut fields.
            span.record("model", "glm-4.6");
            span.record("dialect", "openai");
            span.record("status", 502u16);
            tracing::info!("a line the provider would write");
        });
        assert_eq!(lines.len(), 1);
        let span = &lines[0]["span"];
        assert_eq!(span["request_id"], 4242, "the caller's id, not a remint");
        assert_eq!(span["path"], "/v1/chat/completions");
        for gone in ["model", "dialect", "status", "method", "version"] {
            assert!(
                span.get(gone).is_none(),
                "`{gone}` is back on the span: the event's own copy is the one \
                 that must survive every level, and the span restated it: {span}"
            );
        }
        assert!(
            !lines[0].to_string().contains("super-secret-canary"),
            "a query credential reached the line: {}",
            lines[0]
        );
    }

    /// The ONE-endpoint-decision contract, as a table rather than a comment.
    /// One table, not two: `endpoint` and `format` are separate functions
    /// answering separate questions — the label a metric series and a dashboard
    /// group by, versus the parser and error-envelope grammar — so a path CAN
    /// legitimately pair `unmatched` with the chat envelope. Pinning the pair
    /// on one row is what makes that asymmetry a stated decision rather than
    /// something the next reader has to re-derive from two if-chains.
    ///
    /// Rows deliberately include inputs no route produces, because those are
    /// the ones a later edit would silently re-label. The sharp ones are the
    /// colons: the implementation reads the LAST path segment only
    /// (`rsplit_once('/')`), so a colon in an EARLIER segment must not promote
    /// a listing into `generate-content`, and a trailing slash after the verb
    /// leaves the last segment EMPTY — which is a listing, not a verb. Each of
    /// those rows kills a specific one-token mutation of that call.
    #[test]
    fn one_path_gets_exactly_one_endpoint_label_and_one_dialect() {
        let table: [(&str, InboundFormat, &str); 15] = [
            // The four dialect surfaces, each with its own label.
            (routes::CHAT_PATH, InboundFormat::OpenAi, "chat"),
            (routes::MESSAGES_PATH, InboundFormat::Anthropic, "messages"),
            (
                routes::RESPONSES_PATH,
                InboundFormat::Responses,
                "responses",
            ),
            (
                "/v1beta/models/gemini-2.5-pro:generateContent",
                InboundFormat::Gemini,
                "generate-content",
            ),
            // Model listings, all THREE spellings, one label: otherwise every
            // dashboard shows a series a third of the size it should.
            // `/v1/models` is chat-shaped while `/v1beta/models` is Gemini —
            // the two "models" spellings are the non-obvious half of this
            // table, since the label is shared and the grammar is not.
            ("/v1/models", InboundFormat::OpenAi, "models"),
            ("/v1beta/models", InboundFormat::Gemini, "models"),
            (
                "/v1beta/models/gemini-2.5-pro",
                InboundFormat::Gemini,
                "models",
            ),
            // Empty final segment: a listing with a trailing slash, under the
            // Gemini collection but NOT a verb call.
            ("/v1beta/models/", InboundFormat::Gemini, "models"),
            // A trailing slash AFTER the verb empties the last segment, so the
            // colon is no longer where `rsplit_once` can see it.
            (
                "/v1beta/models/gemini-2.5-pro:generateContent/",
                InboundFormat::Gemini,
                "models",
            ),
            // A colon in a NON-FINAL segment: the model name carries it and a
            // further segment follows. `split_once('/')` would read the tail
            // as `gemini-2.5-pro:generateContent` and mislabel this
            // `generate-content`.
            (
                "/v1beta/models/gemini:2.5/pro",
                InboundFormat::Gemini,
                "models",
            ),
            // ...and the same colon shape with a deeper path after it.
            ("/v1beta/models/x:y/z", InboundFormat::Gemini, "models"),
            // A colon outside the Gemini tree changes NOTHING: not the dialect
            // (a `contains(':')` promotion would claim Gemini here) and not
            // the label (an exact-match chain correctly leaves it unmatched).
            (
                "/v1/chat/completions:ping",
                InboundFormat::OpenAi,
                "unmatched",
            ),
            // Unmatched paths still speak the chat envelope: a caller that
            // reached no route has not declared a dialect we can trust.
            ("/v1/embeddings", InboundFormat::OpenAi, "unmatched"),
            ("/v1/chat/completions/", InboundFormat::OpenAi, "unmatched"),
            ("", InboundFormat::OpenAi, "unmatched"),
        ];
        for (path, format, endpoint) in table {
            assert_eq!(format_of_path(path), format, "dialect for {path:?}");
            assert_eq!(endpoint_of_path(path), endpoint, "endpoint for {path:?}");
        }
    }

    /// The Gemini verb rule: ANY colon in the final segment is the verb, not
    /// just `generateContent`. A whitelist spelling one method would relabel
    /// `:streamGenerateContent` — a real, shipped Gemini surface — as a
    /// listing, so the terminal record for a working streaming request would
    /// file itself under `models` forever.
    #[test]
    fn any_final_segment_colon_is_the_gemini_verb() {
        for verb in ["generateContent", "streamGenerateContent", "countTokens"] {
            let path = format!("/v1beta/models/gemini-2.5-pro:{verb}");
            assert_eq!(endpoint_of_path(&path), "generate-content", "{path}");
        }
    }
}
