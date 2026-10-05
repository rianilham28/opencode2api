//! THE seam. A new "X-to-2api" service implements `Provider` and nothing else
//! on the request path.
//!
//! Design contracts:
//! - Object-safe (`BoxStream` returns), so the router holds only
//!   `Arc<dyn Provider>`; the concrete service type appears exactly once, in
//!   the bin's `main`.
//! - `stream()`/`stream_relay()` must not hand back a stream until the
//!   attempt is *committable* (connect, status-gate, ideally a first-frame
//!   probe), and must fail *before* handoff for anything the retry loop
//!   should see: the retry unit ends at the handoff. Mid-stream failures
//!   surface as terminal `Err` items, which the dialect's bridge renders as
//!   its truncation frame.
//! - Providers return IR (`ChatChunk` with `raw` set) or verbatim chat-
//!   shape bytes, never HTTP responses; byte-level framing and client
//!   envelopes are the server's job.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use http::{HeaderMap, HeaderValue};
use serde_json::Value;

use crate::errors::ProviderError;
use crate::types::{ChatChunk, ChatRequest, Completion};
use crate::wire::ChunkStream;

/// A client dialect a provider's UPSTREAM speaks natively — the
/// fidelity-lane key. When the inbound dialect is in `native_dialects()`,
/// the server relays request and response BYTES between client and vendor;
/// nothing folds through the IR, so shapes the IR cannot carry (tool blocks,
/// images, thinking) survive intact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Chat,
    Anthropic,
    Responses,
    Gemini,
}

/// What the vendor sent back on the fidelity lane: status + content-type +
/// body, exactly as received.
pub struct RawReply {
    pub status: u16,
    pub content_type: Option<HeaderValue>,
    /// Vendor headers worth handing to the client — an ALLOWLIST (see
    /// `relayable_headers`), never the whole map.
    pub headers: HeaderMap,
    pub frames: RawFrames,
}

/// The vendor response headers a client genuinely needs, filtered from
/// everything else.
///
/// Why an allowlist and not a copy: this proxy re-frames the body, so any
/// header describing the ORIGINAL framing — `content-length`,
/// `transfer-encoding`, `content-encoding` — would describe bytes the client
/// is not receiving, and hop-by-hop headers (`connection`, `keep-alive`)
/// belong to a connection that ended at us. Forwarding those corrupts the
/// response; forwarding nothing costs the client its backoff.
///
/// What survives, and why each one earns it:
/// - `retry-after` — a 429 without it turns a vendor's precise backoff into
///   the client's guess. This is the header the lane was losing.
/// - `x-ratelimit-*` / `anthropic-ratelimit-*` — quota state clients pace on.
/// - `x-request-id` / `request-id` / `anthropic-request-id` — the id a
///   support ticket is opened with; useless if it stops at the proxy. The
///   fidelity lane keeps the vendor id; folded errors remove these at render.
/// - `x-should-retry` — Anthropic's SDKs read it directly.
/// - `openai-processing-ms` — OpenAI SDKs use it to report server latency.
///
/// New entries require a demonstrated client need; a vendor's header namespace
/// is not itself permission to relay account-scoped metadata.
pub fn relayable_headers(src: &HeaderMap) -> HeaderMap {
    const EXACT: &[&str] = &[
        "retry-after",
        "x-request-id",
        "request-id",
        "anthropic-request-id",
        "x-should-retry",
        "openai-processing-ms",
    ];
    const PREFIXES: &[&str] = &["x-ratelimit-", "anthropic-ratelimit-"];
    let mut out = HeaderMap::new();
    for (name, value) in src {
        let n = name.as_str();
        if EXACT.contains(&n) || PREFIXES.iter().any(|p| n.starts_with(p)) {
            out.insert(name.clone(), value.clone());
        }
    }
    out
}

/// Default buffered-reply ceiling: generous enough that no sane API reply
/// reaches it, finite enough that an upstream cannot make the proxy
/// allocate without bound. Overridable per service via
/// `ServiceConfig::max_response_bytes`.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

impl RawReply {
    /// Wrap a vendor response for `relay_raw`: status and content-type pass
    /// through; an SSE body streams, anything else is read whole UNDER
    /// `max_bytes`. The buffered case is the one point where the lane
    /// cannot be purely byte-transparent: the proxy must hold the body to
    /// hand it over, so it must refuse to hold an endless one. One less
    /// thing every native provider re-derives (and gets subtly wrong, e.g.
    /// by trusting the status instead of the content-type for the fork).
    pub async fn from_upstream(
        resp: crate::wire::UpstreamResponse,
        max_bytes: usize,
    ) -> Result<Self, ProviderError> {
        let status = resp.status().as_u16();
        let resp_headers = &resp.headers().clone();
        let content_type = resp
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| HeaderValue::from_str(v.to_str().unwrap_or("")).ok())
            .or_else(|| {
                resp.is_sse()
                    .then(|| HeaderValue::from_static("text/event-stream"))
            });
        let sse = resp.is_sse();
        let frames = if sse {
            RawFrames::Sse(resp.into_chunks())
        } else {
            RawFrames::Buffered(resp.body_bytes_capped(max_bytes).await?)
        };
        Ok(Self {
            status,
            content_type,
            headers: relayable_headers(resp_headers),
            frames,
        })
    }
}

/// Buffered vs streamed vendor reply, raw bytes either way.
pub enum RawFrames {
    Buffered(Bytes),
    Sse(ChunkStream),
}

/// A live IR-chunk stream from a provider. Boxed for object safety; error
/// items are terminal (the server converts the first one into a truncation
/// frame / error event).
pub type ChatStream = BoxStream<'static, Result<ChatChunk, ProviderError>>;

/// Per-request context handed to a provider: the request id for vendor
/// correlation headers, and the client's request headers for credential
/// passthrough (the provider decides whether to use `authorization` or its
/// own configured secret — a template rule, both patterns are legitimate).
pub struct CallContext<'a> {
    pub request_id: u64,
    pub client_headers: Option<&'a HeaderMap>,
    /// The path the request arrived on, verbatim (`/v1beta/models/gemini-pro:streamGenerateContent`).
    ///
    /// Only the fidelity lane needs it, and only because some dialects put
    /// request state in the URL rather than the body: Gemini's model AND its
    /// stream flag live in the path, so a provider relaying that dialect
    /// byte-for-byte CANNOT rebuild the vendor URL from the body alone. Fold
    /// paths get `None` — the server parses the path there and hands the model
    /// down through the IR, which is where a fold is allowed to learn it.
    pub request_path: Option<&'a str>,
}

impl<'a> CallContext<'a> {
    pub fn new(request_id: u64, client_headers: Option<&'a HeaderMap>) -> Self {
        Self {
            request_id,
            client_headers,
            request_path: None,
        }
    }

    /// The lane's constructor: same context, plus the inbound path.
    pub fn for_lane(request_id: u64, headers: Option<&'a HeaderMap>, path: &'a str) -> Self {
        Self {
            request_id,
            client_headers: headers,
            request_path: Some(path),
        }
    }

    /// The raw `authorization` header value from the inbound request, if any.
    pub fn client_authorization(&self) -> Option<&str> {
        self.client_headers
            .and_then(|h| h.get(http::header::AUTHORIZATION))
            .and_then(|v| v.to_str().ok())
    }
}

#[async_trait]
pub trait Provider: Send + Sync + 'static {
    /// Stable operator-configured label for metrics and log dimensions.
    fn name(&self) -> &str;

    /// Buffered completion. Implementations: build the vendor body from
    /// `req.to_value()` (or `req` fields), send, `gate_response`, parse into
    /// `Completion` (set `raw` when the vendor speaks OpenAI).
    async fn complete(
        &self,
        req: &ChatRequest,
        ctx: &CallContext<'_>,
    ) -> Result<Completion, ProviderError>;

    /// Streamed completion, IR chunks. The stream must fail *before*
    /// handoff for anything the retry loop should see; mid-stream failures
    /// surface as terminal `Err` items.
    async fn stream(
        &self,
        req: &ChatRequest,
        ctx: &CallContext<'_>,
    ) -> Result<ChatStream, ProviderError>;

    /// Verbatim relay path: a stream of upstream bytes that ALREADY match
    /// the OpenAI chat-completions SSE the client asked for. The server uses
    /// this ONLY for the chat dialect; bridge dialects fall back to
    /// `stream()` because they must read payloads to translate them.
    /// The guarantee is PAYLOAD bytes, not record framing: multi-line data
    /// records are joined, `event:`/`id:` fields drop, CRLF normalizes.
    ///
    /// Contract differences from `stream()` the server depends on:
    /// - Nothing is parsed, so `finish_reason`/usage are invisible here.
    ///   Clean end is `data: [DONE]`; a clean EOF **without** it gets a
    ///   synthesized `[DONE]` (all major clients accept EOF), but a partial
    ///   trailing record means truncation and gets none.
    /// - Terminal accounting comes from the transport, not payloads.
    /// - Frames after `[DONE]` (vendor bookkeeping, e.g. a trailing `cost`
    ///   record) must never reach the client.
    ///
    /// Default reports unsupported (501), so vendors needing per-frame
    /// translation implement nothing extra; the server then calls `stream()`.
    /// An `Err` returned *after* an upstream round-trip means the request
    /// was already sent — implementations should prefer Ok-with-reencode
    /// over late-Err fallback (double billing on paid vendors).
    async fn stream_relay(
        &self,
        _req: &ChatRequest,
        _ctx: &CallContext<'_>,
    ) -> Result<ChunkStream, ProviderError> {
        Err(ProviderError::unsupported("raw stream relay"))
    }
    /// `/v1/models` payload (OpenAI shape). The context makes catalogue fetches
    /// attributable to the inbound request for provider correlation. Default:
    /// typed 501.
    async fn models(&self, _ctx: &CallContext<'_>) -> Result<Value, ProviderError> {
        Err(ProviderError::unsupported("models"))
    }

    /// Can this provider serve right now? A load balancer asks `/ready`, and
    /// the honest answer depends on egress: a proxy whose every lane is dead
    /// answers 503 to real traffic, so it must not answer 200 here.
    ///
    /// Default `true` for providers with no such state. OBSERVATIONAL ONLY —
    /// a readiness probe runs every few seconds, so this must never mint a
    /// lane, open a connection, or send anything upstream.
    fn ready(&self) -> bool {
        true
    }

    /// Dialects the upstream speaks natively; the server only calls
    /// `relay_raw` for a client request whose dialect appears here.
    /// Default: none — every client dialect folds through the IR, which is
    /// the right answer whenever the upstream is OpenAI-dialect-only.
    /// `Dialect::Chat` caveat: declaring it opts the chat stream out of the
    /// server's sentinel enforcement (no synthesized `[DONE]`, none
    /// withheld, no post-`[DONE]` suppression — first bullet of
    /// `relay_raw`'s contract). `stream_relay` is the protected fast path
    /// for OpenAI-shape streams; declare Chat only for an upstream whose
    /// streams you trust to terminate themselves.
    fn native_dialects(&self) -> &'static [Dialect] {
        &[]
    }

    /// FIDELITY LANE: forward the client's dialect-native body verbatim to
    /// the vendor and return its reply verbatim. Contract, deliberately
    /// stricter than `stream_relay`'s — this lane is defined by NOT
    /// touching the stream:
    /// - the server injects no keepalive comments and no synthesized or
    ///   withheld terminators: what the vendor sent is what the client sees
    ///   (a vendor `[DONE]`/`message_stop` is the stream's end; a silent
    ///   EOF is the vendor's truncation, faithfully relayed);
    /// - non-2xx replies come back as `Ok(RawReply)` — the vendor's own
    ///   dialect-shaped error envelope is the client's answer; `Err` is
    ///   reserved for proxy-side failures (auth, connect) before send.
    ///   This is the ONE deliberate exception to the fold path's
    ///   derived-error hygiene: a vendor's 502 HTML page reaches the
    ///   client verbatim, traded for dialect fidelity (the fold collapses
    ///   those bodies because vendor 5xx internals leak). Two 401s exist
    ///   by design at one URL: our auth-gate 401 renders in the client's
    ///   dialect shape; a configured vendor 401 passes through in theirs.
    /// - single attempt: no retry wrapper on this lane (re-sending a
    ///   vendor-committed stream is the bug this lane must never have);
    /// - model-alias or quota rewrites that need the IR are the fold
    ///   lane's job — a provider that must rewrite native bodies does so
    ///   inside its own `relay_raw` implementation, in code;
    /// - `ctx.request_path` is THIS request's inbound path, and it is the
    ///   lane's only channel for state a dialect keeps in the URL. Gemini
    ///   carries the model and the stream verb there, so a provider declaring
    ///   `[Dialect::Gemini]` native cannot build the vendor's
    ///   `models/{model}:{verb}` endpoint from the body at all. Reading the
    ///   path is not license to rewrite it: the bytes still pass untouched, and
    ///   the path is here so the REQUEST can be aimed, not edited.
    ///
    /// A DeepSeek `/anthropic` or GLM coding-plan provider declares
    /// `[Dialect::Anthropic]` and Claude-style clients keep their tool
    /// calls end to end. Buffered replies must come from
    /// `RawReply::from_upstream(resp, ceiling)` — the one place the lane
    /// cannot be byte-transparent (the proxy holds the body to hand it
    /// over), so the ceiling is enforced there, never after the fact.
    async fn relay_raw(
        &self,
        _dialect: Dialect,
        _client_body: Bytes,
        _ctx: &CallContext<'_>,
    ) -> Result<RawReply, ProviderError> {
        Err(ProviderError::unsupported("native fidelity relay"))
    }
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderValue};

    use super::relayable_headers;

    #[test]
    fn relayable_headers_keeps_client_headers_and_rejects_unrecognized_metadata() {
        let kept = [
            ("retry-after", "120"),
            ("x-ratelimit-remaining", "42"),
            ("anthropic-ratelimit-requests", "17"),
            ("x-request-id", "req_x"),
            ("request-id", "req_generic"),
            ("anthropic-request-id", "req_anthropic"),
            ("x-should-retry", "true"),
            ("openai-processing-ms", "314"),
        ];
        let dropped = [
            ("content-length", "1234"),
            ("transfer-encoding", "chunked"),
            ("content-encoding", "gzip"),
            ("connection", "close"),
            ("keep-alive", "timeout=5"),
            ("set-cookie", "session=secret"),
            ("openai-detected-account", "private-account"),
            ("openai-unrecognized", "not-allowlisted"),
        ];
        let mut source = HeaderMap::new();
        for (name, value) in kept.into_iter().chain(dropped) {
            source.insert(name, HeaderValue::from_static(value));
        }

        let relayable = relayable_headers(&source);

        for (name, value) in kept {
            assert_eq!(
                relayable.get(name).map(HeaderValue::as_bytes),
                Some(value.as_bytes()),
                "{name} must be relayed with its original value"
            );
        }
        for (name, _) in dropped {
            assert!(!relayable.contains_key(name), "{name} must not be relayed");
        }
    }
}
