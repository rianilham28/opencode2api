//! End-to-end pipeline tests over the REAL router (`build_router`), driven
//! by `tower::ServiceExt::oneshot` with a scripted provider instead of an
//! upstream socket (socket-level upstream behavior is covered by the
//! service service tests).
//!
//! These defend the template's contracts: dialect envelopes, retry
//! semantics on `is_retryable`, verbatim `raw` passthrough, in-stream
//! truncation without [DONE], Anthropic event ordering, and the capability /
//! error renderers in both formats.

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode};
use futures_util::{StreamExt, stream};
use opencode2api_kit::config::RetryConfig;
use opencode2api_kit::{
    CallContext, ChatChunk, ChatRequest, ChatStream, Completion, Dialect, Provider, ProviderError,
    RawFrames, RawReply, Usage,
};
use opencode2api_server::{Pipeline, ServerConfig};
use tower::ServiceExt;

// ── harness ──────────────────────────────────────────────────────────────

/// What one `stream()` handoff does.
enum StreamPlan {
    /// Fail before handoff: the retry loop may still re-send (the retry
    /// unit ends at handoff).
    PreHandoff(ProviderError),
    /// Hand off a stream that yields these items (an `Err` here is an
    /// in-stream failure — terminal, no retry possible).
    Chunks(Vec<Result<ChatChunk, ProviderError>>),
    /// Panic after the provider has been entered.
    Panic,
}

/// Scripted provider: per-attempt outcomes from queues, attempts counted.
struct Scripted {
    completions: Mutex<VecDeque<Result<Completion, ProviderError>>>,
    streams: Mutex<VecDeque<StreamPlan>>,
    /// Verbatim relay scripts (frames as the wire sent them). Empty queue =
    /// `stream_relay` reports its 501 default, so the server falls back to
    /// the transcoded `stream()` — which is how every non-relay test runs.
    relays: Mutex<VecDeque<Vec<Result<bytes::Bytes, String>>>>,
    /// Fidelity-lane scripts; `native` makes the provider claim every
    /// dialect, so the lane must be taken before any fold.
    raws: Mutex<VecDeque<Result<RawReply, ProviderError>>>,
    native: bool,
    /// A relay handoff whose frames arrive over time (the live-upstream
    /// shape): taken once, ahead of the canned `relays` queue.
    relay_live: Mutex<Option<opencode2api_kit::ChunkStream>>,
    /// The same for the transcoded path: IR chunks that arrive over time.
    stream_live: Mutex<Option<ChatStream>>,
    /// What the fidelity lane last saw as `ctx.request_path`. A dialect whose
    /// model lives in the URL is only servable natively if the lane carries
    /// that URL, so this is the one place that can observe it.
    lane_path: Mutex<Option<String>>,
    /// `Provider::models()` answers; an empty queue keeps the trait's 501 default.
    models: Mutex<VecDeque<Result<serde_json::Value, ProviderError>>>,
    /// Count of `Provider::models()` calls, independent of chat provider attempts.
    models_attempts: AtomicUsize,
    /// Milliseconds each `models()` call spends before returning.
    models_delay_ms: AtomicU64,
    /// Milliseconds added to the second and later `models()` calls. Used to
    /// let a retry enter the deadline while keeping the first attempt fast.
    models_delay_after_first_ms: AtomicU64,
    /// `Provider::models()` request id supplied by the route context.
    models_request_id: AtomicU64,
    /// Make `Provider::models()` panic after recording its request id.
    panic_models: bool,
    attempts: AtomicUsize,
    /// Milliseconds each `complete` spends before answering. The only way to
    /// make the ADMISSION path observable in a unit test — without it every
    /// request gets a slot instantly and `queued_ms` is a field that can never
    /// be seen non-zero, which is how an instrument ships dead.
    hold_ms: AtomicUsize,
    /// Milliseconds each `complete()` call after the first spends before
    /// answering; unlike `hold_ms`, this does not delay the initial attempt.
    hold_after_first_ms: AtomicUsize,
    /// Milliseconds each `stream()` call spends before handing off its stream.
    /// This models a provider that is slow to establish the upstream stream,
    /// so the route's pre-handoff deadline must still cover the call.
    stream_delay_ms: AtomicU64,
    /// Milliseconds each `stream()` call spends when its plan queue is empty.
    /// This lets a retry block while the initial scripted error stays fast.
    stream_plan_delay_ms: AtomicU64,
    /// Milliseconds each served `relay_raw` call spends before returning.
    /// This models a provider accepting the request but not sending headers.
    relay_delay_ms: AtomicU64,
    panic_completion: AtomicUsize,
}

impl Scripted {
    fn new() -> Self {
        Self {
            completions: Mutex::new(VecDeque::new()),
            streams: Mutex::new(VecDeque::new()),
            relays: Mutex::new(VecDeque::new()),
            raws: Mutex::new(VecDeque::new()),
            native: false,
            relay_live: Mutex::new(None),
            stream_live: Mutex::new(None),
            lane_path: Mutex::new(None),
            models: Mutex::new(VecDeque::new()),
            attempts: AtomicUsize::new(0),
            models_attempts: AtomicUsize::new(0),
            models_delay_ms: AtomicU64::new(0),
            models_request_id: AtomicU64::new(0),
            models_delay_after_first_ms: AtomicU64::new(0),
            hold_ms: AtomicUsize::new(0),
            panic_completion: AtomicUsize::new(0),
            stream_delay_ms: AtomicU64::new(0),
            relay_delay_ms: AtomicU64::new(0),
            panic_models: false,
            hold_after_first_ms: AtomicUsize::new(0),
            stream_plan_delay_ms: AtomicU64::new(0),
        }
    }
    fn native(mut self) -> Self {
        self.native = true;
        self
    }
    fn with_raws(self, queue: Vec<Result<RawReply, ProviderError>>) -> Self {
        *self.raws.lock() = queue.into();
        self
    }
    fn with_live_stream(self, stream: ChatStream) -> Self {
        *self.stream_live.lock() = Some(stream);
        self
    }
    fn with_relay_stream(self, stream: opencode2api_kit::ChunkStream) -> Self {
        *self.relay_live.lock() = Some(stream);
        self
    }
    fn with_relays(self, queue: Vec<Vec<Result<bytes::Bytes, String>>>) -> Self {
        *self.relays.lock() = queue.into();
        self
    }
    fn with_completions(self, queue: Vec<Result<Completion, ProviderError>>) -> Self {
        *self.completions.lock() = queue.into();
        self
    }
    fn with_streams(self, queue: Vec<StreamPlan>) -> Self {
        *self.streams.lock() = queue.into();
        self
    }
    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
    fn with_completion_panic(self) -> Self {
        self.panic_completion.store(1, Ordering::SeqCst);
        self
    }
    fn lane_path(&self) -> Option<String> {
        self.lane_path.lock().clone()
    }
    fn models_request_id(&self) -> u64 {
        self.models_request_id.load(Ordering::SeqCst)
    }
    fn with_hold(self, ms: usize) -> Self {
        self.hold_ms.store(ms, Ordering::SeqCst);
        self
    }
    fn with_hold_after_first(self, ms: usize) -> Self {
        self.hold_after_first_ms.store(ms, Ordering::SeqCst);
        self
    }
    fn with_stream_plan_delay(self, ms: u64) -> Self {
        self.stream_plan_delay_ms.store(ms, Ordering::SeqCst);
        self
    }
    fn with_relay_delay(self, ms: u64) -> Self {
        self.relay_delay_ms.store(ms, Ordering::SeqCst);
        self
    }

    fn with_models(self, answer: Result<serde_json::Value, ProviderError>) -> Self {
        *self.models.lock() = VecDeque::from([answer]);
        self
    }
    fn with_model_queue(self, queue: Vec<Result<serde_json::Value, ProviderError>>) -> Self {
        *self.models.lock() = queue.into();
        self
    }
    fn with_models_delay_after_first(self, ms: u64) -> Self {
        self.models_delay_after_first_ms.store(ms, Ordering::SeqCst);
        self
    }
    fn models_attempts(&self) -> usize {
        self.models_attempts.load(Ordering::SeqCst)
    }
    fn with_models_panic(mut self) -> Self {
        self.panic_models = true;
        self
    }
}

#[async_trait::async_trait]
impl Provider for Scripted {
    fn name(&self) -> &str {
        "scripted"
    }
    async fn complete(
        &self,
        _req: &ChatRequest,
        _ctx: &CallContext<'_>,
    ) -> Result<Completion, ProviderError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let hold = self.hold_ms.load(Ordering::SeqCst);
        if self.attempts.load(Ordering::SeqCst) > 1 {
            let hold = self.hold_after_first_ms.load(Ordering::SeqCst);
            if hold > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(hold as u64)).await;
            }
        }
        if hold > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(hold as u64)).await;
        }
        if self.panic_completion.load(Ordering::SeqCst) != 0 {
            panic!("scripted completion panic");
        }
        self.completions
            .lock()
            .pop_front()
            .unwrap_or_else(|| Err(ProviderError::internal("script exhausted")))
    }
    async fn stream(
        &self,
        _req: &ChatRequest,
        _ctx: &CallContext<'_>,
    ) -> Result<ChatStream, ProviderError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let delay_ms = self.stream_delay_ms.load(Ordering::SeqCst);
        if delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        let plan_delay = self.stream_plan_delay_ms.load(Ordering::SeqCst);
        if plan_delay > 0 && self.streams.lock().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(plan_delay)).await;
        }
        if self.streams.lock().is_empty() && self.panic_completion.load(Ordering::SeqCst) != 0 {
            panic!("scripted stream panic");
        }
        if let Some(live) = self.stream_live.lock().take() {
            return Ok(live);
        }
        let plan =
            self.streams.lock().pop_front().unwrap_or_else(|| {
                StreamPlan::PreHandoff(ProviderError::internal("script exhausted"))
            });
        match plan {
            StreamPlan::PreHandoff(e) => Err(e),
            StreamPlan::Chunks(v) => Ok(stream::iter(v).boxed()),
            StreamPlan::Panic => panic!("scripted stream panic"),
        }
    }
    async fn stream_relay(
        &self,
        _req: &ChatRequest,
        _ctx: &CallContext<'_>,
    ) -> Result<opencode2api_kit::ChunkStream, ProviderError> {
        // Pop FIRST; an unsupported fast path costs no upstream round-trip
        // and must not touch the attempt counter (fallback stream() counts
        // its own — otherwise every relay-first test double-counts).
        if let Some(live) = self.relay_live.lock().take() {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            return Ok(live);
        }
        let next = self.relays.lock().pop_front();
        match next {
            Some(frames) => {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                Ok(stream::iter(frames).boxed())
            }
            None => Err(ProviderError::unsupported("raw stream relay")),
        }
    }
    fn native_dialects(&self) -> &'static [Dialect] {
        if self.native {
            &[
                Dialect::Chat,
                Dialect::Anthropic,
                Dialect::Responses,
                Dialect::Gemini,
            ]
        } else {
            &[]
        }
    }
    async fn relay_raw(
        &self,
        _d: Dialect,
        _body: bytes::Bytes,
        ctx: &CallContext<'_>,
    ) -> Result<RawReply, ProviderError> {
        *self.lane_path.lock() = ctx.request_path.map(str::to_string);
        // Like stream_relay: only a served script counts as a round-trip.
        let next = self.raws.lock().pop_front();
        match next {
            Some(r) => {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                let delay_ms = self.relay_delay_ms.load(Ordering::SeqCst);
                if delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
                r
            }
            None => Err(ProviderError::unsupported("native fidelity relay")),
        }
    }

    async fn models(&self, ctx: &CallContext<'_>) -> Result<serde_json::Value, ProviderError> {
        self.models_attempts.fetch_add(1, Ordering::SeqCst);
        self.models_request_id
            .store(ctx.request_id, Ordering::SeqCst);
        if self.panic_models {
            panic!("scripted models panic");
        }
        let delay_ms = self.models_delay_ms.load(Ordering::SeqCst);
        if delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        if self.models_attempts.load(Ordering::SeqCst) > 1 {
            let after_first = self.models_delay_after_first_ms.load(Ordering::SeqCst);
            if after_first > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(after_first)).await;
            }
        }
        self.models
            .lock()
            .pop_front()
            .unwrap_or_else(|| Err(ProviderError::unsupported("models")))
    }
}

fn test_cfg() -> ServerConfig {
    ServerConfig {
        retry: RetryConfig {
            max_attempts: 3,
            base_ms: 1,
            cap_ms: 2,
        },
        sse_keepalive_secs: 0,
        request_timeout_secs: 5,
        stream_deadline_secs: 30,
        admission_wait_secs: 1,
        max_inflight: 4,
        ..Default::default()
    }
}

fn pipeline(scripted: Arc<Scripted>, cfg: ServerConfig) -> Pipeline {
    Pipeline::new(scripted, Arc::new(cfg), None)
}

fn post(uri: &'static str, body: serde_json::Value, auth: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header(HeaderName::from_static("content-type"), "application/json");
    if let Some(token) = auth {
        builder = builder.header(
            HeaderName::from_static("authorization"),
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

async fn send(
    p: Pipeline,
    uri: &'static str,
    body: serde_json::Value,
    auth: Option<&str>,
) -> (StatusCode, serde_json::Value, String) {
    let resp = opencode2api_server::build_router(p)
        .oneshot(post(uri, body, auth))
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let json = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    (status, json, text)
}

async fn get(p: Pipeline, uri: &'static str) -> (StatusCode, String) {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = opencode2api_server::build_router(p)
        .oneshot(req)
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

fn completion(text: &str) -> Completion {
    Completion {
        refusal: None,
        id: "gen-1".into(),
        model: "svc-model".into(),
        text: text.into(),
        finish_reason: Some("stop".into()),
        usage: Some(Usage {
            prompt_tokens: 3,
            completion_tokens: 2,
            ..Default::default()
        }),
        tool_calls: Vec::new(),
        raw: Some(serde_json::json!({
            "id": "gen-1",
            "object": "chat.completion",
            "model": "svc-model",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 3, "completion_tokens": 2, "vendor_extra": {"cost_cents": 1}},
            "vendor_field": "must-survive-relay"
        })),
        reasoning: None,
    }
}

/// A chunk's `raw` is wire bytes (the IR keeps the vendor's own frame, not a
/// re-parsed document), so scripted chunks serialize theirs the same way the
/// provider does.
fn raw(value: serde_json::Value) -> Option<bytes::Bytes> {
    Some(bytes::Bytes::from(serde_json::to_vec(&value).unwrap()))
}

fn chunk(text: &str) -> ChatChunk {
    ChatChunk {
        refusal: String::new(),
        id: "gen-1".into(),
        model: "svc-model".into(),
        text: text.into(),
        finish_reason: None,
        usage: None,
        tool_calls: Vec::new(),
        raw: raw(serde_json::json!({
            "id": "gen-1", "object": "chat.completion.chunk", "model": "svc-model",
            "choices": [{"index": 0, "delta": {"content": text}, "finish_reason": null}]
        })),
        reasoning: String::new(),
    }
}

fn final_chunk() -> ChatChunk {
    ChatChunk {
        refusal: String::new(),
        id: "gen-1".into(),
        model: "svc-model".into(),
        text: String::new(),
        finish_reason: Some("stop".into()),
        usage: Some(Usage {
            prompt_tokens: 3,
            completion_tokens: 5,
            ..Default::default()
        }),
        tool_calls: Vec::new(),
        raw: raw(serde_json::json!({
            "id": "gen-1", "object": "chat.completion.chunk", "model": "svc-model",
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 3, "completion_tokens": 5}
        })),
        reasoning: String::new(),
    }
}

fn chunks_ok(v: Vec<ChatChunk>) -> StreamPlan {
    StreamPlan::Chunks(v.into_iter().map(Ok).collect())
}

async fn stream_request(
    scripted: Arc<Scripted>,
    anthropic: bool,
) -> (StatusCode, serde_json::Value, String) {
    let (uri, body) = if anthropic {
        (
            "/v1/messages",
            serde_json::json!({"model": "m", "max_tokens": 64, "stream": true, "messages": [{"role": "user", "content": "x"}]}),
        )
    } else {
        (
            "/v1/chat/completions",
            serde_json::json!({"model": "m", "stream": true, "messages": [{"role": "user", "content": "x"}]}),
        )
    };
    send(pipeline(scripted, test_cfg()), uri, body, None).await
}

// ── buffered, OpenAI dialect ─────────────────────────────────────────────

#[tokio::test]
async fn openai_buffered_relays_raw_verbatim() {
    let scripted = Arc::new(Scripted::new().with_completions(vec![Ok(completion("hi"))]));
    let (status, body, _) = send(
        pipeline(scripted, test_cfg()),
        "/v1/chat/completions",
        serde_json::json!({"model": "m", "messages": [{"role": "user", "content": "x"}]}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["vendor_field"], "must-survive-relay");
    assert_eq!(body["usage"]["vendor_extra"]["cost_cents"], 1);
}

#[tokio::test]
async fn retry_re_sends_retryables_until_success() {
    let scripted = Arc::new(Scripted::new().with_completions(vec![
        Err(ProviderError::rate_limited("slow down")),
        Err(ProviderError::bad_gateway("flaky")),
        Ok(completion("third time")),
    ]));
    let (status, body, _) = send(
        pipeline(scripted.clone(), test_cfg()),
        "/v1/chat/completions",
        serde_json::json!({"model": "m", "messages": []}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["choices"][0]["message"]["content"], "third time");
    assert_eq!(scripted.attempts(), 3);
}

#[tokio::test]
async fn non_retryable_fails_fast_one_attempt() {
    let scripted = Arc::new(
        Scripted::new().with_completions(vec![Err(ProviderError::bad_request("bad model"))]),
    );
    let (status, body, _) = send(
        pipeline(scripted.clone(), test_cfg()),
        "/v1/chat/completions",
        serde_json::json!({"model": "m", "messages": []}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(scripted.attempts(), 1);
}

#[tokio::test]
async fn vendor_error_envelope_passes_through_verbatim() {
    let err = ProviderError::from_upstream(
        "svc",
        400,
        br#"{"error":{"message":"model not found","type":"invalid_request_error","code":"model_not_found"}}"#,
    );
    let scripted = Arc::new(Scripted::new().with_completions(vec![Err(err)]));
    let (status, body, _) = send(
        pipeline(scripted, test_cfg()),
        "/v1/chat/completions",
        serde_json::json!({"model": "m", "messages": []}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "model_not_found");
}

// ── streaming, OpenAI dialect ────────────────────────────────────────────

fn rate_limited_with_relay_headers(computed_retry_after: Option<u64>) -> ProviderError {
    let mut upstream = HeaderMap::new();
    upstream.insert("retry-after", HeaderValue::from_static("42"));
    upstream.insert("x-ratelimit-remaining", HeaderValue::from_static("0"));
    upstream.insert("request-id", HeaderValue::from_static("req_folded"));
    upstream.insert("x-request-id", HeaderValue::from_static("must_not_relay"));
    upstream.insert("content-length", HeaderValue::from_static("999999"));
    upstream.insert("x-account-secret", HeaderValue::from_static("do-not-relay"));

    let mut error = ProviderError::rate_limited("slow down");
    if let Some(secs) = computed_retry_after {
        error = error.with_retry_after(secs);
    }
    error.with_upstream_headers(upstream)
}

#[tokio::test]
async fn folded_error_relays_client_headers_but_not_vendor_framing() {
    let scripted = Arc::new(
        Scripted::new().with_completions(vec![Err(rate_limited_with_relay_headers(None))]),
    );
    let mut cfg = test_cfg();
    cfg.retry.max_attempts = 1;
    let resp = opencode2api_server::build_router(pipeline(scripted, cfg))
        .oneshot(post(
            "/v1/chat/completions",
            serde_json::json!({"model":"m","messages":[]}),
            None,
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    let headers = resp.headers();
    assert_eq!(headers["x-ratelimit-remaining"], "0");
    assert_ne!(
        headers
            .get("request-id")
            .map(|value| value.to_str().unwrap()),
        Some("req_folded"),
        "the folded error must not restore a vendor request id"
    );
    assert_ne!(
        headers
            .get("x-request-id")
            .map(|value| value.to_str().unwrap()),
        Some("must_not_relay"),
        "the folded error must not restore a vendor x-request-id"
    );
    assert!(
        headers.contains_key("x-request-id"),
        "the fold must retain the proxy-stamped correlation id"
    );
    assert!(headers.get("x-account-secret").is_none());
    assert_ne!(
        headers
            .get("content-length")
            .map(|value| value.to_str().unwrap()),
        Some("999999"),
        "vendor framing must not describe the folded response body"
    );
    assert_eq!(
        headers["retry-after"], "42",
        "with no computed retry hint, the vendor hint survives the fold"
    );
}

#[tokio::test]
async fn folded_computed_retry_after_overrides_vendor_header() {
    let scripted = Arc::new(
        Scripted::new().with_completions(vec![Err(rate_limited_with_relay_headers(Some(7)))]),
    );
    let mut cfg = test_cfg();
    cfg.retry.max_attempts = 1;
    let resp = opencode2api_server::build_router(pipeline(scripted, cfg))
        .oneshot(post(
            "/v1/chat/completions",
            serde_json::json!({"model":"m","messages":[]}),
            None,
        ))
        .await
        .unwrap();

    assert_eq!(resp.headers()["retry-after"], "7");
}

#[tokio::test]
async fn openai_stream_frames_and_terminates_with_done() {
    let scripted = Arc::new(Scripted::new().with_streams(vec![chunks_ok(vec![
        chunk("he"),
        chunk("llo"),
        final_chunk(),
    ])]));
    let (_, _, text) = stream_request(scripted.clone(), false).await;
    assert!(text.contains("data: {") && text.contains("\"id\":\"gen-1\""));
    assert!(text.ends_with("data: [DONE]\n\n"), "got {text:?}");
    assert_eq!(text.matches("data: ").count(), 4);
    // The counter tracks upstream ROUND-TRIPS, not method calls: an
    // unsupported fast path sends nothing and must not cost an attempt on
    // top of the transcoded fallback's one.
    assert_eq!(
        scripted.attempts(),
        1,
        "declined relay + one stream() call = one attempt"
    );
}

#[tokio::test]
async fn in_stream_failure_truncates_without_done() {
    let scripted = Arc::new(Scripted::new().with_streams(vec![StreamPlan::Chunks(vec![
        Ok(chunk("partial")),
        Err(ProviderError::bad_gateway("upstream died mid-stream")),
    ])]));
    let (_, _, text) = stream_request(scripted.clone(), false).await;
    assert!(text.contains("\"error\""));
    assert!(
        !text.contains("[DONE]"),
        "a truncated answer must not look complete: {text:?}"
    );
    assert_eq!(
        scripted.attempts(),
        1,
        "a post-handoff failure must never re-send"
    );
}

// ── Anthropic dialect ────────────────────────────────────────────────────

#[tokio::test]
async fn anthropic_missing_max_tokens_is_rejected_with_one_terminal_record() {
    let shared = captured();
    let scripted = Arc::new(Scripted::new());
    let req = Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header("x-request-id", "74001")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"model":"m","messages":[{"role":"user","content":"x"}]}"#,
        ))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted.clone(), test_cfg()))
        .oneshot(req)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(resp.headers()["x-request-id"], "74001");
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(
        scripted.attempts(),
        0,
        "rejection must precede the provider"
    );

    let lines = terminal_lines(&shared, "74001");
    assert_eq!(lines.len(), 1, "one terminal record for the rejection");
    let fields = &lines[0]["fields"];
    assert_eq!(fields["result"], "rejected");
    assert_eq!(fields["status"], 400);
    assert_eq!(fields["format"], "anthropic");
    assert_eq!(fields["endpoint"], "messages");
}

#[tokio::test]
async fn anthropic_buffered_shapes_message() {
    let scripted = Arc::new(Scripted::new().with_completions(vec![Ok(completion("hola"))]));
    let (status, body, _) = send(
        pipeline(scripted, test_cfg()),
        "/v1/messages",
        serde_json::json!({"model": "m", "max_tokens": 64, "messages": [{"role": "user", "content": "x"}]}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["type"], "message");
    assert_eq!(body["content"][0]["type"], "text");
    assert_eq!(body["content"][0]["text"], "hola");
    assert_eq!(body["stop_reason"], "end_turn");
    assert_eq!(body["usage"]["output_tokens"], 2);
}

#[tokio::test]
async fn anthropic_stream_event_order_is_faithful() {
    let scripted = Arc::new(Scripted::new().with_streams(vec![chunks_ok(vec![
        chunk("a"),
        chunk("b"),
        final_chunk(),
    ])]));
    let (_, _, text) = stream_request(scripted, true).await;
    let events: Vec<&str> = text
        .lines()
        .filter_map(|l| l.strip_prefix("event: "))
        .collect();
    assert_eq!(
        events,
        vec![
            "message_start",
            "ping",
            "content_block_start",
            "content_block_delta",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ]
    );
    assert!(!text.contains("[DONE]"), "Anthropic has no [DONE] sentinel");
}

#[tokio::test]
async fn anthropic_in_stream_error_closes_without_message_stop() {
    let scripted = Arc::new(Scripted::new().with_streams(vec![StreamPlan::Chunks(vec![
        Ok(chunk("a")),
        Err(ProviderError::bad_gateway("vendor exploded")),
    ])]));
    let (_, _, text) = stream_request(scripted.clone(), true).await;
    assert!(text.contains("event: error"), "got {text:?}");
    assert!(!text.contains("message_stop"), "got {text:?}");
    assert_eq!(
        scripted.attempts(),
        1,
        "a post-handoff failure must never re-send"
    );
}

// ── auth gate, models, ops surface ───────────────────────────────────────

#[tokio::test]
async fn client_api_key_gate() {
    let mut cfg = test_cfg();
    cfg.client_api_key = Some("secret-token".into());
    let scripted = Arc::new(Scripted::new().with_completions(vec![Ok(completion("ok"))]));
    let p = pipeline(scripted.clone(), cfg);

    let (status, _, _) = send(
        p.clone(),
        "/v1/chat/completions",
        serde_json::json!({"model": "m", "messages": []}),
        Some("wrong"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(scripted.attempts(), 0, "gate must run before provider");

    let (status, _, _) = send(
        p,
        "/v1/chat/completions",
        serde_json::json!({"model": "m", "messages": []}),
        Some("secret-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn models_capability_gap_is_typed_501() {
    let scripted = Arc::new(Scripted::new());
    let (status, body) = get(pipeline(scripted, test_cfg()), "/v1/models").await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert!(body.contains("does not support"), "got {body:?}");
}

#[tokio::test]
async fn models_request_supplies_nonzero_provider_context() {
    let scripted = Arc::new(
        Scripted::new().with_models(Ok(serde_json::json!({"object": "list", "data": []}))),
    );
    let req = Request::builder()
        .method("GET")
        .uri("/v1/models")
        .header("x-request-id", "74009")
        .body(Body::empty())
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted.clone(), test_cfg()))
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let echoed: u64 = resp.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    let _ = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    assert_eq!(
        scripted.models_request_id(),
        echoed,
        "the exact client request id must reach the provider"
    );
}

#[tokio::test]
async fn health_is_open_and_metrics_is_501_without_recorder() {
    let scripted = Arc::new(Scripted::new());
    let p = pipeline(scripted, test_cfg());
    let (status, _) = get(p.clone(), "/health").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = get(p, "/metrics").await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn unknown_paths_and_methods_answer_parseable_envelopes() {
    let scripted = Arc::new(Scripted::new());
    let p = pipeline(scripted, test_cfg());
    let (status, body) = get(p.clone(), "/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["error"]["type"], "not_found_error");

    let req = Request::builder()
        .method("GET")
        .uri("/v1/chat/completions")
        .body(Body::empty())
        .unwrap();
    let resp = opencode2api_server::build_router(p)
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}

// ── Responses dialect ────────────────────────────────────────────────────

#[tokio::test]
async fn responses_buffered_shapes_response_object() {
    let scripted = Arc::new(Scripted::new().with_completions(vec![Ok(completion("bonjour"))]));
    let (status, body, _) = send(
        pipeline(scripted, test_cfg()),
        "/v1/responses",
        serde_json::json!({"model":"m","input":[{"role":"user","content":[{"type":"input_text","text":"x"}]}]}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["object"], "response");
    assert_eq!(body["status"], "completed");
    assert_eq!(body["output"][0]["content"][0]["type"], "output_text");
    assert_eq!(body["output_text"], "bonjour");
    assert_eq!(body["usage"]["total_tokens"], 5);
}

#[tokio::test]
async fn responses_stream_grammar_is_faithful() {
    let scripted = Arc::new(Scripted::new().with_streams(vec![chunks_ok(vec![
        chunk("a"),
        chunk("b"),
        final_chunk(),
    ])]));
    let (_, _, text) = send(
        pipeline(scripted, test_cfg()),
        "/v1/responses",
        serde_json::json!({"model":"m","input":"x","stream":true}),
        None,
    )
    .await;
    let events: Vec<&str> = text
        .lines()
        .filter_map(|l| l.strip_prefix("event: "))
        .collect();
    assert_eq!(
        events,
        vec![
            "response.created",
            "response.in_progress",
            "response.output_item.added",
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]
    );
    assert!(!text.contains("[DONE]"), "responses has no [DONE] sentinel");
    // sequence numbers: fixtures pin the origin at 0, strictly increasing
    let mut seqs: Vec<u64> = Vec::new();
    for l in text.lines().filter_map(|l| l.strip_prefix("data: ")) {
        let v: serde_json::Value = serde_json::from_str(l).unwrap();
        seqs.push(
            v["sequence_number"]
                .as_u64()
                .expect("every responses event carries sequence_number"),
        );
    }
    assert_eq!(seqs.first().copied(), Some(0), "stream opens at sequence 0");
    for w in seqs.windows(2) {
        assert!(w[1] > w[0], "sequence must strictly increase: {w:?}");
    }
}

#[tokio::test]
async fn responses_in_stream_error_emits_error_event_not_completed() {
    let scripted = Arc::new(Scripted::new().with_streams(vec![StreamPlan::Chunks(vec![
        Ok(chunk("a")),
        Err(ProviderError::bad_gateway("vendor exploded")),
    ])]));
    let (_, _, text) = send(
        pipeline(scripted.clone(), test_cfg()),
        "/v1/responses",
        serde_json::json!({"model":"m","input":"x","stream":true}),
        None,
    )
    .await;
    assert!(text.contains("event: error"), "got {text:?}");
    assert!(!text.contains("response.completed"), "got {text:?}");
    assert_eq!(
        scripted.attempts(),
        1,
        "a post-handoff failure must never re-send"
    );
}

// ── verbatim relay path (chat dialect only) ──────────────────────────────

/// Discriminating frames: deliberately unsorted keys (a transcoded relay
/// would alphabetize them through `serde_json::Value`), the `[DONE]`
/// sentinel, and the vendor bookkeeping frame AFTER it (a trailing `cost`)
/// that must never reach the client.
fn relay_frames() -> Vec<Result<bytes::Bytes, String>> {
    [
        "data: {\"z_field\":1,\"a_field\":2,\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n",
        "data: [DONE]\n\n",
        "data: {\"cost\":0.1}\n\n",
    ]
    .into_iter()
    .map(|f| Ok(bytes::Bytes::from_static(f.as_bytes())))
    .collect()
}

#[tokio::test]
async fn chat_stream_prefers_verbatim_and_preserves_vendor_bytes() {
    let scripted = Arc::new(Scripted::new().with_relays(vec![relay_frames()]));
    let (_, _, text) = send(
        pipeline(scripted.clone(), test_cfg()),
        "/v1/chat/completions",
        serde_json::json!({"model":"m","messages":[],"stream":true}),
        None,
    )
    .await;
    assert!(
        text.contains(r#""z_field":1,"a_field":2"#),
        "verbatim path must not re-serialize through Value (key sort): {text:?}"
    );
    assert_eq!(text.matches("[DONE]").count(), 1);
    assert!(text.ends_with("data: [DONE]\n\n"));
    assert!(
        !text.contains("cost"),
        "post-[DONE] bookkeeping must not relay: {text:?}"
    );
    assert_eq!(
        scripted.attempts(),
        1,
        "relay satisfied the request; no fallback send"
    );
}

#[tokio::test]
async fn bridge_dialects_never_take_the_relay_path() {
    // A relay script exists, but /v1/messages must ignore it and transcode
    // (the relay's raw bytes are OpenAI-shaped; the bridge cannot fold them).
    let scripted = Arc::new(
        Scripted::new()
            .with_relays(vec![relay_frames()])
            .with_streams(vec![chunks_ok(vec![chunk("t"), final_chunk()])]),
    );
    let (_, _, text) = send(
        pipeline(scripted.clone(), test_cfg()),
        "/v1/messages",
        serde_json::json!({"model":"m","max_tokens":8,"stream":true,"messages":[{"role":"user","content":"x"}]}),
        None,
    )
    .await;
    assert!(text.contains("event: message_start"), "got {text:?}");
    assert!(
        !text.contains("z_field"),
        "relay frames must not reach the bridge"
    );
    // attempts: 1 stream_relay probe never happens for anthropic (only
    // stream()), so exactly one provider call
    assert_eq!(scripted.attempts(), 1);
}

#[tokio::test]
async fn verbatim_eof_without_done_does_not_synthesize_terminator() {
    // A clean EOF with no vendor sentinel is truncated, not complete: the
    // gateway preserves the bytes but must not make the response look finished.
    let frames = vec![Ok(bytes::Bytes::from_static(
        b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n",
    ))];
    let scripted = Arc::new(Scripted::new().with_relays(vec![frames]));
    let (_, _, text) = send(
        pipeline(scripted, test_cfg()),
        "/v1/chat/completions",
        serde_json::json!({"model":"m","messages":[],"stream":true}),
        None,
    )
    .await;
    let upstream = "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n";
    assert_eq!(text, upstream, "clean EOF must not be normalized");
    assert!(!text.contains("[DONE]"), "got {text:?}");
}

#[tokio::test]
async fn verbatim_partial_tail_withholds_done() {
    // EOF mid-record (no trailing blank line): a truncated answer must NOT
    // be completed with the sentinel.
    let frames = vec![Ok(bytes::Bytes::from_static(
        b"data: {\"complete\":false}\n\ndata: {\"partial\":",
    ))];
    let scripted = Arc::new(Scripted::new().with_relays(vec![frames]));
    let (_, _, text) = send(
        pipeline(scripted, test_cfg()),
        "/v1/chat/completions",
        serde_json::json!({"model":"m","messages":[],"stream":true}),
        None,
    )
    .await;
    assert!(
        text.contains("\"complete\":false"),
        "complete record relays: {text:?}"
    );
    assert!(
        !text.contains("[DONE]"),
        "partial tail must not look complete: {text:?}"
    );
}

/// The relay's whole reason to exist is pace: a frame decoded from upstream
/// must reach the client while the upstream stream is still open. Buffering
/// until `[DONE]` would pass every other relay test in this file (same bytes,
/// same order) and still leave a client staring at nothing for the length of
/// a generation — so this test holds the upstream open and demands the frame.
#[tokio::test]
async fn verbatim_relay_yields_before_upstream_completes() {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, String>>(4);
    let upstream =
        stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|i| (i, rx)) }).boxed();
    let scripted = Arc::new(Scripted::new().with_relay_stream(upstream));

    let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
        .oneshot(post(
            "/v1/chat/completions",
            serde_json::json!({"model":"m","messages":[],"stream":true}),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let mut body = resp.into_body().into_data_stream();

    tx.send(Ok(bytes::Bytes::from_static(
        b"data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n",
    )))
    .await
    .unwrap();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(2), body.next())
        .await
        .expect("relay withheld the frame while upstream was still open")
        .expect("stream ended early")
        .unwrap();
    assert!(
        String::from_utf8_lossy(&frame).contains("first"),
        "got {frame:?}"
    );

    // …and the sentinel still terminates it when upstream finally says so.
    tx.send(Ok(bytes::Bytes::from_static(b"data: [DONE]\n\n")))
        .await
        .unwrap();
    drop(tx);
    let rest = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut out = String::new();
        while let Some(Ok(b)) = body.next().await {
            out.push_str(&String::from_utf8_lossy(&b));
        }
        out
    })
    .await
    .expect("stream did not finish");
    assert!(rest.ends_with("data: [DONE]\n\n"), "got {rest:?}");
}

/// The transcoded twin of the relay's pace test. The fold batches whatever
/// the provider already has into one write, which is free — but only as long
/// as it never WAITS for more: a chunk in hand must reach the client while
/// the provider stream is still open.
#[tokio::test]
async fn transcoded_stream_yields_before_the_provider_finishes() {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<ChatChunk, ProviderError>>(4);
    let upstream =
        stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|i| (i, rx)) }).boxed();
    let scripted = Arc::new(Scripted::new().with_live_stream(upstream));

    let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
        .oneshot(post(
            "/v1/messages",
            serde_json::json!({"model":"m","max_tokens":8,"stream":true,
                               "messages":[{"role":"user","content":"x"}]}),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let mut body = resp.into_body().into_data_stream();

    // The banner leaves at handoff, before any chunk exists.
    let head = tokio::time::timeout(std::time::Duration::from_secs(2), body.next())
        .await
        .expect("banner withheld")
        .expect("stream ended early")
        .unwrap();
    assert!(
        String::from_utf8_lossy(&head).contains("message_start"),
        "got {head:?}"
    );

    tx.send(Ok(chunk("first"))).await.unwrap();
    let delta = tokio::time::timeout(std::time::Duration::from_secs(2), body.next())
        .await
        .expect("fold withheld a chunk it already had")
        .expect("stream ended early")
        .unwrap();
    assert!(
        String::from_utf8_lossy(&delta).contains("first"),
        "got {delta:?}"
    );

    drop(tx);
    let rest = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut out = String::new();
        while let Some(Ok(b)) = body.next().await {
            out.push_str(&String::from_utf8_lossy(&b));
        }
        out
    })
    .await
    .expect("stream did not finish");
    assert!(rest.contains("message_stop"), "got {rest:?}");
}

/// A keepalive is a standalone SSE record: it may appear in provider silence,
/// but never inside a folded event. This uses the real one-second clock.
#[tokio::test]
async fn folded_keepalive_does_not_split_a_complete_frame() {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<ChatChunk, ProviderError>>(2);
    let upstream = stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
    .boxed();
    let scripted = Arc::new(Scripted::new().with_live_stream(upstream));
    let mut cfg = test_cfg();
    cfg.sse_keepalive_secs = 1;
    let resp = opencode2api_server::build_router(pipeline(scripted, cfg))
        .oneshot(post(
            "/v1/messages",
            serde_json::json!({"model":"m","max_tokens":8,"stream":true,
                               "messages":[{"role":"user","content":"x"}]}),
            None,
        ))
        .await
        .unwrap();
    let mut body = resp.into_body().into_data_stream();
    tx.send(Ok(chunk("folded"))).await.unwrap();
    let mut wire = String::new();
    for _ in 0..3 {
        let part = tokio::time::timeout(std::time::Duration::from_secs(2), body.next())
            .await
            .expect("folded stream withheld a record or keepalive")
            .expect("stream ended before keepalive")
            .unwrap();
        wire.push_str(&String::from_utf8_lossy(&part));
    }
    assert!(wire.contains(": keepalive\n\n"), "got {wire:?}");
    let records: Vec<&str> = wire
        .split("\n\n")
        .filter(|record| !record.is_empty())
        .collect();
    for record in &records {
        if let Some(payload) = record
            .split("\n")
            .find_map(|line| line.strip_prefix("data: "))
        {
            serde_json::from_str::<serde_json::Value>(payload)
                .unwrap_or_else(|err| panic!("keepalive split data record {payload:?}: {err}"));
        } else {
            assert_eq!(
                *record, ": keepalive",
                "injected a non-frame record: {record:?}"
            );
        }
    }
    assert!(
        records.iter().any(|record| record.contains("folded")),
        "got {wire:?}"
    );
}

#[tokio::test]
async fn live_streams_truncate_after_the_post_handoff_deadline() {
    let shared = captured();
    let mut cfg = test_cfg();
    cfg.stream_deadline_secs = 1;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, String>>(2);
    let upstream = stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
    .boxed();
    let scripted = Arc::new(Scripted::new().with_relay_stream(upstream));
    let mut req = post(
        "/v1/chat/completions",
        serde_json::json!({"model":"m","messages":[],"stream":true}),
        None,
    );
    req.headers_mut()
        .insert("x-request-id", HeaderValue::from_static("75001"));
    let resp = opencode2api_server::build_router(pipeline(scripted, cfg))
        .oneshot(req)
        .await
        .unwrap();
    let body = resp.into_body().into_data_stream();
    let record =
        bytes::Bytes::from_static(b"data: {\"choices\":[{\"delta\":{\"content\":\"last\"}}]}\n\n");
    tx.send(Ok(record.clone())).await.unwrap();
    let wire = axum::body::to_bytes(axum::body::Body::from_stream(body), 1 << 20)
        .await
        .unwrap();
    assert_eq!(wire, record, "deadline changed or invented relay bytes");
    assert!(
        !String::from_utf8_lossy(&wire).contains("[DONE]"),
        "got {wire:?}"
    );
    let lines = terminal_lines(&shared, "75001");
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["fields"]["result"], "truncated");
    assert_eq!(lines[0]["fields"]["frames"], 1);
}

#[tokio::test]
async fn transcoded_stream_deadline_appends_error_and_withholds_done() {
    let shared = captured();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<ChatChunk, ProviderError>>(2);
    let upstream = stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
    .boxed();
    let scripted = Arc::new(Scripted::new().with_live_stream(upstream));
    let mut cfg = test_cfg();
    cfg.stream_deadline_secs = 1;
    let mut req = post(
        "/v1/chat/completions",
        serde_json::json!({"model":"m","messages":[],"stream":true}),
        None,
    );
    req.headers_mut()
        .insert("x-request-id", HeaderValue::from_static("75002"));
    let resp = opencode2api_server::build_router(pipeline(scripted, cfg))
        .oneshot(req)
        .await
        .unwrap();
    let body = resp.into_body().into_data_stream();
    tx.send(Ok(chunk("last"))).await.unwrap();
    let wire = axum::body::to_bytes(axum::body::Body::from_stream(body), 1 << 20)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&wire);
    assert!(text.contains("last"), "got {text:?}");
    let last = text
        .rsplit("\n\n")
        .find(|record| !record.is_empty())
        .unwrap();
    assert!(
        last.starts_with("data: ") && last.contains("\"error\""),
        "got {last:?}"
    );
    assert!(
        !text.contains("[DONE]"),
        "truncation looked complete: {text:?}"
    );
    let lines = terminal_lines(&shared, "75002");
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["fields"]["result"], "truncated");
    assert_eq!(lines[0]["fields"]["frames"], 1);
}

/// A failure after one relayed record is observable to an operator as both
/// the failed outcome and the work that reached the client before failure.
#[tokio::test]
async fn failed_live_stream_records_the_frames_it_relayed() {
    let shared = captured();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, String>>(2);
    let upstream =
        stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|i| (i, rx)) }).boxed();
    let scripted = Arc::new(Scripted::new().with_relay_stream(upstream));
    let mut req = post(
        "/v1/chat/completions",
        serde_json::json!({"model":"m","messages":[],"stream":true}),
        None,
    );
    req.headers_mut()
        .insert("x-request-id", HeaderValue::from_static("74008"));
    let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // A failed response cannot complete before these sends, proving the error
    // is an in-stream event rather than a pre-handoff rejection.
    tx.send(Ok(bytes::Bytes::from_static(
        b"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
    )))
    .await
    .unwrap();
    tx.send(Err("upstream disconnected".into())).await.unwrap();
    drop(tx);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&body).contains("partial"));

    let lines = terminal_lines(&shared, "74008");
    assert_eq!(lines.len(), 1, "one terminal record for the failed stream");
    let fields = &lines[0]["fields"];
    assert_eq!(fields["stream"], true);
    assert_eq!(fields["result"], "failed");
    assert_eq!(fields["frames"], 1);
}

/// The stream guard catches an upstream poll panic after the response has
/// committed. The client must see a finite, unterminated stream and the record
/// must name that distinct outcome.
#[tokio::test]
async fn verbatim_upstream_poll_panic_records_a_panicked_stream() {
    let shared = captured();
    let upstream = stream::poll_fn(
        |_| -> std::task::Poll<Option<Result<bytes::Bytes, String>>> {
            panic!("scripted upstream poll panic");
        },
    )
    .boxed();
    let scripted = Arc::new(Scripted::new().with_relay_stream(upstream));
    let mut req = post(
        "/v1/chat/completions",
        serde_json::json!({"model":"m","messages":[],"stream":true}),
        None,
    );
    req.headers_mut()
        .insert("x-request-id", HeaderValue::from_static("75003"));
    let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = String::from_utf8_lossy(
        &axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap(),
    )
    .into_owned();
    assert!(
        !text.contains("[DONE]"),
        "panic must not look complete: {text:?}"
    );
    let lines = terminal_lines(&shared, "75003");
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["fields"]["result"], "panicked");
    assert_eq!(lines[0]["fields"]["frames"], 0);
}

/// Dropping the response body is the public client-disconnect signal. The
/// stream guard must emit a dropped record even though no terminal frame was
/// received from the still-live provider channel.
#[tokio::test]
async fn dropping_a_live_client_body_records_a_dropped_stream() {
    let shared = captured();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, String>>(1);
    let upstream =
        stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|i| (i, rx)) }).boxed();
    let scripted = Arc::new(Scripted::new().with_relay_stream(upstream));
    let mut req = post(
        "/v1/chat/completions",
        serde_json::json!({"model":"m","messages":[],"stream":true}),
        None,
    );
    req.headers_mut()
        .insert("x-request-id", HeaderValue::from_static("75004"));
    let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
        .oneshot(req)
        .await
        .unwrap();
    drop(resp.into_body());
    assert!(
        tx.send(Ok(bytes::Bytes::from_static(b"data: [DONE]\n\n")))
            .await
            .is_err()
    );
    let lines = terminal_lines(&shared, "75004");
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["fields"]["result"], "dropped");
}

// ── fidelity lane (native-dialect pass-through) ──────────────────────────

fn raw_json(status: u16, body: &'static str) -> RawReply {
    raw_json_with(status, body, HeaderMap::new())
}

fn raw_json_with(status: u16, body: &'static str, headers: HeaderMap) -> RawReply {
    RawReply {
        status,
        content_type: Some(HeaderValue::from_static("application/json")),
        headers,
        frames: RawFrames::Buffered(bytes::Bytes::from_static(body.as_bytes())),
    }
}

#[tokio::test]
async fn fidelity_lane_keeps_tool_bodies_the_fold_must_reject() {
    // The lane's entire purpose: an Anthropic request carrying tools and
    // tool_result blocks — which the text-only fold 400s on — reaches a
    // native vendor untouched, and the vendor's reply comes back intact.
    let vendor_body = r#"{"type":"message","content":[{"type":"tool_use","id":"tu_1","name":"get_weather","input":{"city":"SF"}}],"stop_reason":"tool_use"}"#;
    let scripted = Arc::new(
        Scripted::new()
            .native()
            .with_raws(vec![Ok(raw_json(200, vendor_body))]),
    );
    let (status, body, _) = send(
        pipeline(scripted.clone(), test_cfg()),
        "/v1/messages",
        serde_json::json!({
            "model": "claude-x", "max_tokens": 64,
            "tools": [{"name": "get_weather"}],
            "messages": [
                {"role": "assistant", "content": [{"type": "tool_use", "id": "tu_1", "name": "get_weather", "input": {}}]},
                {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "tu_1", "content": "sunny"}]}
            ]
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["content"][0]["type"], "tool_use",
        "lane preserves tool blocks"
    );
    assert_eq!(body["stop_reason"], "tool_use");
    assert_eq!(
        scripted.attempts(),
        1,
        "lane made exactly one provider call"
    );
}

#[tokio::test]
async fn fidelity_lane_error_reply_passes_through_untouched() {
    // The vendor's own dialect-shaped envelope IS the client's answer; our
    // renderer never touches it (the 4xx-passthrough rule at byte level).
    // Two requests pin the x-request-id PRECEDENCE on hand-built lane
    // responses: absent from the vendor, the middleware's stamp lands;
    // supplied by the vendor, its id wins (relayable_headers allowlist +
    // stamp-only-if-absent) — so a lane client may quote an id this
    // proxy's own logs never minted. By design, loudly.
    let vendor_err =
        r#"{"type":"error","error":{"type":"rate_limit_error","message":"vendor says slow"}}"#;
    let mut vendor_headers = HeaderMap::new();
    vendor_headers.insert("x-request-id", HeaderValue::from_static("vendor-ticket-1"));
    let scripted = Arc::new(Scripted::new().native().with_raws(vec![
        Ok(raw_json(429, vendor_err)),
        Ok(raw_json_with(429, vendor_err, vendor_headers)),
    ]));

    let resp = opencode2api_server::build_router(pipeline(scripted.clone(), test_cfg()))
        .oneshot(post(
            "/v1/messages",
            serde_json::json!({"model": "m", "max_tokens": 8, "messages": []}),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    let minted = resp
        .headers()
        .get("x-request-id")
        .expect("proxy stamps lane responses too")
        .clone();
    let bytes = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["error"]["message"], "vendor says slow");
    assert_eq!(
        text, vendor_err,
        "byte-for-byte vendor envelope, not our render"
    );

    let resp2 = opencode2api_server::build_router(pipeline(scripted.clone(), test_cfg()))
        .oneshot(post(
            "/v1/messages",
            serde_json::json!({"model": "m", "max_tokens": 8, "messages": []}),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(
        resp2.headers()["x-request-id"],
        HeaderValue::from_static("vendor-ticket-1"),
        "a vendor-supplied id must survive the lane — support tickets open with it"
    );
    assert_ne!(
        resp2.headers()["x-request-id"],
        minted,
        "vendor id replaces the proxy-minted one, not the reverse"
    );
}

#[tokio::test]
async fn fidelity_stream_pumps_vendor_bytes_without_touching_them() {
    // Fidelity means fidelity: no injected keepalives, no synthesized or
    // withheld terminators — even the post-[DONE] trailer the CHAT relay
    // swallows passes here, because judging "completion" is the VENDOR
    // dialect's semantics, not ours to parse.
    let frames: Vec<Result<bytes::Bytes, String>> = [
        "event: message_start\ndata: {\"type\":\"message_start\"}\n\n",
        "data: [DONE]\n\n",
        "data: {\"vendor_trailer\":true}\n\n",
    ]
    .into_iter()
    .map(|f| Ok(bytes::Bytes::from_static(f.as_bytes())))
    .collect();
    let scripted = Arc::new(Scripted::new().native().with_raws(vec![Ok(RawReply {
        status: 200,
        content_type: Some(HeaderValue::from_static("text/event-stream")),
        headers: HeaderMap::new(),
        frames: RawFrames::Sse(stream::iter(frames).boxed()),
    })]));
    let (_, _, text) = send(
        pipeline(scripted, test_cfg()),
        "/v1/messages",
        serde_json::json!({"model": "m", "max_tokens": 8, "stream": true, "messages": []}),
        None,
    )
    .await;
    assert!(text.contains("event: message_start"), "got {text:?}");
    assert!(
        text.contains("vendor_trailer"),
        "lane passes post-sentinel bytes: {text:?}"
    );
    assert!(
        !text.contains(": keepalive"),
        "no injected comments on the lane"
    );
}

#[tokio::test]
async fn fold_still_rejects_what_it_cannot_represent_when_not_native() {
    // Decision-order regression pin: without a native claim, a request the
    // fold cannot represent must hit its loud 400 — never a silent lane miss,
    // never a half-represented fold. Tools fold, user-turn media folds, so the
    // pin sits on a block nothing but the fidelity lane can carry.
    let scripted = Arc::new(Scripted::new());
    let (status, body, _) = send(
        pipeline(scripted.clone(), test_cfg()),
        "/v1/messages",
        serde_json::json!({"model":"m","max_tokens":8,"messages":[
            {"role":"user","content":[{"type":"search_result","content":[]}]}
        ]}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("search_result")),
        "got {body}"
    );
    assert_eq!(
        scripted.attempts(),
        0,
        "relay_raw must not even be attempted"
    );
}

/// The other half of that order: a shape the fold CAN represent reaches the
/// provider instead of 400ing. What the provider actually got is pinned at
/// the wire in the service crate's
/// `anthropic_media_reaches_the_upstream_as_image_and_file_parts`, where a body
/// really reaches a socket — this is the decision-order half, not a copy.
#[tokio::test]
async fn fold_carries_a_user_turn_image_to_the_provider() {
    let scripted = Arc::new(Scripted::new().with_completions(vec![Ok(completion("hi"))]));
    let (status, _, _) = send(
        pipeline(scripted.clone(), test_cfg()),
        "/v1/messages",
        serde_json::json!({"model":"m","max_tokens":8,"messages":[
            {"role":"user","content":[
                {"type":"text","text":"look"},
                {"type":"image","source":{
                    "type":"base64","media_type":"image/png","data":"AANA"
                }}
            ]}
        ]}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        scripted.attempts(),
        1,
        "the fold accepted it, so the provider ran"
    );
}

// ── readiness + usage accounting ─────────────────────────────────────────

/// A provider whose egress is gone. `/health` must still answer (the process
/// is fine and a restart would hide the outage); `/ready` must not.
struct Unready;

#[async_trait::async_trait]
impl Provider for Unready {
    fn name(&self) -> &str {
        "unready"
    }
    fn ready(&self) -> bool {
        false
    }
    async fn complete(
        &self,
        _req: &ChatRequest,
        _ctx: &CallContext<'_>,
    ) -> Result<Completion, ProviderError> {
        Err(ProviderError::unavailable("no egress"))
    }
    async fn stream(
        &self,
        _req: &ChatRequest,
        _ctx: &CallContext<'_>,
    ) -> Result<ChatStream, ProviderError> {
        Err(ProviderError::unavailable("no egress"))
    }
}

#[tokio::test]
async fn health_answers_while_ready_reports_the_egress_is_gone() {
    let unready = || Pipeline::new(Arc::new(Unready), Arc::new(test_cfg()), None);
    let (status, body) = get(unready(), "/ready").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("unready"), "got {body}");

    let (status, body) = get(unready(), "/health").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "health is about the process, not its upstream: {body}"
    );
}

#[tokio::test]
async fn a_healthy_provider_is_ready() {
    let (status, _) = get(pipeline(Arc::new(Scripted::new()), test_cfg()), "/ready").await;
    assert_eq!(status, StatusCode::OK);
}

/// Usage must ride the IR on both paths — a dropped count is invisible
/// loss, which is what these pin.
/// These pin that it reaches the metrics recorder — buffered from the
/// completion, streamed from whichever frame carried it.
/// Not `#[tokio::test]`: `with_local_recorder` installs a THREAD-LOCAL
/// recorder, so the work has to run on this thread — hence a current-thread
/// runtime driven inside the closure.
#[test]
fn token_usage_reaches_the_recorder_on_both_paths() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        rt.block_on(async {
            let scripted = Arc::new(
                Scripted::new()
                    .with_completions(vec![Ok(completion("hi"))])
                    .with_streams(vec![chunks_ok(vec![chunk("a"), final_chunk()])]),
            );
            // buffered: usage comes from the completion
            let _ = send(
                pipeline(scripted.clone(), test_cfg()),
                "/v1/chat/completions",
                serde_json::json!({"model":"m","messages":[]}),
                None,
            )
            .await;
            // streamed: usage arrives on the terminal chunk
            let _ = send(
                pipeline(scripted, test_cfg()),
                "/v1/messages",
                serde_json::json!({"model":"m","max_tokens":8,"stream":true,
                                   "messages":[{"role":"user","content":"x"}]}),
                None,
            )
            .await;
        });
    });

    let counters: Vec<(String, u64)> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter_map(|(key, _, _, value)| match value {
            metrics_util::debugging::DebugValue::Counter(v) => {
                Some((key.key().name().to_string(), v))
            }
            _ => None,
        })
        .filter(|(name, _)| name == "opencode2api_tokens_total")
        .collect();
    let total: u64 = counters.iter().map(|(_, v)| v).sum();
    assert!(
        counters.len() >= 4,
        "prompt+completion for each path: {counters:?}"
    );
    assert!(
        total > 0,
        "tokens counted, not just registered: {counters:?}"
    );
}

/// A 429 without `retry-after` turns a vendor's precise backoff into the
/// client's guess — and the lane was dropping it. Framing headers must NOT
/// come along: the proxy re-frames the body, so the vendor's `content-length`
/// would describe bytes the client is not receiving.
#[tokio::test]
async fn the_lane_forwards_backoff_headers_and_no_framing_ones() {
    let vendor = r#"{"type":"error","error":{"type":"rate_limit_error"}}"#;
    let mut headers = HeaderMap::new();
    headers.insert("retry-after", HeaderValue::from_static("42"));
    headers.insert(
        "anthropic-ratelimit-requests-remaining",
        HeaderValue::from_static("0"),
    );
    headers.insert("request-id", HeaderValue::from_static("req_abc"));
    // These must be filtered by the provider's allowlist before they ever
    // reach a RawReply; passing them here proves the response builder does
    // not reintroduce them either.
    headers.insert("content-length", HeaderValue::from_static("999999"));
    headers.insert("content-encoding", HeaderValue::from_static("gzip"));

    let scripted = Arc::new(Scripted::new().native().with_raws(vec![Ok(raw_json_with(
        429,
        vendor,
        opencode2api_kit::relayable_headers(&headers),
    ))]));
    let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
        .oneshot(post(
            "/v1/messages",
            serde_json::json!({"model":"m","max_tokens":8,"messages":[]}),
            None,
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    let h = resp.headers();
    assert_eq!(
        h.get("retry-after").unwrap(),
        "42",
        "the client can back off"
    );
    assert_eq!(
        h.get("anthropic-ratelimit-requests-remaining").unwrap(),
        "0"
    );
    assert_eq!(h.get("request-id").unwrap(), "req_abc");
    assert!(
        h.get("content-encoding").is_none(),
        "we did not send gzip: {h:?}"
    );
    assert_ne!(
        h.get("content-length").map(|v| v.to_str().unwrap()),
        Some("999999"),
        "the vendor's framing describes bytes we re-framed"
    );
}

/// One counter series: the metric name, its labels in sorted order, and the
/// recorded value. Sorting makes an assertion independent of label order, and
/// owning the name keeps a phase's filter from depending on the recorder.
type CounterRow = (String, Vec<(String, String)>, u64);

/// Every counter in one snapshot, flattened to owned rows. A snapshot can
/// only be consumed once, so a phase that asserts on two metric names takes
/// its rows here once and filters them in place.
fn counter_rows(snapshot: metrics_util::debugging::Snapshot) -> Vec<CounterRow> {
    snapshot
        .into_vec()
        .into_iter()
        .filter_map(|(key, _, _, value)| match value {
            metrics_util::debugging::DebugValue::Counter(n) => {
                let mut labels = key
                    .key()
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect::<Vec<_>>();
                labels.sort();
                Some((key.key().name().to_string(), labels, n))
            }
            _ => None,
        })
        .collect()
}
/// Stream counters distinguish committed outcomes. Media is pinned by two
/// independent recorders: the folded request must produce exactly one image
/// series, and a fresh recorder around the native fidelity request must
/// produce none at all — a net-only check would stay green if one broke while
/// the other started counting.
#[test]
fn recorder_separates_stream_outcomes_from_folded_media() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let image = serde_json::json!({
        "type": "image_url",
        "image_url": { "url": "https://example.test/a.png" }
    });

    let folded_recorder = metrics_util::debugging::DebuggingRecorder::new();
    let folded_snapshot = folded_recorder.snapshotter();
    let folded_rows = metrics::with_local_recorder(&folded_recorder, || {
        rt.block_on(async {
            let ok = Arc::new(
                Scripted::new().with_streams(vec![chunks_ok(vec![chunk("ok"), final_chunk()])]),
            );
            assert_eq!(stream_request(ok, false).await.0, StatusCode::OK);
            let failed = Arc::new(Scripted::new().with_streams(vec![StreamPlan::Chunks(vec![
                Ok(chunk("partial")),
                Err(ProviderError::bad_gateway("upstream failed")),
            ])]));
            assert_eq!(stream_request(failed, false).await.0, StatusCode::OK);
            let folded = pipeline(
                Arc::new(Scripted::new().with_completions(vec![Ok(completion("seen"))])),
                test_cfg(),
            );
            assert_eq!(
                send(
                    folded,
                    "/v1/chat/completions",
                    serde_json::json!({"model":"m","messages":[
                        {"role":"user","content":[image]}
                    ]}),
                    None,
                )
                .await
                .0,
                StatusCode::OK,
            );
        });
        counter_rows(folded_snapshot.snapshot())
    });
    let labelled = |name: &str| -> Vec<(Vec<(String, String)>, u64)> {
        folded_rows
            .iter()
            .filter(|(metric, _, _)| metric == name)
            .map(|(_, labels, value)| (labels.clone(), *value))
            .collect()
    };
    let streams = labelled("opencode2api_streams_total");
    let stream = |result: &str, n: u64| {
        (
            vec![
                ("endpoint".to_string(), "chat".to_string()),
                ("format".to_string(), "openai".to_string()),
                ("result".to_string(), result.to_string()),
            ],
            n,
        )
    };
    assert!(
        streams.contains(&stream("ok", 1)),
        "successful stream was not counted: {streams:?}",
    );
    assert!(
        streams.contains(&stream("failed", 1)),
        "failed stream was not counted: {streams:?}",
    );
    assert_eq!(
        labelled("opencode2api_media_total"),
        vec![(
            vec![
                ("endpoint".to_string(), "chat".to_string()),
                ("format".to_string(), "openai".to_string()),
                ("kind".to_string(), "image".to_string()),
            ],
            1,
        )],
        "the folded image must be the one counted media series",
    );

    let native_recorder = metrics_util::debugging::DebuggingRecorder::new();
    let native_snapshot = native_recorder.snapshotter();
    let native_rows = metrics::with_local_recorder(&native_recorder, || {
        rt.block_on(async {
            let fidelity = vec![Ok(bytes::Bytes::from_static(b"data: [DONE]\n\n"))];
            let native = pipeline(
                Arc::new(Scripted::new().native().with_raws(vec![Ok(RawReply {
                    status: 200,
                    content_type: Some(HeaderValue::from_static("text/event-stream")),
                    headers: HeaderMap::new(),
                    frames: RawFrames::Sse(stream::iter(fidelity).boxed()),
                })])),
                test_cfg(),
            );
            assert_eq!(
                send(
                    native,
                    "/v1/chat/completions",
                    serde_json::json!({"model":"m","messages":[
                        {"role":"user","content":[image]}
                    ],"stream":true}),
                    None,
                )
                .await
                .0,
                StatusCode::OK,
            );
        });
        counter_rows(native_snapshot.snapshot())
    });
    assert!(
        native_rows
            .iter()
            .all(|(name, _, _)| name != "opencode2api_media_total"),
        "the fidelity lane counted media it never folded",
    );
}

/// The chat relay forwards bytes without parsing them — but a proxy in front
/// of a metered API still owes its operator a token count, so the one field
/// it reads is `usage`, gated by a byte scan.
#[test]
fn the_verbatim_relay_still_counts_tokens() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        rt.block_on(async {
            let frames: Vec<Result<bytes::Bytes, String>> = [
                // include_usage puts a NULL usage on every chunk…
                "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}],\"usage\":null}\n\n",

                // …and the real numbers on the last one.
                "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":22}}\n\n",
                "data: [DONE]\n\n",
            ]
            .into_iter()
            .map(|f| Ok(bytes::Bytes::from_static(f.as_bytes())))
            .collect();
            let scripted = Arc::new(Scripted::new().with_relays(vec![frames]));
            let (_, _, text) = send(
                pipeline(scripted, test_cfg()),
                "/v1/chat/completions",
                serde_json::json!({"model":"m","messages":[],"stream":true}),
                None,
            )
            .await;
            assert!(text.ends_with("data: [DONE]\n\n"), "got {text:?}");
        });
    });

    let counted: u64 = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter_map(|(key, _, _, value)| match value {
            metrics_util::debugging::DebugValue::Counter(v)
                if key.key().name() == "opencode2api_tokens_total" =>
            {
                Some(v)
            }
            _ => None,
        })
        .sum();
    assert_eq!(counted, 33, "11 prompt + 22 completion, from the fast path");
}

/// A dialect this deployment does not serve must be ABSENT, not present and
/// refusing: a client reading a 404 envelope knows the surface is not there,
/// where a 400 or 501 reads as a broken endpoint.
#[tokio::test]
async fn an_unserved_dialect_is_not_routed_at_all() {
    let cfg = ServerConfig {
        dialects: vec!["chat".into()],
        ..test_cfg()
    };
    let scripted = Arc::new(Scripted::new().with_relays(vec![relay_frames()]));
    let p = pipeline(scripted.clone(), cfg);

    let (status, body, _) = send(
        p.clone(),
        "/v1/messages",
        serde_json::json!({"model":"m","max_tokens":8,"messages":[]}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body["error"]["message"].is_string(),
        "still an envelope, not axum's plain text: {body}"
    );
    assert_eq!(scripted.attempts(), 0, "nothing reached the provider");

    // …while the dialect it does serve is untouched.
    let (status, _, text) = send(
        p,
        "/v1/chat/completions",
        serde_json::json!({"model":"m","messages":[],"stream":true}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(text.contains("[DONE]"), "got {text:?}");
}

// ── Gemini dialect (route-level: proves the `{model}:{verb}` path param) ──

/// The brace-param route is the load-bearing thing a bridge unit test cannot
/// see: matchit 0.8 has no `:param` sigil, so a `:action` route would register
/// a LITERAL segment and every real request would fall through to a 404. These
/// tests POST the real path and assert the fold answers.
#[tokio::test]
async fn gemini_buffered_route_renders_generate_content() {
    let scripted = Arc::new(Scripted::new().with_completions(vec![Ok(completion("hi"))]));
    let (status, body, _) = send(
        pipeline(scripted, test_cfg()),
        "/v1beta/models/gemini-2.5-flash:generateContent",
        serde_json::json!({"contents": [{"role": "user", "parts": [{"text": "hi"}]}]}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "route + model-in-path must serve");
    assert_eq!(body["candidates"][0]["content"]["parts"][0]["text"], "hi");
    assert_eq!(body["candidates"][0]["finishReason"], "STOP");
    assert_eq!(body["usageMetadata"]["totalTokenCount"], 5);
}

#[tokio::test]
async fn gemini_stream_verb_has_no_chat_sentinel() {
    let scripted = Arc::new(Scripted::new().with_streams(vec![chunks_ok(vec![
        chunk("he"),
        chunk("llo"),
        final_chunk(),
    ])]));
    let (status, _, text) = send(
        pipeline(scripted, test_cfg()),
        "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse",
        serde_json::json!({"contents": [{"role": "user", "parts": [{"text": "hi"}]}]}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // `:streamGenerateContent` in the PATH turns on streaming; the body carries
    // no `stream` field, so only the verb-derived flag could have done it.
    assert!(
        text.contains("\"text\":\"he\"") && text.contains("\"text\":\"llo\""),
        "got {text:?}"
    );
    assert!(
        text.contains("\"finishReason\":\"STOP\""),
        "terminal frame must carry the reason: {text:?}"
    );
    assert!(
        !text.contains("[DONE]"),
        "gemini must not emit a chat sentinel: {text:?}"
    );
}

#[tokio::test]
async fn gemini_fold_rejection_renders_google_envelope() {
    let scripted = Arc::new(Scripted::new());
    let (status, body, _) = send(
        pipeline(scripted.clone(), test_cfg()),
        "/v1beta/models/gemini-2.5-flash:generateContent",
        serde_json::json!({"contents": [{"parts": [{"fileData": {"fileUri": "files/abc", "mimeType": "image/png"}}]}]}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["status"], "INVALID_ARGUMENT");
    assert_eq!(body["error"]["code"], 400);
    assert_eq!(
        scripted.attempts(),
        0,
        "the fold rejected before the provider"
    );
}

// ── the Gemini SURFACE: framing rule, credential spellings, discovery ───

/// One request with arbitrary headers — each dialect authenticates with a
/// different spelling, so the harness cannot hardcode `authorization`.
async fn send_with(
    p: Pipeline,
    uri: &'static str,
    body: serde_json::Value,
    headers: &[(&'static str, &'static str)],
) -> (StatusCode, serde_json::Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header(HeaderName::from_static("content-type"), "application/json");
    for (name, value) in headers {
        builder = builder.header(
            HeaderName::from_static(name),
            HeaderValue::from_static(value),
        );
    }
    let resp = opencode2api_server::build_router(p)
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let text = String::from_utf8_lossy(
        &axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap(),
    )
    .into_owned();
    (
        status,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::Null),
    )
}

fn key_cfg() -> ServerConfig {
    ServerConfig {
        client_api_key: Some("sekret".into()),
        ..test_cfg()
    }
}

/// The gate is ONE function, and every served dialect's documented credential
/// must reach it: a bearer for chat, `x-api-key` for Anthropic SDK defaults,
/// `x-goog-api-key` and `?key=` for Google's. A dialect shipping with one of
/// its two documented forms unusable is the bug this pins shut.
#[tokio::test]
async fn gate_accepts_each_dialects_own_credential_spelling() {
    let gemini = || serde_json::json!({"contents": [{"parts": [{"text": "hi"}]}]});
    /// (uri, body, headers) — one credential spelling per dialect.
    type Case = (
        &'static str,
        serde_json::Value,
        Vec<(&'static str, &'static str)>,
    );
    let cases: Vec<Case> = vec![
        (
            "/v1/chat/completions",
            serde_json::json!({"model": "m", "messages": []}),
            vec![("authorization", "Bearer sekret")],
        ),
        (
            "/v1/messages",
            serde_json::json!({"model": "m", "max_tokens": 8, "messages": []}),
            vec![("x-api-key", "sekret")],
        ),
        (
            "/v1beta/models/gemini-2.5-flash:generateContent",
            gemini(),
            vec![("x-goog-api-key", "sekret")],
        ),
        (
            // The URL form, with no credential header at all.
            "/v1beta/models/gemini-2.5-flash:generateContent?key=sekret",
            gemini(),
            vec![],
        ),
    ];
    for (uri, body, headers) in cases {
        let scripted = Arc::new(Scripted::new().with_completions(vec![Ok(completion("hi"))]));
        let (status, json) =
            send_with(pipeline(scripted, key_cfg()), uri, body, headers.as_slice()).await;
        assert_eq!(status, StatusCode::OK, "{uri} {headers:?} -> {json}");
    }
    // A wrong value in the same spelling is still refused — in the dialect.
    let (status, json) = send_with(
        pipeline(Arc::new(Scripted::new()), key_cfg()),
        "/v1beta/models/gemini-2.5-flash:generateContent",
        gemini(),
        &[("x-goog-api-key", "wrong")],
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(json["error"]["status"], "UNAUTHENTICATED");
}

/// `:streamGenerateContent` without `?alt=sse` is Google's JSON-ARRAY stream.
/// This proxy frames SSE, so such a request is REFUSED rather than answered
/// with bytes in a shape the client never asked for: the rejecting-bridge rule
/// applied to framing, not only to content.
#[tokio::test]
async fn gemini_stream_verb_requires_alt_sse() {
    for uri in [
        // The default a naive client actually hits: no `alt` at all.
        "/v1beta/models/gemini-2.5-flash:streamGenerateContent",
        // The array form, asked for explicitly.
        "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=json",
    ] {
        let scripted = Arc::new(
            Scripted::new().with_streams(vec![chunks_ok(vec![chunk("no"), final_chunk()])]),
        );
        let (status, json, _) = send(
            pipeline(scripted.clone(), test_cfg()),
            uri,
            serde_json::json!({"contents": [{"parts": [{"text": "hi"}]}]}),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} -> {json}");
        assert_eq!(json["error"]["status"], "INVALID_ARGUMENT", "{uri}");
        assert!(
            json["error"]["message"]
                .as_str()
                .is_some_and(|m| m.contains("alt=sse")),
            "{uri}: must say what to send, got {json}"
        );
        assert_eq!(scripted.attempts(), 0, "{uri}: refused before the provider");
    }
}

/// Order and payload both matter: the lane answers BEFORE the `alt` rule, and
/// it carries the inbound PATH — without which a native Gemini provider cannot
/// rebuild a vendor URL at all, since the model is not in the body. Refusing at
/// the lane would have this proxy editorialising bytes it never read.
#[tokio::test]
async fn fidelity_lane_carries_the_path_and_precedes_the_alt_rule() {
    let scripted = Arc::new(Scripted::new().native().with_raws(vec![Ok(raw_json(
        200,
        "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"native\"}]}}]}",
    ))]));
    let (status, json, _) = send(
        pipeline(scripted.clone(), test_cfg()),
        "/v1beta/models/gemini-pro:streamGenerateContent",
        serde_json::json!({"contents": [{"parts": [{"text": "hi"}]}]}),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "no alt=sse, yet the lane serves it: {json}"
    );
    assert_eq!(
        json["candidates"][0]["content"]["parts"][0]["text"],
        "native"
    );
    assert_eq!(
        scripted.lane_path().as_deref(),
        Some("/v1beta/models/gemini-pro:streamGenerateContent"),
        "the model-in-path must reach a native provider intact"
    );
}

/// A credential that does not match is refused IN THE GOOGLE ENVELOPE, on the
/// query form of the credential as much as the header form. `?key=` arrives in
/// the URL, so the value can be anything a client mangled; the dialect parser
/// hands it through rather than turning it into a proxy-side error the client
/// did not cause, and the gate answers the way it answers any wrong secret.
#[tokio::test]
async fn undecodable_query_credential_is_refused_in_the_google_envelope() {
    let gemini = serde_json::json!({"contents": [{"parts": [{"text": "hi"}]}]});
    for (uri, label) in [
        (
            "/v1beta/models/gemini-2.5-flash:generateContent?key=ab%zz",
            "bad escape",
        ),
        // Lossy-decoded, still simply not the configured secret.
        (
            "/v1beta/models/gemini-2.5-flash:generateContent?key=%FF%FE",
            "invalid utf-8",
        ),
        // A duplicate resolves to the FIRST mention, so a later `key=` cannot
        // authorise what its predecessor denied — nor veto what it accepted. A
        // `HashMap` would have kept the LAST value and flipped this result.
        (
            "/v1beta/models/gemini-2.5-flash:generateContent?key=wrong&key=sekret",
            "dup keys",
        ),
    ] {
        // The handle is held so the claim "rejected at the gate" is checked,
        // not assumed: an empty script queue would turn a round trip into a
        // 500 `script exhausted`, which a status-only assertion cannot tell
        // apart from a clean refusal that never reached the provider.
        let scripted = Arc::new(Scripted::new());
        let (status, json) = send_with(
            pipeline(scripted.clone(), key_cfg()),
            uri,
            gemini.clone(),
            &[],
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{label}: {json}");
        assert_eq!(json["error"]["status"], "UNAUTHENTICATED", "{label}");
        assert_eq!(json["error"]["code"], 401, "{label}");
        assert_eq!(
            scripted.attempts(),
            0,
            "{label}: the gate refused, the provider was never asked"
        );
    }
    // And with no key configured at all, a query with a valueless pair is
    // HANDLED — the bare `?alt` reads as absent, so the framing rule answers in
    // the dialect rather than as a parse error.
    let scripted = Arc::new(Scripted::new());
    let (status, json, _) = send(
        pipeline(scripted.clone(), test_cfg()),
        "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt",
        gemini,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert_eq!(json["error"]["status"], "INVALID_ARGUMENT");
    assert_eq!(scripted.attempts(), 0, "refused before the provider");
    assert!(
        json["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("alt=sse")),
        "{}",
        json["error"]["message"]
    );
}

/// Discovery is how a Gemini client picks a model, so `listModels` is served
/// from the same `Provider::models()` the OpenAI route renders.
#[tokio::test]
async fn gemini_models_listing_renders_resource_names() {
    let list = serde_json::json!({"object": "list", "data": [
        {"id": "gemini-2.5-flash", "object": "model"},
        {"id": "gemini-2.5-pro", "object": "model"},
    ]});
    let (status, body) = get(
        pipeline(Arc::new(Scripted::new().with_models(Ok(list))), test_cfg()),
        "/v1beta/models",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["models"][0]["name"], "models/gemini-2.5-flash");
    assert_eq!(json["models"][0]["displayName"], "gemini-2.5-flash");
    assert_eq!(
        json["models"][0]["supportedGenerationMethods"]
            .as_array()
            .map(Vec::len),
        Some(2),
        "both verbs this proxy serves"
    );
    assert!(
        json["models"][0].get("inputTokenLimit").is_none(),
        "a discovery answer the vendor never gave is a lie about limits"
    );
}

/// `getModel` takes either spelling a client may hold — the resource name the
/// listing returned, or the bare id from the URL template — and a miss is a
/// loud 404, because this dialect has no "not found, here is nothing" answer.
#[tokio::test]
async fn gemini_model_get_resolves_both_spellings_and_404s_loudly() {
    let list = serde_json::json!({"object": "list", "data": [
        {"id": "gemini-2.5-flash", "object": "model"}
    ]});
    for uri in [
        "/v1beta/models/gemini-2.5-flash",
        "/v1beta/models/models/gemini-2.5-flash",
    ] {
        let (status, body) = get(
            pipeline(
                Arc::new(Scripted::new().with_models(Ok(list.clone()))),
                test_cfg(),
            ),
            uri,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{uri} -> {body}");
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["name"], "models/gemini-2.5-flash", "{uri}");
    }
    let (status, body) = get(
        pipeline(Arc::new(Scripted::new().with_models(Ok(list))), test_cfg()),
        "/v1beta/models/gemini-nope",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["error"]["status"], "NOT_FOUND");
}

/// `:generateContent` is a POST verb, and the wildcard route means a `GET` of
/// it now reaches this handler at all. Answering 404 "not in this proxy's
/// catalogue" would send an operator hunting a model id that was never the
/// problem, so a method confusion says so.
#[tokio::test]
async fn get_on_a_post_verb_says_method_not_missing_model() {
    let shared = captured();
    let req = Request::builder()
        .method("GET")
        .uri("/v1beta/models/gemini-2.5-flash:generateContent")
        .header("x-request-id", "74010")
        .body(Body::empty())
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(Arc::new(Scripted::new()), test_cfg()))
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error"]["status"], "UNIMPLEMENTED");
    assert_eq!(body["error"]["code"], 405);
    let lines = terminal_lines(&shared, "74010");
    assert_eq!(
        lines.len(),
        1,
        "one terminal record for the method rejection"
    );
    assert_eq!(lines[0]["fields"]["endpoint"], "generate-content");
    assert_eq!(lines[0]["fields"]["status"], 405);
}

#[tokio::test]
async fn same_path_records_one_endpoint_from_handler_and_from_router() {
    let shared = captured();
    let panic_pipeline = pipeline(
        Arc::new(Scripted::new().with_streams(vec![StreamPlan::Panic])),
        test_cfg(),
    );
    let req = Request::builder()
        .method("POST")
        .uri("/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse")
        .header("x-request-id", "74011")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"stream":true}"#,
        ))
        .unwrap();
    let resp = opencode2api_server::build_router(panic_pipeline)
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let _ = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/v1beta/models/gemini-2.5-flash:streamGenerateContent")
        .header("x-request-id", "74012")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"stream":true}"#,
        ))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(Arc::new(Scripted::new()), test_cfg()))
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let _ = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();

    let handler = terminal_lines(&shared, "74011");
    let refused = terminal_lines(&shared, "74012");
    assert_eq!(handler.len(), 1);
    assert_eq!(refused.len(), 1);
    assert_eq!(handler[0]["fields"]["endpoint"], "generate-content");
    assert_eq!(
        refused[0]["fields"]["endpoint"],
        handler[0]["fields"]["endpoint"]
    );
}

/// A provider with no catalogue still answers the route — in the dialect's own
/// envelope, as UNIMPLEMENTED rather than the OpenAI 501 `/v1/models` gives.
#[tokio::test]
async fn gemini_models_capability_gap_renders_in_dialect() {
    let (status, body) = get(
        pipeline(Arc::new(Scripted::new()), test_cfg()),
        "/v1beta/models",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["error"]["status"], "UNIMPLEMENTED");
    assert_eq!(json["error"]["code"], 501);
}

/// The read surface belongs to the dialect, so `server.dialects` removes it
/// too: absent, 404 like any unknown path — not registered-and-refusing.
#[tokio::test]
async fn gemini_surface_is_absent_when_the_dialect_is_not_served() {
    let cfg = ServerConfig {
        dialects: vec!["chat".into()],
        ..test_cfg()
    };
    let (status, body) = get(
        pipeline(Arc::new(Scripted::new()), cfg.clone()),
        "/v1beta/models",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "listing must be absent: {body}"
    );
    let (status, json, _) = send(
        pipeline(Arc::new(Scripted::new()), cfg),
        "/v1beta/models/gemini-2.5-flash:generateContent",
        serde_json::json!({"contents": [{"parts": [{"text": "hi"}]}]}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "generation too: {json}");
}

/// The id is DERIVED, not minted over. A client that already runs a
/// correlation spine gets its own number back — and because the request span is
/// built from the same derivation, that number is what its log lines carry. A
/// client that sent a UUID (or nothing) gets a fresh numeric one, since this
/// proxy keys on an integer.
#[tokio::test]
async fn the_response_echoes_a_numeric_id_and_mints_for_anything_else() {
    let p = pipeline(Arc::new(Scripted::new()), test_cfg());
    let ask = |id: &'static str| {
        let router = opencode2api_server::build_router(p.clone());
        async move {
            router
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri("/nope")
                        .header("x-request-id", id)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
        }
    };

    let echoed = ask("4242").await;
    assert_eq!(echoed.headers()["x-request-id"], "4242");

    let minted = ask("8f2c-uuid-from-the-edge-proxy").await;
    let value = minted.headers()["x-request-id"]
        .to_str()
        .expect("ascii id header")
        .to_string();
    let fresh = value
        .parse::<u64>()
        .unwrap_or_else(|e| panic!("a minted id is numeric, got {value:?}: {e}"));
    assert_ne!(
        fresh, 0,
        "a zero id would collapse every such request onto one key"
    );
}

/// An unmatched path was the one client-visible refusal with no record
/// ANYWHERE: `/nope` and a wrong method answered in-envelope and touched
/// neither `opencode2api_requests_total` nor the log, so a scanner — or a client
/// pointed at a dialect this deployment deliberately does not serve, which is
/// exactly how the gate makes a surface 404 — read as a broken router.
#[test]
fn unmatched_paths_are_counted_like_refusals() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        rt.block_on(async {
            let p = pipeline(Arc::new(Scripted::new()), test_cfg());
            let (missing, _) = get(p.clone(), "/nope").await;
            assert_eq!(missing, StatusCode::NOT_FOUND);
            let resp = opencode2api_server::build_router(p)
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri("/v1/chat/completions")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        });
    });

    let counters: Vec<String> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter_map(|(key, _, _, value)| match value {
            metrics_util::debugging::DebugValue::Counter(n)
                if key.key().name() == "opencode2api_requests_total" =>
            {
                let labels = key
                    .key()
                    .labels()
                    .map(|l| format!("{}={}", l.key(), l.value()))
                    .collect::<Vec<_>>()
                    .join(",");
                Some(format!("{labels} {n}"))
            }
            _ => None,
        })
        .collect();
    assert!(
        counters
            .iter()
            .any(|c| c.contains("endpoint=unmatched") && c.contains("status=404")),
        "the 404 left no trace: {counters:?}"
    );
    assert!(
        counters
            .iter()
            .any(|c| c.contains("endpoint=unmatched") && c.contains("status=405")),
        "the 405 left no trace: {counters:?}"
    );
}

// ── the terminal record, read end to end ──────────────────────────

/// One shared json-line buffer for the whole binary. A tracing dispatcher is
/// process-global and cannot be unset, so this installs ONCE and each case then
/// filters by the `x-request-id` it sent — which is exactly how an operator
/// greps, and what makes a shared buffer safe under parallel tests.
static LINES: std::sync::OnceLock<Arc<Mutex<Vec<u8>>>> = std::sync::OnceLock::new();

#[derive(Clone)]
struct Sink(Arc<Mutex<Vec<u8>>>);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Sink {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}

impl std::io::Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn captured() -> Arc<Mutex<Vec<u8>>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    let shared = LINES
        .get_or_init(|| Arc::new(Mutex::new(Vec::new())))
        .clone();
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let layer = tracing_subscriber::fmt::layer()
            .json()
            .with_span_list(false)
            .with_writer(Sink(shared.clone()));
        let sub = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("info"))
            .with(layer);
        let _ = tracing::subscriber::set_global_default(sub);
    });
    shared
}

fn terminal_lines(shared: &Arc<Mutex<Vec<u8>>>, id: &str) -> Vec<serde_json::Value> {
    let needle = format!("\"request_id\":{id}");
    String::from_utf8_lossy(&shared.lock())
        .lines()
        .filter(|l| l.contains(&needle))
        .filter_map(|l| serde_json::from_str(l).ok())
        .filter(|v: &serde_json::Value| v["fields"]["message"] == "request completed")
        .collect()
}

/// The invariant the whole spine exists to deliver, asserted at the one seam
/// that can prove it: the `x-request-id` the caller gets back IS the
/// `request_id` on that request's terminal line, written exactly once, with the
/// outcome the scripted provider produced. It doubles as the guard against a
/// second terminal line (the old `"stream ended"` record surviving next to the
/// unified one) — a missing line and a duplicated one are both caught by the
/// count, which a `contains` check cannot tell apart.
///
/// It also pins why `format` stays on the line: both listing routes are recorded
/// under `endpoint="models"`, and only the dialect separates a Gemini discovery
/// probe from an OpenAI one.
#[tokio::test]
async fn the_client_id_is_the_log_key_and_the_record_is_written_once() {
    let shared = captured();
    for (id, path, want_format) in [
        ("71001", "/v1/models", "openai"),
        ("71002", "/v1beta/models", "gemini"),
    ] {
        let scripted = Arc::new(
            Scripted::new().with_models(Ok(serde_json::json!({"object":"list","data":[]}))),
        );
        let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(path)
                    .header("x-request-id", id)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let echoed = resp.headers()["x-request-id"].to_str().unwrap().to_string();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 16)
            .await
            .unwrap();
        assert!(!body.is_empty(), "the listing body vanished");

        let lines = terminal_lines(&shared, id);
        assert_eq!(
            lines.len(),
            1,
            "exactly one terminal record per request (echoed {echoed})"
        );
        let fields = &lines[0]["fields"];
        assert_eq!(fields["endpoint"], "models");
        assert_eq!(
            fields["format"], want_format,
            "the two listing routes must not collapse into one row"
        );
        assert_eq!(fields["result"], "ok");
        assert_eq!(fields["status"], 200);
        assert_eq!(echoed, id, "the caller's id is the log key, both sides");
    }
}

/// The unit test around `Done` proves the field RENDERS; only an end-to-end run
/// proves the retry loop POPULATES it. That distinction is the difference between
/// a field that answers "why did this take 4 s" and a field that is always absent
/// — and the always-absent kind still passes every other test in the file, which
/// is what makes it the worst kind of instrument to ship.
#[tokio::test]
async fn the_terminal_record_reports_the_attempts_the_retry_loop_made() {
    let shared = captured();
    let scripted = Arc::new(Scripted::new().with_completions(vec![
        Err(ProviderError::rate_limited("slow down")),
        Err(ProviderError::bad_gateway("flaky")),
        Ok(completion("third time")),
    ]));
    let body = serde_json::json!({"model": "retried-buffered", "messages": []});
    let resp = opencode2api_server::build_router(pipeline(scripted.clone(), test_cfg()))
        .oneshot(post("/v1/chat/completions", body.clone(), Some("Bearer k")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // Drain the body: the terminal record is emitted on the buffered path before
    // the response is returned, but reading the id back from the response is what
    // ties this request to its line without guessing.
    let id = resp.headers()["x-request-id"].to_str().unwrap().to_string();
    let _ = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();

    let lines = terminal_lines(&shared, &id);
    assert_eq!(lines.len(), 1, "one terminal record for {id}");
    let fields = &lines[0]["fields"];
    assert_eq!(fields["endpoint"], "chat");
    assert_eq!(fields["result"], "ok");
    // Two extra sends after the first attempt, and the scripted provider's own
    // counter is the ground truth — not a literal I could get wrong twice.
    assert_eq!(
        fields["retries"],
        scripted.attempts() - 1,
        "the record must agree with what the mock counted"
    );
    assert_eq!(fields["model"], "retried-buffered");
}

/// The mirror case: an unretried request must NOT grow a `retries` key. A field
/// that always prints (`retries=0`) reads as "measured, and the answer is none"
/// on a lane that never looked, and it spends 12 bytes on every line of traffic
/// to say something that is true of almost all of them.
#[tokio::test]
async fn a_single_attempt_request_omits_the_retries_key_entirely() {
    let shared = captured();
    let scripted = Arc::new(Scripted::new().with_completions(vec![Ok(completion("first"))]));
    let resp = opencode2api_server::build_router(pipeline(scripted.clone(), test_cfg()))
        .oneshot(post(
            "/v1/chat/completions",
            serde_json::json!({"model": "m", "messages": []}),
            Some("Bearer k"),
        ))
        .await
        .unwrap();
    let id = resp.headers()["x-request-id"].to_str().unwrap().to_string();
    assert_eq!(resp.status(), StatusCode::OK);
    let _ = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();

    assert_eq!(scripted.attempts(), 1, "the loop ran once");
    let fields = &terminal_lines(&shared, &id)[0]["fields"];
    assert_eq!(fields["result"], "ok");
    assert!(
        !fields.as_object().unwrap().contains_key("retries"),
        "an unretried request grew a measurement it never took: {fields}"
    );
    assert!(
        !fields.as_object().unwrap().contains_key("queued_ms"),
        "and no admission wait happened either: {fields}"
    );
}

/// `queued_ms` answers "was that our queue or their vendor", and admission only
/// engages when concurrency exceeds the ceiling — which never happens at the
/// default `max_inflight`. So the field is proven here by squeezing the ceiling
/// to one and holding the first request: the second must report a wait that is
/// genuinely time spent in OUR queue, not upstream time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_that_waited_for_a_slot_reports_the_wait() {
    // ONE number for the vendor hold, so the assertion at the end is expressed
    // as a fraction of the quantity it bounds instead of a pair of magic
    // milliseconds that silently drift the moment someone tunes the hold.
    const HOLD_MS: u64 = 300;
    let shared = captured();
    let mut cfg = test_cfg();
    cfg.max_inflight = 1;
    cfg.admission_wait_secs = 5;
    let scripted = Arc::new(
        Scripted::new()
            .with_hold(HOLD_MS as usize)
            .with_completions(vec![Ok(completion("first")), Ok(completion("second"))]),
    );
    let body = serde_json::json!({"model": "queued", "messages": []});
    // Two ROUTERS over one PIPELINE: `Router` is neither `Clone` nor `Copy`, and
    // the ceiling lives in the pipeline's `Arc<Semaphore>` — so cloning the
    // pipeline (not calling `pipeline()` twice, which mints a fresh permit set
    // and made this test prove nothing) is what puts both requests in the same
    // queue.
    let queue = pipeline(scripted, cfg);
    let first = opencode2api_server::build_router(queue.clone());
    let second = opencode2api_server::build_router(queue);
    let (a, b) = tokio::join!(
        first.oneshot(post("/v1/chat/completions", body.clone(), Some("Bearer k"))),
        second.oneshot(post("/v1/chat/completions", body, Some("Bearer k"))),
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.status(), StatusCode::OK);
    assert_eq!(b.status(), StatusCode::OK);
    let ids = [
        a.headers()["x-request-id"].to_str().unwrap().to_string(),
        b.headers()["x-request-id"].to_str().unwrap().to_string(),
    ];

    let mut waits = Vec::new();
    for id in ids {
        let lines = terminal_lines(&shared, &id);
        assert_eq!(lines.len(), 1, "one record for {id}");
        let fields = &lines[0]["fields"];
        assert_eq!(fields["result"], "ok");
        // Exactly one of the two had to wait for the other's permit.
        let queued = match fields.get("queued_ms") {
            Some(v) => v.as_u64().expect("queued_ms is a number"),
            None => 0,
        };
        waits.push(queued);
        // The relationship, not just the value: the wait is a COMPONENT of the
        // total, so a `queued_ms` larger than `duration_ms` would mean one of the
        // two is being stamped at the wrong moment (the failure mode is real —
        // re-deriving the wait from the pre-gate `started` at record time would
        // fold the request's own vendor hold into it and the field would silently
        // measure something else while still looking plausible).
        let total = fields["duration_ms"].as_u64().expect("duration_ms");
        assert!(
            queued <= total,
            "wait {queued} exceeds the whole request {total}: {fields:?}"
        );
        if queued > 0 {
            assert!(
                total > queued,
                "a queued request whose total equals its wait never reached the provider: {fields:?}"
            );
        }
    }
    waits.sort_unstable();
    // Ascending: index 0 is the request that walked straight in, index 1 the one
    // that queued behind it. Both bounds are written FROM the hold so tuning it
    // cannot silently invalidate them. The lower (H/3) proves the loser really
    // waited. The upper (2H) keeps the quantity honest: `queued_ms` must be OUR
    // queue, and the structural pair above (`queued <= total`, `total > queued`
    // whenever it queued) is what pins that relationship — this bound is its
    // backstop, not its replacement. It is deliberately the same 2x the test has
    // always carried: a real-time sleep on a shared runner needs the headroom,
    // and tightening it here would trade a flake-free gate for a sharper number.
    assert_eq!(waits[0], 0, "one request must never have queued");
    assert!(
        (HOLD_MS / 3..=HOLD_MS * 2).contains(&waits[1]),
        "the queued request must report the admission wait it paid, and only that: {waits:?}"
    );
}

#[tokio::test]
async fn large_valid_json_reaches_provider_under_16_mib_limit() {
    let mut cfg = test_cfg();
    cfg.max_body_bytes = 16 * 1024 * 1024;
    let scripted = Arc::new(Scripted::new().with_completions(vec![Ok(completion("ok"))]));
    let body = serde_json::json!({
        "model": "large",
        "messages": [{"role": "user", "content": "x".repeat(3 * 1024 * 1024)}]
    });
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted.clone(), cfg))
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        scripted.attempts(),
        1,
        "large valid body must reach provider"
    );
    let bytes = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "ok");
}

#[tokio::test]
async fn oversized_json_returns_dialect_413_and_one_terminal_record() {
    let cfg = test_cfg();
    let shared = captured();
    let scripted = Arc::new(Scripted::new().with_completions(vec![Ok(completion("must not run"))]));
    let body = serde_json::json!({
        "model": "large",
        "messages": [{"role": "user", "content": "x".repeat(17 * 1024 * 1024)}]
    });
    let req = Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted.clone(), cfg))
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(scripted.attempts(), 0);
    let id = resp.headers()["x-request-id"].to_str().unwrap().to_string();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let error: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(error["error"]["type"], "request_too_large");
    let lines = terminal_lines(&shared, &id);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["fields"]["result"], "failed");
    assert_eq!(lines[0]["fields"]["endpoint"], "messages");
    assert_eq!(lines[0]["fields"]["status"], 413);
}

#[tokio::test]
async fn delete_messages_uses_anthropic_error_shape() {
    let scripted = Arc::new(Scripted::new());
    let req = Request::builder()
        .method("DELETE")
        .uri("/v1/messages")
        .body(Body::empty())
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["type"], "error");
    assert_eq!(body["error"]["type"], "api_error");
}

#[tokio::test]
async fn wrong_gemini_method_uses_google_status_shape() {
    let scripted = Arc::new(Scripted::new());
    let req = Request::builder()
        .method("PUT")
        .uri("/v1beta/models/gemini-2.5-flash:generateContent")
        .body(Body::empty())
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error"]["code"], 405);
    assert_eq!(body["error"]["status"], "UNIMPLEMENTED");
}

#[tokio::test]
async fn provider_completion_panic_returns_request_id_and_one_panicked_record() {
    let shared = captured();
    let scripted = Arc::new(Scripted::new().with_completion_panic());
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("x-request-id", "72001")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"model":"panic","messages":[]}"#))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(resp.headers()["x-request-id"], "72001");
    let _ = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let lines = terminal_lines(&shared, "72001");
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["fields"]["result"], "panicked");
    assert_eq!(lines[0]["fields"]["endpoint"], "chat");
    assert_eq!(lines[0]["fields"]["status"], 500);
}

#[tokio::test]
async fn provider_stream_panic_returns_request_id_and_one_panicked_record() {
    let shared = captured();
    let scripted = Arc::new(Scripted::new().with_streams(vec![StreamPlan::Panic]));
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("x-request-id", "72002")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"model":"panic","messages":[],"stream":true}"#,
        ))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
        .oneshot(req)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(resp.headers()["x-request-id"], "72002");
    let _ = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let lines = terminal_lines(&shared, "72002");
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["fields"]["result"], "panicked");
    assert_eq!(lines[0]["fields"]["endpoint"], "chat");
    assert_eq!(lines[0]["fields"]["status"], 500);
}

#[tokio::test]
async fn provider_413_envelope_survives_normalization_and_logs_once() {
    let shared = captured();
    let envelope = serde_json::json!({
        "error": {
            "message": "distinctive scripted payload-too-large",
            "type": "request_too_large",
            "code": "scripted_body_limit"
        }
    });
    let err = ProviderError::from_upstream(
        "scripted-vendor",
        413,
        serde_json::to_vec(&envelope).unwrap().as_slice(),
    );
    let scripted = Arc::new(Scripted::new().with_completions(vec![Err(err)]));
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("x-request-id", "73001")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"model":"m","messages":[]}"#))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
        .oneshot(req)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error"], envelope["error"]);
    let lines = terminal_lines(&shared, "73001");
    assert_eq!(lines.len(), 1, "one terminal record for the 413 response");
}

#[tokio::test]
async fn native_fidelity_text_plain_413_is_preserved_and_logged_once() {
    let shared = captured();
    let vendor_body = bytes::Bytes::from_static(b"vendor-body-too-large");
    let scripted = Arc::new(Scripted::new().native().with_raws(vec![Ok(RawReply {
        status: 413,
        content_type: Some(HeaderValue::from_static("text/plain")),
        headers: HeaderMap::new(),
        frames: RawFrames::Buffered(vendor_body.clone()),
    })]));
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("x-request-id", "73005")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"model":"m","messages":[]}"#))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
        .oneshot(req)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    assert_eq!(body, vendor_body);
    assert_eq!(terminal_lines(&shared, "73005").len(), 1);
}

#[tokio::test]
async fn native_fidelity_sse_413_preserves_bytes_and_logs_once() {
    let shared = captured();
    let vendor_body = concat!(
        "event: error\n",
        "data: {\"error\":{\"message\":\"vendor body too large\"}}\n\n",
        "data: vendor-trailer-without-normalization\n\n",
    );
    let frames = vec![Ok(bytes::Bytes::from_static(vendor_body.as_bytes()))];
    let scripted = Arc::new(Scripted::new().native().with_raws(vec![Ok(RawReply {
        status: 413,
        content_type: Some(HeaderValue::from_static("text/event-stream")),
        headers: HeaderMap::new(),
        frames: RawFrames::Sse(stream::iter(frames).boxed()),
    })]));
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("x-request-id", "73006")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"model":"m","messages":[],"stream":true}"#))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
        .oneshot(req)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), vendor_body.as_bytes());
    let lines = terminal_lines(&shared, "73006");
    assert_eq!(lines.len(), 1, "one terminal record for the 413 response");
    assert_eq!(lines[0]["fields"]["status"], 413);
}

#[tokio::test]
async fn request_body_limit_unknown_path_uses_openai_chat_error() {
    let mut cfg = test_cfg();
    cfg.max_body_bytes = 256;
    let scripted = Arc::new(Scripted::new());
    // The outer RBL rejects this declared oversize before routing; its plain 413
    // must pass through the unknown-route fallback and become the OpenAI chat error.
    let req = Request::builder()
        .method("POST")
        .uri("/v1beta/models-not-real")
        .header("content-type", "application/json")
        .header("content-length", "257")
        .body(Body::from(vec![b'x'; 257]))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted, cfg))
        .oneshot(req)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    let error: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(error["error"]["type"], "api_error");
    assert!(error["error"].get("status").is_none());
}

#[tokio::test]
async fn oversized_responses_request_records_responses_format() {
    let shared = captured();
    let mut cfg = test_cfg();
    cfg.max_body_bytes = 256;
    let scripted = Arc::new(Scripted::new());
    let body = serde_json::json!({
        "model": "m",
        "input": "x".repeat(257)
    });
    let req = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("x-request-id", "73003")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted, cfg))
        .oneshot(req)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let _ = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    let lines = terminal_lines(&shared, "73003");
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["fields"]["format"], "responses");
}

#[tokio::test]
async fn provider_models_panic_returns_request_id_and_models_record() {
    let shared = captured();
    let scripted = Arc::new(Scripted::new().with_models_panic());
    let req = Request::builder()
        .method("GET")
        .uri("/v1/models")
        .header("x-request-id", "73004")
        .body(Body::empty())
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted, test_cfg()))
        .oneshot(req)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(resp.headers()["x-request-id"], "73004");
    let _ = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    let lines = terminal_lines(&shared, "73004");
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["fields"]["result"], "panicked");
    assert_eq!(lines[0]["fields"]["endpoint"], "models");
    assert_eq!(lines[0]["fields"]["status"], 500);
}

#[tokio::test]
async fn gemini_missing_verb_is_rejected_with_one_terminal_record() {
    let shared = captured();
    let scripted = Arc::new(Scripted::new());
    let req = Request::builder()
        .method("POST")
        .uri("/v1beta/models/gemini-2.5-flash")
        .header("x-request-id", "74002")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"contents":[]}"#))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted.clone(), test_cfg()))
        .oneshot(req)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(resp.headers()["x-request-id"], "74002");
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error"]["code"], 400);
    assert_eq!(body["error"]["status"], "INVALID_ARGUMENT");
    assert_eq!(
        scripted.attempts(),
        0,
        "rejection must precede the provider"
    );

    let lines = terminal_lines(&shared, "74002");
    assert_eq!(lines.len(), 1, "one terminal record for the rejection");
    let fields = &lines[0]["fields"];
    assert_eq!(fields["result"], "rejected");
    assert_eq!(fields["status"], 400);
    assert_eq!(fields["format"], "gemini");
    assert_eq!(fields["endpoint"], "models");
}

#[tokio::test]
async fn stream_provider_delay_before_handoff_times_out_once() {
    let shared = captured();
    let scripted = Arc::new(
        Scripted::new()
            .with_streams(vec![StreamPlan::PreHandoff(ProviderError::rate_limited(
                "retry me",
            ))])
            .with_stream_plan_delay(1_100),
    );
    let mut cfg = test_cfg();
    cfg.request_timeout_secs = 1;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("x-request-id", "74003")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"model":"slow","messages":[],"stream":true}"#,
        ))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted.clone(), cfg))
        .oneshot(req)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(resp.headers()["x-request-id"], "74003");
    let _ = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    assert_eq!(
        scripted.attempts(),
        2,
        "the first retryable attempt and the blocking retry must both run"
    );
    let lines = terminal_lines(&shared, "74003");
    assert_eq!(lines.len(), 1, "one terminal record for the timeout");
    let fields = &lines[0]["fields"];
    assert_eq!(fields["result"], "timeout");
    assert_eq!(fields["status"], 504);
    assert_eq!(fields["format"], "openai");
}

#[tokio::test]
async fn lane_relay_delay_before_handoff_times_out_once() {
    let shared = captured();
    let scripted = Arc::new(
        Scripted::new()
            .native()
            .with_relay_delay(100)
            .with_raws(vec![Ok(raw_json(200, "{}"))]),
    );
    let mut cfg = test_cfg();
    cfg.request_timeout_secs = 0;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("x-request-id", "74004")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"model":"slow","messages":[],"stream":false}"#,
        ))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted.clone(), cfg))
        .oneshot(req)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(resp.headers()["x-request-id"], "74004");
    let _ = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    assert_eq!(
        scripted.attempts(),
        1,
        "the fidelity lane must make one provider attempt"
    );
    let lines = terminal_lines(&shared, "74004");
    assert_eq!(lines.len(), 1, "one terminal record for the timeout");
    let fields = &lines[0]["fields"];
    assert_eq!(fields["result"], "timeout");
    assert_eq!(fields["status"], 504);
    assert_eq!(fields["format"], "openai");
    assert_eq!(fields["endpoint"], "chat");
    assert!(
        !fields.as_object().unwrap().contains_key("retries"),
        "single-attempt timeout must omit the retries key entirely"
    );
}

#[tokio::test]
async fn buffered_provider_delay_times_out_once() {
    let shared = captured();
    let scripted = Arc::new(
        Scripted::new()
            .with_hold_after_first(1_100)
            .with_completions(vec![
                Err(ProviderError::rate_limited("retry me")),
                Ok(completion("too late")),
            ]),
    );
    let mut cfg = test_cfg();
    cfg.request_timeout_secs = 1;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("x-request-id", "74005")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"model":"slow","messages":[],"stream":false}"#,
        ))
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted.clone(), cfg))
        .oneshot(req)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
    let _ = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    assert_eq!(
        scripted.attempts(),
        2,
        "the first retryable attempt and the blocking retry must both run"
    );
    let lines = terminal_lines(&shared, "74005");
    assert_eq!(lines.len(), 1, "one terminal record for the timeout");
    assert_eq!(lines[0]["fields"]["result"], "timeout");
    assert_eq!(lines[0]["fields"]["status"], 504);
}

#[tokio::test]
async fn listing_retries_retryable_provider_error_once() {
    let shared = captured();
    let list = serde_json::json!({"object": "list", "data": []});
    let scripted = Arc::new(Scripted::new().with_model_queue(vec![
        Err(ProviderError::rate_limited("slow down")),
        Ok(list),
    ]));
    let req = Request::builder()
        .method("GET")
        .uri("/v1/models")
        .header("x-request-id", "74006")
        .body(Body::empty())
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted.clone(), test_cfg()))
        .oneshot(req)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["data"],
        serde_json::json!([])
    );
    assert_eq!(scripted.models_attempts(), 2);
    let lines = terminal_lines(&shared, "74006");
    assert_eq!(lines.len(), 1, "one terminal record for the listing");
    let fields = &lines[0]["fields"];
    assert_eq!(fields["endpoint"], "models");
    assert_eq!(fields["result"], "ok");
    assert_eq!(fields["retries"], 1);
}

#[tokio::test]
async fn listing_provider_delay_times_out_once() {
    let shared = captured();
    let scripted = Arc::new(
        Scripted::new()
            .with_models_delay_after_first(1_100)
            .with_model_queue(vec![
                Err(ProviderError::rate_limited("retry me")),
                Ok(serde_json::json!({"object": "list", "data": []})),
            ]),
    );
    let mut cfg = test_cfg();
    cfg.request_timeout_secs = 1;
    let req = Request::builder()
        .method("GET")
        .uri("/v1/models")
        .header("x-request-id", "74007")
        .body(Body::empty())
        .unwrap();
    let resp = opencode2api_server::build_router(pipeline(scripted.clone(), cfg))
        .oneshot(req)
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
    let _ = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    assert_eq!(
        scripted.models_attempts(),
        2,
        "the first retryable attempt and the blocking retry must both run"
    );
    let lines = terminal_lines(&shared, "74007");
    assert_eq!(lines.len(), 1, "one terminal record for the timeout");
    let fields = &lines[0]["fields"];
    assert_eq!(fields["result"], "timeout");
    assert_eq!(fields["endpoint"], "models");
}
