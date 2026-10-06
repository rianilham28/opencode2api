//! One error taxonomy for the whole pipeline.
//!
//! Rules:
//! - **A client-facing `message` never carries raw vendor text** unless the
//!   vendor body already *is* a valid `{"error":{"message": ...}}` envelope,
//!   in which case it passes through verbatim as `error_body` (same-wire
//!   fidelity; SDKs parse it). Everything else becomes `"{provider}:
//!   {status}: {snippet}"` with a char-safe truncation. Full bodies are for
//!   logs, never for wire.
//! - Status -> OpenAI `error.type` is derived, never prose-parsed.
//! - Capability gaps are typed (`unsupported()` -> 501), not string-matched.
//! - Panics never reach here: the server's catch-panic layer answers 500.

use serde_json::{Map, Value};

/// Max bytes of an upstream error body ever read (WAF-HTML / huge-stack-trace
/// protection).
pub const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
/// Max characters of an upstream error snippet relayed to a client.
pub const MAX_ERROR_MESSAGE_CHARS: usize = 2000;

/// The single error type: produced by providers, consumed by the server for
/// retry decisions and by the format renderers for envelopes.
#[derive(Debug, Clone)]
pub struct ProviderError {
    pub status: u16,
    pub message: String,
    pub error_type: String,
    /// Seconds, if a retry hint was worth honoring.
    pub retry_after: Option<u64>,
    /// A verbatim upstream `error` object; renderers emit `{"error": <this>}`
    /// when present instead of the constructed shape.
    pub error_body: Option<Value>,
    pub retryable: bool,
    /// Keeps a mid-body transport failure separate from the vendor status
    /// because either may independently determine whether a resend is safe.
    pub body_truncated: bool,
    pub upstream_headers: Option<Box<http::HeaderMap>>,
}

impl ProviderError {
    fn new(status: u16, error_type: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            error_type: error_type.to_string(),
            retry_after: None,
            error_body: None,
            retryable: matches!(status, 408 | 429 | 500 | 502 | 503 | 504),
            body_truncated: false,
            upstream_headers: None,
        }
    }

    /// A deterministic local 502 whose cause cannot change by re-sending.
    pub fn permanent_bad_gateway(message: impl Into<String>) -> Self {
        let mut error = Self::bad_gateway(message);
        error.retryable = false;
        error
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(400, "invalid_request_error", message)
    }
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(401, "authentication_error", message)
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(404, "not_found_error", message)
    }
    pub fn rate_limited(message: impl Into<String>) -> Self {
        Self::new(429, "rate_limit_error", message)
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(500, "api_error", message)
    }
    pub fn bad_gateway(message: impl Into<String>) -> Self {
        Self::new(502, "upstream_error", message)
    }
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(503, "service_unavailable", message)
    }
    pub fn gateway_timeout(message: impl Into<String>) -> Self {
        Self::new(504, "server_error", message)
    }
    /// Typed capability gap (no models endpoint, no streaming, ...).
    pub fn unsupported(capability: &str) -> Self {
        Self::new(
            501,
            "not_supported_error",
            format!("this service does not support {capability}"),
        )
    }
    /// Transport failure before the client was committed to (retryable).
    pub fn transport(cause: impl std::fmt::Display) -> Self {
        Self::new(
            502,
            "upstream_error",
            format!("upstream request failed: {cause}"),
        )
    }

    /// The status-derived OpenAI `error.type` used for non-provider errors
    /// (404/405/413 from middleware, admission shed, ...).
    pub fn error_type_for(status: u16) -> &'static str {
        match status {
            400 | 422 => "invalid_request_error",
            401 => "authentication_error",
            403 => "permission_error",
            404 => "not_found_error",
            429 => "rate_limit_error",
            500..=599 => "server_error",
            _ => "api_error",
        }
    }

    /// The message a CLIENT should see: the vendor's own `error.message` when
    /// a trustworthy envelope was retained, else the constructed message.
    /// Every client-facing render — buffered envelope or stream failure
    /// frame — derives through here, so one error cannot read differently
    /// depending on which surface answered (the doctrine that 5xx bodies are
    /// canned keeps those errors envelope-free, so they stay canned here too).
    pub fn shown_message(&self) -> &str {
        self.error_body
            .as_ref()
            .and_then(|body| body.get("message"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or(&self.message)
    }

    /// Map an upstream HTTP error response to a client-safe error.
    ///
    /// If a 4xx body is a valid OpenAI error envelope it passes through as
    /// `error_body`. Otherwise: 4xx bodies surface as
    /// `"{provider}: {status}: {snippet}"` (the request is the caller's to
    /// fix, so the reason helps); **5xx** bodies collapse to a canned
    /// `"{provider} upstream returned status {status}"` because 5xx bodies
    /// embed engine names, shard ids, and ARNs; the full text goes to logs
    /// via `gate_response`, never to the wire.
    pub fn from_upstream(provider: &str, status: u16, body: &[u8]) -> Self {
        if status < 500
            && let Some(envelope) = openai_error_object(body)
        {
            let mut e = Self::new(
                status,
                Self::error_type_for(status),
                "upstream rejected the request",
            );
            e.error_body = Some(envelope);
            return e;
        }
        let text = String::from_utf8_lossy(body);
        let snippet = crate::util::truncate_chars(text.trim(), MAX_ERROR_MESSAGE_CHARS);
        let message = if status >= 500 || snippet.is_empty() {
            format!("{provider} upstream returned status {status}")
        } else {
            format!("{provider}: {status}: {snippet}")
        };
        Self::new(status, Self::error_type_for(status), message)
    }

    pub fn with_retry_after(mut self, secs: u64) -> Self {
        self.retry_after = Some(secs);
        self
    }

    /// Whether the server's retry loop may re-send this request. The flag can
    /// only narrow the status policy; deterministic local verdicts clear it.
    pub fn is_retryable(&self) -> bool {
        matches!(self.status, 408 | 429 | 500 | 502 | 503 | 504) && self.retryable
    }

    pub fn with_upstream_headers(mut self, headers: http::HeaderMap) -> Self {
        self.upstream_headers = Some(Box::new(crate::provider::relayable_headers(&headers)));
        self
    }

    pub fn with_body_truncated(mut self) -> Self {
        self.body_truncated = true;
        self
    }

    pub fn is_body_truncated(&self) -> bool {
        self.body_truncated
    }
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (status {})", self.message, self.status)
    }
}

impl std::error::Error for ProviderError {}

impl From<reqwest::Error> for ProviderError {
    fn from(e: reqwest::Error) -> Self {
        ProviderError::transport(crate::util::error_chain(&e))
    }
}

/// True when the body parses to `{"error": {"message": "<string>", ...}}` (or
/// the `{"error": "text"}` shorthand, normalized into the envelope).
pub fn openai_error_object(body: &[u8]) -> Option<Value> {
    let v: Value = serde_json::from_slice(body).ok()?;
    let err = v.get("error")?;
    if err.get("message").and_then(Value::as_str).is_some() {
        return Some(err.clone());
    }
    if err.is_string() {
        let mut m = Map::new();
        m.insert(
            "message".into(),
            Value::String(err.as_str().unwrap_or_default().to_string()),
        );
        return Some(Value::Object(m));
    }
    None
}

/// Partial bytes remain useful for client-safe extraction, but a failed read
/// must remain observable because it is a lane fault even when status is not.
pub async fn read_capped(resp: &mut reqwest::Response) -> (Vec<u8>, bool) {
    let mut buf = Vec::with_capacity(1024);
    let mut read_failed = false;
    while buf.len() < MAX_ERROR_BODY_BYTES {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                let take = (MAX_ERROR_BODY_BYTES - buf.len()).min(chunk.len());
                buf.extend_from_slice(&chunk[..take]);
            }
            Ok(None) => break,
            Err(_) => {
                read_failed = true;
                break;
            }
        }
    }
    (buf, read_failed)
}

/// Status-gate a provider's upstream response: `Ok(resp)` on 2xx; otherwise
/// consume a capped error body, log the full snippet server-side, and map it
/// into a client-safe error.
///
/// NOTE: upstream `Retry-After` is deliberately *not* copied blindly —
/// free-tier vendors are known to advertise hours in it. A
/// provider that wants it parses the header itself and `with_retry_after`s a
/// capped value.
pub async fn gate_response(
    provider: &str,
    request_id: Option<u64>,
    mut resp: reqwest::Response,
) -> Result<reqwest::Response, ProviderError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let code = status.as_u16();
    let headers = crate::provider::relayable_headers(resp.headers());
    let (bytes, body_truncated) = read_capped(&mut resp).await;
    // `request_id` is passed IN rather than read from the current span, and that
    // is the whole point: this is a WARN, so it survives `log_level: "warn"` —
    // but at that level the request span is disabled, and a span-borne id would
    // vanish from the ONE line that carries the vendor's actual explanation.
    // `None` is the honest value on a call with no request behind it (the
    // catalogue fetch), and tracing omits the key rather than writing zero.
    tracing::warn!(
        provider,
        request_id,
        status = code,
        body_read_failed = body_truncated,
        body = %crate::util::truncate_chars(&String::from_utf8_lossy(&bytes), 200),
        "upstream error"
    );
    let error = ProviderError::from_upstream(provider, code, &bytes).with_upstream_headers(headers);
    Err(if body_truncated {
        error.with_body_truncated()
    } else {
        error
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The WARN line that carries the vendor's own explanation must survive the
    /// level an operator picks WHEN they want only failures: at
    /// `log_level: "warn"` the request span is disabled, so a span-borne id would
    /// vanish from exactly the line that cannot be re-derived. Hence
    /// `request_id` is an argument.
    ///
    /// Both states matter: `Some` prints it, and `None` — the model catalogue
    /// fetch, which has no caller request behind it — omits the key rather than
    /// printing zero, which would be a measurement that never happened.
    #[tokio::test]
    async fn upstream_error_carries_the_request_or_says_it_has_none() {
        for (id, label) in [(Some(4242u64), "attributed"), (None, "catalogue")] {
            let body = br#"{"error":{"message":"vendor says overloaded","type":"server_error"}}"#;
            let resp = reqwest::Response::from(
                http::Response::builder()
                    .status(503)
                    .header("content-type", "application/json")
                    .body(body.to_vec())
                    .unwrap(),
            );
            // The await stays inside the capture: see `Capture::collect_async`
            // for why that is only sound on a current-thread runtime.
            let lines = crate::testlog::Capture::start(label)
                .collect_async(async {
                    let err = gate_response("probeai", id, resp).await;
                    assert_eq!(err.unwrap_err().status, 503, "the gate still gates");
                })
                .await;
            let line = lines
                .iter()
                .find(|l| l["fields"]["message"] == "upstream error")
                .unwrap_or_else(|| panic!("no upstream-error line for {label}: {lines:?}"));
            let fields = line["fields"].as_object().unwrap();
            assert_eq!(fields["provider"], "probeai", "which vendor");
            assert_eq!(fields["status"], 503, "what they answered");
            assert!(
                fields["body"].as_str().unwrap().contains("overloaded"),
                "the vendor's own words are the point of this line: {fields:?}"
            );
            match id {
                Some(want) => assert_eq!(
                    fields["request_id"], want,
                    "{label} lost its id: {fields:?}"
                ),
                None => assert!(
                    !fields.contains_key("request_id"),
                    "{label} invented an id it does not have: {fields:?}"
                ),
            }
        }
    }

    #[test]
    fn valid_envelope_passes_through_verbatim() {
        let body = json!({"error": {"message": "bad model", "type": "invalid_request_error", "code": "model_not_found"}}).to_string();
        let e = ProviderError::from_upstream("svc", 400, body.as_bytes());
        assert_eq!(e.status, 400);
        assert_eq!(
            e.error_body.unwrap()["code"],
            "model_not_found",
            "vendor code must survive"
        );
    }

    #[test]
    fn string_error_shorthand_is_normalized() {
        let body = json!({"error": "quota exhausted"}).to_string();
        let e = ProviderError::from_upstream("svc", 429, body.as_bytes());
        assert_eq!(e.error_body.unwrap()["message"], "quota exhausted");
    }

    #[test]
    fn raw_vendor_text_is_wrapped_and_capped() {
        let body = "x".repeat(9000);
        let e = ProviderError::from_upstream("svc", 400, body.as_bytes());
        assert!(e.error_body.is_none());
        assert!(e.message.starts_with("svc: 400: "));
        assert!(e.message.chars().count() <= MAX_ERROR_MESSAGE_CHARS + 20);
    }

    #[test]
    fn html_waf_page_never_leaks_structured() {
        let body = b"<html><body>502 Bad Gateway - shard us-east-4a</body></html>";
        let e = ProviderError::from_upstream("svc", 502, body);
        assert!(e.error_body.is_none());
        assert!(!e.message.contains("us-east"));
    }

    #[test]
    fn retryable_set_is_status_derived() {
        assert!(ProviderError::rate_limited("x").is_retryable());
        assert!(ProviderError::bad_gateway("x").is_retryable());
        assert!(!ProviderError::bad_request("x").is_retryable());
        assert!(!ProviderError::unauthorized("x").is_retryable());
        assert!(!ProviderError::unsupported("models").is_retryable());
    }

    #[test]
    fn permanent_bad_gateway_local_verdict_is_not_retryable_while_upstream_502_is() {
        assert!(!ProviderError::permanent_bad_gateway("invalid provider config").is_retryable());
        assert!(ProviderError::from_upstream("svc", 502, b"upstream failed").is_retryable());
    }

    #[test]
    fn public_literal_cannot_widen_non_retryable_status_to_retryable() {
        let error = ProviderError {
            status: 400,
            message: "bad request".into(),
            error_type: "invalid_request_error".into(),
            retry_after: None,
            error_body: None,
            retryable: true,
            body_truncated: false,
            upstream_headers: None,
        };

        assert!(!error.is_retryable());
    }

    #[test]
    fn a_5xx_envelope_is_canned_even_when_the_vendor_structured_it() {
        let body = json!({
            "error": {
                "message": "internal engine shard us-east-4a",
                "type": "engine_error"
            }
        })
        .to_string();
        let error = ProviderError::from_upstream("svc", 502, body.as_bytes());

        assert!(error.error_body.is_none());
        assert_eq!(error.message, "svc upstream returned status 502");
    }

    #[tokio::test]
    async fn error_body_chunk_failure_is_marked_truncated() {
        let body = reqwest::Body::wrap_stream(futures_util::stream::iter([
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(
                br#"{"error":{"message":"rejected"}}"#,
            )),
            Err(std::io::Error::other("tunnel died")),
        ]));
        let resp =
            reqwest::Response::from(http::Response::builder().status(400).body(body).unwrap());

        let error = gate_response("svc", None, resp).await.unwrap_err();

        assert_eq!(error.status, 400);
        assert!(error.is_body_truncated());
        assert_eq!(error.error_body.unwrap()["message"], "rejected");
    }

    #[tokio::test]
    async fn complete_error_body_is_not_marked_truncated() {
        let resp = reqwest::Response::from(
            http::Response::builder()
                .status(503)
                .body(br#"{"error":"upstream failed"}"#.to_vec())
                .unwrap(),
        );

        let error = gate_response("svc", None, resp).await.unwrap_err();

        assert_eq!(error.status, 503);
        assert!(!error.is_body_truncated());
    }

    #[test]
    fn body_truncation_marker_preserves_client_classification() {
        let error = ProviderError::from_upstream("svc", 400, br#"{"error":"rejected"}"#)
            .with_body_truncated();

        assert_eq!(error.status, 400);
        assert!(error.is_body_truncated());
        assert_eq!(error.error_body.unwrap()["message"], "rejected");
    }
}
