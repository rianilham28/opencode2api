//! Error rendering: `ProviderError` -> client-facing JSON, one function per
//! inbound format, sharing the safe-message derivation so formats cannot
//! drift, enforced by construction here.

use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Map, json};
use x2api_kit::ProviderError;

use crate::InboundFormat;

/// Render a provider/pipeline error in the client's own dialect.
///
/// This is the ONE funnel every error passes on its way to a client, which is
/// why it — not a per-route log call — is where a status would be recorded if the
/// span carried one. It does not: the status belongs to the terminal `Done`
/// record, whose own copy survives `log_level: "warn"` (a disabled span
/// attributes nothing, and at `warn` the span is disabled on purpose).
pub(crate) fn render(err: &ProviderError, fmt: InboundFormat) -> Response {
    match fmt {
        // Responses shares the chat-completions error envelope (verified in
        // docs/compliance/openai.md); Anthropic has its own kind table; Gemini
        // speaks the Google `error.{code,status,message}` shape.
        InboundFormat::OpenAi | InboundFormat::Responses => render_openai(err),
        InboundFormat::Anthropic => render_anthropic(err),
        InboundFormat::Gemini => render_gemini(err),
    }
}

/// A status+message with no `ProviderError` machinery (404/405/413/panic).
pub(crate) fn api_error(status: u16, message: &str, fmt: InboundFormat) -> Response {
    let err = ProviderError {
        status,
        message: message.to_string(),
        error_type: ProviderError::error_type_for(status).to_string(),
        retry_after: None,
        error_body: None,
        retryable: matches!(status, 408 | 429 | 500 | 502 | 503 | 504),
        body_truncated: false,
        upstream_headers: None,
    };
    render(&err, fmt)
}

fn status_of(err: &ProviderError) -> StatusCode {
    StatusCode::from_u16(err.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

fn common_headers(err: &ProviderError) -> HeaderMap {
    let mut h = err.upstream_headers.as_deref().cloned().unwrap_or_default();
    // The fold owns the proxy-log correlation id; the fidelity lane relays
    // vendor ids because its raw response preserves upstream metadata.
    h.remove("x-request-id");
    h.remove("request-id");
    h.remove("anthropic-request-id");
    h.remove(header::CONTENT_LENGTH);
    h.remove(header::TRANSFER_ENCODING);
    h.remove(header::CONTENT_ENCODING);
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    // A computed hint and the relayed vendor hint would be two authorities for
    // the same field; the proxy's capped value is authoritative when present.
    if let Some(s) = err.retry_after
        && let Ok(v) = HeaderValue::from_str(&s.to_string())
    {
        h.insert(header::RETRY_AFTER, v);
    }
    h
}

fn render_openai(err: &ProviderError) -> Response {
    let body = match &err.error_body {
        Some(envelope) => json!({ "error": envelope }),
        None => json!({
            "error": {
                "message": err.message,
                "type": err.error_type,
                "param": serde_json::Value::Null,
                "code": serde_json::Value::Null,
            }
        }),
    };
    json_response(status_of(err), common_headers(err), body)
}

/// Anthropic error kinds are a closed SDK-parsed set — derive from status,
/// never from our own `error_type` string (SDK-strict literal table).
/// Status -> Anthropic `error.type`, verbatim from the vendor's own table
/// (docs.claude.com/en/api/errors; transcribed in
/// `docs/compliance/anthropic.md`). Clients switch on this string — the TS
/// SDK forwards `body.error.type` straight into the thrown error — so an
/// invented kind is not a cosmetic slip: it silently defeats every
/// `catch (e) { if (e.type === …) }` a caller wrote.
///
/// Two deliberate choices:
/// - **529 and 503 both map to `overloaded_error`.** 529 is the vendor's own
///   overload status; a 503 from an upstream that is not Anthropic means the
///   same thing to a client, and `overloaded_error` is the only kind in the
///   taxonomy that says it. (The tempting `overloaded_server_error` for 503
///   is a string in neither the docs table nor any SDK union —
///   `docs/compliance/anthropic.md` §(b)3.)
/// - **`request_too_large` (413) and `conflict_error` (409) are emitted even
///   though the TS/py discriminated unions lack variants for them.** They are
///   in the vendor's table, so a client that cannot match them cannot match
///   real Anthropic either; being faithful to the wire beats being
///   convenient for an SDK's incomplete union.
///
/// Kinds grow over time (Anthropic's versioning policy), so unknown statuses
/// fall through to `api_error` rather than being guessed at.
pub(crate) fn anthropic_kind(status: u16) -> &'static str {
    match status {
        400 | 422 => "invalid_request_error",
        401 => "authentication_error",
        402 => "billing_error",
        403 => "permission_error",
        404 => "not_found_error",
        409 => "conflict_error",
        413 => "request_too_large",
        429 => "rate_limit_error",
        503 | 529 => "overloaded_error",
        504 | 408 => "timeout_error",
        _ => "api_error",
    }
}

/// Every kind this server can emit. Anthropic's SDKs discriminate on the
/// string, so the set is closed on purpose and a new mapping has to be added
/// here too — the test below is what makes that a rule rather than a wish.
#[cfg(test)]
const ANTHROPIC_KINDS: &[&str] = &[
    "invalid_request_error",
    "authentication_error",
    "billing_error",
    "permission_error",
    "not_found_error",
    "conflict_error",
    "request_too_large",
    "rate_limit_error",
    "overloaded_error",
    "timeout_error",
    "api_error",
];

fn render_anthropic(err: &ProviderError) -> Response {
    let mut error = Map::new();
    error.insert("type".into(), json!(anthropic_kind(err.status)));
    error.insert("message".into(), json!(err.shown_message()));
    let body = json!({ "type": "error", "error": error });
    json_response(status_of(err), common_headers(err), body)
}

/// Google's error envelope: `{"error": {"code": <http int>, "message": …,
/// "status": <google.rpc.Status word>}}`. The status word is the field a
/// `google-genai` client switches on, so it derives from the same shared table
/// the streaming failure frame uses (never a second copy).
fn render_gemini(err: &ProviderError) -> Response {
    let mut error = Map::new();
    error.insert("code".into(), json!(err.status));
    error.insert("message".into(), json!(err.shown_message()));
    error.insert(
        "status".into(),
        json!(x2api_dialects::gemini::google_status(err.status)),
    );
    let body = json!({ "error": error });
    json_response(status_of(err), common_headers(err), body)
}

/// HeaderMap is not an IntoResponse part; assemble the three pieces by hand.
fn json_response(status: StatusCode, headers: HeaderMap, body: serde_json::Value) -> Response {
    let mut resp = axum::Json(body).into_response();
    *resp.status_mut() = status;
    resp.headers_mut().extend(headers);
    resp
}
/// A panic in any handler becomes a 500 envelope instead of a dropped
/// connection (without this, hyper aborts the task and the client sees a
/// reset with no status at all).
pub(crate) fn panic_response(
    err: Box<dyn std::any::Any + Send + 'static>,
    request_id: u64,
    endpoint: &'static str,
    fmt: InboundFormat,
    started: std::time::Instant,
) -> Response {
    let detail = if let Some(s) = err.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    };
    tracing::error!(
        %request_id,
        panic = %detail,
        "handler panicked; answering 500"
    );
    crate::relay::Done::answer(request_id, endpoint, fmt, 500, started)
        .result("panicked")
        .emit();
    let mut response = api_error(500, "internal server error", fmt);
    let value = axum::http::HeaderValue::from_str(&request_id.to_string())
        .expect("a u64 request id is a valid header value");
    response
        .headers_mut()
        .insert(axum::http::HeaderName::from_static("x-request-id"), value);
    crate::router::mark_accounted(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panic_response_stamps_its_own_request_id() {
        let response = panic_response(
            Box::new("boom"),
            4242,
            "messages",
            InboundFormat::Anthropic,
            std::time::Instant::now(),
        );
        assert_eq!(
            response.headers()["x-request-id"],
            axum::http::HeaderValue::from_static("4242")
        );
    }

    #[tokio::test]
    async fn openai_envelope_carries_derived_type_and_nulls() {
        let resp = api_error(
            429,
            "proxy at capacity, retry shortly",
            InboundFormat::OpenAi,
        );
        assert_eq!(resp.status(), 429);
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(resp.into_body(), 1024).await.unwrap())
                .unwrap();
        assert_eq!(body["error"]["type"], "rate_limit_error");
        assert!(body["error"].get("param").unwrap().is_null());
    }

    #[tokio::test]
    async fn anthropic_renders_closed_kind_set() {
        let resp = api_error(503, "no accounts ready", InboundFormat::Anthropic);
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(resp.into_body(), 1024).await.unwrap())
                .unwrap();
        assert_eq!(body["type"], "error");
        assert_eq!(
            body["error"]["type"], "overloaded_error",
            "503 must render a kind the vendor actually defines"
        );
    }

    /// The vendor's own status->kind table, so a mapping cannot drift from it
    /// unnoticed. Sourced from docs/compliance/anthropic.md §(b), which
    /// transcribes docs.claude.com/en/api/errors verbatim.
    #[test]
    fn every_kind_matches_the_vendors_table() {
        for (status, kind) in [
            (400, "invalid_request_error"),
            (401, "authentication_error"),
            (402, "billing_error"),
            (403, "permission_error"),
            (404, "not_found_error"),
            (409, "conflict_error"),
            (413, "request_too_large"),
            (429, "rate_limit_error"),
            (500, "api_error"),
            (504, "timeout_error"),
            (529, "overloaded_error"),
        ] {
            assert_eq!(anthropic_kind(status), kind, "status {status}");
        }
    }

    /// No invented strings, ever: a kind outside the vendor's taxonomy cannot
    /// be matched by a client switching on `error.type`, which is how the
    /// `overloaded_server_error` bug went unnoticed.
    #[test]
    fn no_status_renders_a_kind_the_vendor_never_defines() {
        for status in 100u16..=599 {
            let kind = anthropic_kind(status);
            assert!(
                ANTHROPIC_KINDS.contains(&kind),
                "status {status} rendered {kind:?}, which is not in the taxonomy"
            );
        }
    }

    #[tokio::test]
    async fn openai_render_drops_upstream_framing_headers_but_keeps_allowlisted() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("42"));
        headers.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        headers.insert("x-ratelimit-remaining", HeaderValue::from_static("7"));
        let err = ProviderError {
            status: 400,
            message: "upstream rejected the request".into(),
            error_type: "invalid_request_error".into(),
            retry_after: None,
            error_body: None,
            retryable: false,
            body_truncated: false,
            upstream_headers: Some(Box::new(headers)),
        };

        let response = render_openai(&err);

        assert_eq!(response.status(), 400);
        assert!(!response.headers().contains_key(header::CONTENT_LENGTH));
        assert!(!response.headers().contains_key(header::TRANSFER_ENCODING));
        assert!(!response.headers().contains_key(header::CONTENT_ENCODING));
        assert_eq!(
            response.headers()["x-ratelimit-remaining"],
            "7",
            "non-framing allowlisted headers must still be merged"
        );
    }

    #[tokio::test]
    async fn verbatim_envelope_shares_its_message_across_formats() {
        let err = ProviderError {
            status: 400,
            message: "upstream rejected the request".into(),
            error_type: "invalid_request_error".into(),
            retry_after: None,
            error_body: Some(json!({"message": "model not found", "code": "model_not_found"})),
            retryable: false,
            body_truncated: false,
            upstream_headers: None,
        };
        let a = render_openai(&err);
        let b = render_anthropic(&err);
        let aj: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(a.into_body(), 1024).await.unwrap())
                .unwrap();
        let bj: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(b.into_body(), 1024).await.unwrap())
                .unwrap();
        assert_eq!(aj["error"]["code"], "model_not_found");
        assert_eq!(
            bj["error"]["message"], "model not found",
            "both formats must show the same message, no drift"
        );
    }
}
