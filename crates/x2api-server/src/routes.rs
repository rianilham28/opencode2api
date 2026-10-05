//! Handlers and the shared pipeline: auth gate -> admission -> parse ->
//! retry -> dialect-specific response. All inbound dialects converge here;
//! each keeps its own parsing and rendering in its bridge crate, so this
//! file only sequences them.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{OriginalUri, State};
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};
use x2api_dialects::anthropic::AnthropicRequest;
use x2api_dialects::gemini::GeminiRequest;
use x2api_dialects::responses::ResponsesRequest;
use x2api_kit::util::{constant_time_eq, request_id_of};
use x2api_kit::{ChatRequest, Dialect, ProviderError, RawFrames, RawReply};

use crate::errors::{api_error, render};
use crate::names;
use crate::relay;
use crate::retry::with_retry;
use crate::{InboundFormat, Pipeline};

/// The static route paths, named so a lane call site cannot pass the wrong
/// dialect's URL to a provider. A Gemini request has no static path (the
/// model lives in it), so that handler passes the observed path instead.
pub(crate) const CHAT_PATH: &str = "/v1/chat/completions";
pub(crate) const MESSAGES_PATH: &str = "/v1/messages";
pub(crate) const RESPONSES_PATH: &str = "/v1/responses";

/// The ONE funnel for client-visible refusals before admission. It writes the
/// terminal record with `result="rejected"`; the 404 fallback instead infers
/// `result="failed"` for its own 404, so an echoed `x-request-id` always has
/// a corresponding line.
fn refused(
    headers: &HeaderMap,
    fmt: InboundFormat,
    endpoint: &'static str,
    started: Instant,
    err: &ProviderError,
) -> Response {
    relay::Done::answer(request_id_of(headers), endpoint, fmt, err.status, started)
        .result("rejected")
        .emit();
    crate::router::mark_accounted(render(err, fmt))
}

/// `POST /v1/chat/completions` — the identity dialect.
pub(crate) async fn chat_completions(
    State(p): State<Pipeline>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    // Vendor speaks the client's own dialect: skip the fold entirely.
    if let Some(lane) = fidelity_lane(Lane::new(
        &p,
        InboundFormat::OpenAi,
        "chat",
        &body,
        &headers,
        CHAT_PATH,
        None,
    ))
    .await
    {
        return lane;
    }
    let req = match x2api_dialects::chat::parse_request(&body) {
        Ok(r) => r,
        Err(e) => return refused(&headers, InboundFormat::OpenAi, "chat", started, &e),
    };
    chat_core(&p, InboundFormat::OpenAi, "chat", req, &headers, None).await
}

/// `POST /v1/messages` — Anthropic dialect via its bridge.
pub(crate) async fn messages(
    State(p): State<Pipeline>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let fmt = InboundFormat::Anthropic;
    if let Some(lane) = fidelity_lane(Lane::new(
        &p,
        fmt,
        "messages",
        &body,
        &headers,
        MESSAGES_PATH,
        None,
    ))
    .await
    {
        return lane;
    }
    let areq: AnthropicRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            let err = ProviderError::bad_request(format!("invalid messages body: {e}"));
            return refused(&headers, fmt, "messages", started, &err);
        }
    };
    // Faithful Anthropic servers reject this; real SDKs send it anyway.
    if areq.max_tokens.is_none() {
        let err = ProviderError::bad_request("max_tokens: required field is missing");
        return refused(&headers, fmt, "messages", started, &err);
    }
    let req = match areq.to_chat_request() {
        Ok(r) => r,
        // The fold REJECTS shapes it cannot represent (tool blocks, images,
        // tools fields) — a silent drop would answer from a conversation
        // whose tool history was discarded, which is worse than a 400.
        Err(e) => return refused(&headers, fmt, "messages", started, &e),
    };
    chat_core(&p, fmt, "messages", req, &headers, None).await
}

/// `POST /v1/responses` — OpenAI Responses dialect via its bridge.
pub(crate) async fn responses(
    State(p): State<Pipeline>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let fmt = InboundFormat::Responses;
    if let Some(lane) = fidelity_lane(Lane::new(
        &p,
        fmt,
        "responses",
        &body,
        &headers,
        RESPONSES_PATH,
        None,
    ))
    .await
    {
        return lane;
    }
    let rreq: ResponsesRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            let err = ProviderError::bad_request(format!("invalid responses body: {e}"));
            return refused(&headers, fmt, "responses", started, &err);
        }
    };
    let req = match rreq.to_chat_request() {
        Ok(r) => r,
        Err(e) => return refused(&headers, fmt, "responses", started, &e),
    };
    chat_core(&p, fmt, "responses", req, &headers, None).await
}

/// `POST /v1beta/models/{model}:generateContent` (and `:streamGenerateContent`)
/// — the Gemini dialect via its bridge. The model and the stream flag come from
/// the path verb, not the body; the FRAMING comes from `?alt=sse`.
///
/// `:streamGenerateContent` without `alt=sse` is Google's JSON-ARRAY stream —
/// one document that opens with `[`, grows with commas, and closes with `]`.
/// This proxy speaks anonymous `data:` frames and has no array framing, so a
/// request that did not ask for SSE is REFUSED rather than answered with bytes
/// in a shape the client did not request. The alternative (a second framing per
/// dialect) is the relay's job, not a dialect's, and no dialect may edit it.
pub(crate) async fn generate_content(
    State(p): State<Pipeline>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let fmt = InboundFormat::Gemini;
    // `?key=` is one of Google's two documented credential forms, so this
    // dialect can arrive authenticated purely in its URL. The value is handed
    // to the shared preamble rather than checked here: one gate, in one place,
    // on every lane.
    let query = uri.query();
    // Bound, then borrowed: a decoded (`Cow::Owned`) value must outlive the
    // `&str` handed to the gate and the lane.
    let key_param = x2api_dialects::gemini::query_value(query, "key");
    let key = key_param.as_deref();
    // One path, ONE endpoint label, from the router's own decision: a
    // `/v1beta/models/...` tail without a colon is a `models` request there,
    // so these refusals record `models` — as the 405 arm below does, and as
    // the fallback would. `generate-content` belongs to a tail that names its
    // verb; a path that never named one has not reached generation.
    let Some(action) = uri.path().rsplit_once("/models/").map(|(_, r)| r) else {
        let err = ProviderError::bad_request(
            "malformed path: expected /v1beta/models/{model}:generateContent",
        );
        return refused(
            &headers,
            fmt,
            crate::router::endpoint_of_path(uri.path()),
            started,
            &err,
        );
    };
    // Model names carry no colon, so the last colon is the method delimiter.
    let (model, verb) = match action.rsplit_once(':') {
        Some((m, v)) => (m, v),
        None => {
            let err = ProviderError::bad_request(
                "malformed path: expected /v1beta/models/{model}:generateContent",
            );
            return refused(
                &headers,
                fmt,
                crate::router::endpoint_of_path(uri.path()),
                started,
                &err,
            );
        }
    };
    // A client may name the model by its RESOURCE (`models/gemini-2.5-flash`,
    // the form `listModels` returns) or by the bare id the URL template shows.
    // They are the same model, so both reach the IR identically — and the
    // listing strips the same prefix, so the two surfaces agree on what an id
    // is rather than one accepting a form the other cannot answer.
    let model = model.strip_prefix("models/").unwrap_or(model);
    let stream = match verb {
        "generateContent" => false,
        "streamGenerateContent" => true,
        other => {
            let err = ProviderError::not_found(format!("unknown Gemini method \":{other}\""));
            return refused(&headers, fmt, "generate-content", started, &err);
        }
    };
    // A native vendor speaks its own framing, so the lane answers before the
    // `alt` question is even asked — refusing there would be this proxy
    // editorialising a stream it never read.
    if let Some(lane) = fidelity_lane(Lane::new(
        &p,
        fmt,
        "generate-content",
        &body,
        &headers,
        uri.path(),
        key,
    ))
    .await
    {
        return lane;
    }
    // REQUIRE `alt=sse`, rather than rejecting only a wrong `alt`: Google's
    // default for this verb is a JSON ARRAY stream, so a request that named no
    // framing asked for a shape this proxy does not emit. Answering it with SSE
    // would be the silent degradation the bridges exist to refuse.
    let alt = x2api_dialects::gemini::query_value(query, "alt");
    if stream && alt.as_deref() != Some("sse") {
        let sent = match alt.as_deref() {
            Some(other) => format!("alt={other}"),
            None => "no alt".to_string(),
        };
        let err = ProviderError::bad_request(format!(
            "{sent}: `:streamGenerateContent` is a JSON array stream without \
             `?alt=sse`, and this proxy frames SSE only — send `?alt=sse`, or use \
             the buffered `:generateContent` verb"
        ));
        return refused(&headers, fmt, "generate-content", started, &err);
    }
    let greq: GeminiRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            let err = ProviderError::bad_request(format!("invalid generateContent body: {e}"));
            return refused(&headers, fmt, "generate-content", started, &err);
        }
    };
    let req = match greq.to_chat_request(model, stream) {
        Ok(r) => r,
        Err(e) => return refused(&headers, fmt, "generate-content", started, &e),
    };
    chat_core(&p, fmt, "generate-content", req, &headers, key).await
}

/// `GET /v1beta/models` and `GET /v1beta/models/{model}` — the listing surface
/// a Gemini SDK uses for discovery (`listModels` / `getModel`). Served from the
/// same `Provider::models()` the OpenAI-shaped `/v1/models` uses, re-rendered
/// into resource names; without it a Gemini client cannot enumerate what the
/// proxy can actually answer, and discovery is how a gateway picks a model.
pub(crate) async fn list_models(
    State(p): State<Pipeline>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    let fmt = InboundFormat::Gemini;
    // Bound the Cow, then borrow: a decoded (`Cow::Owned`) credential must
    // outlive the `&str` handed to the gate.
    let key_param = x2api_dialects::gemini::query_value(uri.query(), "key");
    // Listings share the generation preamble (gate + admission permit) by
    // doctrine: an unbounded catalogue fan-out would otherwise starve the
    // vendor while `max_inflight` guarded only generation. The trade — a
    // listing queues behind a full burst, up to `admission_wait_secs` — is
    // the shared preamble being the rule, not an accident to knob away.
    let admitted = match admit(&p, &headers, fmt, "models", key_param.as_deref()).await {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    // The one segment after `/models/` is the requested resource, if any.
    let wanted = uri
        .path()
        .split_once("/v1beta/models/")
        .map(|(_, rest)| rest.trim_end_matches('/'))
        .filter(|rest| !rest.is_empty());
    // A `:` in this position is a METHOD confusion, not a missing model:
    // `:generateContent` is a POST verb, and answering 404 "not in this
    // proxy's catalogue" would send the operator hunting a model id that was
    // never the problem. The wildcard route made this reachable, so saying so
    // is this handler's job. Recorded, because a wrong-method probe that is
    // invisible in `x2api_requests_total` is the "mysteriously can't list
    // models" ticket.
    // One path, ONE endpoint label: `endpoint_of_path` calls a colon-bearing
    // tail `generate-content` (it names a POST verb), so this 405 records that
    // same label — the router's decision, not a second vocabulary.
    if wanted.is_some_and(|w| w.contains(':')) {
        let (_, rid, started, queued_ms) = admitted;
        relay::Done::answer(
            rid,
            crate::router::endpoint_of_path(uri.path()),
            fmt,
            405,
            started,
        )
        .origin(listing_origin(rid, started, queued_ms))
        .emit();
        return crate::router::mark_accounted(api_error(
            405,
            &format!(
                "`{}` is a POST method on this surface — POST it with a body, or GET \
                 `/v1beta/models` to list",
                wanted.unwrap_or_default()
            ),
            fmt,
        ));
    }
    fetch_listing(&p, &headers, fmt, admitted, |v| {
        x2api_dialects::gemini::models_page(&v, wanted, uri.query())
    })
    .await
}

/// The single pipeline. `endpoint` is the metric label, not the route path.
///
/// Media parts are counted HERE, after the fold produced the IR rather than
/// inside the bridges: the dialect modules translate and must not learn that an
/// operator is watching, and the walk is over parts the fold already visited.
/// The chat dialect is counted too — `messages` are parsed into `Value`s by the
/// time they get here, so the walk reads what is already in memory and decodes
/// nothing. The fidelity lane stays uncounted, exactly like its tokens: its
/// bytes are the vendor's dialect, and reading them would give the lane the
/// vendor knowledge it exists to avoid.
async fn chat_core(
    p: &Pipeline,
    fmt: InboundFormat,
    endpoint: &'static str,
    req: ChatRequest,
    headers: &HeaderMap,
    query_key: Option<&str>,
) -> Response {
    let (permit, rid, started, queued_ms) = match admit(p, headers, fmt, endpoint, query_key).await
    {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    let [images, files] = x2api_kit::media_counts(&req.messages);
    if images + files > 0 {
        record_media(endpoint, fmt, images, files);
    }
    let mut origin = relay::Origin {
        request_id: rid,
        model: Some(req.model.clone()),
        images,
        files,
        queued_ms,
        retries: 0,
        started,
    };

    // Owned handles for the retry loop's per-attempt futures; Arc clones,
    // not deep body clones — request bodies can be megabytes.
    let provider = p.provider.clone();
    let req = Arc::new(req);
    let hdrs = Arc::new(headers.clone());
    let wants_stream = req.wants_stream();

    if wants_stream {
        // Chat dialect first asks the provider for its verbatim byte relay
        // (no per-chunk JSON round-trip — avoiding it is the lane's whole
        // purpose). A 501 capability gap falls back to transcoded IR chunks;
        // any other error is a real pre-handoff failure and goes through the
        // retry loop as usual.
        let relay_first = matches!(fmt, InboundFormat::OpenAi);
        let attempt = {
            let provider = provider.clone();
            let req = req.clone();
            let hdrs = hdrs.clone();
            move |_| {
                let provider = provider.clone();
                let req = req.clone();
                let hdrs = hdrs.clone();
                async move {
                    let ctx = x2api_kit::CallContext::new(rid, Some(&hdrs));
                    if relay_first {
                        match provider.stream_relay(&req, &ctx).await {
                            Ok(bytes) => return Ok(relay::Source::Verbatim(bytes)),
                            Err(e) if e.status == 501 => {}
                            Err(e) => return Err(e),
                        }
                    }
                    provider
                        .stream(&req, &ctx)
                        .await
                        .map(relay::Source::Transcoded)
                }
            }
        };
        // Errors here are pre-handoff: the retry unit ends at handoff, so
        // this is the last moment a failure can still be re-sent. The
        // wall-clock bound is the SERVER's half of the division http.rs
        // documents (it sets no total client timeout): a vendor that accepts
        // TCP and never answers headers must not pin the admission permit.
        // A breach is pre-handoff BY DEFINITION — nothing was handed to the
        // relay, so no committed byte of a client stream is cut.
        let outcome = tokio::time::timeout(
            Duration::from_secs(p.cfg.request_timeout_secs),
            with_retry(p, rid, "stream", attempt),
        )
        .await;
        match outcome {
            // Like the buffered branch: a cancelled retry loop has no honest
            // attempt count, so `retries` stays off the record.
            Err(_) => {
                drop(permit); // no handoff happened: the slot is ours again
                let err = ProviderError::gateway_timeout("request timed out");
                relay::Done::answer(rid, endpoint, fmt, err.status, started)
                    .origin(origin)
                    .result("timeout")
                    .emit();
                crate::router::mark_accounted(render(&err, fmt))
            }
            Ok(retried) => {
                origin.retries = retried.retries();
                match retried.outcome {
                    Err(err) => {
                        relay::Done::answer(rid, endpoint, fmt, err.status, started)
                            .origin(origin)
                            .emit();
                        crate::router::mark_accounted(render(&err, fmt))
                    }
                    Ok(source) => {
                        metrics::counter!(
                            names::REQUESTS,
                            "endpoint" => endpoint,
                            "format" => fmt.label(),
                            "status" => "200-committed",
                        )
                        .increment(1);
                        // Borrowed off the request Arc, which outlives the attempt
                        // closures' own clones: the one model clone this path needs
                        // is `origin`'s, for the terminal Drop that runs after
                        // these locals are gone.
                        let sink = matches!(&source, relay::Source::Transcoded(_))
                            .then(|| relay::Sink::new(fmt, &req.model, rid));
                        relay::stream_response(
                            source,
                            sink,
                            fmt,
                            p.cfg.clone(),
                            Some(permit),
                            endpoint,
                            origin,
                        )
                    }
                }
            }
        }
    } else {
        let attempt = move |_| {
            let provider = provider.clone();
            let req = req.clone();
            let hdrs = hdrs.clone();
            async move {
                let ctx = x2api_kit::CallContext::new(rid, Some(&hdrs));
                provider.complete(&req, &ctx).await
            }
        };
        let outcome = tokio::time::timeout(
            Duration::from_secs(p.cfg.request_timeout_secs),
            with_retry(p, rid, "complete", attempt),
        )
        .await;
        drop(permit); // buffered: the attempt is the whole request
        match outcome {
            // The loop's own report rides inside the timeout's Ok arm, so this
            // is the only place that can state an attempt count — and only
            // because `Retried` carries it. A deadline breach has none to give:
            // the future was cancelled mid-attempt, so that arm leaves
            // `retries` at zero rather than inventing a number.
            Ok(retried) => {
                origin.retries = retried.retries();
                match retried.outcome {
                    Ok(completion) => {
                        if let Some(u) = completion.usage {
                            relay::record_tokens(endpoint, fmt.label(), u);
                        }
                        relay::Done::answer(rid, endpoint, fmt, 200, started)
                            .origin(origin)
                            .usage(completion.usage)
                            .emit();
                        let wire = match fmt {
                            InboundFormat::OpenAi => {
                                x2api_dialects::chat::completion_to_wire(&completion)
                            }
                            InboundFormat::Anthropic => {
                                x2api_dialects::anthropic::completion_to_anthropic(&completion)
                            }
                            InboundFormat::Responses => {
                                x2api_dialects::responses::completion_to_response(&completion)
                            }
                            InboundFormat::Gemini => {
                                x2api_dialects::gemini::completion_to_generate_content(&completion)
                            }
                        };
                        (axum::http::StatusCode::OK, axum::Json(wire)).into_response()
                    }
                    Err(err) => {
                        relay::Done::answer(rid, endpoint, fmt, err.status, started)
                            .origin(origin)
                            .emit();
                        crate::router::mark_accounted(render(&err, fmt))
                    }
                }
            }
            Err(_) => {
                let err = ProviderError::gateway_timeout("request timed out");
                relay::Done::answer(rid, endpoint, fmt, err.status, started)
                    .origin(origin)
                    .result("timeout")
                    .emit();
                crate::router::mark_accounted(render(&err, fmt))
            }
        }
    }
}

/// Shared preamble for both lanes: client auth gate, request id, admission
/// permit. Acquired BEFORE any attempt so retries never sleep holding no
/// slot; the permit lives for a stream's whole life (it rides into the
/// relay/pump guards). `query_key` is the credential of a dialect that spells
/// it in the URL (Gemini's `?key=`) and `None` everywhere else — the gate that
/// reads it is shared so no lane can skip it.
async fn admit(
    p: &Pipeline,
    headers: &HeaderMap,
    fmt: InboundFormat,
    endpoint: &'static str,
    query_key: Option<&str>,
) -> Result<(tokio::sync::OwnedSemaphorePermit, u64, Instant, u64), Box<Response>> {
    // `started` precedes the gate because the gate is what records a refusal.
    let started = Instant::now();
    // The id is minted BEFORE the gate: a refusal has to be attributable too,
    // and the caller's own `x-request-id` is what it answers with, so the
    // operator greps for the number the client is holding.
    let rid = request_id_of(headers);
    if let Some(denied) = client_auth_gate(p, rid, headers, fmt, query_key, endpoint, started) {
        return Err(Box::new(denied));
    }
    // Measured from the wait itself, not from `started`: the gate in front of it
    // is a comparison, and folding its cost in would make the field answer a
    // question it is not meant to.
    let waiting = Instant::now();
    let permit = match tokio::time::timeout(
        Duration::from_secs(p.cfg.admission_wait_secs),
        p.admission.clone().acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        _ => {
            let err =
                ProviderError::unavailable("proxy at capacity, retry shortly").with_retry_after(5);
            relay::Done::answer(rid, endpoint, fmt, err.status, started)
                .result("shed")
                .emit();
            return Err(Box::new(crate::router::mark_accounted(render(&err, fmt))));
        }
    };
    let queued = waiting.elapsed();
    metrics::histogram!(
        names::ADMISSION_QUEUE,
        "endpoint" => endpoint,
    )
    .record(queued.as_secs_f64());
    Ok((permit, rid, started, queued.as_millis() as u64))
}

/// Everything one fidelity-lane relay needs about the inbound request, carried
/// as ONE argument. This is the proxy's least-interpretive path — it reads
/// nothing — so its inputs travel as a bundle of request facts rather than a
/// nine-parameter function that every new fact about a request would grow.
///
/// The FIDELITY LANE itself: when the provider declares a dialect native, the
/// client's bytes go to the vendor and the vendor's bytes come back — no fold,
/// no IR, no terminator semantics owned by us. Single attempt by contract: the
/// vendor's response IS the client's response, and re-sending a committed
/// stream is the one bug this lane must never have. Pre-send failures (no
/// credential, connect) are ours and render in-dialect.
struct Lane<'a> {
    p: &'a Pipeline,
    fmt: InboundFormat,
    endpoint: &'static str,
    body: &'a Bytes,
    headers: &'a HeaderMap,
    /// The path the request arrived on. A provider needs it because some
    /// dialects carry request state in the URL: Gemini's model AND its stream
    /// flag live there, so its vendor URL cannot be rebuilt from the body
    /// alone. Fold paths never pass it — there the server parses the model and
    /// hands it down through the IR, which is where a fold may learn it.
    path: &'a str,
    /// A credential spelled in the query (`?key=`), which only Google's
    /// dialect does. It rides here so the ONE shared gate still sees it.
    query_key: Option<&'a str>,
}

impl<'a> Lane<'a> {
    /// One argument per fact the lane is allowed to know and must not read.
    fn new(
        p: &'a Pipeline,
        fmt: InboundFormat,
        endpoint: &'static str,
        body: &'a Bytes,
        headers: &'a HeaderMap,
        path: &'a str,
        query_key: Option<&'a str>,
    ) -> Self {
        Self {
            p,
            fmt,
            endpoint,
            body,
            headers,
            path,
            query_key,
        }
    }
}

/// Take the lane when the provider declares `fmt` native, else `None` so the
/// caller folds. The dialect is DERIVED from the inbound format rather than
/// passed alongside it: the two cannot disagree.
async fn fidelity_lane(lane: Lane<'_>) -> Option<Response> {
    let d = lane.fmt.dialect();
    if !lane.p.provider.native_dialects().contains(&d) {
        return None;
    }
    Some(fidelity_relay(lane, d).await)
}

async fn fidelity_relay(lane: Lane<'_>, d: Dialect) -> Response {
    let Lane {
        p,
        fmt,
        endpoint,
        body,
        headers,
        path,
        query_key,
    } = lane;
    let (permit, rid, started, queued_ms) = match admit(p, headers, fmt, endpoint, query_key).await
    {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    // The lane's Origin carries NO model: reading one would mean parsing the
    // body (or, for Gemini, the path) — the vendor knowledge this lane exists to
    // avoid. The record therefore omits the key, exactly as it omits the tokens
    // and frames this lane never counts.
    let origin = relay::Origin {
        request_id: rid,
        model: None,
        images: 0,
        files: 0,
        queued_ms,
        // The lane is single-attempt by contract, so a retry count would always
        // read 0; omitting it is the truth rather than a measurement.
        retries: 0,
        started,
    };
    // The same server-side wall clock the fold's branches use: `relay_raw`
    // is the lane's PRE-HANDOFF seam, so a breach is pre-handoff BY
    // DEFINITION — the vendor committed no bytes through it, no byte already
    // sent to a client is cut, and only a pinned admission permit is freed.
    let ctx = x2api_kit::CallContext::for_lane(rid, Some(headers), path);
    let handed = tokio::time::timeout(
        Duration::from_secs(p.cfg.request_timeout_secs),
        p.provider.relay_raw(d, body.clone(), &ctx),
    )
    .await;
    match handed {
        Err(_) => {
            drop(permit); // pre-handoff: the slot is ours again
            let err = ProviderError::gateway_timeout("request timed out");
            relay::Done::answer(rid, endpoint, fmt, err.status, started)
                .origin(origin.clone())
                .result("timeout")
                .emit();
            crate::router::mark_accounted(render(&err, fmt))
        }
        Ok(Err(err)) => {
            relay::Done::answer(rid, endpoint, fmt, err.status, started)
                .origin(origin.clone())
                .emit();
            crate::router::mark_accounted(render(&err, fmt))
        }
        Ok(Ok(RawReply {
            status: raw_status,
            content_type,
            headers: vendor_headers,
            frames,
        })) => {
            let status = relay::committed_status(raw_status).as_u16();
            let head = relay::VendorHead {
                status,
                content_type,
                headers: vendor_headers,
            };
            match frames {
                RawFrames::Buffered(bytes) => {
                    drop(permit);
                    relay::Done::answer(rid, endpoint, fmt, status, started)
                        .origin(origin)
                        .emit();
                    relay::fidelity_buffered(head, bytes)
                }
                RawFrames::Sse(stream) => {
                    metrics::counter!(
                        names::REQUESTS,
                        "endpoint" => endpoint,
                        "format" => fmt.label(),
                        "status" => if (200..300).contains(&status) { "200-committed".to_string() } else { status.to_string() },
                    )
                    .increment(1);
                    relay::fidelity_pump(
                        stream,
                        head,
                        p.cfg.clone(),
                        Some(permit),
                        endpoint,
                        fmt,
                        origin,
                    )
                }
            }
        }
    }
}

pub(crate) async fn models(State(p): State<Pipeline>, headers: HeaderMap) -> Response {
    // `/v1/models` joins the Gemini listing through the SAME shared preamble
    // (gate + admission) for the same reason: catalogue fan-out queues under
    // `max_inflight` instead of bypassing it.
    let admitted = match admit(&p, &headers, InboundFormat::OpenAi, "models", None).await {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    fetch_listing(&p, &headers, InboundFormat::OpenAi, admitted, Ok).await
}

/// The terminal origin every listing record carries: a catalogue fetch has
/// no model and no media, and no retries until the shared loop reports them.
fn listing_origin(rid: u64, started: Instant, queued_ms: u64) -> relay::Origin {
    relay::Origin {
        request_id: rid,
        model: None,
        images: 0,
        files: 0,
        queued_ms,
        retries: 0,
        started,
    }
}

/// One admitted catalogue fetch: `Provider::models()` under the shared retry
/// loop — a transient vendor 429 now gets the proxy's own backoff instead of
/// reaching the client on first attempt — inside the same server-side
/// wall-clock bound as generation. The permit rides the whole fetch and the
/// terminal record carries the admission `Origin`, so `queued_ms` and
/// `retries` mean what they mean on every other lane. The breach arm is
/// pre-handoff BY DEFINITION: the client has been promised nothing yet, so
/// cancelling cuts no committed bytes and releases the pinned permit.
async fn fetch_listing(
    p: &Pipeline,
    headers: &HeaderMap,
    fmt: InboundFormat,
    admitted: (tokio::sync::OwnedSemaphorePermit, u64, Instant, u64),
    page: impl FnOnce(serde_json::Value) -> Result<serde_json::Value, ProviderError>,
) -> Response {
    let (permit, rid, started, queued_ms) = admitted;
    let mut origin = listing_origin(rid, started, queued_ms);
    let provider = p.provider.clone();
    let hdrs = Arc::new(headers.clone());
    let attempt = move |_| {
        let provider = provider.clone();
        let hdrs = hdrs.clone();
        async move {
            let ctx = x2api_kit::CallContext::new(rid, Some(&hdrs));
            provider.models(&ctx).await
        }
    };
    let outcome = tokio::time::timeout(
        Duration::from_secs(p.cfg.request_timeout_secs),
        with_retry(p, rid, "models", attempt),
    )
    .await;
    drop(permit); // buffered: the fetch is the whole listing request
    match outcome {
        Ok(retried) => {
            origin.retries = retried.retries();
            match retried.outcome.and_then(page) {
                Ok(v) => {
                    relay::Done::answer(rid, "models", fmt, 200, started)
                        .origin(origin)
                        .emit();
                    (axum::http::StatusCode::OK, axum::Json(v)).into_response()
                }
                Err(err) => {
                    relay::Done::answer(rid, "models", fmt, err.status, started)
                        .origin(origin)
                        .emit();
                    crate::router::mark_accounted(render(&err, fmt))
                }
            }
        }
        Err(_) => {
            let err = ProviderError::gateway_timeout("request timed out");
            relay::Done::answer(rid, "models", fmt, err.status, started)
                .origin(origin)
                .result("timeout")
                .emit();
            crate::router::mark_accounted(render(&err, fmt))
        }
    }
}

/// `/ready` — can this proxy serve? Separate from `/health` on purpose: a
/// live process whose egress is gone should be taken OUT of a load
/// balancer's rotation, not restarted. 503 here means "not me, right now".
pub(crate) async fn ready(State(p): State<Pipeline>) -> Response {
    if p.provider.ready() {
        return (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            Bytes::from_static(br#"{"status":"ready"}"#),
        )
            .into_response();
    }
    (
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        [
            (axum::http::header::CONTENT_TYPE, "application/json"),
            (axum::http::header::RETRY_AFTER, "5"),
        ],
        Bytes::from_static(br#"{"status":"unready","reason":"no upstream egress"}"#),
    )
        .into_response()
}

/// `/health` — is the process up? Nothing more: it must stay answerable even
/// when everything downstream is broken, or a restart loop replaces a
/// diagnosable outage with an undiagnosable one.
pub(crate) async fn health() -> Response {
    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        Bytes::from_static(br#"{"status":"ok"}"#),
    )
        .into_response()
}

pub(crate) async fn metrics_route(State(p): State<Pipeline>) -> Response {
    match &p.metrics {
        Some(handle) => (
            axum::http::StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            handle.render(),
        )
            .into_response(),
        None => (
            axum::http::StatusCode::NOT_IMPLEMENTED,
            "metrics recorder not installed in this process",
        )
            .into_response(),
    }
}

/// Optional client-facing key gate (`server.client_api_key`). Unset = open
/// proxy: legitimate for a LAN/single-tenant deployment, and a deployment
/// decision — recorded here so the code, not a README footnote, owns it.
///
/// Any ONE of four spellings authorises: `Authorization: Bearer`, `x-api-key`
/// (Anthropic SDK default), `x-goog-api-key` and `?key=` (Google's two forms).
/// All are compared constant-time against the same secret, and no precedence is
/// taken between them — a client sending two different values has a bug, and
/// 401 says so without picking a favourite.
///
/// The spellings are deliberately NOT narrowed to the dialect that documents
/// them: `x-goog-api-key` authorises an Anthropic request here too. It is the
/// SAME secret, and gating a credential per route is a policy this config has
/// no field for — inventing it in a gate would be behaviour hidden in a
/// helper. `fmt` selects the error ENVELOPE only. Still ONE place by design: a
/// second gate is a second thing to get wrong.
///
/// `query_key` is the only credential this proxy accepts from a URL. That is
/// why the request span CARRIES the path and never the query
/// (`PathOnlyMakeSpan`): the json layer publishes a span's fields on EVERY line
/// inside it, so a query-bearing URI on the span would ship `?key=` to whatever
/// aggregator a deployment points at. Redacting at the span makes the answer
/// independent of the subscriber a deployment chooses — the handler still reads
/// the real query from typed parts, only the span forgets it. Defense in depth,
/// not a fixed live leak.
fn client_auth_gate(
    p: &Pipeline,
    request_id: u64,
    headers: &HeaderMap,
    fmt: InboundFormat,
    query_key: Option<&str>,
    endpoint: &'static str,
    started: Instant,
) -> Option<Response> {
    let want = p.cfg.client_api_key.as_deref()?;
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|auth| auth.strip_prefix("Bearer "));
    let ok = bearer
        .into_iter()
        .chain(
            ["x-api-key", "x-goog-api-key"]
                .iter()
                .filter_map(|name| headers.get(*name))
                .filter_map(|v| v.to_str().ok()),
        )
        .chain(query_key)
        .any(|got| constant_time_eq(got.as_bytes(), want.as_bytes()));
    if ok {
        None
    } else {
        // Recorded HERE, in the one gate every route passes, with the status
        // actually about to be sent: a refusal visible on one surface and
        // invisible on another reads as "only the listing rejects people".
        let err = ProviderError::unauthorized("invalid api key");
        relay::Done::answer(request_id, endpoint, fmt, err.status, started)
            .result("rejected")
            .emit();
        Some(crate::router::mark_accounted(render(&err, fmt)))
    }
}

/// Media parts accepted on a folded request, by kind. See `chat_core` for what
/// is deliberately NOT counted.
fn record_media(endpoint: &'static str, fmt: InboundFormat, images: u64, files: u64) {
    for (kind, n) in [("image", images), ("file", files)] {
        if n == 0 {
            continue;
        }
        metrics::counter!(
            names::MEDIA,
            "endpoint" => endpoint,
            "format" => fmt.label(),
            "kind" => kind,
        )
        .increment(n);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    struct AdmissionOnlyProvider;

    #[async_trait::async_trait]
    impl x2api_kit::Provider for AdmissionOnlyProvider {
        fn name(&self) -> &str {
            "admission-only"
        }

        async fn complete(
            &self,
            _req: &x2api_kit::ChatRequest,
            _ctx: &x2api_kit::CallContext<'_>,
        ) -> Result<x2api_kit::Completion, ProviderError> {
            Err(ProviderError::internal("unused by admission test"))
        }

        async fn stream(
            &self,
            _req: &x2api_kit::ChatRequest,
            _ctx: &x2api_kit::CallContext<'_>,
        ) -> Result<x2api_kit::ChatStream, ProviderError> {
            Err(ProviderError::internal("unused by admission test"))
        }
    }

    #[test]
    fn admitted_requests_record_their_semaphore_wait_for_the_exact_endpoint() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let headers = HeaderMap::new();

        metrics::with_local_recorder(&recorder, || {
            rt.block_on(async {
                let cfg = crate::ServerConfig {
                    max_inflight: 1,
                    admission_wait_secs: 5,
                    ..crate::ServerConfig::default()
                };
                let pipeline = Pipeline::new(Arc::new(AdmissionOnlyProvider), Arc::new(cfg), None);
                let held = pipeline.admission.clone().acquire_owned().await.unwrap();
                let (calling, called) = tokio::sync::oneshot::channel();
                let waiter = tokio::spawn({
                    let pipeline = pipeline.clone();
                    let headers = headers.clone();
                    async move {
                        calling.send(()).unwrap();
                        admit(&pipeline, &headers, InboundFormat::OpenAi, "chat", None).await
                    }
                });
                called.await.unwrap();
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(30)).await;
                assert!(
                    !waiter.is_finished(),
                    "admission must remain blocked while the only permit is held"
                );
                drop(held);
                drop(waiter.await.unwrap().unwrap().0);
            });
        });

        let series: Vec<_> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| key.key().name() == "x2api_admission_queue_seconds")
            .collect();
        assert_eq!(series.len(), 1, "one admission queue series");
        let (key, _, _, value) = &series[0];
        let labels: Vec<_> = key
            .key()
            .labels()
            .map(|label| (label.key(), label.value()))
            .collect();
        assert_eq!(labels, [("endpoint", "chat")]);
        let DebugValue::Histogram(samples) = value else {
            panic!("admission queue must be a histogram: {value:?}");
        };
        assert_eq!(samples.len(), 1, "one admission observation");
        assert!(
            samples[0].into_inner() > 0.0,
            "squeezed admission observed a wait"
        );
    }
}
