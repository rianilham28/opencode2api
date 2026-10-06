//! The one guarded-stream relay — exactly one, because per-surface Drop
//! guards drift — in two transport modes:
//!
//! - **Transcoded** (`ChatStream` of IR chunks): the dialect's bridge state
//!   machine produces frames; one writer serializes them.
//! - **Verbatim** (`ChunkStream` of upstream bytes, chat dialect only):
//!   payloads are byte-copied — framing normalized, contents untouched —
//!   under the chat dialect's `[DONE]` contract: never relay past `[DONE]`,
//!   and withhold it when the vendor closes without one. A clean EOF without
//!   `[DONE]` is truncation too: the fold already treats the same condition as
//!   an error, while the compliance note allows bare EOF only as an interruption
//!   signal. Synthesizing completion would contradict the fold and make an
//!   incomplete answer look successful. Frames leave as they decode — the batch
//!   boundary is one upstream poll, never the whole stream.
//!
//! Loop rules:
//! - Frames assemble into a reused `BytesMut`, `split().freeze()`d: one
//!   amortized allocation per yielded batch.
//! - Keepalive comments yield only *between* complete frames, so they can
//!   never split an event.
//! - `CatchPanicLayer` cannot help once this response exists. Every
//!   post-handoff sink/decoder/poll call therefore runs inside `catch_unwind`;
//!   on panic, complete earned records are kept, a partial tail is discarded,
//!   and the outcome is the distinct `"panicked"`. The fidelity lane has no
//!   decoder, only a guarded vendor poll because it relays raw bytes.
//! - Client disconnect is not a branch: hyper drops the generator, which
//!   drops the provider stream, which drops the reqwest body — upstream
//!   cancellation by stream drop.
//! - Terminal accounting happens exactly once, in `StreamOutcome`'s Drop.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::{Bytes, BytesMut};
use futures_util::{FutureExt, StreamExt};
use opencode2api_dialects::anthropic::{AnthropicEvent, AnthropicStream};
use opencode2api_dialects::chat::CompatStream;
use opencode2api_dialects::gemini::GeminiStream;
use opencode2api_dialects::responses::{RespEvent, ResponsesStream};
use opencode2api_kit::{ChatChunk, ChatStream, ChunkStream, ProviderError, ServerConfig, sse};
use serde_json::Value;
use tokio::sync::OwnedSemaphorePermit;

fn append_verbatim_data_frame(dst: &mut BytesMut, payload: &[u8]) {
    dst.extend_from_slice(b"data: ");
    dst.extend_from_slice(payload);
    #[cfg(test)]
    if payload == b"__relay_in_body_panic__" {
        panic!("verbatim body panic");
    }
    dst.extend_from_slice(b"\n\n");
}
use crate::InboundFormat;
use crate::names;

/// What the provider handed off after the retry loop committed.
pub(crate) enum Source {
    Transcoded(ChatStream),
    Verbatim(ChunkStream),
}

/// Dialect encoder state for the transcoded mode. Methods append zero or
/// more COMPLETE frames to `dst` and never signal termination themselves —
/// the generator decides when the stream is done.
pub(crate) enum Sink {
    Compat(CompatStream),
    Anthropic(AnthropicStream, Option<String>),
    Responses(ResponsesStream),
    Gemini(GeminiStream),
}

impl Sink {
    pub(crate) fn new(fmt: InboundFormat, model: &str, request_id: u64) -> Self {
        match fmt {
            InboundFormat::OpenAi => Self::Compat(CompatStream::new(model)),
            InboundFormat::Anthropic => Self::Anthropic(
                AnthropicStream::new(model.to_string(), format!("opencode2api-{request_id:016x}")),
                None,
            ),
            InboundFormat::Responses => Self::Responses(ResponsesStream::new(
                model.to_string(),
                format!("opencode2api-{request_id:016x}"),
            )),
            InboundFormat::Gemini => Self::Gemini(GeminiStream::new(model.to_string())),
        }
    }

    fn banner(&mut self, dst: &mut BytesMut) {
        match self {
            // Gemini, like the chat dialect, has no head event — the stream is
            // just a run of `data:` records.
            Self::Compat(_) | Self::Gemini(_) => {}
            Self::Anthropic(s, _) => push_events(s.message_start().into_iter().map(anon), dst),
            Self::Responses(s) => push_events(s.start().into_iter().map(named), dst),
        }
    }

    /// The per-frame path: every dialect writes its events straight into the
    /// frame buffer (`BytesMut` is the bridges' event sink), so a streamed
    /// token never becomes a `serde_json::Value` on the way out.
    fn on_chunk(&mut self, chunk: &ChatChunk, dst: &mut BytesMut) {
        match self {
            Self::Compat(s) => s.on_chunk(chunk, dst),
            Self::Gemini(s) => s.on_chunk(chunk, dst),
            Self::Anthropic(s, last) => {
                if chunk.finish_reason.is_some() {
                    *last = chunk.finish_reason.clone();
                }
                s.on_chunk_into(chunk, dst);
            }
            Self::Responses(s) => s.on_chunk_into(chunk, dst),
        }
    }

    /// Clean end of the provider stream; each bridge supplies its own
    /// terminator (sentinel frame or terminal object event).
    fn finish(&mut self, dst: &mut BytesMut) {
        match self {
            Self::Compat(s) => s.finish(dst),
            Self::Gemini(s) => s.finish(dst),
            Self::Anthropic(s, last) => {
                push_events(s.finish(last.as_deref()).into_iter().map(anon), dst)
            }
            Self::Responses(s) => push_events(s.finish().into_iter().map(named), dst),
        }
    }

    /// Terminal failure frame, per-dialect grammar. The OpenAI byte lane keeps
    /// vendor in-band frames untouched; classification applies only here.
    fn fail(&mut self, err: &ProviderError, dst: &mut BytesMut) {
        match self {
            Self::Compat(s) if err.error_body.is_some() => {
                let frame = serde_json::json!({
                    "id": Value::Null,
                    "object": "chat.completion.chunk",
                    "created": opencode2api_kit::util::now_epoch_secs(),
                    "choices": [{"index": 0, "delta": {}, "finish_reason": Value::Null}],
                    "error": err.error_body,
                });
                let payload = serde_json::to_vec(&frame).unwrap_or_else(|_| b"{}".to_vec());
                opencode2api_kit::sse::append_data_frame(dst, &payload);
            }
            Self::Gemini(s) => s.fail(err, dst),
            Self::Compat(s) => s.fail(err, dst),
            Self::Anthropic(s, _) => {
                let kind = crate::errors::anthropic_kind(err.status);
                push_events(
                    s.error(kind, err.shown_message()).into_iter().map(anon),
                    dst,
                );
            }
            Self::Responses(s) => {
                let code = ProviderError::error_type_for(err.status);
                push_events(
                    s.fail(code, err.shown_message()).into_iter().map(named),
                    dst,
                );
            }
        }
    }

    /// The failure frame for the proxy's OWN bug: neutral, vendor-blind.
    fn panicked(&mut self, dst: &mut BytesMut) {
        self.fail(&ProviderError::internal("encoder panic"), dst);
    }
}

fn anon(ev: AnthropicEvent) -> (Option<&'static str>, serde_json::Value) {
    (ev.0, ev.1)
}

fn named(ev: RespEvent) -> (Option<&'static str>, serde_json::Value) {
    (Some(ev.0), ev.1)
}

/// One event writer for every dialect — payload serialization lives here,
/// not per bridge. This is the once-per-stream path (banner, terminal,
/// failure frames); per-chunk events go through the bridges' `on_chunk_into`
/// and never build a `Value`.
fn push_events(
    events: impl IntoIterator<Item = (Option<&'static str>, serde_json::Value)>,
    dst: &mut BytesMut,
) {
    for (name, value) in events {
        sse::append_event(dst, name, &value);
    }
}

/// Run `f` over the shared frame buffer under panic guard. Ok(true) = the
/// buffer may hold frames; Ok(false)/Err = a panic occurred, `frame` was
/// cleared (partial records must not join anything), and the caller decides
/// whether to attempt the failure frame (which runs under its own guard).
fn guarded(
    sink: &mut Sink,
    frame: &mut BytesMut,
    f: impl FnOnce(&mut Sink, &mut BytesMut),
) -> bool {
    match catch_unwind(AssertUnwindSafe(|| f(sink, frame))) {
        Ok(()) => true,
        Err(payload) => {
            let detail = payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "sink panicked".to_string());
            tracing::error!(panic = %detail, "stream sink panicked; terminating");
            // A sink appends whole records (`\n\n`-terminated), so anything
            // up to the last terminator is deliverable; only the trailing
            // possibly-partial record is discarded. Clearing everything would
            // throw away valid frames the client already earned.
            truncate_to_last_record(frame);
            false
        }
    }
}

/// Panic-recovery rule: keep everything up to and including the LAST
/// record terminator, drop only the trailing partial. Sound because every
/// payload here comes from `serde_json::to_vec`, which escapes newlines
/// inside strings — a `\n\n` in the buffer is necessarily a boundary, never
/// payload bytes. Do NOT feed this buffer non-JSON bytes.
fn truncate_to_last_record(buf: &mut BytesMut) {
    const TERM: &[u8] = b"\n\n";
    let end = memchr::memmem::rfind(buf, TERM)
        .map(|i| i + TERM.len())
        // No terminator at all: no complete record is deliverable, so
        // nothing survives — the whole accumulation is a partial.
        .unwrap_or(0);
    buf.truncate(end);
}

/// The identity a stream inherits from the request that opened it.
///
/// It rides into [`StreamOutcome`] because the terminal line is written by a
/// `Drop` long after the handler's locals are gone, and the body closures must
/// not each capture a pile of them. `model`/media are what the fold already
/// knows; `None`/0 on paths that never parse a body.
#[derive(Clone)]
pub(crate) struct Origin {
    pub(crate) request_id: u64,
    pub(crate) model: Option<String>,
    pub(crate) images: u64,
    pub(crate) files: u64,
    /// How long admission held the request before a slot, and how many extra
    /// upstream attempts the retry loop used. Together they answer the two
    /// questions `duration_ms` cannot: "was that us queueing or them being
    /// slow", and "why did it take this long" — neither of which should need a
    /// join across log lines to answer.
    pub(crate) queued_ms: u64,
    pub(crate) retries: u64,
    /// The instant the request was ADMITTED (stamped before the auth gate), not
    /// the instant this guard was built. Carried in the bundle rather than as a
    /// parameter because it is a per-request fact like the rest of them — and
    /// because minting a fresh start at handoff made `duration_ms` mean vendor
    /// time on streams and admission-to-last-byte on buffered replies, one name
    /// over two intervals, with `queued_ms` beside it inviting the sum.
    pub(crate) started: Instant,
}

/// One completed request, said ONCE, in one field vocabulary.
///
/// The buffered fold and the stream guard both build this, so a log tail
/// answers the same questions about a `/v1/messages` 401 as about a 20-frame
/// stream, and `select(.fields.request_id == N)` finds every line the proxy
/// emitted for one caller request. Metrics deliberately stay path-specific —
/// their labels are a contract with existing dashboards (streams count
/// `result`, buffered counts `status`) — so this is the human/JSON half of the
/// accounting, not a second system competing with the first.
pub(crate) struct Done {
    pub(crate) request_id: u64,
    pub(crate) endpoint: &'static str,
    pub(crate) format: &'static str,
    pub(crate) stream: bool,
    pub(crate) status: u16,
    /// `ok|failed|truncated|panicked|dropped|rejected|shed|timeout` — the
    /// stream outcomes are `StreamOutcome`'s vocabulary, not new words.
    pub(crate) result: &'static str,
    pub(crate) started: Instant,
    pub(crate) model: Option<String>,
    pub(crate) usage: Option<opencode2api_kit::Usage>,
    pub(crate) ttfb: Option<Duration>,
    pub(crate) frames: Option<u64>,
    pub(crate) images: u64,
    pub(crate) files: u64,
    pub(crate) queued_ms: u64,
    pub(crate) retries: u64,
}

impl Done {
    /// The facts literally every route has: whose request, which surface, what
    /// answer, and when the clock started. `result` is inferred from the status
    /// and overridden by sites that know better (`shed`, `rejected`).
    pub(crate) fn answer(
        request_id: u64,
        endpoint: &'static str,
        fmt: InboundFormat,
        status: u16,
        started: Instant,
    ) -> Self {
        Self {
            request_id,
            endpoint,
            format: fmt.label(),
            stream: false,
            status,
            result: if status < 400 { "ok" } else { "failed" },
            started,
            model: None,
            usage: None,
            ttfb: None,
            frames: None,
            images: 0,
            files: 0,
            queued_ms: 0,
            retries: 0,
        }
    }

    pub(crate) fn result(mut self, result: &'static str) -> Self {
        self.result = result;
        self
    }

    /// Take the per-request facts. `request_id` is overwritten from the same
    /// source a constructor was just handed, deliberately: one owner per value,
    /// and a site passing an `Origin` is the one saying which request this is.
    pub(crate) fn origin(mut self, origin: Origin) -> Self {
        self.request_id = origin.request_id;
        self.model = origin.model;
        self.images = origin.images;
        self.files = origin.files;
        self.queued_ms = origin.queued_ms;
        self.retries = origin.retries;
        // Same instant the constructor was handed, from the same request: the
        // metrics histogram and the log line must disagree with each other over
        // nothing, and streams have no constructor to disagree with.
        self.started = origin.started;
        self
    }

    pub(crate) fn usage(mut self, usage: Option<opencode2api_kit::Usage>) -> Self {
        self.usage = usage;
        self
    }

    /// Buffered metrics + the line. Streams take [`Done::log`] only: their
    /// counters were already incremented by the guard that owns them.
    pub(crate) fn emit(self) {
        // Labels are `&'static str` except status: the metrics facade keys on
        // label strings, and per-request `to_string()` here would be the only
        // allocation on the buffered hot path besides serialization itself.
        metrics::counter!(
            names::REQUESTS,
            "endpoint" => self.endpoint,
            "format" => self.format,
            "status" => self.status.to_string(),
        )
        .increment(1);
        metrics::histogram!(
            names::DURATION,
            "endpoint" => self.endpoint,
            "stream" => "false",
        )
        .record(self.started.elapsed().as_secs_f64());
        self.log();
    }

    /// The line an operator reads, and the SHORT record: who / which surface /
    /// how it ended / how long / what it cost. Two things were cut, each for a
    /// reason — zero media counts (`images`/`files` are 0 on almost every
    /// request, so they appeared as noise), and `model: "-"` for a route that
    /// parsed no model, which is the same "not measured" the uncounted lanes
    /// already mean.
    ///
    /// `format` was a candidate for the same trimming and is NOT, and the reason
    /// is NOT that the text layout lacks a span — it does print one, as a
    /// `request{… dialect="openai" …}:` prefix, and the json layer as a `"span"`
    /// object. The reason is that BOTH of those are the SPAN, and the span is
    /// `info`-level: at `log_level: "warn"` it is gone from every layout, and
    /// then anything reading the event's own fields (the shipped text log, a
    /// `jq '.fields'`, a grep) cannot tell `/v1/models` from `/v1beta/models`,
    /// which share `endpoint="models"`. `format` is the copy that survives every
    /// configuration — the same argument for `request_id` on this line.
    pub(crate) fn log(&self) {
        let secs = self.started.elapsed().as_secs_f64();
        tracing::info!(
            request_id = self.request_id,
            endpoint = self.endpoint,
            format = self.format,
            stream = self.stream,
            result = self.result,
            status = self.status,
            duration_ms = (secs * 1000.0) as u64,
            // Zero is omitted, not printed: an unqueued, unretried request is
            // the normal case, and two `=0` columns on every line is the noise
            // the media counts were cut for.
            queued_ms = (self.queued_ms > 0).then_some(self.queued_ms),
            retries = (self.retries > 0).then_some(self.retries),
            model = self.model.as_deref(),
            prompt_tokens = self.usage.map(|u| u.prompt_tokens),
            completion_tokens = self.usage.map(|u| u.completion_tokens),
            images = (self.images > 0).then_some(self.images),
            files = (self.files > 0).then_some(self.files),
            ttfb_ms = self.ttfb.map(|d| d.as_millis() as u64),
            frames = self.frames,
            "request completed",
        );
    }
}

/// Terminal-once stream accounting. Exactly one outcome per stream: set
/// before the generator returns, or `dropped` by a Drop with no terminal
/// (client disconnect / generator drop paths).
struct StreamOutcome {
    started: Instant,
    endpoint: &'static str,
    format: &'static str,
    /// The HTTP status actually committed to the client. Folds only pass 2xx
    /// through the handoff gate; the fidelity lane carries the vendor head.
    status: u16,
    outcome: Option<&'static str>,
    origin: Origin,
    /// Whatever the upstream last reported. Streamed usage arrives late (a
    /// terminal frame, often after `finish_reason`), so it is recorded with
    /// the outcome rather than when it is seen.
    usage: Option<opencode2api_kit::Usage>,
    /// Frames RELAYED, and the time the first of them was. `counts_frames` is
    /// false on the fidelity lane: its bytes are the vendor's dialect, and the
    /// lane exists to not read them, so the same BY-DESIGN rule that keeps its
    /// tokens and media uncounted keeps these uncounted too. Reporting `0`
    /// there would read as "an empty stream", which is a different claim.
    frames: u64,
    ttfb: Option<Duration>,
    counts_frames: bool,
    // the admission permit rides along: held for the whole stream lifetime
    _permit: Option<OwnedSemaphorePermit>,
}

impl StreamOutcome {
    fn set(&mut self, outcome: &'static str) {
        self.outcome = Some(outcome);
    }

    fn note_usage(&mut self, usage: Option<opencode2api_kit::Usage>) {
        if usage.is_some() {
            self.usage = usage;
        }
    }

    /// One SSE data record handed toward the client. Called where the record
    /// is relayed, not where a socket write completes: time-to-first-frame is
    /// what tells an operator the vendor was slow versus hyper was slow, and
    /// this is the cheapest honest approximation of it.
    fn note_frame(&mut self) {
        if !self.counts_frames {
            return;
        }
        self.frames += 1;
        if self.ttfb.is_none() {
            self.ttfb = Some(self.started.elapsed());
        }
    }
}

impl Drop for StreamOutcome {
    fn drop(&mut self) {
        let outcome = self.outcome.unwrap_or("dropped");
        let secs = self.started.elapsed().as_secs_f64();
        if let Some(u) = self.usage {
            record_tokens(self.endpoint, self.format, u);
        }
        metrics::counter!(
            names::STREAMS,
            "endpoint" => self.endpoint,
            "format" => self.format,
            "result" => outcome,
        )
        .increment(1);
        metrics::histogram!(names::DURATION, "endpoint" => self.endpoint, "stream" => "true")
            .record(secs);
        if let Some(ttfb) = self.ttfb {
            metrics::histogram!(
                names::STREAM_FIRST_TOKEN,
                "endpoint" => self.endpoint,
                "format" => self.format,
            )
            .record(ttfb.as_secs_f64());
        }
        Done {
            request_id: self.origin.request_id,
            endpoint: self.endpoint,
            format: self.format,
            stream: true,
            // The client already received this status; `result` says whether
            // the bytes were complete.
            status: self.status,
            result: outcome,
            started: self.started,
            model: self.origin.model.clone(),
            usage: self.usage,
            ttfb: self.ttfb,
            frames: self.counts_frames.then_some(self.frames),
            images: self.origin.images,
            files: self.origin.files,
            queued_ms: self.origin.queued_ms,
            retries: self.origin.retries,
        }
        .log();
    }
}

pub(crate) fn stream_response(
    source: Source,
    // None on the verbatim path: byte relay needs no dialect encoder.
    sink: Option<Sink>,
    fmt: InboundFormat,
    cfg: Arc<ServerConfig>,
    permit: Option<OwnedSemaphorePermit>,
    endpoint: &'static str,
    origin: Origin,
) -> Response {
    let guard = StreamOutcome {
        started: origin.started,
        endpoint,
        format: fmt.label(),
        outcome: None,
        origin,
        status: 200,
        usage: None,
        frames: 0,
        ttfb: None,
        // Both modes of this lane decode the chat SSE records already (the
        // sentinel, the usage gate), so counting them adds no vendor knowledge.
        counts_frames: true,
        _permit: permit,
    };
    match source {
        Source::Verbatim(upstream) => sse_response(verbatim_body(upstream, cfg, guard)),
        Source::Transcoded(chunks) => sse_response(transcoded_body(chunks, sink, cfg, guard)),
    }
}

/// The SSE response envelope both bodies wear.
fn sse_response<S>(body: S) -> Response
where
    S: futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    // No Connection header: keep-alive is the HTTP/1.1 default and hyper
    // strips connection-specific fields on h2 — dead on both protocols.
    headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    (StatusCode::OK, headers, Body::from_stream(body)).into_response()
}

/// Deadline + keepalive, set up identically for both bodies.
macro_rules! stream_clocks {
    ($cfg:expr, $deadline:ident, $keepalive:ident) => {
        let $deadline = tokio::time::sleep(Duration::from_secs($cfg.stream_deadline_secs));
        tokio::pin!($deadline);
        let mut $keepalive =
            tokio::time::interval(Duration::from_secs($cfg.sse_keepalive_secs.max(1)));
        $keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        if $cfg.sse_keepalive_secs > 0 {
            // Intervals complete immediately once: consume that tick so the
            // first keepalive is a period away, not instant. Only worth the
            // await when keepalives are ON — the select arm is disabled
            // otherwise, and this await lands before the stream's FIRST byte.
            $keepalive.tick().await;
        }
    };
}

fn transcoded_body(
    chunks: ChatStream,
    sink: Option<Sink>,
    cfg: Arc<ServerConfig>,
    guard: StreamOutcome,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    async_stream::stream! {
        let mut guard = guard;
        stream_clocks!(cfg, deadline, keepalive);

        let Some(mut sink) = sink else {
            tracing::error!("transcoded relay reached without a sink");
            guard.set("failed");
            return;
        };
        let mut chunks = chunks;
        let mut frame = BytesMut::with_capacity(256);

        // banner (kept under its own guard; a broken head frame kills the stream)
        if !guarded(&mut sink, &mut frame, |s, f| s.banner(f)) {
            sink_panicked(&mut sink, &mut frame);
            guard.set("panicked");
            if let Some(b) = sse::take_batch(&mut frame) {
                yield Ok::<Bytes, std::io::Error>(b);
            }
            return;
        }
        if let Some(b) = sse::take_batch(&mut frame) {
            yield Ok::<Bytes, std::io::Error>(b);
        }

        loop {
            // The select decides only WHEN there is something to do; the
            // handling below is one path per outcome, so a terminal can't be
            // spelled twice.
            let first = tokio::select! {
                biased;
                item = chunks.next() => item,
                _ = &mut deadline => {
                    let err = ProviderError::gateway_timeout("stream deadline exceeded");
                    tracing::warn!("stream deadline reached");
                    guarded(&mut sink, &mut frame, |s, f| s.fail(&err, f));
                    guard.set("truncated");
                    if let Some(b) = sse::take_batch(&mut frame) {
                        yield Ok::<Bytes, std::io::Error>(b);
                    }
                    return;
                }
                _ = keepalive.tick(), if cfg.sse_keepalive_secs > 0 => {
                    // Between records by construction: complete frames only
                    // ever leave `frame`, so a comment cannot split an event.
                    yield Ok::<Bytes, std::io::Error>(Bytes::from_static(sse::KEEPALIVE_FRAME));
                    continue;
                }
            };

            // Fold this chunk and every chunk the provider ALREADY has —
            // `now_or_never` never awaits, so a burst that arrived in one
            // upstream read leaves in one write (what the verbatim relay gets
            // for free by batching per read) while a slow stream still yields
            // frame by frame. Pace is unchanged; syscalls per token are not.
            let mut item = first;
            let terminal = loop {
                match item {
                    Some(Ok(chunk)) => {
                        guard.note_usage(chunk.usage);
                        guard.note_frame();
                        if !guarded(&mut sink, &mut frame, |s, f| s.on_chunk(&chunk, f)) {
                            break Terminal::Panicked;
                        }
                        if frame.len() < COALESCE_LIMIT
                            && let Some(next) = chunks.next().now_or_never()
                        {
                            item = next;
                            continue;
                        }
                        break Terminal::Open;
                    }
                    Some(Err(err)) => {
                        tracing::warn!(error = %err, "stream failed");
                        guarded(&mut sink, &mut frame, |s, f| s.fail(&err, f));
                        break Terminal::Failed;
                    }
                    None => {
                        break if guarded(&mut sink, &mut frame, |s, f| s.finish(f)) {
                            Terminal::Done
                        } else {
                            Terminal::Panicked
                        };
                    }
                }
            };

            match terminal {
                Terminal::Open => {}
                Terminal::Done => guard.set("ok"),
                Terminal::Failed => guard.set("failed"),
                Terminal::Panicked => {
                    sink_panicked(&mut sink, &mut frame);
                    guard.set("panicked");
                }
            }
            if let Some(b) = sse::take_batch(&mut frame) {
                yield Ok::<Bytes, std::io::Error>(b);
            }
            if !matches!(terminal, Terminal::Open) {
                return;
            }
        }
    }
}

/// How far a fold got before the buffer had to be flushed.
enum Terminal {
    /// More chunks may follow; flush what we have and keep going.
    Open,
    /// Provider stream ended cleanly; the dialect's terminator is written.
    Done,
    /// In-stream provider failure; the dialect's failure frame is written.
    Failed,
    /// The sink itself panicked; the neutral failure frame still has to go.
    Panicked,
}

/// How much a single flush may accumulate before it goes out regardless of
/// what else is ready. Frames already in hand cost nothing in latency to
/// batch, but an upstream faster than the client must not be allowed to grow
/// this buffer without bound.
const COALESCE_LIMIT: usize = 64 * 1024;

/// One place counts tokens, for both lanes. Zero prompt/completion counts are
/// recorded; optional detail kinds are recorded only when the upstream
/// reported that counter, so a dashboard never invents an absent measurement.
pub(crate) fn record_tokens(
    endpoint: &'static str,
    format: &'static str,
    usage: opencode2api_kit::Usage,
) {
    metrics::counter!(names::TOKENS, "endpoint" => endpoint, "format" => format, "kind" => "prompt")
        .increment(usage.prompt_tokens);
    metrics::counter!(names::TOKENS, "endpoint" => endpoint, "format" => format, "kind" => "completion")
        .increment(usage.completion_tokens);
    for (kind, value) in [
        ("cached", usage.cached_tokens),
        ("reasoning", usage.reasoning_tokens),
        ("cache_creation", usage.cache_creation_input_tokens),
        ("cache_read", usage.cache_read_input_tokens),
    ] {
        if let Some(value) = value {
            metrics::counter!(names::TOKENS, "endpoint" => endpoint, "format" => format, "kind" => kind)
                .increment(value);
        }
    }
}

/// Emit the neutral failure frame, guarded against a sink so broken the
/// failure path itself panics — then the stream simply ends unterminated,
/// which is the honest signal for chat clients.
fn sink_panicked(sink: &mut Sink, frame: &mut BytesMut) {
    guarded(sink, frame, |s, f| s.panicked(f));
}

fn take_verbatim_batch(frame: &mut BytesMut, safe_len: &mut usize) -> Option<Bytes> {
    let batch = sse::take_batch(frame);
    *safe_len = 0;
    batch
}

/// Byte-copy relay, yielding as it decodes. The batch boundary is one
/// upstream poll: every complete record found in a read leaves in a single
/// `Bytes` (amortized allocation), and nothing is ever held back waiting for
/// a later read — a relayed stream must reach the client at upstream's pace,
/// not upstream's total length.
fn verbatim_body(
    upstream: ChunkStream,
    cfg: Arc<ServerConfig>,
    guard: StreamOutcome,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    async_stream::stream! {
        let mut guard = guard;
        let mut upstream = upstream;
        stream_clocks!(cfg, deadline, keepalive);

        let mut decoder = sse::SseDecoder::new();
        // One events Vec for the whole stream (the decoder drains into it);
        // a per-read allocation would be the relay's only one.
        let mut events: Vec<sse::SseEvent> = Vec::new();
        let mut frame = BytesMut::with_capacity(8192);
        // A vendor payload can contain `\n\n`; only append_data_frame's physical
        // end is a safe recovery boundary on this byte lane.
        let mut safe_len = 0;
        loop {
            tokio::select! {
                biased;
                item = AssertUnwindSafe(upstream.next()).catch_unwind() => {
                    let item = match item {
                        Ok(item) => item,
                        Err(_) => {
                            frame.truncate(safe_len);
                            tracing::error!("verbatim upstream poll panicked; terminating");
                            guard.set("panicked");
                            if let Some(b) = take_verbatim_batch(&mut frame, &mut safe_len) {
                                yield Ok::<Bytes, std::io::Error>(b);
                            }
                            return;
                        }
                    };
                    match item {
                        Some(Ok(bytes)) => {
                            let mut sentinel = false;
                            let decoded = AssertUnwindSafe(async {
                                let decode = decoder.push_into(&bytes, &mut events);
                                for event in events.drain(..) {
                                    match event {
                                        sse::SseEvent::Data { data: payload, .. } => {
                                            guard.note_usage(opencode2api_kit::usage_from_frame(&payload));
                                            append_verbatim_data_frame(&mut frame, &payload);
                                            safe_len = frame.len();
                                            guard.note_frame();
                                        }
                                        sse::SseEvent::Done => {
                                            sse::append_data_frame(&mut frame, b"[DONE]");
                                            safe_len = frame.len();
                                            sentinel = true;
                                            break;
                                        }
                                    }
                                }
                                decode
                            })
                            .catch_unwind()
                            .await;
                            let decode = match decoded {
                                Ok(decode) => decode,
                                Err(_) => {
                                    frame.truncate(safe_len);
                                    tracing::error!("verbatim decoder panicked; terminating");
                                    guard.set("panicked");
                                    if let Some(b) = take_verbatim_batch(&mut frame, &mut safe_len) {
                                        yield Ok::<Bytes, std::io::Error>(b);
                                    }
                                    return;
                                }
                            };
                            if let Err(err) = decode {
                                tracing::warn!(error = %err, "SSE record exceeded decoder limit");
                                guard.set("failed");
                                if let Some(b) = take_verbatim_batch(&mut frame, &mut safe_len) {
                                    yield Ok::<Bytes, std::io::Error>(b);
                                }
                                yield Err(std::io::Error::other(err));
                                return;
                            }
                            // Outcome first, yield second: a client that
                            // disappears while the terminal batch is handed to
                            // hyper still ended this stream cleanly, and the
                            // Drop guard would otherwise call it "dropped".
                            if sentinel {
                                guard.set("ok");
                            }
                            if let Some(b) = take_verbatim_batch(&mut frame, &mut safe_len) {
                                yield Ok::<Bytes, std::io::Error>(b);
                            }
                            if sentinel {
                                return;
                            }
                        }
                        Some(Err(err)) => {
                            // Flush complete-but-unyielded records, then die
                            // WITHOUT a terminator: absence of [DONE] is the
                            // truncation signal (nothing was parsed here, so no
                            // in-band error frame can honestly describe the
                            // vendor payload).
                            tracing::warn!(error = %err, "verbatim stream failed");
                            guard.set("failed");
                            if let Some(b) = take_verbatim_batch(&mut frame, &mut safe_len) {
                                yield Ok::<Bytes, std::io::Error>(b);
                            }
                            return;
                        }
                        None => {
                            let flushed = AssertUnwindSafe(async {
                                let flushed = decoder.finish();
                                let mut saw_done = false;
                                for event in flushed {
                                    match event {
                                        sse::SseEvent::Data { data: payload, .. } => {
                                            guard.note_usage(opencode2api_kit::usage_from_frame(&payload));
                                            sse::append_data_frame(&mut frame, &payload);
                                            safe_len = frame.len();
                                            guard.note_frame();
                                        }
                                        sse::SseEvent::Done => {
                                            sse::append_data_frame(&mut frame, b"[DONE]");
                                            safe_len = frame.len();
                                            saw_done = true;
                                            break;
                                        }
                                    }
                                }
                                saw_done
                            })
                            .catch_unwind()
                            .await;
                            let saw_done = match flushed {
                                Ok(saw_done) => saw_done,
                                Err(_) => {
                                    tracing::error!("verbatim EOF decoder panicked; terminating");
                                    frame.truncate(safe_len);
                                    guard.set("panicked");
                                    if let Some(b) = take_verbatim_batch(&mut frame, &mut safe_len) {
                                        yield Ok::<Bytes, std::io::Error>(b);
                                    }
                                    return;
                                }
                            };
                            // OpenAI chat SSE requires [DONE] for completeness;
                            // a frame-boundary EOF is truncation, not success.
                            guard.set(if saw_done { "ok" } else { "truncated" });
                            if let Some(b) = take_verbatim_batch(&mut frame, &mut safe_len) {
                                yield Ok::<Bytes, std::io::Error>(b);
                            }
                            return;
                        }
                    }
                }
                _ = &mut deadline => {
                    tracing::warn!("verbatim stream deadline reached");
                    guard.set("truncated");
                    if let Some(b) = take_verbatim_batch(&mut frame, &mut safe_len) {
                        yield Ok::<Bytes, std::io::Error>(b);
                    }
                    return;
                }
                _ = keepalive.tick(), if cfg.sse_keepalive_secs > 0 => {
                    yield Ok::<Bytes, std::io::Error>(Bytes::from_static(sse::KEEPALIVE_FRAME));
                }
            }
        }
    }
}

/// FIDELITY LANE response: vendor bytes become client bytes, verbatim.
/// Deliberately NOT the transcoded relay's machinery: no keepalive
/// injection (a comment between two upstream chunks could split an event
/// we never parsed), no synthesized or withheld terminators (whether the
/// stream "completed" is the VENDOR dialect's semantics, not ours to
/// judge from raw bytes). The stream deadline is the only intervention —
/// and when it fires the pump simply stops, leaving the client with an
/// honestly-truncated stream. Non-2xx statuses and vendor-shaped error
/// In-band error classification deliberately covers only transcoded decode
/// paths; this byte lane forwards vendor grammar untouched for byte fidelity.
/// bodies already arrived as `RawReply`, so nothing here renders envelopes.
/// What the vendor said ABOUT its answer, as opposed to the answer itself:
/// status line plus the headers a client is allowed to see. One value,
/// because these three always travel together and separately they were
/// pushing the pump past a sane argument count.
pub(crate) struct VendorHead {
    pub status: u16,
    pub content_type: Option<http::HeaderValue>,
    /// Already filtered by `opencode2api_kit::relayable_headers` at the provider.
    pub headers: HeaderMap,
}

pub(crate) fn committed_status(raw: u16) -> StatusCode {
    StatusCode::from_u16(raw).unwrap_or(StatusCode::BAD_GATEWAY)
}

pub(crate) fn fidelity_pump(
    vendor: ChunkStream,
    head: VendorHead,
    cfg: Arc<ServerConfig>,
    permit: Option<OwnedSemaphorePermit>,
    endpoint: &'static str,
    fmt: InboundFormat,
    // Request facts only — including the admission stamp, so this lane's
    // `duration_ms` measures the same interval as every other lane's. The bytes
    // themselves stay unparsed.
    origin: Origin,
) -> Response {
    let status = committed_status(head.status);
    let committed = status.as_u16();
    let guard = StreamOutcome {
        started: origin.started,
        endpoint,
        format: fmt.label(),
        status: committed,
        outcome: None,
        origin,
        usage: None,
        frames: 0,
        ttfb: None,
        counts_frames: false,
        _permit: permit,
    };
    let body = async_stream::stream! {
        let mut guard = guard;
        let deadline = tokio::time::sleep(Duration::from_secs(cfg.stream_deadline_secs));
        tokio::pin!(deadline);
        let mut vendor = vendor;
        loop {
            tokio::select! {
                biased;
                item = AssertUnwindSafe(vendor.next()).catch_unwind() => {
                    let item = match item {
                        Ok(item) => item,
                        Err(_) => {
                            tracing::error!("fidelity upstream poll panicked; terminating");
                            guard.set("panicked");
                            return;
                        }
                    };
                    match item {
                        Some(Ok(bytes)) => yield Ok::<Bytes, std::io::Error>(bytes),
                        Some(Err(err)) => {
                            tracing::warn!(error = %err, "fidelity stream failed");
                            guard.set("failed");
                            return;
                        }
                        None => {
                            // Vendor dialect owns completion semantics on this lane.
                            guard.set("ok");
                            return;
                        }
                    }
                }
                _ = &mut deadline => {
                    tracing::warn!("fidelity stream deadline reached");
                    guard.set("truncated");
                    return;
                }
            }
        }
    };

    // The vendor's allowlisted headers first, then ours — a lane must not let
    // an upstream describe framing we rewrote.
    let mut headers = head.headers;
    headers.insert(
        header::CONTENT_TYPE,
        head.content_type
            .unwrap_or(HeaderValue::from_static("text/event-stream")),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    let mut response = (status, headers, Body::from_stream(body)).into_response();
    response
        .extensions_mut()
        .insert(crate::router::TerminalRecorded);
    response
}

/// Buffered fidelity reply: status + content-type + bytes, untouched.
pub(crate) fn fidelity_buffered(head: VendorHead, bytes: Bytes) -> Response {
    let mut headers = head.headers;
    headers.insert(
        header::CONTENT_TYPE,
        head.content_type
            .unwrap_or(HeaderValue::from_static("application/json")),
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    let status = committed_status(head.status);
    let mut response = (status, headers, Body::from(bytes)).into_response();
    response
        .extensions_mut()
        .insert(crate::router::TerminalRecorded);
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_keeps_complete_records_drops_partial_tail() {
        let mut buf = BytesMut::from(&b"data: one\n\ndata: two\n\ndata: par"[..]);
        truncate_to_last_record(&mut buf);
        assert_eq!(&buf[..], &b"data: one\n\ndata: two\n\n"[..]);
    }

    #[test]
    fn truncate_without_terminator_yields_nothing() {
        let mut buf = BytesMut::from(&b"data: only-partial"[..]);
        truncate_to_last_record(&mut buf);
        assert!(buf.is_empty());
    }

    /// The contract that distinguishes `guarded` from a plain `clear()`:
    /// false on panic, frames earned before the panic SURVIVE, the partial
    /// tail is gone, and a failure frame can be appended onto clean ground.
    #[test]
    fn guarded_recovers_earned_frames_after_sink_panic() {
        let mut sink = Sink::new(InboundFormat::OpenAi, "m", 1);
        let mut frame = BytesMut::new();
        let ok = guarded(&mut sink, &mut frame, |s, f| {
            s.on_chunk(&probe_chunk("earned"), f);
            panic!("boom");
        });
        assert!(!ok, "panic must report as failure");
        let kept = String::from_utf8(frame.split().freeze().into()).unwrap();
        assert!(
            kept.contains("earned"),
            "complete pre-panic frame survives: {kept:?}"
        );
        assert!(
            !kept.ends_with("data: "),
            "no partial record left dangling: {kept:?}"
        );
        // and the terminal failure frame follows the recovered prefix
        sink.panicked(&mut frame);
        let all = String::from_utf8(frame.freeze().into()).unwrap();
        assert!(
            all.contains("\"error\"") && all.ends_with("\n\n"),
            "got {all:?}"
        );
        assert!(
            !all.ends_with("[DONE]"),
            "failed stream withholds the sentinel"
        );
    }

    /// One error, four surfaces: an in-band client-error envelope must show
    /// the VENDOR's message in every dialect's failure frame — the chat lane
    /// by embedding the envelope itself, the other three via `shown_message`,
    /// exactly as the buffered renderers do. The canned `message` the error
    /// was constructed with must never reach a client that has a real one.
    #[test]
    fn in_band_client_error_frame_carries_the_vendor_envelope() {
        let mut err = ProviderError::bad_gateway("upstream returned status 502");
        err.error_body = Some(serde_json::json!({
            "message": "model not found", "code": "model_not_found"
        }));
        for fmt in [
            InboundFormat::OpenAi,
            InboundFormat::Anthropic,
            InboundFormat::Responses,
            InboundFormat::Gemini,
        ] {
            let mut sink = Sink::new(fmt, "m", 1);
            let mut frame = BytesMut::new();
            sink.fail(&err, &mut frame);
            let text = String::from_utf8(frame.freeze().into()).unwrap();
            assert!(
                text.contains("model not found") && !text.contains("status 502"),
                "{fmt:?} frame shows the canned string over the vendor's: {text:?}"
            );
            assert!(
                !text.contains("[DONE]")
                    && !text.contains("message_stop")
                    && !text.contains("response.completed"),
                "{fmt:?} failure frame withholds every success terminator: {text:?}"
            );
        }
    }

    fn probe_chunk(text: &str) -> ChatChunk {
        ChatChunk {
            id: "p".into(),
            model: "m".into(),
            text: text.into(),
            finish_reason: None,
            usage: None,
            tool_calls: Vec::new(),
            raw: None,
            reasoning: String::new(),
            refusal: String::new(),
        }
    }

    /// The terminal line is a published contract: operators grep its keys.
    /// Two lanes drifting to two vocabularies is
    /// exactly what a reviewer cannot see in a diff, so the shapes are pinned
    /// here — including the stream line, which exists only as whatever the
    /// guard's `Drop` chooses to say.
    ///
    /// `tracing` DROPS an unrecorded `Option` rather than writing null, so the
    /// contract has two halves: the keys that are ALWAYS there, and the names an
    /// unmeasured field may never appear under.
    const CORE: &[&str] = &[
        "duration_ms",
        "endpoint",
        "format",
        "message",
        "request_id",
        "result",
        "status",
        "stream",
    ];

    /// The always-present keys, asserted on the line; returns whatever else the
    /// line carried, sorted, so each measurement is an exact set rather than a
    /// substring that survives a rename.
    fn extras(line: &serde_json::Value) -> Vec<String> {
        let fields = line["fields"].as_object().expect("fields object");
        for key in CORE {
            assert!(
                fields.contains_key(*key),
                "a terminal line lost `{key}`: {fields:?}"
            );
        }
        assert_eq!(fields["message"], "request completed");
        assert_eq!(fields["request_id"], 7);
        fields
            .keys()
            .filter(|k| !CORE.contains(&k.as_str()))
            .cloned()
            .collect()
    }

    #[test]
    fn every_lane_writes_the_same_core_and_only_what_it_measured() {
        let origin = || Origin {
            request_id: 7,
            model: Some("glm-4.6".into()),
            images: 2,
            files: 0,
            queued_ms: 0,
            retries: 0,
            started: Instant::now(),
        };
        let guard =
            |outcome: Option<&'static str>, frames: u64, ttfb: Option<Duration>| StreamOutcome {
                started: Instant::now(),
                endpoint: "chat",
                format: InboundFormat::OpenAi.label(),
                status: 200,
                outcome,
                origin: origin(),
                usage: None,
                frames,
                ttfb,
                counts_frames: true,
                _permit: None,
            };
        let lines = crate::testlog::capture(|| {
            Done::answer(7, "chat", InboundFormat::OpenAi, 200, Instant::now())
                .origin(origin())
                .usage(Some(opencode2api_kit::Usage {
                    prompt_tokens: 18,
                    completion_tokens: 96,
                    ..Default::default()
                }))
                .emit();
            // A guard dropped with no terminal: the client hung up mid-stream.
            drop(guard(None, 3, Some(Duration::from_millis(5))));
            // A counted stream that produced no data record at all: `frames` is
            // PRESENT and zero, which is a different claim from absent.
            drop(guard(Some("ok"), 0, None));
        });
        assert_eq!(lines.len(), 3, "one line per completed request");
        // The two stream lines share the fold's media count (2 images); the
        // buffered one adds what it read out of the completion.
        assert_eq!(
            extras(&lines[0]),
            ["completion_tokens", "images", "model", "prompt_tokens"],
            "a folded request adds only what it read"
        );
        assert_eq!(
            extras(&lines[1]),
            ["frames", "images", "model", "ttfb_ms"],
            "a counted stream adds only what it measured"
        );
        assert_eq!(
            extras(&lines[2]),
            ["frames", "images", "model"],
            "a measured zero stays a zero"
        );
        assert_eq!(lines[2]["fields"]["frames"], 0);
        assert_eq!(lines[0]["fields"]["stream"], false);
        assert_eq!(lines[0]["fields"]["result"], "ok");
        assert_eq!(lines[0]["fields"]["status"], 200);
        // `dropped` is the guard's own word for "no terminal was ever set",
        // and the only one that distinguishes a hung-up client from a clean end.
        assert_eq!(lines[1]["fields"]["result"], "dropped");
        assert_eq!(lines[1]["fields"]["frames"], 3);
        // The zero media count is gone, the non-zero one is not.
        assert_eq!(lines[0]["fields"]["images"], 2);
        assert!(
            !lines[0]["fields"]
                .as_object()
                .unwrap()
                .contains_key("files")
        );
    }

    /// The fidelity lane is uncounted BY DESIGN (tokens, media, frames) and the
    /// folded lanes know no model on the listing routes, so the honest spelling
    /// of both is an ABSENT key — never `0`, never `"-"`, which would each read
    /// as a measurement.
    #[test]
    fn an_unmeasured_field_is_absent_rather_than_zero_or_dash() {
        let lines = crate::testlog::capture(|| {
            drop(StreamOutcome {
                started: Instant::now(),
                endpoint: "messages",
                format: InboundFormat::Anthropic.label(),
                status: 413,
                outcome: Some("ok"),
                origin: Origin {
                    request_id: 7,
                    model: None,
                    images: 0,
                    files: 0,
                    // Nothing queued, nothing retried, so neither key may
                    // appear — a zero there would be a claim.
                    queued_ms: 0,
                    retries: 0,
                    started: Instant::now(),
                },
                usage: None,
                frames: 0,
                ttfb: None,
                counts_frames: false,
                _permit: None,
            });
        });
        assert_eq!(
            extras(&lines[0]),
            Vec::<String>::new(),
            "nothing measured, nothing added"
        );
        assert_eq!(lines[0]["fields"]["result"], "ok");
    }

    /// `queued_ms` and `retries` exist to answer "was that us or them" and "why
    /// did it take this long" from one line, so the pair is asserted in both
    /// states: zero on either side keeps the key off the line (an unretried
    /// request is the normal case, not a measurement), and a non-zero one puts it
    /// back. A silently-never-populated field is the worst kind of observability
    /// — it looks like a working instrument and always reads clean.
    #[test]
    fn queue_and_retry_counts_appear_only_when_they_happened() {
        let with = |queued_ms: u64, retries: u64| {
            let lines = crate::testlog::capture(|| {
                let started = Instant::now();
                Done::answer(7, "chat", InboundFormat::OpenAi, 502, started)
                    .result("failed")
                    .origin(Origin {
                        request_id: 7,
                        model: None,
                        images: 0,
                        files: 0,
                        queued_ms,
                        retries,
                        started,
                    })
                    .emit();
            });
            lines[0]["fields"].clone()
        };
        let quiet = with(0, 0);
        assert!(
            !quiet.as_object().unwrap().contains_key("queued_ms")
                && !quiet.as_object().unwrap().contains_key("retries"),
            "an ordinary failure grew two zero columns: {quiet}"
        );
        let slow = with(1200, 3);
        assert_eq!(slow["queued_ms"], 1200, "the wait for a slot");

        assert_eq!(slow["retries"], 3, "extra upstream attempts");
        assert_eq!(slow["result"], "failed");
    }
    fn test_origin() -> Origin {
        Origin {
            request_id: 19,
            model: Some("m".into()),
            images: 0,
            files: 0,
            queued_ms: 0,
            retries: 0,
            started: Instant::now(),
        }
    }

    fn response_bytes(rt: &tokio::runtime::Runtime, response: Response) -> Bytes {
        rt.block_on(axum::body::to_bytes(response.into_body(), 1024 * 1024))
            .unwrap()
    }

    #[test]
    fn verbatim_frame_boundary_eof_without_done_is_truncated() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let upstream = futures_util::stream::iter(vec![Ok::<_, String>(Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n",
        ))])
        .boxed();
        let response = stream_response(
            Source::Verbatim(upstream),
            None,
            InboundFormat::OpenAi,
            Arc::new(ServerConfig::default()),
            None,
            "chat",
            test_origin(),
        );
        let bytes = response_bytes(&rt, response);
        assert_eq!(
            &bytes[..],
            b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"
        );
        assert!(!String::from_utf8_lossy(&bytes).contains("[DONE]"));
        let lines = crate::testlog::capture(|| {
            let upstream = futures_util::stream::empty().boxed();
            let response = stream_response(
                Source::Verbatim(upstream),
                None,
                InboundFormat::OpenAi,
                Arc::new(ServerConfig::default()),
                None,
                "chat",
                test_origin(),
            );
            let _ = rt
                .block_on(axum::body::to_bytes(response.into_body(), 1024))
                .unwrap();
        });
        assert_eq!(lines[0]["fields"]["result"], "truncated");
        assert_eq!(lines[0]["fields"]["status"], 200);
    }

    #[test]
    fn eof_flushed_usage_frame_is_counted_before_truncation() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let payload = Bytes::from_static(
            br#"data: {"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":11}}"#,
        );
        let response = stream_response(
            Source::Verbatim(
                futures_util::stream::iter(vec![Ok::<_, String>(payload.clone())]).boxed(),
            ),
            None,
            InboundFormat::OpenAi,
            Arc::new(ServerConfig::default()),
            None,
            "chat",
            test_origin(),
        );
        let bytes = response_bytes(&rt, response);
        assert!(!String::from_utf8_lossy(&bytes).contains("[DONE]"));
        let lines = crate::testlog::capture(|| {
            let upstream =
                futures_util::stream::iter(vec![Ok::<_, String>(payload.clone())]).boxed();
            let response = stream_response(
                Source::Verbatim(upstream),
                None,
                InboundFormat::OpenAi,
                Arc::new(ServerConfig::default()),
                None,
                "chat",
                test_origin(),
            );
            let _ = rt
                .block_on(axum::body::to_bytes(response.into_body(), 1024))
                .unwrap();
        });
        let fields = &lines[0]["fields"];
        assert_eq!(fields["result"], "truncated");
        assert_eq!(fields["frames"], 1);
        assert_eq!(fields["prompt_tokens"], 7);
        assert_eq!(fields["completion_tokens"], 11);
    }

    #[test]
    fn verbatim_poll_panic_reports_panicked_and_preserves_prefix() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let first = Ok(Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n",
        ));
        let upstream = futures_util::stream::iter(vec![first])
            .chain(futures_util::stream::poll_fn(|_| {
                panic!("verbatim poll panic")
            }))
            .boxed();
        let lines = crate::testlog::capture(|| {
            let response = stream_response(
                Source::Verbatim(upstream),
                None,
                InboundFormat::OpenAi,
                Arc::new(ServerConfig::default()),
                None,
                "chat",
                test_origin(),
            );
            let bytes = response_bytes(&rt, response);
            assert_eq!(
                &bytes[..],
                b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"
            );
        });
        let terminal = lines
            .iter()
            .find(|line| line["fields"]["message"] == "request completed")
            .expect("terminal stream record");
        assert_eq!(terminal["fields"]["result"], "panicked");
        assert_eq!(terminal["fields"]["frames"], 1);
    }
    #[test]
    fn verbatim_in_body_panic_preserves_multiline_record_and_drops_partial() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let wire = Bytes::from_static(
            b"data: first\ndata:\ndata: tail\n\ndata: __relay_in_body_panic__\n\n",
        );
        let upstream = futures_util::stream::iter(vec![Ok::<_, String>(wire)]).boxed();
        let lines = crate::testlog::capture(|| {
            let response = stream_response(
                Source::Verbatim(upstream),
                None,
                InboundFormat::OpenAi,
                Arc::new(ServerConfig::default()),
                None,
                "chat",
                test_origin(),
            );
            assert_eq!(
                response_bytes(&rt, response).as_ref(),
                b"data: first\n\ntail\n\n"
            );
        });
        let terminal = lines
            .iter()
            .find(|line| line["fields"]["message"] == "request completed")
            .expect("terminal stream record");
        assert_eq!(terminal["fields"]["result"], "panicked");
    }

    #[test]
    fn fidelity_poll_panic_reports_panicked() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let upstream = futures_util::stream::poll_fn(|_| panic!("fidelity poll panic")).boxed();
        let lines = crate::testlog::capture(|| {
            let response = fidelity_pump(
                upstream,
                VendorHead {
                    status: 413,
                    content_type: None,
                    headers: HeaderMap::new(),
                },
                Arc::new(ServerConfig::default()),
                None,
                "chat",
                InboundFormat::OpenAi,
                test_origin(),
            );
            assert!(response_bytes(&rt, response).is_empty());
        });
        let terminal = lines
            .iter()
            .find(|line| line["fields"]["message"] == "request completed")
            .expect("terminal stream record");
        assert_eq!(terminal["fields"]["result"], "panicked");
        assert_eq!(terminal["fields"]["status"], 413);
    }
    #[test]
    fn fidelity_response_dropped_before_first_poll_records_dropped() {
        let lines = crate::testlog::capture(|| {
            let upstream = futures_util::stream::iter(vec![Ok::<_, String>(Bytes::from_static(
                b"data: vendor error\n\n",
            ))])
            .boxed();
            let response = fidelity_pump(
                upstream,
                VendorHead {
                    status: 413,
                    content_type: None,
                    headers: HeaderMap::new(),
                },
                Arc::new(ServerConfig::default()),
                None,
                "messages",
                InboundFormat::Anthropic,
                test_origin(),
            );
            drop(response);
        });
        let terminals: Vec<_> = lines
            .iter()
            .filter(|line| line["fields"]["message"] == "request completed")
            .collect();
        assert_eq!(
            terminals.len(),
            1,
            "dropping an unpolled response emits one terminal record"
        );
        assert_eq!(terminals[0]["fields"]["status"], 413);
        assert_eq!(terminals[0]["fields"]["result"], "dropped");
    }

    #[test]
    fn fidelity_terminal_log_status_matches_vendor_head() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let response = fidelity_pump(
            futures_util::stream::empty().boxed(),
            VendorHead {
                status: 202,
                content_type: None,
                headers: HeaderMap::new(),
            },
            Arc::new(ServerConfig::default()),
            None,
            "messages",
            InboundFormat::Anthropic,
            test_origin(),
        );
        assert!(
            response
                .extensions()
                .get::<crate::router::TerminalRecorded>()
                .is_some()
        );
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(response_bytes(&rt, response).is_empty());
        let lines = crate::testlog::capture(|| {
            let response = fidelity_pump(
                futures_util::stream::empty().boxed(),
                VendorHead {
                    status: 202,
                    content_type: None,
                    headers: HeaderMap::new(),
                },
                Arc::new(ServerConfig::default()),
                None,
                "messages",
                InboundFormat::Anthropic,
                test_origin(),
            );
            let _ = rt
                .block_on(axum::body::to_bytes(response.into_body(), 1024))
                .unwrap();
        });
        assert_eq!(lines[0]["fields"]["status"], 202);
        assert_eq!(lines[0]["fields"]["result"], "ok");
    }

    #[test]
    fn fidelity_malformed_vendor_status_becomes_bad_gateway_terminal_record() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let lines = crate::testlog::capture(|| {
            let response = fidelity_pump(
                futures_util::stream::empty().boxed(),
                VendorHead {
                    status: 0,
                    content_type: None,
                    headers: HeaderMap::new(),
                },
                Arc::new(ServerConfig::default()),
                None,
                "messages",
                InboundFormat::Anthropic,
                test_origin(),
            );
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
            assert!(response_bytes(&rt, response).is_empty());
        });
        let terminal = lines
            .iter()
            .find(|line| line["fields"]["message"] == "request completed")
            .expect("terminal stream record");
        assert_eq!(terminal["fields"]["status"], 502);
        assert_eq!(terminal["fields"]["result"], "ok");
    }

    /// The five stream outcomes are a PUBLISHED vocabulary — `AGENTS.md` names
    /// them, and a dashboard series named after
    /// one is what an operator queries. Every other outcome on the line
    /// (`failed`, `rejected`, `shed`, `timeout`) is inferred from a status the
    /// client already saw, but these five are the guard's own judgement about
    /// whether the BYTES were complete, and they are set at scattered call
    /// sites. A typo at any of them would not fail a build, would not fail a
    /// request, and would split one series in two on the dashboard — so the
    /// whole vocabulary is asserted in one place, through the real `set`
    /// followed by the real `Drop`, for all five words.
    #[test]
    fn every_stream_outcome_reaches_the_terminal_line_spelled_exactly() {
        const FIVE: [&str; 5] = ["ok", "failed", "truncated", "panicked", "dropped"];
        let lines = crate::testlog::capture(|| {
            for word in FIVE {
                let mut guard = StreamOutcome {
                    started: Instant::now(),
                    endpoint: "chat",
                    format: InboundFormat::OpenAi.label(),
                    // Same 200 for every outcome on purpose: the status says
                    // what the client got, the result says whether the bytes
                    // were complete, and pinning them together is what proves
                    // the two are independent (a `failed` here is not the
                    // buffered fold's "5xx" word).
                    status: 200,
                    outcome: None,
                    origin: test_origin(),
                    usage: None,
                    frames: 0,
                    ttfb: None,
                    counts_frames: true,
                    _permit: None,
                };
                // `dropped` has no terminal to set: it is what the Drop infers
                // when nothing was ever recorded (client hung up mid-stream).
                if word != "dropped" {
                    guard.set(word);
                }
                assert_eq!(guard.outcome, (word != "dropped").then_some(word));
                drop(guard);
            }
        });
        let recorded: Vec<_> = lines
            .iter()
            .filter(|line| line["fields"]["message"] == "request completed")
            .collect();
        assert_eq!(lines.len(), 5, "one terminal record per stream, no more");
        assert_eq!(recorded.len(), 5, "and all five are terminal records");
        // A SET, not positional indexing: five guards dropped in order is not
        // a promise about capture order, and an index into a shorter vec
        // would quietly compare the wrong pair. The set is also literally the
        // published contract — exactly these five words, none repeated, none
        // missing, no sixth spelling sneaking in.
        let mut words: Vec<&str> = lines
            .iter()
            .map(|line| line["fields"]["result"].as_str().unwrap())
            .collect();
        words.sort_unstable();
        assert_eq!(words, ["dropped", "failed", "ok", "panicked", "truncated"]);
        for line in &recorded {
            assert_eq!(line["fields"]["stream"], true);
            assert_eq!(line["fields"]["status"], 200);
        }
    }

    /// `failed` is the one word of the five that no existing test drove
    /// through a real lane, and the vocabulary table above deliberately does
    /// not stand in for it: a hand-built guard proves the Drop records what it
    /// was TOLD, not that any lane tells it that. This drives the cheapest
    /// site that can produce it — an upstream body error after one good record
    /// (relay.rs's verbatim `Some(Err(..))` arm) — so the word is pinned from
    /// the producer side. The client gets the complete record and NO
    /// terminator, which is the honest shape: absence of `[DONE]` on this path
    /// IS the failure signal.
    #[test]
    fn a_verbatim_upstream_error_records_failed() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let record =
            Bytes::from_static(b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n");
        let lines = crate::testlog::capture(|| {
            let upstream = futures_util::stream::iter(vec![
                Ok(record.clone()),
                Err::<Bytes, String>("boom".into()),
            ])
            .boxed();
            let response = stream_response(
                Source::Verbatim(upstream),
                None,
                InboundFormat::OpenAi,
                Arc::new(ServerConfig::default()),
                None,
                "chat",
                test_origin(),
            );
            let bytes = response_bytes(&rt, response);
            assert_eq!(&bytes[..], &record[..], "earned records survive");
            assert!(!bytes.ends_with(b"[DONE]"), "no synthesized terminator");
        });
        let terminal = lines
            .iter()
            .find(|line| line["fields"]["message"] == "request completed")
            .expect("terminal stream record");
        assert_eq!(terminal["fields"]["result"], "failed");
        // The status is still 200: the client was already committed to a
        // success status, which is exactly why `result` exists as a separate
        // field. A 5xx here would mean this test measured a different lane.
        assert_eq!(terminal["fields"]["status"], 200);
        assert_eq!(terminal["fields"]["frames"], 1);
    }

    /// The exhaustive positive: a stream that measured EVERYTHING publishes
    /// exactly the documented key set — no missing instrument, and no key the
    /// contract never promised. Asserted as an exact set because a subset
    /// check would survive a field being added, and a field added here is a
    /// field every `jq '.fields'` in an operator's runbook now has to know
    /// about.
    #[test]
    fn a_fully_measured_stream_publishes_exactly_the_documented_keys() {
        let started = Instant::now();
        let lines = crate::testlog::capture(|| {
            let mut guard = StreamOutcome {
                started,
                endpoint: "messages",
                format: InboundFormat::Anthropic.label(),
                status: 200,
                outcome: Some("ok"),
                origin: Origin {
                    request_id: 7,
                    model: Some("glm-4.6".into()),
                    images: 1,
                    files: 2,
                    queued_ms: 15,
                    retries: 1,
                    started,
                },
                // Usage arrives on a STREAM through `note_usage`, never in
                // the constructor: the guard is built before the upstream has
                // said anything, and a terminal usage frame lands minutes
                // later. Building with `None` and setting it through the
                // shipped setter is what makes this line the same code path a
                // real stream takes — a literal here would pin a shape the
                // guard is never built in.
                usage: None,
                frames: 20,
                ttfb: Some(Duration::from_millis(40)),
                counts_frames: true,
                _permit: None,
            };
            guard.note_usage(Some(opencode2api_kit::Usage {
                prompt_tokens: 18,
                completion_tokens: 96,
                ..Default::default()
            }));
            drop(guard);
        });
        assert_eq!(lines.len(), 1);
        let fields = lines[0]["fields"].as_object().expect("fields object");
        let mut keys: Vec<&str> = fields.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "completion_tokens",
                "duration_ms",
                "endpoint",
                "files",
                "format",
                "frames",
                "images",
                "message",
                "model",
                "prompt_tokens",
                "queued_ms",
                "request_id",
                "result",
                "retries",
                "status",
                "stream",
                "ttfb_ms",
            ],
            "the fully-measured terminal record's key set"
        );
        // And the values, so an exact set is not the only thing pinned: the
        // durations are what an operator reads to tell the queue from the
        // vendor.
        assert_eq!(lines[0]["fields"]["endpoint"], "messages");
        assert_eq!(lines[0]["fields"]["format"], "anthropic");
        assert_eq!(lines[0]["fields"]["model"], "glm-4.6");
        assert_eq!(lines[0]["fields"]["images"], 1);
        assert_eq!(lines[0]["fields"]["files"], 2);
        assert_eq!(lines[0]["fields"]["queued_ms"], 15);
        assert_eq!(lines[0]["fields"]["retries"], 1);
        assert_eq!(lines[0]["fields"]["ttfb_ms"], 40);
        assert_eq!(lines[0]["fields"]["frames"], 20);
        assert!(lines[0]["fields"]["duration_ms"].is_u64());
    }

    #[test]
    fn stream_drop_records_first_token_only_after_a_measurement() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let measured = StreamOutcome {
                started: Instant::now(),
                endpoint: "chat",
                format: "openai",
                status: 200,
                outcome: Some("ok"),
                origin: Origin {
                    request_id: 7,
                    model: Some("glm-4.6".into()),
                    images: 0,
                    files: 0,
                    queued_ms: 0,
                    retries: 0,
                    started: Instant::now(),
                },
                usage: None,
                frames: 0,
                ttfb: Some(Duration::from_millis(250)),
                counts_frames: true,
                _permit: None,
            };
            drop(measured);
            drop(StreamOutcome {
                started: Instant::now(),
                endpoint: "messages",
                format: "anthropic",
                status: 200,
                outcome: Some("ok"),
                origin: Origin {
                    request_id: 8,
                    model: Some("claude".into()),
                    images: 0,
                    files: 0,
                    queued_ms: 0,
                    retries: 0,
                    started: Instant::now(),
                },
                usage: None,
                frames: 0,
                ttfb: None,
                counts_frames: true,
                _permit: None,
            });
        });

        let series: Vec<_> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| key.key().name() == "opencode2api_stream_first_token_seconds")
            .collect();
        assert_eq!(series.len(), 1, "only the measured stream is a series");
        let (key, _, _, value) = &series[0];
        let mut labels: Vec<_> = key
            .key()
            .labels()
            .map(|label| (label.key(), label.value()))
            .collect();
        labels.sort_unstable();
        assert_eq!(labels, [("endpoint", "chat"), ("format", "openai")]);
        let DebugValue::Histogram(samples) = value else {
            panic!("first-token metric must be a histogram: {value:?}");
        };
        assert_eq!(samples.len(), 1, "one first-token observation");
        assert_eq!(samples[0].into_inner(), 0.25);
    }

    /// The absence half, on the one lane that genuinely has nothing to report
    /// for these two keys: a buffered reply has no first frame and no relayed
    /// frames, so `frames`/`ttfb_ms` must be ABSENT — not `0`, not `null`, not
    /// `0` pretending to be a measurement of a stream that never streamed. A
    /// `frames: 0` on a buffered line reads to a dashboard as "the stream
    /// produced nothing", which is a different and wrong claim.
    #[test]
    fn a_buffered_reply_carries_no_stream_only_keys() {
        let started = Instant::now();
        let lines = crate::testlog::capture(|| {
            Done::answer(7, "chat", InboundFormat::OpenAi, 200, started)
                .origin(Origin {
                    request_id: 7,
                    model: Some("glm-4.6".into()),
                    images: 1,
                    files: 2,
                    queued_ms: 15,
                    retries: 1,
                    started,
                })
                .usage(Some(opencode2api_kit::Usage {
                    prompt_tokens: 3,
                    completion_tokens: 4,
                    ..Default::default()
                }))
                .emit();
        });
        assert_eq!(lines.len(), 1);
        let fields = lines[0]["fields"].as_object().expect("fields object");
        for stream_only in ["frames", "ttfb_ms"] {
            assert!(
                !fields.contains_key(stream_only),
                "a buffered reply published `{stream_only}`: {fields:?}"
            );
        }
        // Everything else the fully-measured stream published is here, so the
        // absence above is about these two keys and not a lane that measures
        // nothing at all.
        assert_eq!(
            extras(&lines[0]),
            [
                "completion_tokens",
                "files",
                "images",
                "model",
                "prompt_tokens",
                "queued_ms",
                "retries",
            ]
        );
        assert_eq!(lines[0]["fields"]["stream"], false);
    }
}
