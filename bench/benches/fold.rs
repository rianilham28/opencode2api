//! Per-frame fold cost, decomposed — the half of the story `opencode2api-bench`
//! cannot tell.
//!
//! The end-to-end bench measures the proxy over loopback TCP, so one number
//! covers hyper, reqwest, the runtime AND the translation. That is the right
//! shape for "what does the proxy cost", and the wrong shape for "which part
//! of translating a frame costs what": at ~3 µs/frame the transport noise is
//! the same order as the work. These benches run the fold with no I/O at all,
//! in the exact composition `relay.rs` uses:
//!
//!   upstream bytes -> SseDecoder -> serde_json::Value -> ChatChunk
//!                  -> bridge events -> serialized SSE frames
//!
//! Two decode shapes are measured on purpose. `bulk_read` is what a fast
//! local upstream produces (many frames per read); `per_frame_read` is what a
//! real generating model produces (one frame per read) and is the one whose
//! per-call overhead the proxy actually pays in production.
//!
//!   cargo bench -p opencode2api-bench

use bytes::BytesMut;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use opencode2api_dialects::anthropic::AnthropicStream;
use opencode2api_dialects::chat::CompatStream;
use opencode2api_dialects::responses::ResponsesStream;
use opencode2api_kit::ChatChunk;
use opencode2api_kit::sse::{self, SseDecoder, SseEvent};
use std::hint::black_box;

const PAD: &str = "the quick brown fox jumps over the lazy dog 0123456789";
const FRAMES_PER_BATCH: usize = 64;

fn frame_bytes() -> Vec<u8> {
    format!(
        "data: {{\"id\":\"bench\",\"model\":\"bench\",\"object\":\"chat.completion.chunk\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{PAD}\"}},\"finish_reason\":null}}]}}\n\n"
    )
    .into_bytes()
}

fn payload_bytes() -> Vec<u8> {
    let frame = frame_bytes();
    // strip `data: ` and the trailing blank line: what the decoder hands on
    frame[6..frame.len() - 2].to_vec()
}

fn ir_chunk() -> ChatChunk {
    ChatChunk::from_wire(bytes::Bytes::from(payload_bytes())).unwrap()
}

fn decode(c: &mut Criterion) {
    let frame = frame_bytes();
    let batch: Vec<u8> = frame.repeat(FRAMES_PER_BATCH);

    let mut g = c.benchmark_group("decode");
    g.throughput(Throughput::Elements(FRAMES_PER_BATCH as u64));
    g.bench_function("bulk_read", |b| {
        b.iter(|| {
            let mut d = SseDecoder::new();
            black_box(d.push(black_box(&batch)).expect("unbounded decoder").len())
        })
    });
    g.bench_function("per_frame_read", |b| {
        b.iter(|| {
            let mut d = SseDecoder::new();
            let mut n = 0usize;
            for _ in 0..FRAMES_PER_BATCH {
                n += d.push(black_box(&frame)).expect("unbounded decoder").len();
            }
            black_box(n)
        })
    });
    g.finish();
}

fn to_ir(c: &mut Criterion) {
    let payload = payload_bytes();
    let mut g = c.benchmark_group("to_ir");
    g.throughput(Throughput::Elements(1));
    // The document-shaped parse, kept as the yardstick the borrowed parse
    // below is measured against.
    g.bench_function("json_value", |b| {
        b.iter(|| {
            let v: serde_json::Value = serde_json::from_slice(black_box(&payload)).unwrap();
            black_box(v)
        })
    });
    g.bench_function("wire_to_chunk", |b| {
        let payload = bytes::Bytes::from(payload.clone());
        b.iter(|| black_box(ChatChunk::from_wire(black_box(payload.clone())).unwrap()))
    });
    g.finish();
}

/// Where does `wire_to_chunk` actually go? These replicate the IR's own
/// deserialize shape so the pure serde cost can be separated from what the IR
/// builds on top of it — no point optimizing allocations if the parse is the
/// wall.
#[derive(serde::Deserialize)]
struct ParseOnly<'a> {
    #[serde(borrow, default)]
    id: Option<std::borrow::Cow<'a, str>>,
    #[serde(borrow, default)]
    model: Option<std::borrow::Cow<'a, str>>,
    #[serde(borrow, default)]
    choices: Vec<ParseChoice<'a>>,
}

#[derive(serde::Deserialize)]
struct ParseChoice<'a> {
    #[serde(borrow, default)]
    delta: Option<ParseDelta<'a>>,
    #[serde(borrow, default)]
    finish_reason: Option<std::borrow::Cow<'a, str>>,
}

#[derive(serde::Deserialize)]
struct ParseDelta<'a> {
    #[serde(borrow, default)]
    content: Option<std::borrow::Cow<'a, str>>,
}

/// The same fields with the text left as an UNPARSED raw span — the shape a
/// fold that never unescapes (and never re-escapes on the way out) would use.
/// Measured here rather than argued: this is the only parse shape that beat
/// the IR's current one.
#[derive(serde::Deserialize)]
struct ParseRaw<'a> {
    #[serde(borrow, default)]
    choices: Vec<RawChoice<'a>>,
}

#[derive(serde::Deserialize)]
struct RawChoice<'a> {
    #[serde(borrow, default)]
    delta: Option<RawDelta<'a>>,
}

#[derive(serde::Deserialize)]
struct RawDelta<'a> {
    #[serde(borrow, default)]
    content: Option<&'a serde_json::value::RawValue>,
}

/// Only what the bridges read per frame: id/model are re-stamped from the
/// FIRST chunk and never after, so materializing them 2000 times a stream is
/// work with no reader.
#[derive(serde::Deserialize)]
struct ParseChoicesOnly<'a> {
    #[serde(borrow, default)]
    choices: Vec<ParseChoice<'a>>,
    #[serde(default)]
    usage: Option<opencode2api_kit::Usage>,
}

/// Touch every parsed field so the optimizer cannot delete the work being
/// measured.
fn text_len(choices: &[ParseChoice<'_>]) -> usize {
    choices
        .iter()
        .map(|c| {
            c.delta
                .as_ref()
                .and_then(|d| d.content.as_deref())
                .map_or(0, str::len)
                + c.finish_reason.as_deref().map_or(0, str::len)
        })
        .sum()
}

fn parse_shapes(c: &mut Criterion) {
    let payload = payload_bytes();
    let mut g = c.benchmark_group("parse");
    g.throughput(Throughput::Elements(1));
    g.bench_function("borrowed_struct", |b| {
        b.iter(|| {
            let v: ParseOnly<'_> = serde_json::from_slice(black_box(&payload)).unwrap();
            black_box(
                v.choices.len() + usize::from(v.id.is_some()) + usize::from(v.model.is_some()),
            )
        })
    });
    g.bench_function("choices_only", |b| {
        b.iter(|| {
            let v: ParseChoicesOnly<'_> = serde_json::from_slice(black_box(&payload)).unwrap();
            black_box(text_len(&v.choices) + usize::from(v.usage.is_some()))
        })
    });
    g.bench_function("raw_text_span", |b| {
        b.iter(|| {
            let v: ParseRaw<'_> = serde_json::from_slice(black_box(&payload)).unwrap();
            black_box(
                v.choices
                    .iter()
                    .filter_map(|c| c.delta.as_ref()?.content.map(|r| r.get().len()))
                    .sum::<usize>(),
            )
        })
    });
    g.finish();
}

fn sinks(c: &mut Criterion) {
    let chunk = ir_chunk();
    let mut g = c.benchmark_group("sink");
    g.throughput(Throughput::Elements(1));

    g.bench_function("compat", |b| {
        let mut s = CompatStream::new("bench");
        let mut dst = BytesMut::with_capacity(4096);
        b.iter(|| {
            s.on_chunk(black_box(&chunk), &mut dst);
            dst.clear();
        })
    });
    g.bench_function("anthropic", |b| {
        let mut s = AnthropicStream::new("bench", "opencode2api-bench");
        let mut dst = BytesMut::with_capacity(4096);
        b.iter(|| {
            s.on_chunk_into(black_box(&chunk), &mut dst);
            dst.clear();
        })
    });
    g.bench_function("responses", |b| {
        let mut s = ResponsesStream::new("bench", "opencode2api-bench");
        let mut dst = BytesMut::with_capacity(4096);
        b.iter(|| {
            s.on_chunk_into(black_box(&chunk), &mut dst);
            dst.clear();
        })
    });
    g.finish();
}

/// The whole per-frame path a `/v1/messages` stream pays, composed exactly as
/// the server composes it. Compare against the sum of the parts above to see
/// what the composition itself adds.
fn end_to_end(c: &mut Criterion) {
    let frame = frame_bytes();
    let mut g = c.benchmark_group("fold");
    g.throughput(Throughput::Elements(1));
    g.bench_function("anthropic_frame", |b| {
        let mut decoder = SseDecoder::new();
        let mut sink = AnthropicStream::new("bench", "opencode2api-bench");
        let mut dst = BytesMut::with_capacity(4096);
        b.iter(|| {
            for event in decoder.push(black_box(&frame)).expect("unbounded decoder") {
                if let SseEvent::Data { data: payload, .. } = event {
                    let chunk = ChatChunk::from_wire(payload).unwrap();
                    sink.on_chunk_into(&chunk, &mut dst);
                }
            }
            dst.clear();
        })
    });
    g.bench_function("chat_frame_verbatim", |b| {
        // The relay path, for contrast: decode and re-frame, never parse.
        let mut decoder = SseDecoder::new();
        let mut dst = BytesMut::with_capacity(4096);
        b.iter(|| {
            for event in decoder.push(black_box(&frame)).expect("unbounded decoder") {
                if let SseEvent::Data { data: payload, .. } = event {
                    sse::append_data_frame(&mut dst, &payload);
                }
            }
            dst.clear();
        })
    });
    g.finish();
}

criterion_group!(benches, decode, to_ir, parse_shapes, sinks, end_to_end);
criterion_main!(benches);
