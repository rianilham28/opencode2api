//! Executable vendor-shape fixtures (`docs/compliance/fixtures/`).
//!
//! The prose in `docs/compliance/*.md` documents what each dialect's wire
//! looks like, but prose cannot fail a build. Every capture under
//! `docs/compliance/fixtures/` is a HAND-AUTHORED rendering of the documented
//! shape (see each file's sibling `.md` note — none is lifted from a live
//! vendor), and every test here asserts the STABLE properties the proxy
//! depends on: the fold parses it, the IR projection (text / tool calls /
//! usage) comes out as expected, a stream whose terminal frame is missing
//! reports truncated and never success, and each dialect's render carries the
//! fields its clients read. When a vendor changes shape the first symptom
//! should be a red test here, not a customer-visible breakage blamed on this
//! proxy.
//!
//! Parity asserts compare a small golden PROJECTION (names, sequences, text,
//! finish reason, usage totals), never raw blobs: byte-comparing volatile
//! fields (`created_at`, minted ids) would make the suite lie on some days.

use bytes::{Bytes, BytesMut};
use serde_json::{Value, json};
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;
use x2api_dialects::{anthropic, chat, gemini, responses};
use x2api_kit::sse::{self, SseDecoder};
use x2api_kit::{ChatChunk, Completion, ProviderError, ToolCall, ToolCallDelta, Usage};

const CHAT_REQUEST: &str =
    include_str!("../../../docs/compliance/fixtures/openai_chat_request.json");
const CHAT_STREAM: &str =
    include_str!("../../../docs/compliance/fixtures/openai_chat_stream.frames.jsonl");
const CHAT_BUFFERED: &str =
    include_str!("../../../docs/compliance/fixtures/openai_chat_buffered.json");
const ANTHROPIC_REQUEST: &str =
    include_str!("../../../docs/compliance/fixtures/anthropic_messages_request.json");
const ANTHROPIC_STREAM: &str =
    include_str!("../../../docs/compliance/fixtures/anthropic_messages_stream.sse");
const ANTHROPIC_ERROR: &str =
    include_str!("../../../docs/compliance/fixtures/anthropic_error_overloaded.json");
const RESPONSES_STREAM: &str =
    include_str!("../../../docs/compliance/fixtures/responses_stream.events.jsonl");
const RESPONSES_REQUEST: &str =
    include_str!("../../../docs/compliance/fixtures/responses_request.json");
const GEMINI_REQUEST: &str =
    include_str!("../../../docs/compliance/fixtures/gemini_generatecontent_request.json");
const GEMINI_STREAM: &str =
    include_str!("../../../docs/compliance/fixtures/gemini_streamchunks.sse");

/// One `data:` payload per non-blank line; the `[DONE]` sentinel kept as a
/// line so tests can assert on its position rather than reconstruct it.
fn stream_lines(raw: &str) -> Vec<&str> {
    raw.lines().filter(|l| !l.trim().is_empty()).collect()
}

/// A stream frame, reduced to what a client can observe: text, finish reason,
/// `(prompt, completion)` tokens. Named because the inline form is hard enough to
/// read that clippy objects, and a projection nobody can parse is not a golden.
type Frame<'a> = (String, Option<&'a str>, Option<(u64, u64)>);

/// Minimal SSE record reader for the `.sse` captures: `event:` + `data:`
/// lines ended by a blank line. Multi-line `data:` joins with `\n` exactly as
/// the kit decoder does.
fn sse_records(raw: &str) -> Vec<(Option<String>, Value)> {
    sse_records_bytes(raw.as_bytes())
}

/// The byte-shaped half, because what the stream machines actually hand back is
/// a `Vec<u8>` buffer: forcing a `String` allocation to read it would be a
/// second thing to get wrong (and the allocation is the only work).
fn sse_records_bytes(raw: &[u8]) -> Vec<(Option<String>, Value)> {
    let raw = std::str::from_utf8(raw).expect("a capture must be valid UTF-8 to be framed text");
    let mut out = Vec::new();
    let mut event: Option<String> = None;
    let mut data = String::new();
    for line in raw.lines() {
        if line.is_empty() {
            if !data.is_empty() {
                out.push((
                    event.take(),
                    serde_json::from_str(&data).expect("fixture data payload must be valid JSON"),
                ));
                data.clear();
            }
        } else if let Some(name) = line.strip_prefix("event: ") {
            event = Some(name.to_string());
        } else if let Some(payload) = line.strip_prefix("data: ") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(payload);
        } else {
            panic!("fixture line is not SSE field syntax: {line}");
        }
    }
    if !data.is_empty() {
        out.push((
            event.take(),
            serde_json::from_str(&data).expect("fixture data payload must be valid JSON"),
        ));
    }
    out
}

fn text_chunk(text: &str) -> ChatChunk {
    ChatChunk {
        refusal: String::new(),
        id: "c".into(),
        model: String::new(),
        text: text.into(),
        finish_reason: None,
        usage: None,
        tool_calls: Vec::new(),
        raw: None,
        reasoning: String::new(),
    }
}

fn with_usage(chunk: &mut ChatChunk, prompt: u64, completion: u64) {
    chunk.usage = Some(Usage {
        prompt_tokens: prompt,
        completion_tokens: completion,
        ..Default::default()
    });
}

fn completion(text: &str, finish: Option<&str>, usage: Option<(u64, u64)>) -> Completion {
    Completion {
        refusal: None,
        id: "gen-1".into(),
        model: "m".into(),
        text: text.into(),
        finish_reason: finish.map(str::to_string),
        usage: usage.map(|(p, c)| Usage {
            prompt_tokens: p,
            completion_tokens: c,
            ..Default::default()
        }),
        tool_calls: Vec::new(),
        raw: None,
        reasoning: None,
    }
}

fn weather_call() -> ToolCall {
    ToolCall {
        id: "call_w1".into(),
        name: "get_weather".into(),
        arguments: "{\"city\":\"SF\"}".into(),
    }
}

// ---------------------------------------------------------------- chat ----

#[test]
fn chat_request_capture_folds_without_error() {
    let req =
        chat::parse_request(CHAT_REQUEST.as_bytes()).expect("documented chat body must parse");
    assert!(req.wants_stream());
    assert_eq!(req.model, "gpt-4o-mini");
    assert_eq!(req.messages.len(), 4);
    assert_eq!(
        req.messages[2]["tool_calls"][0]["function"]["name"],
        "get_weather"
    );
    assert_eq!(req.messages[3]["role"], "tool");
    // The chat dialect's contract is pass-through: fields the IR does not
    // interpret must arrive at the upstream untouched.
    assert_eq!(req.extra["stream_options"]["include_usage"], true);
    assert_eq!(req.extra["temperature"], 0.2);
    assert_eq!(req.extra["tool_choice"], "auto");
}

#[test]
fn chat_stream_frames_decode_to_the_expected_ir_projection() {
    let lines = stream_lines(CHAT_STREAM);
    assert_eq!(
        lines.last(),
        Some(&"[DONE]"),
        "clean capture ends on the sentinel"
    );
    let chunks: Vec<ChatChunk> = lines[..lines.len() - 1]
        .iter()
        .map(|l| {
            ChatChunk::from_wire(Bytes::from(l.to_string()))
                .unwrap_or_else(|e| panic!("undocumented chunk shape: {e} / {l}"))
        })
        .collect();
    // text, finish reason, (prompt, completion) — the three things a client
    // reads off a stream, which is why they are the golden projection.
    let projection: Vec<Frame<'_>> = chunks
        .iter()
        .map(|c| {
            (
                c.text.clone(),
                c.finish_reason.as_deref(),
                c.usage.map(|u| (u.prompt_tokens, u.completion_tokens)),
            )
        })
        .collect();
    assert_eq!(
        projection,
        vec![
            (String::new(), None, None),
            ("It is".into(), None, None),
            (" foggy".into(), None, None),
            (String::new(), Some("stop"), None),
            (String::new(), None, Some((87, 6))),
        ],
        "text deltas, the finish frame, and the usage-only tail"
    );
    assert!(
        chunks
            .iter()
            .all(|c| c.id == "chatcmpl-9bQd7x" && !c.model.is_empty())
    );
}

#[test]
fn chat_stream_relays_captured_frames_byte_verbatim_then_done() {
    let lines = stream_lines(CHAT_STREAM);
    let mut stream = chat::CompatStream::new("gpt-4o-mini");
    let mut dst = BytesMut::new();
    for line in &lines[..lines.len() - 1] {
        let chunk = ChatChunk::from_wire(Bytes::from(line.to_string())).unwrap();
        stream.on_chunk(&chunk, &mut dst);
    }
    stream.finish(&mut dst);
    // Identity relay: the capture's own framing IS the expected output — a
    // vendor rename reaches the client only through this file failing first.
    let expected: String = lines.iter().map(|l| format!("data: {l}\n\n")).collect();
    assert_eq!(&dst[..], expected.as_bytes());
    assert_eq!(&dst[dst.len() - sse::DONE_FRAME.len()..], sse::DONE_FRAME);
}

#[test]
fn chat_stream_without_terminal_sentinel_reads_truncated_not_done() {
    // The capture stores raw payloads; the transport adds the `data:`
    // framing, so the decoder is fed each capture in its framed form.
    let lines = stream_lines(CHAT_STREAM);
    let framed = |ls: &[&str]| -> String { ls.iter().map(|l| format!("data: {l}\n\n")).collect() };
    let mut decoder = SseDecoder::new();
    decoder
        .push(&framed(&lines).into_bytes())
        .expect("unbounded decoder");
    decoder.finish();
    assert!(decoder.done_seen(), "full capture is a complete stream");

    let mut decoder = SseDecoder::new();
    decoder
        .push(&framed(&lines[..lines.len() - 1]).into_bytes())
        .expect("unbounded decoder");
    decoder.finish();
    assert!(
        !decoder.done_seen(),
        "a capture missing the terminal frame must not report success"
    );

    // The failure terminal must keep that promise: an error frame, no [DONE].
    let mut stream = chat::CompatStream::new("gpt-4o-mini");
    let mut dst = BytesMut::new();
    stream.on_chunk(
        &ChatChunk::from_wire(Bytes::from(lines[0].to_string())).unwrap(),
        &mut dst,
    );
    let mark = dst.len();
    stream.fail(
        &ProviderError::bad_gateway("upstream died mid-stream"),
        &mut dst,
    );
    let frame = String::from_utf8(dst[mark..].to_vec()).unwrap();
    assert!(
        !frame.contains("[DONE]"),
        "a truncated chat stream can never look complete"
    );
    let v: Value = serde_json::from_str(
        frame
            .trim_end()
            .trim_end_matches('\n')
            .strip_prefix("data: ")
            .unwrap(),
    )
    .expect("error frame is a JSON chunk");
    assert_eq!(v["error"]["type"], "upstream_error");
    assert_eq!(v["error"]["message"], "upstream died mid-stream");
    assert_eq!(v["choices"][0]["finish_reason"], Value::Null);
    // Identity echo from the capture: the client's stream keeps its id/model.
    assert_eq!(v["id"], "chatcmpl-9bQd7x");
    assert_eq!(v["model"], "gpt-4o-mini-2026-04-14");
}

#[test]
fn chat_buffered_capture_projects_to_ir_and_relays_verbatim() {
    let doc: Value = serde_json::from_str(CHAT_BUFFERED).unwrap();
    let c = Completion::from_openai(doc.clone()).unwrap();
    assert_eq!(c.text, "14 C and foggy.");
    assert_eq!(c.finish_reason.as_deref(), Some("tool_calls"));
    assert_eq!(
        (c.tool_calls[0].id.as_str(), c.tool_calls[0].name.as_str()),
        ("call_5kd2", "get_weather")
    );
    assert_eq!(c.tool_calls[0].arguments, "{\"city\":\"SF\"}");
    let usage = c.usage.expect("buffered usage reaches the IR");
    assert_eq!((usage.prompt_tokens, usage.completion_tokens), (41, 12));
    // Raw fast path: a chat-dialect client gets the vendor document back
    // unchanged — even fields the IR never reads.
    assert_eq!(chat::completion_to_wire(&c), doc);

    // Without a raw document the render must still carry every field a client
    // reads off a buffered completion.
    let mut c = completion("14 C and foggy.", Some("stop"), Some((41, 12)));
    c.id = "chatcmpl-Az14Q".into();
    c.model = "gpt-4o-mini-2026-04-14".into();
    let v = chat::completion_to_wire(&c);
    assert_eq!(v["object"], "chat.completion");
    assert_eq!(v["id"], "chatcmpl-Az14Q");
    assert_eq!(v["model"], "gpt-4o-mini-2026-04-14");
    assert_eq!(v["choices"][0]["message"]["content"], "14 C and foggy.");
    assert_eq!(v["choices"][0]["finish_reason"], "stop");
    assert_eq!(v["usage"]["prompt_tokens"], 41);
}

// ----------------------------------------------------------- anthropic ----

#[test]
fn anthropic_request_capture_folds_to_the_documented_history() {
    let req: anthropic::AnthropicRequest = serde_json::from_slice(ANTHROPIC_REQUEST.as_bytes())
        .expect("documented /v1/messages body must deserialize");
    let chat = req
        .to_chat_request()
        .expect("tool history is representable");
    assert!(chat.wants_stream());
    let roles: Vec<&str> = chat
        .messages
        .iter()
        .map(|m| m["role"].as_str().unwrap())
        .collect();
    assert_eq!(roles, ["system", "user", "assistant", "tool"]);
    assert_eq!(chat.messages[2]["content"], "Let me compute.");
    assert_eq!(chat.messages[2]["tool_calls"][0]["id"], "toolu_01A1B2");
    assert_eq!(
        chat.messages[2]["tool_calls"][0]["function"]["arguments"], "{\"expression\":\"340*0.15\"}",
        "input object becomes the chat wire's string, byte for byte"
    );
    assert_eq!(chat.messages[3]["tool_call_id"], "toolu_01A1B2");
    assert_eq!(chat.extra["max_tokens"], 256);
    assert_eq!(chat.extra["temperature"], 0.1);
    assert_eq!(chat.extra["tools"][0]["function"]["name"], "calculator");
    assert_eq!(chat.extra["tool_choice"], "auto");
    assert!(
        chat.extra.get("metadata").is_none(),
        "Anthropic-only fields drop"
    );
}

#[test]
fn anthropic_stream_capture_has_the_documented_grammar() {
    let records = sse_records(ANTHROPIC_STREAM);
    let names: Vec<&str> = records.iter().map(|(n, _)| n.as_deref().unwrap()).collect();
    assert_eq!(
        names,
        [
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
    for (name, payload) in &records {
        assert_eq!(
            payload["type"].as_str(),
            name.as_deref(),
            "every Anthropic client keys on event == payload type"
        );
    }
    let text: String = records
        .iter()
        .filter_map(|(_, p)| p["delta"]["text"].as_str())
        .collect();
    assert_eq!(text, "15% of 340 is 51.");
    let delta = &records[6].1;
    assert_eq!(delta["delta"]["stop_reason"], "end_turn");
    assert_eq!(delta["usage"]["output_tokens"], 12);
    assert_eq!(records.last().unwrap().0.as_deref(), Some("message_stop"));
    assert_eq!(
        records
            .iter()
            .filter(|(n, _)| n.as_deref() == Some("message_stop"))
            .count(),
        1,
        "exactly one terminal event, at the end"
    );
}

#[test]
fn anthropic_stream_render_matches_the_captured_event_sequence() {
    let mut s = anthropic::AnthropicStream::new("claude-sonnet-4-6", "msg_01XQ7RdV");
    let mut events = s.message_start();
    let mut tail = text_chunk(" is 51.");
    with_usage(&mut tail, 25, 12);
    events.extend(s.on_chunk(&text_chunk("15% of 340")));
    events.extend(s.on_chunk(&tail));
    events.extend(s.finish(None));

    let capture = sse_records(ANTHROPIC_STREAM);
    let rendered: Vec<&str> = events.iter().map(|e| e.0.unwrap()).collect();
    let captured: Vec<&str> = capture.iter().map(|(n, _)| n.as_deref().unwrap()).collect();
    assert_eq!(rendered, captured, "event grammar drift");
    let text: String = events
        .iter()
        .filter_map(|e| e.1["delta"]["text"].as_str())
        .collect();
    assert_eq!(text, "15% of 340 is 51.");
    let md = events
        .iter()
        .find(|e| e.0 == Some("message_delta"))
        .expect("message_delta");
    assert_eq!(md.1["delta"]["stop_reason"], "end_turn");
    assert_eq!(md.1["usage"]["output_tokens"], 12);
    let ms = &events[0].1["message"];
    assert_eq!(ms["id"], capture[0].1["message"]["id"]);
    assert_eq!(ms["model"], capture[0].1["message"]["model"]);
}

#[test]
fn anthropic_error_capture_is_the_terminal_frame() {
    let want: Value = serde_json::from_str(ANTHROPIC_ERROR).unwrap();
    let mut s = anthropic::AnthropicStream::new("claude-sonnet-4-6", "msg_01XQ7RdV");
    s.message_start();
    s.on_chunk(&text_chunk("Hel"));
    let events = s.error("overloaded_error", "Overloaded");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, Some("error"));
    assert_eq!(
        events[0].1, want,
        "clients parse this envelope to choose retry-vs-fail"
    );
    // Terminal: after an error, NO success event can follow — a truncated
    // stream must not acquire a message_stop.
    assert!(s.finish(None).is_empty());
    assert!(s.on_chunk(&text_chunk("more")).is_empty());
}

#[test]
fn anthropic_buffered_render_carries_what_a_messages_client_reads() {
    let mut c = completion("Let me compute.", None, Some((25, 12)));
    c.tool_calls.push(ToolCall {
        id: "toolu_01A1B2".into(),
        name: "calculator".into(),
        arguments: "{\"expression\":\"340*0.15\"}".into(),
    });
    let v = anthropic::completion_to_anthropic(&c);
    assert_eq!(v["type"], "message");
    assert_eq!(v["id"], "msg_gen-1");
    assert_eq!(v["content"][0]["type"], "text");
    assert_eq!(v["content"][1]["type"], "tool_use");
    assert_eq!(
        v["content"][1]["input"],
        json!({"expression": "340*0.15"}),
        "object, not string"
    );
    assert_eq!(v["stop_reason"], "tool_use");
    assert_eq!(v["usage"]["input_tokens"], 25);
    assert_eq!(v["usage"]["output_tokens"], 12);

    let c = completion("part of the answer", Some("length"), None);
    assert_eq!(
        anthropic::completion_to_anthropic(&c)["stop_reason"],
        "max_tokens"
    );
}

// ----------------------------------------------------------- responses ----

#[test]
fn responses_request_capture_folds_without_error() {
    let req: responses::ResponsesRequest = serde_json::from_slice(RESPONSES_REQUEST.as_bytes())
        .expect("documented /v1/responses body must deserialize");
    let chat = req
        .to_chat_request()
        .expect("function items are representable");
    assert!(chat.wants_stream());
    let roles: Vec<&str> = chat
        .messages
        .iter()
        .map(|m| m["role"].as_str().unwrap())
        .collect();
    assert_eq!(roles, ["system", "user", "assistant", "tool"]);
    assert_eq!(chat.messages[2]["tool_calls"][0]["id"], "call_5o1dz");
    assert_eq!(
        chat.messages[2]["tool_calls"][0]["function"]["arguments"], "{\"expression\":\"340*0.15\"}",
        "already a string on this wire: forwarded, not re-encoded"
    );
    assert_eq!(chat.extra["max_tokens"], 128);
    assert_eq!(chat.extra["reasoning_effort"], "low");
    assert_eq!(chat.extra["tools"][0]["function"]["name"], "calculator");
    assert_eq!(chat.extra["tool_choice"], "auto");
}

#[test]
fn responses_stream_capture_is_a_contiguous_lifecycle() {
    let events: Vec<Value> = stream_lines(RESPONSES_STREAM)
        .iter()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let seq: Vec<u64> = events
        .iter()
        .map(|e| e["sequence_number"].as_u64().unwrap())
        .collect();
    assert_eq!(
        seq,
        (0..events.len() as u64).collect::<Vec<u64>>(),
        "no gaps, no duplicates"
    );
    let delta_text: String = events
        .iter()
        .filter(|e| e["type"] == "response.output_text.delta")
        .map(|e| e["delta"].as_str().unwrap())
        .collect();
    let last = events.last().unwrap();
    assert_eq!(last["type"], "response.completed");
    assert_eq!(
        delta_text, last["response"]["output_text"],
        "deltas assemble to the final text"
    );
    assert_eq!(last["response"]["status"], "completed");
    let usage = &last["response"]["usage"];
    assert_eq!(usage["input_tokens"], 31);
    assert_eq!(usage["output_tokens"], 8);
    assert_eq!(usage["total_tokens"], 39);
    let terminals = events
        .iter()
        .filter(|e| {
            ["response.completed", "response.incomplete", "error"]
                .contains(&e["type"].as_str().unwrap())
        })
        .count();
    assert_eq!(
        terminals, 1,
        "exactly one terminal object event, and it is last"
    );
}

#[test]
fn responses_stream_render_matches_the_captured_ledger() {
    let mut s = responses::ResponsesStream::new("gpt-5-mini", "resp_9kT2mQ");
    let mut events = s.start();
    let mut tail = text_chunk(" is 51.");
    with_usage(&mut tail, 31, 8);
    events.extend(s.on_chunk(&text_chunk("The answer")));
    events.extend(s.on_chunk(&tail));
    events.extend(s.finish());

    let capture: Vec<Value> = stream_lines(RESPONSES_STREAM)
        .iter()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let rendered: Vec<(&str, u64)> = events
        .iter()
        .map(|e| (e.0, e.1["sequence_number"].as_u64().unwrap()))
        .collect();
    let captured: Vec<(&str, u64)> = capture
        .iter()
        .map(|e| {
            (
                e["type"].as_str().unwrap(),
                e["sequence_number"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(rendered, captured, "event/type/sequence ledger drift");
    let last = &events.last().unwrap().1;
    assert_eq!(last["response"]["output_text"], "The answer is 51.");
    assert_eq!(last["response"]["usage"]["total_tokens"], 39);
    assert_eq!(last["response"]["output"][0]["type"], "message");
}

#[test]
fn responses_fail_is_one_error_frame_and_no_completed_event_can_follow() {
    let mut s = responses::ResponsesStream::new("gpt-5-mini", "resp_9kT2mQ");
    s.start();
    s.on_chunk(&text_chunk("Hi"));
    let events = s.fail("server_error", "upstream exploded mid-turn");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, "error");
    assert_eq!(events[0].1["type"], "error");
    assert_eq!(events[0].1["code"], "server_error");
    assert_eq!(events[0].1["message"], "upstream exploded mid-turn");
    // The ledger stays contiguous through the failure (created 0, in_progress
    // 1, item/part 2-3, delta 4 -> error 5): a client tracking sequence
    // numbers must not see a gap that looks like OUR corruption.
    assert_eq!(events[0].1["sequence_number"], 5);
    assert!(
        s.finish().is_empty(),
        "a cut Responses stream never completes"
    );
}

#[test]
fn responses_buffered_render_carries_what_a_responses_client_reads() {
    let mut c = completion("The answer is 51.", None, Some((31, 8)));
    c.id = "resp_gen1".into();
    c.tool_calls.push(weather_call());
    let v = responses::completion_to_response(&c);
    assert_eq!(v["object"], "response");
    assert_eq!(v["status"], "completed");
    assert_eq!(v["id"], "resp_gen1");
    assert_eq!(v["output"][0]["type"], "message");
    assert_eq!(v["output"][0]["content"][0]["text"], "The answer is 51.");
    assert_eq!(v["output"][1]["type"], "function_call");
    assert_eq!(v["output"][1]["arguments"], "{\"city\":\"SF\"}");
    assert_eq!(v["usage"]["total_tokens"], 39);
    assert_eq!(v["output_text"], "The answer is 51.");

    let c = completion("truncated", Some("length"), None);
    let v = responses::completion_to_response(&c);
    assert_eq!(v["status"], "incomplete");
    assert_eq!(v["incomplete_details"]["reason"], "max_output_tokens");
}

// --------------------------------------------------------------- gemini ----

#[test]
fn gemini_request_capture_folds_without_error() {
    let req: gemini::GeminiRequest = serde_json::from_slice(GEMINI_REQUEST.as_bytes())
        .expect("documented :generateContent body must deserialize");
    let chat = req
        .to_chat_request("gemini-2.5-flash", false)
        .expect("function history is representable");
    assert_eq!(chat.model, "gemini-2.5-flash");
    assert!(!chat.wants_stream());
    let roles: Vec<&str> = chat
        .messages
        .iter()
        .map(|m| m["role"].as_str().unwrap())
        .collect();
    assert_eq!(roles, ["system", "user", "assistant", "tool"]);
    assert_eq!(chat.messages[2]["tool_calls"][0]["id"], "fc_71a");
    assert_eq!(
        chat.messages[2]["tool_calls"][0]["function"]["arguments"], "{\"city\":\"SF\"}",
        "args object re-spelled as the chat wire's string"
    );
    assert_eq!(chat.messages[3]["tool_call_id"], "fc_71a");
    assert_eq!(chat.extra["temperature"], 0.2);
    assert_eq!(chat.extra["max_tokens"], 128);
    assert_eq!(chat.extra["stop"], json!(["END"]));
    assert_eq!(chat.extra["tools"][0]["function"]["name"], "get_weather");
    assert_eq!(chat.extra["tool_choice"], "auto");
}

/// The capture's frames as full `GenerateContentResponse` payloads.
fn gemini_frames() -> Vec<Value> {
    sse_records(GEMINI_STREAM)
        .into_iter()
        .map(|(_, payload)| payload)
        .collect()
}

fn candidate(payload: &Value) -> &Value {
    &payload["candidates"][0]
}

#[test]
fn gemini_stream_capture_terminates_on_finish_reason_not_a_sentinel() {
    let frames = gemini_frames();
    assert_eq!(frames.len(), 2);
    for f in &frames {
        assert_eq!(candidate(f)["index"], 0);
        assert_eq!(candidate(f)["content"]["role"], "model");
        // Vendor identity rides every frame on this dialect.
        assert_eq!(f["modelVersion"], "gemini-2.5-flash");
    }
    let with_finish: Vec<usize> = frames
        .iter()
        .enumerate()
        .filter(|(_, f)| candidate(f).get("finishReason").is_some())
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        with_finish,
        vec![frames.len() - 1],
        "exactly one frame carries finishReason and it is last — the only terminator this dialect has"
    );
    assert_eq!(candidate(&frames[1])["finishReason"], "STOP");
    assert!(
        frames[0].get("usageMetadata").is_none(),
        "usage is terminal-frame only"
    );
    let usage = &frames[1]["usageMetadata"];
    assert_eq!(usage["promptTokenCount"], 34);
    assert_eq!(usage["candidatesTokenCount"], 12);
    assert_eq!(usage["totalTokenCount"], 46);
    let fc = &candidate(&frames[1])["content"]["parts"][0]["functionCall"];
    assert_eq!(fc["name"], "get_weather");
    assert_eq!(
        fc["args"],
        json!({"city": "SF"}),
        "args are an OBJECT on this wire"
    );
    assert_eq!(fc["id"], "fc_71a");
}

#[test]
fn gemini_stream_render_projects_the_captured_frames() {
    let mut s = gemini::GeminiStream::new("gemini-2.5-flash");
    let mut dst = BytesMut::new();
    s.on_chunk(&text_chunk("Foggy today"), &mut dst);
    let mut opener = text_chunk("");
    opener.tool_calls.push(ToolCallDelta {
        index: 0,
        id: Some("fc_71a".into()),
        name: Some("get_weather".into()),
        arguments: String::new(),
    });
    s.on_chunk(&opener, &mut dst);
    let mut args = text_chunk("");
    args.tool_calls.push(ToolCallDelta {
        index: 0,
        id: None,
        name: None,
        arguments: "{\"city\":\"SF\"}".into(),
    });
    with_usage(&mut args, 34, 12);
    s.on_chunk(&args, &mut dst);
    s.finish(&mut dst);

    let rendered: Vec<Value> = sse_records_bytes(&dst)
        .into_iter()
        .map(|(_, p)| p)
        .collect();
    let capture = gemini_frames();
    assert_eq!(rendered.len(), capture.len());
    for (got, want) in rendered.iter().zip(&capture) {
        assert_eq!(
            got["candidates"], want["candidates"],
            "candidate grammar drift"
        );
        if let Some(u) = want.get("usageMetadata") {
            assert_eq!(&got["usageMetadata"], u);
        }
    }
}

#[test]
fn gemini_fail_frame_carries_no_finish_reason() {
    let mut s = gemini::GeminiStream::new("gemini-2.5-flash");
    let mut dst = BytesMut::new();
    s.on_chunk(&text_chunk("Foggy"), &mut dst);
    let mark = dst.len();
    s.fail(&ProviderError::bad_gateway("connection reset"), &mut dst);
    let frame = String::from_utf8(dst[mark..].to_vec()).unwrap();
    assert!(
        !frame.contains("finishReason"),
        "a cut Gemini stream cannot read as a finished turn"
    );
    assert!(!frame.contains("[DONE]"));
    let v: Value = serde_json::from_str(
        frame
            .trim_end()
            .trim_end_matches('\n')
            .strip_prefix("data: ")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(v["error"]["code"], 502);
    assert_eq!(v["error"]["message"], "connection reset");
    assert_eq!(v["error"]["status"], "INTERNAL");
    let mut after = BytesMut::new();
    s.finish(&mut after);
    assert!(
        after.is_empty(),
        "finish() after fail() writes no terminal frame"
    );
}

#[test]
fn gemini_buffered_render_carries_what_a_generatecontent_client_reads() {
    let mut c = completion("Foggy today", Some("length"), Some((34, 12)));
    c.model = "gemini-2.5-flash".into();
    c.tool_calls.push(weather_call());
    let v = gemini::completion_to_generate_content(&c);
    let cand = &v["candidates"][0];
    assert_eq!(cand["finishReason"], "MAX_TOKENS");
    assert_eq!(cand["content"]["parts"][0]["text"], "Foggy today");
    assert_eq!(
        cand["content"]["parts"][1]["functionCall"]["args"],
        json!({"city": "SF"}),
        "IR string arguments leave as the dialect's object"
    );
    assert_eq!(v["usageMetadata"]["promptTokenCount"], 34);
    assert_eq!(v["usageMetadata"]["totalTokenCount"], 46);
    assert_eq!(v["modelVersion"], "gemini-2.5-flash");
}

// ---------------------------------------------------------------- live ----

/// MANUAL, ignored by default. Suspecting vendor drift? Run this by hand
/// against a locally running proxy and eyeball the frame sequence it prints
/// against `docs/compliance/fixtures/openai_chat_stream.frames.jsonl`:
///
/// ```text
/// X2API_CLIENT_API_KEY=<the key your local proxy gates on> \
/// cargo test -p x2api-dialects --test vendor_fixtures -- --ignored --nocapture
/// ```
///
/// Optional env: `X2API_BIND` / `X2API_PORT` (the proxy's own listeners,
/// default `127.0.0.1:10080`) and `X2API_LIVE_MODEL` (default
/// `gpt-4o-mini`; whatever you set must be routable by that proxy's upstream
/// config). Plain HTTP over a loopback socket on purpose — this crate has no
/// TLS client, and the test exists to compare the proxy's OWN egress shape,
/// not to add a dependency for a test nobody runs in CI.
#[test]
#[ignore = "requires a locally running proxy + credential; see the doc comment"]
fn live_chat_stream_frames_for_eyeball_comparison() {
    let key = env::var("X2API_CLIENT_API_KEY")
        .or_else(|_| env::var("CLIENT_API_KEY"))
        .expect("set X2API_CLIENT_API_KEY (or CLIENT_API_KEY) to the key the proxy gates on");
    let host = env::var("X2API_BIND").unwrap_or_else(|_| "127.0.0.1".into());
    // A wildcard bind is not connectable; loopback reaches the same listener.
    let host = match host.as_str() {
        "0.0.0.0" | "::" => "127.0.0.1".to_string(),
        other => other.to_string(),
    };
    let port: u16 = env::var("X2API_PORT")
        .ok()
        .map(|p| p.parse().expect("X2API_PORT must be a number"))
        .unwrap_or(10080);
    let model = env::var("X2API_LIVE_MODEL").unwrap_or_else(|_| "gpt-4o-mini".into());
    let body = json!({
        "model": model,
        "messages": [{ "role": "user", "content": "Reply with exactly: OK" }],
        "stream": true,
        "stream_options": { "include_usage": true },
    })
    .to_string();
    let request = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: {host}:{port}\r\n\
         Authorization: Bearer {key}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );

    let mut stream = TcpStream::connect((host.as_str(), port))
        .expect("cannot reach the local proxy — start it first");
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    stream.flush().unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("stream aborted");
    let text = String::from_utf8_lossy(&response).into_owned();
    let (_, body) = text
        .split_once("\r\n\r\n")
        .expect("no HTTP/1.1 head — is the proxy speaking TLS on this port?");
    println!("--- live frames ---\n{body}\n--- end ---");
    assert!(
        body.contains("data: "),
        "no SSE frames at all; the live shape is not even chat-framed"
    );
}
