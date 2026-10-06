//! # opencode2api-dialects::chat
//!
//! The chat-completions dialect bridge. Clients speaking this dialect talk
//! **identity** with the IR (the IR *is* this shape), so the translation work
//! is small — but it is still *the dialect's code*, kept out of the server:
//!
//! - `parse_request` — body → IR, with the dialect's 400 wording.
//! - `completion_to_wire` / `append_chunk_frame` — IR → verbatim-first JSON
//!   (the `raw` fast path lives here, because *this dialect* can use it).
//! - `CompatStream` — terminal-frame grammar: `[DONE]` on clean end, the
//!   truncation chunk **without** `[DONE]` on failure, and the id/model
//!   echo tracking both frames need.
//!
//! Vendor-compat quirks for OpenAI-shaped upstreams (field renames, cost
//! strips, normalization gates) belong HERE as code — never as server
//! branches, never as config.

use bytes::{BufMut, BytesMut};
use opencode2api_kit::sse;
use opencode2api_kit::{ChatChunk, ChatRequest, Completion, ProviderError};
use serde_json::{Map, Value, json};

/// Parse a `/v1/chat/completions` body into IR.
pub fn parse_request(body: &[u8]) -> Result<ChatRequest, ProviderError> {
    serde_json::from_slice(body)
        .map_err(|e| ProviderError::bad_request(format!("invalid chat completions body: {e}")))
}

/// Buffered completion -> the wire JSON a chat-dialect client receives:
/// verbatim `raw` when the provider supplied one (zero re-shaping), else
/// synthesized from the normalized fields.
pub fn completion_to_wire(c: &Completion) -> Value {
    if let Some(raw) = &c.raw {
        return raw.clone();
    }
    let mut choice = Map::new();
    let mut message = Map::new();
    message.insert("role".into(), "assistant".into());
    message.insert("content".into(), c.text.clone().into());
    if let Some(refusal) = &c.refusal {
        message.insert("refusal".into(), refusal.clone().into());
    }
    if let Some(reasoning) = &c.reasoning {
        message.insert("reasoning_content".into(), reasoning.clone().into());
    }
    if !c.tool_calls.is_empty() {
        let calls: Vec<Value> = c
            .tool_calls
            .iter()
            .enumerate()
            .map(|(index, call)| {
                tool_call_fragment(index as u32, &call.id, &call.name, &call.arguments)
            })
            .collect();
        message.insert("tool_calls".into(), calls.into());
    }
    choice.insert("message".into(), Value::Object(message));
    choice.insert("index".into(), 0.into());
    choice.insert(
        "finish_reason".into(),
        match &c.finish_reason {
            Some(r) => Value::String(r.clone()),
            None => Value::Null,
        },
    );
    let mut root = Map::new();
    root.insert("id".into(), c.id.clone().into());
    root.insert("object".into(), "chat.completion".into());
    root.insert(
        "created".into(),
        opencode2api_kit::util::now_epoch_secs().into(),
    );
    root.insert("model".into(), c.model.clone().into());
    root.insert("choices".into(), vec![Value::Object(choice)].into());
    if let Some(u) = &c.usage
        && let Ok(v) = serde_json::to_value(u)
    {
        root.insert("usage".into(), v);
    }
    Value::Object(root)
}

fn tool_call_fragment(index: u32, id: &str, name: &str, arguments: &str) -> Value {
    json!({
        "index": index,
        "id": id,
        "type": "function",
        "function": {
            "name": name,
            "arguments": arguments,
        }
    })
}

/// IR chunk -> a complete `data:` frame (raw-first, same rule). Writing
/// rather than returning a `Value` is the point: when the provider kept the
/// vendor's bytes, this dialect's client wants exactly those bytes, and
/// re-parsing them into a document only to print it back is work with no
/// output. Synthesis (no `raw`) is the fallback, not the path.
pub fn append_chunk_frame(chunk: &ChatChunk, dst: &mut BytesMut) {
    match &chunk.raw {
        Some(raw) => sse::append_data_frame(dst, raw),
        None => {
            let payload =
                serde_json::to_vec(&synthesize_chunk(chunk)).unwrap_or_else(|_| b"{}".to_vec());
            sse::append_data_frame(dst, &payload);
        }
    }
}

/// The chunk JSON this dialect emits when the provider had no verbatim bytes
/// to hand over (buffered upstream answering a streamed request).
pub fn synthesize_chunk(chunk: &ChatChunk) -> Value {
    let mut delta = Map::new();
    if !chunk.text.is_empty() {
        delta.insert("content".into(), chunk.text.clone().into());
    }
    if !chunk.refusal.is_empty() {
        delta.insert("refusal".into(), chunk.refusal.clone().into());
    }
    if !chunk.reasoning.is_empty() {
        delta.insert("reasoning_content".into(), chunk.reasoning.clone().into());
    }
    if !chunk.tool_calls.is_empty() {
        let calls: Vec<Value> = chunk
            .tool_calls
            .iter()
            .map(|call| {
                tool_call_fragment(
                    call.index,
                    call.id.as_deref().unwrap_or_default(),
                    call.name.as_deref().unwrap_or_default(),
                    &call.arguments,
                )
            })
            .collect();
        delta.insert("tool_calls".into(), calls.into());
    }
    let mut choice = Map::new();
    choice.insert("index".into(), 0.into());
    choice.insert("delta".into(), Value::Object(delta));
    choice.insert(
        "finish_reason".into(),
        match &chunk.finish_reason {
            Some(r) => Value::String(r.clone()),
            None => Value::Null,
        },
    );
    let mut root = Map::new();
    root.insert("id".into(), chunk.id.clone().into());
    root.insert("object".into(), "chat.completion.chunk".into());
    root.insert(
        "created".into(),
        opencode2api_kit::util::now_epoch_secs().into(),
    );
    root.insert("model".into(), chunk.model.clone().into());
    root.insert("choices".into(), vec![Value::Object(choice)].into());
    if let Some(usage) = &chunk.usage
        && let Ok(value) = serde_json::to_value(usage)
    {
        root.insert("usage".into(), value);
    }
    Value::Object(root)
}

/// Stream terminalities for this dialect: identity relay plus the two frames
/// that encode "how it ended".
pub struct CompatStream {
    id: String,
    model: String,
}

impl CompatStream {
    /// `model` seeds from the client's request (best echo); the first chunk
    /// carrying a model wins from then on.
    pub fn new(model: &str) -> Self {
        Self {
            id: String::new(),
            model: model.to_string(),
        }
    }

    /// Relay one chunk frame (identity serialization, no re-shaping).
    pub fn on_chunk(&mut self, chunk: &ChatChunk, dst: &mut BytesMut) {
        if !chunk.id.is_empty() {
            self.id.clone_from(&chunk.id);
        }
        if !chunk.model.is_empty() {
            self.model.clone_from(&chunk.model);
        }
        append_chunk_frame(chunk, dst);
    }

    /// Clean end: the `[DONE]` sentinel — the only dialect with one.
    pub fn finish(&self, dst: &mut BytesMut) {
        dst.put_slice(sse::DONE_FRAME);
    }

    /// Terminal failure: a `chat.completion.chunk` carrying a top-level
    /// `error` object — deliberately WITHOUT `[DONE]`, so a truncated answer
    /// cannot look complete.
    pub fn fail(&self, err: &ProviderError, dst: &mut BytesMut) {
        let frame = json!({
            "id": if self.id.is_empty() { Value::Null } else { json!(&self.id) },
            "object": "chat.completion.chunk",
            "created": opencode2api_kit::util::now_epoch_secs(),
            "model": self.model,
            "choices": [{ "index": 0, "delta": {}, "finish_reason": Value::Null }],
            "error": {
                "message": err.shown_message(),
                "type": err.error_type,
                "param": Value::Null,
                "code": Value::Null,
            },
        });
        let payload = serde_json::to_vec(&frame).unwrap_or_else(|_| b"{}".to_vec());
        sse::append_data_frame(dst, &payload);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_round_trips_a_plain_body() {
        let req = parse_request(
            json!({"model":"m","messages":[{"role":"user","content":"x"}],"stream":true,"temperature":0.4})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
        assert!(req.wants_stream());
        assert_eq!(req.extra["temperature"], 0.4);
    }

    #[test]
    fn parse_failure_is_dialect_400() {
        let e = parse_request(b"{oops").unwrap_err();
        assert_eq!(e.status, 400);
        assert!(e.message.starts_with("invalid chat completions body"));
    }

    #[test]
    fn completion_prefers_verbatim_raw() {
        let c = Completion {
            refusal: None,
            id: "x".into(),
            model: "m".into(),
            text: "t".into(),
            finish_reason: None,
            usage: None,
            tool_calls: Vec::new(),
            raw: Some(json!({"id": "x", "vendor_only": true})),
            reasoning: None,
        };
        assert_eq!(completion_to_wire(&c)["vendor_only"], true);
    }

    #[test]
    fn completion_without_raw_synthesizes() {
        let c = Completion {
            refusal: None,
            id: "x".into(),
            model: "m".into(),
            text: "t".into(),
            finish_reason: Some("stop".into()),
            usage: Some(opencode2api_kit::Usage {
                prompt_tokens: 1,
                completion_tokens: 2,
                ..Default::default()
            }),
            tool_calls: Vec::new(),
            raw: None,
            reasoning: None,
        };
        let v = completion_to_wire(&c);
        assert_eq!(v["object"], "chat.completion");
        assert_eq!(v["choices"][0]["message"]["content"], "t");
        assert_eq!(v["usage"]["completion_tokens"], 2);
    }

    #[test]
    fn synthesized_completion_and_chunk_preserve_refusal_text() {
        let completion = Completion {
            refusal: Some("I cannot assist".into()),
            id: "x".into(),
            model: "m".into(),
            text: "partial answer".into(),
            finish_reason: Some("refusal".into()),
            usage: None,
            tool_calls: Vec::new(),
            raw: None,
            reasoning: None,
        };
        let buffered = completion_to_wire(&completion);
        assert_eq!(
            buffered["choices"][0]["message"]["refusal"],
            "I cannot assist"
        );

        let chunk = ChatChunk {
            refusal: "I cannot assist".into(),
            id: "x".into(),
            model: "m".into(),
            text: "partial answer".into(),
            finish_reason: Some("refusal".into()),
            usage: None,
            tool_calls: Vec::new(),
            raw: None,
            reasoning: String::new(),
        };
        let streamed = synthesize_chunk(&chunk);
        assert_eq!(
            streamed["choices"][0]["delta"]["refusal"],
            "I cannot assist"
        );
        assert_eq!(streamed["choices"][0]["finish_reason"], "refusal");
    }

    #[test]
    fn clean_stream_ends_with_done() {
        let s = CompatStream::new("m");
        let mut dst = BytesMut::new();
        s.finish(&mut dst);
        assert_eq!(&dst[..], sse::DONE_FRAME);
    }

    #[test]
    fn failure_truncates_without_done_and_echoes_identity() {
        let mut s = CompatStream::new("req-model");
        let mut dst = BytesMut::new();
        s.on_chunk(
            &ChatChunk {
                refusal: String::new(),
                id: "gen-7".into(),
                model: "real-model".into(),
                text: "hel".into(),
                finish_reason: None,
                usage: None,
                tool_calls: Vec::new(),
                raw: None,
                reasoning: String::new(),
            },
            &mut dst,
        );
        let before_fail = dst.len();
        s.fail(&ProviderError::bad_gateway("died"), &mut dst);
        let text = String::from_utf8(dst[before_fail..].to_vec()).unwrap();
        assert!(text.contains("\"error\""));
        assert!(!text.contains("[DONE]"));
        let v: Value = serde_json::from_str(
            text.trim_end()
                .trim_end_matches('\n')
                .trim_start_matches("data: "),
        )
        .unwrap();
        assert_eq!(v["id"], "gen-7");
        assert_eq!(v["model"], "real-model");
    }

    #[test]
    fn terminal_chunk_synthesizes_tool_usage_and_reasoning_metadata() {
        let chunk = ChatChunk {
            refusal: String::new(),
            id: "gen-8".into(),
            model: "test-model".into(),
            text: String::new(),
            finish_reason: Some("tool_calls".into()),
            usage: Some(opencode2api_kit::Usage {
                prompt_tokens: 11,
                completion_tokens: 7,
                ..Default::default()
            }),
            tool_calls: vec![opencode2api_kit::ToolCallDelta {
                index: 3,
                id: Some("call_3".into()),
                name: Some("lookup_weather".into()),
                arguments: "{\"city\":\"Paris\"}".into(),
            }],
            raw: None,
            reasoning: "I should check the forecast.".into(),
        };

        let mut wire = synthesize_chunk(&chunk);
        wire.as_object_mut().unwrap().remove("created");
        assert_eq!(
            wire,
            json!({
                "id": "gen-8",
                "object": "chat.completion.chunk",
                "model": "test-model",
                "choices": [{
                    "index": 0,
                    "delta": {
                        "reasoning_content": "I should check the forecast.",
                        "tool_calls": [{
                            "index": 3,
                            "id": "call_3",
                            "type": "function",
                            "function": {
                                "name": "lookup_weather",
                                "arguments": "{\"city\":\"Paris\"}"
                            }
                        }]
                    },
                    "finish_reason": "tool_calls"
                }],
                "usage": {
                    "prompt_tokens": 11,
                    "completion_tokens": 7
                }
            })
        );
    }

    #[test]
    fn content_only_chunk_omits_empty_optional_fields() {
        let chunk = ChatChunk {
            refusal: String::new(),
            id: "gen-9".into(),
            model: "test-model".into(),
            text: "hello".into(),
            finish_reason: Some("stop".into()),
            usage: None,
            tool_calls: Vec::new(),
            raw: None,
            reasoning: String::new(),
        };

        let mut wire = synthesize_chunk(&chunk);
        wire.as_object_mut().unwrap().remove("created");
        assert_eq!(
            wire,
            json!({
                "id": "gen-9",
                "object": "chat.completion.chunk",
                "model": "test-model",
                "choices": [{
                    "index": 0,
                    "delta": {"content": "hello"},
                    "finish_reason": "stop"
                }]
            })
        );
    }
}
