//! # opencode2api-dialects::anthropic
//!
//! The inbound-dialect bridge between Anthropic's Messages API and the
//! OpenAI-shaped IR that `opencode2api-kit` defines. Pure JSON in, pure JSON out:
//! **no I/O, no axum, no reqwest** — SSE framing of the returned events is
//! `opencode2api-server`'s job.
//!
//! Scope: **text, function tools, and user-turn media (images + documents)**;
//! rejection is still part of the contract. The fold answers `Err(400)` —
//! never a silently-degraded IR — for anything it cannot represent back:
//! - `search_result` blocks, top-level `thinking`, and media in the positions
//!   the chat dialect has no field for: an image/document in an `assistant`
//!   turn, or one nested inside `tool_result` content (chat tool content is
//!   `string | text parts`, so a computer-use screenshot has nowhere legal to
//!   go). A `document` also refuses its `url` and `file` sources, and any
//!   block claiming what the wire cannot honour (`citations` enabled, a
//!   non-empty `title`/`context`, `oversized_image: "error"`) — a field left
//!   at its documented default claims nothing and folds. Ordinary
//!   `tool_use`/successful `tool_result` blocks and user function tools fold,
//!   but the bridge refuses rather than changing meaning: a `tool_result` with
//!   `is_error: true`; a tool whose present `type` is not `"function"` or whose
//!   name is not a string; a `tool_choice.type` outside `auto|any|none|tool`
//!   or a named tool choice with no name; a message with no `content`; and a
//!   `text` block without a string `text`. Neither
//!   `docs/compliance/anthropic.md` nor the pinned request fixture
//!   `docs/compliance/fixtures/anthropic_messages_request.json` contains an
//!   `is_error`, and chat has no field that preserves it, so folding would
//!   turn a claimed failure into apparent success. Streamed tool frames are
//!   HELD until `finish()` and emitted there one block at a time, refusal
//!   first: Anthropic never has two content blocks open, and holding is what
//!   keeps one call to exactly one `tool_use` block (see [`AnthropicStream`]).
//! - outbound, `stop_reason: "tool_use"` is reported only when tool blocks
//!   were actually emitted. `"tool_use"` is a *promise* of blocks, so an
//!   upstream that claims `tool_calls` while sending none still collapses to
//!   `end_turn` — a waiting client is worse than a finished turn.
//!
//! A service that needs MORE (documents by URL, audio, thinking, server-side
//! tools, media OUTPUT) extends THIS crate — the fold, the event machine, the
//! stop map — which is what the crate exists for.
//!
//! Known asymmetry, documented not hidden: `message_start` carries the
//! minted `opencode2api-…` id before any provider chunk exists, so it can differ
//! from the id an OpenAI-dialect client sees for the same completion. The
//! terminal `message_delta` is authoritative for usage; the head event's
//! zeros are spec-legal placeholders.

use opencode2api_kit::sse::EventSink;
use opencode2api_kit::{
    ChatChunk, ChatRequest, Completion, ProviderError, Usage, data_url, file_part, image_part,
    parts_content, text_part,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

/// The one per-frame event, typed rather than built as a document: a text
/// delta is emitted once per upstream chunk, so the `Map` allocations a
/// `json!` costs are paid on every token of every stream. Shape is defined
/// HERE once — the `Value` API below renders it through the same struct.
#[derive(Debug, Clone, PartialEq, Serialize)]
struct TextDeltaEvent<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    index: u32,
    delta: TextDeltaBody<'a>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct TextDeltaBody<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    text: &'a str,
}

impl<'a> TextDeltaEvent<'a> {
    fn new(text: &'a str, index: usize) -> Self {
        Self {
            kind: "content_block_delta",
            index: index as u32,
            delta: TextDeltaBody {
                kind: "text_delta",
                text,
            },
        }
    }
}

/// A thinking delta: same envelope as a text delta, different body key. Its
/// own struct for the same reason `TextDeltaEvent` exists — one per streamed
/// token, so the `Map` a `json!` allocates is paid on every frame.
#[derive(Debug, Clone, PartialEq, Serialize)]
struct ThinkingDeltaEvent<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    index: u32,
    delta: ThinkingDeltaBody<'a>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct ThinkingDeltaBody<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    thinking: &'a str,
}

impl<'a> ThinkingDeltaEvent<'a> {
    fn new(thinking: &'a str, index: usize) -> Self {
        Self {
            kind: "content_block_delta",
            index: index as u32,
            delta: ThinkingDeltaBody {
                kind: "thinking_delta",
                thinking,
            },
        }
    }
}

/// The block-opening event, emitted at most once per stream from either the
/// chunk path or `finish` — defined once so the two cannot drift.
fn block_start(index: usize) -> Value {
    json!({
        "type": "content_block_start",
        "index": index,
        "content_block": { "type": "text", "text": "" },
    })
}

/// The thinking block that precedes text. Anthropic numbers content blocks in
/// ONE sequence, so opening thinking takes index 0 and pushes the text block
/// (and every tool block after it) up by one — a client that indexes on the
/// order it was told cannot be served a text block at 0 twice.
fn thinking_start(index: usize) -> Value {
    json!({
        "type": "content_block_start",
        "index": index,
        "content_block": { "type": "thinking", "thinking": "", "signature": "" },
    })
}

/// Collects events as `Value`s, for callers that want documents instead of
/// frames (the `on_chunk` API, and every test in this crate).
#[derive(Default)]
struct Collected(Vec<AnthropicEvent>);

impl EventSink for Collected {
    fn event<T: Serialize + ?Sized>(&mut self, name: Option<&'static str>, payload: &T) {
        self.0.push(AnthropicEvent(
            name,
            serde_json::to_value(payload).unwrap_or_else(|_| json!({})),
        ));
    }
}

/// One named SSE event awaiting framing (`None` = anonymous `data:` event;
/// every real Anthropic event is named, the optionality exists for
/// forward-compat with unnamed stream data).
#[derive(Debug, Clone, PartialEq)]
pub struct AnthropicEvent(pub Option<&'static str>, pub Value);

/// A parsed `/v1/messages` body (only what the bridge needs is typed; the
/// rest is either forwarded where OpenAI also spells it, or rejected).
#[derive(Debug, Clone, Deserialize)]
pub struct AnthropicRequest {
    pub model: String,
    pub messages: Vec<Value>,
    #[serde(default)]
    pub system: Option<Value>,
    #[serde(default)]
    pub max_tokens: Option<u64>,
    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl AnthropicRequest {
    /// Fold the Anthropic body into the OpenAI-shaped IR, or reject shapes
    /// whose loss would make the answer wrong rather than merely reduced.
    pub fn to_chat_request(&self) -> Result<ChatRequest, ProviderError> {
        // The distinction this refusal draws is between asking for reasoning
        // and carrying it. A `thinking` block on an assistant turn is HISTORY
        // and folds (see `fold_message`, which lifts it to
        // `reasoning_content`). This top-level knob is a REQUEST for the
        // upstream to reason, and the chat wire spells that per vendor
        // (`thinking:{type:"enabled"}` on DeepSeek, `reasoning_effort` on GLM)
        // — a bridge that guessed a spelling would be doing provider work and
        // silently changing which upstream the request suits. Refusing is
        // honest; forwarding it unmapped would be a no-op the client believes
        // took effect.
        for key in ["thinking"] {
            if self.extra.contains_key(key) {
                return Err(ProviderError::bad_request(format!(
                    "{key} requests reasoning from the upstream, which the chat wire spells \
                     per vendor and this bridge cannot translate without choosing a vendor \
                     (set it in the provider crate, or send it on the chat dialect)"
                )));
            }
        }

        let mut messages = Vec::with_capacity(self.messages.len() + 1);
        if let Some(sys) = &self.system {
            let text = content_to_text(sys, "system")?;
            if !text.is_empty() {
                let mut m = Map::new();
                m.insert("role".into(), json!("system"));
                m.insert("content".into(), json!(text));
                messages.push(Value::Object(m));
            }
        }
        for msg in &self.messages {
            let role = match msg.get("role").and_then(Value::as_str) {
                Some("assistant") => "assistant",
                // user/anything-else arrive as user turns.
                _ => "user",
            };
            let content = msg
                .get("content")
                .ok_or_else(|| ProviderError::bad_request("message has no content"))?;
            fold_message(role, content, &mut messages)?;
        }

        // Pass the sampling knobs OpenAI also spells; Anthropic-only names
        // (anthropic-version etc.) are dropped rather than leaked upstream.
        let mut extra = Map::new();
        if let Some(tools) = self.extra.get("tools").and_then(Value::as_array) {
            let folded = fold_tools(tools)?;
            if !folded.is_empty() {
                extra.insert("tools".into(), Value::Array(folded));
            }
        }
        if let Some(choice) = self.extra.get("tool_choice") {
            let (folded, parallel) = tool_choice_to_openai(choice)?;
            extra.insert("tool_choice".into(), folded);
            if let Some(parallel) = parallel {
                extra.insert("parallel_tool_calls".into(), json!(parallel));
            }
        }
        for key in ["temperature", "top_p", "top_k"] {
            if let Some(v) = self.extra.get(key) {
                extra.insert(key.to_string(), v.clone());
            }
        }
        if let Some(mt) = self.max_tokens {
            extra.insert("max_tokens".into(), json!(mt));
        }
        if let Some(stop) = self.extra.get("stop_sequences") {
            extra.insert("stop".into(), stop.clone());
        }

        Ok(ChatRequest {
            model: self.model.clone(),
            messages,
            stream: self.stream,
            extra,
        })
    }
}

/// Refuse a media block that CLAIMS anything beyond its payload — the field
/// list is the reason a document/image fold is allowed to exist at all.
///
/// One rule, stated once: a field left at its documented default claims
/// nothing and folds fine; a field set to something the chat dialect cannot
/// express changes what the client asked for, and dropping it would answer a
/// different request than the one that arrived.
///  * `citations: {"enabled": false}` is that field's default → accepted.
///    Enabled → refused: citations change the RESPONSE the client parses.
///  * `title` / `context` are labels with no chat counterpart — absent or
///    empty claims nothing, a value is a claim.
///  * `transformations.oversized_image`: `downsize` IS the upstream's default
///    behavior, so it claims nothing and folds; `error` demands a failure the
///    chat wire cannot ask for, and dropping it would turn a promised error
///    into a silent resize. An unreadable value refuses rather than guessing.
///
/// `cache_control` is deliberately NOT here, and it is NOT the same case as
/// OpenAI's `prompt_cache_breakpoint`: Anthropic's shape is
/// `{type:"ephemeral",ttl}` with no chat counterpart, so there is nothing to
/// forward and refusing it would 400 most real Claude traffic (Claude Code
/// sets it on nearly every block) for zero fidelity gain — it is dropped and
/// said so. The responses module, by contrast, FORWARDS `prompt_cache_breakpoint`,
/// because chat's own media parts carry that exact field.
fn reject_unfolding_claims(block: &Value, kind: &str) -> Result<(), ProviderError> {
    if let Some(citations) = block.get("citations").filter(|v| !v.is_null())
        && citations.get("enabled").and_then(Value::as_bool) != Some(false)
    {
        return Err(ProviderError::bad_request(format!(
            "{kind} block citations are not representable: the chat media parts have no \
             such field and enabling citations changes the response shape (declare this \
             dialect native and take the fidelity lane)"
        )));
    }
    for key in ["title", "context"] {
        let claims_something = block
            .get(key)
            .is_some_and(|v| !v.is_null() && v.as_str().is_none_or(|s| !s.is_empty()));
        if claims_something {
            return Err(ProviderError::bad_request(format!(
                "{kind} block {key} is not representable: the chat media parts have no \
                 such field (declare this dialect native and take the fidelity lane)"
            )));
        }
    }
    if let Some(t) = block.get("transformations").filter(|v| !v.is_null()) {
        // Absent, or the default level, claims nothing the chat wire cannot do.
        let asks_for_a_refusal = match t.get("oversized_image").and_then(Value::as_str) {
            None | Some("downsize") => false,
            Some(_) => true,
        };
        if asks_for_a_refusal || !t.is_object() {
            return Err(ProviderError::bad_request(format!(
                "{kind} block transformations are not representable: oversized_image can \
                 demand an error instead of a resize, and dropping that would flip the \
                 client's failure contract (take the fidelity lane)"
            )));
        }
    }
    Ok(())
}

/// Anthropic's closed `media_type` enum for image sources (vision guide,
/// "Supported formats"). Enforced because it is THIS dialect's own input rule:
/// a client sending `image/jpg` or `image/heic` to `/v1/messages` is already
/// broken against the API it thinks it is talking to, and letting it through
/// would compose a data URL that every OpenAI-shaped vendor then rejects for
/// syntax it cannot even see.
const ANTHROPIC_MEDIA_TYPES: &[&str] = &["image/jpeg", "image/png", "image/gif", "image/webp"];

/// One Anthropic `image` block -> the IR's `image_url` part.
///
/// Anthropic has no `detail`, so only the URL is composed — the fold never
/// invents a sizing level the client did not ask for. Of the three documented
/// source kinds, two fold: `base64` becomes an inline data URL, `url` passes
/// through as the vendor-side fetch the chat dialect documents. The third,
/// `file`, is a Files-API id: chat's `image_url` has no `file_id` field to
/// carry it, and resolving one would take a vendor round-trip with a
/// credential, which is provider-crate work — a bridge does no I/O.
fn image_block_to_part(block: &Value) -> Result<Value, ProviderError> {
    reject_unfolding_claims(block, "image")?;
    let source = block
        .get("source")
        .ok_or_else(|| ProviderError::bad_request("image block has no source"))?;
    match source.get("type").and_then(Value::as_str).unwrap_or("?") {
        "base64" => {
            let media = source
                .get("media_type")
                .and_then(Value::as_str)
                .unwrap_or("");
            if !ANTHROPIC_MEDIA_TYPES.contains(&media) {
                return Err(ProviderError::bad_request(format!(
                    "image media_type {media:?} is not one of {}",
                    ANTHROPIC_MEDIA_TYPES.join(", ")
                )));
            }
            let data = source.get("data").and_then(Value::as_str).unwrap_or("");
            let url = data_url(media, data)?;
            image_part(&url, None)
        }
        "url" => {
            let url = source
                .get("url")
                .and_then(Value::as_str)
                .ok_or_else(|| ProviderError::bad_request("image url source has no url"))?;
            image_part(url, None)
        }
        "file" => Err(ProviderError::bad_request(
            "image source type \"file\" is a Files-API id, which this proxy does not \
             resolve (the chat dialect's image_url has no file_id field) — send a base64 \
             or url source, or declare the dialect native and take the fidelity lane",
        )),
        other => Err(ProviderError::bad_request(format!(
            "image source type {other:?} is not one of base64, url, file"
        ))),
    }
}

/// Media types Anthropic's `document` block accepts inline: the PDF guide's
/// "Standard PDF", plus plain-text files as `text/plain` (`.txt`/`.csv`/`.md`).
/// Binaries such as `.xlsx`/`.docx` are documented as NOT supported in
/// document blocks — they must be converted to text or PDF first.
const ANTHROPIC_DOCUMENT_MEDIA_TYPES: &[&str] = &["application/pdf", "text/plain"];

/// One Anthropic `document` block -> the IR's `file` part.
///
/// Only the `base64` source folds. `url` has nowhere to go: chat's `file`
/// object carries `filename`/`file_data`/`file_id` and NO url field. And
/// `file` is an Anthropic Files-API id — chat CAN express a `file_id`, so
/// passing one through would reach the upstream as a guaranteed
/// "file not found" the client cannot attribute to its own namespace;
/// refusing it says so. (Contrast the responses module, where a `file_id` IS
/// the upstream's own and passes through.)
///
/// `citations`/`title`/`context`/`transformations` follow the shared rule in
/// `reject_unfolding_claims`: a value that claims something the chat file part
/// cannot say is refused, and a field left at its documented default (an
/// explicit `citations: {"enabled": false}`) folds like an absent one.
fn document_block_to_part(block: &Value) -> Result<Value, ProviderError> {
    reject_unfolding_claims(block, "document")?;
    let source = block
        .get("source")
        .ok_or_else(|| ProviderError::bad_request("document block has no source"))?;
    match source.get("type").and_then(Value::as_str).unwrap_or("?") {
        "base64" => {
            let media = source
                .get("media_type")
                .and_then(Value::as_str)
                .unwrap_or("");
            if !ANTHROPIC_DOCUMENT_MEDIA_TYPES.contains(&media) {
                return Err(ProviderError::bad_request(format!(
                    "document media_type {media:?} is not one of {}",
                    ANTHROPIC_DOCUMENT_MEDIA_TYPES.join(", ")
                )));
            }
            let data = source.get("data").and_then(Value::as_str).unwrap_or("");
            file_part(None, &data_url(media, data)?)
        }
        "url" => Err(ProviderError::bad_request(
            "a url-sourced document is not representable: the chat file part has no url \
             field (only filename/file_data/file_id) — inline the base64 or take the \
             fidelity lane",
        )),
        "file" => Err(ProviderError::bad_request(
            "a document file_id names a file in Anthropic's Files API, not the upstream's — \
             the chat file part can carry the field but not that reference; upload to the \
             upstream or take the fidelity lane",
        )),
        other => Err(ProviderError::bad_request(format!(
            "document source type {other:?} is not one of base64, url, file"
        ))),
    }
}

/// Fold one Anthropic message into the OpenAI messages it becomes. A turn
/// carrying tool blocks is not one message: `tool_use` blocks ride the
/// assistant turn as `tool_calls`, and every `tool_result` becomes its own
/// `role: "tool"` message, which is the shape OpenAI requires and the order
/// it requires them in.
fn fold_message(role: &str, content: &Value, out: &mut Vec<Value>) -> Result<(), ProviderError> {
    let blocks = match content {
        Value::Array(blocks) => blocks.as_slice(),
        // A plain string is the simple case and stays one message.
        _ => {
            let text = content_to_text(content, role)?;
            let mut m = Map::new();
            m.insert("role".into(), json!(role));
            m.insert("content".into(), json!(text));
            out.push(Value::Object(m));
            return Ok(());
        }
    };

    // Text and media parts are collected in the client's order rather than
    // joined: "look at this / <image> / now answer" is a different prompt
    // than all the text first, and the chat dialect can say that.
    let mut parts: Vec<Value> = Vec::new();
    let mut has_media = false;
    let mut tool_calls: Vec<Value> = Vec::new();
    let mut results: Vec<Value> = Vec::new();
    let mut reasoning = String::new();
    for b in blocks {
        let kind = b.get("type").and_then(Value::as_str).unwrap_or("?");
        match kind {
            "text" => {
                let t = b.get("text").and_then(Value::as_str).ok_or_else(|| {
                    ProviderError::bad_request(format!(
                        "text block in a {role} turn has no string text"
                    ))
                })?;
                parts.push(text_part(t));
            }
            "image" | "document" => {
                // The chat dialect puts media parts on USER content only —
                // system/developer accept text parts, an assistant message
                // text and refusal. Folding one anywhere else would hand the
                // vendor a body it refuses, with an error naming neither the
                // block nor the reason.
                if role != "user" {
                    return Err(ProviderError::bad_request(format!(
                        "{kind} in a {role} turn is not representable: the chat \
                         dialect carries media parts on user turns only (extend this \
                         module or use the fidelity lane)"
                    )));
                }
                parts.push(if kind == "image" {
                    image_block_to_part(b)?
                } else {
                    document_block_to_part(b)?
                });
                has_media = true;
            }
            "tool_use" => tool_calls.push(json!({
                "id": b.get("id").and_then(Value::as_str).unwrap_or_default(),
                "type": "function",
                "function": {
                    "name": b.get("name").and_then(Value::as_str).unwrap_or_default(),
                    // OpenAI carries arguments as a STRING; Anthropic sends an
                    // object, so this is where the shapes actually differ.
                    "arguments": b.get("input").map(Value::to_string).unwrap_or_else(|| "{}".into()),
                },
            })),
            "tool_result" => {
                match b.get("is_error") {
                    Some(Value::Bool(true)) => {
                        return Err(ProviderError::bad_request(format!(
                            "tool_result in a {role} turn has is_error:true, which the chat \
                             dialect cannot represent without changing the result into apparent \
                             success (use the fidelity lane)"
                        )));
                    }
                    Some(Value::Bool(false)) | None => {}
                    Some(_) => {
                        return Err(ProviderError::bad_request(format!(
                            "tool_result in a {role} turn has a non-boolean is_error"
                        )));
                    }
                }
                // Text only, and the error PROPAGATES: an image (or document)
                // returned by a tool has no legal chat position, and
                // stringifying the block would bury a megabyte of base64 in
                // tool output that no model can look at.
                let body = match b.get("content") {
                    Some(Value::String(s)) => s.clone(),
                    Some(other) => content_to_text(other, role)?,
                    None => String::new(),
                };
                results.push(json!({
                    "role": "tool",
                    "tool_call_id": b.get("tool_use_id").and_then(Value::as_str).unwrap_or_default(),
                    "content": body,
                }));
            }
            "thinking" => {
                // Assistant-only: the chat dialect has `reasoning_content` on
                // an assistant message and nowhere else, so a thinking block in
                // any other turn has no position to fold into.
                if role != "assistant" {
                    return Err(ProviderError::bad_request(format!(
                        "thinking in a {role} turn is not representable: reasoning text has a \
                         home only on an assistant message (extend this module or use the \
                         fidelity lane)"
                    )));
                }
                if let Some(t) = b.get("thinking").and_then(Value::as_str) {
                    if !reasoning.is_empty() {
                        reasoning.push('\n');
                    }
                    reasoning.push_str(t);
                }
                // `signature` is read and DISCARDED on purpose. Only Anthropic
                // can mint one, and the upstream here is chat-shaped; keeping it
                // would require a field the IR does not have, and inventing one
                // would let a client believe a proxy-written block was signed.
            }
            // An obfuscated thinking block carries opaque `data` with no
            // counterpart anywhere; forwarding the turn without it would answer
            // as though the redaction had not happened.
            "redacted_thinking" => {
                return Err(ProviderError::bad_request(
                    "redacted_thinking is not representable: its opaque payload has no field \
                     on the shared IR (use the fidelity lane to a native Anthropic upstream)",
                ));
            }
            kind => {
                return Err(ProviderError::bad_request(format!(
                    "content block type \"{kind}\" is not representable in this bridge \
                     (text, image, thinking, and tool blocks are; extend this module for the \
                     rest)"
                )));
            }
        }
    }
    // The turn itself, when it carried anything of its own. `parts_content`
    // decides the shape: text-only turns keep folding to the joined string
    // they always produced, and the array appears only when an image rode in
    // the turn. A tool-call-only assistant turn keeps its explicit null.
    let text: String = parts
        .iter()
        .filter_map(|p| p.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    if !text.is_empty() || has_media || !tool_calls.is_empty() || !reasoning.is_empty() {
        let mut m = Map::new();
        m.insert("role".into(), json!(role));
        m.insert(
            "content".into(),
            if has_media || !text.is_empty() {
                parts_content(parts)
            } else {
                Value::Null
            },
        );
        if !tool_calls.is_empty() {
            m.insert("tool_calls".into(), Value::Array(tool_calls));
        }
        // The chat dialect's home for thinking text on an assistant turn — the
        // name the reasoning-capable OpenAI-compatible upstreams read, and what
        // DeepSeek's thinking+tools rule demands be PRESENT on every prior
        // assistant turn. Emitted only when the client actually sent thinking;
        // synthesizing an empty one would invent an answer the model did not
        // give.
        if !reasoning.is_empty() {
            m.insert("reasoning_content".into(), json!(reasoning));
        }
        out.push(Value::Object(m));
    }
    // …then every result it answered, each its own message.
    out.extend(results);
    Ok(())
}

/// Anthropic function tool -> OpenAI function tool. Custom Anthropic tools
/// have no `type` key; a present type must name the function form. Versioned
/// server tools are a different execution contract and cannot become a blank
/// client-runnable function through this fold.
fn fold_tools(tools: &[Value]) -> Result<Vec<Value>, ProviderError> {
    tools
        .iter()
        .map(|tool| {
            if let Some(kind) = tool.get("type")
                && kind.as_str() != Some("function")
            {
                return Err(ProviderError::bad_request(match kind.as_str() {
                    Some(kind) => format!(
                        "built-in tool type {kind:?} cannot be served through the IR fold \
                         (only client function tools fold; use the fidelity lane for server-side tools)"
                    ),
                    None => "tool type must be a string".to_string(),
                }));
            }
            if !tool.is_object() {
                return Err(ProviderError::bad_request(
                    "tool must be an object with a string name",
                ));
            }
            let name = tool.get("name").and_then(Value::as_str).ok_or_else(|| {
                ProviderError::bad_request("tool must have a string name")
            })?;
            let mut f = Map::new();
            f.insert("name".into(), json!(name));
            if let Some(d) = tool.get("description") {
                f.insert("description".into(), d.clone());
            }
            if let Some(schema) = tool.get("input_schema") {
                f.insert("parameters".into(), schema.clone());
            }
            Ok(json!({ "type": "function", "function": Value::Object(f) }))
        })
        .collect()
}

/// `{"type":"auto"|"any"|"none"|"tool","name":…}` -> OpenAI's
/// `tool_choice`, plus Anthropic's parallel-tool disable flag when present.
fn tool_choice_to_openai(choice: &Value) -> Result<(Value, Option<bool>), ProviderError> {
    let folded = match choice.get("type").and_then(Value::as_str) {
        Some("any") => json!("required"),
        Some("none") => json!("none"),
        Some("auto") => json!("auto"),
        Some("tool") => {
            let name = choice.get("name").and_then(Value::as_str).ok_or_else(|| {
                ProviderError::bad_request("tool_choice type \"tool\" must have a string name")
            })?;
            json!({
                "type": "function",
                "function": { "name": name },
            })
        }
        other => {
            return Err(ProviderError::bad_request(format!(
                "tool_choice type {other:?} is not one of auto, any, none, tool"
            )));
        }
    };
    let parallel = match choice.get("disable_parallel_tool_use") {
        Some(v) if v.as_bool() == Some(true) => Some(false),
        Some(v) if v.as_bool() == Some(false) => Some(true),
        None => None,
        Some(_) => {
            return Err(ProviderError::bad_request(
                "tool_choice disable_parallel_tool_use must be a boolean",
            ));
        }
    };
    Ok((folded, parallel))
}

/// Join the `text` blocks of a string-or-blocks content value, rejecting every
/// other block type. This is the helper for the positions the chat dialect can
/// carry as TEXT ALONE — `system` content and a `tool` result — so an `image`
/// refused here is not an oversight: the same block folds in a user turn
/// (`fold_message`).
fn content_to_text(content: &Value, role: &str) -> Result<String, ProviderError> {
    match content {
        Value::String(s) => Ok(s.clone()),
        Value::Array(blocks) => {
            let mut out = String::new();
            for b in blocks {
                let kind = b.get("type").and_then(Value::as_str).unwrap_or("?");
                if kind == "text" {
                    let text = b.get("text").and_then(Value::as_str).ok_or_else(|| {
                        ProviderError::bad_request(format!(
                            "text block in a {role} position has no string text"
                        ))
                    })?;
                    if !out.is_empty() {
                        out.push('\n');
                    }
                    out.push_str(text);
                } else {
                    return Err(ProviderError::bad_request(format!(
                        "content block type \"{kind}\" in a {role} position is not \
                         representable in this position, which takes text only (images and \
                         documents fold in user turns; extend this module or use the chat dialect)"
                    )));
                }
            }
            Ok(out)
        }
        // `null` is ABSENT content, not a shape to refuse — SDKs send explicit
        // nulls, and a system/tool turn with none folds to empty as it always
        // did. Anything else is not a content value, and silently becoming
        // `""` here would erase a tool result the model never sees.
        Value::Null => Ok(String::new()),
        other => Err(ProviderError::bad_request(format!(
            "content must be a string or an array of blocks (got {}) (images fold in \
             user turns; extend this module or use the chat dialect)",
            match other {
                Value::Object(_) => "an object",
                Value::Number(_) => "a number",
                Value::Bool(_) => "a boolean",
                _ => "something else",
            }
        ))),
    }
}

/// OpenAI finish_reason -> Anthropic stop_reason. Refusal takes precedence
/// over tool/end-turn reporting so a safety terminal is not disguised.
fn stop_reason(finish: Option<&str>, emitted_tools: bool) -> &'static str {
    match finish {
        Some("length") => "max_tokens",
        Some("refusal") | Some("content_filter") => "refusal",
        _ if emitted_tools => "tool_use",
        _ => "end_turn",
    }
}

/// Anthropic carries cache creation/read counters natively. OpenAI cached and
/// completion-reasoning counters have no distinct Anthropic usage fields and
/// are omitted rather than relabelled.
fn usage_fields(u: Usage) -> Value {
    let mut usage = Map::from_iter([
        ("input_tokens".into(), u.prompt_tokens.into()),
        ("output_tokens".into(), u.completion_tokens.into()),
    ]);
    if let Some(v) = u.cache_creation_input_tokens {
        usage.insert("cache_creation_input_tokens".into(), v.into());
    }
    if let Some(v) = u.cache_read_input_tokens {
        usage.insert("cache_read_input_tokens".into(), v.into());
    }
    Value::Object(usage)
}

/// Buffered IR completion -> Anthropic `message` JSON. Refusal text is an
/// ordinary text block; Anthropic's grammar has no typed refusal block. A
/// synthesized `thinking` block's `signature` is emitted EMPTY: only
/// Anthropic can sign, and such a block must never be replayed to Anthropic
/// itself (the fidelity lane's job). Replay to THIS proxy is valid —
/// `fold_message` accepts an inbound signature and re-issues
/// `reasoning_content`, which is what closes the Claude agent loop.
pub fn completion_to_anthropic(c: &Completion) -> Value {
    let mut content: Vec<Value> = Vec::new();
    if let Some(r) = c.reasoning.as_deref().filter(|s| !s.is_empty()) {
        content.push(json!({ "type": "thinking", "thinking": r, "signature": "" }));
    }
    let nothing_emitted = content.is_empty() && c.tool_calls.is_empty();
    if !c.text.is_empty() {
        content.push(json!({ "type": "text", "text": c.text }));
    }
    if let Some(refusal) = c.refusal.as_deref().filter(|s| !s.is_empty()) {
        content.push(json!({ "type": "text", "text": refusal }));
    }
    if content.is_empty() && nothing_emitted {
        content.push(json!({ "type": "text", "text": "" }));
    }
    for call in &c.tool_calls {
        content.push(tool_use_block(call));
    }
    json!({
        "id": message_id(&c.id), "type": "message", "role": "assistant",
        "model": c.model, "content": content,
        "stop_reason": stop_reason(c.finish_reason.as_deref(), !c.tool_calls.is_empty()),
        "stop_sequence": Value::Null,
        "usage": c.usage.map(usage_fields).unwrap_or(json!({"input_tokens": 0, "output_tokens": 0})),
    })
}

/// IR tool call -> Anthropic `tool_use` block. `arguments` arrives as a JSON
/// STRING and Anthropic wants an object, so this is the one place the two
/// wires genuinely disagree; unparseable arguments become `{}` rather than
/// failing the whole answer, because a malformed call is the vendor's bug
/// and the client still needs the turn.
fn tool_use_block(call: &opencode2api_kit::ToolCall) -> Value {
    let input: Value = serde_json::from_str(&call.arguments).unwrap_or_else(|_| json!({}));
    json!({ "type": "tool_use", "id": call.id, "name": call.name, "input": input })
}

fn message_id(id: &str) -> String {
    if id.starts_with("msg_") {
        id.to_string()
    } else {
        format!("msg_{id}")
    }
}

/// Stateful translation of an IR chunk stream into the Anthropic event
/// sequence. Text and thinking stream as they arrive; EVERY tool fragment is
/// held and emitted by `finish()`, which then writes the terminal burst in
/// Anthropic's own order — one block at a time, refusal first because that is
/// where the buffered renderer puts it, then each held call complete (start,
/// its `input_json_delta` fragments, stop). Holding is what makes the grammar
/// hold for a turn that refuses mid-call: a live `tool_use` block and the
/// refusal block can never be open together, and one call is never split into
/// two `tool_use` entries a client would execute twice. The cost is that a
/// client sees no tool frames while the model is still writing arguments —
/// `message_start`'s `ping` frames, which the server keeps sending, are what
/// keep that connection alive. The server drives this exactly:
/// `message_start()` once at handoff, `on_chunk()` per chunk, `finish()` at
/// clean end. A terminal `error()` never emits held tool fragments.
#[derive(Debug)]
pub struct AnthropicStream {
    id: String,
    model: String,
    block_open: bool,
    finished: bool,
    usage: Option<Usage>,
    /// Thinking text opened, and the index the TEXT block then has to use:
    /// `text_block` is 0 until a thinking block takes index 0 (see
    /// [`thinking_start`]), which is also why tool numbering reads `next_block`
    /// rather than a literal.
    thinking_open: bool,
    thinking_block: usize,
    text_block: usize,
    /// Tool blocks opened so far, keyed by the upstream's `index`, holding
    /// the Anthropic block index they were given. Anthropic numbers content
    /// blocks in ONE sequence with the text block, and a tool's arguments
    /// arrive in fragments long after its block opened — so the mapping has
    /// to be remembered, not recomputed.
    tool_blocks: Vec<(u32, usize)>,
    refusal: String,
    refusal_block: Option<usize>,
    /// Every tool fragment is held here and opened at the terminal, so a
    /// refusal can never interleave with a tool block (Anthropic carries one
    /// block at a time) and one call is never split into two `tool_use`
    /// entries. Refusal is emitted BEFORE these, matching the buffered render.
    pending_tool_calls: Vec<opencode2api_kit::ToolCallDelta>,
    /// Next free Anthropic content-block index.
    next_block: usize,
}

impl AnthropicStream {
    /// `model`/`id` are echoed from the first chunk when possible; the server
    /// passes its best guess at handoff time (client-requested model, minted
    /// id) and we re-stamp once a real model arrives — note the head event
    /// may then disagree with later frames (crate docs: the terminal event
    /// is authoritative for usage; ids are minted, never reconciled).
    pub fn new(model: impl Into<String>, id: impl Into<String>) -> Self {
        let id = id.into();
        Self {
            id: message_id(&id),
            model: model.into(),
            block_open: false,
            finished: false,
            usage: None,
            thinking_open: false,
            // Indices come from `alloc_block` in first-appearance order, so
            // nothing is claimed at construction and 0 goes to whatever the
            // stream emits first — thinking, text, or a tool.
            thinking_block: 0,
            text_block: 0,
            tool_blocks: Vec::new(),
            refusal: String::new(),
            pending_tool_calls: Vec::new(),
            refusal_block: None,
            next_block: 0,
        }
    }

    pub fn message_start(&mut self) -> Vec<AnthropicEvent> {
        if self.finished {
            return Vec::new();
        }
        vec![
            AnthropicEvent(
                Some("message_start"),
                json!({
                    "type": "message_start",
                    "message": {
                        "id": self.id,
                        "type": "message",
                        "role": "assistant",
                        "model": self.model,
                        "content": [],
                        "usage": { "input_tokens": 0, "output_tokens": 0 },
                    },
                }),
            ),
            AnthropicEvent(Some("ping"), json!({ "type": "ping" })),
        ]
    }

    /// The streaming path: one chunk's events, written straight into the
    /// caller's sink. `on_chunk` is this function with a collecting sink, so
    /// the state machine exists exactly once.
    pub fn on_chunk_into(&mut self, chunk: &ChatChunk, out: &mut impl EventSink) {
        if self.finished {
            return;
        }
        if chunk.usage.is_some() {
            self.usage = chunk.usage;
        }
        if self.model.is_empty() && !chunk.model.is_empty() {
            self.model.clone_from(&chunk.model);
        }
        // Thinking first, and every block takes the NEXT free index rather
        // than a literal: Anthropic numbers content blocks in one sequence, so
        // a stream that opens thinking, then text, then a tool must say 0, 1,
        // 2 — hard-coding text at 0 would collide the moment reasoning appears.
        if !chunk.reasoning.is_empty() {
            if !self.thinking_open {
                self.thinking_open = true;
                self.thinking_block = self.alloc_block();
                out.event(
                    Some("content_block_start"),
                    &thinking_start(self.thinking_block),
                );
            }
            out.event(
                Some("content_block_delta"),
                &ThinkingDeltaEvent::new(&chunk.reasoning, self.thinking_block),
            );
        }
        if !chunk.text.is_empty() {
            if !self.block_open {
                self.block_open = true;
                self.text_block = self.alloc_block();
                out.event(Some("content_block_start"), &block_start(self.text_block));
            }
            out.event(
                Some("content_block_delta"),
                &TextDeltaEvent::new(&chunk.text, self.text_block),
            );
        }
        if !chunk.refusal.is_empty() {
            self.refusal.push_str(&chunk.refusal);
        }
        // Every tool fragment is held for the terminal. Anthropic carries ONE
        // content block at a time, so a live `tool_use` block and the refusal
        // block can never both be open; holding also means a call opened once
        // keeps ONE block, carrying the id and name the upstream announced, so
        // no client ever sees two `tool_use` entries for one call.
        self.pending_tool_calls
            .extend(chunk.tool_calls.iter().cloned());
    }

    /// Take the next content-block index, in first-appearance order.
    fn alloc_block(&mut self) -> usize {
        let b = self.next_block;
        self.next_block += 1;
        b
    }

    /// One tool-call fragment. The first fragment for an index opens a
    /// `tool_use` block (that is the only frame carrying the id and name);
    /// every later one is `input_json_delta` carrying raw argument text,
    /// which is NOT valid JSON on its own and must never be parsed here — the
    /// client assembles it, exactly as Anthropic's own wire intends.
    fn on_tool_delta(&mut self, call: &opencode2api_kit::ToolCallDelta, out: &mut impl EventSink) {
        let block = match self.tool_blocks.iter().find(|(i, _)| *i == call.index) {
            Some((_, block)) => *block,
            None => {
                let block = self.alloc_block();
                self.tool_blocks.push((call.index, block));
                out.event(
                    Some("content_block_start"),
                    &json!({
                        "type": "content_block_start",
                        "index": block,
                        "content_block": {
                            "type": "tool_use",
                            "id": call.id.clone().unwrap_or_default(),
                            "name": call.name.clone().unwrap_or_default(),
                            "input": {},
                        },
                    }),
                );
                block
            }
        };
        if call.arguments.is_empty() {
            return;
        }
        out.event(
            Some("content_block_delta"),
            &json!({
                "type": "content_block_delta",
                "index": block,
                "delta": { "type": "input_json_delta", "partial_json": call.arguments },
            }),
        );
    }

    pub fn on_chunk(&mut self, chunk: &ChatChunk) -> Vec<AnthropicEvent> {
        let mut collected = Collected::default();
        self.on_chunk_into(chunk, &mut collected);
        collected.0
    }

    pub fn finish(&mut self, finish_reason: Option<&str>) -> Vec<AnthropicEvent> {
        if self.finished {
            return Vec::new();
        }
        // `finished` is latched only after the complete terminal sequence has
        // been built. A collision panic must leave the stream open for `error()`,
        // because Anthropic's only valid terminal is a failure event there.
        let mut out = Vec::new();
        // Open a text block ONLY when nothing at all opened. An answer that
        // produced no content of any kind still has to close a grammar that
        // began with `message_start`, but a turn that went straight to a tool
        // does not get an empty text block invented for it: the vendor's own
        // sample stream (`docs/compliance/anthropic.md` §(b) 4) numbers blocks
        // by opening order, and its `tool_use` sits at index 1 only because a
        // real `text_delta` opened at 0 first. The buffered render already
        // omits the empty text block for a tool-only turn, so synthesizing one
        // here would hand the same completion two shapes depending on whether
        // the client asked for a stream.
        if !self.block_open
            && !self.thinking_open
            && self.tool_blocks.is_empty()
            && self.pending_tool_calls.is_empty()
            && self.refusal.is_empty()
        {
            self.text_block = self.alloc_block();
            self.block_open = true;
            out.push(AnthropicEvent(
                Some("content_block_start"),
                block_start(self.text_block),
            ));
        }
        // Terminal burst, sequenced ONE block at a time (Anthropic never has
        // two content blocks open): stop whatever text and thinking opened,
        // then refusal, then each held tool call complete. Refusal comes
        // before the tools because that is where the buffered renderer puts it.
        let mut closed: Vec<usize> = Vec::new();
        if self.block_open {
            closed.push(self.text_block);
        }
        if self.thinking_open {
            closed.push(self.thinking_block);
        }
        closed.sort_unstable();
        for block in &closed {
            out.push(AnthropicEvent(
                Some("content_block_stop"),
                json!({ "type": "content_block_stop", "index": block }),
            ));
        }
        if !self.refusal.is_empty() {
            self.refusal_block = Some(self.alloc_block());
            let refusal_index = self.refusal_block.expect("just assigned");
            out.push(AnthropicEvent(
                Some("content_block_start"),
                block_start(refusal_index),
            ));
            out.push(AnthropicEvent(
                Some("content_block_delta"),
                serde_json::to_value(TextDeltaEvent::new(&self.refusal, refusal_index))
                    .expect("typed text delta serializes"),
            ));
            out.push(AnthropicEvent(
                Some("content_block_stop"),
                json!({ "type": "content_block_stop", "index": refusal_index }),
            ));
            closed.push(refusal_index);
        }
        // Every held tool fragment opens and completes here, ONE call at a
        // time — start, its argument deltas, stop — so two `tool_use` blocks
        // are never open at once and a call announced once keeps exactly one
        // block, carrying the id and name the upstream announced.
        let mut order: Vec<u32> = Vec::new();
        for call in &self.pending_tool_calls {
            if !order.contains(&call.index) {
                order.push(call.index);
            }
        }
        for index in order {
            let fragments: Vec<opencode2api_kit::ToolCallDelta> = self
                .pending_tool_calls
                .iter()
                .filter(|call| call.index == index)
                .cloned()
                .collect();
            let mut call_out = Collected::default();
            for call in &fragments {
                self.on_tool_delta(call, &mut call_out);
            }
            out.extend(call_out.0);
            if let Some(block) = self
                .tool_blocks
                .iter()
                .find(|(i, _)| *i == index)
                .map(|(_, b)| *b)
            {
                // No `dedup()`: every index was handed out by `alloc_block`, so
                // a duplicate here would mean two blocks claiming one index —
                // break rather than emit a well-formed but wrong stop list.
                assert!(!closed.contains(&block), "content block index collision");
                closed.push(block);
                out.push(AnthropicEvent(
                    Some("content_block_stop"),
                    json!({ "type": "content_block_stop", "index": block }),
                ));
            }
        }
        self.pending_tool_calls.clear();
        // One index per block, across EVERY allocation this stream made — the
        // terminal burst stopped some blocks early, so the tool loop's own
        // check cannot see a collision with text, thinking or refusal. Every
        // index came from `alloc_block`, so a duplicate means two blocks
        // claiming one position; break here, before `finished` latches, so
        // `error()` can still deliver the terminal failure.
        let mut allocated: Vec<usize> = self.tool_blocks.iter().map(|(_, b)| *b).collect();
        if self.block_open {
            allocated.push(self.text_block);
        }
        if self.thinking_open {
            allocated.push(self.thinking_block);
        }
        if let Some(block) = self.refusal_block {
            allocated.push(block);
        }
        allocated.sort_unstable();
        for pair in allocated.windows(2) {
            assert_ne!(pair[0], pair[1], "content block index collision");
        }
        let emitted_tools = !self.tool_blocks.is_empty();
        out.push(AnthropicEvent(
            Some("message_delta"),
            json!({
                "type": "message_delta",
                "delta": {
                    "stop_reason": stop_reason(finish_reason, emitted_tools),
                    "stop_sequence": Value::Null,
                },
                "usage": self.usage.map(usage_fields).unwrap_or(json!({"output_tokens": 0})),
            }),
        ));
        out.push(AnthropicEvent(
            Some("message_stop"),
            json!({ "type": "message_stop" }),
        ));
        self.finished = true;
        out
    }

    /// A terminal in-stream error. Anthropic answers `event: error` and
    /// closes; no message_stop follows.
    pub fn error(&mut self, error_type: &str, message: &str) -> Vec<AnthropicEvent> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        vec![AnthropicEvent(
            Some("error"),
            json!({
                "type": "error",
                "error": { "type": error_type, "message": message },
            }),
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req_body() -> AnthropicRequest {
        serde_json::from_value(json!({
            "model": "claude-x",
            "max_tokens": 256,
            "system": "be terse",
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
                {"role": "user", "content": "again"}
            ],
            "temperature": 0.2,
            "stop_sequences": ["END"],
            "metadata": {"drop": "me"}
        }))
        .unwrap()
    }

    #[test]
    fn inbound_fold_puts_system_first_and_keeps_text() {
        let chat = req_body().to_chat_request().unwrap();
        assert_eq!(chat.model, "claude-x");
        assert_eq!(chat.messages.len(), 4); // system + 3 turns
        assert_eq!(chat.messages[0]["role"], "system");
        assert_eq!(chat.messages[3]["role"], "user");
        assert_eq!(chat.extra["max_tokens"], 256);
        assert_eq!(chat.extra["temperature"], 0.2);
        assert_eq!(chat.extra["stop"], json!(["END"]));
        assert!(chat.extra.get("metadata").is_none());
        assert!(chat.extra.get("anthropic-version").is_none());
    }

    #[test]
    fn unrepresentable_blocks_are_rejected_not_dropped() {
        // The silent-drop class, pinned on a block this fold still cannot
        // represent: a dropped document would answer from a history whose
        // attachment vanished. (`image` folded out of this list; the tests
        // below hold the line where the chat dialect itself runs out of room.)
        let req: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8,
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "read this"},
                {"type": "document", "source": {"type": "text", "data": "x"}}
            ]}]
        }))
        .unwrap();
        let e = req.to_chat_request().unwrap_err();
        assert_eq!(e.status, 400);
        assert!(e.message.contains("document"), "got {}", e.message);
    }

    /// Both source kinds Anthropic documents: base64 composes a data URL, a
    /// url source passes through as the vendor-side fetch.
    #[test]
    fn user_turn_images_fold_into_image_url_parts() {
        for (source, want) in [
            (
                json!({"type": "base64", "media_type": "image/png", "data": "AANA"}),
                "data:image/png;base64,AANA",
            ),
            (
                json!({"type": "url", "url": "https://host/a.jpg"}),
                "https://host/a.jpg",
            ),
        ] {
            let req: AnthropicRequest = serde_json::from_value(json!({
                "model": "m", "max_tokens": 8,
                "messages": [{"role": "user", "content": [
                    {"type": "image", "source": source}
                ]}]
            }))
            .unwrap();
            let chat = req.to_chat_request().expect("user-turn images fold");
            let part = &chat.messages[0]["content"][0];
            assert_eq!(part["type"], "image_url", "{want}");
            assert_eq!(part["image_url"]["url"], want);
            assert!(
                part["image_url"].get("detail").is_none(),
                "Anthropic has no sizing field, so none is invented"
            );
        }
    }

    /// Text and image keep the client's ORDER: a fold that joined all text
    /// first would answer a different prompt than the one that was sent.
    #[test]
    fn image_and_text_interleave_in_order() {
        let req: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8,
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "first"},
                {"type": "image", "source": {
                    "type": "base64", "media_type": "image/png", "data": "AANA"
                }},
                {"type": "text", "text": "then this"}
            ]}]
        }))
        .unwrap();
        let chat = req.to_chat_request().unwrap();
        let parts = chat.messages[0]["content"]
            .as_array()
            .expect("array content");
        assert_eq!(
            parts
                .iter()
                .map(|p| p["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["text", "image_url", "text"]
        );
        assert_eq!(parts[0]["text"], "first");
        assert_eq!(parts[2]["text"], "then this");
    }

    /// The refusals the chat dialect forces, each for its own stated reason:
    /// a media type outside Anthropic's enum, an image in a role that has no
    /// image parts, an image returned BY a tool (the computer-use screenshot,
    /// which has no legal chat position), and a Files-API id this proxy does
    /// not resolve.
    #[test]
    fn images_are_refused_where_chat_cannot_carry_them() {
        let cases = [
            (
                "media_type",
                json!({"model": "m", "max_tokens": 8, "messages": [
                    {"role": "user", "content": [
                        {"type": "image", "source": {
                            "type": "base64", "media_type": "image/jpg", "data": "AANA"
                        }}
                    ]}
                ]}),
            ),
            (
                "assistant turn",
                json!({"model": "m", "max_tokens": 8, "messages": [
                    {"role": "assistant", "content": [
                        {"type": "image", "source": {"type": "url", "url": "https://h/a.png"}}
                    ]}
                ]}),
            ),
            (
                "tool_result",
                json!({"model": "m", "max_tokens": 8, "messages": [
                    {"role": "assistant", "content": [
                        {"type": "tool_use", "id": "t1", "name": "screenshot", "input": {}}
                    ]},
                    {"role": "user", "content": [
                        {"type": "tool_result", "tool_use_id": "t1", "content": [
                            {"type": "image", "source": {
                                "type": "base64", "media_type": "image/png", "data": "AANA"
                            }}
                        ]}
                    ]}
                ]}),
            ),
            (
                "file_id",
                json!({"model": "m", "max_tokens": 8, "messages": [
                    {"role": "user", "content": [
                        {"type": "image", "source": {"type": "file", "file_id": "file_01ABC"}}
                    ]}
                ]}),
            ),
        ];
        for (what, body) in cases {
            let req: AnthropicRequest = serde_json::from_value(body).unwrap();
            let e = req
                .to_chat_request()
                .expect_err(&format!("{what} must 400"));
            assert_eq!(e.status, 400, "{what}");
            assert!(e.message.contains("image"), "{what}: got {}", e.message);
        }
    }

    /// A PDF arrives as Anthropic's base64 `document` block and must leave as
    /// chat's `file` part: data URL composed, and the `filename` OpenAI's
    /// examples always pair with `file_data` supplied neutrally — Anthropic
    /// has no name field, and refusing a working document over a label would
    /// be the worse trade.
    #[test]
    fn user_turn_documents_fold_into_file_parts() {
        for (media, name) in [
            ("application/pdf", "document.pdf"),
            ("text/plain", "document.txt"),
        ] {
            let req: AnthropicRequest = serde_json::from_value(json!({
                "model": "m", "max_tokens": 8,
                "messages": [{"role": "user", "content": [
                    {"type": "document", "source": {
                        "type": "base64", "media_type": media, "data": "JVBER"
                    }},
                    {"type": "text", "text": "summarize"}
                ]}]
            }))
            .unwrap();
            let chat = req.to_chat_request().expect("user-turn documents fold");
            let parts = chat.messages[0]["content"]
                .as_array()
                .expect("array content");
            assert_eq!(parts[0]["type"], "file", "{media}");
            assert_eq!(
                parts[0]["file"]["file_data"],
                format!("data:{media};base64,JVBER")
            );
            assert_eq!(parts[0]["file"]["filename"], name, "{media}");
            assert_eq!(parts[1]["type"], "text", "the client's order survives");
        }
    }

    /// What the chat `file` part has no field for: a hosted URL, another
    /// vendor's file id, `citations` (a different RESPONSE contract, not
    /// metadata), a media type Anthropic itself refuses, and a document in a
    /// role that carries no media.
    #[test]
    fn documents_are_refused_where_the_chat_fields_do_not_exist() {
        let inline = json!({"type": "base64", "media_type": "application/pdf", "data": "JVBER"});
        let cases = [
            (
                "url source",
                json!({"type": "document", "source": {"type": "url", "url": "https://h/a.pdf"}}),
                "url",
            ),
            (
                "file source",
                json!({"type": "document", "source": {"type": "file", "file_id": "file_01ABC"}}),
                "anthropic",
            ),
            (
                "citations",
                json!({"type": "document", "source": inline, "citations": {"enabled": true}}),
                "citations",
            ),
            (
                "title",
                json!({"type": "document", "source": inline, "title": "Q3 report"}),
                "title",
            ),
            (
                "docx",
                json!({"type": "document", "source": {
                    "type": "base64", "media_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    "data": "JVBER"
                }}),
                "media_type",
            ),
        ];
        for (what, block, needle) in cases {
            let req: AnthropicRequest = serde_json::from_value(json!({
                "model": "m", "max_tokens": 8,
                "messages": [{"role": "user", "content": [block]}]
            }))
            .unwrap();
            let e = req
                .to_chat_request()
                .expect_err(&format!("{what} must 400"));
            assert_eq!(e.status, 400, "{what}");
            assert!(
                e.message.to_lowercase().contains(needle),
                "{what}: got {}",
                e.message
            );
        }
        // A document in an assistant turn is refused for the chat dialect's
        // own reason, not Anthropic's: assistant content has no media parts.
        let req: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8,
            "messages": [{"role": "assistant", "content": [
                {"type": "document", "source": inline}
            ]}]
        }))
        .unwrap();
        let e = req.to_chat_request().unwrap_err();
        assert_eq!(e.status, 400);
        assert!(
            e.message.contains("document") && e.message.contains("user turns"),
            "got {}",
            e.message
        );
    }

    /// A document with no text at all — "just summarize this PDF" — is the
    /// common case, and it is the one that vanishes silently if the turn's
    /// emission is decided by the joined text or by an image-only flag.
    #[test]
    fn a_document_only_turn_folds_to_a_part_array() {
        let req: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8,
            "messages": [{"role": "user", "content": [
                {"type": "document", "source": {
                    "type": "base64", "media_type": "application/pdf", "data": "JVBER"
                }}
            ]}]
        }))
        .unwrap();
        let chat = req.to_chat_request().unwrap();
        let parts = chat.messages[0]["content"]
            .as_array()
            .expect("a document-only turn must not collapse to null text");
        assert_eq!(parts.len(), 1);
        assert_eq!(
            parts[0]["file"]["file_data"],
            "data:application/pdf;base64,JVBER"
        );
    }

    /// The default-is-nothing rule, both directions: an explicit
    /// `citations: {"enabled": false}` and `transformations.oversized_image:
    /// "downsize"` claim nothing (that IS the upstream's behavior) and fold
    /// like absent fields, while a value that changes what happens refuses.
    /// `transformations` is checked on IMAGES too — `oversized_image: "error"`
    /// is a promised failure a fold would otherwise swallow into a resize.
    #[test]
    fn a_field_left_at_its_default_claims_nothing() {
        let off: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8,
            "messages": [{"role": "user", "content": [
                {"type": "document", "source": {
                    "type": "base64", "media_type": "application/pdf", "data": "JVBER"
                }, "citations": {"enabled": false}, "title": ""}
            ]}]
        }))
        .unwrap();
        assert!(
            off.to_chat_request().is_ok(),
            "citations off and an empty title say nothing"
        );

        // The default level folds on BOTH block kinds — and note each case
        // uses a source that folds, so a 400 here can only come from
        // `transformations` and not from the block's own shape.
        let pdf = json!({"type": "base64", "media_type": "application/pdf", "data": "JVBER"});
        for block in [
            json!({"type": "image", "source": {"type": "url", "url": "https://h/a.png"},
                   "transformations": {"oversized_image": "downsize"}}),
            json!({"type": "document", "source": pdf,
                   "transformations": {"oversized_image": "downsize"}}),
        ] {
            let req: AnthropicRequest = serde_json::from_value(json!({
                "model": "m", "max_tokens": 8,
                "messages": [{"role": "user", "content": [block]}]
            }))
            .unwrap();
            assert!(
                req.to_chat_request().is_ok(),
                "the default level claims nothing"
            );
        }
        for block in [
            json!({"type": "image", "source": {"type": "url", "url": "https://h/a.png"},
                   "transformations": {"oversized_image": "error"}}),
            json!({"type": "document", "source": pdf,
                   "transformations": {"oversized_image": "error"}}),
            json!({"type": "image", "source": {"type": "url", "url": "https://h/a.png"},
                   "transformations": "please"}),
        ] {
            let req: AnthropicRequest = serde_json::from_value(json!({
                "model": "m", "max_tokens": 8,
                "messages": [{"role": "user", "content": [block]}]
            }))
            .unwrap();
            let e = req.to_chat_request().unwrap_err();
            assert_eq!(e.status, 400);
            assert!(e.message.contains("transformations"), "got {}", e.message);
        }
    }

    /// A PDF returned BY a tool is refused for the chat dialect's reason, and
    /// the message has to point somewhere useful: image-only advice would send
    /// a document-returning tool client after the wrong fix.
    #[test]
    fn a_document_returned_by_a_tool_says_where_media_does_fold() {
        let req: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8,
            "messages": [
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "t1", "name": "fetch", "input": {}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "t1", "content": [
                        {"type": "document", "source": {
                            "type": "base64", "media_type": "application/pdf", "data": "JVBER"
                        }}
                    ]}
                ]}
            ]
        }))
        .unwrap();
        let e = req.to_chat_request().unwrap_err();
        assert_eq!(e.status, 400);
        assert!(
            e.message.contains("document")
                && e.message.contains("text only")
                && e.message.contains("user turns"),
            "got {}",
            e.message
        );
    }

    /// Propagating the fold's 400s (instead of stringifying what it could not
    /// represent) also closed a quieter hole: a `content` value that is
    /// neither a string, an array, nor `null` used to fold to an empty tool
    /// result. Absent content still folds; malformed content is refused.
    #[test]
    fn malformed_content_is_refused_not_emptied() {
        let cases = [
            (
                "tool result",
                json!({"model": "m", "max_tokens": 8, "messages": [
                    {"role": "user", "content": [
                        {"type": "tool_result", "tool_use_id": "t1", "content": {"oops": true}}
                    ]}
                ]}),
            ),
            (
                "system",
                json!({"model": "m", "max_tokens": 8, "system": 42, "messages": []}),
            ),
        ];
        for (what, body) in cases {
            let req: AnthropicRequest = serde_json::from_value(body).unwrap();
            let e = req
                .to_chat_request()
                .expect_err(&format!("{what} content is malformed"));
            assert_eq!(e.status, 400, "{what}");
            assert!(
                e.message.contains("string or an array"),
                "{what}: got {}",
                e.message
            );
        }
        // Absent content is not malformed: SDKs send explicit nulls, and a
        // system turn with none has always folded to nothing.
        let req: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8, "system": null, "messages": []
        }))
        .unwrap();
        assert!(req.to_chat_request().is_ok(), "null system is absent");
    }

    #[test]
    fn tool_errors_and_malformed_text_refuse_before_conversation_loss() {
        let cases = [
            (
                "tool failure",
                json!({"model": "m", "max_tokens": 8, "messages": [
                    {"role": "user", "content": [
                        {"type": "tool_result", "tool_use_id": "t1", "is_error": true, "content": "failed"}
                    ]}
                ]}),
                vec!["tool_result", "user", "is_error:true"],
            ),
            (
                "message text",
                json!({"model": "m", "max_tokens": 8, "messages": [
                    {"role": "assistant", "content": [{"type": "text", "text": 42}]}
                ]}),
                vec!["text", "assistant", "string text"],
            ),
            (
                "system text",
                json!({"model": "m", "max_tokens": 8, "system": [{"type": "text"}], "messages": []}),
                vec!["text", "system", "string text"],
            ),
            (
                "tool result text",
                json!({"model": "m", "max_tokens": 8, "messages": [
                    {"role": "user", "content": [
                        {"type": "tool_result", "tool_use_id": "t1", "content": [{"type": "text", "text": null}]}
                    ]}
                ]}),
                vec!["text", "user", "string text"],
            ),
        ];
        for (what, body, needles) in cases {
            let req: AnthropicRequest = serde_json::from_value(body).unwrap();
            let error = req.to_chat_request().expect_err(what);
            assert_eq!(error.status, 400, "{what}");
            for needle in needles {
                assert!(error.message.contains(needle), "{what}: {}", error.message);
            }
        }
    }

    #[test]
    fn server_tools_and_unknown_tool_choices_refuse() {
        for (tools, tool_choice, needles) in [
            (
                vec![json!({"type": "web_search_20250305", "name": "web_search"})],
                json!({"type": "auto"}),
                vec!["web_search_20250305", "fidelity lane"],
            ),
            (
                Vec::new(),
                json!({"type": "required"}),
                vec!["auto", "any", "none", "tool"],
            ),
        ] {
            let req: AnthropicRequest = serde_json::from_value(json!({
                "model": "m", "max_tokens": 8, "messages": [],
                "tools": tools, "tool_choice": tool_choice
            }))
            .unwrap();
            let error = req
                .to_chat_request()
                .expect_err("unrepresentable tool input");
            assert_eq!(error.status, 400);
            for needle in needles {
                assert!(error.message.contains(needle), "{}", error.message);
            }
        }
    }

    #[test]
    fn nameless_tools_refuse_instead_of_becoming_blank_functions() {
        for (tool, tool_choice, needle) in [
            (json!(42), json!({"type": "auto"}), "string name"),
            (
                json!({"type": "function", "description": "nameless"}),
                json!({"type": "auto"}),
                "string name",
            ),
            (
                json!({"name": "lookup", "input_schema": {"type": "object"}}),
                json!({"type": "tool"}),
                "type \"tool\"",
            ),
        ] {
            let req: AnthropicRequest = serde_json::from_value(json!({
                "model": "m", "max_tokens": 8, "messages": [],
                "tools": [tool], "tool_choice": tool_choice
            }))
            .unwrap();
            let error = req.to_chat_request().expect_err("nameless selection");
            assert_eq!(error.status, 400);
            assert!(error.message.contains(needle), "{}", error.message);
        }
    }

    #[test]
    fn disabling_parallel_tools_forwards_its_chat_equivalent() {
        let req: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8, "messages": [],
            "tool_choice": {"type": "auto", "disable_parallel_tool_use": true}
        }))
        .unwrap();
        let chat = req.to_chat_request().unwrap();
        assert_eq!(chat.extra["tool_choice"], "auto");
        assert_eq!(chat.extra["parallel_tool_calls"], false);

        let req: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8, "messages": [],
            "tool_choice": {"type": "auto", "disable_parallel_tool_use": false}
        }))
        .unwrap();
        assert_eq!(
            req.to_chat_request().unwrap().extra["parallel_tool_calls"],
            true
        );
    }

    #[test]
    fn streamed_refusal_block_sequences_match_buffered_exactly() {
        let completion = |text: &str, tools: bool| Completion {
            refusal: Some("I cannot assist".into()),
            id: "gen".into(),
            model: "m".into(),
            text: text.into(),
            finish_reason: Some("refusal".into()),
            usage: None,
            tool_calls: if tools {
                vec![opencode2api_kit::ToolCall {
                    id: "tu_1".into(),
                    name: "lookup".into(),
                    arguments: "{}".into(),
                }]
            } else {
                Vec::new()
            },
            raw: None,
            reasoning: None,
        };
        for (name, chunks, oracle) in [
            (
                "refusal only",
                vec![refusal_chunk("I cannot assist")],
                completion("", false),
            ),
            (
                "answer then refusal",
                vec![probe_chunk("partial"), refusal_chunk("I cannot assist")],
                completion("partial", false),
            ),
            (
                "answer refusal and tool",
                vec![
                    probe_chunk("partial"),
                    refusal_chunk("I cannot assist"),
                    tool_chunk(0, Some("tu_1"), Some("lookup"), "{}"),
                ],
                completion("partial", true),
            ),
        ] {
            let buffered = completion_to_anthropic(&oracle)["content"]
                .as_array()
                .unwrap()
                .clone();
            let mut stream = AnthropicStream::new("m", name);
            let mut events = Vec::new();
            for chunk in &chunks {
                events.extend(stream.on_chunk(chunk));
            }
            events.extend(stream.finish(Some("refusal")));
            let opened: Vec<Value> = events
                .iter()
                .filter(|event| event.0 == Some("content_block_start"))
                .map(|event| {
                    let index = event.1["index"].clone();
                    let mut block = event.1["content_block"].clone();
                    if block["type"] == "text" {
                        let text = events
                            .iter()
                            .filter(|delta| {
                                delta.0 == Some("content_block_delta") && delta.1["index"] == index
                            })
                            .filter_map(|delta| delta.1["delta"]["text"].as_str())
                            .collect::<String>();
                        block["text"] = json!(text);
                    }
                    block
                })
                .collect();
            assert_eq!(opened, buffered, "{name}");
        }
    }

    fn refusal_chunk(text: &str) -> ChatChunk {
        let mut chunk = probe_chunk("");
        chunk.refusal = text.into();
        chunk
    }

    #[test]
    fn a_message_without_content_refuses() {
        let req: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8, "messages": [{"role": "user"}]
        }))
        .unwrap();
        let error = req
            .to_chat_request()
            .expect_err("missing content is a lost turn");
        assert_eq!(error.status, 400);
        assert!(error.message.contains("no content"), "{}", error.message);
    }

    #[test]
    fn a_block_collision_panics_before_terminal_and_leaves_error_available() {
        let mut stream = AnthropicStream::new("m", "collision");
        stream.on_chunk(&probe_chunk("answer"));
        // A second block claiming the text block's index: the terminal close
        // list must break rather than dedup it away.
        stream.tool_blocks.push((0, stream.text_block));
        let panic =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| stream.finish(Some("stop"))));
        assert!(panic.is_err(), "duplicate block index must break");
        let error = stream.error("api_error", "terminal build failed");
        assert_eq!(error.len(), 1);
        assert_eq!(error[0].0, Some("error"));
        assert_eq!(error[0].1["error"]["message"], "terminal build failed");
        assert!(stream.finish(Some("stop")).is_empty());
    }

    /// A tool call split across a refusal, and two calls streaming at once:
    /// the terminal emits refusal first, then each call complete — one
    /// `tool_use` block per call, never two open at once, no argument delta
    /// after any stop — and each call's fragments still assemble.
    #[test]
    fn a_refusal_mid_tool_keeps_one_block_per_call_and_refusal_first() {
        let mut stream = AnthropicStream::new("m", "split");
        let mut events = stream.on_chunk(&probe_chunk("partial"));
        events.extend(stream.on_chunk(&tool_chunk(0, Some("tu_1"), Some("alpha"), "{\"ci")));
        events.extend(stream.on_chunk(&tool_chunk(1, Some("tu_2"), Some("beta"), "{\"x")));
        events.extend(stream.on_chunk(&refusal_chunk("I cannot assist")));
        events.extend(stream.on_chunk(&tool_chunk(0, None, None, "ty\":\"SF\"}")));
        events.extend(stream.on_chunk(&tool_chunk(1, None, None, "y\":2}")));
        events.extend(stream.finish(Some("refusal")));

        let trace: Vec<String> = events
            .iter()
            .map(|event| match event.0 {
                Some("content_block_start") => format!(
                    "start:{}:{}:{}",
                    event.1["index"],
                    event.1["content_block"]["type"].as_str().unwrap_or("?"),
                    event.1["content_block"]["name"].as_str().unwrap_or("")
                ),
                Some("content_block_delta") => format!(
                    "delta:{}:{}",
                    event.1["index"],
                    event.1["delta"]["partial_json"].as_str().unwrap_or("text")
                ),
                Some("content_block_stop") => format!("stop:{}", event.1["index"]),
                Some(name) => name.to_string(),
                None => "anonymous".into(),
            })
            .collect();
        assert_eq!(
            trace,
            vec![
                "start:0:text:",
                "delta:0:text",
                "stop:0",
                "start:1:text:",
                "delta:1:text",
                "stop:1",
                "start:2:tool_use:alpha",
                "delta:2:{\"ci",
                "delta:2:ty\":\"SF\"}",
                "stop:2",
                "start:3:tool_use:beta",
                "delta:3:{\"x",
                "delta:3:y\":2}",
                "stop:3",
                "message_delta",
                "message_stop",
            ],
            "one block at a time, refusal before tools, one block per call"
        );
        let mut by_name: Vec<(String, String)> = Vec::new();
        for event in events.iter().filter(|e| e.0 == Some("content_block_start")) {
            if event.1["content_block"]["type"] == "tool_use" {
                by_name.push((
                    event.1["content_block"]["name"]
                        .as_str()
                        .unwrap()
                        .to_string(),
                    event.1["content_block"]["id"].as_str().unwrap().to_string(),
                ));
            }
        }
        assert_eq!(
            by_name,
            vec![
                ("alpha".into(), "tu_1".into()),
                ("beta".into(), "tu_2".into())
            ],
            "each call keeps the id and name the upstream announced, once"
        );
        for (name, want) in [("alpha", json!({"city": "SF"})), ("beta", json!({"xy": 2}))] {
            let index = events
                .iter()
                .find(|e| {
                    e.0 == Some("content_block_start")
                        && e.1["content_block"]["name"].as_str() == Some(name)
                })
                .unwrap()
                .1["index"]
                .clone();
            let assembled: String = events
                .iter()
                .filter(|e| e.0 == Some("content_block_delta") && e.1["index"] == index)
                .filter_map(|e| e.1["delta"]["partial_json"].as_str())
                .collect();
            assert_eq!(
                serde_json::from_str::<Value>(&assembled).unwrap_or(Value::Null),
                want,
                "{name} arguments complete"
            );
        }
    }

    #[test]
    fn tools_fold_into_the_chat_dialects_own_shape() {
        let req: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8, "messages": [],
            "tools": [{"name": "get_weather", "description": "look it up",
                       "input_schema": {"type": "object", "properties": {"city": {"type": "string"}}}}],
            "tool_choice": {"type": "tool", "name": "get_weather"}
        }))
        .unwrap();
        let chat = req.to_chat_request().expect("tools are representable now");
        let tool = &chat.extra["tools"][0];
        assert_eq!(tool["type"], "function");
        assert_eq!(tool["function"]["name"], "get_weather");
        assert_eq!(tool["function"]["description"], "look it up");
        assert_eq!(
            tool["function"]["parameters"]["properties"]["city"]["type"], "string",
            "the schema passes through untouched"
        );
        assert_eq!(chat.extra["tool_choice"]["function"]["name"], "get_weather");
    }

    #[test]
    fn tool_choice_any_means_required() {
        for (anthropic, openai) in [("any", "required"), ("auto", "auto"), ("none", "none")] {
            let req: AnthropicRequest = serde_json::from_value(json!({
                "model": "m", "max_tokens": 8, "messages": [],
                "tool_choice": {"type": anthropic}
            }))
            .unwrap();
            let chat = req.to_chat_request().unwrap();
            assert_eq!(chat.extra["tool_choice"], openai, "{anthropic}");
        }
    }

    /// A tool turn is not one message: the assistant's `tool_use` blocks ride
    /// its own turn, and every `tool_result` becomes a separate `role:"tool"`
    /// message — in that order, which is what the chat dialect requires.
    #[test]
    fn tool_blocks_fold_into_calls_and_result_messages() {
        let req: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8,
            "messages": [
                {"role": "assistant", "content": [
                    {"type": "text", "text": "checking"},
                    {"type": "tool_use", "id": "tu_1", "name": "get_weather", "input": {"city": "SF"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "tu_1", "content": "sunny"}
                ]}
            ]
        }))
        .unwrap();
        let chat = req.to_chat_request().unwrap();
        assert_eq!(chat.messages.len(), 2, "{:?}", chat.messages);
        assert_eq!(chat.messages[0]["role"], "assistant");
        assert_eq!(chat.messages[0]["content"], "checking");
        assert_eq!(chat.messages[0]["tool_calls"][0]["id"], "tu_1");
        assert_eq!(
            chat.messages[0]["tool_calls"][0]["function"]["arguments"], "{\"city\":\"SF\"}",
            "Anthropic sends an object, the chat wire wants a string"
        );
        assert_eq!(chat.messages[1]["role"], "tool");
        assert_eq!(chat.messages[1]["tool_call_id"], "tu_1");
        assert_eq!(chat.messages[1]["content"], "sunny");
    }

    /// The streamed grammar: a tool gets its OWN numbered content block, its
    /// arguments arrive as `input_json_delta` fragments that are not valid
    /// JSON alone, every block closes, and the turn ends on `tool_use`. Tool
    /// frames are held for the terminal so a refusal can never interleave
    /// with them, so nothing is emitted before `finish()`.
    #[test]
    fn streamed_tool_calls_open_their_own_block_and_close_it() {
        let mut s = AnthropicStream::new("m", "opencode2api-1");
        let events = s.on_chunk(&tool_chunk(0, Some("tu_1"), Some("get_weather"), ""));
        assert!(
            events.is_empty(),
            "tool frames wait for the terminal: {:?}",
            events
        );
        let mut events = s.on_chunk(&tool_chunk(0, None, None, "{\"ci"));
        events.extend(s.on_chunk(&tool_chunk(0, None, None, "ty\":\"SF\"}")));
        assert!(events.is_empty(), "no argument delta escapes the terminal");
        let fin = s.finish(Some("tool_calls"));
        let all: Vec<&AnthropicEvent> = events.iter().chain(&fin).collect();
        let start = all[0];
        assert_eq!(start.0, Some("content_block_start"));
        // Open order, not a fixed text-at-0 convention. THIS stream never
        // emitted text, so the tool legitimately owns index 0.
        assert_eq!(start.1["index"], 0, "first opened block owns index 0");
        assert_eq!(start.1["content_block"]["type"], "tool_use");
        assert_eq!(start.1["content_block"]["name"], "get_weather");
        let deltas: Vec<&str> = all
            .iter()
            .filter_map(|e| e.1["delta"]["partial_json"].as_str())
            .collect();
        assert_eq!(deltas, vec!["{\"ci", "ty\":\"SF\"}"], "fragments, unparsed");
        let stops: Vec<u64> = fin
            .iter()
            .filter(|e| e.0 == Some("content_block_stop"))
            .filter_map(|e| e.1["index"].as_u64())
            .collect();
        assert_eq!(
            stops,
            vec![0],
            "only the tool block closes — no empty text block is invented for a tool-only turn"
        );
        let delta = fin.iter().find(|e| e.0 == Some("message_delta")).unwrap();
        assert_eq!(
            delta.1["delta"]["stop_reason"], "tool_use",
            "the promise can be kept now, so it is made"
        );
    }

    /// One IR, two paths, one shape. A tool-only turn must render the same
    /// content whether the client asked for a stream or not — otherwise
    /// toggling `stream: true` changes the answer, which is a contract no
    /// client can see coming.
    #[test]
    fn a_tool_only_turn_renders_the_same_blocks_streamed_or_buffered() {
        let c = Completion {
            refusal: None,
            id: "gen".into(),
            model: "m".into(),
            text: String::new(),
            finish_reason: Some("tool_calls".into()),
            usage: None,
            tool_calls: vec![opencode2api_kit::ToolCall {
                id: "tu_1".into(),
                name: "get_weather".into(),
                arguments: "{\"city\":\"SF\"}".into(),
            }],
            raw: None,
            reasoning: None,
        };
        let buffered: Vec<String> = completion_to_anthropic(&c)["content"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["type"].as_str().unwrap().to_string())
            .collect();

        let mut s = AnthropicStream::new("m", "gen");
        // The whole sequence, not just `finish`: a block's START is emitted
        // with the first fragment that arrives, so dropping the head of the
        // stream is what made this read as an empty list.
        let mut seq = s.on_chunk(&tool_chunk(
            0,
            Some("tu_1"),
            Some("get_weather"),
            "{\"city\":\"SF\"}",
        ));
        seq.extend(s.finish(Some("tool_calls")));
        let streamed: Vec<String> = seq
            .iter()
            .filter(|e| e.0 == Some("content_block_start"))
            .map(|e| e.1["content_block"]["type"].as_str().unwrap().to_string())
            .collect();

        assert_eq!(
            buffered,
            vec!["tool_use".to_string()],
            "no phantom text block"
        );
        assert_eq!(streamed, buffered, "the two paths agree on one completion");
        // Starts alone cannot catch the bug that lives here: a `content_block_stop`
        // for a block that never opened, or a missing one for the tool. Compare
        // the whole opened/closed pair.
        let stops: Vec<usize> = seq
            .iter()
            .filter(|e| e.0 == Some("content_block_stop"))
            .map(|e| e.1["index"].as_u64().unwrap() as usize)
            .collect();
        let opens: Vec<usize> = seq
            .iter()
            .filter(|e| e.0 == Some("content_block_start"))
            .map(|e| e.1["index"].as_u64().unwrap() as usize)
            .collect();
        assert_eq!(
            stops, opens,
            "every block that opened is closed, and nothing else is"
        );
    }

    /// The sequence a reasoning model actually streams: thinking, then text,
    /// then a tool call. Content blocks share ONE index sequence, so they must
    /// be numbered in the order they open and every one must close — a client
    /// that indexes by `index` cannot be handed two blocks claiming 0.
    #[test]
    fn thinking_text_and_tools_number_in_open_order_and_all_close() {
        let mut s = AnthropicStream::new("m", "opencode2api-1");
        let mut think = probe_chunk("");
        think.reasoning = "weighing options".into();
        let mut events = s.on_chunk(&think);
        let said = probe_chunk("answering");
        events.extend(s.on_chunk(&said));
        events.extend(s.on_chunk(&tool_chunk(0, Some("tu_1"), Some("get_weather"), "{}")));
        events.extend(s.finish(Some("tool_calls")));

        let started: Vec<(u64, &str)> = events
            .iter()
            .filter(|e| e.0 == Some("content_block_start"))
            .map(|e| {
                (
                    e.1["index"].as_u64().unwrap(),
                    e.1["content_block"]["type"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            started,
            vec![(0, "thinking"), (1, "text"), (2, "tool_use")],
            "one sequence, allocated in opening order"
        );
        let kinds: Vec<&str> = events
            .iter()
            .filter_map(|e| e.1["delta"]["type"].as_str())
            .collect();
        assert_eq!(
            kinds,
            vec!["thinking_delta", "text_delta", "input_json_delta"],
            "each body kind belongs to its own block"
        );
        let stops: Vec<u64> = events
            .iter()
            .filter(|e| e.0 == Some("content_block_stop"))
            .filter_map(|e| e.1["index"].as_u64())
            .collect();
        assert_eq!(stops, vec![0, 1, 2], "every opened block closes, in order");
    }

    /// The loop this channel exists to close: a Claude-style client replays its
    /// assistant history, and the thinking block it sends back must become the
    /// field a reasoning upstream demands on prior assistant turns. Without it
    /// DeepSeek 400s the whole turn once `tools` are in play.
    #[test]
    fn an_assistant_thinking_block_folds_to_reasoning_content() {
        let req: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8,
            "messages": [
                {"role": "user", "content": "weather?"},
                {"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "need the city first", "signature": "sig"},
                    {"type": "text", "text": "checking"},
                    {"type": "tool_use", "id": "tu_1", "name": "get_weather", "input": {"city": "SF"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "tu_1", "content": "sunny"}
                ]}
            ]
        }))
        .unwrap();
        let chat = req.to_chat_request().unwrap();
        assert_eq!(chat.messages[1]["role"], "assistant");
        assert_eq!(chat.messages[1]["content"], "checking");
        assert_eq!(
            chat.messages[1]["reasoning_content"], "need the city first",
            "the thinking text reaches the field the upstream reads"
        );
        assert_eq!(chat.messages[1]["tool_calls"][0]["id"], "tu_1");
        // Anthropic-only: only Anthropic can mint one, so it stops here rather
        // than being smuggled toward a chat-shaped vendor that would reject it.
        assert!(chat.messages[1].get("signature").is_none());
    }

    /// A thinking block anywhere else has no home: the chat dialect carries
    /// reasoning text on an assistant message and nowhere else.
    #[test]
    fn thinking_outside_an_assistant_turn_refuses() {
        let req: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8,
            "messages": [{"role": "user", "content": [
                {"type": "thinking", "thinking": "in the wrong place"}
            ]}]
        }))
        .unwrap();
        let e = req.to_chat_request().unwrap_err();
        assert_eq!(e.status, 400);
        assert!(e.message.contains("user turn"), "{}", e.message);
    }

    /// An obfuscated thinking block is opaque bytes with no counterpart;
    /// forwarding the turn as if it had folded would erase a redaction.
    #[test]
    fn redacted_thinking_refuses_rather_than_vanishing() {
        let req: AnthropicRequest = serde_json::from_value(json!({
            "model": "m", "max_tokens": 8,
            "messages": [{"role": "assistant", "content": [
                {"type": "redacted_thinking", "data": "abc123"}
            ]}]
        }))
        .unwrap();
        let e = req.to_chat_request().unwrap_err();
        assert_eq!(e.status, 400, "must not fold silently to an empty turn");
        assert!(e.message.contains("redacted_thinking"), "{}", e.message);
    }

    /// The render half of the same contract, buffered: reasoning text becomes a
    /// thinking block and PRECEDES the text it produced, because that is the
    /// order the dialect's own answers use.
    #[test]
    fn buffered_reasoning_renders_a_thinking_block_first() {
        let mut c = Completion {
            refusal: None,
            id: "gen".into(),
            model: "m".into(),
            text: "because".into(),
            finish_reason: Some("stop".into()),
            usage: None,
            tool_calls: Vec::new(),
            raw: None,
            reasoning: Some("the user asked why".into()),
        };
        let v = completion_to_anthropic(&c);
        assert_eq!(v["content"][0]["type"], "thinking");
        assert_eq!(v["content"][0]["thinking"], "the user asked why");
        assert_eq!(
            v["content"][0]["signature"], "",
            "synthesized, so unsigned — stated openly rather than faked"
        );
        assert_eq!(v["content"][1]["type"], "text");
        // No reasoning means no empty thinking block riding along.
        c.reasoning = None;
        let v = completion_to_anthropic(&c);
        assert_eq!(v["content"].as_array().map(Vec::len), Some(1));
        assert_eq!(v["content"][0]["type"], "text");
    }

    /// …and the promise is still refused when there is nothing to back it.
    #[test]
    fn a_tool_finish_without_tool_blocks_still_collapses() {
        let mut s = AnthropicStream::new("m", "opencode2api-1");
        let _ = s.on_chunk(&probe_chunk("just text"));
        let fin = s.finish(Some("tool_calls"));
        let delta = fin.iter().find(|e| e.0 == Some("message_delta")).unwrap();
        assert_eq!(delta.1["delta"]["stop_reason"], "end_turn");
    }

    #[test]
    fn buffered_tool_calls_become_tool_use_blocks() {
        let c = Completion {
            refusal: None,
            id: "g".into(),
            model: "m".into(),
            text: String::new(),
            finish_reason: Some("tool_calls".into()),
            usage: None,
            tool_calls: vec![opencode2api_kit::ToolCall {
                id: "tu_1".into(),
                name: "get_weather".into(),
                arguments: "{\"city\":\"SF\"}".into(),
            }],
            raw: None,
            reasoning: None,
        };
        let v = completion_to_anthropic(&c);
        assert_eq!(
            v["content"][0]["type"], "tool_use",
            "tool-only turn: no empty text block"
        );
        assert_eq!(v["content"][0]["input"]["city"], "SF", "string -> object");
        assert_eq!(v["stop_reason"], "tool_use");
    }

    fn tool_chunk(index: u32, id: Option<&str>, name: Option<&str>, args: &str) -> ChatChunk {
        ChatChunk {
            refusal: String::new(),
            id: "c".into(),
            model: "m".into(),
            text: String::new(),
            finish_reason: None,
            usage: None,
            tool_calls: vec![opencode2api_kit::ToolCallDelta {
                index,
                id: id.map(str::to_string),
                name: name.map(str::to_string),
                arguments: args.to_string(),
            }],
            raw: None,
            reasoning: String::new(),
        }
    }

    fn probe_chunk(text: &str) -> ChatChunk {
        ChatChunk {
            refusal: String::new(),
            id: "c".into(),
            model: "m".into(),
            text: text.into(),
            finish_reason: None,
            usage: None,
            tool_calls: Vec::new(),
            raw: None,
            reasoning: String::new(),
        }
    }

    #[test]
    fn tool_finish_collapses_to_end_turn_never_tool_use() {
        // "tool_use" promises blocks this bridge cannot emit; end_turn is
        // the honest terminal for a text-only fold.
        for reason in ["tool_calls", "function_call"] {
            let c = Completion {
                refusal: None,
                id: "g".into(),
                model: "m".into(),
                text: "answer".into(),
                finish_reason: Some(reason.into()),
                usage: None,
                tool_calls: Vec::new(),
                raw: None,
                reasoning: None,
            };
            let v = completion_to_anthropic(&c);
            assert_eq!(v["stop_reason"], "end_turn", "{reason} must collapse");
        }
    }

    #[test]
    fn buffered_completion_renames_stop_and_usage() {
        let c = Completion {
            refusal: None,
            id: "gen-9".into(),
            model: "claude-x".into(),
            text: "done".into(),
            finish_reason: Some("length".into()),
            usage: Some(Usage {
                prompt_tokens: 5,
                completion_tokens: 2,
                ..Default::default()
            }),
            tool_calls: Vec::new(),
            raw: None,
            reasoning: None,
        };
        let v = completion_to_anthropic(&c);
        assert_eq!(v["stop_reason"], "max_tokens");
        assert_eq!(v["id"], "msg_gen-9");
        assert_eq!(v["usage"]["input_tokens"], 5);
        assert_eq!(v["content"][0]["text"], "done");
    }

    #[test]
    fn event_stream_shape_is_anthropic_ordering() {
        let mut s = AnthropicStream::new("m", "r1");
        let start = s.message_start();
        assert_eq!(
            start.iter().map(|e| e.0).collect::<Vec<_>>(),
            vec![Some("message_start"), Some("ping")]
        );
        let chunk = ChatChunk {
            refusal: String::new(),
            id: "r1".into(),
            model: "m".into(),
            text: "tok".into(),
            finish_reason: None,
            usage: None,
            tool_calls: Vec::new(),
            raw: None,
            reasoning: String::new(),
        };
        let d = s.on_chunk(&chunk);
        assert_eq!(d[0].0, Some("content_block_start"));
        assert_eq!(d[1].0, Some("content_block_delta"));
        assert_eq!(d[1].1["delta"]["text"], "tok");
        // second chunk must not re-open the block
        assert_eq!(s.on_chunk(&chunk)[0].0, Some("content_block_delta"));
        let f = s.finish(Some("stop"));
        let names: Vec<_> = f.iter().map(|e| e.0.unwrap()).collect();
        assert_eq!(
            names,
            vec!["content_block_stop", "message_delta", "message_stop"]
        );
        assert_eq!(f[1].1["delta"]["stop_reason"], "end_turn");
        // message_start must NOT carry stop fields (strict SDKs) — assert
        // once at the payload level:
        let ms = &start[0].1["message"];
        assert!(ms.get("stop_reason").is_none());
        assert!(ms.get("stop_sequence").is_none());
        // idempotent terminal
        assert!(s.finish(Some("stop")).is_empty());
    }

    #[test]
    fn silent_model_still_yields_a_closed_empty_block() {
        let mut s = AnthropicStream::new("m", "r2");
        s.message_start();
        let f = s.finish(None);
        let names: Vec<_> = f.iter().map(|e| e.0.unwrap()).collect();
        assert_eq!(
            names,
            vec![
                "content_block_start",
                "content_block_stop",
                "message_delta",
                "message_stop"
            ]
        );
    }

    #[test]
    fn error_event_closes_the_stream() {
        let mut s = AnthropicStream::new("m", "r3");
        s.message_start();
        let e = s.error("overloaded_error", "upstream died");
        assert_eq!(e[0].0, Some("error"));
        assert_eq!(e[0].1["error"]["type"], "overloaded_error");
        assert!(s.finish(Some("stop")).is_empty());
    }

    #[test]
    fn buffered_text_and_refusal_emit_text_blocks_with_refusal_terminal() {
        let completion = Completion {
            refusal: Some("I cannot assist".into()),
            id: "gen-9".into(),
            model: "claude-x".into(),
            text: "partial answer".into(),
            finish_reason: Some("refusal".into()),
            usage: None,
            tool_calls: Vec::new(),
            raw: None,
            reasoning: None,
        };
        let message = completion_to_anthropic(&completion);
        assert_eq!(message["content"][0]["text"], "partial answer");
        assert_eq!(message["content"][1]["type"], "text");
        assert_eq!(message["content"][1]["text"], "I cannot assist");
        assert_eq!(message["stop_reason"], "refusal");
    }

    /// Refusal may arrive before the answer, but Anthropic buffers it into a
    /// second text block. The stream's closed block sequence must therefore
    /// contain the same payloads, in the same order, as the buffered message.
    #[test]
    fn refusal_first_stream_closes_the_same_text_block_sequence_as_buffered() {
        let mut stream = AnthropicStream::new("claude-x", "opencode2api-1");
        let mut refusal_first = probe_chunk("");
        refusal_first.refusal = "I cannot assist".into();
        let mut events = stream.on_chunk(&refusal_first);
        events.extend(stream.on_chunk(&probe_chunk("partial answer")));
        events.extend(stream.finish(Some("refusal")));

        let closed: Vec<&str> = events
            .iter()
            .filter(|event| event.0 == Some("content_block_stop"))
            .map(|event| event.1["index"].as_u64().unwrap())
            .map(|index| {
                events
                    .iter()
                    .find(|event| {
                        event.0 == Some("content_block_delta") && event.1["index"] == index
                    })
                    .map(|event| event.1["delta"]["text"].as_str().unwrap())
                    .unwrap_or_else(|| panic!("block {index} never emitted its text"))
            })
            .collect();
        assert_eq!(closed, ["partial answer", "I cannot assist"]);
    }
}
