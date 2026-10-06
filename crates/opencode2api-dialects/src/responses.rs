//! # opencode2api-dialects::responses
//!
//! The second inbound-dialect bridge (after `the anthropic module`):
//! translates OpenAI's **Responses API** (`/v1/responses`) to the
//! chat-completions-shaped IR, and back. Pure JSON, no I/O — the same
//! boundary rules as every bridge: it never names a vendor, only a dialect.
//!
//! Capability honesty, the same contract as every bridge: shapes the fold
//! cannot represent are **rejected with a 400**, never silently dropped —
//! `item_reference` items, hosted/MCP tools, and `input_audio` parts still
//! 400, while function `tools`/`tool_choice`, `function_call*` items, a
//! `reasoning` item's replayable summary text (folded onto the assistant turn
//! it precedes), user-turn `input_image` parts and user-turn `input_file`
//! parts fold. A `reasoning` item with no summary or content text still 400s:
//! nothing about it can be replayed. A non-function object `tool_choice` is
//! refused for the same reason as hosted tools: the chat vendor schema accepts
//! only a string or a named function. An
//! explicit `null` for `tools`/`tool_choice` — what SDKs emit for an unset
//! optional — counts as absence, not as a refusal.
//! Note the asymmetry on `file_id`: an IMAGE reference cannot fold (chat's
//! `image_url` has no such field), but a DOCUMENT reference passes through —
//! the `file` object does have one, and on this dialect the id IS the
//! upstream's own namespace.
//! A cache hint is FORWARDED when the target wire defines it: the Chat
//! Completions `image_url` and `file` parts both carry the same
//! `prompt_cache_breakpoint` object this dialect's parts carry (verified
//! against the spec-generated types), so dropping it would cost the client a
//! cache hit on a wire that could honour it. Anthropic's `cache_control` is
//! the opposite case — `{type:"ephemeral",ttl}` has no chat counterpart at all,
//! so that one is dropped (see the anthropic module), and neither is refused,
//! because both would 400 ordinary cached traffic.
//! State fields
//! (`previous_response_id`, `store`, `conversation`) are UNCONDITIONALLY
//! accepted-and-dropped: a proxy cannot see what the referenced response
//! contained, so a check could not reject the risky requests without
//! breaking the legitimate ones. Consequence, stated plainly: a request
//! relying on stored chains is answered from ONLY the `input` it carries —
//! no error, no history injection. Clients needing continuity must replay
//! their history in `input` (the API's documented stateless mode).
//! Sampling and tool-execution controls this dialect's own request grammar
//! defines are forwarded verbatim: `temperature`, `top_p`, `stop`, and
//! `parallel_tool_calls`. The chat-only sampling knobs `frequency_penalty`,
//! `presence_penalty`, `seed`, and `logit_bias` are REFUSED by name: they
//! are not part of this request grammar, so forwarding them would retune the
//! upstream behind the client's back and dropping them would lose the intent
//! in silence. Other request-only knobs with no chat position are
//! accepted-and-dropped: `background`, `include`, `metadata`, `prompt`,
//! `safety_identifier`, `service_tier`, `text` beyond `text.format`,
//! `truncation`, and `user`; they govern storage, retrieval, or provider
//! policy rather than completion sampling. Request-echo fields on terminal
//! responses are STATIC rather than a replay of the inbound request: the
//! shared `Completion` IR does not retain those request values, so
//! `parallel_tool_calls` is null alongside null `temperature`/`top_p`, while
//! `store` and `previous_response_id` stay their documented stateless values.
//!
//! Streaming surface, verified against real-stream fixtures rather than
//! recall (`axonhub` `llm/transformer/openai/codex/outbound_executor_test.go:316`
//! and `llm/transformer/openai/responses/aggregator_test.go:140-236`):
//! named `response.*` events with `sequence_number` **starting at 0** and
//! strictly increasing across the stream, opening `response.created` +
//! `response.in_progress` + `output_item.added` + `content_part.added`,
//! per-token `response.output_text.delta`, closing `output_text.done` +
//! `content_part.done` + `output_item.done` + `response.completed` (the only
//! event carrying usage; a `length` stop downgrades to
//! `response.incomplete` with `incomplete_details.reason`). No `[DONE]`
//! sentinel exists in this dialect — the terminal object event ends the
//! stream.
//! Refusal parts follow the same lifecycle as output text. Stream content
//! indexes follow arrival order; buffered `Completion` has no field order, so
//! its render always places output text before refusal.
//! Output-item `output_index` follows ANNOUNCEMENT order: slot 0 belongs to
//! the message unless reasoning was announced first (which is also the
//! buffered order), and the message's slot is reserved before any tool item
//! allocates, so a tool call that arrives before any text still leaves the
//! message where the buffered render puts it. The one deliberate deviation is
//! reasoning streamed AFTER the message: it takes the next free slot, so its
//! terminal array position differs from the buffered render's reasoning-first
//! order. The parity test therefore covers the reasoning-first direction.

use opencode2api_kit::sse::EventSink;
use opencode2api_kit::{
    ChatChunk, ChatRequest, Completion, ProviderError, file_part, file_ref_part, image_part,
    parts_content, text_part,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

/// The single output item this bridge ever emits — one text message, so its
/// id is a constant every event that names an item shares.
const ITEM_ID: &str = "msg_opencode2api";

/// Collects events as `Value`s, for callers that want documents instead of
/// frames (the `on_chunk` API, and every test in this crate).
#[derive(Default)]
struct Collected(Vec<RespEvent>);

impl EventSink for Collected {
    fn event<T: Serialize + ?Sized>(&mut self, name: Option<&'static str>, payload: &T) {
        // Every Responses event is named; an anonymous one would be a bug in
        // this bridge, not something to render onto the wire.
        if let Some(name) = name {
            self.0.push(RespEvent(
                name,
                serde_json::to_value(payload).unwrap_or_else(|_| json!({})),
            ));
        }
    }
}

/// One named Responses SSE event awaiting framing.
#[derive(Debug, Clone, PartialEq)]
pub struct RespEvent(pub &'static str, pub Value);

/// A parsed `/v1/responses` body.
#[derive(Debug, Clone, Deserialize)]
pub struct ResponsesRequest {
    pub model: String,
    /// String or item array; both fold to chat messages.
    pub input: Value,
    #[serde(default)]
    pub instructions: Option<Value>,
    #[serde(default)]
    pub max_output_tokens: Option<u64>,
    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl ResponsesRequest {
    pub fn to_chat_request(&self) -> Result<ChatRequest, ProviderError> {
        let mut messages = Vec::new();
        if let Some(sys) = &self.instructions {
            // `system` content is text-only in the chat dialect, so a media
            // part here refuses rather than folding.
            let content = fold_content(sys, "system")?;
            if content.as_str().is_some_and(|t| !t.is_empty()) {
                messages.push(json!({"role": "system", "content": content}));
            }
        }
        match &self.input {
            Value::String(s) => messages.push(json!({"role": "user", "content": s})),
            Value::Array(items) => {
                // A `reasoning` item is history for the assistant turn that
                // FOLLOWS it, and the chat wire keeps thinking text on that
                // message rather than as its own entry — so it accumulates here
                // and is consumed by the next assistant message whatever that
                // message turns out to be (a `function_call`, or the assistant
                // `message` two items later).
                let mut pending_reasoning = String::new();
                for item in items {
                    // Tool items fold into the chat dialect's own shapes; a
                    // type this bridge cannot represent is still REJECTED
                    // rather than skipped, because dropping one would answer
                    // from a history whose tool traffic vanished.
                    match item.get("type").and_then(Value::as_str) {
                        Some("reasoning") => {
                            let text = reasoning_text(item)?;
                            if !pending_reasoning.is_empty() {
                                pending_reasoning.push('\n');
                            }
                            pending_reasoning.push_str(&text);
                            continue;
                        }
                        Some("function_call") => {
                            let reasoning = std::mem::take(&mut pending_reasoning);
                            let mut call = json!({
                                "role": "assistant",
                                "content": Value::Null,
                                "tool_calls": [{
                                    "id": item.get("call_id").or_else(|| item.get("id"))
                                        .cloned().unwrap_or(json!("")),
                                    "type": "function",
                                    "function": {
                                        "name": item.get("name").cloned().unwrap_or(json!("")),
                                        // Already a JSON string on this wire,
                                        // exactly as the chat dialect wants it.
                                        "arguments": item.get("arguments").cloned()
                                            .unwrap_or(json!("{}")),
                                    },
                                }],
                            });
                            if !reasoning.is_empty() {
                                call["reasoning_content"] = json!(reasoning);
                            }
                            messages.push(call);
                            continue;
                        }
                        Some("function_call_output") => {
                            let body = match item.get("output") {
                                Some(Value::String(s)) => s.clone(),
                                // A function may RETURN image or file parts on
                                // this wire; chat's tool content is `string |
                                // text parts`, so text joins and anything else
                                // 400s. There is deliberately no stringify
                                // fallback: an out-of-spec `output` would
                                // otherwise land in the tool result as a JSON
                                // dump the model reads as the answer's text.
                                Some(Value::Array(items)) => tool_output_text(items)?,
                                Some(Value::Null) | None => String::new(),
                                Some(_) => {
                                    return Err(ProviderError::bad_request(
                                        "function_call_output.output must be a string or an \
                                         array of parts",
                                    ));
                                }
                            };
                            messages.push(json!({
                                "role": "tool",
                                "tool_call_id": item.get("call_id").cloned().unwrap_or(json!("")),
                                "content": body,
                            }));
                            continue;
                        }
                        Some(t) if t != "message" => {
                            return Err(ProviderError::bad_request(format!(
                                "input item type \"{t}\" is not representable in this \
                                 bridge (message and function items are; extend \
                                 the responses module for the rest)"
                            )));
                        }
                        _ => {}
                    }
                    let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
                    let content = item
                        .get("content")
                        .ok_or_else(|| ProviderError::bad_request("input item has no content"))?;
                    let mut m = json!({
                        "role": role,
                        "content": fold_content(content, role)?,
                    });
                    // An assistant `message` item is the other place a preceding
                    // reasoning item's text belongs.
                    if role == "assistant" && !pending_reasoning.is_empty() {
                        m["reasoning_content"] = json!(std::mem::take(&mut pending_reasoning));
                    }
                    messages.push(m);
                }
                // Reasoning with no following assistant turn has nowhere to go:
                // appending it as its own message would put thinking text on a
                // role the chat wire does not carry it for, and dropping it is
                // the silence this bridge is built to avoid.
                if !pending_reasoning.is_empty() {
                    return Err(ProviderError::bad_request(
                        "a trailing reasoning item is not representable: its text belongs to \
                         an assistant turn that never followed (extend this module or use \
                         the fidelity lane)",
                    ));
                }
            }
            _ => {
                return Err(ProviderError::bad_request(
                    "input must be a string or item array",
                ));
            }
        }

        let mut extra = Map::new();
        for key in ["temperature", "top_p", "stop", "parallel_tool_calls"] {
            if let Some(value) = self.extra.get(key).filter(|value| !value.is_null()) {
                extra.insert(key.to_string(), value.clone());
            }
        }
        // `frequency_penalty`, `presence_penalty`, `seed` and `logit_bias` are
        // CHAT sampling knobs this dialect's own request grammar does not
        // define. Flattened `extra` would otherwise accept them as if native
        // and retune the upstream, or drop them in silence — so they are
        // refused by name instead.
        for key in [
            "frequency_penalty",
            "presence_penalty",
            "seed",
            "logit_bias",
        ] {
            if self.extra.get(key).is_some_and(|value| !value.is_null()) {
                return Err(ProviderError::bad_request(format!(
                    "{key} is a chat-completions sampling knob the /v1/responses request \
                     grammar does not define, so this bridge cannot claim it (drop it or take \
                     the fidelity lane)"
                )));
            }
        }
        if let Some(mt) = self.max_output_tokens {
            extra.insert("max_tokens".into(), json!(mt));
        }
        // Responses spells a function tool FLAT (`{type,name,parameters}`)
        // where chat nests it under `function` — the only representable form.
        match self.extra.get("tools") {
            None | Some(Value::Null) => {}
            Some(Value::Array(tools)) => {
                let mut folded = Vec::with_capacity(tools.len());
                for tool in tools {
                    let Value::Object(tool) = tool else {
                        return Err(ProviderError::bad_request(format!(
                            "tools entries must be function objects, not a {} payload",
                            value_kind(tool)
                        )));
                    };
                    let kind = tool
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or("object-without-type");
                    if kind != "function" {
                        return Err(ProviderError::bad_request(format!(
                            "tool type \"{kind}\" is a hosted or MCP tool with no closed chat \
                             request shape: it is not representable in this bridge (use \
                             function tools or take the fidelity lane)"
                        )));
                    }
                    let mut function = Map::new();
                    function.insert(
                        "name".into(),
                        tool.get("name").cloned().unwrap_or(json!("")),
                    );
                    if let Some(description) = tool.get("description") {
                        function.insert("description".into(), description.clone());
                    }
                    if let Some(parameters) = tool.get("parameters") {
                        function.insert("parameters".into(), parameters.clone());
                    }
                    folded.push(json!({
                        "type": "function",
                        "function": Value::Object(function),
                    }));
                }
                if !folded.is_empty() {
                    extra.insert("tools".into(), Value::Array(folded));
                }
            }
            Some(value) => {
                return Err(ProviderError::bad_request(format!(
                    "tools must be an array of function tools, not a {} payload",
                    value_kind(value)
                )));
            }
        }
        match self.extra.get("tool_choice") {
            None | Some(Value::Null) => {}
            Some(Value::String(choice)) => {
                extra.insert("tool_choice".into(), Value::String(choice.clone()));
            }
            Some(Value::Object(object))
                if object.get("type").and_then(Value::as_str) == Some("function") =>
            {
                extra.insert(
                    "tool_choice".into(),
                    json!({
                        "type": "function",
                        "function": {
                            "name": object.get("name").cloned().unwrap_or(json!(""))
                        }
                    }),
                );
            }
            Some(Value::Object(object)) => {
                let kind = object
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                return Err(ProviderError::bad_request(format!(
                    "tool_choice type \"{kind}\" is a hosted/MCP selection with no closed \
                     chat request shape: it is not representable in this bridge (use a \
                     string or a named function tool, or take the fidelity lane)"
                )));
            }
            Some(value) => {
                return Err(ProviderError::bad_request(format!(
                    "tool_choice must be a string or named function object, not a {} payload",
                    value_kind(value)
                )));
            }
        }
        // text.format maps to response_format, reasoning.effort to
        // reasoning_effort.
        // text.format {format:{…}} -> response_format {…}
        if let Some(fmt) = self.extra.get("text").and_then(|t| t.get("format")) {
            extra.insert("response_format".into(), fmt.clone());
        }
        // reasoning {effort} -> reasoning_effort (chat-dialect spelling)
        if let Some(e) = self.extra.get("reasoning").and_then(|r| r.get("effort")) {
            extra.insert("reasoning_effort".into(), e.clone());
        }

        Ok(ChatRequest {
            model: self.model.clone(),
            messages,
            stream: self.stream,
            extra,
        })
    }
}

/// The string a text-ish content part carries. A `refusal` spells its payload
/// `refusal`, not `text` — reading only `text` would drop a replayed refusal
/// out of the history in silence, which is the exact loss this bridge exists
/// to prevent. Shared by the message fold and the tool-output fold because
/// "what counts as this dialect's text" must have one answer.
fn part_text(part: &Value) -> Result<&str, ProviderError> {
    let kind = part.get("type").and_then(Value::as_str).unwrap_or("?");
    let key = if kind == "refusal" { "refusal" } else { "text" };
    match part.get(key) {
        Some(Value::String(text)) => Ok(text),
        Some(value) => Err(ProviderError::bad_request(format!(
            "content part type \"{kind}\" has a {key} payload of kind \"{}\", not a string: \
             it cannot be folded without losing the part (return string text or take the \
             fidelity lane)",
            value_kind(value)
        ))),
        None => Err(ProviderError::bad_request(format!(
            "content part type \"{kind}\" has no {key} payload: it cannot be folded without \
             losing the part (return string text or take the fidelity lane)"
        ))),
    }
}

fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Fold a string-or-parts content value into the chat dialect's `content`.
/// `parts_content` owns the resulting shape: text-only turns join with
/// newlines exactly as before media folded, and a turn that carried an image
/// becomes an ordered part array in the client's order, because interleaving
/// text and images is part of the prompt.
///
/// `role` is a parameter, not decoration: this dialect's schema accepts an
/// `input_image` under `system`, `developer` and `assistant` as well, but the
/// chat dialect puts image parts on USER content only. Folding one into any
/// other role would hand the vendor a body it refuses for a reason the client
/// cannot see, so it is refused here with that reason named.
fn fold_content(content: &Value, role: &str) -> Result<Value, ProviderError> {
    let Value::Array(parts) = content else {
        // A plain string is the common case and stays exactly that; `null` is
        // absent content. Anything else is not a content value, and folding it
        // to empty text would answer from an input that lost a part.
        return match content {
            Value::String(_) => Ok(content.clone()),
            Value::Null => Ok(json!("")),
            _ => Err(ProviderError::bad_request(
                "message content must be a string or an array of parts",
            )),
        };
    };
    let mut out: Vec<Value> = Vec::with_capacity(parts.len());
    for p in parts {
        let kind = p.get("type").and_then(Value::as_str).unwrap_or("?");
        if matches!(kind, "input_image" | "input_file") {
            // The chat dialect puts media parts on USER content only, while
            // this dialect's schema accepts them under system/developer and
            // even assistant. Folding one into another role would hand the
            // vendor a body it refuses for a reason the client cannot see.
            if role != "user" {
                return Err(ProviderError::bad_request(format!(
                    "a {kind} in a {role} message is not representable: the chat dialect \
                     carries media parts on user turns only (send it as a user turn or \
                     use the fidelity lane)"
                )));
            }
            out.push(with_cache_hint(
                p,
                if kind == "input_image" {
                    image_part_from_input(p)?
                } else {
                    file_part_from_input(p)?
                },
            ));
            continue;
        }
        match kind {
            "input_text" | "output_text" | "text" | "refusal" => {
                out.push(text_part(part_text(p)?));
            }
            other => {
                return Err(ProviderError::bad_request(format!(
                    "content part type \"{other}\" is not representable in this bridge \
                     (text, input_image and input_file are; audio needs the fidelity lane)"
                )));
            }
        }
    }
    Ok(parts_content(out))
}

/// One `input_image` part -> the IR's `image_url` part.
///
/// The two dialects spell the same image differently: here `image_url` is a
/// FLAT string (URL or `data:` URL), in chat it is an object holding `url`
/// and `detail`. One input has no chat equivalent at all — a `file_id`.
/// Chat's `image_url` has no field to carry it, and resolving a Files-API id
/// takes a vendor round-trip with a credential, which is provider-crate work
/// since a bridge does no I/O. Refused, not dropped: answering from a history
/// whose image vanished is the failure this bridge exists to avoid. `detail`
/// is the client's own word and passes through untouched.
fn image_part_from_input(part: &Value) -> Result<Value, ProviderError> {
    if part.get("file_id").and_then(Value::as_str).is_some() {
        return Err(ProviderError::bad_request(
            "input_image references a Files-API file_id, which this proxy does not \
             resolve (the chat dialect's image_url has no file_id) — inline the image as \
             a data:image/…;base64, URL or declare this dialect native for the fidelity lane",
        ));
    }
    let url = part
        .get("image_url")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ProviderError::bad_request("input_image has neither image_url nor file_id")
        })?;
    image_part(url, part.get("detail").and_then(Value::as_str))
}

/// One `input_file` part -> the IR's `file` part.
///
/// This is the one place the two media parts differ in what they can carry:
/// chat's `file` object has `filename` + `file_data` (the same
/// `data:<media>;base64,` composition images use) AND a `file_id`, so a
/// reference that already belongs to the upstream's file store passes through
/// intact. `file_url` is the exception — documented as Responses-only, with no
/// field on the chat side to hold it.
///
/// Exactly one of `file_data`/`file_id` is required: OpenAI documents no
/// precedence when both are present, and a fold that guessed would decide
/// which bytes the model sees. `detail` is refused unless it is `auto`:
/// "Chat Completions file inputs don't support `detail`", so forwarding it is
/// an unknown key the vendor may reject for a reason this proxy caused, while
/// dropping an explicit `low`/`high` would silently change how PDF pages are
/// rendered. `auto` is that field's documented default, so omitting it loses
/// nothing the client asked for.
fn file_part_from_input(part: &Value) -> Result<Value, ProviderError> {
    if let Some(d) = part.get("detail").and_then(Value::as_str)
        && d != "auto"
    {
        return Err(ProviderError::bad_request(format!(
            "input_file detail {d:?} is not representable: Chat Completions file inputs \
             have no detail field, and dropping it would change how the pages render \
             (omit it or take the fidelity lane)"
        )));
    }
    if part.get("file_url").and_then(Value::as_str).is_some() {
        return Err(ProviderError::bad_request(
            "input_file file_url is Responses-only: the chat file object carries \
             filename/file_data/file_id and no url field — inline the base64 or take the \
             fidelity lane",
        ));
    }
    let data = part.get("file_data").and_then(Value::as_str);
    let id = part
        .get("file_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    match (data, id) {
        (Some(_), Some(_)) => Err(ProviderError::bad_request(
            "input_file carries both file_data and file_id — the fold requires exactly one \
             source, because OpenAI documents no precedence between them",
        )),
        (Some(data), None) => file_part(part.get("filename").and_then(Value::as_str), data),
        (None, Some(id)) => Ok(file_ref_part(id)),
        (None, None) => Err(ProviderError::bad_request(
            "input_file has none of file_data/file_id/file_url — there is no document to \
             fold, and dropping the part would answer from an input that lost it",
        )),
    }
}

/// Carry the client's `prompt_cache_breakpoint` onto a folded media part.
///
/// The Chat Completions `image_url` and `file` parts define this exact field
/// (both spec-generated types carry `prompt_cache_breakpoint: {mode:…}`), so
/// this is a pass-through, not an invention — the same rule that forwards
/// `detail` and a `file_id` rather than discarding what the target can carry.
/// Anthropic's `cache_control` cannot be treated the same way: its
/// `{type:"ephemeral",ttl}` shape has no chat counterpart, so the anthropic
/// module drops it and says so.
///
/// The fold stays generic, and narrow schemas are the provider's problem:
/// GLM's OpenAPI is `additionalProperties:false` on exactly these media parts,
/// so a GLM-shaped `to_upstream_body` strips this key (and `detail`) in code —
/// which is the same forward-and-strip split README "Vendor specials" already
/// prescribes. Emitting the OpenAI-defined shape here is what keeps the
/// deployment knob a vendor concern instead of a bridge branch.
///
/// Two guards, then: text parts are untouched (the field was verified on the
/// image and file types, and an unverified field is not one to inject), and a
/// NON-object under that key is dropped — forwarding `true` or a string is
/// inventing a value, and it would only reach the strict vendors above.
fn with_cache_hint(src: &Value, mut part: Value) -> Value {
    let Some(bp) = src.get("prompt_cache_breakpoint").filter(|v| v.is_object()) else {
        return part;
    };
    if let Some(obj) = part.as_object_mut() {
        obj.insert("prompt_cache_breakpoint".into(), bp.clone());
    }
    part
}

/// A `function_call_output.output` array: text items join into the tool
/// message, anything else 400s.
///
/// This dialect lets a function return `input_image` or `input_file` items —
/// documented, and exactly what a computer-use client does with a screenshot.
/// Chat's tool content is `string | text parts`, so there is no position for
/// them. Refused rather than relocated: hoisting a screenshot into an
/// invented user turn would tell the model the human sent it.
fn tool_output_text(items: &[Value]) -> Result<String, ProviderError> {
    let mut texts: Vec<&str> = Vec::with_capacity(items.len());
    for p in items {
        match p.get("type").and_then(Value::as_str).unwrap_or("?") {
            "input_text" | "output_text" | "text" | "refusal" => {
                texts.push(part_text(p)?);
            }
            other => {
                return Err(ProviderError::bad_request(format!(
                    "a function_call_output item of type \"{other}\" has no chat position \
                     (tool content is a string or text parts) — return text, or declare \
                     this dialect native and take the fidelity lane"
                )));
            }
        }
    }
    Ok(texts.join("\n"))
}

fn resp_id(id: &str) -> String {
    let core = id.strip_prefix("resp_").unwrap_or(id);
    format!("resp_{core}")
}

fn message_item(text: &str, refusal: Option<&str>) -> Value {
    let mut content = Vec::new();
    if !text.is_empty() || refusal.is_none() {
        content.push(json!({"type":"output_text","text":text,"annotations":[]}));
    }
    if let Some(refusal) = refusal {
        content.push(json!({"type":"refusal","refusal":refusal}));
    }
    json!({
        "id": ITEM_ID, "type": "message", "role": "assistant",
        "status": "completed", "content": content,
    })
}

fn message_item_in_order(text: &str, refusal: Option<&str>, order: &[&str]) -> Value {
    let mut content = Vec::new();
    for kind in order {
        match *kind {
            "text" => content.push(json!({"type":"output_text","text":text,"annotations":[]})),
            "refusal" => {
                if let Some(refusal) = refusal {
                    content.push(json!({"type":"refusal","refusal":refusal}));
                }
            }
            _ => unreachable!("only response content kinds are tracked"),
        }
    }
    if content.is_empty() {
        content.push(json!({"type":"output_text","text":"","annotations":[]}));
    }
    json!({
        "id": ITEM_ID, "type": "message", "role": "assistant",
        "status": "completed", "content": content,
    })
}

/// Usage carries only fields the Responses wire can represent. A vendor's own
/// total wins; otherwise it is the arithmetic fallback. Cached and reasoning
/// details ride in the standard nested objects; no server-tool usage has a
/// Responses position and is therefore omitted.
fn usage_value(usage: &opencode2api_kit::Usage) -> Value {
    let mut value = Map::from_iter([
        ("input_tokens".into(), usage.prompt_tokens.into()),
        ("output_tokens".into(), usage.completion_tokens.into()),
        (
            "total_tokens".into(),
            usage
                .total_tokens
                .unwrap_or(usage.prompt_tokens + usage.completion_tokens)
                .into(),
        ),
    ]);
    if let Some(cached) = usage.cached_tokens {
        value.insert(
            "input_tokens_details".into(),
            json!({"cached_tokens": cached}),
        );
    }
    if let Some(reasoning) = usage.reasoning_tokens {
        value.insert(
            "output_tokens_details".into(),
            json!({"reasoning_tokens": reasoning}),
        );
    }
    Value::Object(value)
}

/// The `output` array: reasoning, the assistant message, then one
/// `function_call` per tool call. A refusal is a message content part, so it
/// replaces `output_text` rather than becoming a synthetic error.
fn output_items(c: &Completion) -> Value {
    let mut items = Vec::new();
    if let Some(reasoning) = c.reasoning.as_deref().filter(|s| !s.is_empty()) {
        items.push(reasoning_item(&reasoning_id(&c.id), reasoning));
    }
    items.push(message_item(&c.text, c.refusal.as_deref()));
    for (i, call) in c.tool_calls.iter().enumerate() {
        items.push(function_call_item(i, &call.id, &call.name, &call.arguments));
    }
    Value::Array(items)
}

/// The terminal reasoning item. Streaming and buffered renders share this
/// builder so their completed output arrays can be compared byte-for-byte.
fn reasoning_item(id: &str, text: &str) -> Value {
    json!({
        "type": "reasoning",
        "id": id,
        "summary": [{ "type": "summary_text", "text": text }],
    })
}

fn reasoning_id(response_id: &str) -> String {
    let core = response_id
        .rsplit_once('_')
        .map_or(response_id, |(_, core)| core);
    format!("rs_{core}")
}

/// The text a `reasoning` item carries: `summary` blocks are what a stateless
/// client can replay, and `content` (when a vendor sends it) is the raw text.
/// Neither present means there is nothing to fold — refused, because an item
/// claiming to be reasoning with no readable text is not this shape.
fn reasoning_text(item: &Value) -> Result<String, ProviderError> {
    let mut out = Vec::new();
    for key in ["summary", "content"] {
        if let Value::Array(parts) = item.get(key).cloned().unwrap_or(Value::Null) {
            for part in parts {
                out.push(part_text(&part)?.to_string());
            }
        }
    }
    if out.is_empty() {
        return Err(ProviderError::bad_request(
            "reasoning item carries no summary or content text: nothing can be replayed \
             to a chat-shaped upstream (an encrypted `id`-only item needs the state this \
             proxy does not keep — take the fidelity lane)",
        ));
    }
    Ok(out.join("\n"))
}

/// One `function_call` output item. `arguments` stays the JSON STRING the
/// wire carries — this dialect spells it the same way the chat one does, so
/// there is nothing to convert and nothing to risk mangling.
fn function_call_item(index: usize, call_id: &str, name: &str, arguments: &str) -> Value {
    function_call_item_status(index, call_id, name, arguments, "completed")
}

/// The same item mid-flight: `output_item.added` announces one before its
/// arguments exist, so the status has to be the caller's to choose.
fn function_call_item_status(
    index: usize,
    call_id: &str,
    name: &str,
    arguments: &str,
    status: &str,
) -> Value {
    json!({
        "id": format!("fc_opencode2api_{index}"),
        "type": "function_call",
        "status": status,
        "call_id": call_id,
        "name": name,
        "arguments": arguments,
    })
}

/// Buffered IR completion -> full `response` object. A refusal remains a
/// successful `completed` response; the native refusal content part carries
/// the payload and the top-level `output_text` is empty by the dialect's rule.
pub fn completion_to_response(c: &Completion) -> Value {
    let incomplete = c.finish_reason.as_deref() == Some("length");
    json!({
        "id": resp_id(&c.id), "object": "response",
        "created_at": opencode2api_kit::util::now_epoch_secs(),
        "status": if incomplete { "incomplete" } else { "completed" },
        "error": Value::Null,
        "incomplete_details": if incomplete { json!({"reason": "max_output_tokens"}) } else { Value::Null },
        "model": c.model, "output": output_items(c),
        "output_text": c.text,
        "parallel_tool_calls": Value::Null, "previous_response_id": Value::Null,
        "store": false, "temperature": Value::Null, "top_p": Value::Null,
        "usage": c.usage.as_ref().map(usage_value).unwrap_or(Value::Null),
    })
}

/// Stateful translation of IR chunks into the Responses event stream. The
/// server drives: `start()` once at handoff, `on_chunk()` per chunk,
/// `finish()` at clean end, `fail()` on terminal error (mutually exclusive).
#[derive(Debug)]
pub struct ResponsesStream {
    id: String,
    model: String,
    seq: u64,
    created_at: u64,
    text: String,
    /// Reasoning text streamed so far, announced as its own `reasoning` output
    /// item (see `on_chunk_into`).
    reasoning: String,
    /// Whether the reasoning item was announced, and the output slot it owns.
    /// Announcement order decides slots, so reasoning streamed BEFORE the text
    /// takes index 0 — the same order the buffered render uses — while
    /// reasoning streamed after takes the next free slot.
    reasoning_open: bool,
    /// Output index carried by the reasoning item's lifecycle events.
    reasoning_index: usize,
    /// Output slot for the message item plus whether it is already claimed.
    /// Resolved before ANY tool item allocates, so the message keeps the
    /// position the buffered render gives it even when its own announce is
    /// deferred to `finish()`.
    message_index: usize,
    message_slot_reserved: bool,
    /// Whether the message item is open, and the content indexes assigned to
    /// text/refusal in first-production order.
    message_opened: bool,
    text_index: Option<usize>,
    refusal_index: Option<usize>,
    parts: Vec<&'static str>,
    refusal: String,
    finish_reason: Option<String>,
    usage: Option<opencode2api_kit::Usage>,
    started: bool,
    finished: bool,
    /// Tool calls under construction, in the order their output items were
    /// added: `(upstream index, call_id, name, arguments so far)`. The
    /// arguments are accumulated because this dialect's terminal events
    /// (`.done`, and the final response object) must carry the WHOLE string,
    /// unlike Anthropic's, which only ever streams fragments.
    tools: Vec<ToolBuild>,
}

#[derive(Debug, Default)]
struct ToolBuild {
    upstream_index: u32,
    call_id: String,
    name: String,
    arguments: String,
    /// Output slot assigned at announcement, so the terminal array and every
    /// later event can reuse it instead of re-deriving an index.
    output_index: usize,
}

impl ResponsesStream {
    pub fn new(model: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            id: resp_id(&id.into()),
            model: model.into(),
            seq: 0,
            created_at: opencode2api_kit::util::now_epoch_secs(),
            text: String::new(),
            reasoning: String::new(),
            reasoning_open: false,
            reasoning_index: 0,
            message_index: 0,
            message_slot_reserved: false,
            message_opened: false,
            text_index: None,
            refusal_index: None,
            parts: Vec::new(),
            refusal: String::new(),
            finish_reason: None,
            usage: None,
            started: false,
            finished: false,
            tools: Vec::new(),
        }
    }

    fn next_seq(&mut self) -> u64 {
        let s = self.seq;
        self.seq += 1;
        s
    }

    /// Snapshot of the in-flight response object (as the spec embeds in
    /// created/in_progress/completed events).
    fn snapshot(&self, status: &str) -> Value {
        json!({
            "id": self.id, "object": "response", "created_at": self.created_at,
            "status": status, "error": Value::Null, "incomplete_details": Value::Null,
            "model": self.model, "output": [], "output_text": self.text,
            "parallel_tool_calls": Value::Null, "previous_response_id": Value::Null,
            "store": false, "temperature": Value::Null, "top_p": Value::Null,
            "usage": self.usage.as_ref().map(usage_value).unwrap_or(Value::Null),
        })
    }

    pub fn start(&mut self) -> Vec<RespEvent> {
        if self.started || self.finished {
            return Vec::new();
        }
        self.started = true;
        vec![
            RespEvent(
                "response.created",
                json!({"type":"response.created","sequence_number":self.next_seq(),"response":self.snapshot("in_progress")}),
            ),
            RespEvent(
                "response.in_progress",
                json!({"type":"response.in_progress","sequence_number":self.next_seq(),"response":self.snapshot("in_progress")}),
            ),
        ]
    }

    /// Open only the message item. Each content part announces its own index;
    /// sharing one hard-coded index would make text and refusal overwrite each
    /// other's lifecycle instead of forming the same ordered `content` array as
    /// the buffered render.
    fn ensure_message_opened(&mut self, out: &mut impl EventSink) {
        if self.message_opened {
            return;
        }
        self.message_opened = true;
        let index = self.reserve_message_slot();
        let seq = self.next_seq();
        out.event(
            Some("response.output_item.added"),
            &json!({"type":"response.output_item.added","sequence_number":seq,"output_index":index,"item":{"id":ITEM_ID,"type":"message","role":"assistant","status":"in_progress","content":[]}}),
        );
    }

    /// Claim the message's output slot if no item has yet. A tool call arriving
    /// before any text calls this first, so the message keeps the position the
    /// buffered render gives it even though its announce waits for `finish()`.
    fn reserve_message_slot(&mut self) -> usize {
        if !self.message_slot_reserved {
            self.message_index = if self.reasoning_open {
                self.next_slot()
            } else {
                0
            };
            self.message_slot_reserved = true;
        }
        self.message_index
    }

    fn ensure_text_part(&mut self, out: &mut impl EventSink) -> usize {
        self.ensure_message_opened(out);
        if let Some(index) = self.text_index {
            return index;
        }
        let index = self.refusal_index.map_or(0, |i| i + 1);
        self.text_index = Some(index);
        self.parts.push("text");
        let seq = self.next_seq();
        out.event(
            Some("response.content_part.added"),
            &json!({
                "type":"response.content_part.added","sequence_number":seq,
                "item_id":ITEM_ID,"output_index":self.message_index,"content_index":index,
                "part":{"type":"output_text","text":"","annotations":[]}
            }),
        );
        index
    }

    fn ensure_refusal_part(&mut self, out: &mut impl EventSink) -> usize {
        self.ensure_message_opened(out);
        if let Some(index) = self.refusal_index {
            return index;
        }
        let index = self.text_index.map_or(0, |i| i + 1);
        self.refusal_index = Some(index);
        self.parts.push("refusal");
        let seq = self.next_seq();
        out.event(
            Some("response.content_part.added"),
            &json!({
                "type":"response.content_part.added","sequence_number":seq,
                "item_id":ITEM_ID,"output_index":self.message_index,"content_index":index,
                "part":{"type":"refusal","refusal":""}
            }),
        );
        index
    }

    /// The streaming path: one chunk's events, written straight into the
    /// caller's sink. `on_chunk` is this function with a collecting sink, so
    /// the state machine — sequence numbers included — exists exactly once.
    pub fn on_chunk_into(&mut self, chunk: &ChatChunk, out: &mut impl EventSink) {
        if self.finished {
            return;
        }
        // Re-stamp BEFORE the preamble is emitted so `response.created`'s
        // snapshot carries the real model when the caller skipped banner().
        if !chunk.model.is_empty() && self.model.is_empty() {
            self.model.clone_from(&chunk.model);
        }
        if chunk.usage.is_some() {
            self.usage = chunk.usage;
        }
        if chunk.finish_reason.is_some() {
            self.finish_reason.clone_from(&chunk.finish_reason);
        }
        // A stream that reaches this point without banner() must still open
        // with response.created at sequence 0 — the preamble is EMITTED,
        // never generated-and-discarded (discarding would burn the numbers
        // while hiding the lifecycle event clients wait on).
        if !self.started {
            for ev in self.start() {
                out.event(Some(ev.0), &ev.1);
            }
        }
        // Reasoning opens as its own output item before any text, mirroring the
        // buffered order. `summary` is the only field this proxy can fill: an
        // `encrypted_content` chain credential belongs to the upstream, so
        // emitting one here would be a block claiming a promise it cannot keep.
        if !chunk.reasoning.is_empty() {
            let rs_id = reasoning_id(&self.id);
            if !self.reasoning_open {
                self.reasoning_open = true;
                // Reasoning announced before the message takes slot 0, exactly
                // as the buffered render places it; announced after, it takes
                // the next free slot instead of colliding with the message.
                self.reasoning_index = if self.message_slot_reserved {
                    self.next_slot()
                } else {
                    self.message_index = 1;
                    self.message_slot_reserved = true;
                    0
                };
                let seq = self.next_seq();
                out.event(
                    Some("response.output_item.added"),
                    &json!({
                        "type": "response.output_item.added",
                        "sequence_number": seq,
                        "output_index": self.reasoning_index,
                        "item": { "id": rs_id, "type": "reasoning", "summary": [] },
                    }),
                );
                let seq = self.next_seq();
                out.event(
                    Some("response.reasoning_summary_part.added"),
                    &json!({
                        "type": "response.reasoning_summary_part.added",
                        "sequence_number": seq,
                        "item_id": rs_id,
                        "output_index": self.reasoning_index,
                        "summary_index": 0,
                        "part": {"type": "summary_text", "text": ""},
                    }),
                );
            }
            self.reasoning.push_str(&chunk.reasoning);
            let seq = self.next_seq();
            out.event(
                Some("response.reasoning_summary_part.delta"),
                &json!({
                    "type": "response.reasoning_summary_part.delta",
                    "sequence_number": seq,
                    "item_id": rs_id,
                    "output_index": self.reasoning_index,
                    "summary_index": 0,
                    "delta": chunk.reasoning,
                }),
            );
        }
        if !chunk.text.is_empty() {
            let index = self.ensure_text_part(out);
            self.text.push_str(&chunk.text);
            let seq = self.next_seq();
            out.event(
                Some("response.output_text.delta"),
                &json!({"type":"response.output_text.delta","sequence_number":seq,"item_id":ITEM_ID,"output_index":self.message_index,"content_index":index,"delta":chunk.text}),
            );
        }
        if !chunk.refusal.is_empty() {
            let index = self.ensure_refusal_part(out);
            self.refusal.push_str(&chunk.refusal);
            let seq = self.next_seq();
            out.event(
                Some("response.refusal.delta"),
                &json!({
                    "type":"response.refusal.delta","sequence_number":seq,
                    "item_id":ITEM_ID,"output_index":self.message_index,
                    "content_index":index,"delta":chunk.refusal,
                }),
            );
        }
        for call in &chunk.tool_calls {
            self.on_tool_delta(call, out);
        }
    }

    /// Take the lowest `output_index` no item holds. Reservations count as
    /// held: the message claims its slot before it opens, so a tool call that
    /// arrives first cannot take it and then find the message displaced.
    fn next_slot(&mut self) -> usize {
        let mut used: Vec<usize> = self.tools.iter().map(|tool| tool.output_index).collect();
        if self.message_slot_reserved {
            used.push(self.message_index);
        }
        if self.reasoning_open {
            used.push(self.reasoning_index);
        }
        (0..)
            .find(|index| !used.contains(index))
            .expect("output indices are unbounded")
    }

    /// One tool-call fragment. The first for an upstream index adds its own
    /// `output_item` (the only frame carrying `call_id` and `name`); the rest
    /// are `function_call_arguments.delta`. The fragments are ALSO accumulated
    /// here, because this dialect's `.done` and its final response object owe
    /// the client the complete arguments string — a client that only saw the
    /// deltas would still be right, but one that trusts `.done` must be too.
    fn on_tool_delta(&mut self, call: &opencode2api_kit::ToolCallDelta, out: &mut impl EventSink) {
        let slot = match self
            .tools
            .iter()
            .position(|t| t.upstream_index == call.index)
        {
            Some(i) => i,
            None => {
                let slot = self.tools.len();
                self.reserve_message_slot();
                let output_index = self.next_slot();
                self.tools.push(ToolBuild {
                    upstream_index: call.index,
                    call_id: call.id.clone().unwrap_or_default(),
                    name: call.name.clone().unwrap_or_default(),
                    arguments: String::new(),
                    output_index,
                });
                let item = function_call_item_status(
                    slot,
                    &self.tools[slot].call_id,
                    &self.tools[slot].name,
                    "",
                    "in_progress",
                );
                let seq = self.next_seq();
                out.event(
                    Some("response.output_item.added"),
                    &json!({
                        "type": "response.output_item.added",
                        "sequence_number": seq,
                        "output_index": output_index,
                        "item": item,
                    }),
                );
                slot
            }
        };
        if call.arguments.is_empty() {
            return;
        }
        self.tools[slot].arguments.push_str(&call.arguments);
        let seq = self.next_seq();
        out.event(
            Some("response.function_call_arguments.delta"),
            &json!({
                "type": "response.function_call_arguments.delta",
                "sequence_number": seq,
                "item_id": format!("fc_opencode2api_{slot}"),
                "output_index": self.tools[slot].output_index,
                "delta": call.arguments,
            }),
        );
    }

    pub fn on_chunk(&mut self, chunk: &ChatChunk) -> Vec<RespEvent> {
        let mut collected = Collected::default();
        self.on_chunk_into(chunk, &mut collected);
        collected.0
    }

    /// Clean terminal. If the stream never started (a provider that handed
    /// back an empty ChatStream — the zero-chunk path), the FULL preamble is
    /// included before the closing chain, contiguous from sequence 0; a
    /// lifecycle-keyed client must never see done-events without a
    /// `response.created` or a numbering gap.
    pub fn finish(&mut self) -> Vec<RespEvent> {
        if self.finished {
            return Vec::new();
        }
        // Seed the preamble BEFORE marking finished: `start()` refuses to run
        // once the stream is terminal, so setting the flag first silently
        // dropped the lifecycle events on the zero-chunk path;
        // `finish_without_start_emits_full_preamble_from_zero` pins the order.
        let mut out = if self.started {
            Vec::new()
        } else {
            self.start()
        };
        // `finished` deliberately stays false throughout terminal construction.
        // A panic must leave `fail()` available as the client-visible stop signal.
        let incomplete = self.finish_reason.as_deref() == Some("length");
        let status = if incomplete {
            "incomplete"
        } else {
            "completed"
        };
        let final_event = if incomplete {
            "response.incomplete"
        } else {
            "response.completed"
        };
        if self.reasoning_open {
            let reasoning = reasoning_item(&reasoning_id(&self.id), &self.reasoning);
            out.push(RespEvent(
                "response.reasoning_summary_part.done",
                json!({
                    "type": "response.reasoning_summary_part.done",
                    "sequence_number": self.next_seq(),
                    "item_id": reasoning_id(&self.id),
                    "output_index": self.reasoning_index,
                    "summary_index": 0,
                    "part": reasoning["summary"][0],
                }),
            ));
            out.push(RespEvent(
                "response.output_item.done",
                json!({
                    "type": "response.output_item.done",
                    "sequence_number": self.next_seq(),
                    "output_index": self.reasoning_index,
                    "item": reasoning,
                }),
            ));
        }
        // A lifecycle-keyed client waits for this opened item to close before it
        // advances to the message chain. The completed response also repeats
        // it so streamed and buffered output arrays remain identical.
        if !self.message_opened {
            let mut opened = Collected::default();
            self.ensure_message_opened(&mut opened);
            out.extend(opened.0);
        }
        if self.text_index.is_none() && self.refusal_index.is_none() {
            let mut opened = Collected::default();
            self.ensure_text_part(&mut opened);
            out.extend(opened.0);
        }
        if let Some(index) = self.text_index {
            out.push(RespEvent(
                "response.output_text.done",
                json!({
                    "type":"response.output_text.done","sequence_number":self.next_seq(),
                    "item_id":ITEM_ID,"output_index":self.message_index,"content_index":index,
                    "text":self.text,
                }),
            ));
            out.push(RespEvent(
                "response.content_part.done",
                json!({
                    "type":"response.content_part.done","sequence_number":self.next_seq(),
                    "item_id":ITEM_ID,"output_index":self.message_index,"content_index":index,
                    "part":{"type":"output_text","text":self.text,"annotations":[]},
                }),
            ));
        }
        if let Some(index) = self.refusal_index {
            out.push(RespEvent(
                "response.refusal.done",
                json!({
                    "type":"response.refusal.done","sequence_number":self.next_seq(),
                    "item_id":ITEM_ID,"output_index":self.message_index,"content_index":index,
                    "refusal":self.refusal,
                }),
            ));
            out.push(RespEvent(
                "response.content_part.done",
                json!({
                    "type":"response.content_part.done","sequence_number":self.next_seq(),
                    "item_id":ITEM_ID,"output_index":self.message_index,"content_index":index,
                    "part":{"type":"refusal","refusal":self.refusal},
                }),
            ));
        }
        let terminal_item = message_item_in_order(
            &self.text,
            (!self.refusal.is_empty()).then_some(self.refusal.as_str()),
            &self.parts,
        );
        out.push(RespEvent(
            "response.output_item.done",
            json!({
                "type":"response.output_item.done","sequence_number":self.next_seq(),
                "output_index":self.message_index,"item":terminal_item,
            }),
        ));
        // Every tool item closes with the arguments COMPLETE — the deltas
        // were fragments, and a client that waited for `.done` gets the whole
        // string here rather than being asked to have reassembled it.
        for slot in 0..self.tools.len() {
            let (call_id, name, arguments) = {
                let t = &self.tools[slot];
                (t.call_id.clone(), t.name.clone(), t.arguments.clone())
            };
            let output_index = self.tools[slot].output_index;
            out.push(RespEvent(
                "response.function_call_arguments.done",
                json!({
                    "type": "response.function_call_arguments.done",
                    "sequence_number": self.next_seq(),
                    "item_id": format!("fc_opencode2api_{slot}"),
                    "output_index": output_index,
                    "arguments": arguments,
                }),
            ));
            out.push(RespEvent(
                "response.output_item.done",
                json!({
                    "type": "response.output_item.done",
                    "sequence_number": self.next_seq(),
                    "output_index": output_index,
                    "item": function_call_item(slot, &call_id, &name, &arguments),
                }),
            ));
        }
        let mut response = self.snapshot(status);
        if let Some(object) = response.as_object_mut() {
            // Announcement order, i.e. assigned output slot order: reasoning is
            // NOT first when the upstream streamed it after the message.
            let mut items: Vec<(usize, Value)> = vec![(self.message_index, terminal_item)];
            if self.reasoning_open {
                items.push((
                    self.reasoning_index,
                    reasoning_item(&reasoning_id(&self.id), &self.reasoning),
                ));
            }
            for (slot, tool) in self.tools.iter().enumerate() {
                items.push((
                    tool.output_index,
                    function_call_item(slot, &tool.call_id, &tool.name, &tool.arguments),
                ));
            }
            items.sort_by_key(|(index, _)| *index);
            object.insert(
                "output".into(),
                Value::Array(items.into_iter().map(|(_, item)| item).collect()),
            );
            if incomplete {
                object.insert(
                    "incomplete_details".into(),
                    json!({"reason": "max_output_tokens"}),
                );
            }
        }
        out.push(RespEvent(
            final_event,
            json!({"type": final_event, "sequence_number": self.next_seq(), "response": response}),
        ));
        // Latch only after the complete terminal frame belongs to `out`; an
        // unwind above must not prevent `fail()` from ending the client wait.
        self.finished = true;
        out
    }

    /// Terminal failure for this dialect: the spec's `error` frame, then the
    /// stream closes. No completed/incomplete event follows. Asymmetric with
    /// `finish` on purpose: a failure before any event emits exactly ONE
    /// frame with no preamble — there is no message to open.
    pub fn fail(&mut self, error_type: &str, message: &str) -> Vec<RespEvent> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        vec![RespEvent(
            "error",
            json!({
                "type": "error",
                "sequence_number": self.next_seq(),
                "code": error_type,
                "message": message,
                "param": Value::Null,
            }),
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opencode2api_kit::Usage;

    #[test]
    fn input_folds_state_fields_drop_and_sampling_maps() {
        let req: ResponsesRequest = serde_json::from_value(json!({
            "model": "gpt-x",
            "instructions": "be nice",
            "input": [
                {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
                {"role": "assistant", "content": [{"type": "output_text", "text": "hello"}]}
            ],
            "stream": true,
            "temperature": 0.3,
            "top_p": 0.8,
            "stop": ["END"],
            "parallel_tool_calls": false,
            "text": {"format": {"type": "json_object"}},
            "reasoning": {"effort": "low"},
            "previous_response_id": "resp_statedrop",
            "store": true,
            "max_output_tokens": 128
        }))
        .unwrap();
        let chat = req.to_chat_request().unwrap();
        assert!(chat.wants_stream());
        assert_eq!(chat.messages[0]["role"], "system");
        assert_eq!(chat.messages[1]["content"], "hi");
        assert_eq!(chat.messages[2]["role"], "assistant");
        assert_eq!(chat.messages.len(), 3);
        assert_eq!(chat.extra["max_tokens"], 128);
        for (key, expected) in [
            ("temperature", json!(0.3)),
            ("top_p", json!(0.8)),
            ("stop", json!(["END"])),
            ("parallel_tool_calls", json!(false)),
        ] {
            assert_eq!(chat.extra[key], expected, "{key} must reach the chat body");
        }
        for key in [
            "frequency_penalty",
            "presence_penalty",
            "seed",
            "logit_bias",
        ] {
            let request: ResponsesRequest = serde_json::from_value(json!({
                "model": "g", "input": "hi", key: 1
            }))
            .unwrap();
            let error = request.to_chat_request().expect_err("not in the grammar");
            assert_eq!(error.status, 400, "{key}");
            assert!(error.message.contains(key), "{}", error.message);
            assert!(error.message.contains("fidelity lane"), "{}", error.message);
        }
        let null_control: ResponsesRequest = serde_json::from_value(json!({
            "model": "g", "input": "hi", "seed": null, "temperature": null
        }))
        .unwrap();
        let null_chat = null_control
            .to_chat_request()
            .expect("null optionals are absent");
        assert!(null_chat.extra.get("seed").is_none());
        assert!(null_chat.extra.get("temperature").is_none());
        assert_eq!(chat.extra["response_format"]["type"], "json_object");
        assert_eq!(chat.extra["reasoning_effort"], "low");
        assert!(chat.extra.get("previous_response_id").is_none());
        assert!(chat.extra.get("store").is_none());
    }

    /// Function items fold now; everything else this dialect can carry is
    /// still REFUSED rather than skipped, because a dropped item answers from
    /// a history that did not happen. The rejection rule survives the feature.
    #[test]
    fn unrepresentable_items_are_still_rejected_not_folded() {
        for kind in ["item_reference", "computer_call", "web_search_call"] {
            let req: ResponsesRequest = serde_json::from_value(json!({
                "model": "g",
                "input": [{"type": kind, "id": "x"}]
            }))
            .unwrap();
            let e = req
                .to_chat_request()
                .expect_err("{kind} is not representable");
            assert_eq!(e.status, 400);
            assert!(e.message.contains(kind), "got {}", e.message);
        }
    }

    /// The divergence this test exists to kill: the buffered render puts a
    /// `reasoning` item BEFORE the message and shifts the tool to index 2, and
    /// the stream must announce the same three items in the same order with the
    /// same indices. Otherwise toggling `stream: true` changes the shape of one
    /// completion.
    #[test]
    fn a_reasoning_turn_announces_the_same_output_items_streamed_or_buffered() {
        let c = Completion {
            refusal: None,
            id: "resp_ab".into(),
            model: "g".into(),
            text: "sunny".into(),
            finish_reason: Some("stop".into()),
            usage: None,
            tool_calls: Vec::new(),
            raw: None,
            reasoning: Some("checking the city".into()),
        };
        let buffered: Vec<(u64, String)> = completion_to_response(&c)["output"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(i, item)| (i as u64, item["type"].as_str().unwrap().to_string()))
            .collect();
        assert_eq!(
            buffered,
            vec![(0, "reasoning".to_string()), (1, "message".to_string())],
            "reasoning first, in the buffered shape too"
        );

        let mut st = ResponsesStream::new("g", "ab");
        let mut think = delta("");
        think.reasoning = "checking the city".into();
        let mut events = st.on_chunk(&think);
        events.extend(st.on_chunk(&delta("sunny")));
        events.extend(st.finish());
        let announced: Vec<(u64, String)> = events
            .iter()
            .filter(|e| e.0 == "response.output_item.added")
            .map(|e| {
                (
                    e.1["output_index"].as_u64().unwrap(),
                    e.1["item"]["type"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(
            announced,
            vec![(0, "reasoning".to_string()), (1, "message".to_string())],
            "the stream announces the same items at the same indices"
        );
        let added_seq = events
            .iter()
            .find(|event| event.0 == "response.reasoning_summary_part.added")
            .expect("summary part is announced before it closes")
            .1["sequence_number"]
            .as_u64()
            .unwrap();
        let done_seq = events
            .iter()
            .find(|event| event.0 == "response.reasoning_summary_part.done")
            .unwrap()
            .1["sequence_number"]
            .as_u64()
            .unwrap();
        assert!(added_seq < done_seq);
        assert!(events.iter().any(|event| {
            event.0 == "response.reasoning_summary_part.done"
                && event.1["item_id"] == "rs_ab"
                && event.1["output_index"] == 0
        }));
        assert!(events.iter().any(|event| {
            event.0 == "response.output_item.done"
                && event.1["output_index"] == 0
                && event.1["item"]["type"] == "reasoning"
        }));
        let completed = events.last().unwrap();
        assert_eq!(completed.0, "response.completed");
        assert_eq!(
            completed.1["response"]["output"],
            completion_to_response(&c)["output"],
            "toggling stream must not change the output array"
        );
        let close_order: Vec<(&str, u64)> = events
            .iter()
            .filter(|event| event.0 == "response.output_item.done")
            .map(|event| {
                (
                    event.1["item"]["type"].as_str().unwrap(),
                    event.1["sequence_number"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(close_order[0].0, "reasoning", "index 0 must close first");
        assert!(
            close_order[0].1
                < events
                    .iter()
                    .find(|event| event.0 == "response.content_part.done")
                    .unwrap()
                    .1["sequence_number"]
                    .as_u64()
                    .unwrap(),
            "no message part may close while the reasoning item is still open"
        );
    }

    /// The direction the slot allocator exists for: text arrives first, so the
    /// message owns index 0 and late reasoning must take the NEXT free slot —
    /// not reuse 0, and not force the terminal array to reorder itself.
    #[test]
    fn late_reasoning_takes_a_free_slot_after_the_message() {
        let mut stream = ResponsesStream::new("m", "late-1");
        let mut events = stream.on_chunk(&delta("sunny"));
        let mut think = delta("");
        think.reasoning = "second thoughts".into();
        events.extend(stream.on_chunk(&think));
        events.extend(stream.finish());

        let added: Vec<(u64, String)> = events
            .iter()
            .filter(|event| event.0 == "response.output_item.added")
            .map(|event| {
                (
                    event.1["output_index"].as_u64().unwrap(),
                    event.1["item"]["type"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(
            added,
            vec![(0, "message".to_string()), (1, "reasoning".to_string())],
            "late reasoning must not claim the message's index"
        );
        let indices: Vec<u64> = added.iter().map(|(index, _)| *index).collect();
        let mut unique = indices.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), indices.len(), "no two items share a slot");

        let completed = events.last().unwrap();
        let output_types: Vec<&str> = completed.1["response"]["output"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["type"].as_str().unwrap())
            .collect();
        assert_eq!(
            output_types,
            ["message", "reasoning"],
            "the terminal array follows announcement order, not reasoning-first"
        );
    }

    /// A tool call with no text at all: the message has no announce until
    /// `finish()`, so its slot must be reserved up front or the tool would
    /// take the position the buffered render gives the message.
    #[test]
    fn a_tool_only_turn_keeps_the_buffered_output_order() {
        let mut stream = ResponsesStream::new("m", "toolonly-1");
        let mut events = stream.on_chunk(&tool_chunk(0, Some("call_1"), Some("lookup"), "{}"));
        events.extend(stream.finish());

        let added_order: Vec<(&str, u64)> = events
            .iter()
            .filter(|event| event.0 == "response.output_item.added")
            .map(|event| {
                (
                    event.1["item"]["type"].as_str().unwrap(),
                    event.1["output_index"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            added_order,
            [("function_call", 1), ("message", 0)],
            "the tool follows the message's reserved slot"
        );

        let completion = Completion {
            refusal: None,
            id: "toolonly-1".into(),
            model: "m".into(),
            text: String::new(),
            finish_reason: Some("tool_calls".into()),
            usage: None,
            tool_calls: vec![opencode2api_kit::ToolCall {
                id: "call_1".into(),
                name: "lookup".into(),
                arguments: "{}".into(),
            }],
            raw: None,
            reasoning: None,
        };
        let completed = events.last().unwrap();
        let streamed: Vec<&str> = completed.1["response"]["output"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["type"].as_str().unwrap())
            .collect();
        let buffered_render = completion_to_response(&completion);
        let buffered: Vec<&str> = buffered_render["output"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["type"].as_str().unwrap())
            .collect();
        assert_eq!(streamed, buffered, "tool-only turns render the same array");
    }

    /// The exact edge the reservation exists for: reasoning, then a tool call,
    /// never any text. The message announces only at `finish()`, and it must
    /// still sit between the two in the terminal array, as buffered does.
    #[test]
    fn reasoning_and_a_tool_with_no_text_keep_the_buffered_output_order() {
        let mut stream = ResponsesStream::new("m", "rs-tool-1");
        let mut think = delta("");
        think.reasoning = "which tool".into();
        let mut events = stream.on_chunk(&think);
        events.extend(stream.on_chunk(&tool_chunk(0, Some("call_1"), Some("lookup"), "{}")));
        events.extend(stream.finish());

        let announced: Vec<(u64, String)> = events
            .iter()
            .filter(|event| event.0 == "response.output_item.added")
            .map(|event| {
                (
                    event.1["output_index"].as_u64().unwrap(),
                    event.1["item"]["type"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(
            announced,
            vec![
                (0, "reasoning".to_string()),
                (2, "function_call".to_string()),
                (1, "message".to_string()),
            ],
            "the message keeps slot 1, so the tool lands at 2"
        );
        let indices: Vec<u64> = announced.iter().map(|(index, _)| *index).collect();
        let mut unique = indices.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), indices.len(), "no two items share a slot");

        let completed = events.last().unwrap();
        let streamed: Vec<&str> = completed.1["response"]["output"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["type"].as_str().unwrap())
            .collect();
        assert_eq!(
            streamed,
            ["reasoning", "message", "function_call"],
            "terminal order matches the buffered render even with no text"
        );
    }

    /// `reasoning` moved out of that list: its summary text IS replayable, and
    /// the chat wire has a field for it. An item with no summary and no
    /// encrypted-content stand-in still refuses rather than folding to nothing
    /// — the name is representable now, an empty claim is not.
    #[test]
    fn a_reasoning_item_folds_onto_the_assistant_turn_it_precedes() {
        let req: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [
                {"role": "user", "content": "weather?"},
                {"type": "reasoning", "id": "rs_1",
                 "summary": [{"type": "summary_text", "text": "need the city"}]},
                {"type": "function_call", "call_id": "c1", "name": "get_weather",
                 "arguments": "{\"city\":\"SF\"}"}
            ]
        }))
        .unwrap();
        let chat = req.to_chat_request().unwrap();
        assert_eq!(chat.messages[1]["role"], "assistant");
        assert_eq!(
            chat.messages[1]["reasoning_content"], "need the city",
            "the summary reaches the field a reasoning upstream replays"
        );
        assert_eq!(chat.messages[1]["tool_calls"][0]["id"], "c1");

        let bare: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [{"type": "reasoning", "id": "rs_2"}]
        }))
        .unwrap();
        let e = bare.to_chat_request().unwrap_err();
        assert_eq!(e.status, 400);
        assert!(e.message.contains("no summary"), "{}", e.message);

        // Trailing, with no assistant turn to own it: still refused.
        let dangling: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [
                {"role": "user", "content": "hi"},
                {"type": "reasoning", "id": "rs_3",
                 "summary": [{"type": "summary_text", "text": "orphaned"}]}
            ]
        }))
        .unwrap();
        let e = dangling.to_chat_request().unwrap_err();
        assert_eq!(e.status, 400);
        assert!(e.message.contains("trailing reasoning"), "{}", e.message);

        let mixed: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [
                {"role": "user", "content": "weather?"},
                {"type": "reasoning", "id": "rs_4", "summary": [
                    {"type": "summary_text", "text": "readable"},
                    {"type": "summary_text", "text": 7}
                ]},
                {"type": "function_call", "call_id": "c2", "name": "get_weather",
                 "arguments": "{}"}
            ]
        }))
        .unwrap();
        let error = mixed
            .to_chat_request()
            .expect_err("a partial reasoning drop is loss");
        assert_eq!(error.status, 400);
        assert!(error.message.contains("number"), "{}", error.message);
    }

    #[test]
    fn malformed_text_and_refusal_parts_refuse_in_message_and_tool_output() {
        for (content, named, payload) in [
            (json!([{"type": "input_text"}]), "input_text", "no text"),
            (
                json!([{"type": "refusal", "refusal": 7}]),
                "refusal",
                "number",
            ),
        ] {
            let request: ResponsesRequest = serde_json::from_value(json!({
                "model": "g",
                "input": [{"role": "user", "content": content}]
            }))
            .unwrap();
            let error = request.to_chat_request().unwrap_err();
            assert_eq!(error.status, 400);
            assert!(error.message.contains(named), "{}", error.message);
            assert!(error.message.contains(payload), "{}", error.message);
        }

        let request: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [{
                "type": "function_call_output", "call_id": "c1",
                "output": [{"type": "output_text", "text": false}]
            }]
        }))
        .unwrap();
        let error = request.to_chat_request().unwrap_err();
        assert_eq!(error.status, 400);
        assert!(error.message.contains("output_text"), "{}", error.message);
        assert!(error.message.contains("boolean"), "{}", error.message);
    }

    /// The render half: reasoning becomes a `reasoning` output item ahead of
    /// the message item, and no reasoning means no item is invented.
    #[test]
    fn reasoning_renders_as_an_output_item_before_the_message() {
        let mut c = Completion {
            refusal: None,
            id: "resp_ab".into(),
            model: "g".into(),
            text: "sunny".into(),
            finish_reason: Some("stop".into()),
            usage: None,
            tool_calls: Vec::new(),
            raw: None,
            reasoning: Some("it asked for weather".into()),
        };
        let v = completion_to_response(&c);
        assert_eq!(v["output"][0]["type"], "reasoning");
        assert_eq!(v["output"][0]["summary"][0]["text"], "it asked for weather");
        assert_eq!(v["output"][1]["type"], "message");
        assert_eq!(v["output_text"], "sunny");
        c.reasoning = None;
        let v = completion_to_response(&c);
        assert_eq!(v["output"].as_array().map(Vec::len), Some(1));
        assert_eq!(v["output"][0]["type"], "message");
    }

    /// The two dialects spell the same image differently (here `image_url` is
    /// a FLAT string, in chat an object), and the client's own `detail`
    /// survives — including `original`, which this dialect documents and the
    /// chat reference's literal list does not.
    #[test]
    fn user_turn_input_images_fold_into_image_url_parts() {
        let req: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [{"role": "user", "content": [
                {"type": "input_text", "text": "what is in this?"},
                {"type": "input_image", "image_url": "https://host/a.jpg", "detail": "original"},
                {"type": "input_image", "image_url": "data:image/png;base64,AANA"}
            ]}]
        }))
        .unwrap();
        let chat = req.to_chat_request().expect("user-turn images fold");
        let parts = chat.messages[0]["content"]
            .as_array()
            .expect("array content");
        assert_eq!(
            parts
                .iter()
                .map(|p| p["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["text", "image_url", "image_url"],
            "the client's order survives"
        );
        assert_eq!(parts[1]["image_url"]["url"], "https://host/a.jpg");
        assert_eq!(parts[1]["image_url"]["detail"], "original");
        assert_eq!(parts[2]["image_url"]["url"], "data:image/png;base64,AANA");
        assert!(
            parts[2]["image_url"].get("detail").is_none(),
            "an omitted detail stays omitted — the dialect's default is auto"
        );
    }

    /// A text-only turn keeps folding to the plain string it always produced,
    /// and a `null` detail (which SDKs do send) is the same as an absent one.
    #[test]
    fn text_only_turns_still_fold_to_a_string() {
        let req: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [
                {"role": "user", "content": [
                    {"type": "input_text", "text": "a"},
                    {"type": "input_text", "text": "b"}
                ]},
                {"role": "user", "content": [
                    {"type": "input_image", "image_url": "https://h/a.png", "detail": null}
                ]}
            ]
        }))
        .unwrap();
        let chat = req.to_chat_request().unwrap();
        assert_eq!(chat.messages[0]["content"], "a\nb");
        assert!(
            chat.messages[1]["content"][0]["image_url"]
                .get("detail")
                .is_none()
        );
    }

    /// The refusals this dialect's extra reach forces on the fold: an image
    /// under a role the chat dialect has no image parts for, a Files-API
    /// reference (alone or beside a URL — exactly one source is required), a
    /// part with no source at all, an image a FUNCTION returned, and a
    /// document.
    #[test]
    fn media_is_refused_where_chat_cannot_carry_it() {
        let cases = [
            (
                "assistant role",
                json!({"model": "g", "input": [
                    {"role": "assistant", "content": [
                        {"type": "input_image", "image_url": "https://h/a.png"}
                    ]}
                ]}),
            ),
            (
                "system instructions",
                json!({"model": "g", "instructions": [
                    {"type": "input_image", "image_url": "https://h/a.png"}
                ], "input": "hi"}),
            ),
            (
                "file_id alone",
                json!({"model": "g", "input": [
                    {"role": "user", "content": [
                        {"type": "input_image", "file_id": "file-6F2k"}
                    ]}
                ]}),
            ),
            (
                "file_id beside a url",
                json!({"model": "g", "input": [
                    {"role": "user", "content": [
                        {"type": "input_image",
                         "image_url": "https://h/a.png", "file_id": "file-6F2k"}
                    ]}
                ]}),
            ),
            (
                "no source at all",
                json!({"model": "g", "input": [
                    {"role": "user", "content": [
                        {"type": "input_image", "detail": "high"}
                    ]}
                ]}),
            ),
            (
                "image returned by a tool",
                json!({"model": "g", "input": [
                    {"type": "function_call_output", "call_id": "c1", "output": [
                        {"type": "input_image", "image_url": "https://h/shot.png"}
                    ]}
                ]}),
            ),
            (
                "file_url document (Responses-only field)",
                json!({"model": "g", "input": [
                    {"role": "user", "content": [
                        {"type": "input_file", "file_url": "https://h/a.pdf"}
                    ]}
                ]}),
            ),
        ];
        for (what, body) in cases {
            let req: ResponsesRequest = serde_json::from_value(body).unwrap();
            let e = req
                .to_chat_request()
                .expect_err(&format!("{what} must 400"));
            assert_eq!(e.status, 400, "{what}");
        }
    }

    /// The document half of the fold: `file_data` keeps its `filename`, and a
    /// `file_id` passes through — the chat `file` object has the field, and
    /// unlike an image reference this dialect's ids ARE the upstream's own.
    #[test]
    fn user_turn_input_files_fold_into_file_parts() {
        let req: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [{"role": "user", "content": [
                {"type": "input_file", "filename": "letter.pdf",
                 "file_data": "data:application/pdf;base64,JVBER"},
                {"type": "input_text", "text": "summarize"}
            ]}]
        }))
        .unwrap();
        let chat = req.to_chat_request().expect("user-turn documents fold");
        let parts = chat.messages[0]["content"]
            .as_array()
            .expect("array content");
        assert_eq!(parts[0]["type"], "file");
        assert_eq!(parts[0]["file"]["filename"], "letter.pdf");
        assert_eq!(
            parts[0]["file"]["file_data"],
            "data:application/pdf;base64,JVBER"
        );
        assert_eq!(parts[1]["type"], "text", "the client's order survives");

        let by_id: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [{"role": "user", "content": [
                {"type": "input_file", "file_id": "file-6F2k"}
            ]}]
        }))
        .unwrap();
        let chat = by_id.to_chat_request().expect("a file_id has a chat field");
        assert_eq!(
            chat.messages[0]["content"][0]["file"]["file_id"],
            "file-6F2k"
        );

        // `detail: "auto"` is that field's documented default, so it is
        // omitted rather than forwarded as a key chat does not define.
        let auto: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [{"role": "user", "content": [
                {"type": "input_file", "file_id": "file-6F2k", "detail": "auto"}
            ]}]
        }))
        .unwrap();
        assert!(
            auto.to_chat_request().is_ok(),
            "auto is the default, not a request"
        );
    }

    /// OpenAI's guide shows `file_data` both ways — as a data URL and as a bare
    /// base64 payload — so the fold forwards either. What it will not do is
    /// invent a name for a payload whose type it cannot see: a raw payload with
    /// no `filename` is refused, because no OpenAI example shows `{file_data}`
    /// unnamed and the name is what tells PDF from DOCX.
    #[test]
    fn a_bare_base64_file_data_needs_a_filename_to_fold() {
        let req: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [{"role": "user", "content": [
                {"type": "input_file", "filename": "draconomicon.pdf",
                 "file_data": "JVBERi0xLjUKJY8K"}
            ]}]
        }))
        .unwrap();
        let chat = req
            .to_chat_request()
            .expect("raw base64 is a documented form");
        let part = &chat.messages[0]["content"][0];
        assert_eq!(part["file"]["file_data"], "JVBERi0xLjUKJY8K");
        assert_eq!(part["file"]["filename"], "draconomicon.pdf");

        // Raw payload + no filename is refused, not emitted namelessly: no
        // OpenAI example shows `{file_data}` without a name, and a missing
        // name is the one thing this proxy should not decide for the client.
        let nameless: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [{"role": "user", "content": [
                {"type": "input_file", "file_data": "JVBERi0xLjUKJY8K"}
            ]}]
        }))
        .unwrap();
        let e = nameless.to_chat_request().unwrap_err();
        assert_eq!(e.status, 400);
        assert!(e.message.contains("filename"), "got {}", e.message);

        // Garbage is still refused — forwarding that would be inventing content.
        let junk: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [{"role": "user", "content": [
                {"type": "input_file", "filename": "a.pdf", "file_data": "not base64 !@#"}
            ]}]
        }))
        .unwrap();
        assert_eq!(junk.to_chat_request().unwrap_err().status, 400);
    }

    /// A cache hint the target wire DOES define is forwarded, not dropped:
    /// chat's `image_url`/`file` parts carry `prompt_cache_breakpoint`, so
    /// discarding it would cost the client a cache hit for no fidelity reason.
    /// A value that is not the documented OBJECT is dropped instead — it can
    /// never be honoured, and relaying it only buys a strict vendor's
    /// unknown-key rejection for a directive that was already nonsense.
    #[test]
    fn a_prompt_cache_breakpoint_reaches_the_folded_part() {
        let req: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [{"role": "user", "content": [
                {"type": "input_image", "image_url": "https://h/a.png",
                 "prompt_cache_breakpoint": {"mode": "explicit"}},
                {"type": "input_file", "file_id": "file-6F2k",
                 "prompt_cache_breakpoint": {"mode": "explicit"}}
            ]}]
        }))
        .unwrap();
        let chat = req
            .to_chat_request()
            .expect("cost hints never break a fold");
        let parts = chat.messages[0]["content"].as_array().unwrap();
        assert_eq!(parts[0]["type"], "image_url");
        // Sibling of `image_url`, exactly where the chat part defines it.
        assert_eq!(parts[0]["prompt_cache_breakpoint"]["mode"], "explicit");
        assert_eq!(parts[1]["file"]["file_id"], "file-6F2k");
        assert_eq!(parts[1]["prompt_cache_breakpoint"]["mode"], "explicit");

        let malformed: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [{"role": "user", "content": [
                {"type": "input_image", "image_url": "https://h/a.png",
                 "prompt_cache_breakpoint": "yes"}
            ]}]
        }))
        .unwrap();
        let chat = malformed
            .to_chat_request()
            .expect("a bad hint never breaks a fold");
        assert_eq!(
            chat.messages[0]["content"][0].get("prompt_cache_breakpoint"),
            None,
            "shape is knowable here, so it is checked"
        );
    }

    /// What the document fold refuses, each for a stated reason: two sources
    /// at once (OpenAI documents no precedence), no source at all, a
    /// Responses-only `file_url`, and a rendering level chat cannot express.
    #[test]
    fn document_sources_are_required_to_be_unambiguous() {
        let cases = [
            (
                "input_file with both sources",
                json!({"model":"g","input":[{"role":"user","content":[
                    {"type":"input_file","file_id":"file-1",
                     "file_data":"data:application/pdf;base64,JVBER"}]}]}),
            ),
            (
                "input_file with no source",
                json!({"model":"g","input":[{"role":"user","content":[
                    {"type":"input_file","filename":"a.pdf"}]}]}),
            ),
            (
                "input_file with file_url",
                json!({"model":"g","input":[{"role":"user","content":[
                    {"type":"input_file","file_url":"https://h/a.pdf"}]}]}),
            ),
            (
                "input_file with detail high",
                json!({"model":"g","input":[{"role":"user","content":[
                    {"type":"input_file","file_id":"file-1","detail":"high"}]}]}),
            ),
            (
                "input_file in an assistant turn",
                json!({"model":"g","input":[{"role":"assistant","content":[
                    {"type":"input_file","file_id":"file-1"}]}]}),
            ),
        ];
        for (what, body) in cases {
            let req: ResponsesRequest = serde_json::from_value(body).unwrap();
            let e = req
                .to_chat_request()
                .expect_err(&format!("{what} must 400"));
            assert_eq!(e.status, 400, "{what}");
        }
    }

    /// A function returning an ARRAY of text parts is legal on this wire, and
    /// used to be JSON-dumped into the tool message as the model's "output".
    #[test]
    fn tool_output_arrays_join_their_text() {
        let req: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [
                {"type": "function_call", "call_id": "c1", "name": "n", "arguments": "{}"},
                {"type": "function_call_output", "call_id": "c1", "output": [
                    {"type": "input_text", "text": "sunny"},
                    {"type": "input_text", "text": "21C"}
                ]}
            ]
        }))
        .unwrap();
        let chat = req.to_chat_request().unwrap();
        assert_eq!(chat.messages[1]["role"], "tool");
        assert_eq!(chat.messages[1]["content"], "sunny\n21C");
    }

    /// A replayed refusal is history, not noise — and its payload lives under
    /// `refusal`, so a fold reading only `text` dropped it in silence.
    #[test]
    fn a_refusal_part_keeps_its_text() {
        let req: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "input": [{"role": "assistant", "content": [
                {"type": "refusal", "refusal": "I can't do that"}
            ]}]
        }))
        .unwrap();
        let chat = req.to_chat_request().unwrap();
        assert_eq!(chat.messages[0]["content"], "I can't do that");
    }

    /// Pins the unconditional state-field policy from BOTH sides:
    /// stateless traffic echoing an id (must keep folding — a reject here
    /// would break working clients), and a bare stateful request (folds to
    /// just its input; the doc says so, the test says it stays so).
    #[test]
    fn state_fields_never_reject_and_never_inject() {
        let full: ResponsesRequest = serde_json::from_value(json!({
            "model": "g",
            "previous_response_id": "resp_1",
            "store": true,
            "input": [
                {"role": "user", "content": "first"},
                {"role": "assistant", "content": "answer"},
                {"role": "user", "content": "and now?"}
            ]
        }))
        .unwrap();
        let chat = full
            .to_chat_request()
            .expect("echoed id must not break stateless traffic");
        assert_eq!(chat.messages.len(), 3, "history comes ONLY from input");
        assert!(chat.extra.get("previous_response_id").is_none());
        assert!(chat.extra.get("store").is_none());

        let bare: ResponsesRequest = serde_json::from_value(json!({
            "model": "g", "previous_response_id": "resp_1", "input": "and now?"
        }))
        .unwrap();
        let chat = bare
            .to_chat_request()
            .expect("documented accept-and-drop, not a 400");
        assert_eq!(chat.messages.len(), 1, "stored chain is NOT injected");
    }

    #[test]
    fn buffered_response_completed_and_incomplete() {
        let mut c = Completion {
            refusal: None,
            id: "gen-9".into(),
            model: "m".into(),
            text: "done".into(),
            finish_reason: Some("stop".into()),
            usage: Some(Usage {
                prompt_tokens: 2,
                completion_tokens: 3,
                ..Default::default()
            }),
            tool_calls: Vec::new(),
            raw: None,
            reasoning: None,
        };
        let v = completion_to_response(&c);
        assert_eq!(v["id"], "resp_gen-9");
        assert_eq!(v["status"], "completed");
        assert_eq!(v["output"][0]["content"][0]["type"], "output_text");
        assert_eq!(v["output_text"], "done");
        assert_eq!(v["usage"]["total_tokens"], 5);
        c.finish_reason = Some("length".into());
        let v = completion_to_response(&c);
        assert_eq!(v["status"], "incomplete");
        assert_eq!(v["incomplete_details"]["reason"], "max_output_tokens");
    }

    fn delta(text: &str) -> ChatChunk {
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
    fn event_grammar_order_and_sequence() {
        let mut s = ResponsesStream::new("m", "gen-1");
        let start = s.start();
        let start_seqs: Vec<_> = start
            .iter()
            .map(|e| {
                e.1["sequence_number"]
                    .as_u64()
                    .expect("every responses event carries sequence_number")
            })
            .collect();
        // `start()` is the LIFECYCLE preamble only. The message item is
        // announced by the first chunk that produces text, so item order follows
        // production order and a reasoning-first turn can take index 0 (see
        // `ensure_message_opened`); contiguity from sequence 0 is unchanged.
        assert_eq!(
            start_seqs,
            vec![0, 1],
            "fixtures pin response.created at sequence 0; preamble contiguous"
        );
        assert_eq!(
            start.iter().map(|e| e.0).collect::<Vec<_>>(),
            vec!["response.created", "response.in_progress"]
        );
        let d1 = s.on_chunk(&delta("hel"));
        assert_eq!(
            d1.iter().map(|e| e.0).collect::<Vec<_>>(),
            vec![
                "response.output_item.added",
                "response.content_part.added",
                "response.output_text.delta"
            ],
            "the first text chunk opens the item it then writes to"
        );
        let d2 = s.on_chunk(&delta("lo"));
        assert_eq!(d2[0].0, "response.output_text.delta");
        assert_eq!(d2[0].1["delta"], "lo");
        // correlate, don't just name: the fields a real client parses
        assert_eq!(d2[0].1["item_id"], "msg_opencode2api");
        assert_eq!(d2[0].1["output_index"], 0);
        assert_eq!(d2[0].1["content_index"], 0);
        let fin = s.finish();
        let names: Vec<_> = fin.iter().map(|e| e.0).collect();
        assert_eq!(
            names,
            vec![
                "response.output_text.done",
                "response.content_part.done",
                "response.output_item.done",
                "response.completed",
            ]
        );
        assert_eq!(fin[0].1["text"], "hello");
        assert_eq!(
            fin[0].1["item_id"], "msg_opencode2api",
            "same id across the chain"
        );
        assert_eq!(fin[3].1["response"]["status"], "completed");
        // CONTIGUOUS +1 across EVERY event the client would see (preamble +
        // deltas + terminal): catches an emitter that forgets to take a
        // sequence number, which strict-increase alone passes silently.
        let all: Vec<u64> = start
            .iter()
            .chain(d1.iter())
            .chain(d2.iter())
            .chain(fin.iter())
            .map(|e| {
                e.1["sequence_number"]
                    .as_u64()
                    .expect("every responses event carries sequence_number")
            })
            .collect();
        assert_eq!(all.first().copied(), Some(0), "stream opens at sequence 0");
        assert!(
            all.windows(2).all(|w| w[1] == w[0] + 1),
            "sequence numbers are contiguous, got {all:?}"
        );
        assert!(s.finish().is_empty(), "terminal twice is a no-op");
    }

    /// The reorder guard: `on_chunk` as the FIRST call (no banner) must emit
    /// a preamble whose `response.created` snapshot already carries THIS
    /// chunk's model — model re-stamp precedes start() precisely so the
    /// snapshot never opens empty. Every other assertion passes with a
    /// blank model, so this line alone pins the order.
    #[test]
    fn first_chunk_without_banner_opens_with_chunk_model() {
        let mut s = ResponsesStream::new("", "gen-m");
        let mut c = delta("x");
        c.model = "real-model".into();
        let out = s.on_chunk(&c);
        assert_eq!(out[0].0, "response.created");
        assert_eq!(out[0].1["response"]["model"], "real-model");
        let last = out.last().unwrap();
        assert_eq!(last.0, "response.output_text.delta");
        assert_eq!(last.1["delta"], "x");
        assert_eq!(
            out.iter()
                .map(|e| e.1["sequence_number"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4],
            "preamble + delta are contiguous from a cold stream"
        );
    }

    #[test]
    fn length_stop_downgrades_terminal_event() {
        let mut s = ResponsesStream::new("m", "g");
        s.start();
        let mut tail = delta("");
        tail.finish_reason = Some("length".into());
        s.on_chunk(&tail);
        let fin = s.finish();
        assert_eq!(fin.last().unwrap().0, "response.incomplete");
        assert_eq!(
            fin.last().unwrap().1["response"]["incomplete_details"]["reason"],
            "max_output_tokens"
        );
    }

    #[test]
    fn fail_emits_single_error_event_and_closes() {
        let mut s = ResponsesStream::new("m", "g");
        s.start();
        let e = s.fail("server_error", "upstream exploded");
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].0, "error");
        assert_eq!(e[0].1["message"], "upstream exploded");
        assert!(s.finish().is_empty());
    }

    #[test]
    fn terminal_construction_panic_leaves_failure_latch_available() {
        let mut stream = ResponsesStream::new("m", "g");
        stream.start();
        stream.text_index = Some(0);
        stream.parts.push("unreachable-content-kind");
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| stream.finish()));
        assert!(
            panic.is_err(),
            "test must panic during terminal construction"
        );
        let error = stream.fail("server_error", "terminal construction failed");
        assert_eq!(
            error.len(),
            1,
            "a panic must not suppress the client stop event"
        );
        assert_eq!(error[0].0, "error");
    }

    /// Zero-chunk stream (provider handed back an empty ChatStream — the
    /// shape a buffered upstream reply takes before re-encode): finish()
    /// must still open the lifecycle at `response.created` / sequence 0.
    #[test]
    fn finish_without_start_emits_full_preamble_from_zero() {
        let mut s = ResponsesStream::new("m", "gen-0");
        let fin = s.finish();
        let names: Vec<_> = fin.iter().map(|e| e.0).collect();
        assert_eq!(
            names,
            vec![
                "response.created",
                "response.in_progress",
                "response.output_item.added",
                "response.content_part.added",
                "response.output_text.done",
                "response.content_part.done",
                "response.output_item.done",
                "response.completed",
            ]
        );
        let seqs: Vec<u64> = fin
            .iter()
            .map(|e| {
                e.1["sequence_number"]
                    .as_u64()
                    .expect("every responses event carries sequence_number")
            })
            .collect();
        assert_eq!(seqs, (0..8).collect::<Vec<u64>>(), "contiguous from origin");
        assert_eq!(
            fin[6].1["item"]["content"][0]["type"], "output_text",
            "the empty completion still closes the part it announced"
        );
    }

    /// Mirror asymmetry, pinned: fail() before any event is exactly ONE
    /// frame with NO preamble — there is no message to open.
    #[test]
    fn fail_before_start_is_a_single_unframed_error_event() {
        let mut s = ResponsesStream::new("m", "gen-0");
        let e = s.fail("api_error", "died pre-handoff");
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].0, "error");
        assert_eq!(e[0].1["sequence_number"], 0);
        assert!(s.finish().is_empty());
    }

    // ── tool calling ─────────────────────────────────────────────────────

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

    /// Responses spells a function tool FLAT; the chat dialect nests it. That
    /// is the entire difference, and dropping it would send the upstream a
    /// tool it cannot read.
    #[test]
    fn tools_fold_from_flat_to_nested() {
        let req: ResponsesRequest = serde_json::from_value(json!({
            "model": "m", "input": "hi",
            "tools": [{"type": "function", "name": "lookup", "description": "d",
                       "parameters": {"type": "object"}}],
            "tool_choice": {"type": "function", "name": "lookup"}
        }))
        .unwrap();
        let chat = req.to_chat_request().expect("tools are representable now");
        assert_eq!(chat.extra["tools"][0]["type"], "function");
        assert_eq!(chat.extra["tools"][0]["function"]["name"], "lookup");
        assert_eq!(
            chat.extra["tools"][0]["function"]["parameters"]["type"],
            "object"
        );
        assert_eq!(chat.extra["tool_choice"]["function"]["name"], "lookup");
    }

    #[test]
    fn hosted_tools_and_non_function_choices_refuse_fidelity_lane() {
        for (tools, choice, named) in [
            (json!([{"type": "web_search"}]), json!("auto"), "web_search"),
            (
                json!([{"type": "mcp", "server_label": "docs"}]),
                json!("auto"),
                "mcp",
            ),
            (json!([]), json!({"type": "web_search"}), "web_search"),
        ] {
            let request: ResponsesRequest = serde_json::from_value(json!({
                "model": "m", "input": "hi", "tools": tools, "tool_choice": choice
            }))
            .unwrap();
            let error = request.to_chat_request().expect_err("closed chat schema");
            assert_eq!(error.status, 400);
            assert!(error.message.contains(named), "{}", error.message);
            assert!(error.message.contains("fidelity lane"), "{}", error.message);
        }

        for (tools, named) in [
            (json!(["web_search"]), "string"),
            (json!([{"name": "lookup"}]), "object-without-type"),
        ] {
            let request: ResponsesRequest = serde_json::from_value(json!({
                "model": "m", "input": "hi", "tools": tools
            }))
            .unwrap();
            let error = request
                .to_chat_request()
                .expect_err("tools entry must be an object");
            assert_eq!(error.status, 400);
            assert!(error.message.contains(named), "{}", error.message);
        }

        let request: ResponsesRequest = serde_json::from_value(json!({
            "model": "m", "input": "hi", "tools": null, "tool_choice": null
        }))
        .unwrap();
        let chat = request
            .to_chat_request()
            .expect("SDK null optionals are absent");
        assert!(chat.extra.get("tools").is_none());
        assert!(chat.extra.get("tool_choice").is_none());
    }

    #[test]
    fn empty_and_null_tool_fields_never_reach_the_chat_body() {
        for tools in [json!(null), json!([])] {
            let request: ResponsesRequest = serde_json::from_value(json!({
                "model": "m", "input": "hi", "tools": tools, "tool_choice": "auto"
            }))
            .unwrap();
            let chat = request.to_chat_request().expect("no tools to fold");
            assert!(
                chat.extra.get("tools").is_none(),
                "an empty tool list carries no information upstream accepts"
            );
            assert_eq!(chat.extra["tool_choice"], json!("auto"));
        }
    }

    /// A tool exchange in the INPUT is history: the call becomes an assistant
    /// turn with `tool_calls`, its output a `role:"tool"` message. Dropping
    /// either would answer from a conversation that never happened.
    #[test]
    fn function_items_fold_into_call_and_result_messages() {
        let req: ResponsesRequest = serde_json::from_value(json!({
            "model": "m",
            "input": [
                {"type": "message", "role": "user", "content": "weather?"},
                {"type": "function_call", "call_id": "call_1", "name": "lookup",
                 "arguments": "{\"city\":\"SF\"}"},
                {"type": "function_call_output", "call_id": "call_1", "output": "sunny"}
            ]
        }))
        .unwrap();
        let chat = req.to_chat_request().unwrap();
        assert_eq!(chat.messages.len(), 3);
        assert_eq!(chat.messages[1]["tool_calls"][0]["id"], "call_1");
        assert_eq!(
            chat.messages[1]["tool_calls"][0]["function"]["arguments"], "{\"city\":\"SF\"}",
            "already a string on this wire: forwarded, not re-encoded"
        );
        assert_eq!(chat.messages[2]["role"], "tool");
        assert_eq!(chat.messages[2]["tool_call_id"], "call_1");
        assert_eq!(chat.messages[2]["content"], "sunny");
    }

    /// The streamed grammar: its own output item, argument fragments, then a
    /// `.done` carrying the WHOLE string — a client that trusted `.done`
    /// instead of reassembling deltas must still be right.
    #[test]
    fn streamed_function_calls_add_an_item_and_complete_its_arguments() {
        let mut s = ResponsesStream::new("m", "opencode2api-1");
        let mut events = s.on_chunk(&tool_chunk(0, Some("call_1"), Some("lookup"), ""));
        events.extend(s.on_chunk(&tool_chunk(0, None, None, "{\"ci")));
        events.extend(s.on_chunk(&tool_chunk(0, None, None, "ty\":\"SF\"}")));

        let added = events
            .iter()
            .find(|e| e.0 == "response.output_item.added" && e.1["output_index"] == 1)
            .expect("the tool got its own output item");
        assert_eq!(added.1["item"]["type"], "function_call");
        assert_eq!(added.1["item"]["call_id"], "call_1");
        assert_eq!(added.1["item"]["status"], "in_progress");
        let deltas: Vec<&str> = events
            .iter()
            .filter(|e| e.0 == "response.function_call_arguments.delta")
            .filter_map(|e| e.1["delta"].as_str())
            .collect();
        assert_eq!(deltas, vec!["{\"ci", "ty\":\"SF\"}"]);

        let fin = s.finish();
        let done = fin
            .iter()
            .find(|e| e.0 == "response.function_call_arguments.done")
            .expect("arguments close");
        assert_eq!(
            done.1["arguments"], "{\"city\":\"SF\"}",
            "complete, not a fragment"
        );
        let completed = fin.last().expect("terminal event");
        assert_eq!(
            completed.1["response"]["output"][1]["type"],
            "function_call"
        );
        assert_eq!(
            completed.1["response"]["output"][1]["arguments"], "{\"city\":\"SF\"}",
            "the final object carries the whole call"
        );
        // Sequence numbers stay one contiguous run across both item kinds.
        let seqs: Vec<u64> = events
            .iter()
            .chain(fin.iter())
            .filter_map(|e| e.1["sequence_number"].as_u64())
            .collect();
        assert!(
            seqs.windows(2).all(|w| w[1] == w[0] + 1),
            "gaps or repeats in {seqs:?}"
        );
    }

    #[test]
    fn buffered_tool_calls_become_function_call_items() {
        let c = Completion {
            refusal: None,
            id: "g".into(),
            model: "m".into(),
            text: String::new(),
            finish_reason: Some("tool_calls".into()),
            usage: None,
            tool_calls: vec![opencode2api_kit::ToolCall {
                id: "call_1".into(),
                name: "lookup".into(),
                arguments: "{\"city\":\"SF\"}".into(),
            }],
            raw: None,
            reasoning: None,
        };
        let v = completion_to_response(&c);
        assert_eq!(
            v["output"][0]["type"], "message",
            "index 0 stays the message"
        );
        assert_eq!(v["output"][1]["type"], "function_call");
        assert_eq!(v["output"][1]["call_id"], "call_1");
        assert_eq!(v["output"][1]["arguments"], "{\"city\":\"SF\"}");
    }

    #[test]
    fn buffered_refusal_is_a_second_content_part_beside_output_text() {
        let completion = Completion {
            refusal: Some("I cannot assist".into()),
            id: "resp_ab".into(),
            model: "m".into(),
            text: "partial answer".into(),
            finish_reason: Some("refusal".into()),
            usage: None,
            tool_calls: Vec::new(),
            raw: None,
            reasoning: None,
        };
        let response = completion_to_response(&completion);
        let content = response["output"][0]["content"].as_array().unwrap();
        assert_eq!(content[0]["type"], "output_text");
        assert_eq!(content[0]["text"], "partial answer");
        assert_eq!(content[1]["type"], "refusal");
        assert_eq!(content[1]["refusal"], "I cannot assist");
    }

    #[test]
    fn streamed_text_and_refusal_own_separate_content_part_lifecycles() {
        let mut stream = ResponsesStream::new("m", "opencode2api-1");
        let text_chunk = delta("partial answer");
        let mut refusal_chunk = delta("");
        refusal_chunk.refusal = "I cannot assist".into();
        let mut events = stream.on_chunk(&text_chunk);
        events.extend(stream.on_chunk(&refusal_chunk));
        events.extend(stream.finish());

        let added: Vec<(u64, &str)> = events
            .iter()
            .filter(|event| event.0 == "response.content_part.added")
            .map(|event| {
                (
                    event.1["content_index"].as_u64().unwrap(),
                    event.1["part"]["type"].as_str().unwrap(),
                )
            })
            .collect();
        let done: Vec<(u64, &str)> = events
            .iter()
            .filter(|event| event.0 == "response.content_part.done")
            .map(|event| {
                (
                    event.1["content_index"].as_u64().unwrap(),
                    event.1["part"]["type"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(added, [(0, "output_text"), (1, "refusal")]);
        assert_eq!(done, [(0, "output_text"), (1, "refusal")]);
        assert!(events.iter().any(|event| {
            event.0 == "response.refusal.delta"
                && event.1["content_index"] == 1
                && event.1["delta"] == "I cannot assist"
        }));
    }

    #[test]
    fn streamed_refusal_only_never_invents_an_output_text_part() {
        let mut stream = ResponsesStream::new("m", "opencode2api-1");
        let mut refusal_chunk = delta("");
        refusal_chunk.refusal = "I cannot assist".into();
        let mut events = stream.on_chunk(&refusal_chunk);
        events.extend(stream.finish());

        let part_types: Vec<&str> = events
            .iter()
            .filter(|event| event.0 == "response.content_part.done")
            .map(|event| event.1["part"]["type"].as_str().unwrap())
            .collect();
        assert_eq!(part_types, ["refusal"]);
        assert!(
            events
                .iter()
                .all(|event| event.0 != "response.output_text.delta"
                    && event.0 != "response.output_text.done"),
            "a refusal-only turn has no output-text lifecycle: {events:?}"
        );
    }

    /// The terminal item and embedded completed response are snapshots a client
    /// may use instead of replaying deltas. Their content array must preserve
    /// the first-announced content indexes, whether text or refusal came first.
    #[test]
    fn terminal_content_order_matches_announced_indexes_in_both_stream_orders() {
        for (name, chunks, expected) in [
            (
                "text first",
                vec![("text", "partial answer"), ("refusal", "I cannot assist")],
                ["output_text", "refusal"],
            ),
            (
                "refusal first",
                vec![("refusal", "I cannot assist"), ("text", "partial answer")],
                ["refusal", "output_text"],
            ),
        ] {
            let mut stream = ResponsesStream::new("m", "opencode2api-1");
            let mut events = Vec::new();
            for (kind, payload) in chunks {
                let mut chunk = delta("");
                match kind {
                    "text" => chunk.text = payload.into(),
                    "refusal" => chunk.refusal = payload.into(),
                    _ => unreachable!(),
                }
                events.extend(stream.on_chunk(&chunk));
            }
            events.extend(stream.finish());

            let announced: Vec<u64> = events
                .iter()
                .filter(|event| event.0 == "response.content_part.added")
                .map(|event| event.1["content_index"].as_u64().unwrap())
                .collect();
            assert_eq!(announced, [0, 1], "{name}");
            let item_done = events
                .iter()
                .find(|event| event.0 == "response.output_item.done")
                .expect("message item completes");
            let completed = events.last().expect("completed event");
            let done_types: Vec<&str> = item_done.1["item"]["content"]
                .as_array()
                .unwrap()
                .iter()
                .map(|part| part["type"].as_str().unwrap())
                .collect();
            let completed_types: Vec<&str> = completed.1["response"]["output"][0]["content"]
                .as_array()
                .unwrap()
                .iter()
                .map(|part| part["type"].as_str().unwrap())
                .collect();
            assert_eq!(done_types, expected, "output_item.done: {name}");
            assert_eq!(completed_types, expected, "response.completed: {name}");
        }
    }
}
