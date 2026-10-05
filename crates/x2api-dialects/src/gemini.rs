//! # x2api-dialects::gemini
//!
//! The inbound-dialect bridge between Google's **Gemini `generateContent`**
//! API and the OpenAI-shaped IR. Wire facts verified against
//! `ai.google.dev/api/generate-content` and the function-calling / structured
//! -output / streaming guides (fetched 2026-09-04): camelCase protobuf JSON,
//! `contents[].parts[]` (text / functionCall / functionResponse), a separate
//! `systemInstruction`, `generationConfig`, `tools[].functionDeclarations`,
//! `:streamGenerateContent?alt=sse` yielding one `data: {GenerateContentResponse}`
//! per frame with **no `[DONE]` sentinel** (the last frame carries
//! `finishReason`), and `x-goog-api-key` auth.
//!
//! The model is NOT in the body — it is the path segment
//! (`/v1beta/models/{model}:generateContent`), so the handler reads it there
//! and passes it to [`GeminiRequest::to_chat_request`] beside the streaming
//! flag the `:streamGenerateContent` verb implies.
//!
//! Two rules every bridge here keeps, restated because this fold leans on both:
//!
//! 1. **Reject, never degrade.** Shapes the IR cannot carry back are
//!    `Err(400)`, not a silent skip: server-side-tool parts
//!    (`toolCall`/`toolResponse`/`executableCode`), `thought` parts and
//!    `thoughtSignature` (reasoning has no IR field), and built-in server
//!    tools (`googleSearch`/`codeExecution`/…) all reject — a chat-speaking
//!    upstream cannot honour them and dropping them would answer from a
//!    conversation that did not happen. Media folds only where the chat
//!    dialect can put it: `inlineData` on a USER turn becomes the `image_url`
//!    part (images) or the `file` part (`application/pdf`/`text/plain`).
//!    What still rejects: `fileData` (a `files/…` URI is Google's namespace,
//!    which the chat upstream does not own, and the chat file object has no
//!    url field), audio/video and every other inline media type, and any
//!    media outside a user turn.
//! 2. **Never promise what was not emitted.** A `functionCall` is rendered from
//!    the tool blocks this bridge actually wrote, and `finishReason` says
//!    `STOP` only from an upstream that genuinely finished the turn.
//!
//! Accepted-and-dropped (documented, not hidden) — the same honesty the
//! responses fold applies to `previous_response_id`: `safetySettings`
//! (the fold has no per-category block control, so a request relying on them
//! is answered UNfiltered), `cachedContent` (the proxy cannot see the cached
//! context; the request must carry its own `contents`), `serviceTier`,
//! `store`. Sampling knobs are forwarded only where OpenAI spells the same
//! thing (`temperature`/`topP`→`top_p`/`topK`→`top_k`/`candidateCount`→`n`/
//! `maxOutputTokens`→`max_tokens`/`stopSequences`→`stop`/penalties/`seed`);
//! `responseMimeType`+`responseSchema` become OpenAI `response_format`.

use bytes::BytesMut;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::borrow::Cow;
use x2api_kit::sse;
use x2api_kit::{
    ChatChunk, ChatRequest, Completion, ProviderError, Usage, data_url, file_part, image_part,
    parts_content, text_part,
};

/// A parsed `/v1beta/models/{model}:generateContent` body. Only the fields the
/// fold must sequence are typed (`contents`, `tools`); the config objects are
/// read out of `extra` by [`GeminiRequest::get`] with both wire spellings, and
/// every other unknown top-level field is accepted-and-dropped — the same
/// posture the responses fold takes for its state fields.
#[derive(Debug, Clone, Deserialize)]
pub struct GeminiRequest {
    /// Required. Conversation turns; each is `{role, parts[]}`.
    #[serde(default)]
    pub contents: Vec<Value>,
    #[serde(default)]
    pub tools: Option<Vec<Value>>,
    /// Everything else. serde's expected key for a field IS the Rust
    /// identifier, so the camelCase wire keys (`systemInstruction`,
    /// `generationConfig`, `toolConfig`, …) were never claimable by snake_case
    /// fields, and `#[serde(flatten)]` collects them here; `get` reads both
    /// spellings in one lookup. (The alternative — naming a field for the wire
    /// with `#[serde(rename = "generationConfig", alias = "generation_config")]`
    /// — works too; this module prefers one lookup that accepts both.)
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl GeminiRequest {
    /// A field by either wire spelling (camelCase canonical, snake_case from
    /// the protobuf-JSON mapping the SDKs also emit).
    fn get(&self, camel: &str, snake: &str) -> Option<&Value> {
        self.extra
            .get(camel)
            .or_else(|| self.extra.get(snake))
            .filter(|v| !v.is_null())
    }

    /// Fold the Gemini body into the OpenAI-shaped IR. `model` comes from the
    /// request path; `stream` from the `:streamGenerateContent` verb. Rejects
    /// unrepresentable shapes rather than answering from a degraded history.
    pub fn to_chat_request(&self, model: &str, stream: bool) -> Result<ChatRequest, ProviderError> {
        // `contents` is REQUIRED by this dialect; an empty/absent one would
        // otherwise fold to `messages: []` and die as a confusing upstream
        // error rather than a 400 at the seam.
        if self.contents.is_empty() {
            return Err(ProviderError::bad_request(
                "contents: a request must carry at least one Content",
            ));
        }
        let mut messages = Vec::new();
        if let Some(sys) = self.get("systemInstruction", "system_instruction") {
            let text = content_text(sys)?;
            if !text.is_empty() {
                messages.push(json!({"role": "system", "content": text}));
            }
        }
        for content in &self.contents {
            fold_content(content, &mut messages)?;
        }

        let mut extra = Map::new();
        if let Some(gc) = self.get("generationConfig", "generation_config") {
            fold_generation_config(gc, &mut extra)?;
        }
        if let Some(tools) = &self.tools {
            fold_tools(tools, &mut extra)?;
        }
        if let Some(cfg) = self.get("toolConfig", "tool_config")
            && let Some(choice) = tool_choice_from_config(cfg)?
        {
            extra.insert("tool_choice".into(), choice);
        }

        Ok(ChatRequest {
            model: model.to_string(),
            messages,
            stream: Some(stream),
            extra,
        })
    }
}

/// The `Content.part` oneof arms, as `(camelCase, snake_case)`. The single
/// spelling site: the top-level gate, the nested `functionResponse.parts`
/// loop, and the reject diagnostic all read this, because an arm missing from
/// any one of them shows up as a silently dropped sibling.
const PART_ARMS: &[(&str, Option<&str>)] = &[
    ("text", None),
    ("inlineData", Some("inline_data")),
    ("fileData", Some("file_data")),
    ("functionCall", Some("function_call")),
    ("functionResponse", Some("function_response")),
    ("toolCall", Some("tool_call")),
    ("toolResponse", Some("tool_response")),
    ("executableCode", Some("executable_code")),
    ("codeExecutionResult", Some("code_execution_result")),
];

/// Which oneof arms this part names, by the exact keys the client used. Both
/// spellings of an arm are reported when both are present: they are two keys
/// on the wire, and collapsing them to one is how one of them would vanish
/// unread.
fn named_part_arms(part: &Value) -> Vec<&str> {
    let mut arms = Vec::new();
    for (camel, snake) in PART_ARMS {
        if part.get(*camel).is_some() {
            arms.push(*camel);
        }
        if let Some(snake) = snake
            && part.get(*snake).is_some()
        {
            arms.push(snake);
        }
    }
    arms
}

/// Fold one `Content` (`{role, parts[]}`) into the OpenAI messages it becomes.
/// A Gemini turn is not always one message: a `model` turn carrying function
/// calls is one `assistant` message with `tool_calls`; each `functionResponse`
/// in a `user` turn is its own `role: "tool"` message, which is what OpenAI
/// requires and the order it expects them in. A user turn's text and media
/// parts keep the client's order; media in any other turn rejects.
fn fold_content(content: &Value, out: &mut Vec<Value>) -> Result<(), ProviderError> {
    let is_model = content.get("role").and_then(Value::as_str) == Some("model");
    let empty = Vec::new();
    let wire = content
        .get("parts")
        .and_then(Value::as_array)
        .unwrap_or(&empty);

    // Text and media ride the turn in the client's order rather than joined:
    // "look at this / <image> / now answer" is a different prompt than all
    // the text first, and the chat dialect can say that.
    let mut parts: Vec<Value> = Vec::new();
    let mut has_media = false;
    let mut tool_calls: Vec<Value> = Vec::new();
    let mut tool_results: Vec<Value> = Vec::new();
    let mut call_seq = 0usize;

    for part in wire {
        // `thought: true` (reasoning text) and `thoughtSignature` (the
        // encrypted reasoning context) can ride on ANY part kind — a `text`
        // part, a `functionCall`, anything — and neither has an IR field. Check
        // before the per-kind dispatch so a signed function call cannot fold
        // while silently shedding the context it carried.
        if is_true(part.get("thought"))
            || part.get("thoughtSignature").is_some()
            || part.get("thought_signature").is_some()
        {
            return Err(reasoning_unrepresentable("thought/thoughtSignature part"));
        }
        // A part is a protobuf oneof: exactly one key names it. A part
        // carrying two arms is malformed, and dispatching on the first would
        // silently drop the rest — an image riding beside its caption text,
        // or a fileData whose refusal a text key must not make skippable —
        // so reject the whole part. Unknown keys and the server-tool parts
        // fall through to the reject arm below.
        // Same oneof discipline one level down and in the reject naming below,
        // from one table: a list repeated in three places drifts, and the
        // drift shows up as a silently dropped arm.
        let arms = named_part_arms(part);
        if arms.len() > 1 {
            return Err(ProviderError::bad_request(format!(
                "part carries {} at once: a Content part is a oneof, and folding \
                 one arm while ignoring the rest would drop content (extend this \
                 module or use the fidelity lane)",
                arms.iter()
                    .map(|arm| format!("\"{arm}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        if let Some(t) = part.get("text").and_then(Value::as_str) {
            parts.push(text_part(t));
            continue;
        }
        if let Some(call) = part
            .get("functionCall")
            .or_else(|| part.get("function_call"))
        {
            let args = call.get("args").cloned().unwrap_or_else(|| json!({}));
            let id = call
                .get("id")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| {
                    call_seq += 1;
                    format!("call_{call_seq}")
                });
            tool_calls.push(json!({
                "id": id,
                "type": "function",
                "function": {
                    "name": call.get("name").cloned().unwrap_or(json!("")),
                    // Gemini carries args as an OBJECT; OpenAI wants a JSON
                    // STRING — the one place the two wires genuinely disagree.
                    "arguments": Value::String(args.to_string()),
                },
            }));
            continue;
        }
        if let Some(resp) = part
            .get("functionResponse")
            .or_else(|| part.get("function_response"))
        {
            let body = resp.get("response").cloned().unwrap_or_else(|| json!({}));
            let id = resp
                .get("id")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_default();
            // A `functionResponse` may ALSO carry `parts` — the multimodal
            // tool return computer-use and function-signature flows actually
            // send. Text in them is part of the answer and joins the payload;
            // anything else is media a tool RETURNED, which the chat dialect
            // has no position for (a `role:"tool"` message is a string), and
            // stringifying a screenshot into tool output buries megabytes no
            // model can look at. Refuse it by name.
            let mut extra_text = Vec::new();
            for p in resp
                .get("parts")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                // Reasoning can ride a nested part exactly as it rides a
                // top-level one, and the replay credential cannot be minted
                // here — refuse before the arm dispatch can fold the text and
                // shed the signature with it.
                if is_true(p.get("thought"))
                    || p.get("thoughtSignature").is_some()
                    || p.get("thought_signature").is_some()
                {
                    return Err(reasoning_unrepresentable(
                        "functionResponse thought/thoughtSignature part",
                    ));
                }
                // The same oneof discipline as the top-level dispatch: a key
                // list, not a "has text" test. Treating a `text` key as
                // proof of a text part would let `{text, fileData}` through
                // as text — the exact silent drop this loop exists to stop,
                // one level down.
                let arms = named_part_arms(p);
                match arms.as_slice() {
                    ["text"] => match p.get("text").and_then(Value::as_str) {
                        Some(t) => extra_text.push(t),
                        // A `text` that is not a string is a wrong-shaped
                        // client; folding it to "" would answer as though the
                        // tool had returned nothing. The top-level dispatch
                        // refuses that case by falling through — so refuse it.
                        None => {
                            return Err(ProviderError::bad_request(
                                "functionResponse part \"text\" is not a string: this fold \
                                 cannot tell an empty answer from a malformed one (use the \
                                 fidelity lane)",
                            ));
                        }
                    },
                    [] => {
                        return Err(ProviderError::bad_request(
                            "functionResponse part names no content at all: an empty part \
                             beside a tool payload cannot be folded or refused on its \
                             merits (drop it client-side or use the fidelity lane)",
                        ));
                    }
                    named => {
                        let kind = named
                            .iter()
                            .find(|k| **k != "text")
                            .or(named.first())
                            .copied()
                            .unwrap_or("media");
                        return Err(ProviderError::bad_request(format!(
                            "functionResponse part \"{kind}\" (arms: {}) is not representable: \
                             a tool RETURN is a string here, so media, a nested call, and \
                             every other non-text arm have no position — return it in the \
                             next user turn's parts, or use the fidelity lane",
                            named.join(", ")
                        )));
                    }
                }
            }
            let content = match extra_text.is_empty() {
                true => body.to_string(),
                false => {
                    let mut c = body.to_string();
                    for t in extra_text {
                        c.push('\n');
                        c.push_str(t);
                    }
                    c
                }
            };
            tool_results.push(json!({
                "role": "tool",
                "tool_call_id": Value::String(if id.is_empty() {
                    resp.get("name").and_then(Value::as_str).unwrap_or("").to_string()
                } else {
                    id
                }),
                "content": Value::String(content),
            }));
            continue;
        }
        if let Some(inline) = part.get("inlineData").or_else(|| part.get("inline_data")) {
            reject_non_user_media(is_model, "inlineData")?;
            let mime = inline
                .get("mimeType")
                .or_else(|| inline.get("mime_type"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let data = inline.get("data").and_then(Value::as_str).unwrap_or("");
            // The chat IR has exactly two homes for inline bytes: an image
            // data URL and a document `file` part, which self-names only
            // from the two media types the wire documents for it
            // (`application/pdf`, `text/plain`). Audio, video, and arbitrary
            // binaries have no shape here and must not smuggle themselves
            // into one that does.
            let is_image = mime.starts_with("image/");
            if !is_image && !matches!(mime, "application/pdf" | "text/plain") {
                return Err(ProviderError::bad_request(format!(
                    "inlineData mime_type {mime:?} is not representable: the chat media \
                     parts carry inline images and application/pdf or text/plain \
                     documents only (extend this module or use the fidelity lane)"
                )));
            }
            let url = data_url(mime, data)?;
            parts.push(if is_image {
                image_part(&url, None)?
            } else {
                // Gemini inline parts carry no filename; the media type the
                // data URL composes into the payload is what names the file.
                file_part(None, &url)?
            });
            has_media = true;
            continue;
        }
        if part
            .get("fileData")
            .or_else(|| part.get("file_data"))
            .is_some()
        {
            reject_non_user_media(is_model, "fileData")?;
            return Err(ProviderError::bad_request(
                "fileData names a Google-hosted `files/…` URI: a namespace the chat \
                 upstream does not own, and the chat file object has no url field to \
                 carry a reference — inline the bytes as `inlineData` or use the \
                 fidelity lane",
            ));
        }
        // Anything else this fold cannot represent: server-tool context,
        // executable code. Reject rather than answer from a vanished turn.
        let named = named_part_arms(part);
        let key = match named.iter().copied().find(|k| *k != "text") {
            Some(k) => k,
            None => {
                return Err(match part.get("text") {
                    Some(_) => ProviderError::bad_request(
                        "part \"text\" is not a string: this fold cannot tell an empty \
                         answer from a malformed one (use the fidelity lane)",
                    ),
                    None => ProviderError::bad_request(format!(
                        "part key \"{}\" is not a known Content arm and is not \
                         representable in this text-and-function-tools bridge \
                         (extend this module or use the fidelity lane)",
                        part.as_object()
                            .and_then(|o| o.keys().next())
                            .map(String::as_str)
                            .unwrap_or("(none)")
                    )),
                });
            }
        };
        return Err(ProviderError::bad_request(format!(
            "part \"{key}\" is not representable in this text-and-function-tools \
             bridge (extend this module or use the fidelity lane)"
        )));
    }

    // The turn's own text for the emptiness gates below; `parts_content`
    // re-derives this same join when it collapses a text-only array.
    let text: String = parts
        .iter()
        .filter_map(|p| p.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    if is_model {
        if !text.is_empty() || !tool_calls.is_empty() {
            let mut m = Map::new();
            m.insert("role".into(), json!("assistant"));
            m.insert(
                "content".into(),
                if text.is_empty() {
                    Value::Null
                } else {
                    json!(text)
                },
            );
            if !tool_calls.is_empty() {
                m.insert("tool_calls".into(), Value::Array(tool_calls));
            }
            out.push(Value::Object(m));
        }
    } else {
        // `parts_content` picks the shape: a text-only turn still folds to
        // the joined string it always produced; the array appears only when
        // media rode in the turn.
        if !text.is_empty() || has_media {
            out.push(json!({"role": "user", "content": parts_content(parts)}));
        }
        // Tool results follow the user text so an upstream sees the answer
        // after the call it responds to.
        out.extend(tool_results);
    }
    Ok(())
}

/// Join the text of a `Content` (system instruction); reject non-text parts.
fn content_text(content: &Value) -> Result<String, ProviderError> {
    match content {
        Value::String(s) => Ok(s.clone()),
        Value::Object(_) => {
            let empty = Vec::new();
            let parts = content
                .get("parts")
                .and_then(Value::as_array)
                .unwrap_or(&empty);
            let mut out = String::new();
            for part in parts {
                if let Some(t) = part.get("text").and_then(Value::as_str) {
                    if !out.is_empty() {
                        out.push('\n');
                    }
                    out.push_str(t);
                } else {
                    return Err(ProviderError::bad_request(
                        "systemInstruction must be text-only in this bridge",
                    ));
                }
            }
            Ok(out)
        }
        _ => Ok(String::new()),
    }
}

/// `generationConfig` → the OpenAI sampling fields it spells. Unrepresentable
/// output modalities (non-JSON `responseMimeType`, e.g. audio) reject; `thinking`
/// rejects (no IR reasoning field).
fn fold_generation_config(gc: &Value, extra: &mut Map<String, Value>) -> Result<(), ProviderError> {
    for key in ["thinkingConfig", "thinking_config"] {
        if gc.get(key).is_some() {
            return Err(reasoning_unrepresentable(key));
        }
    }
    let map = [
        ("temperature", "temperature"),
        ("topP", "top_p"),
        ("topK", "top_k"),
        ("candidateCount", "n"),
        ("maxOutputTokens", "max_tokens"),
        ("presencePenalty", "presence_penalty"),
        ("frequencyPenalty", "frequency_penalty"),
        ("seed", "seed"),
    ];
    for (from, to) in map {
        if let Some(v) = gc
            .get(from)
            .or_else(|| gc.get(camel_to_snake(from).as_str()))
        {
            extra.insert(to.to_string(), v.clone());
        }
    }
    if let Some(stop) = gc
        .get("stopSequences")
        .or_else(|| gc.get("stop_sequences"))
        .filter(|v| !v.is_null())
    {
        extra.insert("stop".into(), stop.clone());
    }
    let mime = gc
        .get("responseMimeType")
        .or_else(|| gc.get("response_mime_type"))
        .and_then(Value::as_str);
    match mime {
        None | Some("text/plain") => {}
        Some("application/json") => {
            let schema = gc
                .get("responseSchema")
                .or_else(|| gc.get("response_schema"))
                .filter(|v| !v.is_null());
            let fmt = match schema {
                Some(s) => json!({
                    "type": "json_schema",
                    "json_schema": {"name": "response", "schema": s},
                }),
                None => json!({"type": "json_object"}),
            };
            extra.insert("response_format".into(), fmt);
        }
        Some(other) => {
            return Err(ProviderError::bad_request(format!(
                "responseMimeType \"{other}\": only text and JSON are representable \
                 in this bridge (audio/image output needs the fidelity lane)"
            )));
        }
    }
    if let Some(modalities) = gc
        .get("responseModalities")
        .or_else(|| gc.get("response_modalities"))
        .filter(|v| !v.is_null())
    {
        let Some(modalities) = modalities.as_array() else {
            return Err(ProviderError::bad_request(
                "responseModalities must be an array of modality names",
            ));
        };
        if modalities.iter().any(|m| m.as_str() != Some("TEXT")) {
            return Err(ProviderError::bad_request(
                "responseModalities with non-TEXT output is not representable in this bridge",
            ));
        }
    }
    Ok(())
}

/// `tools[]` → OpenAI function tools. Function declarations fold; built-in
/// server tools (Google Search, code execution, …) reject — a chat upstream
/// cannot run them, and dropping them would answer without the grounding the
/// client asked for.
fn fold_tools(tools: &[Value], extra: &mut Map<String, Value>) -> Result<(), ProviderError> {
    let mut folded: Vec<Value> = Vec::new();
    for tool in tools {
        let decls = tool
            .get("functionDeclarations")
            .or_else(|| tool.get("function_declarations"))
            .and_then(Value::as_array);
        match decls {
            Some(list) => {
                for d in list {
                    let mut f = Map::new();
                    f.insert("name".into(), d.get("name").cloned().unwrap_or(json!("")));
                    if let Some(desc) = d.get("description") {
                        f.insert("description".into(), desc.clone());
                    }
                    // The OpenAPI-subset schema passes through untouched.
                    if let Some(params) = d.get("parameters") {
                        f.insert("parameters".into(), params.clone());
                    }
                    folded.push(json!({"type": "function", "function": Value::Object(f)}));
                }
            }
            // A Tool object with only built-in keys (googleSearch, codeExecution,
            // urlContext, fileSearch, googleMaps, computerUse) is a server-side
            // tool this fold cannot serve.
            None => {
                let named = tool
                    .as_object()
                    .map(|o| o.keys().next().cloned().unwrap_or_default())
                    .unwrap_or_default();
                return Err(ProviderError::bad_request(format!(
                    "built-in tool {named:?} cannot be served through the IR fold \
                     (only functionDeclarations fold; use the fidelity lane for \
                     server-side tools)"
                )));
            }
        }
    }
    if !folded.is_empty() {
        extra.insert("tools".into(), Value::Array(folded));
    }
    Ok(())
}

/// `toolConfig.functionCallingConfig` → OpenAI `tool_choice`. A list of TWO
/// or more `allowedFunctionNames` is refused, not downgraded: the chat
/// dialect can name exactly one function, and bare `"required"` would widen
/// the client's restriction to every declared tool — the silent-loss failure
/// the rejecting-bridge rule exists to prevent.
fn tool_choice_from_config(cfg: &Value) -> Result<Option<Value>, ProviderError> {
    let Some(fc) = cfg
        .get("functionCallingConfig")
        .or_else(|| cfg.get("function_calling_config"))
    else {
        return Ok(None);
    };
    let mode = fc.get("mode").and_then(Value::as_str).unwrap_or("AUTO");
    let allowed = fc
        .get("allowedFunctionNames")
        .or_else(|| fc.get("allowed_function_names"))
        .and_then(Value::as_array);
    let named = match allowed {
        Some(list) if list.len() > 1 && matches!(mode, "ANY" | "VALIDATED") => {
            return Err(ProviderError::bad_request(format!(
                "functionCallingConfig.allowedFunctionNames lists {} names: chat tool_choice \
                 names at most one function, and a bare \"required\" would widen the \
                 restriction to every declared tool (use the fidelity lane to a native \
                 Gemini upstream)",
                list.len()
            )));
        }
        Some(list) => list.first(),
        None => None,
    };
    Ok(Some(match mode {
        "NONE" => json!("none"),
        "ANY" | "VALIDATED" => match named {
            Some(name) => json!({"type": "function", "function": {"name": name}}),
            None => json!("required"),
        },
        _ => json!("auto"),
    }))
}

fn is_true(v: Option<&Value>) -> bool {
    matches!(v, Some(Value::Bool(true)))
}

fn reasoning_unrepresentable(what: &str) -> ProviderError {
    ProviderError::bad_request(format!(
        "{what}: reasoning has no field on the shared IR, so it cannot survive the \
         fold (use the fidelity lane to a native Gemini upstream)"
    ))
}

/// Media has a role gate before it has a shape: the chat dialect's
/// `system`/`assistant`/`tool` content is `string | text parts`, so a media
/// part outside a user turn has no legal position at the upstream. Naming the
/// turn, not just the part, is what makes the 400 attributable.
fn reject_non_user_media(is_model: bool, kind: &str) -> Result<(), ProviderError> {
    if is_model {
        return Err(ProviderError::bad_request(format!(
            "{kind} in a model turn is not representable: the chat dialect carries \
             media parts on user turns only (extend this module or use the fidelity lane)"
        )));
    }
    Ok(())
}

/// `camelCase` → `camel_case` (only used to accept the snake_case aliases the
/// protobuf JSON mapping also produces).
fn camel_to_snake(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for c in s.chars() {
        if c.is_ascii_uppercase() {
            out.push('_');
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Gemini `finishReason`, reported from what the fold actually emitted. A
/// refusal is a successful text part with the honest `SAFETY` terminal when
/// the upstream says so; the IR payload is not reinterpreted as an error.
fn finish_reason(finish: Option<&str>, emitted_tools: bool) -> &'static str {
    match finish {
        Some("length") => "MAX_TOKENS",
        Some("content_filter") | Some("refusal") => "SAFETY",
        _ if emitted_tools => "STOP",
        _ => "STOP",
    }
}

/// Gemini's traffic details represent cached prompt and reasoning output
/// tokens. Anthropic cache counters have no separate Gemini field and are
/// omitted instead of being folded into an unrelated count.
fn usage_metadata(u: Usage) -> Value {
    let reasoning = u.reasoning_tokens.unwrap_or(0);
    // OpenAI completion_tokens includes reasoning; Gemini separates candidate
    // and thought traffic, so subtract rather than count thoughts twice.
    let candidates = u.completion_tokens.saturating_sub(reasoning);
    let mut usage = Map::from_iter([
        ("promptTokenCount".into(), u.prompt_tokens.into()),
        ("candidatesTokenCount".into(), candidates.into()),
        (
            "totalTokenCount".into(),
            u.total_tokens
                .unwrap_or(u.prompt_tokens.saturating_add(u.completion_tokens))
                .into(),
        ),
    ]);
    if let Some(cached) = u.cached_tokens {
        usage.insert("cachedContentTokenCount".into(), cached.into());
    }
    if u.reasoning_tokens.is_some() {
        usage.insert("thoughtsTokenCount".into(), reasoning.into());
    }
    Value::Object(usage)
}

/// IR tool call → a Gemini `functionCall` part. `arguments` arrives as a JSON
/// string; Gemini wants an object, so unparseable arguments become `{}` rather
/// than failing a turn that otherwise reached the client.
fn function_call_part(call: &x2api_kit::ToolCall) -> Value {
    let args: Value = serde_json::from_str(&call.arguments).unwrap_or_else(|_| json!({}));
    let mut fc = Map::new();
    fc.insert("name".into(), json!(call.name));
    fc.insert("args".into(), args);
    if !call.id.is_empty() {
        fc.insert("id".into(), json!(call.id));
    }
    json!({ "functionCall": Value::Object(fc) })
}

/// Buffered IR completion -> a `GenerateContentResponse`.
///
/// Refusal text is a normal text part; the upstream refusal/content-filter stop
/// becomes `SAFETY`. Reasoning text is deliberately NOT rendered as a `thought`
/// part because Gemini requires a replayable `thoughtSignature` this bridge
/// cannot mint; reasoning TOKEN accounting still appears in usageMetadata.
pub fn completion_to_generate_content(c: &Completion) -> Value {
    let mut parts: Vec<Value> = Vec::new();
    if !c.text.is_empty() || (c.refusal.is_none() && c.tool_calls.is_empty()) {
        parts.push(json!({"text": c.text}));
    }
    if let Some(refusal) = c.refusal.as_deref().filter(|s| !s.is_empty()) {
        parts.push(json!({"text": refusal}));
    }
    for call in &c.tool_calls {
        parts.push(function_call_part(call));
    }
    let mut candidate = Map::new();
    candidate.insert("content".into(), json!({"role": "model", "parts": parts}));
    candidate.insert(
        "finishReason".into(),
        json!(finish_reason(
            c.finish_reason.as_deref(),
            !c.tool_calls.is_empty()
        )),
    );
    candidate.insert("index".into(), 0.into());
    let mut resp = Map::new();
    resp.insert(
        "candidates".into(),
        Value::Array(vec![Value::Object(candidate)]),
    );
    if let Some(u) = c.usage {
        resp.insert("usageMetadata".into(), usage_metadata(u));
    }
    if !c.model.is_empty() {
        resp.insert("modelVersion".into(), json!(c.model));
    }
    Value::Object(resp)
}

/// Stateful translation of an IR chunk stream into the Gemini SSE sequence.
/// Gemini frames are anonymous `data:` records (like the chat dialect) and the
/// stream carries no sentinel: the terminal `finishReason` frame ends it, so
/// `finish` emits exactly that frame and `fail` emits a Google error object
/// WITHOUT a `finishReason` — a truncated answer must not read as complete.
#[derive(Debug)]
pub struct GeminiStream {
    model: String,
    finished: bool,
    usage: Option<Usage>,
    /// IR `finish_reason` last seen on a chunk, mapped to a Gemini enum at end.
    finish_reason: Option<String>,
    /// Accumulated function-call fragments, keyed by upstream `index`, so the
    /// complete `functionCall` can be written on the terminal frame (Gemini
    /// does not stream argument deltas).
    calls: Vec<(u32, String, String, String)>,
    emitted_content: bool,
}

impl GeminiStream {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            finished: false,
            usage: None,
            finish_reason: None,
            calls: Vec::new(),
            emitted_content: false,
        }
    }

    /// One chunk → a partial `data:` frame carrying its text, or nothing. Usage
    /// and the finish reason are held for the terminal frame.
    pub fn on_chunk(&mut self, chunk: &ChatChunk, dst: &mut BytesMut) {
        if self.finished {
            return;
        }
        if chunk.usage.is_some() {
            self.usage = chunk.usage;
        }
        if self.model.is_empty() && !chunk.model.is_empty() {
            self.model.clone_from(&chunk.model);
        }
        if let Some(r) = &chunk.finish_reason {
            self.finish_reason = Some(r.clone());
        }
        for call in &chunk.tool_calls {
            self.note_tool_delta(call);
        }
        let mut parts = Vec::new();
        if !chunk.text.is_empty() {
            parts.push(json!({"text": chunk.text}));
        }
        if !chunk.refusal.is_empty() {
            parts.push(json!({"text": chunk.refusal}));
        }
        if !parts.is_empty() {
            self.emitted_content = true;
            write_frame(
                dst,
                &json!({
                    "candidates": [{
                        "content": {"role": "model", "parts": parts},
                        "index": 0,
                    }],
                }),
            );
        }
    }

    fn note_tool_delta(&mut self, call: &x2api_kit::ToolCallDelta) {
        match self.calls.iter_mut().find(|(i, ..)| *i == call.index) {
            Some(entry) => {
                let entry: &mut (u32, String, String, String) = entry;
                if let Some(name) = &call.name
                    && entry.1.is_empty()
                {
                    entry.1 = name.clone();
                }
                if let Some(id) = &call.id
                    && entry.2.is_empty()
                {
                    entry.2 = id.clone();
                }
                entry.3.push_str(&call.arguments);
            }
            None => self.calls.push((
                call.index,
                call.name.clone().unwrap_or_default(),
                call.id.clone().unwrap_or_default(),
                call.arguments.clone(),
            )),
        }
    }

    /// Clean end: the terminal frame that carries `finishReason` (and any
    /// accumulated `functionCall` parts) — the only terminator this dialect has.
    pub fn finish(&mut self, dst: &mut BytesMut) {
        if self.finished {
            return;
        }
        let emitted_tools = !self.calls.is_empty();
        let mut parts: Vec<Value> = self
            .calls
            .iter()
            .map(|(_, name, id, args)| {
                let parsed: Value = serde_json::from_str(args).unwrap_or_else(|_| json!({}));
                let mut fc = Map::new();
                fc.insert("name".into(), json!(name));
                fc.insert("args".into(), parsed);
                if !id.is_empty() {
                    fc.insert("id".into(), json!(id));
                }
                json!({"functionCall": Value::Object(fc)})
            })
            .collect();
        if parts.is_empty() && !self.emitted_content {
            parts.push(json!({"text": ""}));
        }
        let mut candidate = Map::new();
        candidate.insert("content".into(), json!({"role": "model", "parts": parts}));
        candidate.insert(
            "finishReason".into(),
            json!(finish_reason(self.finish_reason.as_deref(), emitted_tools)),
        );
        candidate.insert("index".into(), 0.into());

        let mut resp = Map::new();
        resp.insert(
            "candidates".into(),
            Value::Array(vec![Value::Object(candidate)]),
        );
        if let Some(u) = self.usage {
            resp.insert("usageMetadata".into(), usage_metadata(u));
        }
        if !self.model.is_empty() {
            resp.insert("modelVersion".into(), json!(self.model));
        }
        self.finished = true;
        write_frame(dst, &resp);
    }

    /// Terminal failure: a Google-shaped error object and NO `finishReason`, so
    /// the client cannot mistake a cut stream for a finished turn.
    pub fn fail(&mut self, err: &ProviderError, dst: &mut BytesMut) {
        if self.finished {
            return;
        }
        let frame = json!({
            "error": {
                "code": err.status,
                "message": err.shown_message(),
                "status": google_status(err.status),
            },
        });
        self.finished = true;
        write_frame(dst, &frame);
    }
}

/// The Google `google.rpc.Status` code this proxy can emit for an HTTP status —
/// the same table the buffered error envelope renders from, shared so the
/// stream and the unary answer never disagree.
pub fn google_status(status: u16) -> &'static str {
    match status {
        // 413 is this proxy's request-size limit — a client error, not quota.
        400 | 413 | 422 => "INVALID_ARGUMENT",
        404 => "NOT_FOUND",
        401 => "UNAUTHENTICATED",
        403 => "PERMISSION_DENIED",
        409 => "ABORTED",
        429 => "RESOURCE_EXHAUSTED",
        499 => "CANCELLED",
        // The canonical HTTP↔gRPC transcoding maps 405 to `UNIMPLEMENTED`
        // alongside 501, and the meaning is the same on the wire here: this
        // surface exists, the METHOD on it does not. Without this arm a wrong
        // verb — which `method_not_allowed` answers for ANY route — reported
        // itself as `INTERNAL`, a server fault the proxy never had.
        405 | 501 => "UNIMPLEMENTED",
        503 => "UNAVAILABLE",
        408 | 504 => "DEADLINE_EXCEEDED",
        _ => "INTERNAL",
    }
}

/// Read one parameter out of the raw query string.
///
/// Parsed here, in the dialect module, because `alt` and `key` are this
/// dialect's grammar, not the transport's. Two properties a typed extractor
/// would not give: the FIRST mention answers (a `HashMap` lets a later
/// duplicate win, which would let a query suffix steer a credential lookup),
/// and a value this fold cannot read is HANDLED rather than refused — a bad
/// percent-escape passes through verbatim, so a credential that does not match
/// 401s in-dialect instead of becoming a proxy-side error the client did not
/// cause. It also allocates nothing unless a `%` actually appears, where a
/// map built per request would allocate on every Gemini call.
///
/// (A note for whoever narrows this to a typed `Query<T>` later: `Query<
/// HashMap<String, String>>` does NOT reject malformed input — it decodes
/// `%FF%FE` lossily and reads a bare `?alt` as empty, verified — so the
/// envelope this dialect guarantees on every other path, the 404 and 405
/// fallbacks included, survives. A struct with required or unknown-field
/// constraints would change that, and its rejection is plain text built before
/// the handler runs. Keep the tolerance if the shape stays optional.)
pub fn query_value<'a>(query: Option<&'a str>, want: &str) -> Option<Cow<'a, str>> {
    query?.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == want).then(|| match v.rfind('%') {
            Some(_) => Cow::Owned(percent_decode(v)),
            None => Cow::Borrowed(v),
        })
    })
}

/// Decode `%XX` escapes, leaving anything that is not a well-formed escape as
/// written. Deliberately tolerant: this value is either a framing word or a
/// secret, and inventing a 400 for a mangled one would replace a clean
/// in-dialect 401 with a proxy-side error the client did not cause.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Ok(hi), Ok(lo)) = (decode_hex(bytes[i + 1]), decode_hex(bytes[i + 2]))
        {
            out.push(hi << 4 | lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    // The input came from a URI, so it was valid UTF-8; a decoded byte
    // sequence need not be, and a lossy read keeps a credential comparable
    // rather than turning a mangled escape into a panic, or a 500.
    String::from_utf8_lossy(&out).into_owned()
}

fn decode_hex(b: u8) -> Result<u8, ()> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(()),
    }
}

/// `Provider::models()` answer (an OpenAI-shape list) → this dialect's
/// `ListModelsResponse`, or one `Model` resource when `wanted` names one.
///
/// Rendered here rather than in the server because it is this dialect's
/// grammar: the resource is `models/<id>`, and the methods a client may call
/// on it are this module's own verbs. Nothing is invented — no
/// `inputTokenLimit`, no `temperature` range, no `version` the upstream never
/// said — because a discovery answer a client trusts is worse than a short
/// one, and the whole surface this proxy can claim is what the vendor's list
/// carried.
///
/// `wanted` is matched against both spellings a client uses (bare id and
/// `models/<id>`); a miss is a 404, not an empty page, because `getModel` has
/// no "not found but here is nothing" answer in this dialect. List requests
/// honour Gemini's opaque-token contract with a decimal catalogue offset and
/// return the next offset only when another model follows; the final page
/// omits `nextPageToken` entirely rather than sending an empty one.
///
/// The catalogue contract, decided here rather than guessed at the route: a
/// catalogue ROOT that is not an object, a `data` that is present but not an
/// array, an entry without a string `id`, or a paging value that is not a
/// usable offset or size each answer 400 by name — a discovery surface that
/// silently renders a short list is worse than one that says it cannot read
/// the answer. An object root whose `data` is absent is the one shape that
/// legitimately means "this proxy serves no models", so it lists empty. A
/// `pageToken` past the end is the same 400 rather than a silent short page.
pub fn models_page(
    list: &Value,
    wanted: Option<&str>,
    query: Option<&str>,
) -> Result<Value, ProviderError> {
    if !list.is_object() {
        return Err(ProviderError::bad_request(
            "models catalogue must be an object",
        ));
    }
    let data: &[Value] = match list.get("data") {
        Some(value) => value.as_array().ok_or_else(|| {
            ProviderError::bad_request("models catalogue \"data\" must be an array")
        })?,
        None => &[],
    };
    let ids = || {
        data.iter()
            .map(|model| {
                model.get("id").and_then(Value::as_str).ok_or_else(|| {
                    ProviderError::bad_request(
                        "models catalogue entries must be objects with a string \"id\"",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()
    };
    let resource = |id: &str| {
        json!({
            "name": format!("models/{id}"),
            "displayName": id,
            "apiVersion": "v1beta",
            "supportedGenerationMethods": ["generateContent", "streamGenerateContent"],
        })
    };
    match wanted {
        None => {
            let offset = match query_value(query, "pageToken") {
                Some(token) => token.parse::<usize>().map_err(|_| {
                    ProviderError::bad_request("pageToken must be a decimal catalogue offset")
                })?,
                None => 0,
            };
            let page_size = match query_value(query, "pageSize") {
                Some(size) => {
                    let size = size.parse::<usize>().map_err(|_| {
                        ProviderError::bad_request("pageSize must be a positive integer")
                    })?;
                    if size == 0 {
                        return Err(ProviderError::bad_request(
                            "pageSize must be a positive integer",
                        ));
                    }
                    size
                }
                None => data.len(),
            };
            let mut page = json!({
                "models": ids()?
                    .into_iter()
                    .skip(offset)
                    .take(page_size)
                    .map(&resource)
                    .collect::<Vec<_>>(),
            });
            if offset > data.len() {
                return Err(ProviderError::bad_request(
                    "pageToken is outside this proxy's catalogue",
                ));
            }
            let next = offset.saturating_add(page_size);
            if next < data.len() {
                page["nextPageToken"] = json!(next.to_string());
            }
            Ok(page)
        }
        Some(name) => {
            let bare = name.strip_prefix("models/").unwrap_or(name);
            ids()?
                .into_iter()
                .find(|id| *id == bare)
                .map(&resource)
                .ok_or_else(|| {
                    ProviderError::not_found(format!(
                        "model \"{name}\" is not in this proxy's catalogue"
                    ))
                })
        }
    }
}

/// Serialize + frame one Gemini event as an anonymous `data:` record.
fn write_frame<T: serde::Serialize + ?Sized>(dst: &mut BytesMut, value: &T) {
    match serde_json::to_vec(value) {
        Ok(payload) => sse::append_data_frame(dst, &payload),
        Err(_) => sse::append_data_frame(dst, b"{}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fold(v: Value) -> Result<ChatRequest, ProviderError> {
        let req: GeminiRequest = serde_json::from_value(v).unwrap();
        req.to_chat_request("gemini-test", false)
    }

    #[test]
    fn models_page_get_miss_is_not_an_empty_list() {
        let list = json!({"data": [{"id": "gemini-a"}]});
        let e = models_page(&list, Some("models/missing"), None).unwrap_err();
        assert_eq!(e.status, 404);
        assert!(e.message.contains("models/missing"), "{}", e.message);
    }

    #[test]
    fn models_page_rejects_non_array_catalogue() {
        let e = models_page(&json!({"data": {"id": "gemini-a"}}), None, None).unwrap_err();
        assert_eq!(e.status, 400);
        assert!(e.message.contains("must be an array"), "{}", e.message);
    }

    #[test]
    fn models_page_paginates_with_opaque_offsets() {
        let list = json!({"data": [
            {"id": "gemini-a"}, {"id": "gemini-b"}, {"id": "gemini-c"}
        ]});
        let first = models_page(&list, None, Some("pageSize=2")).unwrap();
        assert_eq!(first["models"].as_array().unwrap().len(), 2);
        assert_eq!(first["nextPageToken"], "2");
        let second = models_page(&list, None, Some("pageSize=2&pageToken=2")).unwrap();
        assert_eq!(second["models"][0]["name"], "models/gemini-c");
        assert!(second.get("nextPageToken").is_none());
    }

    #[test]
    fn models_page_rejects_invalid_and_out_of_range_paging() {
        let list = json!({"data": [{"id": "gemini-a"}]});
        for query in [
            "pageSize=wat",
            "pageSize=0",
            "pageSize=-1",
            "pageToken=wat",
            "pageToken=2",
            "pageToken=18446744073709551615",
        ] {
            let e = models_page(&list, None, Some(query)).unwrap_err();
            assert_eq!(e.status, 400, "{query}");
            assert!(
                e.message.contains("pageSize") || e.message.contains("pageToken"),
                "{query}: {}",
                e.message
            );
        }
    }

    #[test]
    fn models_page_distinguishes_empty_and_malformed_catalogues() {
        assert_eq!(
            models_page(&json!({}), None, None).unwrap(),
            json!({"models": []})
        );
        for list in [
            json!({"data": ["gemini-a"]}),
            json!({"data": [{"name": "gemini-a"}]}),
        ] {
            let e = models_page(&list, None, None).unwrap_err();
            assert_eq!(e.status, 400);
            assert!(e.message.contains("string \"id\""), "{}", e.message);
        }
    }

    #[test]
    fn models_page_rejects_non_object_catalogue_roots() {
        for root in [json!([]), json!(null), json!(42), json!("gemini-a")] {
            let e = models_page(&root, None, None).unwrap_err();
            assert_eq!(e.status, 400);
            assert!(e.message.contains("must be an object"), "{}", e.message);
        }
    }

    #[test]
    fn folds_text_and_roles() {
        let c = fold(json!({
            "systemInstruction": {"parts": [{"text": "be terse"}]},
            "contents": [
                {"role": "user", "parts": [{"text": "hi"}]},
                {"role": "model", "parts": [{"text": "hello"}]},
                {"parts": [{"text": "again"}]},
            ],
        }))
        .unwrap();
        assert_eq!(c.model, "gemini-test");
        assert_eq!(c.messages.len(), 4);
        assert_eq!(c.messages[0]["role"], "system");
        assert_eq!(c.messages[0]["content"], "be terse");
        assert_eq!(c.messages[1], json!({"role": "user", "content": "hi"}));
        assert_eq!(c.messages[2]["role"], "assistant");
        // A missing role defaults to user, matching the API.
        assert_eq!(c.messages[3]["role"], "user");
    }

    #[test]
    fn folds_function_call_to_openai_tool_calls() {
        let c = fold(json!({
            "contents": [{"role": "model", "parts": [{
                "functionCall": {"name": "get_weather", "args": {"city": "SF"}, "id": "call_9"}
            }]}],
        }))
        .unwrap();
        assert_eq!(c.messages[0]["role"], "assistant");
        assert!(c.messages[0]["content"].is_null());
        let call = &c.messages[0]["tool_calls"][0];
        assert_eq!(call["id"], "call_9");
        assert_eq!(call["function"]["name"], "get_weather");
        // args object -> JSON STRING, the one place the wires disagree.
        assert_eq!(
            call["function"]["arguments"],
            json!({"city": "SF"}).to_string()
        );
    }

    #[test]
    fn folds_function_response_to_tool_message() {
        let c = fold(json!({
            "contents": [{"role": "user", "parts": [{
                "functionResponse": {"name": "get_weather", "id": "call_9", "response": {"temp": 15}}
            }]}],
        }))
        .unwrap();
        assert_eq!(
            c.messages[0],
            json!({"role": "tool", "tool_call_id": "call_9", "content": json!({"temp": 15}).to_string()})
        );
    }

    #[test]
    fn maps_generation_config_to_openai_sampling() {
        let c = fold(json!({
            "contents": [{"parts": [{"text": "x"}]}],
            "generationConfig": {
                "temperature": 0.5, "topP": 0.9, "topK": 40, "maxOutputTokens": 128,
                "candidateCount": 2, "stopSequences": ["a", "b"],
            },
        }))
        .unwrap();
        assert_eq!(c.extra["temperature"], 0.5);
        assert_eq!(c.extra["top_p"], 0.9);
        assert_eq!(c.extra["top_k"], 40);
        assert_eq!(c.extra["max_tokens"], 128);
        assert_eq!(c.extra["n"], 2);
        assert_eq!(c.extra["stop"], json!(["a", "b"]));
    }

    #[test]
    fn structured_output_becomes_response_format() {
        let c = fold(json!({
            "contents": [{"parts": [{"text": "x"}]}],
            "generationConfig": {"responseMimeType": "application/json",
                                 "responseSchema": {"type": "object"}},
        }))
        .unwrap();
        assert_eq!(c.extra["response_format"]["type"], "json_schema");
        assert_eq!(
            c.extra["response_format"]["json_schema"]["schema"]["type"],
            "object"
        );
    }

    #[test]
    fn tool_choice_from_function_calling_config() {
        let c = fold(json!({
            "contents": [{"parts": [{"text": "x"}]}],
            "tools": [{"functionDeclarations": [
                {"name": "f", "parameters": {"type": "object", "properties": {}}}
            ]}],
            "toolConfig": {"functionCallingConfig": {"mode": "ANY", "allowedFunctionNames": ["f"]}},
        }))
        .unwrap();
        assert_eq!(c.extra["tools"][0]["function"]["name"], "f");
        assert_eq!(
            c.extra["tool_choice"],
            json!({"type": "function", "function": {"name": "f"}})
        );
    }

    #[test]
    fn multi_name_allowed_function_names_refuse_instead_of_widening() {
        // A bare "required" would answer "any of the declared tools" where the
        // client asked for a restriction to two named ones — widening, not
        // folding, so the bridge refuses.
        for mode in ["ANY", "VALIDATED"] {
            let err = fold(json!({
                "contents": [{"parts": [{"text": "x"}]}],
                "tools": [{"functionDeclarations": [
                    {"name": "f"}, {"name": "g"}, {"name": "h"}
                ]}],
                "toolConfig": {"functionCallingConfig": {
                    "mode": mode, "allowedFunctionNames": ["f", "g"]
                }},
            }))
            .expect_err(mode);
            let msg = err.to_string();
            assert!(msg.contains("allowedFunctionNames lists 2"), "{msg}");
        }
        // One name still folds to the named form; NONE ignores the list.
        let one = fold(json!({
            "contents": [{"parts": [{"text": "x"}]}],
            "toolConfig": {"functionCallingConfig": {
                "mode": "ANY", "allowedFunctionNames": ["f"]
            }},
        }))
        .unwrap();
        assert_eq!(
            one.extra["tool_choice"],
            json!({"type": "function", "function": {"name": "f"}})
        );
        let none = fold(json!({
            "contents": [{"parts": [{"text": "x"}]}],
            "toolConfig": {"functionCallingConfig": {
                "mode": "NONE", "allowedFunctionNames": ["f", "g"]
            }},
        }))
        .unwrap();
        assert_eq!(none.extra["tool_choice"], json!("none"));
    }

    #[test]
    fn rejects_media_builtin_tool_and_reasoning() {
        for (body, needle) in [
            (
                json!({"contents": [{"parts": [{
                    "inlineData": {"mimeType": "application/octet-stream", "data": "AAAA"}
                }]}]}),
                "not representable",
            ),
            (
                json!({"contents": [{"parts": [{"text": "x"}]}], "tools": [{"googleSearch": {}}]}),
                "built-in tool",
            ),
            (
                json!({"contents": [{"parts": [{"text": "x", "thought": true}]}]}),
                "thought",
            ),
            (
                json!({"contents": [{"parts": [{"text": "x"}]}], "generationConfig": {"thinkingConfig": {"includeThoughts": true}}}),
                "thinkingConfig",
            ),
            // A signed function call: the signature rides a non-text part, so
            // this is exactly the case the pre-dispatch hoist defends.
            (
                json!({"contents": [{"parts": [{"text": "x"}]}], "generationConfig": {"responseModalities": ["AUDIO"]}}),
                "responseModalities",
            ),
            (
                json!({"contents": [{"parts": [{"text": "x"}]}], "generationConfig": {"response_modalities": ["AUDIO"]}}),
                "responseModalities",
            ),
            (
                json!({"contents": [{"parts": [{"text": "x"}]}], "generationConfig": {"response_modalities": "AUDIO"}}),
                "must be an array",
            ),
            (
                json!({"contents": [{"role": "model", "parts": [{
                "functionCall": {"name": "f", "args": {}}, "thoughtSignature": "eyJ…"}]}]}),
                "thoughtSignature",
            ),
        ] {
            let e = fold(body).unwrap_err();
            assert_eq!(e.status, 400, "unrepresentable shape must 400, not degrade");
            assert!(
                e.message.contains(needle),
                "expected {needle:?} in: {}",
                e.message
            );
        }
    }

    /// ProtoJSON lets a client send an explicit `null` for an unset optional
    /// field, and every sibling config read treats it as absent; the non-TEXT
    /// guard must not be the one lookup that refuses it.
    #[test]
    fn null_response_modalities_fold_as_unset() {
        let c = fold(json!({
            "contents": [{"parts": [{"text": "x"}]}],
            "generationConfig": {"responseModalities": null, "response_modalities": null},
        }))
        .unwrap();
        assert_eq!(c.messages[0]["content"], "x");
    }

    #[test]
    fn nested_and_top_level_diagnostics_name_the_fault() {
        // A nested part whose only arm was missing from the old nested list
        // must name it, not claim the part named nothing.
        let e = fold(json!({
            "contents": [{"role": "user", "parts": [{
                "functionResponse": {"name": "f", "response": {}, "parts": [
                    {"toolCall": {"name": "g", "args": {}}}
                ]}
            }]}]
        }))
        .unwrap_err();
        assert_eq!(e.status, 400);
        assert!(e.message.contains("toolCall"), "{}", e.message);

        // A non-string `text` is a wrong-shaped answer, not an unsupported arm.
        let e = fold(json!({"contents": [{"parts": [{"text": 42}]}]})).unwrap_err();
        assert_eq!(e.status, 400);
        assert!(e.message.contains("not a string"), "{}", e.message);

        // Reasoning inside a tool return has the same unrepresentable answer
        // it has at the top level.
        let e = fold(json!({
            "contents": [{"role": "user", "parts": [{
                "functionResponse": {"name": "f", "response": {}, "parts": [
                    {"text": "seen", "thought": true}
                ]}
            }]}]
        }))
        .unwrap_err();
        assert_eq!(e.status, 400);
        assert!(e.message.contains("thought"), "{}", e.message);
    }

    /// The two properties this parser exists to provide, each checked against
    /// what `Query<HashMap<String, String>>` actually does (verified, not
    /// assumed: it accepts every one of these queries, decodes `%FF%FE` lossily,
    /// reads a bare `?alt` as empty, and lets a duplicate key WIN).
    #[test]
    fn query_value_is_first_mention_wins_and_never_rejects() {
        // First mention: a later `key=` must not steer a credential lookup.
        assert_eq!(
            query_value(Some("key=a&key=b"), "key").as_deref(),
            Some("a")
        );
        // A value this fold cannot decode is handed through, not refused: a
        // wrong credential then 401s in-dialect instead of becoming a
        // proxy-side parse error the client did not cause.
        assert_eq!(
            query_value(Some("key=ab%zz"), "key").as_deref(),
            Some("ab%zz")
        );
        assert_eq!(
            query_value(Some("key=%FF%FE"), "key").as_deref(),
            Some("\u{fffd}\u{fffd}")
        );
        assert_eq!(query_value(Some("alt"), "alt"), None);
        assert_eq!(query_value(Some("alt=&key=x"), "alt").as_deref(), Some(""));
        // Well-formed escapes DO decode, and `+` is not a space here.
        assert_eq!(
            query_value(Some("key=a%42b"), "key").as_deref(),
            Some("aBb")
        );
        assert_eq!(
            query_value(Some("key=a%2Bb"), "key").as_deref(),
            Some("a+b")
        );
        assert_eq!(
            query_value(Some("alt=sse&key=k"), "alt").as_deref(),
            Some("sse")
        );
        assert_eq!(query_value(None, "alt"), None);
        assert_eq!(query_value(Some("other=1"), "alt"), None);
    }

    /// The status word is the field a `google-genai` client switches on, so no
    /// client error may reach it as `INTERNAL` — that word tells the caller the
    /// proxy broke, and it is how a wrong HTTP verb used to read.
    #[test]
    fn every_client_status_maps_to_a_client_status_word() {
        for status in [
            400, 401, 403, 404, 405, 408, 413, 422, 429, 499, 501, 503, 504,
        ] {
            assert_ne!(
                google_status(status),
                "INTERNAL",
                "{status} is not a proxy-side fault"
            );
        }
        // The fall-through is still the fault bucket, and only the fault bucket.
        for status in [500, 502, 599] {
            assert_eq!(google_status(status), "INTERNAL", "{status}");
        }
        assert_eq!(google_status(405), "UNIMPLEMENTED");
    }

    #[test]
    fn folds_user_turn_inline_image() {
        let c = fold(json!({
            "contents": [{"role": "user", "parts": [
                {"inlineData": {"mimeType": "image/png", "data": "aGVsbG8="}}
            ]}],
        }))
        .unwrap();
        assert_eq!(c.messages.len(), 1);
        assert_eq!(c.messages[0]["role"], "user");
        let parts = c.messages[0]["content"].as_array().unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0]["type"], "image_url");
        // Raw base64 on the wire becomes the IR's composed data URL.
        assert_eq!(
            parts[0]["image_url"]["url"],
            "data:image/png;base64,aGVsbG8="
        );
    }

    #[test]
    fn user_turn_part_order_survives_the_fold() {
        let c = fold(json!({
            "contents": [{"role": "user", "parts": [
                {"text": "look at this"},
                {"inlineData": {"mimeType": "image/png", "data": "aGVsbG8="}},
                {"text": "now answer"},
            ]}],
        }))
        .unwrap();
        let types: Vec<&str> = c.messages[0]["content"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["type"].as_str().unwrap())
            .collect();
        assert_eq!(types, ["text", "image_url", "text"]);
        // A text-only turn still collapses to the plain string it always was.
        let plain = fold(json!({
            "contents": [{"role": "user", "parts": [{"text": "a"}, {"text": "b"}]}],
        }))
        .unwrap();
        assert_eq!(plain.messages[0]["content"], "a\nb");
    }

    #[test]
    fn folds_inline_document_with_derived_name() {
        // The snake_case spellings the protobuf-JSON mapping emits must work.
        let c = fold(json!({
            "contents": [{"role": "user", "parts": [
                {"inline_data": {"mime_type": "application/pdf", "data": "JVBERi0x"}}
            ]}],
        }))
        .unwrap();
        let parts = c.messages[0]["content"].as_array().unwrap();

        assert_eq!(parts[0]["type"], "file");
        // Inline parts carry no filename; the media type inside the composed
        // data URL is what lets the file part name itself.
        assert_eq!(parts[0]["file"]["filename"], "document.pdf");
        assert_eq!(
            parts[0]["file"]["file_data"],
            "data:application/pdf;base64,JVBERi0x"
        );
    }

    #[test]
    fn file_data_uri_refuses_with_both_reasons() {
        let e = fold(json!({
            "contents": [{"role": "user", "parts": [
                {"fileData": {"fileUri": "files/abc123", "mimeType": "image/png"}}
            ]}],
        }))
        .unwrap_err();
        assert_eq!(e.status, 400);
        assert!(e.message.contains("namespace"), "{}", e.message);
        assert!(e.message.contains("url field"), "{}", e.message);
        assert!(e.message.contains("fidelity lane"), "{}", e.message);
    }

    #[test]
    fn audio_inline_data_refuses() {
        let e = fold(json!({
            "contents": [{"role": "user", "parts": [
                {"inlineData": {"mimeType": "audio/wav", "data": "UklGRzg="}}
            ]}],
        }))
        .unwrap_err();
        assert_eq!(e.status, 400);
        assert!(e.message.contains("not representable"), "{}", e.message);
    }

    #[test]
    fn media_in_a_model_turn_refuses() {
        let e = fold(json!({
            "contents": [{"role": "model", "parts": [
                {"text": "here is the chart"},
                {"inlineData": {"mimeType": "image/png", "data": "aGVsbG8="}},
            ]}],
        }))
        .unwrap_err();
        assert_eq!(e.status, 400);
        assert!(
            e.message.contains("in a model turn") && e.message.contains("user turns only"),
            "{}",
            e.message
        );
    }

    #[test]
    fn snake_case_multi_arm_and_nested_parts_refuse_instead_of_dropping() {
        for part in [
            json!({"text": "x", "executable_code": {"language": "PYTHON", "code": "print(1)"}}),
            json!({"text": "x", "code_execution_result": {"outcome": "OUTCOME_OK"}}),
            json!({"text": "x", "function_call": {"name": "f", "args": {}}}),
        ] {
            let e = fold(json!({"contents": [{"role": "user", "parts": [part]}]})).unwrap_err();
            assert_eq!(e.status, 400);
            assert!(e.message.contains("oneof"), "{}", e.message);
        }

        // Every arm the top-level gate knows, not just the ones the nested
        // list used to spell: a nested `toolCall` beside `text` is the same
        // silent drop one level down.
        for key in [
            "inlineData",
            "inline_data",
            "fileData",
            "file_data",
            "functionCall",
            "function_call",
            "functionResponse",
            "function_response",
            "toolCall",
            "tool_call",
            "toolResponse",
            "tool_response",
            "executableCode",
            "executable_code",
            "codeExecutionResult",
            "code_execution_result",
        ] {
            let value = if key.starts_with("function") || key.starts_with("tool") {
                json!({"name": "f", "args": {}})
            } else {
                json!({})
            };
            let e = fold(json!({
                "contents": [{"role": "user", "parts": [{
                    "functionResponse": {"name": "f", "response": {}, "parts": [
                        {"text": "seen", key: value}
                    ]}
                }]}]
            }))
            .unwrap_err();
            assert_eq!(e.status, 400);
            assert!(e.message.contains(key), "{}", e.message);
        }
    }

    #[test]
    fn multi_arm_part_refuses_instead_of_dropping_the_rest() {
        // Dispatch order sees `text` first; without the oneof gate each of
        // these would fold the text and vanish the sibling arm — exactly the
        // degrade rule 1 forbids, including past a fileData refusal.
        for part in [
            json!({"text": "look", "inlineData": {"mimeType": "image/png", "data": "aGVsbG8="}}),
            json!({"text": "x", "fileData": {"fileUri": "files/abc"}}),
            json!({"text": "saw it", "functionCall": {"name": "f", "args": {}}}),
            // Three arms must refuse exactly as two do: a count-of-two test
            // would let a text part fold past two silent siblings.
            json!({"text": "x", "fileData": {"fileUri": "files/abc"},
                   "toolCall": {"name": "f", "args": {}}}),
            json!({"text": "x", "function_call": {"name": "f", "args": {}},
                   "tool_call": {"name": "g", "args": {}},
                   "executable_code": {"language": "PYTHON"}}),
        ] {
            let e = fold(json!({"contents": [{"role": "user", "parts": [part]}]})).unwrap_err();
            assert_eq!(e.status, 400);
            assert!(e.message.contains("oneof"), "{}", e.message);
        }
    }

    /// Both spellings of one arm are two keys on the wire. Folding the camel
    /// one and leaving the snake one unread is the same silent drop the gate
    /// exists to stop, so the pair collides at both nesting levels.
    #[test]
    fn two_spellings_of_one_arm_refuse_at_both_levels() {
        for part in [
            json!({
                "inlineData": {"mimeType": "image/png", "data": "aGVsbG8="},
                "inline_data": {"mime_type": "image/png", "data": "aGVsbG8="},
            }),
            json!({"text": "x", "file_data": {"fileUri": "files/abc"}}),
            json!({"text": "x", "fileData": {"fileUri": "files/abc"},
                   "inlineData": {"mimeType": "image/png", "data": "aGVsbG8="},
                   "toolCall": {"name": "f", "args": {}}}),
        ] {
            let e = fold(json!({"contents": [{"role": "user", "parts": [part]}]})).unwrap_err();
            assert_eq!(e.status, 400);
            assert!(e.message.contains("oneof"), "{}", e.message);
        }

        let e = fold(json!({
            "contents": [{"role": "user", "parts": [{
                "functionResponse": {"name": "f", "response": {}, "parts": [
                    {"text": "seen", "toolCall": {"name": "g", "args": {}}, "tool_call": {"name": "g", "args": {}}}
                ]}
            }]}]
        }))
        .unwrap_err();
        assert_eq!(e.status, 400);
        assert!(
            e.message.contains("toolCall") && e.message.contains("tool_call"),
            "both spellings must be named: {}",
            e.message
        );
    }

    /// The `functionResponse.parts` a computer-use or signed-function flow
    /// really sends: the payload under `response` was never the only channel,
    /// and reading only it dropped a returned screenshot in silence — the one
    /// failure mode rule 1 exists to prevent, arriving through a field the
    /// fold already trusted.
    /// The query is this dialect's grammar (`alt` picks the framing, `key` is
    /// a credential), so its parser is tested where it lives. The cases are the
    /// ones an extractor would have rejected outright.
    #[test]
    fn rejects_unrepresentable_snake_case_parts_by_name() {
        for (key, value) in [
            ("executable_code", json!({"language": "PYTHON"})),
            ("code_execution_result", json!({"outcome": "OUTCOME_OK"})),
        ] {
            let part = json!({key: value});
            let e = fold(json!({"contents": [{"parts": [part]}]})).unwrap_err();
            assert_eq!(e.status, 400);
            assert!(e.message.contains(key), "{}", e.message);
        }
    }

    #[test]
    fn query_value_reads_the_two_parameters_this_dialect_uses() {
        let q = Some("alt=sse&key=abc123");
        assert_eq!(query_value(q, "alt").as_deref(), Some("sse"));
        assert_eq!(query_value(q, "key").as_deref(), Some("abc123"));
        assert_eq!(query_value(q, "pageToken"), None);
        // A valueless pair and an empty value are read, not rejected.
        assert_eq!(query_value(Some("alt"), "alt"), None);
        assert_eq!(query_value(Some("alt=&key=x"), "alt").as_deref(), Some(""));
        // Only the FIRST mention answers: a duplicate is the client's bug, and
        // picking a later one would let a suffix steer a credential lookup.
        assert_eq!(
            query_value(Some("key=a&key=b"), "key").as_deref(),
            Some("a")
        );
        // A mangled escape survives verbatim rather than becoming an error —
        // a non-matching credential 401s in-dialect, which is the honest answer.
        assert_eq!(
            query_value(Some("key=ab%zz"), "key").as_deref(),
            Some("ab%zz")
        );
        assert_eq!(
            query_value(Some("key=a%42b"), "key").as_deref(),
            Some("aBb")
        );
        assert_eq!(query_value(None, "alt"), None);
    }

    #[test]
    fn first_stream_terminal_wins() {
        use bytes::BytesMut;
        let mut failed = GeminiStream::new(String::new());
        let mut dst = BytesMut::new();
        failed.fail(&ProviderError::bad_gateway("boom"), &mut dst);
        let first_len = dst.len();
        failed.fail(&ProviderError::bad_gateway("again"), &mut dst);
        assert_eq!(dst.len(), first_len);

        let mut finished = GeminiStream::new(String::new());
        let mut dst = BytesMut::new();
        finished.finish(&mut dst);
        let first_len = dst.len();
        finished.fail(&ProviderError::bad_gateway("late"), &mut dst);
        assert_eq!(dst.len(), first_len);
    }

    #[test]
    fn media_returned_by_a_tool_refuses_instead_of_vanishing() {
        let base = |parts: Value| {
            json!({"contents": [{"role": "user", "parts": [
                {"functionResponse": {"name": "screenshot", "response": {"ok": true}, "parts": parts}}
            ]}]})
        };
        for parts in [
            json!([{"inlineData": {"mimeType": "image/png", "data": "AANA"}}]),
            // A `text` key beside a media arm must NOT read as a text part.
            json!([{"text": "here", "fileData": {"fileUri": "files/abc"}}]),
        ] {
            let e = fold(base(parts)).unwrap_err();
            assert_eq!(e.status, 400, "{e:?}");
            assert!(
                e.message.contains("tool RETURN is a string here"),
                "must name why tool output cannot carry it: {}",
                e.message
            );
        }
        // A part naming no arm is not "no media", it is an unparseable shape;
        // guessing text here would invent content the tool never returned.
        for (parts, why) in [
            (json!([{}]), "no content at all"),
            // A `text` that is not a string must not fold to "" — that would
            // read as a tool that answered with nothing.
            (json!([{"text": 42}]), "is not a string"),
        ] {
            let e = fold(base(parts)).unwrap_err();
            assert_eq!(e.status, 400);
            assert!(e.message.contains(why), "{}", e.message);
        }
        // Text riding beside the payload is part of the ANSWER, so it joins
        // rather than refusing — refusing it would be the same rule inverted
        // into gratuitous rejection.
        let chat = fold(base(json!([{"text": "captured 800x600"}]))).unwrap();
        let content = chat.messages[0]["content"].as_str().unwrap();
        assert!(content.contains("\"ok\":true"), "payload kept: {content}");
        assert!(
            content.ends_with("captured 800x600"),
            "text joined: {content}"
        );
    }

    /// The reasoning channel is deliberately not rendered here, and a test is
    /// the only way that stays true instead of becoming an oversight: a
    /// signature-less `thought` part would be history this module's own fold
    /// rejects, handing the client a turn it cannot replay.
    #[test]
    fn reasoning_is_not_rendered_as_an_unreplayable_thought_part() {
        let c = Completion {
            refusal: None,
            id: "g".into(),
            model: "gemini-x".into(),
            text: "answer".into(),
            finish_reason: Some("stop".into()),
            usage: None,
            tool_calls: Vec::new(),
            raw: None,
            reasoning: Some("private deliberation".into()),
        };
        let v = completion_to_generate_content(&c);
        let parts = v["candidates"][0]["content"]["parts"].as_array().unwrap();
        assert_eq!(parts.len(), 1, "no thinking part is invented: {v}");
        assert!(
            parts[0].get("thought").is_none() && parts[0].get("text") == Some(&json!("answer")),
            "only the answer text rides out: {v}"
        );
        assert!(
            !v.to_string().contains("private deliberation"),
            "reasoning text must not leak into a turn the client cannot replay"
        );
    }

    #[test]
    fn contents_is_required_at_the_seam() {
        for body in [json!({}), json!({"contents": []})] {
            let e = fold(body).unwrap_err();
            assert_eq!(e.status, 400);
            assert!(
                e.message.contains("contents"),
                "empty/absent contents is a 400, not an empty fold: {}",
                e.message
            );
        }
    }

    #[test]
    fn drops_safety_and_cached_context_documented() {
        let c = fold(json!({
            "contents": [{"parts": [{"text": "x"}]}],
            "safetySettings": [{"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_ONLY_HIGH"}],
            "cachedContent": "cachedContents/abc",
        }))
        .unwrap();
        assert!(!c.extra.contains_key("safetySettings"));
        assert!(!c.extra.contains_key("cachedContent"));
    }

    #[test]
    fn renders_buffered_completion_as_generate_content() {
        let v = completion_to_generate_content(&Completion {
            refusal: None,
            id: "gen".into(),
            model: "gemini-x".into(),
            text: "hi".into(),
            finish_reason: Some("stop".into()),
            usage: Some(Usage {
                prompt_tokens: 3,
                completion_tokens: 2,
                ..Default::default()
            }),
            tool_calls: vec![],
            raw: None,
            reasoning: None,
        });
        assert_eq!(v["candidates"][0]["content"]["parts"][0]["text"], "hi");
        assert_eq!(v["candidates"][0]["finishReason"], "STOP");
        assert_eq!(v["usageMetadata"]["totalTokenCount"], 5);
        assert_eq!(v["modelVersion"], "gemini-x");
    }

    #[test]
    fn tool_only_turn_reports_stop_with_function_call() {
        let v = completion_to_generate_content(&Completion {
            refusal: None,
            id: "gen".into(),
            model: String::new(),
            text: String::new(),
            finish_reason: Some("tool_calls".into()),
            usage: None,
            tool_calls: vec![x2api_kit::ToolCall {
                id: "c1".into(),
                name: "f".into(),
                arguments: "{\"a\":1}".into(),
            }],
            raw: None,
            reasoning: None,
        });
        let parts = v["candidates"][0]["content"]["parts"].as_array().unwrap();
        assert_eq!(
            parts.len(),
            1,
            "no spurious empty text block on a tool-only turn"
        );
        assert_eq!(parts[0]["functionCall"]["name"], "f");
        assert_eq!(parts[0]["functionCall"]["args"], json!({"a": 1}));
        assert_eq!(v["candidates"][0]["finishReason"], "STOP");
    }

    #[test]
    fn stream_terminates_on_finish_reason_frame_not_done() {
        use bytes::BytesMut;
        let mut s = GeminiStream::new("gemini-x".to_string());
        let mut dst = BytesMut::new();
        for t in ["he", "llo"] {
            s.on_chunk(
                &ChatChunk {
                    refusal: String::new(),
                    id: String::new(),
                    model: String::new(),
                    text: t.into(),
                    finish_reason: None,
                    usage: None,
                    tool_calls: Vec::new(),
                    raw: None,
                    reasoning: String::new(),
                },
                &mut dst,
            );
        }
        s.finish(&mut dst);
        let body = String::from_utf8(dst.to_vec()).unwrap();
        // No [DONE] sentinel in this dialect.
        assert!(
            !body.contains("[DONE]"),
            "gemini must not emit a chat sentinel: {body}"
        );
        assert_eq!(body.matches("data: ").count(), 3, "2 partial + 1 terminal");
        let last = body
            .lines()
            .filter_map(|l| l.strip_prefix("data: "))
            .next_back()
            .unwrap();
        let v: Value = serde_json::from_str(last).unwrap();
        assert_eq!(v["candidates"][0]["finishReason"], "STOP");
    }

    #[test]
    fn stream_failure_frame_has_no_finish_reason() {
        use bytes::BytesMut;
        let mut s = GeminiStream::new(String::new());
        let mut dst = BytesMut::new();
        s.fail(&ProviderError::bad_gateway("boom"), &mut dst);
        let body = String::from_utf8(dst.to_vec()).unwrap();
        let payload = body
            .strip_prefix("data: ")
            .and_then(|b| b.strip_suffix("\n\n"))
            .expect("one framed data record");
        let v: Value = serde_json::from_str(payload).unwrap();
        assert_eq!(v["error"]["status"], "INTERNAL");
        assert_eq!(v["error"]["message"], "boom");
        assert!(
            !body.contains("finishReason"),
            "a truncated stream must not look complete"
        );
    }

    #[test]
    fn google_status_table_is_closed_and_maps_core_codes() {
        assert_eq!(google_status(400), "INVALID_ARGUMENT");
        assert_eq!(google_status(413), "INVALID_ARGUMENT");
        assert_eq!(google_status(401), "UNAUTHENTICATED");
        assert_eq!(google_status(403), "PERMISSION_DENIED");
        assert_eq!(google_status(404), "NOT_FOUND");
        assert_eq!(google_status(429), "RESOURCE_EXHAUSTED");
        assert_eq!(google_status(503), "UNAVAILABLE");
        assert_eq!(google_status(504), "DEADLINE_EXCEEDED");
        assert_eq!(google_status(418), "INTERNAL");
    }

    #[test]
    fn buffered_text_and_refusal_emit_parts_with_safety_terminal() {
        let completion = Completion {
            refusal: Some("I cannot assist".into()),
            id: "gen".into(),
            model: "gemini-x".into(),
            text: "partial answer".into(),
            finish_reason: Some("content_filter".into()),
            usage: None,
            tool_calls: Vec::new(),
            raw: None,
            reasoning: None,
        };
        let response = completion_to_generate_content(&completion);
        let parts = response["candidates"][0]["content"]["parts"]
            .as_array()
            .unwrap();
        assert_eq!(parts[0]["text"], "partial answer");
        assert_eq!(parts[1]["text"], "I cannot assist");
        assert_eq!(response["candidates"][0]["finishReason"], "SAFETY");
    }

    #[test]
    fn streamed_refusal_emits_text_frame_and_safety_terminal() {
        use bytes::BytesMut;
        let mut stream = GeminiStream::new("gemini-x".to_string());
        let mut output = BytesMut::new();
        stream.on_chunk(
            &ChatChunk {
                refusal: "I cannot assist".into(),
                id: String::new(),
                model: String::new(),
                text: String::new(),
                finish_reason: Some("refusal".into()),
                usage: None,
                tool_calls: Vec::new(),
                raw: None,
                reasoning: String::new(),
            },
            &mut output,
        );
        stream.finish(&mut output);
        let frames: Vec<Value> = String::from_utf8(output.to_vec())
            .unwrap()
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|payload| serde_json::from_str(payload).unwrap())
            .collect();
        assert_eq!(frames.len(), 2, "refusal frame plus terminal");
        assert_eq!(
            frames[0]["candidates"][0]["content"]["parts"][0]["text"],
            "I cannot assist"
        );
        assert_eq!(frames[1]["candidates"][0]["finishReason"], "SAFETY");

        let buffered = completion_to_generate_content(&Completion {
            refusal: Some("I cannot assist".into()),
            id: String::new(),
            model: String::new(),
            text: String::new(),
            finish_reason: Some("refusal".into()),
            usage: None,
            tool_calls: Vec::new(),
            raw: None,
            reasoning: None,
        });
        let streamed_parts: Vec<Value> = frames
            .iter()
            .flat_map(|frame| {
                frame["candidates"][0]["content"]["parts"]
                    .as_array()
                    .unwrap()
            })
            .cloned()
            .collect();
        let buffered_parts = buffered["candidates"][0]["content"]["parts"]
            .as_array()
            .unwrap();
        assert_eq!(streamed_parts, *buffered_parts);
    }

    /// Text and refusal remain ordered answer-then-refusal when they arrive in
    /// the same upstream chunk, matching the buffered Gemini response.
    #[test]
    fn text_and_refusal_in_one_chunk_emit_buffered_part_order() {
        use bytes::BytesMut;
        let mut stream = GeminiStream::new("gemini-x");
        let mut output = BytesMut::new();
        stream.on_chunk(
            &ChatChunk {
                refusal: "I cannot assist".into(),
                id: String::new(),
                model: String::new(),
                text: "partial answer".into(),
                finish_reason: Some("refusal".into()),
                usage: None,
                tool_calls: Vec::new(),
                raw: None,
                reasoning: String::new(),
            },
            &mut output,
        );
        stream.finish(&mut output);

        let frames: Vec<Value> = String::from_utf8(output.to_vec())
            .unwrap()
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|payload| serde_json::from_str(payload).unwrap())
            .collect();
        let content_frames: Vec<&Value> = frames
            .iter()
            .filter(|frame| frame["candidates"][0]["finishReason"].is_null())
            .collect();
        let parts: Vec<&str> = content_frames
            .into_iter()
            .flat_map(|frame| {
                frame["candidates"][0]["content"]["parts"]
                    .as_array()
                    .unwrap()
            })
            .map(|part| part["text"].as_str().unwrap())
            .collect();
        assert_eq!(parts, ["partial answer", "I cannot assist"]);
    }

    /// OpenAI completion tokens include reasoning, while Gemini reports
    /// candidate and thought traffic separately. The rendered metadata must
    /// subtract reasoning once and preserve prompt plus both Gemini counts.
    #[test]
    fn usage_separates_reasoning_from_candidate_tokens() {
        let response = completion_to_generate_content(&Completion {
            refusal: None,
            id: "gen".into(),
            model: "gemini-x".into(),
            text: "answer".into(),
            finish_reason: Some("stop".into()),
            usage: Some(Usage {
                prompt_tokens: 10,
                completion_tokens: 7,
                reasoning_tokens: Some(3),
                ..Default::default()
            }),
            tool_calls: Vec::new(),
            raw: None,
            reasoning: None,
        });
        let usage = &response["usageMetadata"];
        assert_eq!(usage["promptTokenCount"], 10);
        assert_eq!(usage["candidatesTokenCount"], 4);
        assert_eq!(usage["thoughtsTokenCount"], 3);
        assert_eq!(usage["totalTokenCount"], 17);
    }
}
