//! The internal representation (IR): everything the pipeline speaks between
//! inbound parsing and provider serialization is OpenAI Chat-Completions
//! shaped. Providers translate vendor wire JSON to/from these types; the
//! dialect bridges consume them.
//!
//! Boundary note: IR types carry DATA and their dialect *parser*
//! (`from_openai`, the shape the IR is defined by). Rendering IR back to any
//! wire — including this dialect's own — belongs to the bridge crates
//! (`the chat module` / `-anthropic` / `-responses`), so "which formats
//! we speak" is decided exactly once, by which bridges the server depends on.
//!
//! IR is deliberately *lossy-tolerant*: `ChatRequest.extra` and the `raw`
//! fields on `Completion`/`ChatChunk` carry verbatim JSON so an OpenAI-format
//! client never loses a field the IR does not model (tool calls, vendor
//! extras, ...). A lossy spot is only ever observable to *other* dialects,
//! which is the honest cost of a single-normalized-IR design.

use std::borrow::Cow;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

/// A parsed inbound chat request, in OpenAI shape.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChatRequest {
    pub model: String,
    /// Message objects are passed through unparsed: the provider (or an
    /// inbound bridge folding another dialect) decides how to interpret
    /// `content`.
    pub messages: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    /// Every other client field (temperature, tools, response_format, ...)
    /// rides along and is merged back on `to_value`.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl ChatRequest {
    pub fn wants_stream(&self) -> bool {
        self.stream.unwrap_or(false)
    }

    /// Re-serialize to the chat-completions JSON shape (used to build
    /// upstream bodies). Data-serializing IR->its-own-shape, not a bridge
    /// render — the provider still decides field-by-field what to send.
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| {
            // A ChatRequest is built from JSON, so this cannot realistically
            // fail; fall back to the minimum a vendor accepts.
            let mut m = Map::new();
            m.insert("model".into(), self.model.clone().into());
            m.insert("messages".into(), self.messages.clone().into());
            Value::Object(m)
        })
    }
}

// ── request content parts: the chat shape IS the IR shape ───────────────

// The IR's image representation, in the only spelling the IR has.
//
// `ChatRequest::messages` are carried UNPARSED, so a chat-dialect client's
// `content` arrays — text parts and `image_url` parts — already reach the
// provider without passing through anything below. These helpers exist for
// the OTHER inbound dialects, whose media blocks must become this dialect's
// spelling because this spelling is the IR. Building it in one place is what
// keeps folds from inventing slightly different "IR image" shapes, and a
// provider that later renders IR messages into some vendor's own dialect
// reads the parts back from the same definition.
//
// ```json
// {"type":"image_url","image_url":{"url":"https://host/a.png","detail":"high"}}
// ```
//
// `url` is either an absolute http(s) URL or an inline data URL
// (`data:image/png;base64,…`); `detail` is optional, and omitting it is the
// documented `auto`. A text-only message stays a plain STRING
// (`parts_content`), because every existing fold, upstream tokenizer, and
// pinned assertion is written against that.

/// A text part of a multi-part `content` array.
pub fn text_part(text: &str) -> Value {
    json!({ "type": "text", "text": text })
}

/// Assemble a `content` value from parts, collapsing to the plain-string form
/// only when EVERY part is text. Predicated on CONTENT, not on part count: a
/// length test would turn an ordinary two-block Anthropic text turn into an
/// array, changing the wire shape every provider, assertion, and upstream
/// tokenizer already sees.
///
/// Written as "all parts are text" rather than "no part is an image_url" on
/// purpose: the day another media kind folds (audio, video, anything the chat
/// dialect grows), an image-shaped predicate would join the text and DROP the
/// new part in silence — the one failure mode this function exists to avoid.
pub fn parts_content(parts: Vec<Value>) -> Value {
    let text_only = parts
        .iter()
        .all(|p| p.get("type").and_then(Value::as_str) == Some("text"));
    if !text_only {
        return Value::Array(parts);
    }
    Value::String(
        parts
            .iter()
            .filter_map(|p| p.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// One `image_url` part.
///
/// The URL is validated instead of passed through, for two reasons. It is
/// SYNTACTIC: a data URL with a stray `,`, `;` or `#` in its mime describes a
/// payload the vendor will mis-parse, and the client would see someone else's
/// error. And the value becomes a fetch instruction for the VENDOR, so
/// schemes no dialect documents as an image source (`file://`, `gs://`,
/// `data:text/plain`) must not be forwarded at all.
///
/// `original` is accepted for `detail` even though the Chat Completions
/// reference lists only `auto|low|high`: the vision guide documents
/// `original` across both APIs and calls support model-dependent, and the
/// generated SDK literals lag the guide. Rewriting it to `auto` here would
/// silently change image sizing on a path (coordinates, computer use) that
/// exists precisely because sizing matters; a model that truly refuses it
/// answers a loud vendor 400, which is the honest outcome.
pub fn image_part(url: &str, detail: Option<&str>) -> Result<Value, crate::errors::ProviderError> {
    let good = match data_url_parts(url) {
        Some((mime, _)) => is_image_mime(mime),
        None => url.starts_with("https://") || url.starts_with("http://"),
    };
    if !good {
        return Err(crate::errors::ProviderError::bad_request(format!(
            "image url must be an absolute http(s) URL or an inline \
             data:image/…;base64, URL (got `{}`)",
            crate::util::truncate_chars(url, 64)
        )));
    }
    if let Some(d) = detail
        && !matches!(d, "auto" | "low" | "high" | "original")
    {
        return Err(crate::errors::ProviderError::bad_request(format!(
            "image detail {d:?} is not one of auto|low|high|original"
        )));
    }
    let mut inner = Map::new();
    inner.insert("url".into(), url.into());
    if let Some(d) = detail {
        inner.insert("detail".into(), d.into());
    }
    Ok(json!({ "type": "image_url", "image_url": Value::Object(inner) }))
}

/// Compose the inline form from a media type and a RAW base64 payload.
///
/// Both media parts want this composition: chat's `image_url.url` inline form
/// and its `file.file_data` (`data:application/pdf;base64,…`, which is what
/// Chat Completions documents for an inline document even though the
/// generated SDK's docstring says only "base64 encoded file data").
///
/// The payload must not itself be a data URL: dialects that carry the base64
/// separately (Anthropic's `source.data`) always carry the bare alphabet, and
/// a value already prefixed `data:` is a client that double-wrapped. Taking
/// it would compose a URL whose payload is somebody else's URL.
///
/// Which media types a given dialect ACCEPTS is the fold's business (Anthropic
/// images are four closed values, its documents two); this owns only that the
/// composition is grammatical.
pub fn data_url(media_type: &str, base64: &str) -> Result<String, crate::errors::ProviderError> {
    if !is_media_mime(media_type) {
        return Err(crate::errors::ProviderError::bad_request(format!(
            "media_type `{}` is not a `type/subtype` media type",
            crate::util::truncate_chars(media_type, 64)
        )));
    }
    if !is_base64(base64) {
        return Err(crate::errors::ProviderError::bad_request(
            "inline data is not raw base64 (the base64 alphabet alone — no `data:` \
             prefix, no whitespace, no url-safe -_ variants)",
        ));
    }
    Ok(format!("data:{media_type};base64,{base64}"))
}

/// One `file` part: `{"type":"file","file":{"filename","file_data"}}`.
///
/// `data` is forwarded in either documented form — the composed data URL
/// (`data:application/pdf;base64,…`, what the guide's JSON samples and every
/// SDK example show) or a bare base64 payload (what the guide's curl sample
/// shows and the generated docstring describes). A same-name field from the
/// client's own dialect is not this proxy's to editorialize.
///
/// What "validates" means here is STRUCTURAL: the payload must be the base64
/// alphabet, and a data URL must carry a grammatical media type. Whether those
/// bytes are really a PDF is not knowable without decoding megabytes per
/// request, and the vendor is the right judge of that — so a syntactically
/// clean but nonsensical payload forwards, by design.
///
/// Unlike `image_url`, this object DOES have a `file_id` alternative — so a
/// vendor-namespace id CAN be expressed, and it is the fold's choice whether
/// the id it holds belongs to the upstream it is talking to (see the anthropic
/// and responses modules: an OpenAI id passes through, an Anthropic Files-API
/// id is refused).
///
/// `filename` is paired with `file_data` in every OpenAI example, and
/// Anthropic's `document` block has no name field at all. When the payload
/// carries a media type the fold derives a NEUTRAL name from it
/// (`document.pdf`) — a visible label the client did not send loses far less
/// than refusing a working document. A raw payload names nothing, so the
/// client MUST supply the name: an unnamed `{file_data}` is a shape no OpenAI
/// example shows, and building one anyway would blame the vendor's 400 for a
/// body this proxy composed.
pub fn file_part(
    filename: Option<&str>,
    data: &str,
) -> Result<Value, crate::errors::ProviderError> {
    let derived_name = match data_url_parts(data) {
        Some((mime, _)) => Some(extension_for(mime)),
        None if is_base64(data) => {
            if filename.filter(|f| !f.trim().is_empty()).is_none() {
                return Err(crate::errors::ProviderError::bad_request(
                    "raw base64 file_data needs a filename — the payload carries no media \
                     type to derive one from, and a name is what tells the upstream PDF \
                     from DOCX",
                ));
            }
            None
        }
        None => {
            return Err(crate::errors::ProviderError::bad_request(format!(
                "file_data must be a data:<media-type>;base64, URL or a base64 payload \
                 (got `{}`)",
                crate::util::truncate_chars(data, 64)
            )));
        }
    };
    let name = match (filename.filter(|f| !f.trim().is_empty()), derived_name) {
        (Some(f), _) => Some(Cow::Borrowed(f)),
        (None, Some(ext)) => Some(Cow::Owned(format!("document.{ext}"))),
        (None, None) => None,
    };
    let mut file = Map::new();
    if let Some(n) = name {
        file.insert("filename".into(), Value::from(n.as_ref()));
    }
    file.insert("file_data".into(), data.into());
    Ok(json!({ "type": "file", "file": Value::Object(file) }))
}

/// The `file_id` form of the same part: `{"type":"file","file":{"file_id":…}}`.
///
/// `image_url` has no such field, which is why a hosted image reference cannot
/// fold at all while a document one can. Whether the id means anything to the
/// upstream is a judgement only a fold can make — an OpenAI Responses id is
/// the vendor's own namespace and passes through, an Anthropic Files-API id
/// does not, and each module says so at its own call site.
pub fn file_ref_part(file_id: &str) -> Value {
    json!({ "type": "file", "file": { "file_id": file_id } })
}

/// The fallback extension for an unnamed document. Content types beyond
/// Anthropic's two (PDF, plain text) and OpenAI's wider input-file table are
/// rare on this path, and the name is cosmetic next to the media type the
/// data URL already carries — so an unrecognized one says `bin` rather than
/// inventing a plausible-looking suffix.
fn extension_for(mime: &str) -> &'static str {
    match mime {
        "application/pdf" => "pdf",
        "text/plain" => "txt",
        _ => "bin",
    }
}

/// Split and validate a data URL as `<media-type>;base64,<alphabet>`,
/// returning the media type and the payload. The ONE definition of the data
/// URL this proxy forwards; image-ness is a further check on top of it.
///
/// Public because a provider rendering the IR into a vendor whose media has NO
/// URL source (Anthropic's `source.base64`, Gemini's `inlineData`) must split
/// the composed form back apart, and re-implementing this grammar per crate is
/// how a payload ends up forwarded with a media type the sender never checked.
pub fn data_url_parts(data: &str) -> Option<(&str, &str)> {
    let rest = data.strip_prefix("data:")?;
    let (mime, payload) = rest.split_once(";base64,")?;
    (is_media_mime(mime) && is_base64(payload)).then_some((mime, payload))
}

/// `image/<token>` — an image data URL, specifically.
fn is_image_mime(mime: &str) -> bool {
    mime.starts_with("image/") && is_media_mime(mime)
}

/// `type/subtype` where neither token can break out of the data-URL grammar
/// (no `;`, `,`, whitespace or `#`).
///
/// Deliberately NOT a membership test against some vendor's format list: that
/// enum is per dialect (Anthropic's images are four closed values, OpenAI
/// documents PNG/JPEG/WEBP/non-animated GIF, Gemini takes more, its documents
/// are a whole table) and belongs to the fold or provider that owns the side
/// it bites. What this owns is the composition staying unambiguous.
fn is_media_mime(mime: &str) -> bool {
    let Some((ty, sub)) = mime.split_once('/') else {
        return false;
    };
    fn token(s: &str) -> bool {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'+' | b'-'))
    }
    token(ty) && token(sub)
}

/// The standard base64 alphabet with optional trailing `=` padding, checked by
/// character class — never decoded, and deliberately no length-multiple-of-four
/// rule: unpadded base64 is common enough in the wild that a strict `% 4` guard
/// would turn a working request into a proxy-side 400, which is the one error
/// mode this check must never create. URL-safe `-`/`_` are refused rather than
/// transcoded: no dialect's documented example produces them, and a silent
/// transcode would be this proxy editing a client's payload.
fn is_base64(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let body = s.trim_end_matches('=');
    let pad = s.len() - body.len();
    // The body must exist: `"="` and `"=="` are pure padding, and an empty
    // slice passes a `bytes().all(..)` check vacuously — which would let a
    // zero-payload "document" fold as if it had content.
    !body.is_empty()
        && pad <= 2
        && body
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/'))
}

/// A media part read back OUT of the IR.
///
/// This is the reader half of the shape `image_part`/`file_part` write. A
/// provider whose upstream is NOT chat-shaped (a native Anthropic or Gemini
/// crate) has to turn `messages[]` back into that vendor's own blocks, and
/// doing it against this enum is what stops each provider crate inventing its
/// own idea of "an IR image" — the exact drift the builders exist to prevent.
///
/// Borrowed, because the payloads are megabytes of base64 that a render never
/// needs to own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Media<'a> {
    /// `{"type":"image_url","image_url":{"url":…,"detail":…}}`. `url` is
    /// either absolute http(s) or a `data:` URL — a renderer for a vendor with
    /// no URL source tells them apart with [`data_url_parts`].
    Image {
        url: &'a str,
        detail: Option<&'a str>,
    },
    /// `{"type":"file","file":{"filename":…,"file_data":…}}` — an INLINE
    /// document. `data` is verbatim what the client sent (data URL or raw
    /// base64), because this reader does not decode payloads either.
    Document {
        filename: Option<&'a str>,
        data: &'a str,
    },
    /// `{"type":"file","file":{"file_id":…}}` — a reference into SOME
    /// namespace. Whether the upstream can resolve it is a judgement only the
    /// provider owns; a vendor that cannot take a file id must refuse it
    /// loudly rather than send a dangling reference.
    FileRef { id: &'a str },
    /// A media-shaped part this core cannot interpret. The named kind forces a
    /// native renderer to accept, reject, or implement it explicitly rather
    /// than silently omitting a client payload.
    Other { kind: &'a str },
}

/// Classify one content part. Complete known shapes yield Image/Document/FileRef;
/// unknown or malformed known kinds yield `Other` so a native renderer must
/// decide explicitly, while the image|file gauge counts neither Other case.
fn media_part(part: &Value) -> Option<(Media<'_>, MediaBucket)> {
    let kind = part.get("type").and_then(Value::as_str)?;
    let (media, bucket) = match kind {
        "image_url" => {
            let img = &part["image_url"];
            let Some(url) = img.get("url").and_then(Value::as_str) else {
                return Some((Media::Other { kind }, MediaBucket::Other));
            };
            (
                Media::Image {
                    url,
                    detail: img.get("detail").and_then(Value::as_str),
                },
                MediaBucket::Image,
            )
        }
        "file" => {
            let file = &part["file"];
            if let Some(data) = file.get("file_data").and_then(Value::as_str) {
                (
                    Media::Document {
                        filename: file.get("filename").and_then(Value::as_str),
                        data,
                    },
                    MediaBucket::File,
                )
            } else if let Some(id) = file.get("file_id").and_then(Value::as_str) {
                (Media::FileRef { id }, MediaBucket::File)
            } else {
                return Some((Media::Other { kind }, MediaBucket::Other));
            }
        }
        "text" | "input_text" | "output_text" => return None,
        _ => (Media::Other { kind }, MediaBucket::Other),
    };
    Some((media, bucket))
}

#[derive(Clone, Copy)]
enum MediaBucket {
    Image,
    File,
    Other,
}

/// Every media part of one message's `content`, in document order.
///
/// Text parts and a plain-string `content` yield nothing. Unknown and malformed
/// known kinds yield [`Media::Other`] for renderers to decide, but are not
/// counted as image/file media.
pub fn media_of(content: &Value) -> Vec<Media<'_>> {
    let Value::Array(parts) = content else {
        return Vec::new();
    };
    parts
        .iter()
        .filter_map(media_part)
        .map(|(media, _)| media)
        .collect()
}

/// How many foldable media parts a WHOLE request carries, as `[images, files]`.
///
/// This shares [`media_part`] with [`media_of`], so every image/file counted
/// here is yieldable there and malformed known shapes are counted by neither.
/// `Media::Other` is deliberately absent: the current image|file gauge cannot
/// represent a part no chat-fold renderer accepts, and inventing a third series
/// would advertise traffic this bridge cannot serve.
pub fn media_counts(messages: &[Value]) -> [u64; 2] {
    let mut counts = [0u64; 2];
    for message in messages {
        let Some(parts) = message.get("content").and_then(Value::as_array) else {
            continue;
        };
        for part in parts {
            let Some((_, bucket)) = media_part(part) else {
                continue;
            };
            match bucket {
                MediaBucket::Image => counts[0] += 1,
                MediaBucket::File => counts[1] += 1,
                MediaBucket::Other => {}
            }
        }
    }
    counts
}

/// Pull `usage` out of an OpenAI-shaped frame without modelling the rest of
/// it, for a path that deliberately does not parse frames: the verbatim
/// relay forwards bytes, but a proxy in front of a metered API still owes its
/// operator a token count.
///
/// The scan is the point. With `stream_options.include_usage` set, OpenAI
/// puts `"usage":null` on EVERY chunk and the real numbers only on the last
/// one — so looking for the field name alone would match every frame and
/// parse every frame, which is exactly what this path exists to avoid.
/// Requiring `{` after the colon means one `memmem` per frame (~10 ns on a
/// 200-byte chunk) and one parse per stream.
pub fn usage_from_frame(payload: &[u8]) -> Option<Usage> {
    if !has_usage_object(payload) {
        return None;
    }
    #[derive(Deserialize)]
    struct Frame {
        #[serde(default)]
        usage: Option<Usage>,
    }
    serde_json::from_slice::<Frame>(payload).ok()?.usage
}

/// True when the frame carries `"usage": { … }` rather than `"usage": null`.
fn has_usage_object(payload: &[u8]) -> bool {
    let mut from = 0usize;
    while let Some(at) = memchr::memmem::find(&payload[from..], b"\"usage\"") {
        let mut i = from + at + b"\"usage\"".len();
        // Skip the colon and any whitespace the vendor's serializer left.
        while let Some(b) = payload.get(i) {
            match b {
                b':' | b' ' | b'\t' | b'\n' | b'\r' => i += 1,
                b'{' => return true,
                _ => break,
            }
        }
        from += at + 1;
    }
    false
}

/// One completed tool call, as the vendor finished it.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    /// Arguments as the wire carries them: a JSON **string**, not an object.
    /// Kept unparsed because every dialect that renders it either forwards
    /// the string or embeds it — parsing here would only be re-serialized.
    pub arguments: String,
}

/// A fragment of a tool call arriving mid-stream. OpenAI sends the id and
/// name once, then argument text in pieces, all keyed by `index`; a bridge
/// accumulates by that index and cannot assume order or completeness.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ToolCallDelta {
    #[serde(default)]
    pub index: u32,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    /// This frame's slice of the arguments JSON. Usually a few characters,
    /// often not valid JSON on its own.
    #[serde(default)]
    pub arguments: String,
}

/// Recurring token counters reported by OpenAI-compatible and Anthropic-style
/// upstreams. Optional detail fields distinguish "absent" from zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
    pub cache_read_input_tokens: Option<u64>,
}

impl<'de> Deserialize<'de> for Usage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let value = Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| D::Error::custom("usage must be an object"))?;
        let required = |key: &str| {
            let value = object
                .get(key)
                .ok_or_else(|| D::Error::custom(format!("usage.{key} is required")))?;
            value.as_u64().ok_or_else(|| {
                D::Error::custom(format!("usage.{key} must be a non-negative integer"))
            })
        };
        let optional = |key: &str| object.get(key).and_then(Value::as_u64);
        let nested = |key: &str, child: &str| {
            object
                .get(key)
                .and_then(Value::as_object)
                .and_then(|v| v.get(child))
                .and_then(Value::as_u64)
        };
        Ok(Self {
            prompt_tokens: required("prompt_tokens")?,
            completion_tokens: required("completion_tokens")?,
            total_tokens: optional("total_tokens"),
            cached_tokens: nested("prompt_tokens_details", "cached_tokens")
                .or_else(|| optional("prompt_cache_hit_tokens")),
            reasoning_tokens: nested("completion_tokens_details", "reasoning_tokens"),
            cache_creation_input_tokens: optional("cache_creation_input_tokens"),
            cache_read_input_tokens: optional("cache_read_input_tokens"),
        })
    }
}

impl Serialize for Usage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap as _;
        let mut map = serializer.serialize_map(Some(7))?;
        map.serialize_entry("prompt_tokens", &self.prompt_tokens)?;
        map.serialize_entry("completion_tokens", &self.completion_tokens)?;
        if let Some(total) = self.total_tokens {
            map.serialize_entry("total_tokens", &total)?;
        }
        if let Some(cached) = self.cached_tokens {
            map.serialize_entry("prompt_tokens_details", &{
                #[derive(Serialize)]
                struct Details<'a> {
                    cached_tokens: &'a u64,
                }
                Details {
                    cached_tokens: &cached,
                }
            })?;
        }
        if let Some(reasoning) = self.reasoning_tokens {
            map.serialize_entry("completion_tokens_details", &{
                #[derive(Serialize)]
                struct Details<'a> {
                    reasoning_tokens: &'a u64,
                }
                Details {
                    reasoning_tokens: &reasoning,
                }
            })?;
        }
        if let Some(value) = self.cache_creation_input_tokens {
            map.serialize_entry("cache_creation_input_tokens", &value)?;
        }
        if let Some(value) = self.cache_read_input_tokens {
            map.serialize_entry("cache_read_input_tokens", &value)?;
        }
        map.end()
    }
}

/// A buffered completion. `raw` is the full OpenAI `chat.completion` object
/// when the provider can supply one (relay path: verbatim to the client).
#[derive(Debug, Clone, Default)]
pub struct Completion {
    pub id: String,
    pub model: String,
    /// Concatenated assistant text (first choice). Empty for tool-call-only
    /// completions — those survive via `raw`.
    pub text: String,
    pub finish_reason: Option<String>,
    pub usage: Option<Usage>,
    /// Tool calls the assistant asked for. Empty is the common case and costs
    /// no allocation; the chat dialect still serves them from `raw`, but the
    /// other bridges can only see them here.
    pub tool_calls: Vec<ToolCall>,
    pub raw: Option<Value>,
    /// Reasoning text the upstream produced, as an IR FIELD rather than
    /// smuggled inside `raw`.
    ///
    /// The distinction is the point: `raw` is the chat dialect's escape hatch,
    /// so a non-chat bridge can only ever see what is a field. That is how
    /// upstream thinking vanished for Anthropic and Responses clients — and why
    /// a client that must replay it (DeepSeek demands `reasoning_content` on
    /// prior assistant turns once `tools` are in play) had nothing to replay.
    ///
    /// `None` means the upstream said nothing of the kind; empty vendor text is
    /// normalized to `None`, never carried as a fake answer.
    pub reasoning: Option<String>,
    /// Safety refusal text. A refusal is a successful model answer, not an
    /// error; non-chat bridges render its payload in their native grammar.
    pub refusal: Option<String>,
}

impl Completion {
    /// Parse an OpenAI-shaped `chat.completion` value into IR.
    pub fn from_openai(value: Value) -> Result<Self, crate::errors::ProviderError> {
        if value.get("error").is_some()
            && !value
                .get("choices")
                .and_then(Value::as_array)
                .is_some_and(|choices| !choices.is_empty())
            && let Some(envelope) =
                crate::errors::openai_error_object(&serde_json::to_vec(&value).unwrap_or_default())
        {
            return Err(in_band_error(envelope));
        }
        let choice = value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|c| c.first());
        // Hoisted above the literal: the borrow of `value` must end before
        // `raw: Some(value)` moves it below.
        let reasoning = choice
            .and_then(|c| c.get("message"))
            .and_then(|m| m.get("reasoning_content"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let refusal = choice
            .and_then(|c| c.get("message"))
            .and_then(|m| m.get("refusal"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        Ok(Self {
            id: value
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("opencode2api-completion")
                .to_string(),
            model: value
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            text: choice
                .and_then(|c| c.get("message"))
                .and_then(|m| m.get("content"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            finish_reason: choice
                .and_then(|c| c.get("finish_reason"))
                .and_then(Value::as_str)
                .map(str::to_string),
            usage: value
                .get("usage")
                .and_then(|u| serde_json::from_value::<Usage>(u.clone()).ok()),
            tool_calls: choice
                .and_then(|c| c.get("message"))
                .and_then(|m| m.get("tool_calls"))
                .and_then(Value::as_array)
                .map(|calls| calls.iter().map(tool_call_from_wire).collect())
                .unwrap_or_default(),
            raw: Some(value),
            // Buffered spelling of the same compatible field — a sibling of
            // `content` on the message, empty normalized to `None`.
            reasoning,
            refusal,
        })
    }
}

/// Read one `tool_calls[]` entry in either the buffered or streamed shape:
/// the function object is nested, and every field is optional on the wire.
fn tool_call_from_wire(v: &Value) -> ToolCall {
    let f = v.get("function");
    ToolCall {
        id: v
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        name: f
            .and_then(|f| f.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        arguments: f
            .and_then(|f| f.get("arguments"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
    }
}

/// One streamed delta in IR terms. As with `Completion`, `raw` carries the
/// vendor's own chunk so chat-dialect clients get it verbatim — but as the
/// wire BYTES, not a parsed `Value`: the only consumer (the compat bridge)
/// writes them straight into the frame, so parsing and re-serializing a
/// document just to reproduce it was pure loss. The text/usage fields exist
/// for the other bridges, which never look at `raw` at all.
#[derive(Debug, Clone, Default)]
pub struct ChatChunk {
    pub id: String,
    pub model: String,
    /// Concatenated text deltas of choice zero in this frame; the IR template
    /// is single-choice.
    pub text: String,
    pub finish_reason: Option<String>,
    pub usage: Option<Usage>,
    /// Tool-call fragments carried by THIS frame, in wire order.
    pub tool_calls: Vec<ToolCallDelta>,
    pub raw: Option<Bytes>,
    /// Reasoning text carried by THIS frame, parallel to `text`. See
    /// [`Completion::reasoning`] for why it is a field and not a `raw` rider.
    pub reasoning: String,
    /// Safety refusal text carried by this frame. Empty means no refusal.
    pub refusal: String,
}

/// The wire fields the IR actually reads off a `chat.completion.chunk`.
/// Borrowed where the payload allows it (serde falls back to owned only for
/// strings carrying escapes), so a frame is not copied to be understood.
#[derive(Deserialize)]
struct WireChunk<'a> {
    #[serde(borrow, default)]
    id: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    model: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    choices: Option<Vec<WireChoice<'a>>>,
    usage: Option<Usage>,
}

#[derive(Deserialize)]
struct WireChoice<'a> {
    #[serde(default)]
    index: u32,
    #[serde(borrow, default)]
    delta: Option<WireDelta<'a>>,
    #[serde(borrow, default)]
    finish_reason: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct WireDelta<'a> {
    #[serde(borrow, default)]
    content: Option<Cow<'a, str>>,
    /// The chat-completions-compatible name several reasoning vendors use —
    /// a sibling of `content` in the same delta, per DeepSeek's documented
    /// stream shape. Read here because the IR only needs "reasoning text
    /// appeared in this frame"; a provider whose upstream spells the field
    /// differently sets [`ChatChunk::reasoning`] in its own decode, which is
    /// where a vendor's field NAME belongs.
    #[serde(borrow, default)]
    reasoning_content: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    refusal: Option<Cow<'a, str>>,
    #[serde(default)]
    tool_calls: Vec<WireToolCall<'a>>,
}

#[derive(Deserialize)]
struct WireToolCall<'a> {
    #[serde(default)]
    index: u32,
    #[serde(borrow, default)]
    id: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    function: Option<WireFunction<'a>>,
}

#[derive(Deserialize)]
struct WireFunction<'a> {
    #[serde(borrow, default)]
    name: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    arguments: Option<Cow<'a, str>>,
}

impl ChatChunk {
    /// Parse an OpenAI `chat.completion.chunk` payload into IR, keeping the
    /// payload itself as `raw`. This is the streaming hot path: one parse,
    /// no intermediate document, and `raw` is a refcount bump on the buffer
    /// the bytes already live in.
    pub fn from_wire(payload: Bytes) -> Result<Self, crate::errors::ProviderError> {
        let wire: WireChunk<'_> = serde_json::from_slice(&payload).map_err(|e| {
            crate::errors::ProviderError::bad_gateway(format!("malformed upstream chunk: {e}"))
        })?;
        if wire.choices.as_ref().is_none_or(Vec::is_empty)
            && let Some(envelope) = crate::errors::openai_error_object(&payload)
        {
            return Err(in_band_error(envelope));
        }
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut refusal = String::new();
        let mut finish_reason = None;
        let mut tool_calls: Vec<ToolCallDelta> = Vec::new();
        // Multi-arm frames are outside the single-choice template by design;
        // a one-arm non-zero index is the only unambiguous fallback.
        let choices = wire.choices.as_deref().unwrap_or_default();
        if let Some(c) = choices
            .iter()
            .find(|choice| choice.index == 0)
            .or_else(|| (choices.len() == 1).then(|| &choices[0]))
        {
            if let Some(delta) = c.delta.as_ref() {
                if let Some(t) = delta.content.as_deref() {
                    text.push_str(t);
                }
                // Its own buffer, so a frame that interleaves both keeps both.
                if let Some(r) = delta.reasoning_content.as_deref() {
                    reasoning.push_str(r);
                }
                if let Some(r) = delta.refusal.as_deref() {
                    refusal.push_str(r);
                }
                for call in &delta.tool_calls {
                    tool_calls.push(ToolCallDelta {
                        index: call.index,
                        id: call.id.as_deref().map(str::to_string),
                        name: call
                            .function
                            .as_ref()
                            .and_then(|f| f.name.as_deref())
                            .map(str::to_string),
                        arguments: call
                            .function
                            .as_ref()
                            .and_then(|f| f.arguments.as_deref())
                            .unwrap_or("")
                            .to_string(),
                    });
                }
            }
            finish_reason = c.finish_reason.as_deref().map(str::to_string);
        }
        Ok(Self {
            id: wire
                .id
                .as_deref()
                .unwrap_or("opencode2api-chunk")
                .to_string(),
            model: wire.model.as_deref().unwrap_or("").to_string(),
            text,
            finish_reason,
            usage: wire.usage,
            tool_calls,
            raw: Some(payload),
            reasoning,
            refusal,
        })
    }

    /// Parse an already-materialized chunk `Value` (the buffered-upstream
    /// fallback, where a document exists whether or not the IR wants one).
    pub fn from_openai(value: Value) -> Result<Self, crate::errors::ProviderError> {
        let payload = serde_json::to_vec(&value).map_err(|e| {
            crate::errors::ProviderError::bad_gateway(format!("unserializable chunk: {e}"))
        })?;
        Self::from_wire(Bytes::from(payload))
    }
}

fn in_band_error(envelope: Value) -> crate::errors::ProviderError {
    // The operator keeps the vendor explanation in logs; verbatim 5xx relay
    // would leak internals, while retrying a committed 200 can double-bill.
    let message = envelope
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("");
    let error_type = envelope
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let error_code = envelope
        .get("code")
        .map_or_else(String::new, Value::to_string)
        .trim_matches('"')
        .to_string();
    tracing::warn!(
        error_type = %error_type,
        error_code = %error_code,
        error_message = %crate::util::truncate_chars(message, 200),
        "upstream returned an error inside a successful response"
    );
    let mut error =
        crate::errors::ProviderError::permanent_bad_gateway("upstream returned status 502");
    // Parse both hints: malformed or 5xx values veto a safe sibling because a
    // contradictory vendor hint is less trustworthy than a single clean one.
    enum Hint {
        Absent,
        Numeric,
        Bad,
    }
    let hints = ["code", "status"].into_iter().map(|key| {
        let Some(value) = envelope.get(key) else {
            return Hint::Absent;
        };
        match value {
            Value::Number(number) => number
                .as_u64()
                .and_then(|number| u16::try_from(number).ok())
                .map_or(Hint::Bad, |status| {
                    if (400..500).contains(&status) {
                        Hint::Numeric
                    } else {
                        Hint::Bad
                    }
                }),
            Value::String(text) => text.parse::<u16>().map_or_else(
                |_| {
                    if matches!(
                        text.as_str(),
                        "permission_denied"
                            | "rate_limit_exceeded"
                            | "model_not_found"
                            | "invalid_api_key"
                            | "content_filter"
                    ) {
                        Hint::Absent
                    } else {
                        Hint::Bad
                    }
                },
                |status| {
                    if (400..500).contains(&status) {
                        Hint::Numeric
                    } else {
                        Hint::Bad
                    }
                },
            ),
            _ => Hint::Bad,
        }
    });
    let hint = hints.fold(Hint::Absent, |class, hint| match (class, hint) {
        (Hint::Bad, _) | (_, Hint::Bad) => Hint::Bad,
        (Hint::Numeric, Hint::Numeric) => Hint::Numeric,
        (Hint::Absent, hint) | (hint, Hint::Absent) => hint,
    });
    let client_error = match hint {
        Hint::Numeric => true,
        Hint::Bad => false,
        Hint::Absent => {
            let canonical_type = matches!(
                envelope.get("type").and_then(Value::as_str),
                Some(
                    "invalid_request_error"
                        | "authentication_error"
                        | "permission_error"
                        | "not_found_error"
                        | "rate_limit_error"
                )
            );
            let client_code = matches!(
                envelope.get("code").and_then(Value::as_str),
                Some(
                    "permission_denied"
                        | "rate_limit_exceeded"
                        | "model_not_found"
                        | "invalid_api_key"
                        | "content_filter"
                )
            );
            canonical_type || client_code
        }
    };
    if client_error {
        error.error_body = Some(envelope);
    }
    error
}

/// Buffered -> streamed mode conversion. Lives in core (not the compat
/// bridge) because every provider whose upstream may answer a
/// `stream:true` request with plain JSON needs it — it is a *provider-side
/// fallback*, not a client-facing render. The synthesized chunks carry
/// `raw`, so whatever dialect serves them stays verbatim downstream.
pub fn completion_to_chunks(c: &Completion) -> Vec<ChatChunk> {
    let created = crate::util::now_epoch_secs();
    let mut first = Map::new();
    first.insert("id".into(), c.id.clone().into());
    first.insert("object".into(), "chat.completion.chunk".into());
    first.insert("created".into(), created.into());
    first.insert("model".into(), c.model.clone().into());
    let mut delta_choice = Map::new();
    delta_choice.insert(
        "delta".into(),
        Value::Object({
            let mut d = Map::new();
            d.insert("role".into(), "assistant".into());
            d.insert("content".into(), c.text.clone().into());
            // Re-emitted under the same compatible name: the buffered→streamed
            // conversion must not be where thinking quietly disappears for a
            // chat-dialect client that would have seen it live.
            if let Some(r) = &c.refusal {
                d.insert("refusal".into(), r.clone().into());
            }
            if let Some(r) = &c.reasoning {
                d.insert("reasoning_content".into(), r.clone().into());
            }
            d
        }),
    );
    delta_choice.insert("index".into(), 0.into());
    delta_choice.insert("finish_reason".into(), Value::Null);
    first.insert("choices".into(), vec![Value::Object(delta_choice)].into());

    let mut last = Map::new();
    last.insert("id".into(), c.id.clone().into());
    last.insert("object".into(), "chat.completion.chunk".into());
    last.insert("created".into(), created.into());
    last.insert("model".into(), c.model.clone().into());
    let mut fin_choice = Map::new();
    fin_choice.insert("index".into(), 0.into());
    let terminal_calls: Vec<Value> = c
        .tool_calls
        .iter()
        .enumerate()
        .map(|(index, call)| {
            json!({
                "index": index,
                "id": call.id,
                "type": "function",
                "function": {
                    "name": call.name,
                    "arguments": call.arguments
                }
            })
        })
        .collect();
    let mut terminal_delta = Map::new();
    if !terminal_calls.is_empty() {
        terminal_delta.insert("tool_calls".into(), terminal_calls.into());
    }
    fin_choice.insert("delta".into(), Value::Object(terminal_delta));
    let finish_reason = c.finish_reason.clone().unwrap_or_else(|| {
        if c.tool_calls.is_empty() {
            "stop".into()
        } else {
            "tool_calls".into()
        }
    });
    fin_choice.insert("finish_reason".into(), Value::String(finish_reason.clone()));
    last.insert("choices".into(), vec![Value::Object(fin_choice)].into());
    if let Some(u) = &c.usage
        && let Ok(v) = serde_json::to_value(u)
    {
        last.insert("usage".into(), v);
    }

    vec![
        ChatChunk {
            id: c.id.clone(),
            model: c.model.clone(),
            text: c.text.clone(),
            finish_reason: None,
            usage: None,
            tool_calls: Vec::new(),
            raw: serialized(Value::Object(first)),
            // Reasoning rides on the FIRST frame, matching where `content`
            // went: a renderer that opens a thinking block must open it before
            // any text block it emits afterwards.
            reasoning: c.reasoning.clone().unwrap_or_default(),
            refusal: c.refusal.clone().unwrap_or_default(),
        },
        ChatChunk {
            id: c.id.clone(),
            model: c.model.clone(),
            text: String::new(),
            finish_reason: Some(finish_reason),
            usage: c.usage,
            tool_calls: c
                .tool_calls
                .iter()
                .enumerate()
                .map(|(index, call)| ToolCallDelta {
                    index: index as u32,
                    id: Some(call.id.clone()),
                    name: Some(call.name.clone()),
                    arguments: call.arguments.clone(),
                })
                .collect(),
            raw: serialized(Value::Object(last)),
            reasoning: String::new(),
            refusal: String::new(),
        },
    ]
}

/// A synthesized chunk's `raw` is the same thing an upstream frame's is: the
/// bytes a chat-dialect client will receive. Serialize once, here.
fn serialized(value: Value) -> Option<Bytes> {
    serde_json::to_vec(&value).ok().map(Bytes::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn usage_decodes_openai_nested_and_anthropic_flat_accounting() {
        let openai: Usage = serde_json::from_value(json!({
            "prompt_tokens": 10,
            "completion_tokens": 4,
            "total_tokens": 17,
            "prompt_tokens_details": {"cached_tokens": 3},
            "completion_tokens_details": {"reasoning_tokens": 2}
        }))
        .unwrap();
        assert_eq!(openai.total_tokens, Some(17));
        assert_eq!(openai.cached_tokens, Some(3));
        assert_eq!(openai.reasoning_tokens, Some(2));

        let anthropic: Usage = serde_json::from_value(json!({
            "prompt_tokens": 10,
            "completion_tokens": 4,
            "total_tokens": 19,
            "prompt_cache_hit_tokens": 5,
            "cache_creation_input_tokens": 6,
            "cache_read_input_tokens": 7
        }))
        .unwrap();
        assert_eq!(anthropic.total_tokens, Some(19));
        assert_eq!(anthropic.cached_tokens, Some(5));
        assert_eq!(anthropic.cache_creation_input_tokens, Some(6));
        assert_eq!(anthropic.cache_read_input_tokens, Some(7));
    }

    #[test]
    fn buffered_and_streamed_refusals_decode_from_their_native_fields() {
        let completion = Completion::from_openai(json!({
            "id": "c1", "model": "m",
            "choices": [{"index": 0, "message": {
                "content": null, "refusal": "I cannot assist"
            }}]
        }))
        .unwrap();
        assert_eq!(completion.refusal.as_deref(), Some("I cannot assist"));

        let chunk = ChatChunk::from_openai(json!({
            "id": "c1", "model": "m",
            "choices": [{"index": 0, "delta": {"refusal": "I cannot assist"}}]
        }))
        .unwrap();
        assert_eq!(chunk.refusal, "I cannot assist");
    }

    #[test]
    fn completion_to_chunks_carries_refusal_on_the_first_frame() {
        let completion = Completion {
            id: "c1".into(),
            model: "m".into(),
            refusal: Some("I cannot assist".into()),
            ..Default::default()
        };
        let chunks = completion_to_chunks(&completion);
        assert_eq!(chunks[0].refusal, "I cannot assist");
        let raw: Value = serde_json::from_slice(chunks[0].raw.as_ref().unwrap()).unwrap();
        assert_eq!(raw["choices"][0]["delta"]["refusal"], "I cannot assist");
    }

    #[test]
    fn completion_to_chunks_preserves_length_when_tools_are_present() {
        let completion = Completion {
            id: "c1".into(),
            model: "m".into(),
            finish_reason: Some("length".into()),
            tool_calls: vec![ToolCall {
                id: "call_1".into(),
                name: "lookup".into(),
                arguments: "{\"city\":\"SF\"}".into(),
            }],
            ..Default::default()
        };
        let chunks = completion_to_chunks(&completion);
        let terminal_raw: Value = serde_json::from_slice(chunks[1].raw.as_ref().unwrap()).unwrap();
        assert_eq!(chunks[1].finish_reason.as_deref(), Some("length"));
        assert_eq!(chunks[1].tool_calls[0].id.as_deref(), Some("call_1"));
        assert_eq!(terminal_raw["choices"][0]["finish_reason"], "length");
    }

    #[test]
    fn completion_to_chunks_synthesizes_tool_calls_finish_when_reason_is_absent() {
        let completion = Completion {
            id: "c1".into(),
            model: "m".into(),
            tool_calls: vec![ToolCall {
                id: "call_1".into(),
                name: "lookup".into(),
                arguments: "{\"city\":\"SF\"}".into(),
            }],
            ..Default::default()
        };
        let chunks = completion_to_chunks(&completion);
        let terminal_raw: Value = serde_json::from_slice(chunks[1].raw.as_ref().unwrap()).unwrap();
        assert_eq!(chunks[1].finish_reason.as_deref(), Some("tool_calls"));
        assert_eq!(chunks[1].tool_calls[0].name.as_deref(), Some("lookup"));
        assert_eq!(terminal_raw["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(
            terminal_raw["choices"][0]["delta"]["tool_calls"][0]["id"],
            "call_1"
        );
    }

    #[test]
    fn usage_serialization_round_trips_openai_nested_details() {
        let usage = Usage {
            prompt_tokens: 10,
            completion_tokens: 4,
            total_tokens: Some(17),
            cached_tokens: Some(3),
            reasoning_tokens: Some(2),
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
        };
        let wire = serde_json::to_value(usage).unwrap();
        assert_eq!(wire["prompt_tokens_details"]["cached_tokens"], 3);
        assert_eq!(wire["completion_tokens_details"]["reasoning_tokens"], 2);
        assert_eq!(serde_json::from_value::<Usage>(wire).unwrap(), usage);
    }

    #[test]
    fn null_choices_are_classified_without_choice_iteration() {
        let payload = Bytes::from_static(
            br#"{"choices":null,"error":{"message":"blocked","type":"server_error"}}"#,
        );
        let error = ChatChunk::from_wire(payload).unwrap_err();
        assert_eq!(error.status, 502);
    }

    #[test]
    fn usage_rejects_a_non_numeric_prompt_count_but_accepts_absent_details() {
        let invalid = serde_json::from_value::<Usage>(json!({
            "prompt_tokens": "many",
            "completion_tokens": 2
        }));
        assert!(invalid.is_err(), "a string count cannot be coerced to zero");

        let valid: Usage = serde_json::from_value(json!({
            "prompt_tokens": 10,
            "completion_tokens": 4
        }))
        .unwrap();
        assert_eq!((valid.prompt_tokens, valid.completion_tokens), (10, 4));
        assert_eq!(valid.cached_tokens, None);
        assert_eq!(valid.reasoning_tokens, None);
    }

    #[test]
    fn malformed_images_and_unknown_audio_are_explicit_other_media_without_being_counted() {
        let parts = json!([
            {"type": "image_url", "image_url": {"detail": "high"}},
            {"type": "input_audio", "input_audio": {"data": "AA=="}}
        ]);
        assert_eq!(
            media_of(&parts),
            vec![
                Media::Other { kind: "image_url" },
                Media::Other {
                    kind: "input_audio"
                }
            ],
            "native renderers see the actual kind rather than losing the part"
        );
        let messages = vec![json!({"role": "user", "content": parts})];
        assert_eq!(
            media_counts(&messages),
            [0, 0],
            "malformed image and unsupported audio are not metered as served media"
        );
    }

    #[test]
    fn chat_request_round_trips_extras() {
        let raw = json!({
            "model": "gpt-x",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": true,
            "temperature": 0.5,
            "tools": [{"type": "function"}]
        });
        let req: ChatRequest = serde_json::from_value(raw.clone()).unwrap();
        assert!(req.wants_stream());
        assert_eq!(req.to_value(), raw);
    }

    #[test]
    fn usage_is_read_from_the_frame_that_actually_carries_it() {
        // The shape OpenAI sends on every chunk when include_usage is set:
        // the field is present and null, and must NOT look like a count.
        assert!(
            usage_from_frame(br#"{"choices":[{"delta":{"content":"x"}}],"usage":null}"#).is_none(),
            "a null usage is not a usage"
        );
        assert!(usage_from_frame(br#"{"choices":[]}"#).is_none());
        // …and the terminal frame, which does.
        let u = usage_from_frame(
            br#"{"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":22}}"#,
        )
        .expect("counted");
        assert_eq!((u.prompt_tokens, u.completion_tokens), (11, 22));
        // Whitespace from a pretty-printing vendor still reads.
        assert!(
            usage_from_frame(br#"{"usage" : {"prompt_tokens":1,"completion_tokens":2}}"#).is_some()
        );
        // A frame merely MENTIONING usage in text is not one.
        assert!(
            usage_from_frame(br#"{"choices":[{"delta":{"content":"\"usage\": null"}}]}"#).is_none()
        );
        // Garbage never panics; it simply is not a usage frame.
        assert!(usage_from_frame(b"{\"usage\":{ truncated").is_none());
    }

    #[test]
    fn chunk_parses_only_choice_zero_text_and_finish() {
        let v = json!({
            "id": "c1", "model": "m", "object": "chat.completion.chunk",
            "choices": [
                {"index": 0, "delta": {"content": "primary"}, "finish_reason": "stop"},
                {"index": 1, "delta": {"content": "must not leak", "reasoning_content": "secondary reasoning",
                    "tool_calls": [{"index": 7, "id": "wrong", "function": {"name": "wrong", "arguments": "{}"}}]},
                 "finish_reason": "length"}
            ]
        });
        let c = ChatChunk::from_openai(v).unwrap();
        assert_eq!(c.text, "primary");
        assert_eq!(c.finish_reason.as_deref(), Some("stop"));
        assert!(c.reasoning.is_empty());
        assert!(c.tool_calls.is_empty());
    }

    #[test]
    fn in_band_errors_are_canned_but_vendor_details_reach_logs() {
        let error = json!({
            "message": "upstream overloaded",
            "type": "server_error",
            "code": "overloaded"
        });
        let capture = crate::testlog::Capture::start("types-in-band");
        let completion = Completion::from_openai(json!({ "error": error.clone() })).unwrap_err();
        let payload = Bytes::from(serde_json::to_vec(&json!({ "error": error })).unwrap());
        let chunk = ChatChunk::from_wire(payload).unwrap_err();
        let logged = capture.finish();
        for err in [&completion, &chunk] {
            assert_eq!(err.status, 502);
            assert!(!err.is_retryable());
            assert_eq!(err.error_body, None);
            assert!(!err.message.contains("upstream overloaded"));
        }
        let warning = logged
            .into_iter()
            .find(|line| line["level"] == "WARN")
            .expect("in-band error warning");
        assert!(
            warning["fields"]["error_message"]
                .as_str()
                .unwrap()
                .contains("upstream overloaded")
        );
        assert_eq!(warning["fields"]["error_type"], "server_error");
        assert_eq!(warning["fields"]["error_code"], "overloaded");
    }

    #[test]
    fn client_safe_in_band_envelope_keeps_its_verbatim_body() {
        let error = json!({
            "message": "bad model",
            "type": "invalid_request_error",
            "code": "model_not_found"
        });
        let err = Completion::from_openai(json!({ "error": error.clone() })).unwrap_err();
        assert_eq!(err.status, 502);
        assert!(!err.is_retryable());
        assert_eq!(err.error_body, Some(error));
    }

    #[test]
    fn explicit_server_status_overrides_a_client_safe_type() {
        let capture = crate::testlog::Capture::start("types-in-band-precedence");
        let err = Completion::from_openai(json!({
            "error": {
                "message": "internal failure",
                "type": "invalid_request_error",
                "code": 500
            }
        }))
        .unwrap_err();
        let logged = capture.finish();
        assert_eq!(err.status, 502);
        assert!(!err.is_retryable());
        assert_eq!(err.error_body, None);
        assert!(
            logged
                .iter()
                .any(|line| line["level"] == "WARN" && line["fields"]["error_code"] == "500")
        );
    }

    #[test]
    fn canonical_type_and_client_code_are_safely_classified() {
        for error in [
            json!({"message": "bad request", "type": "invalid_request_error"}),
            json!({
                "message": "missing model",
                "type": "vendor_specific_error",
                "code": "model_not_found"
            }),
        ] {
            let err = Completion::from_openai(json!({ "error": error.clone() })).unwrap_err();
            assert!(!err.is_retryable());
            assert_eq!(err.error_body, Some(error));
        }
    }

    #[test]
    fn unknown_type_and_code_stay_canned() {
        let err = Completion::from_openai(json!({
            "error": {
                "message": "unknown failure",
                "type": "vendor_specific_error",
                "code": "opaque_failure"
            }
        }))
        .unwrap_err();
        assert!(!err.is_retryable());
        assert_eq!(err.error_body, None);
    }

    #[test]
    fn multiple_nonzero_choices_do_not_choose_an_arm() {
        let c = ChatChunk::from_openai(json!({
            "choices": [
                {"index": 1, "delta": {"content": "A"}},
                {"index": 2, "delta": {"content": "B"}, "finish_reason": "stop"}
            ]
        }))
        .unwrap();
        assert!(c.text.is_empty());
        assert!(c.finish_reason.is_none());
    }

    #[test]
    fn finish_reason_never_comes_from_a_nonzero_choice() {
        let c = ChatChunk::from_openai(json!({
            "choices": [
                {"index": 0, "delta": {"content": "A"}, "finish_reason": null},
                {"index": 1, "delta": {}, "finish_reason": "stop"}
            ]
        }))
        .unwrap();
        assert!(c.finish_reason.is_none());
    }

    #[test]
    fn out_of_range_numeric_hint_vetoes_client_type() {
        let err = Completion::from_openai(json!({
            "error": {"message": "bad", "type": "invalid_request_error", "code": 70000}
        }))
        .unwrap_err();
        assert_eq!(err.error_body, None);
    }

    #[test]
    fn invalid_api_key_is_a_client_error_code() {
        let error = json!({
            "message": "bad key",
            "type": "vendor_error",
            "code": "invalid_api_key"
        });
        let err = Completion::from_openai(json!({ "error": error.clone() })).unwrap_err();
        assert!(!err.is_retryable());
        assert_eq!(err.error_body, Some(error));
    }

    #[test]
    fn both_decoders_log_and_operator_message_is_capped() {
        let message = "x".repeat(240);
        let error = json!({"message": message, "type": "server_error"});
        let capture = crate::testlog::Capture::start("types-two-warns");
        let _ = Completion::from_openai(json!({ "error": error.clone() })).unwrap_err();
        let _ = ChatChunk::from_openai(json!({ "error": error })).unwrap_err();
        let lines = capture.finish();
        let warnings: Vec<_> = lines
            .iter()
            .filter(|line| line["level"] == "WARN")
            .collect();
        assert_eq!(warnings.len(), 2);
        for warning in warnings {
            let logged = warning["fields"]["error_message"].as_str().unwrap();
            assert!(logged.chars().count() <= 201, "{logged:?}");
            assert!(logged.starts_with('x'));
        }
    }

    #[test]
    fn conflicting_numeric_hints_veto_verbatim_relay() {
        for error in [
            json!({"message": "x", "type": "invalid_api_key", "code": 400, "status": 500}),
            json!({"message": "x", "type": "invalid_api_key", "code": "oops", "status": 500}),
            json!({"message": "x", "type": "invalid_api_key", "code": 400, "status": "oops"}),
        ] {
            let err = Completion::from_openai(json!({ "error": error })).unwrap_err();
            assert_eq!(err.error_body, None);
        }
        let err = Completion::from_openai(json!({
            "error": {"message": "x", "type": "invalid_api_key", "code": 400}
        }))
        .unwrap_err();
        assert!(err.error_body.is_some());
    }

    #[test]
    fn string_error_shorthand_is_rejected_by_both_decoders() {
        assert!(Completion::from_openai(json!({"error": "quota exhausted"})).is_err());
        assert!(ChatChunk::from_openai(json!({"error": "quota exhausted"})).is_err());
    }

    #[test]
    fn one_based_only_choice_is_not_silently_dropped() {
        let c = ChatChunk::from_openai(json!({
            "choices": [{"index": 1, "delta": {"content": "present"}}]
        }))
        .unwrap();
        assert_eq!(c.text, "present");
    }

    #[test]
    fn in_band_error_with_empty_choices_is_still_an_error() {
        let buffered = Completion::from_openai(json!({
            "error": {"message": "blocked"},
            "choices": []
        }))
        .unwrap_err();
        let streamed = ChatChunk::from_openai(json!({
            "error": {"message": "blocked"},
            "choices": []
        }))
        .unwrap_err();
        assert_eq!(buffered.status, 502);
        assert_eq!(streamed.status, 502);
    }

    #[test]
    fn error_metadata_alongside_choices_does_not_reject_success_documents() {
        let completion = json!({
            "id": "c1", "model": "m", "error": {"message": "nonfatal metadata"},
            "choices": [{"index": 0, "message": {"content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 3, "completion_tokens": 4}
        });
        let decoded = Completion::from_openai(completion).unwrap();
        assert_eq!(decoded.text, "ok");
        assert_eq!(decoded.finish_reason.as_deref(), Some("stop"));

        let chunk = json!({
            "id": "c2", "model": "m", "error": {"message": "nonfatal metadata"},
            "choices": [{"index": 0, "delta": {"content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 6}
        });
        let decoded = ChatChunk::from_openai(chunk).unwrap();
        assert_eq!(decoded.text, "ok");
        assert_eq!(decoded.finish_reason.as_deref(), Some("stop"));
        assert_eq!(decoded.usage.unwrap().prompt_tokens, 5);
    }

    #[test]
    fn completion_to_chunks_is_two_frames_ending_with_finish_and_tool_calls() {
        let c = Completion {
            id: "x".into(),
            model: "m".into(),
            text: "hi".into(),
            finish_reason: Some("stop".into()),
            usage: Some(Usage {
                prompt_tokens: 1,
                completion_tokens: 2,
                ..Default::default()
            }),
            tool_calls: vec![ToolCall {
                id: "call_1".into(),
                name: "lookup_weather".into(),
                arguments: r#"{"city":"Paris"}"#.into(),
            }],
            raw: None,
            reasoning: None,
            refusal: None,
        };
        let chunks = completion_to_chunks(&c);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].text, "hi");
        assert!(chunks[0].tool_calls.is_empty());

        let terminal = &chunks[1];
        assert_eq!(
            terminal.finish_reason.as_deref(),
            Some("stop"),
            "an explicit vendor reason wins"
        );
        assert_eq!(
            terminal.usage.as_ref().map(|usage| usage.completion_tokens),
            Some(2)
        );
        assert_eq!(terminal.tool_calls.len(), 1);
        assert_eq!(terminal.tool_calls[0].index, 0);
        assert_eq!(terminal.tool_calls[0].id.as_deref(), Some("call_1"));
        assert_eq!(
            terminal.tool_calls[0].name.as_deref(),
            Some("lookup_weather")
        );
        assert_eq!(terminal.tool_calls[0].arguments, r#"{"city":"Paris"}"#);

        let last: Value =
            serde_json::from_slice(terminal.raw.as_ref().expect("raw synthesized")).unwrap();
        assert_eq!(last["usage"]["completion_tokens"], 2);
        assert_eq!(last["choices"][0]["finish_reason"], "stop");
        assert_eq!(last["choices"][0]["index"], 0);
        assert_eq!(
            last["choices"][0]["delta"]["tool_calls"],
            json!([{
                "index": 0,
                "id": "call_1",
                "type": "function",
                "function": {"name": "lookup_weather", "arguments": r#"{"city":"Paris"}"#}
            }])
        );
    }

    #[test]
    fn image_part_takes_both_documented_url_forms() {
        let url = image_part("https://host/a.png", Some("high")).unwrap();
        assert_eq!(url["type"], "image_url");
        assert_eq!(url["image_url"]["url"], "https://host/a.png");
        assert_eq!(url["image_url"]["detail"], "high");
        let inline = image_part("data:image/png;base64,AANA", None).unwrap();
        assert_eq!(inline["image_url"]["url"], "data:image/png;base64,AANA");
        assert!(
            inline["image_url"].get("detail").is_none(),
            "absent, not null — the dialect's own default is auto"
        );
        // `original` is the vision guide's cross-API value; the chat
        // reference's literal list lags it. Passing it through is the
        // decision, so this pin is what stops a later "fix" to auto.
        assert_eq!(
            image_part("https://h/a.png", Some("original")).unwrap()["image_url"]["detail"],
            "original"
        );
    }

    #[test]
    fn image_part_refuses_urls_no_dialect_documents() {
        for bad in [
            "file:///etc/passwd",
            "gs://bucket/a.png",
            "//host/a.png",
            "data:text/plain;base64,AAA",
            "data:image/png;base64,not base64!",
            "data:image/pn g;base64,AAA",
        ] {
            let e = image_part(bad, None).unwrap_err();
            assert_eq!(e.status, 400, "{bad}");
            assert!(e.message.contains("image url"), "{bad}: got {}", e.message);
        }
        assert_eq!(
            image_part("https://h/a.png", Some("tiny"))
                .unwrap_err()
                .status,
            400
        );
    }

    #[test]
    fn data_url_composes_for_images_and_documents() {
        assert_eq!(
            data_url("image/jpeg", "/9j+ABC=").unwrap(),
            "data:image/jpeg;base64,/9j+ABC="
        );
        // Documents compose the same way — this is the inline `file_data`
        // form Chat Completions documents.
        assert_eq!(
            data_url("application/pdf", "JVBER").unwrap(),
            "data:application/pdf;base64,JVBER"
        );
        // A payload that is itself a data URL would compose a URL whose
        // payload is somebody else's URL.
        assert_eq!(
            data_url("image/png", "data:image/png;base64,AAA")
                .unwrap_err()
                .status,
            400
        );
        // The url-safe alphabet is refused, not transcoded.
        assert_eq!(data_url("image/png", "AA_-_1").unwrap_err().status, 400);
        // No subtype at all is not a media type.
        assert_eq!(data_url("pdf", "AAA").unwrap_err().status, 400);
    }

    #[test]
    fn file_part_keeps_the_clients_name_or_supplies_a_neutral_one() {
        let named = file_part(Some("report.pdf"), "data:application/pdf;base64,JVBER").unwrap();
        assert_eq!(named["type"], "file");
        assert_eq!(named["file"]["filename"], "report.pdf");
        assert_eq!(
            named["file"]["file_data"],
            "data:application/pdf;base64,JVBER"
        );

        // Anthropic's document block has no name field, so a derived label
        // fills the slot OpenAI's examples always pair with the data.
        assert_eq!(
            file_part(None, "data:application/pdf;base64,JVBER").unwrap()["file"]["filename"],
            "document.pdf"
        );
        assert_eq!(
            file_part(None, "data:text/plain;base64,SGk=").unwrap()["file"]["filename"],
            "document.txt"
        );
        // A blank name is no name.
        assert_eq!(
            file_part(Some("  "), "data:image/png;base64,AAA").unwrap()["file"]["filename"],
            "document.bin"
        );
    }

    #[test]
    fn file_part_refuses_a_payload_that_is_not_inline_data() {
        for bad in [
            "https://host/a.pdf",
            "data:application/pdf;utf8,AAAA",
            "data:application/pdf;base64,n0t!",
            "data:application//;base64,AAAA",
            // Pure padding is not a payload.
            "=",
            "==",
            "data:image/png;base64,==",
        ] {
            let e = file_part(Some("a.pdf"), bad).unwrap_err();
            assert_eq!(e.status, 400, "{bad}");
            assert!(e.message.contains("file_data"), "{bad}: got {}", e.message);
        }
        // The same guard fronts the image path and the composer, since all
        // three share one alphabet check.
        assert_eq!(
            image_part("data:image/png;base64,=", None)
                .unwrap_err()
                .status,
            400
        );
        assert_eq!(data_url("application/pdf", "==").unwrap_err().status, 400);
    }

    /// The grammar guard, not the happy path: a `;` is BOTH the data-URL
    /// terminator and a legal mime parameter separator, so a parameterized
    /// media type is the shape most likely to slip through a loosened check —
    /// and it is exactly what breaks data-URL parsing vendor-side.
    #[test]
    fn data_url_refuses_a_parameterized_media_type() {
        assert_eq!(
            data_url("text/plain;charset=utf-8", "AAA")
                .unwrap_err()
                .status,
            400
        );
        assert_eq!(data_url("image/png#frag", "AAA").unwrap_err().status, 400);
        assert_eq!(data_url("image/png x", "AAA").unwrap_err().status, 400);
        // Hyphenated and dotted subtypes ARE legal, and the composition must
        // carry the payload — compared exactly, since a prefix check would
        // pass on a string that forgot to append anything.
        let composed = data_url(
            "application/vnd.openxmlformats-officedocument.spreadsheetml-sheet",
            "AAA",
        )
        .unwrap();
        assert_eq!(
            composed,
            "data:application/vnd.openxmlformats-officedocument.spreadsheetml-sheet;base64,AAA"
        );
    }

    /// Raw base64 with no data-URL wrapper is the OTHER form OpenAI's guide
    /// shows, so it forwards — but the payload then names nothing, so the
    /// client must supply the `filename` its own examples always pair with it.
    /// An unnamed raw payload is refused rather than turned into a body no
    /// OpenAI sample shows.
    #[test]
    fn raw_base64_file_data_needs_the_clients_name() {
        let named = file_part(Some("x.pdf"), "JVBERi0xLjUK").unwrap();
        assert_eq!(named["file"]["file_data"], "JVBERi0xLjUK");
        assert_eq!(named["file"]["filename"], "x.pdf");

        let nameless = file_part(None, "JVBERi0xLjUK").unwrap_err();
        assert_eq!(nameless.status, 400);
        assert!(
            nameless.message.contains("filename"),
            "got {}",
            nameless.message
        );
        assert_eq!(
            file_part(None, "data:application/pdf;base64,JVBERi0xLjUK").unwrap()["file"]["filename"],
            "document.pdf",
            "a media type in the payload makes the derivation honest"
        );
        // Padding shape: at most two `=`, and only at the end. Unpadded
        // payloads stay legal on purpose (a `% 4` rule would 400 a working
        // request), so this is the only structural claim the check can make.
        for bad in ["AAA===", "AA=AA"] {
            assert_eq!(
                file_part(Some("x.pdf"), bad).unwrap_err().status,
                400,
                "{bad}"
            );
        }
        assert!(file_part(Some("x.pdf"), "SGk=").is_ok(), "one pad is fine");
    }

    /// The collapse rule is CONTENT, not length and not an image-shaped
    /// predicate: text-only turns keep folding to the joined string every
    /// provider and pinned assertion already expects, and ANY non-text part —
    /// image, file, or one this dialect has not grown yet — earns the array.
    #[test]
    fn parts_content_collapse_is_decided_by_media() {
        assert_eq!(parts_content(vec![text_part("hi")]), json!("hi"));
        assert_eq!(
            parts_content(vec![text_part("a"), text_part("b")]),
            json!("a\nb")
        );
        let two = parts_content(vec![
            text_part("hi"),
            image_part("https://h/a.png", None).unwrap(),
        ]);
        assert_eq!(two.as_array().map(Vec::len), Some(2));
        assert_eq!(two[1]["image_url"]["url"], "https://h/a.png");
        // A media-only turn keeps the array form even with one part; a fold
        // never reaches this with nothing, but the function must not invent
        // a string for it either.
        assert_eq!(
            parts_content(vec![image_part("https://h/a.png", None).unwrap()]).as_array(),
            Some(&vec![image_part("https://h/a.png", None).unwrap()])
        );
        // The false-green case: a text part plus a FILE part. An
        // image-shaped predicate would join the text and drop the document,
        // which is the exact silent loss the collapse rule exists to prevent.
        let with_file = parts_content(vec![
            text_part("summarize"),
            file_part(Some("a.pdf"), "data:application/pdf;base64,JVBER").unwrap(),
        ]);
        assert_eq!(with_file.as_array().map(Vec::len), Some(2));

        assert_eq!(with_file[1]["file"]["filename"], "a.pdf");
        // A document earns the array form exactly like an image does.
        assert_eq!(
            parts_content(vec![
                text_part("summarize"),
                file_part(None, "data:application/pdf;base64,JVBER").unwrap()
            ])
            .as_array()
            .map(Vec::len),
            Some(2)
        );
        assert_eq!(
            parts_content(vec![text_part("a"), text_part("b")]),
            json!("a\nb"),
            "text alone still collapses"
        );
    }

    /// The reader is the builders' inverse, not a second guess at the shape.
    /// A provider rendering the IR into a vendor's OWN media blocks reads it
    /// back through `media_of`; if that read missed anything a builder wrote,
    /// the media would silently vanish on the way out — the same failure the
    /// builders exist to prevent, mirrored.
    #[test]
    fn every_built_media_part_reads_back_unchanged() {
        let built = vec![
            image_part("https://host/a.png", Some("high")).unwrap(),
            image_part("data:image/png;base64,AANA", None).unwrap(),
            file_part(Some("report.pdf"), "data:application/pdf;base64,JVBER").unwrap(),
            file_ref_part("file-abc"),
        ];
        let parts = Value::Array(built.clone());
        let read = media_of(&parts);
        assert_eq!(
            read,
            vec![
                Media::Image {
                    url: "https://host/a.png",
                    detail: Some("high"),
                },
                Media::Image {
                    url: "data:image/png;base64,AANA",
                    detail: None,
                },
                Media::Document {
                    filename: Some("report.pdf"),
                    data: "data:application/pdf;base64,JVBER",
                },
                Media::FileRef { id: "file-abc" },
            ],
            "what the builder wrote must be exactly what the reader sees"
        );
        // A data URL is splittable, which is how a vendor with no URL source
        // (Anthropic's `source.base64`, Gemini's `inlineData`) gets its mime.
        assert_eq!(
            data_url_parts("data:image/png;base64,AANA"),
            Some(("image/png", "AANA"))
        );
        // The count is the same walk, and text never counts as media.
        let messages = vec![json!({"role": "user", "content": built})];
        assert_eq!(media_counts(&messages), [2, 2]);
        assert_eq!(
            media_counts(&[json!({"role": "user", "content": "plain string"})]),
            [0, 0],
            "the string form carries no parts"
        );
    }
}
