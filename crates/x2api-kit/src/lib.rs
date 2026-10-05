//! # x2api-kit
//!
//! The shared, framework-agnostic surface every x2api service reuses.
//!
//! **Doctrine**: this crate
//! never depends on axum, tower, or tower-http. `IntoResponse` impls and
//! envelope *rendering* live in `x2api-server`. `reqwest` is allowed here
//! because `wire::UpstreamResponse` is the transport-neutral envelope every
//! provider hands back — keeping it here is what lets `x2api-server` retry
//! without naming a vendor.
//!
//! Crate map:
//! - `wire`    — leaf types shared by providers and the server (`ChunkStream`).
//! - `types`   — the internal representation: OpenAI-shaped chat IR.
//! - `provider`— THE seam. A new "X-to-2api" service implements exactly this.
//! - `errors`  — one `ProviderError` taxonomy; safe-client-message rules live
//!   here so no format renderer can drift from it.
//! - `sse`     — memchr-based SSE framing (decode upstream, frame downstream).
//! - `backoff` — full-jitter retry delay on splitmix64 (no `rand`, no clock
//!   syscall per retry).
//! - `config` / `telemetry` / `util` — the boring-but-load-bearing
//!   plumbing (server config, tracing+metrics init, and shared helpers).

pub mod backoff;
pub mod config;
pub mod errors;
pub mod pool;
pub mod provider;
pub mod sse;
pub mod telemetry;
#[cfg(test)]
mod testenv;
#[cfg(test)]
mod testlog;
pub mod types;
pub mod util;
pub mod wire;

pub use config::{
    DIALECT_CHAT, DIALECT_GENERATE_CONTENT, DIALECT_LABELS, DIALECT_MESSAGES, DIALECT_RESPONSES,
    ENV_CONFIG, ServerConfig, load_dotenv, load_env_file,
};
pub use errors::ProviderError;
pub use provider::{
    CallContext, ChatStream, Dialect, Provider, RawFrames, RawReply, relayable_headers,
};
pub use types::{
    ChatChunk, ChatRequest, Completion, Media, ToolCall, ToolCallDelta, Usage, data_url,
    data_url_parts, file_part, file_ref_part, image_part, media_counts, media_of, parts_content,
    text_part, usage_from_frame,
};
pub use wire::{ChunkStream, UpstreamResponse};
