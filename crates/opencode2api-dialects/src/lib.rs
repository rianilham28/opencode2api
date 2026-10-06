//! # opencode2api-dialects — the inbound half of the proxy
//!
//! One crate, one module per client dialect. Each module owns exactly three
//! things for its dialect: `parse_request` (wire -> IR), a buffered renderer
//! (IR -> that dialect's document), and a stream state machine
//! (`start`/`on_chunk`/`finish`/`fail`). Nothing here performs I/O, and
//! nothing here may name a vendor.
//!
//! **The boundary that matters is the dependency direction, not the crate
//! count.** A dialect must never learn which upstream answered it, and a
//! provider must never learn which dialect asked — that holds because this
//! crate depends only on `opencode2api-kit` and can no more see the service crate
//! than it could when each dialect had a crate of its own. What the split
//! cost instead was ceremony: adding a dialect meant adding a crate.
//!
//! Two rules every module here keeps, and any new one must:
//!
//! 1. **Reject, never degrade.** A request carrying something the fold cannot
//!    represent (audio, `thinking`, `reasoning` items, media outside a user
//!    turn or returned BY a tool, a hosted-`url` document) answers
//!    `Err(400)`. Silently dropping it would answer from a conversation that
//!    did not happen. Text, function tools and user-turn media — images AND
//!    documents — DO fold, as far as the chat dialect can say them, which is
//!    why media parts ride `opencode2api_kit::{image_part, file_part, data_url}` and
//!    never a private shape per module.
//! 2. **Never promise what was not emitted.** A terminal `tool_use` /
//!    `function_call` tells the client that tool blocks are present, so it is
//!    reported from what this bridge actually wrote — an upstream claiming
//!    `tool_calls` while sending none still ends the turn plainly.
//!
//! Adding a dialect: a module here, then exactly five touches in
//! `opencode2api-server` (`InboundFormat` variant + label, route, handler, buffered
//! render arm, error envelope arm, `Sink` arm). The relay loop, retry, SSE
//! framing, admission and telemetry are never edited for a dialect — if you
//! are editing them, the dialect is being modelled in the wrong place.

pub mod anthropic;
pub mod chat;
pub mod gemini;
pub mod responses;
