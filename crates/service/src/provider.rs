//! CUSTOMIZATION LAYER — everything vendor-shaped lives in this file, in
//! code. Four seams, each a function you can edit without touching anything
//! else: `decorate` (auth/headers), `to_upstream_body` (request translation),
//! `OpenAiProvider::complete` (buffered parse), `decode_upstream_sse`
//! (stream translation).
//!
//! Compliance facts baked in (source-verified 2026-09-06 reports):
//! - OpenAI: `max_tokens` deprecated for reasoning models in favor of
//!   `max_completion_tokens`; usage needs `stream_options.include_usage`.
//! - DeepSeek: keeps `max_tokens`; adds `thinking`/`reasoning_content` —
//!   passes through untouched inside `raw` (OpenAI-dialect clients see it).
//! - GLM: its OpenAI-compatible endpoint is rooted at `/api/paas/v4`, not
//!   DeepSeek's `/v1` form.
//! - Both: `Authorization: Bearer` static keys -> client-key passthrough is
//!   viable; server-side key optional.

use async_stream::{stream, try_stream};
use futures_util::{StreamExt, TryStreamExt};
use http::header;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::{Duration, Instant};

use x2api_kit::{
    CallContext, ChatChunk, ChatRequest, ChatStream, ChunkStream, Completion, Provider,
    ProviderError, ToolCall, ToolCallDelta, UpstreamResponse, Usage, pool::CooldownReason,
};
use x2api_transport::{Lane, Transport};

use crate::ServiceConfig;

/// Clean upstream EOF confirms lane health; an error or client abort only counts
/// as use. A stalled downstream client may leave this guard pending, allowing
/// pool maintenance to replace only the lane entry while already-open body
/// bytes continue independently on their connection.
struct TouchOnDrop {
    lane: Arc<Lane>,
    completed: bool,
    transport_error: bool,
}

impl TouchOnDrop {
    fn complete(&mut self) {
        self.completed = true;
    }

    fn transport_error(&mut self) {
        self.transport_error = true;
    }

    fn classify_body_error(&self, error: &ProviderError) -> bool {
        // Gate read failures carry an explicit marker; other transport errors
        // retain reqwest's retryable 502 classification, unlike local verdicts.
        error.is_body_truncated()
            || (error.is_retryable() && error.message.starts_with("upstream request failed:"))
    }
}

impl Drop for TouchOnDrop {
    fn drop(&mut self) {
        if self.transport_error {
            // Failure wins even after a false-positive terminal signal.
            self.lane.note_failure();
        } else if self.completed {
            self.lane.note_success();
        } else {
            // Client aborts, local verdicts, and header timeouts record use
            // without a health claim. The touch on a timeout is deliberate: the
            // request occupied the lane for the full read timeout and burned
            // that much of the session's TTL, so the lane is not idle — and an
            // untouched lane behind a slow vendor would be retired as idle and
            // replaced with a session bought from that same slow vendor.
            // `note_slow` at the call site is the whole of what the timeout is
            // charged to.
            self.lane.note_used();
        }
        // Record the outcome while maintenance still sees this lane in flight.
        self.lane.end_use();
    }
}

/// The verbatim lane's byte-precise DONE-trim, as a pure sync state machine:
/// forward everything up to and including `data: [DONE]` + its record
/// terminator (`\r?\n\r?\n` once present), drop everything after — opencode
/// appends a `{"choices":[],"cost":"0"}` frame after `[DONE]` and no client
/// may ever see it. While searching, at most `marker.len()-1` tail bytes are
/// held (they might START a marker); once the terminator lands, every later
/// byte is dropped, in the same push and in all later ones.
const DONE_MARKER: &[u8] = b"data: [DONE]";

/// The accepted forms of the DONE record's terminator, `\r?\n\r?\n`.
const DONE_TERMINATORS: [&[u8]; 4] = [b"\n\n", b"\r\n\n", b"\n\r\n", b"\r\n\r\n"];

#[derive(Clone, Copy, PartialEq, Eq)]
enum TrimPhase {
    /// Still looking for the marker; `held` is the tail that might start one.
    Searching,
    /// Marker seen; `held` is the marker plus whatever of the terminator
    /// arrived, waiting for the record boundary (or an EOF flush).
    InDone,
    /// Past the terminator: `held` empty, everything dropped.
    Dropped,
}

#[derive(PartialEq, Eq)]
enum TerminatorState {
    /// So far a prefix of some accepted form — keep waiting.
    Prefix,
    Complete,
    /// A byte that ends no accepted form: it is post-marker content, not a
    /// boundary.
    Diverged,
}

fn terminator_state(term: &[u8]) -> TerminatorState {
    if DONE_TERMINATORS.contains(&term) {
        TerminatorState::Complete
    } else if DONE_TERMINATORS.iter().any(|form| form.starts_with(term)) {
        TerminatorState::Prefix
    } else {
        TerminatorState::Diverged
    }
}

fn find_marker(hay: &[u8]) -> Option<usize> {
    // `windows` yields nothing (never panics) when hay is shorter than the
    // marker — an incomplete tail is exactly what the hold-back is for.
    hay.windows(DONE_MARKER.len())
        .position(|window| window == DONE_MARKER)
}

/// How many trailing bytes of `hay` are a prefix of the marker: the only
/// bytes that must be held while searching. Bounded by `marker.len()-1`.
fn marker_overlap(hay: &[u8]) -> usize {
    let max = DONE_MARKER.len().saturating_sub(1).min(hay.len());
    (1..=max)
        .rev()
        .find(|&len| hay[hay.len() - len..] == DONE_MARKER[..len])
        .unwrap_or(0)
}

struct DoneTrim {
    phase: TrimPhase,
    held: Vec<u8>,
}

impl DoneTrim {
    fn new() -> Self {
        Self {
            phase: TrimPhase::Searching,
            held: Vec::with_capacity(DONE_MARKER.len() + DONE_TERMINATORS[3].len()),
        }
    }

    /// One upstream chunk in, the bytes to forward NOW out. `None` is hold
    /// or drop — the trim never invents a byte either way.
    fn push(&mut self, chunk: bytes::Bytes) -> Option<bytes::Bytes> {
        match self.phase {
            TrimPhase::Dropped => None,
            TrimPhase::InDone => self.feed_terminator(&chunk),
            TrimPhase::Searching if self.held.is_empty() => {
                // The zero-copy path: no boundary straddles this chunk, so
                // the input buffer itself goes out as-is.
                if let Some(at) = find_marker(&chunk) {
                    let pre = (at > 0).then(|| chunk.slice(0..at));
                    self.begin_done();
                    let term = self.feed_terminator(&chunk[at + DONE_MARKER.len()..]);
                    return Self::join(pre, term);
                }
                let keep = marker_overlap(&chunk);
                if keep == 0 {
                    return Some(chunk);
                }
                let split = chunk.len() - keep;
                self.held.extend_from_slice(&chunk[split..]);
                (split > 0).then(|| chunk.slice(0..split))
            }
            TrimPhase::Searching => {
                // A hold is pending: the boundary may straddle it, so this
                // one chunk crosses the seam and gets copied — the only
                // per-stream copy on the pass-through path.
                let mut scratch = std::mem::take(&mut self.held);
                scratch.extend_from_slice(&chunk);
                if let Some(at) = find_marker(&scratch) {
                    let pre = (at > 0).then(|| bytes::Bytes::copy_from_slice(&scratch[..at]));
                    self.begin_done();
                    let term = self.feed_terminator(&scratch[at + DONE_MARKER.len()..]);
                    return Self::join(pre, term);
                }
                let keep = marker_overlap(&scratch);
                let split = scratch.len() - keep;
                self.held = scratch.split_off(split);
                (split > 0).then(|| bytes::Bytes::copy_from_slice(&scratch))
            }
        }
    }

    /// The DONE record starts here: hold the marker, then measure the
    /// terminator against whatever follows it.
    fn begin_done(&mut self) {
        self.held.clear();
        self.held.extend_from_slice(DONE_MARKER);
        self.phase = TrimPhase::InDone;
    }

    /// Feed bytes that follow the marker; returns the flushed record once
    /// its terminator lands (or diverges — then what was held minus the
    /// breaking byte goes out and the rest is post-marker content).
    fn feed_terminator(&mut self, incoming: &[u8]) -> Option<bytes::Bytes> {
        for &byte in incoming {
            self.held.push(byte);
            match terminator_state(&self.held[DONE_MARKER.len()..]) {
                TerminatorState::Prefix => {}
                TerminatorState::Complete => {
                    self.phase = TrimPhase::Dropped;
                    return Some(bytes::Bytes::from(std::mem::take(&mut self.held)));
                }
                TerminatorState::Diverged => {
                    self.held.pop();
                    self.phase = TrimPhase::Dropped;
                    return Some(bytes::Bytes::from(std::mem::take(&mut self.held)));
                }
            }
        }
        None
    }

    fn join(pre: Option<bytes::Bytes>, done: Option<bytes::Bytes>) -> Option<bytes::Bytes> {
        match (pre, done) {
            (Some(pre), Some(done)) => {
                let mut joined = bytes::BytesMut::with_capacity(pre.len() + done.len());
                joined.extend_from_slice(&pre);
                joined.extend_from_slice(&done);
                Some(joined.freeze())
            }
            (Some(pre), None) => Some(pre),
            (None, done) => done,
        }
    }

    /// EOF: flush whatever is held. A DONE record without its terminator is
    /// still forwarded — the downstream decoder's `finish()` tolerates it —
    /// and nothing after the marker ever was.
    fn finish(&mut self) -> Option<bytes::Bytes> {
        self.phase = TrimPhase::Dropped;
        (!self.held.is_empty()).then(|| bytes::Bytes::from(std::mem::take(&mut self.held)))
    }
}

/// Watch the relayed stream for the vendor's terminal record without altering
/// a single forwarded byte. Completion is deliberately `done_seen`-only: a
/// clean EOF is indistinguishable from a downstream cut mid-stream, the same
/// doctrine as W6a's missing-`[DONE]` truncation rule, so EOF stays use-only.
/// Fed the TRIMMED bytes — what the client actually receives — so the lane's
/// verdict describes the stream the client saw.
fn observe_terminal(
    decoder: &mut x2api_kit::sse::SseDecoder,
    events: &mut Vec<x2api_kit::sse::SseEvent>,
    bytes: &[u8],
    touch: &mut TouchOnDrop,
) {
    // Decoder errors are local framing/ceiling verdicts: bytes arrived through
    // a working tunnel, so the discard deliberately leaves the guard's Drop
    // default (`note_used`). Only ChunkStream errors elsewhere are lane faults.
    let _ = decoder.push_into(bytes, events);
    events.clear();
    if decoder.done_seen() {
        touch.complete();
    }
}

/// The verbatim chat-stream lane: upstream chunks in, TRIMMED bytes out —
/// everything through the DONE record verbatim, the cost frame and anything
/// after it dropped at the source. Lane accounting mirrors the fold:
/// transport errors charge the lane, a DONE seen in what was forwarded
/// completes it, EOF without one stays use-only.
fn touch_when_stream_ends(
    resp: UpstreamResponse,
    max: usize,
    mut touch: TouchOnDrop,
) -> ChunkStream {
    let mut upstream = resp.into_chunks();
    stream! {
        let mut trim = DoneTrim::new();
        let mut terminal = x2api_kit::sse::SseDecoder::with_max_record(max);
        let mut events = Vec::new();
        while let Some(chunk) = upstream.next().await {
            match chunk {
                Ok(bytes) => {
                    if let Some(forwarded) = trim.push(bytes) {
                        observe_terminal(&mut terminal, &mut events, &forwarded, &mut touch);
                        yield Ok(forwarded);
                    }
                }
                Err(error) => {
                    touch.transport_error();
                    yield Err(error);
                }
            }
        }
        if let Some(tail) = trim.finish() {
            observe_terminal(&mut terminal, &mut events, &tail, &mut touch);
            yield Ok(tail);
        }
        if terminal.done_seen() {
            touch.complete();
        }
        drop(touch);
    }
    .boxed()
}

/// The opencode client identity presented on EVERY request: upstream's
/// free-tier gate 403s unless its headers are present with this exact shape.
/// Every value here is built ONCE at `new()` — request time only clones
/// `HeaderValue`s (a refcount bump, no parse, no validation). The minted
/// pair rolls ONCE per provider so a served process reads as one long-lived
/// client session instead of a new one per call. The user-agent is NOT
/// minted here — it is read live at request time, because it follows the
/// published release.
struct OpenCodeProfile {
    client: http::HeaderValue,
    project: http::HeaderValue,
    session: http::HeaderValue,
    /// The affinity/id copies are separate `HeaderValue`s on purpose: the
    /// request path clones a stored value rather than re-deriving it from
    /// the session under a lock.
    affinity: http::HeaderValue,
    session_id: http::HeaderValue,
}

/// The 2.x agent-string shape, captured from opencode 2.0.18. Only the
/// starting point: [`LIVE_UA`] follows the published release from boot, so
/// this goes stale without the proxy doing so.
const OPENCODE_UA: &str = "opencode/latest/2.0.18/cli";

/// The CLI's own update check: unauthenticated, and answers with the release
/// every up-to-date client announces — `{"version":"2.0.22",…}`.
const CLI_RELEASE_URL: &str = "https://opencode.ai/update/api/latest/cli/npm";

/// How long any release lookup may take before the current agent string is
/// kept instead. Boot and the routine cadence must never stall on a slow
/// endpoint; a 426 is already paying for one refused round trip.
const RELEASE_LOOKUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// The agent string in force, stored WITH its parsed header: the request
/// path clones a value that was validated exactly once — at adopt time —
/// instead of parsing on every call. Read on every request, written only
/// when it moves, so the lock is uncontended in practice.
struct LiveUa {
    /// The string, for logs and operator-facing messages.
    text: String,
    /// Parsed ONCE, at construction or adopt — never per request.
    header: http::HeaderValue,
}

impl LiveUa {
    fn pinned() -> Self {
        Self {
            text: OPENCODE_UA.to_string(),
            // `from_static` is a const constructor: zero runtime validation.
            header: http::HeaderValue::from_static(OPENCODE_UA),
        }
    }
}

static LIVE_UA: LazyLock<RwLock<LiveUa>> = LazyLock::new(|| RwLock::new(LiveUa::pinned()));

/// Unix second before which the catalogue-cadence release refresh is skipped:
/// one lookup per minute, and the first attempt to claim the slot pays while
/// concurrent callers see a future deadline and skip.
static NEXT_RELEASE_REFRESH: AtomicU64 = AtomicU64::new(0);

/// The release lookup rides its own client: the update endpoint is a public,
/// unauthenticated surface unrelated to the proxied vendor lane — borrowing a
/// lane would charge pool accounting for a version check.
static RELEASE_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

fn unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The agent string to send, read live at request time.
fn live_ua() -> String {
    LIVE_UA
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .text
        .clone()
}

/// The already-parsed agent header: a clone, never a parse.
fn live_ua_header() -> http::HeaderValue {
    LIVE_UA
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .header
        .clone()
}

/// The first `major.minor[.patch…]` triple in `s`. Extra segments past the
/// triple are accepted — the release endpoint's own shape is what matters —
/// and anything unreadable is `None`, so a caller keeps what it has.
fn parse_version_triple(s: &str) -> Option<(u32, u32, u32)> {
    let mut parts = s.trim().split('.');
    Some((
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
    ))
}

/// The update endpoint's top-level `version` field as a semver triple.
fn parse_release_version(json: &str) -> Option<(u32, u32, u32)> {
    let value: Value = serde_json::from_str(json).ok()?;
    parse_version_triple(value.get("version")?.as_str()?)
}

/// `live` moved to `candidate`, rendered in the 2.x profile shape — but ONLY
/// when strictly newer. A stale, older or equal candidate, or an unreadable
/// live string, is `None`: adoption never moves backward and never guesses.
fn adopt_version(live: &str, candidate: (u32, u32, u32)) -> Option<String> {
    let current = live
        .strip_prefix("opencode/latest/")?
        .split('/')
        .next()
        .and_then(parse_version_triple)?;
    (candidate > current).then(|| {
        let (major, minor, patch) = candidate;
        format!("opencode/latest/{major}.{minor}.{patch}/cli")
    })
}

/// Claim the next release-refresh slot for `now`: whoever's CAS lands first
/// owns the minute, everyone else sees a future deadline and skips.
fn claim_release_refresh(now: u64) -> bool {
    let mut seen = NEXT_RELEASE_REFRESH.load(Ordering::Relaxed);
    loop {
        if now < seen {
            return false;
        }
        match NEXT_RELEASE_REFRESH.compare_exchange(
            seen,
            now + 60,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(current) => seen = current,
        }
    }
}

/// Move the live agent string to `candidate` when strictly newer. Returns
/// whether it moved; a failed or stale adoption changes nothing.
fn adopt_release(candidate: (u32, u32, u32)) -> bool {
    let mut live = LIVE_UA
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(moved) = adopt_version(&live.text, candidate) else {
        return false;
    };
    // Rendered from u32 fields — header-safe by construction; the parse
    // happens HERE, once, so no request ever pays for it.
    let header = http::HeaderValue::from_str(&moved).expect("rendered agent string is header-safe");
    let was = std::mem::replace(
        &mut *live,
        LiveUa {
            text: moved,
            header,
        },
    );
    tracing::info!(
        from = %was.text,
        to = %live.text,
        "agent string follows the published cli release"
    );
    true
}

/// The published CLI release as the update endpoint answers it: an
/// unauthenticated GET announcing the CURRENT live agent string, parsed from
/// the body's top-level `version`. `None` on anything unreachable or
/// unreadable, so a caller keeps what it has.
async fn fetch_release_version() -> Option<(u32, u32, u32)> {
    let response = RELEASE_CLIENT
        .get(CLI_RELEASE_URL)
        .header(header::USER_AGENT, live_ua_header())
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?;
    parse_release_version(&response.text().await.ok()?)
}

/// One bounded release lookup + adoption: failure, timeout, or an older
/// published release all warn and keep the agent string in force.
async fn refresh_release_version() {
    match tokio::time::timeout(RELEASE_LOOKUP_TIMEOUT, fetch_release_version()).await {
        Ok(Some(candidate)) => {
            adopt_release(candidate);
        }
        Ok(None) => tracing::warn!(
            live = %live_ua(),
            "could not read the published opencode cli release; keeping the current agent string"
        ),
        Err(_) => tracing::warn!(
            live = %live_ua(),
            "opencode cli release lookup timed out; keeping the current agent string"
        ),
    }
}

/// Answer a 426 by announcing the published floor to SUBSEQUENT requests:
/// fetch + adopt immediately, bypassing the catalogue throttle — this is the
/// escalation path — then log both versions so the refusal is traceable.
/// The REFUSED request itself still fails: the template derives retryability
/// from the status in kit, 426 is not retryable, and that classification is
/// deliberately untouched. This branch only moves the string forward for the
/// requests after it.
async fn announce_version_floor() {
    let refused = live_ua();
    refresh_release_version().await;
    let live = live_ua();
    tracing::warn!(
        refused = %refused,
        live = %live,
        "upstream refused the agent version (426)"
    );
}

/// Last `millis << 12 | counter` handed out, so values minted inside the
/// same millisecond still differ — and still ascend — in their ordered half.
fn next_ordered() -> u64 {
    static ID_CLOCK: AtomicU64 = AtomicU64::new(0);
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut prev = ID_CLOCK.load(Ordering::Relaxed);
    loop {
        // A clock that went backwards keeps counting off the last value rather
        // than reissuing ids that were already handed out.
        let next = if millis > (prev >> 12) {
            (millis << 12) | 1
        } else {
            prev + 1
        };
        match ID_CLOCK.compare_exchange_weak(prev, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(reloaded) => prev = reloaded,
        }
    }
}

/// splitmix64: dependency-free, well-distributed mixing for the random
/// halves of minted ids and the project hex.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The session id, in the exact shape the gate's regex accepts:
/// `ses_` + 12 hex + 14 base62. The ordered half is INVERTED, which is how
/// opencode mints sessions — the newest sorts first among its siblings.
fn mint_opencode_session() -> http::HeaderValue {
    const BASE62: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    const MASK: u64 = 0xFFFF_FFFF_FFFF;
    let ordered = next_ordered() & MASK;
    let ordered = !ordered & MASK;
    let mut id = format!("ses_{ordered:012x}");
    // Seeded from the ordered half, so two mints never share a tail even
    // within one millisecond.
    let mut seed = ordered;
    for _ in 0..14 {
        seed = splitmix64(seed);
        id.push(BASE62[(seed % BASE62.len() as u64) as usize] as char);
    }
    http::HeaderValue::from_str(&id).expect("hex and base62 are header-legal")
}

/// The project id: 40 hex chars, four splitmix64 words (10 hex digits each)
/// seeded from the ordered clock plus this process's pid — distinct per mint
/// within a process, distinct across processes.
fn mint_opencode_project() -> http::HeaderValue {
    let mut seed = next_ordered().wrapping_add(u64::from(std::process::id()));
    let mut id = String::with_capacity(40);
    for _ in 0..4 {
        seed = splitmix64(seed);
        // Low 40 bits of each word, printed as exactly 10 hex digits: four
        // words sum to the 40-character header the vendor expects.
        id.push_str(&format!("{:010x}", seed & 0xF_FFFF_FFFF));
    }
    http::HeaderValue::from_str(&id).expect("hex is header-legal")
}

pub struct OpenAiProvider {
    transport: Arc<Transport>,
    cfg: ServiceConfig,
    /// Client-facing alias lookup, prebuilt because responses and catalogue
    /// requests cross this seam in the hot path.
    reverse_model_map: Arc<HashMap<String, String>>,
    /// Endpoints resolved ONCE. `reqwest` parses and validates a URL on every
    /// `post(&str)`; building the string first would add two allocations
    /// and a parse per request, for a value that cannot change after boot.
    /// Cloning a parsed `Url` copies its buffer and skips the parse; a
    /// malformed `base_url` fails at boot, not every request identically
    /// at runtime.
    chat_url: reqwest::Url,
    /// The Responses-API endpoint: the muse-spark free models answer ONLY
    /// this dialect (a chat call on them 500s upstream), so it resolves from
    /// the same normalized base as `chat_url` at boot.
    responses_url: reqwest::Url,
    models_url: reqwest::Url,
    /// The client identity every request presents, minted once here rather
    /// than per call — see `OpenCodeProfile`.
    opencode: OpenCodeProfile,
    /// Upstream credentials, in slot order. The POOL holds indices, never keys
    /// (`x2api_kit::pool`), so the thing that gets logged or Debug-formatted
    /// cannot carry a credential.
    keys: Vec<String>,
    pool: x2api_kit::pool::Pool,
    /// Vendor models upstream answered dead, mapped to when the quiet
    /// window ends. Catalogue-side ONLY: the request path never reads it,
    /// the 400 still reaches the client verbatim, and a repeat answer
    /// refreshes the expiry.
    unavailable: Mutex<HashMap<String, Instant>>,
    /// The last raw catalogue answer and when it was fetched; see
    /// `CatalogCache`.
    catalogue_cache: Mutex<CatalogCache>,
}

impl OpenAiProvider {
    fn alias_for<'a>(&'a self, vendor_model: &'a str) -> Option<&'a str> {
        self.reverse_model_map.get(vendor_model).map(String::as_str)
    }

    fn completion_from_openai(
        &self,
        requested_model: &str,
        value: Value,
    ) -> Result<Completion, ProviderError> {
        let mut completion = Completion::from_openai(value)?;
        restore_requested_model(
            &mut completion.model,
            model_restore(requested_model, &self.cfg.model_map).as_ref(),
        );
        Ok(completion)
    }

    fn catalogue(&self, mut value: Value) -> Value {
        if self.reverse_model_map.is_empty() {
            return value;
        }
        let Some(entries) = value.get_mut("data").and_then(Value::as_array_mut) else {
            return value;
        };
        let aliases = entries
            .iter()
            .filter_map(|entry| {
                let id = entry.get("id")?.as_str()?;
                let alias = self.alias_for(id)?;
                Some((id.to_string(), alias.to_string(), entry.clone()))
            })
            .collect::<Vec<_>>();
        for (_vendor, alias, mut alias_entry) in aliases {
            if entries
                .iter()
                .any(|entry| entry.get("id").and_then(Value::as_str) == Some(alias.as_str()))
            {
                continue;
            }
            let Some(object) = alias_entry.as_object_mut() else {
                continue;
            };
            object.insert("id".into(), alias.into());
            entries.push(alias_entry);
        }
        value
    }

    /// The catalogue render pipeline over a raw answer: vendor aliases, the
    /// free-only list, then the dead-model window — pure transforms, so a
    /// cached or stale copy renders exactly like a fresh one.
    fn render_catalogue(&self, raw: Value) -> Value {
        let listed = free_only(self.catalogue(raw));
        let unavailable = self
            .unavailable
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        withhold_unavailable(listed, &self.cfg.model_map, &unavailable, Instant::now())
    }

    /// The fetch half of `models()`, extracted verbatim so the cache can
    /// fall back around it — lane accounting, credential pool, gate, and
    /// body cap are exactly what lived in the trait method.
    ///
    /// A catalogue fetch rides a pooled lane, so it REPORTS: a dead tunnel is
    /// charged to the lane, a slow vendor is counted, and every exchange that
    /// held the lane — completed, aborted, or timed out waiting for headers —
    /// touches its use clock. It still skips inflight accounting (W6d guards
    /// chat handoffs); the lane is only borrowed for the length of a short
    /// catalogue call.
    ///
    /// The timeout arm touching the use clock is the SAME rule the chat path
    /// applies in `TouchOnDrop::drop`, and for the same reason: the fetch held
    /// the lane for the full read timeout and burned that much session TTL, so
    /// the lane is not idle. A catalogue-only lane behind a slow vendor that
    /// left the clock alone would be retired as idle and re-minted from that
    /// same slow vendor.
    ///
    /// Dropping the lane here is what made this method invisible: `client()`
    /// discarded the `Arc`, so a catalogue fetch against a broken exit IP
    /// taught the pool nothing and chat kept receiving the same one.
    async fn fetch_catalogue(&self, ctx: &CallContext<'_>) -> Result<Value, ProviderError> {
        let (client, lane) = self.transport.lane_client()?;
        let req = client.get(self.models_url());
        // The same pool as chat keeps a cooling key from being probed on every
        // catalogue refresh. The configured credential remains authoritative;
        // caller credentials are not accepted on this proxy-owned catalogue.
        let (req, slot) = if self.pool.is_empty() {
            (self.with_request_id(req, ctx), None)
        } else {
            self.decorate(req, None, ctx)?
        };
        let resp = req.send().await.map_err(|e| {
            match x2api_transport::classify_send(&e) {
                x2api_transport::SendFault::Lane => lane.note_failure(),
                x2api_transport::SendFault::Slow => {
                    lane.note_slow();
                    lane.note_used();
                }
                x2api_transport::SendFault::Other => {}
            }
            ProviderError::from(e)
        })?;
        // Headers arrived: the tunnel is proven, and only the completion below
        // is left to record.
        lane.note_healthy();
        let hint = Self::retry_hint(resp.headers());
        if resp.status() == http::StatusCode::UPGRADE_REQUIRED {
            announce_version_floor().await;
        }
        let resp = x2api_kit::errors::gate_response(&self.cfg.name, Some(ctx.request_id), resp)
            .await
            .map_err(|error| match hint {
                Some(secs) => error.with_retry_after(secs.min(Self::MAX_COOLDOWN_SECS)),
                None => error,
            })
            .inspect_err(|e| self.note_credential(slot, e))?;
        let bytes = UpstreamResponse(resp)
            .body_bytes_capped(self.cfg.response_ceiling())
            .await?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|e| {
            ProviderError::permanent_bad_gateway(format!("models reply was not JSON: {e}"))
        })?;
        lane.note_used();
        Ok(value)
    }

    pub fn new(transport: Arc<Transport>, cfg: ServiceConfig) -> anyhow::Result<Self> {
        let mut reverse_model_map = HashMap::with_capacity(cfg.model_map.len());
        for (alias, vendor) in &cfg.model_map {
            if let Some(existing) = reverse_model_map.insert(vendor.clone(), alias.clone()) {
                anyhow::bail!(
                    "provider.model_map maps vendor model {vendor:?} to both {existing:?} and {alias:?}"
                );
            }
        }
        let base = normalized_base(&cfg.base_url);
        let chat_url = parse_endpoint(&base, "chat/completions")?;
        let responses_url = parse_endpoint(&base, "responses")?;
        let models_url = parse_endpoint(&base, "models")?;
        let session = mint_opencode_session();
        let opencode = OpenCodeProfile {
            client: http::HeaderValue::from_static("cli"),
            project: mint_opencode_project(),
            affinity: session.clone(),
            session_id: session.clone(),
            session,
        };
        // Follow the published release from boot: fire-and-forget inside the
        // runtime, so a slow or unreachable update endpoint never delays
        // serving — the first requests simply announce the pinned fallback
        // until it lands. Outside a runtime (unit tests), there is no spawn
        // and no network; nothing here may fail a boot either way.
        if let Ok(handle) = tokio::runtime::Handle::try_current()
            && claim_release_refresh(unix_seconds())
        {
            handle.spawn(refresh_release_version());
        }
        let keys = cfg.credentials();
        tracing::info!(
            provider = %cfg.name,
            chat_url = %visible_endpoint(&chat_url),
            responses_url = %visible_endpoint(&responses_url),
            "provider endpoint resolved"
        );
        Ok(Self {
            transport,
            cfg,
            chat_url,
            responses_url,
            models_url,
            opencode,
            // A pool of one is still a pool: the rotation code then has ONE
            // path instead of a special case that only the second key ever
            // exercised. Zero slots is the passthrough mode, where the pool is
            // never consulted.
            pool: x2api_kit::pool::Pool::new(keys.len()),
            keys,
            reverse_model_map: Arc::new(reverse_model_map),
            unavailable: Mutex::new(HashMap::new()),
            catalogue_cache: Mutex::new(CatalogCache::default()),
        })
    }

    /// The requested model resolved through `model_map` — the id upstream
    /// actually sees, and therefore the key the style split reads.
    fn vendor_model<'a>(&'a self, requested: &'a str) -> &'a str {
        self.cfg
            .model_map
            .get(requested)
            .map_or(requested, String::as_str)
    }

    /// Which dialect this vendor id is answered in: the muse-spark free
    /// models answer only `/responses`, everything else the chat endpoint.
    fn upstream_url(&self, vendor: &str) -> reqwest::Url {
        if is_responses_style(vendor) {
            self.responses_url.clone()
        } else {
            self.chat_url.clone()
        }
    }

    fn models_url(&self) -> reqwest::Url {
        self.models_url.clone()
    }

    /// The cheapest endpoint this upstream serves, for warming an egress
    /// lane. Comes from the SAME normalized base the real calls use, so a
    /// probe can never drift into a 404 the pool then reports as healthy.
    pub fn probe_url(&self) -> &reqwest::Url {
        &self.models_url
    }

    // ── credential resolution (code, not config) ─────────────────────────
    /// Which vendor answer means "spend this credential", and for how long.
    ///
    /// `429` is the quota case, and the vendor's own `Retry-After` beats any
    /// guess we could make. `401`/`403` are included because a rejected key is
    /// worse than a rate-limited one — it fails everything until a human fixes
    /// it — but they cool LONGER and not forever, so a 401 that turns out to be
    /// a transient vendor problem heals itself instead of leaving a valid key
    /// permanently out of rotation.
    /// The longest a vendor hint may hold a key out of rotation — and the value
    /// stamped onto the error the CLIENT sees.
    ///
    /// Two documents agree on the number from different directions:
    /// `gate_response` refuses to copy `Retry-After` because "free-tier vendors
    /// are known to advertise hours", and `docs/compliance/openai.md` §5 lands on
    /// ~120 s as what this provider should cap at. Honouring the hint matters
    /// because it is the only answer that knows when quota returns; capping it
    /// matters because a two-key pool parked for an hour is a proxy running on one
    /// key with nobody awake to notice.
    const MAX_COOLDOWN_SECS: u64 = 120;

    /// Read the rate-limit hint off an upstream response, in the order the
    /// official SDKs read it: `retry-after-ms` FIRST (nonstandard, sub-second,
    /// and the one OpenAI actually sends), then `retry-after`, which RFC 9110
    /// allows to be EITHER seconds OR an IMF-fixdate.
    ///
    /// All three spellings are handled because they are one instruction arriving
    /// three ways. Reading only the bare-integer form is what makes a parser look
    /// like it honours the header while `retry-after-ms: 800` and
    /// `Retry-After: Fri, 31 Dec 2021 23:59:59 GMT` both fall through to the
    /// default — the exact failure mode this function exists to close, and the one
    /// this file shipped with until it was measured against a live 429.
    fn retry_hint(headers: &http::HeaderMap) -> Option<u64> {
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
        };
        if let Some(ms) = header("retry-after-ms").and_then(|v| v.parse::<f64>().ok()) {
            return Some((ms / 1000.0).ceil() as u64);
        }
        let raw = header("retry-after")?;
        if let Ok(secs) = raw.parse::<u64>() {
            return Some(secs);
        }
        // HTTP-date. `jiff::fmt::rfc2822::parse` is the entry point that accepts
        // the IMF-fixdate shape (`Retry-After: Fri, 31 Dec 2021 23:59:59 GMT`);
        // `Timestamp::from_str` is NOT — it is the RFC 3339 parser, which rejects
        // a string beginning with a weekday name, so reaching for it here would
        // leave this arm dead and the date form silently back to the default.
        let when = jiff::fmt::rfc2822::parse(raw).ok()?;
        // A date already behind us subtracts to a NEGATIVE duration — the same
        // instruction as "now", so floor it rather than let a negative reach an
        // unsigned conversion.
        let wait = when
            .timestamp()
            .duration_since(jiff::Timestamp::now())
            .as_secs();
        Some(wait.max(0) as u64)
    }

    fn cooldown(err: &ProviderError) -> Option<std::time::Duration> {
        match err.status {
            429 => Some(Duration::from_secs(err.retry_after.unwrap_or(30))),
            401 | 403 => Some(Duration::from_secs(300)),
            _ => None,
        }
    }

    /// This attempt's credential, as `(token, slot)`.
    ///
    /// The slot is `None` for a PASSTHROUGH key deliberately: when the operator
    /// configured no upstream credential and we forward the caller's own token,
    /// cooling it would punish an innocent client for someone else's rate limit
    /// — and there is no other key to move to anyway. Passthrough therefore
    /// succeeds or fails alone.
    ///
    /// `Err` is the all-cooling case, which is THIS proxy's capacity answer
    /// rather than the vendor's: a 503 carrying how long until the first slot
    /// frees.
    fn credential<'a>(
        &'a self,
        client_bearer: Option<&'a str>,
    ) -> Result<Option<(&'a str, Option<usize>)>, ProviderError> {
        use std::time::Instant;
        if !self.pool.is_empty() {
            let now = Instant::now();
            // Publishing at the moment of use is what keeps the gauge honest: a
            // cooldown that expired silently would otherwise keep being counted
            // as spent until the next failure happened to touch this code.
            return match self.pool.pick(now) {
                Some(slot) => Ok(Some((self.keys[slot].as_str(), Some(slot)))),
                None => {
                    let wait = self.pool.ready_in(now).unwrap_or(30);
                    Err(
                        ProviderError::unavailable("all upstream credentials are rate-limited")
                            .with_retry_after(wait),
                    )
                }
            };
        }
        Ok(client_bearer
            .and_then(|a| a.strip_prefix("Bearer "))
            .filter(|t| !t.is_empty())
            .map(|t| (t, None)))
    }

    /// Record what a failed attempt says about the credential that carried it,
    /// and say so ONCE per episode.
    ///
    /// The once matters: a rate-limited key is not one event but one per request,
    /// so warning on every 429 would turn a single spent credential into the
    /// loudest thing in the log — the exact volume this crate has been spending
    /// effort to shrink. `Pool::cool` answers the question that lets it be one
    /// line (was this slot ready a moment ago?), and the gauge keeps the state
    /// observable while requests are arriving — a pool idle for an hour reports
    /// what was last true of it, which is why the card says when it scraped.
    fn note_credential(&self, slot: Option<usize>, err: &ProviderError) {
        let (Some(slot), Some(for_)) = (slot, Self::cooldown(err)) else {
            return;
        };
        let reason = match err.status {
            429 => CooldownReason::Quota,
            401 | 403 => CooldownReason::Auth,
            _ => CooldownReason::Other,
        };
        if self.pool.cool(slot, for_, reason) {
            tracing::warn!(
                provider = self.cfg.name,
                slot,
                secs = for_.as_secs(),
                status = err.status,
                "upstream credential spent"
            );
        }
    }

    /// SEAM: auth, correlation, and required headers. A vendor needing
    /// `x-api-key`, JWT, or OAuth refresh grows its logic here.
    ///
    /// Returns the SLOT it authenticated with, not just the builder, and that is
    /// the point: the credential that carried a request is the credential a 429
    /// against that request has to spend. A seam returning only the builder
    /// pushes every caller to pick again, and a second pick hands back a
    /// DIFFERENT key under concurrency — the rate limit would then cool a
    /// credential that never saw it.
    fn decorate(
        &self,
        req: reqwest::RequestBuilder,
        client_bearer: Option<&str>,
        ctx: &CallContext<'_>,
    ) -> Result<(reqwest::RequestBuilder, Option<usize>), ProviderError> {
        let req = self.with_request_id(req, ctx);
        // The whole identity rides every request: upstream's gate 403s unless
        // all of these are present, the minted pair is the SAME on every call
        // so this process reads as one long-lived opencode session, and the
        // agent string is read LIVE here — a release adopted mid-flight
        // announces itself from the very next request.
        let profile = &self.opencode;
        let user_agent = live_ua_header();
        let req = req
            .header(header::USER_AGENT, user_agent)
            .header("x-opencode-client", profile.client.clone())
            .header("x-opencode-project", profile.project.clone())
            .header("x-opencode-session", profile.session.clone())
            .header("x-session-affinity", profile.affinity.clone())
            .header("x-session-id", profile.session_id.clone());
        match self.credential(client_bearer)? {
            Some((token, slot)) => Ok((req.bearer_auth(token), slot)),
            None => Err(ProviderError::bad_request(
                "no upstream credential available",
            )),
        }
    }

    fn with_request_id(
        &self,
        req: reqwest::RequestBuilder,
        ctx: &CallContext<'_>,
    ) -> reqwest::RequestBuilder {
        req.header("x-request-id", ctx.request_id.to_string())
    }

    /// SEAM: IR -> vendor request body. Per-model field renames belong HERE
    /// in code, not as config knobs. The style split keys on the vendor id
    /// AFTER `model_map`: muse-spark free models answer only the Responses
    /// dialect, everything else the chat dialect.
    fn to_upstream_body(&self, req: &ChatRequest) -> Value {
        let vendor = self.vendor_model(&req.model);
        if is_responses_style(vendor) {
            self.responses_body(req, vendor)
        } else {
            self.chat_body(req, vendor)
        }
    }

    /// The chat-dialect body, with the free-tier gate satisfied: `stream` is
    /// forced true (a buffered request 403s even when the tools are right),
    /// and `bash`/`read` names must appear in `tools`.
    fn chat_body(&self, req: &ChatRequest, vendor: &str) -> Value {
        let mut body = req.to_value();
        let Some(map) = body.as_object_mut() else {
            return body;
        };
        map.insert("model".into(), vendor.into());
        map.insert("stream".into(), true.into());
        map.entry("stream_options")
            .or_insert_with(|| json!({ "include_usage": true }));
        let client_sent_tools = map.contains_key("tools");
        let mut tools = match map.remove("tools") {
            Some(Value::Array(tools)) => tools,
            _ => Vec::new(),
        };
        for (index, name) in GATE_TOOLS.iter().enumerate() {
            if !tools
                .iter()
                .any(|tool| chat_function_name(tool) == Some(*name))
            {
                tools.push(CHAT_DECOYS[index].clone());
            }
        }
        map.insert("tools".into(), Value::Array(tools));
        if !client_sent_tools {
            // The decoys satisfy the gate's name check but must stay
            // unreachable: without this pin an eager client could call a tool
            // that does not exist on this side.
            map.insert("tool_choice".into(), "none".into());
        }
        body
    }

    /// The Responses-dialect body: `input` instead of `messages`, flat tool
    /// spellings, and only the knobs this dialect documents — chat-only
    /// extras (and `max_tokens`, spelled `max_output_tokens` here, clamped
    /// to the vendor's floor of 16) would 400.
    fn responses_body(&self, req: &ChatRequest, vendor: &str) -> Value {
        let mut map = serde_json::Map::new();
        map.insert("model".into(), vendor.into());
        map.insert("stream".into(), true.into());
        map.insert(
            "input".into(),
            Value::Array(messages_to_input(&req.messages)),
        );
        if let Some(max_tokens) = req.extra.get("max_tokens") {
            map.insert(
                "max_output_tokens".into(),
                max_tokens.as_u64().unwrap_or(16).max(16).into(),
            );
        }
        let mut tools: Vec<Value> = match req.extra.get("tools").and_then(Value::as_array) {
            Some(tools) => tools.iter().filter_map(responses_tool).collect(),
            None => Vec::new(),
        };
        for (index, name) in GATE_TOOLS.iter().enumerate() {
            if !tools
                .iter()
                .any(|tool| tool.get("name").and_then(Value::as_str) == Some(*name))
            {
                tools.push(FLAT_DECOYS[index].clone());
            }
        }
        map.insert("tools".into(), Value::Array(tools));
        // Never pinned on this style: the gate reads the tool NAMES only, and
        // a forced choice here would override the client's own policy.
        if let Some(tool_choice) = req.extra.get("tool_choice") {
            map.insert("tool_choice".into(), tool_choice.clone());
        }
        for key in ["temperature", "top_p", "stop", "parallel_tool_calls"] {
            if let Some(value) = req.extra.get(key) {
                map.insert(key.into(), value.clone());
            }
        }
        Value::Object(map)
    }

    /// Send + status-gate. Everything after a handoff can only fail
    /// in-stream (Provider trait contract: the retry unit ends at handoff).
    async fn send(
        &self,
        url: reqwest::Url,
        body: Value,
        ctx: &CallContext<'_>,
    ) -> Result<(UpstreamResponse, TouchOnDrop), ProviderError> {
        let lane = self.transport.lane()?;
        lane.begin_use();
        let mut touch = TouchOnDrop {
            lane,
            completed: false,
            transport_error: false,
        };
        let req = touch
            .lane
            .client()
            .post(url)
            .json(&body)
            // The gate does not read Accept (`*/*` and `application/json`
            // both pass), so one value serves the buffered and streaming
            // paths instead of a per-mode choice. `from_static` validates
            // nothing at request time.
            .header(header::ACCEPT, http::HeaderValue::from_static("*/*"));
        let (req, slot) = self.decorate(req, ctx.client_authorization(), ctx)?;
        let resp = match req.send().await {
            Ok(resp) => resp,
            // Only transport-shaped failures are the lane's fault. An
            // upstream 429 or 500 arrived THROUGH a working tunnel and says
            // nothing about the exit IP — attributing those would retire
            // healthy lanes every time the vendor rate-limits us. A timeout
            // gets its own label for the same reason: `is_timeout` cannot
            // tell a dead tunnel from a slow generation, and three slow
            // generations must not retire three healthy exit IPs.
            Err(e) => {
                match x2api_transport::classify_send(&e) {
                    x2api_transport::SendFault::Lane => touch.transport_error(),
                    // Counted and labelled, never charged: the guard drops
                    // into `note_used`, which touches the lane's clock without
                    // clearing or advancing its health verdict.
                    x2api_transport::SendFault::Slow => touch.lane.note_slow(),
                    x2api_transport::SendFault::Other => {}
                }
                return Err(ProviderError::from(e));
            }
        };
        // A working tunnel proves lane health at handoff; idle use remains
        // completion-bound and is recorded by the guard below.
        touch.lane.note_healthy();
        // Before the gate: `gate_response` consumes the body to build the error,
        // and the headers go with it. Its doc assigns the header to the PROVIDER
        // (it is a standard HTTP field, but what to DO with it is vendor policy --
        // OpenAI's values are seconds-scale and worth honouring, a bogus hours-long
        // one is not), so this is the half that had never been written.
        let hint = Self::retry_hint(resp.headers());
        if resp.status() == http::StatusCode::UPGRADE_REQUIRED {
            // Narrow branch: move the agent string for the requests AFTER
            // this one (see announce_version_floor for why this request
            // still fails).
            announce_version_floor().await;
        }
        let gated = x2api_kit::errors::gate_response(&self.cfg.name, Some(ctx.request_id), resp)
            .await
            .map_err(|mut e| {
                // One value, two consumers: the pool's cooldown below reads
                // `err.retry_after`, and the same field is what `render` turns
                // into the client's `Retry-After`. Deliberately not two numbers --
                // a client told to wait 5 s while the proxy parks the key for 120
                // would retry into a slot we know is shut, and both sides of the
                // claim would be unverifiable from the other.
                if let Some(secs) = hint {
                    e = e.with_retry_after(secs.min(Self::MAX_COOLDOWN_SECS));
                }
                e
            })
            .inspect_err(|e| self.note_credential(slot, e))
            // Chain off the credential note, not a new branch: gate
            // classification, retryability, and lane accounting above stay
            // untouched. The model is the one the body already rewrote
            // through model_map — the vendor's id, which is what the
            // catalogue lists — and only RECORDS here; this 400 still
            // reaches the client verbatim.
            .inspect_err(|e| {
                if let Some(model) = body.get("model").and_then(Value::as_str) {
                    let mut suppressed = self
                        .unavailable
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    note_unavailable_model(&mut suppressed, e, model, Instant::now());
                }
            });
        match gated {
            Ok(resp) => Ok((UpstreamResponse(resp), touch)),
            Err(error) => {
                let mut touch = touch;
                if touch.classify_body_error(&error) {
                    touch.transport_error();
                } else {
                    // A complete rejected body proves the tunnel reached EOF.
                    touch.complete();
                }
                Err(error)
            }
        }
    }
}

// ── endpoint construction ────────────────────────────────────────────────
// Preserve a configured API prefix when it is a terminal `/v1` or GLM `/v4`,
// or when `/v1` appears as a path segment; otherwise append the OpenAI-style
// `/v1` suffix. Operators needing exact custom control must account for this
// normalization when supplying the complete base path.
fn normalized_base(base: &str) -> String {
    if base.ends_with("/v1") || base.contains("/v1/") || base.ends_with("/v4") {
        base.to_string()
    } else {
        format!("{base}/v1")
    }
}

/// A boot diagnostic must never carry endpoint credentials. Reconstructing
/// only scheme, authority, and path structurally excludes userinfo, query,
/// and fragment instead of relying on URL mutators succeeding.
fn visible_endpoint(url: &reqwest::Url) -> String {
    match url.host_str() {
        Some(host) if !host.is_empty() => {
            let port = url.port().map_or(String::new(), |port| format!(":{port}"));
            format!("{}://{}{}{}", url.scheme(), host, port, url.path())
        }
        _ => format!("{}:<no-host>", url.scheme()),
    }
}

fn parse_endpoint(base: &str, path: &str) -> anyhow::Result<reqwest::Url> {
    reqwest::Url::parse(&format!("{base}/{path}"))
        .map_err(|e| anyhow::anyhow!("provider.base_url does not form a usable URL ({e})"))
}

/// The vendor ids answered ONLY by the Responses dialect — a chat call on
/// them 500s upstream (live-probed), so this split is keyed on the id after
/// `model_map`, never on the client's alias.
fn is_responses_style(vendor: &str) -> bool {
    matches!(
        vendor,
        "muse-spark-1.2-contributor-free" | "muse-spark-1.3-contributor-free"
    )
}

/// The tool names upstream's free-tier gate demands in EVERY request's
/// `tools` array — it reads names only, never schema or description.
const GATE_TOOLS: [&str; 2] = ["bash", "read"];

/// Decoys exist for the gate's name check and must never be callable; the
/// description is the contract a client-side agent reads before invoking.
const DECOY_DESCRIPTION: &str = "Compatibility placeholder required by the upstream gateway; this tool is unavailable and must never be called.";

fn chat_function_name(tool: &Value) -> Option<&str> {
    tool.get("function")?.get("name")?.as_str()
}

fn chat_decoy(name: &str) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": name,
            "description": DECOY_DESCRIPTION,
            "parameters": {"type": "object", "properties": {}}
        }
    })
}

fn responses_decoy(name: &str) -> Value {
    json!({
        "type": "function",
        "name": name,
        "description": DECOY_DESCRIPTION,
        "parameters": {"type": "object", "properties": {}}
    })
}

/// The gate decoys, built ONCE: the literals never change, so per-body
/// `json!` construction was pure overhead. Cloned into place per request —
/// the same `Value`, so the serialized bytes are identical to building them
/// inline every time.
static CHAT_DECOYS: LazyLock<[Value; 2]> =
    LazyLock::new(|| [chat_decoy(GATE_TOOLS[0]), chat_decoy(GATE_TOOLS[1])]);
static FLAT_DECOYS: LazyLock<[Value; 2]> = LazyLock::new(|| {
    [
        responses_decoy(GATE_TOOLS[0]),
        responses_decoy(GATE_TOOLS[1]),
    ]
});

/// One client tool in the flat Responses spelling. Chat-spelled entries are
/// unwrapped (`function` inward); an already-flat entry passes through
/// unchanged rather than being dropped for its shape.
fn responses_tool(tool: &Value) -> Option<Value> {
    let Some(function) = tool.get("function") else {
        return Some(tool.clone());
    };
    let mut flat = serde_json::Map::new();
    flat.insert("type".into(), "function".into());
    flat.insert("name".into(), function.get("name")?.clone());
    flat.insert(
        "parameters".into(),
        function
            .get("parameters")
            .cloned()
            .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
    );
    if let Some(description) = function.get("description") {
        flat.insert("description".into(), description.clone());
    }
    Some(Value::Object(flat))
}

/// Chat messages -> the Responses `input` item array: text parts rename by
/// role (`input_text` for everyone, `output_text` for the assistant),
/// `image_url` parts become `input_image`, tool results become
/// `function_call_output`, and an assistant's `tool_calls` follow its
/// message as one `function_call` item each.
fn messages_to_input(messages: &[Value]) -> Vec<Value> {
    let mut input = Vec::with_capacity(messages.len());
    for message in messages {
        let role = message.get("role").and_then(Value::as_str).unwrap_or("");
        if role == "tool" {
            let output = match message.get("content") {
                Some(Value::String(text)) => text.clone(),
                Some(other) => other.to_string(),
                None => String::new(),
            };
            input.push(json!({
                "type": "function_call_output",
                "call_id": message.get("tool_call_id").and_then(Value::as_str).unwrap_or(""),
                "output": output,
            }));
            continue;
        }
        let text_kind = if role == "assistant" {
            "output_text"
        } else {
            "input_text"
        };
        input.push(json!({
            "role": role,
            "content": content_to_input_parts(message.get("content"), text_kind),
        }));
        if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
            for call in tool_calls {
                input.push(json!({
                    "type": "function_call",
                    "call_id": call.get("id").and_then(Value::as_str).unwrap_or(""),
                    "name": call.pointer("/function/name").and_then(Value::as_str).unwrap_or(""),
                    "arguments": call.pointer("/function/arguments").and_then(Value::as_str).unwrap_or(""),
                }));
            }
        }
    }
    input
}

fn content_to_input_parts(content: Option<&Value>, text_kind: &str) -> Value {
    match content {
        Some(Value::String(text)) => Value::Array(vec![json!({ "type": text_kind, "text": text })]),
        Some(Value::Array(parts)) => Value::Array(
            parts
                .iter()
                .map(|part| match part.get("image_url") {
                    Some(image)
                        if part.get("type").and_then(Value::as_str) == Some("image_url") =>
                    {
                        let mut mapped = serde_json::Map::new();
                        mapped.insert("type".into(), "input_image".into());
                        mapped.insert(
                            "image_url".into(),
                            image.get("url").cloned().unwrap_or(Value::Null),
                        );
                        if let Some(detail) = image.get("detail") {
                            mapped.insert("detail".into(), detail.clone());
                        }
                        Value::Object(mapped)
                    }
                    _ if part.get("type").and_then(Value::as_str) == Some("text") => {
                        let mut mapped = part.as_object().cloned().unwrap_or_default();
                        mapped.insert("type".into(), text_kind.into());
                        Value::Object(mapped)
                    }
                    // Unrecognised part shapes keep theirs rather than being
                    // silently rewritten into something the client sent.
                    _ => part.clone(),
                })
                .collect(),
        ),
        _ => Value::Array(Vec::new()),
    }
}

/// The catalogue surface this proxy serves: only `-free` models are listed.
/// Requests for other ids still forward — upstream's own answer stays
/// honest — so this is a filter on ADVERTISING, not on routing.
fn free_only(value: Value) -> Value {
    let mut value = value;
    if let Some(entries) = value.get_mut("data").and_then(Value::as_array_mut) {
        entries.retain(|entry| {
            entry
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| id.ends_with("-free"))
        });
    }
    value
}

/// How long a dead model stays off the LIST after upstream's last
/// "Model is unavailable" 400 — the sibling's catalogue refresh interval; a
/// repeat answer refreshes the expiry.
const SUPPRESSION_WINDOW: Duration = Duration::from_secs(900);

/// How long one raw `/models` answer serves the endpoint before a refetch:
/// the same order as the sibling's catalogue refresh, so a flapping upstream
/// cannot flap the list.
const CATALOGUE_TTL: Duration = Duration::from_secs(900);

/// Upstream's dead-model marker, readable in EITHER field kit preserves on
/// a <500 answer: the message or the verbatim vendor envelope.
const UNAVAILABLE_MARKER: &str = "Model is unavailable";

/// Detect + record one dead-model answer against the suppression map.
///
/// Reads BOTH fields kit preserves — `message` and the <500 `error_body`
/// envelope — because either can carry the marker alone, and gates on 400
/// so a vendor fault's wording never withholds a model. Recording RESETS
/// the window: every repeat answer buys another full `SUPPRESSION_WINDOW`
/// from `now`.
///
/// The bool is the detection verdict, which is also what the field-by-field
/// tests pin; recording is the only side effect.
fn note_unavailable_model(
    suppressed: &mut HashMap<String, Instant>,
    err: &ProviderError,
    model: &str,
    now: Instant,
) -> bool {
    let marked = err.message.contains(UNAVAILABLE_MARKER)
        || err
            .error_body
            .as_ref()
            .is_some_and(|body| body.to_string().contains(UNAVAILABLE_MARKER));
    if err.status != 400 || !marked {
        return false;
    }
    suppressed.insert(model.to_string(), now + SUPPRESSION_WINDOW);
    true
}

/// The catalogue render step: drop every entry whose own id is inside its
/// window, or whose `model_map` vendor is — an alias row disappears WITH the
/// vendor row it names. Expiry is read at render time, so entries reappear
/// on their own the moment the window lapses.
fn withhold_unavailable(
    value: Value,
    model_map: &HashMap<String, String>,
    suppressed: &HashMap<String, Instant>,
    now: Instant,
) -> Value {
    let mut value = value;
    if let Some(entries) = value.get_mut("data").and_then(Value::as_array_mut) {
        entries.retain(|entry| {
            let Some(id) = entry.get("id").and_then(Value::as_str) else {
                return true;
            };
            let listed = suppressed.get(id).is_some_and(|until| now < *until);
            let aliased = model_map
                .get(id)
                .is_some_and(|vendor| suppressed.get(vendor).is_some_and(|until| now < *until));
            !listed && !aliased
        });
    }
    value
}

/// The last RAW `/models` answer (post-fetch, pre-transform), so a flapping
/// upstream cannot flap the list endpoint. The transforms — aliases,
/// free-only, suppression — run fresh per request off whichever copy serves.
#[derive(Default)]
struct CatalogCache {
    raw: Option<Arc<Value>>,
    fetched_at: Option<Instant>,
}

impl CatalogCache {
    /// The cached copy only within `ttl` of its fetch. `Duration::ZERO` is
    /// "always stale", a huge ttl "always fresh" — how tests drive expiry
    /// without a clock injection.
    fn fresh(&self, ttl: Duration) -> Option<Arc<Value>> {
        let fetched_at = self.fetched_at?;
        let raw = self.raw.clone()?;
        (fetched_at.elapsed() < ttl).then_some(raw)
    }

    fn store(&mut self, raw: Value) {
        self.raw = Some(Arc::new(raw));
        self.fetched_at = Some(Instant::now());
    }

    /// The last-good copy at ANY age — what a failed refetch serves.
    fn stale(&self) -> Option<Arc<Value>> {
        self.raw.clone()
    }
}

#[async_trait::async_trait]
impl Provider for OpenAiProvider {
    fn name(&self) -> &str {
        &self.cfg.name
    }

    fn ready(&self) -> bool {
        self.transport.is_ready()
    }

    async fn complete(
        &self,
        req: &ChatRequest,
        ctx: &CallContext<'_>,
    ) -> Result<Completion, ProviderError> {
        let vendor = self.vendor_model(&req.model);
        let body = self.to_upstream_body(req);
        let (resp, mut touch) = self.send(self.upstream_url(vendor), body, ctx).await?;
        let max = self.cfg.response_ceiling();
        let restore = model_restore(&req.model, &self.cfg.model_map);
        if resp.is_sse() {
            // The gate forces every request to stream, so a buffered client's
            // completion is a fold over the very SSE the stream path decodes.
            return if is_responses_style(vendor) {
                fold_completion(
                    decode_responses_sse(resp.into_chunks(), max, vendor, touch),
                    restore,
                )
                .await
            } else {
                fold_completion(
                    decode_upstream_sse(resp.into_chunks(), max, restore.clone(), touch),
                    restore,
                )
                .await
            };
        }
        let bytes = resp.body_bytes_capped(max).await.inspect_err(|error| {
            if touch.classify_body_error(error) {
                touch.transport_error();
            }
        })?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|e| {
            ProviderError::permanent_bad_gateway(format!("upstream body was not JSON: {e}"))
        })?;
        let completion = if is_responses_style(vendor) {
            let mut completion = completion_from_responses(value, vendor)?;
            restore_requested_model(&mut completion.model, restore.as_ref());
            completion
        } else {
            self.completion_from_openai(&req.model, value)?
        };
        touch.complete();
        Ok(completion)
    }

    async fn stream(
        &self,
        req: &ChatRequest,
        ctx: &CallContext<'_>,
    ) -> Result<ChatStream, ProviderError> {
        let vendor = self.vendor_model(&req.model);
        let body = self.to_upstream_body(req);
        let (resp, touch) = self.send(self.upstream_url(vendor), body, ctx).await?;
        let max = self.cfg.response_ceiling();
        let restore = model_restore(&req.model, &self.cfg.model_map);
        if is_responses_style(vendor) {
            to_ir_stream_responses(resp, max, vendor, restore, touch).await
        } else {
            to_ir_stream(resp, max, restore, touch).await
        }
    }

    /// The verbatim fast path, restored with a byte-precise DONE-trim: when
    /// upstream streams OpenAI-shape SSE, hand the server raw BYTES — a
    /// per-chunk JSON parse + re-encode is exactly what this lane exists to
    /// avoid. The trim forwards every byte up to and including the DONE
    /// record and drops what follows (opencode's post-DONE cost frame), so
    /// pre-marker bytes reach the client byte-identical. A stream-request
    /// answered with buffered JSON is re-encoded HERE via the SAME mode
    /// conversion as `stream()` — never late-Err'd, because the round-trip
    /// is already spent and the server's fallback call would re-send it
    /// (double billing on paid vendors).
    ///
    /// Responses-style models answer 501 BEFORE any round trip: their
    /// grammar must never be byte-relayed to a chat client, and the server's
    /// 501 fallback then sends its one and only request down the fold.
    async fn stream_relay(
        &self,
        req: &ChatRequest,
        ctx: &CallContext<'_>,
    ) -> Result<ChunkStream, ProviderError> {
        let vendor = self.vendor_model(&req.model);
        if is_responses_style(vendor) {
            return Err(ProviderError::unsupported("raw stream relay"));
        }
        let body = self.to_upstream_body(req);
        let (resp, touch) = self.send(self.upstream_url(vendor), body, ctx).await?;
        if resp.is_sse() {
            return Ok(touch_when_stream_ends(
                resp,
                self.cfg.response_ceiling(),
                touch,
            ));
        }
        let ir = to_ir_stream(
            resp,
            self.cfg.response_ceiling(),
            model_restore(&req.model, &self.cfg.model_map),
            touch,
        )
        .await?;
        // Frame each chunk through the compat bridge's raw-first emitter:
        // no expect() in a byte-forwarding function, and chunk JSON keeps one
        // source, so relayed and transcoded frames cannot drift in shape.
        let framed = ir
            .map_ok(|c| {
                let mut buf = bytes::BytesMut::with_capacity(256);
                x2api_dialects::chat::append_chunk_frame(&c, &mut buf);
                buf.freeze()
            })
            .map_err(|e| e.to_string())
            .chain(futures_util::stream::iter(vec![Ok(
                bytes::Bytes::from_static(x2api_kit::sse::DONE_FRAME),
            )]));
        Ok(framed.boxed())
    }

    /// The catalogue, as an ADVISORY list: served from cache within
    /// `CATALOGUE_TTL` so a flapping upstream cannot flap this endpoint — a
    /// failed refetch falls back to the last-good copy when one exists — and
    /// rendered through aliases → free-only → the dead-model window on EVERY
    /// call, so suppression, expiry, and aliasing show immediately. Requests
    /// for models missing from the list still forward: upstream's own answer
    /// stays honest.
    async fn models(&self, ctx: &CallContext<'_>) -> Result<Value, ProviderError> {
        // The release-version cadence ticks on EVERY catalogue call (60s
        // CAS), on both the cache-hit and fetch paths — moving it into the
        // fetch-only branch would drop the dynamic agent string's refresh to
        // once per catalogue TTL.
        if claim_release_refresh(unix_seconds()) {
            refresh_release_version().await;
        }
        // A handful of concurrent duplicate fetches on an expiry boundary is
        // accepted on purpose: this is ONE cheap public GET per
        // CATALOGUE_TTL, not a workload that earns single-flight machinery.
        if let Some(raw) = self
            .catalogue_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .fresh(CATALOGUE_TTL)
        {
            return Ok(self.render_catalogue((*raw).clone()));
        }
        match self.fetch_catalogue(ctx).await {
            Ok(raw) => {
                self.catalogue_cache
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .store(raw.clone());
                Ok(self.render_catalogue(raw))
            }
            Err(error) => {
                let stale = self
                    .catalogue_cache
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .stale();
                match stale {
                    Some(raw) => {
                        tracing::warn!(
                            provider = %self.cfg.name,
                            error = %error.message,
                            "catalogue refetch failed; serving the last-good copy"
                        );
                        Ok(self.render_catalogue((*raw).clone()))
                    }
                    // Nothing cached yet: the error propagates exactly as it
                    // always has.
                    None => Err(error),
                }
            }
        }
    }
}

/// The client name to restore only when the vendor returns the exact model
/// selected for this request. `None` means the request was not alias-rewritten.
#[derive(Clone)]
struct ModelRestore {
    vendor: String,
    alias: String,
}

fn model_restore(
    requested_model: &str,
    model_map: &HashMap<String, String>,
) -> Option<ModelRestore> {
    Some(ModelRestore {
        vendor: model_map.get(requested_model)?.clone(),
        alias: requested_model.to_string(),
    })
}

fn restore_requested_model(response_model: &mut String, restore: Option<&ModelRestore>) {
    if let Some(restore) = restore
        && response_model == &restore.vendor
    {
        response_model.clone_from(&restore.alias);
    }
}

/// Shared fork for both stream methods: the client's stream flag is
/// authoritative over the upstream content-type — an SSE body decodes to IR
/// chunks, a buffered JSON reply mode-converts, never an error. Lives here
/// so `stream()` and `stream_relay()` cannot drift on the buffered case.
async fn to_ir_stream(
    resp: UpstreamResponse,
    max: usize,
    restore: Option<ModelRestore>,
    touch: TouchOnDrop,
) -> Result<ChatStream, ProviderError> {
    if !resp.is_sse() {
        let mut touch = touch;
        let bytes = resp.body_bytes_capped(max).await.inspect_err(|error| {
            if touch.classify_body_error(error) {
                touch.transport_error();
            }
        })?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|e| {
            ProviderError::permanent_bad_gateway(format!("upstream stream reply was not JSON: {e}"))
        })?;
        let mut completion = Completion::from_openai(value)?;
        restore_requested_model(&mut completion.model, restore.as_ref());
        touch.complete();
        return Ok(futures_util::stream::iter(
            x2api_kit::types::completion_to_chunks(&completion)
                .into_iter()
                .map(Ok),
        )
        .boxed());
    }
    Ok(decode_upstream_sse(resp.into_chunks(), max, restore, touch).boxed())
}

/// The responses-dialect twin of `to_ir_stream`: buffered JSON mode-converts
/// through `completion_from_responses`, an SSE body decodes through the
/// responses decoder — never an error either way.
async fn to_ir_stream_responses(
    resp: UpstreamResponse,
    max: usize,
    vendor_model: &str,
    restore: Option<ModelRestore>,
    touch: TouchOnDrop,
) -> Result<ChatStream, ProviderError> {
    if !resp.is_sse() {
        let mut touch = touch;
        let bytes = resp.body_bytes_capped(max).await.inspect_err(|error| {
            if touch.classify_body_error(error) {
                touch.transport_error();
            }
        })?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|e| {
            ProviderError::permanent_bad_gateway(format!("upstream stream reply was not JSON: {e}"))
        })?;
        let mut completion = completion_from_responses(value, vendor_model)?;
        restore_requested_model(&mut completion.model, restore.as_ref());
        touch.complete();
        return Ok(futures_util::stream::iter(
            x2api_kit::types::completion_to_chunks(&completion)
                .into_iter()
                .map(Ok),
        )
        .boxed());
    }
    Ok(decode_responses_sse(resp.into_chunks(), max, vendor_model, touch).boxed())
}

/// Fold a decoded SSE stream into the one `Completion` `complete()` returns:
/// concat what the IR template concatenates, keep the LAST finish reason and
/// usage, and assemble tool-call fragments by `index` (arguments concatenate
/// as strings — they are wire slices of one JSON document). `id`/`model`
/// come from the first chunk, the frame the vendor stamps on every delta.
/// `raw` stays None: there is no single frame to replay, so the chat dialect
/// renders this from fields.
async fn fold_completion(
    stream: impl futures_util::Stream<Item = Result<ChatChunk, ProviderError>>,
    restore: Option<ModelRestore>,
) -> Result<Completion, ProviderError> {
    let mut stream = std::pin::pin!(stream);
    let mut id = String::new();
    let mut model = String::new();
    let mut text = String::new();
    let mut reasoning = String::new();
    let mut refusal = String::new();
    let mut finish_reason = None;
    let mut usage = None;
    let mut calls: BTreeMap<u32, ToolCall> = BTreeMap::new();
    let mut first = true;
    while let Some(item) = stream.next().await {
        let chunk = item?;
        if first {
            id.clone_from(&chunk.id);
            model.clone_from(&chunk.model);
            first = false;
        }
        text.push_str(&chunk.text);
        reasoning.push_str(&chunk.reasoning);
        refusal.push_str(&chunk.refusal);
        if chunk.finish_reason.is_some() {
            finish_reason = chunk.finish_reason;
        }
        if chunk.usage.is_some() {
            usage = chunk.usage;
        }
        for delta in chunk.tool_calls {
            let entry = calls.entry(delta.index).or_default();
            if let Some(name) = delta.name.filter(|name| !name.is_empty()) {
                entry.name = name;
            }
            if let Some(id) = delta.id.filter(|id| !id.is_empty()) {
                entry.id = id;
            }
            entry.arguments.push_str(&delta.arguments);
        }
    }
    restore_requested_model(&mut model, restore.as_ref());
    Ok(Completion {
        id,
        model,
        text,
        finish_reason,
        usage,
        tool_calls: calls.into_values().collect(),
        raw: None,
        reasoning: (!reasoning.is_empty()).then_some(reasoning),
        refusal: (!refusal.is_empty()).then_some(refusal),
    })
}

/// Parse a buffered Responses-API object into IR. `raw` stays None on
/// purpose: the chat dialect would relay this document verbatim, and a
/// responses-shaped document is not chat shape — it renders from fields.
/// `vendor_model` is the model's fallback when the object omits it: the
/// caller always knows which id it asked upstream for, and a client must
/// never read an empty model back.
fn completion_from_responses(
    value: Value,
    vendor_model: &str,
) -> Result<Completion, ProviderError> {
    let status = value.get("status").and_then(Value::as_str).unwrap_or("");
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    if let Some(output) = value.get("output").and_then(Value::as_array) {
        for item in output {
            match item.get("type").and_then(Value::as_str) {
                Some("message") => {
                    if let Some(parts) = item.get("content").and_then(Value::as_array) {
                        for part in parts {
                            if part.get("type").and_then(Value::as_str) == Some("output_text")
                                && let Some(piece) = part.get("text").and_then(Value::as_str)
                            {
                                text.push_str(piece);
                            }
                        }
                    }
                }
                Some("function_call") => tool_calls.push(ToolCall {
                    id: item
                        .get("call_id")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    name: item
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    arguments: item
                        .get("arguments")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                }),
                _ => {}
            }
        }
    }
    Ok(Completion {
        id: value
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("x2api-completion")
            .to_string(),
        model: value
            .get("model")
            .and_then(Value::as_str)
            .filter(|model| !model.is_empty())
            .unwrap_or(vendor_model)
            .to_string(),
        text,
        finish_reason: Some(
            if status == "incomplete" {
                "length"
            } else {
                "stop"
            }
            .to_string(),
        ),
        usage: responses_usage(value.get("usage")),
        tool_calls,
        raw: None,
        reasoning: None,
        refusal: None,
    })
}

/// Responses usage counters -> the IR spelling: the same numbers under the
/// chat field names, detail maps mapped one-to-one.
fn responses_usage(usage: Option<&Value>) -> Option<Usage> {
    let usage = usage?;
    Some(Usage {
        prompt_tokens: usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        completion_tokens: usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        total_tokens: usage.get("total_tokens").and_then(Value::as_u64),
        cached_tokens: usage
            .pointer("/input_tokens_details/cached_tokens")
            .and_then(Value::as_u64),
        reasoning_tokens: usage
            .pointer("/output_tokens_details/reasoning_tokens")
            .and_then(Value::as_u64),
        cache_creation_input_tokens: None,
        cache_read_input_tokens: None,
    })
}

/// What one responses-SSE data record asks the stream to do.
enum ResponsesFrame {
    /// Yield this chunk and keep reading.
    Chunk(ChatChunk),
    /// Terminal: yield this chunk, then end the stream cleanly.
    Terminal(ChatChunk),
    /// Vendor failure — the message is for the log, never for a client.
    Failed(String),
    /// `ping`, `response.created`, and anything else with no IR meaning.
    Skip,
}

/// The vendor's own failure text, wherever this shape nests it. Log-only:
/// every client-facing error message in this file is canned.
fn vendor_failure_message(value: &Value) -> String {
    value
        .pointer("/response/error/message")
        .or_else(|| value.pointer("/error/message"))
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("the upstream reported a failed response")
        .to_string()
}

/// SEAM: one responses-SSE data payload -> the frame it yields. `saw_arguments`
/// remembers which `output_index` already streamed argument deltas, because a
/// vendor that sent none repeats the FULL arguments on `output_item.done` —
/// and appending that copy after the deltas would double every call's args.
/// `identity` is this stream's stashed `(id, model)`: the events' embedded
/// `response` object is where the dialect carries them, `response.created`
/// arrives first, and a later event fills whatever the first left empty.
fn responses_frame(
    payload: &[u8],
    saw_arguments: &mut HashSet<u32>,
    identity: &mut Option<(String, String)>,
    model_fallback: &str,
) -> Result<ResponsesFrame, ProviderError> {
    let value: Value = serde_json::from_slice(payload).map_err(|e| {
        ProviderError::bad_gateway(format!("malformed upstream response event: {e}"))
    })?;
    if let Some(response) = value
        .get("response")
        .filter(|response| response.is_object())
    {
        let slot = identity.get_or_insert_with(|| (String::new(), String::new()));
        if slot.0.is_empty() {
            slot.0 = response
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
        }
        if slot.1.is_empty() {
            slot.1 = response
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
        }
    }
    let kind = value.get("type").and_then(Value::as_str).unwrap_or("");
    let text_delta = |text: String| ChatChunk {
        text,
        ..ChatChunk::default()
    };
    let frame = match kind {
        "response.output_text.delta" => ResponsesFrame::Chunk(text_delta(
            value
                .get("delta")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        )),
        "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
            ResponsesFrame::Chunk(ChatChunk {
                reasoning: value
                    .get("delta")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                ..ChatChunk::default()
            })
        }
        "response.output_item.added" => {
            let item = &value["item"];
            if item.get("type").and_then(Value::as_str) == Some("function_call") {
                ResponsesFrame::Chunk(ChatChunk {
                    tool_calls: vec![ToolCallDelta {
                        index: value
                            .get("output_index")
                            .and_then(Value::as_u64)
                            .unwrap_or(0) as u32,
                        id: item
                            .get("call_id")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        name: item.get("name").and_then(Value::as_str).map(str::to_string),
                        arguments: String::new(),
                    }],
                    ..ChatChunk::default()
                })
            } else {
                ResponsesFrame::Skip
            }
        }
        "response.function_call_arguments.delta" => {
            let index = value
                .get("output_index")
                .and_then(Value::as_u64)
                .unwrap_or(0) as u32;
            saw_arguments.insert(index);
            ResponsesFrame::Chunk(ChatChunk {
                tool_calls: vec![ToolCallDelta {
                    index,
                    id: None,
                    name: None,
                    arguments: value
                        .get("delta")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                }],
                ..ChatChunk::default()
            })
        }
        "response.output_item.done" => {
            let item = &value["item"];
            let index = value
                .get("output_index")
                .and_then(Value::as_u64)
                .unwrap_or(0) as u32;
            let arguments = item.get("arguments").and_then(Value::as_str);
            if item.get("type").and_then(Value::as_str) == Some("function_call")
                && let Some(arguments) = arguments
                && !saw_arguments.contains(&index)
            {
                ResponsesFrame::Chunk(ChatChunk {
                    tool_calls: vec![ToolCallDelta {
                        index,
                        id: None,
                        name: None,
                        arguments: arguments.to_string(),
                    }],
                    ..ChatChunk::default()
                })
            } else {
                ResponsesFrame::Skip
            }
        }
        "response.completed" | "response.incomplete" => {
            let finish_reason = if kind == "response.incomplete" {
                "length"
            } else {
                "stop"
            };
            ResponsesFrame::Terminal(ChatChunk {
                finish_reason: Some(finish_reason.to_string()),
                usage: responses_usage(value.pointer("/response/usage")),
                ..ChatChunk::default()
            })
        }
        "response.failed" | "error" => ResponsesFrame::Failed(vendor_failure_message(&value)),
        _ => ResponsesFrame::Skip,
    };
    let frame = match frame {
        ResponsesFrame::Chunk(mut chunk) => {
            stamp_identity(&mut chunk, identity, model_fallback);
            ResponsesFrame::Chunk(chunk)
        }
        ResponsesFrame::Terminal(mut chunk) => {
            stamp_identity(&mut chunk, identity, model_fallback);
            ResponsesFrame::Terminal(chunk)
        }
        failed_or_skip => failed_or_skip,
    };
    Ok(frame)
}

/// Every yielded chunk carries the stashed response identity, because the
/// fold takes `id`/`model` from the FIRST chunk — a delta that arrives before
/// any identity would otherwise poison the whole completion with empties. The
/// model falls back to the vendor id the caller knows: a vendor that omits
/// `response.model` must still not answer a client with an empty model.
fn stamp_identity(
    chunk: &mut ChatChunk,
    identity: &Option<(String, String)>,
    model_fallback: &str,
) {
    match identity {
        Some((id, model)) => {
            chunk.id.clone_from(id);
            if model.is_empty() {
                chunk.model = model_fallback.to_string();
            } else {
                chunk.model.clone_from(model);
            }
        }
        None => chunk.model = model_fallback.to_string(),
    }
}

/// SEAM: OpenCode responses-SSE bytes -> IR chunks. Framing, transport-error
/// marking, and the EOF flush mirror `decode_upstream_sse`; the dispatch is
/// on the event's `type` field. Terminal detection is deliberately LOCAL:
/// this dialect has no `[DONE]` — a terminal frame ends the stream on the
/// spot, so reaching the EOF check below means none ever arrived, and
/// `SseDecoder::done_seen()` (which answers only about `[DONE]`) is
/// irrelevant here. Nothing after a terminal is ever yielded — not even a
/// final bookkeeping `ping`, whose `cost` bookkeeping must never surface.
fn decode_responses_sse(
    mut upstream: ChunkStream,
    max_record: usize,
    model_fallback: &str,
    mut touch: TouchOnDrop,
) -> impl futures_util::Stream<Item = Result<ChatChunk, ProviderError>> + Send + 'static {
    // Owned before the generator captures it: the returned stream is 'static.
    let model_fallback = model_fallback.to_string();
    let mut decoder = x2api_kit::sse::SseDecoder::with_max_record(max_record);
    try_stream! {
        use x2api_kit::sse::SseEvent;
        // One events Vec for the whole stream; the decoder drains into it.
        let mut events: Vec<SseEvent> = Vec::new();
        let mut saw_arguments: HashSet<u32> = HashSet::new();
        let mut identity: Option<(String, String)> = None;
        while let Some(chunk) = upstream.next().await {
            if chunk.is_err() {
                touch.transport_error();
            }
            let bytes = chunk.map_err(ProviderError::transport)?;
            let decode = decoder.push_into(&bytes, &mut events);
            for event in events.drain(..) {
                if let SseEvent::Data { data: payload, .. } = event {
                    if payload.is_empty() {
                        continue;
                    }
                    match responses_frame(&payload, &mut saw_arguments, &mut identity, &model_fallback)? {
                        ResponsesFrame::Chunk(frame) => yield frame,
                        ResponsesFrame::Terminal(frame) => {
                            touch.complete();
                            yield frame;
                            // Nothing may follow a terminal: the frames after
                            // it are vendor bookkeeping, never client content.
                            return;
                        }
                        ResponsesFrame::Failed(message) => {
                            // The vendor's own words go to the log only; the
                            // client gets a canned 502 and NO terminal chunk,
                            // so a truncated answer never looks complete.
                            tracing::warn!("upstream response stream failed: {message}");
                            Err(ProviderError::permanent_bad_gateway(
                                "the upstream reported a failed response",
                            ))?;
                        }
                        ResponsesFrame::Skip => {}
                    }
                }
            }
            decode.map_err(|e| ProviderError::bad_gateway(e.to_string()))?;
        }
        // Flush a final unterminated record before judging the ending, so its
        // last complete payload cannot be discarded.
        for event in decoder.finish() {
            if let SseEvent::Data { data: payload, .. } = event {
                if payload.is_empty() {
                    continue;
                }
                match responses_frame(&payload, &mut saw_arguments, &mut identity, &model_fallback)? {
                    ResponsesFrame::Chunk(frame) => yield frame,
                    ResponsesFrame::Terminal(frame) => {
                        touch.complete();
                        yield frame;
                        return;
                    }
                    ResponsesFrame::Failed(message) => {
                        tracing::warn!("upstream response stream failed: {message}");
                        Err(ProviderError::permanent_bad_gateway(
                            "the upstream reported a failed response",
                        ))?;
                    }
                    ResponsesFrame::Skip => {}
                }
            }
        }
        // Terminal frames return on the spot, so arriving here means the
        // stream was cut: truncation, never a clean end.
        Err(ProviderError::bad_gateway(
            "upstream stream ended without a terminal response event",
        ))?;
    }
}

/// SEAM: vendor SSE bytes -> IR chunks. Event framing and the `[DONE]`
/// terminator are handled here; `ChatChunk::from_wire` handles in-band errors.
fn decode_upstream_sse(
    mut upstream: ChunkStream,
    max_record: usize,
    restore: Option<ModelRestore>,
    mut touch: TouchOnDrop,
) -> impl futures_util::Stream<Item = Result<ChatChunk, ProviderError>> + Send + 'static {
    let mut decoder = x2api_kit::sse::SseDecoder::with_max_record(max_record);
    try_stream! {
        use x2api_kit::sse::SseEvent;
        // One events Vec for the whole stream; the decoder drains into it.
        let mut events: Vec<SseEvent> = Vec::new();
        while let Some(chunk) = upstream.next().await {
            if chunk.is_err() {
                touch.transport_error();
            }
            let bytes = chunk.map_err(ProviderError::transport)?;
            let decode = decoder.push_into(&bytes, &mut events);
            if events
                .iter()
                .any(|event| matches!(event, SseEvent::Done))
            {
                // Observe a terminal in the same chunk before yielding any
                // earlier Data event, which a client may abandon.
                touch.complete();
            }
            for event in events.drain(..) {
                match event {
                    SseEvent::Data { data: payload, .. } => {
                        if payload.is_empty() {
                            continue;
                        }
                        let mut chunk = ChatChunk::from_wire(payload)?;
                        restore_requested_model(&mut chunk.model, restore.as_ref());
                        yield chunk;
                    }
                    SseEvent::Done => {
                        // Never relay past [DONE]: trailing vendor bookkeeping
                        // frames must not reach clients.
                        return;
                    }
                }
            }
            decode.map_err(|e| ProviderError::bad_gateway(e.to_string()))?;
        }
        // Flush a final unterminated record before deciding whether the stream
        // completed, so its last complete payload cannot be discarded.
        for event in decoder.finish() {
            if let SseEvent::Data { data: payload, .. } = event {
                if payload.is_empty() {
                    continue;
                }
                let mut chunk = ChatChunk::from_wire(payload)?;
                restore_requested_model(&mut chunk.model, restore.as_ref());
                yield chunk;
            }
        }
        if decoder.done_seen() {
            touch.complete();
        } else {
            // The retry unit ended when the provider handed off this stream, so
            // this 502 is not retried; it becomes the dialect's failure frame.
            Err(ProviderError::bad_gateway("upstream stream ended without [DONE]"))?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_sse_decoder_recognizes_coalesced_split_and_suffix_done() {
        use x2api_kit::sse::{SseDecoder, SseEvent};
        let done = |parts: &[&[u8]]| {
            let mut decoder = SseDecoder::new();
            let mut events = Vec::new();
            let mut found = false;
            for part in parts {
                decoder.push_into(part, &mut events).unwrap();
                found |= events
                    .drain(..)
                    .any(|event| matches!(event, SseEvent::Done));
            }
            found
        };
        assert!(done(&[b"data: {}\n\ndata: [DONE]\n\n"]));
        assert!(done(&[b"data: {}\n\nda", b"ta: [DONE]\n\n"]));
        assert!(!done(&[b"data: [DONE] suffix\n\n"]));
    }

    #[test]
    fn local_body_verdicts_are_not_lane_transport_errors() {
        let transport = x2api_transport::Transport::direct(&x2api_kit::ServerConfig::default());
        let guard = TouchOnDrop {
            lane: transport.lane().unwrap(),
            completed: false,
            transport_error: false,
        };
        assert!(!guard.classify_body_error(&ProviderError::permanent_bad_gateway("too large")));
    }

    /// A provider over N configured credentials, for the pool tests.
    fn pooled(keys: &[&str]) -> OpenAiProvider {
        OpenAiProvider::new(
            x2api_transport::Transport::direct(&x2api_kit::ServerConfig::default()),
            ServiceConfig {
                name: "openai".into(),
                base_url: "https://api.openai.com".into(),
                api_key: None,
                api_keys: keys.iter().map(|k| (*k).to_string()).collect(),
                model_map: Default::default(),
                max_response_bytes: None,
            },
        )
        .unwrap()
    }
    fn model_mapped_provider(pairs: &[(&str, &str)]) -> anyhow::Result<OpenAiProvider> {
        OpenAiProvider::new(
            x2api_transport::Transport::direct(&x2api_kit::ServerConfig::default()),
            ServiceConfig {
                name: "openai".into(),
                base_url: "https://api.openai.com".into(),
                api_key: None,
                api_keys: Vec::new(),
                model_map: pairs
                    .iter()
                    .map(|(alias, vendor)| ((*alias).into(), (*vendor).into()))
                    .collect(),
                max_response_bytes: None,
            },
        )
    }

    #[test]
    fn duplicate_vendor_targets_are_rejected_as_non_injective() {
        let error = match model_mapped_provider(&[
            ("friendly", "vendor-model"),
            ("shortcut", "vendor-model"),
        ]) {
            Ok(_) => {
                panic!("two aliases cannot reverse-map one vendor response to a stable client id")
            }
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(message.contains("vendor-model"), "got {message}");
        assert!(
            message.contains("friendly") && message.contains("shortcut"),
            "got {message}"
        );
    }

    #[test]
    fn completion_reverse_maps_only_the_alias_that_was_requested() {
        let p = model_mapped_provider(&[("friendly-model", "vendor-model")])
            .expect("an injective alias map is valid");
        let completion = |requested: &str| {
            p.completion_from_openai(
                requested,
                json!({
                    "id": "c1",
                    "model": "vendor-model",
                    "object": "chat.completion",
                    "choices": [{
                        "index": 0,
                        "message": {"role": "assistant", "content": "hello"},
                        "finish_reason": "stop"
                    }]
                }),
            )
            .unwrap()
        };
        assert_eq!(completion("friendly-model").model, "friendly-model");
        assert_eq!(completion("vendor-model").model, "vendor-model");
        assert_eq!(completion("other-model").model, "vendor-model");
    }

    /// The hint parser, per spelling. Each form is a way the SAME instruction
    /// arrives, so a test that covers only the integer one would still let a dead
    /// date arm ship as "we honour Retry-After" — which is what this function was
    /// written to replace.
    #[test]
    fn every_retry_after_spelling_reaches_the_pool() {
        use http::{HeaderMap, HeaderValue as HV};
        let hint = |pairs: &[(&'static str, &'static str)]| {
            let mut h = HeaderMap::new();
            for (k, v) in pairs {
                h.insert(http::HeaderName::from_static(k), HV::from_static(v));
            }
            OpenAiProvider::retry_hint(&h)
        };
        assert_eq!(hint(&[]), None, "no header, no opinion");
        assert_eq!(hint(&[("retry-after", "42")]), Some(42));
        // Sub-second and nonstandard, but the one OpenAI actually sends — and it
        // must WIN over the seconds form when both are present, as the SDKs do.
        assert_eq!(hint(&[("retry-after-ms", "800")]), Some(1));
        assert_eq!(
            hint(&[("retry-after-ms", "2500"), ("retry-after", "9")]),
            Some(3),
            "ms is read first"
        );
        // Round up rather than truncate: 1200 ms is not "0 seconds" to wait.
        assert_eq!(hint(&[("retry-after-ms", "1200")]), Some(2));
        // IMF-fixdate. Asserted as SOME, not None: an unparseable-date bug here is
        // invisible unless something demands the arm answer.
        let future = {
            let when = jiff::Timestamp::now() + jiff::SignedDuration::from_secs(30);
            let zoned = when.to_zoned(jiff::tz::TimeZone::UTC);
            jiff::fmt::rfc2822::to_string(&zoned).expect("print as IMF-fixdate")
        };
        let mut map = HeaderMap::new();
        map.insert(http::header::RETRY_AFTER, HV::try_from(future).unwrap());
        let secs = OpenAiProvider::retry_hint(&map).expect("a date must parse");
        assert!(
            (20..=31).contains(&secs),
            "date computed as {secs}s from now"
        );
        // A date already gone is "retry now", never a wrapped huge number.
        let mut map = HeaderMap::new();
        map.insert(
            http::header::RETRY_AFTER,
            HV::from_static("Fri, 31 Dec 2021 23:59:59 GMT"),
        );
        assert_eq!(OpenAiProvider::retry_hint(&map), Some(0));
        // Garbage is "no hint", which the caller then answers with the default.
        assert_eq!(hint(&[("retry-after", "soon-ish")]), None);
        assert_eq!(
            hint(&[("retry-after", "-5")]),
            None,
            "negative is not a wait"
        );
    }

    /// A vendor answer, as the pool sees it.
    fn upstream(status: u16, retry_after: Option<u64>) -> ProviderError {
        ProviderError {
            status,
            message: "from the vendor".into(),
            error_type: "upstream".into(),
            retry_after,
            error_body: None,
            retryable: true,
            body_truncated: false,
            upstream_headers: None,
        }
    }

    #[test]
    fn credential_failures_emit_cooldown_metrics_with_their_reason() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let p = pooled(&["quota", "unauthorized", "forbidden"]);
            p.note_credential(Some(0), &upstream(429, Some(30)));
            p.note_credential(Some(1), &upstream(401, None));
            p.note_credential(Some(2), &upstream(403, None));
        });

        let mut counts = std::collections::BTreeMap::new();
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            if key.key().name() != x2api_kit::telemetry::names::CREDENTIAL_COOLDOWNS {
                continue;
            }
            let DebugValue::Counter(c) = value else {
                continue;
            };
            let reason = key
                .key()
                .labels()
                .find_map(|label| (label.key() == "reason").then_some(label.value().to_owned()));
            counts.insert(reason, c);
        }
        assert_eq!(
            counts,
            std::collections::BTreeMap::from([
                (Some("auth".to_owned()), 2),
                (Some("quota".to_owned()), 1),
            ])
        );
    }

    /// The credential that gets cooled must be the one that carried the request.
    /// The failure mode this pins is a second pick inside the error path: under
    /// concurrency that spends a key which never saw the rate limit while the
    /// one that did keeps taking traffic, so every request 429s forever and the
    /// pool looks healthy the whole time.
    #[test]
    fn a_rate_limit_spends_the_credential_that_received_it() {
        let p = pooled(&["a", "b", "c"]);
        assert_eq!(p.credential(None).unwrap().unwrap().1, Some(0));
        assert_eq!(p.credential(None).unwrap().unwrap().1, Some(1));
        p.note_credential(Some(0), &upstream(429, Some(30)));
        assert_eq!(
            p.credential(None).unwrap().unwrap().1,
            Some(2),
            "slot 0 is out of rotation"
        );
        assert_eq!(
            p.credential(None).unwrap().unwrap().1,
            Some(1),
            "and stays out"
        );
        // A 500 is about the vendor, not the key: slot 2 must still be usable.
        p.note_credential(Some(2), &upstream(500, None));
        assert_eq!(p.credential(None).unwrap().unwrap().1, Some(2));
    }

    /// A pool with every slot spent is this proxy's OWN capacity answer, so it
    /// is a 503 carrying the shortest remaining wait — never a silent fallthrough
    /// to an unauthenticated request, which would read as a vendor 401.
    #[test]
    fn an_exhausted_pool_refuses_with_a_wait_instead_of_picking_anyway() {
        let p = pooled(&["a", "b"]);
        p.note_credential(Some(0), &upstream(429, Some(10)));
        p.note_credential(Some(1), &upstream(429, Some(45)));
        let err = match p.credential(None) {
            Err(e) => e,
            Ok(Some((_, slot))) => panic!("handed out slot {slot:?} while every slot cooled"),
            Ok(None) => panic!("every slot is cooling, so there is no credential"),
        };
        assert_eq!(err.status, 503, "our capacity, not their fault");
        // A range, not `Some(10)`: the cooldown was stamped on an earlier clock
        // read than the one answering here, so an exact assert would be a
        // flaky race against `Instant`. What matters is that the caller is told
        // about the SOONER of the two slots (10 s) rather than the later (45 s).
        let wait = err
            .retry_after
            .expect("an exhausted pool must say how long");
        assert!(
            (1..=10).contains(&wait),
            "shortest-slot advice, got {wait}: {err}"
        );
        assert!(err.message.contains("rate-limited"), "say why: {err}");
    }

    /// `Retry-After` from the vendor beats our guess; a rejected key cools
    /// LONGER than a rate-limited one, because it will keep failing until a human
    /// intervenes — but not forever, since a 401 can also be a transient vendor
    /// problem. A 500 spends nothing: same reasoning the egress lane uses for
    /// "only transport failures are the lane's fault".
    #[test]
    fn only_quota_and_auth_spend_a_credential() {
        assert_eq!(
            OpenAiProvider::cooldown(&upstream(429, Some(90))),
            Some(Duration::from_secs(90))
        );
        assert_eq!(
            OpenAiProvider::cooldown(&upstream(429, None)),
            Some(Duration::from_secs(30)),
            "a hint-less 429 still costs something"
        );
        assert_eq!(
            OpenAiProvider::cooldown(&upstream(401, None)),
            Some(Duration::from_secs(300))
        );
        assert_eq!(
            OpenAiProvider::cooldown(&upstream(403, None)),
            Some(Duration::from_secs(300))
        );
        assert_eq!(OpenAiProvider::cooldown(&upstream(500, Some(5))), None);
        assert_eq!(OpenAiProvider::cooldown(&upstream(400, None)), None);
    }

    /// Passthrough mode (no configured credential, the caller's own token
    /// forwarded upstream) has no slot to spend: cooling it would punish this
    /// caller for another caller's limit, and there is no second key anyway.
    #[test]
    fn a_passthrough_credential_carries_no_slot_and_is_never_cooled() {
        let p = pooled(&[]);
        let (token, slot) = p
            .credential(Some("Bearer caller-token"))
            .unwrap()
            .expect("the caller's token is the credential");
        assert_eq!(token, "caller-token", "the `Bearer ` prefix is stripped");
        assert_eq!(slot, None);
        p.note_credential(slot, &upstream(429, Some(60)));
        assert!(
            p.credential(Some("Bearer caller-token")).unwrap().is_some(),
            "a 429 must not take the passthrough path away"
        );
    }

    /// `api_key` and `api_keys` are additive, and a duplicate is NOT a second
    /// account: it would double that key's traffic and halve the pool's purpose.
    #[test]
    fn credentials_merge_deduplicate_and_drop_blanks() {
        let cfg = ServiceConfig {
            name: "openai".into(),
            base_url: "https://x".into(),
            api_key: Some("a".into()),
            api_keys: vec!["a".into(), " b ".into(), String::new(), "c".into()],
            model_map: Default::default(),
            max_response_bytes: None,
        };
        assert_eq!(
            cfg.credentials(),
            vec!["a".to_string(), "b".into(), "c".into()],
            "one slot per real account, trimmed, in order"
        );
        assert!(
            ServiceConfig {
                api_key: None,
                api_keys: vec![],
                ..cfg
            }
            .credentials()
            .is_empty(),
            "no keys is passthrough, not a pool of one empty string"
        );
    }

    fn provider(base: &str) -> OpenAiProvider {
        OpenAiProvider::new(
            x2api_transport::Transport::direct(&x2api_kit::ServerConfig::default()),
            ServiceConfig {
                name: "openai".into(),
                base_url: base.to_string(),
                api_key: Some("k".into()),
                model_map: Default::default(),
                api_keys: Vec::new(),
                max_response_bytes: None,
            },
        )
        .expect("test base urls parse")
    }

    #[test]
    fn base_normalization_matches_gateway_alias_rules() {
        assert_eq!(
            provider("https://api.deepseek.com").chat_url.as_str(),
            "https://api.deepseek.com/v1/chat/completions"
        );
        assert_eq!(
            provider("https://api.deepseek.com/v1").chat_url.as_str(),
            "https://api.deepseek.com/v1/chat/completions"
        );
        assert_eq!(
            provider("https://open.bigmodel.cn/api/paas/v4")
                .chat_url
                .as_str(),
            "https://open.bigmodel.cn/api/paas/v4/chat/completions"
        );
        // These current appends are intentional visibility, not vendor-specific
        // recognition: a nonstandard prefix does not suppress OpenAI `/v1`.
        // An operator needing exact control must write the full endpoint base
        // and accept the normalization documented above.
        assert_eq!(
            provider("https://generativelanguage.googleapis.com/v1beta")
                .chat_url
                .as_str(),
            "https://generativelanguage.googleapis.com/v1beta/v1/chat/completions"
        );
        assert_eq!(
            provider("https://vendor.example/api/v2").chat_url.as_str(),
            "https://vendor.example/api/v2/v1/chat/completions"
        );

        // A base that cannot form a URL is a boot failure, not a 500 per call.
        assert!(
            OpenAiProvider::new(
                x2api_transport::Transport::direct(&x2api_kit::ServerConfig::default()),
                ServiceConfig {
                    name: "openai".into(),
                    base_url: "not a url".into(),
                    api_key: None,
                    api_keys: Vec::new(),
                    model_map: Default::default(),
                    max_response_bytes: None,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn endpoint_log_omits_credentials_query_and_fragment_but_keeps_port() {
        let url = reqwest::Url::parse(
            "http://user:s3cr3t@127.0.0.1:8080/v1/chat/completions?token=BOOTQUERYSECRET#fragment",
        )
        .unwrap();
        assert_eq!(
            visible_endpoint(&url),
            "http://127.0.0.1:8080/v1/chat/completions"
        );
    }

    #[test]
    fn forced_stream_holds_even_when_the_client_asked_buffered() {
        // The vendor gate 403s a buffered request even when the tools are
        // right, so `stream: true` is not a preference here — it is part of
        // the credential the request presents.
        let req: ChatRequest = serde_json::from_value(json!({
            "model": "m", "messages": [{"role": "user", "content": "x"}], "stream": false
        }))
        .unwrap();
        let p = provider("https://x.test/v1");
        let body = p.to_upstream_body(&req);
        assert_eq!(body["stream"], true);
    }

    #[test]
    fn model_map_rewrites_in_body() {
        let mut map = std::collections::HashMap::new();
        map.insert("alias".to_string(), "real-model".to_string());
        let p = OpenAiProvider::new(
            x2api_transport::Transport::direct(&x2api_kit::ServerConfig::default()),
            ServiceConfig {
                name: "openai".into(),
                base_url: "https://x.test/v1".into(),
                api_key: None,
                api_keys: Vec::new(),
                model_map: map,
                max_response_bytes: None,
            },
        )
        .unwrap();
        let req: ChatRequest =
            serde_json::from_value(json!({"model": "alias", "messages": []})).unwrap();
        assert_eq!(p.to_upstream_body(&req)["model"], "real-model");
    }

    #[test]
    fn deepseek_reasoning_content_and_tools_replay_verbatim() {
        // DeepSeek thinking models 400 on tool turns unless the assistant
        // message's reasoning_content is replayed untouched (compliance
        // report deepseek.md; omp client flag replayReasoningContent). The
        // relay's contract: nothing in messages or unknown top-level params
        // is ever stripped.
        let req: ChatRequest = serde_json::from_value(json!({
            "model": "deepseek-reasoner",
            "messages": [
                {"role": "user", "content": "weather?"},
                {"role": "assistant", "content": null,
                 "reasoning_content": "I should call get_weather…",
                 "tool_calls": [{"id": "call_1", "type": "function",
                                  "function": {"name": "get_weather", "arguments": "{\"city\":\"SF\"}"}}]},
                {"role": "tool", "tool_call_id": "call_1", "content": "sunny"}
            ],
            "thinking": {"type": "enabled"},
            "reasoning_effort": "high",
            "tools": [{"type": "function", "function": {"name": "get_weather"}}]
        }))
        .unwrap();
        let p = provider("https://api.deepseek.com");
        let body = p.to_upstream_body(&req);
        assert_eq!(
            body["messages"][1]["reasoning_content"], "I should call get_weather…",
            "reasoning_content must survive the relay byte-for-byte"
        );
        assert_eq!(body["messages"][1]["tool_calls"][0]["id"], "call_1");
        assert_eq!(
            body["messages"][2]["role"], "tool",
            "unknown roles pass through"
        );
        assert_eq!(
            body["thinking"]["type"], "enabled",
            "vendor params ride in extra"
        );
        assert_eq!(body["reasoning_effort"], "high");
        assert_eq!(body["tools"][0]["function"]["name"], "get_weather");
    }

    #[test]
    fn release_endpoint_version_parses_the_semver_triple() {
        // The real update-endpoint shape, field names and all.
        const RELEASE: &str = r#"{"channel":"latest","name":"cli","distribution":"npm","version":"2.0.22","os":"darwin"}"#;
        assert_eq!(
            parse_release_version(RELEASE),
            Some((2, 0, 22)),
            "top-level `version` reads as its first semver triple"
        );
        // Extra segments past the triple still read as the triple.
        assert_eq!(
            parse_release_version(r#"{"version":"2.0.22.7"}"#),
            Some((2, 0, 22))
        );
        // Anything unreadable keeps what it has, never invents a version.
        assert_eq!(parse_release_version("not json"), None);
        assert_eq!(parse_release_version(r#"{"channel":"latest"}"#), None);
        assert_eq!(parse_release_version(r#"{"version":22}"#), None);
        assert_eq!(parse_release_version(r#"{"version":"v-next"}"#), None);
        assert_eq!(parse_release_version(r#"{"version":"2.0"}"#), None);
    }

    #[test]
    fn adopt_version_moves_only_strictly_forward() {
        let live = "opencode/latest/2.0.18/cli";
        assert_eq!(
            adopt_version(live, (2, 0, 22)),
            Some("opencode/latest/2.0.22/cli".to_string()),
            "a newer release renders in the 2.x profile shape"
        );
        assert_eq!(adopt_version(live, (2, 0, 17)), None, "never backward");
        assert_eq!(
            adopt_version(live, (2, 0, 18)),
            None,
            "the same version never re-adopts"
        );
        assert_eq!(
            adopt_version("garbage", (9, 9, 9)),
            None,
            "an unreadable live string changes nothing"
        );
    }

    // ── OpenCode Zen port ────────────────────────────────────────────────

    /// The provider shape this port runs as: the configured key upstream's
    /// free-tier gate treats as the public credential.
    fn zen_provider() -> OpenAiProvider {
        OpenAiProvider::new(
            x2api_transport::Transport::direct(&x2api_kit::ServerConfig::default()),
            ServiceConfig {
                name: "openai".into(),
                base_url: "https://opencode.ai/zen/v1".into(),
                api_key: Some("public".into()),
                api_keys: Vec::new(),
                model_map: Default::default(),
                max_response_bytes: None,
            },
        )
        .expect("a literal https base parses")
    }

    /// A request decorated the way `send()` decorates it, for header asserts.
    fn decorated(profile: &OpenAiProvider) -> reqwest::Request {
        let headers = http::HeaderMap::new();
        let ctx = CallContext::new(9001, Some(&headers));
        profile
            .decorate(reqwest::Client::new().post("https://x.test"), None, &ctx)
            .expect("a configured credential authenticates")
            .0
            .build()
            .expect("request builds")
    }

    /// One SSE record per upstream chunk, the shape transport delivers.
    fn sse_records(records: &[&str]) -> ChunkStream {
        let frames = records
            .iter()
            .map(|record| Ok::<_, String>(bytes::Bytes::from(format!("data: {record}\n\n"))))
            .collect::<Vec<_>>();
        futures_util::stream::iter(frames).boxed()
    }

    fn touch_guard() -> TouchOnDrop {
        let transport = x2api_transport::Transport::direct(&x2api_kit::ServerConfig::default());
        TouchOnDrop {
            lane: transport.lane().unwrap(),
            completed: false,
            transport_error: false,
        }
    }

    #[test]
    fn opencode_identity_headers_follow_the_2x_profile() {
        let first = decorated(&zen_provider());
        let h = first.headers();
        // Shape-pinned, version-live: the agent string follows the published
        // release, so pinning one version here would fight the background
        // refresh. The `opencode/latest/<major.minor.patch>/cli` shape itself
        // stays strict.
        let user_agent = h
            .get("user-agent")
            .and_then(|v| v.to_str().ok())
            .expect("user-agent header");
        assert!(
            user_agent
                .strip_prefix("opencode/latest/")
                .and_then(|rest| rest.strip_suffix("/cli"))
                .and_then(parse_version_triple)
                .is_some(),
            "user-agent must be opencode/latest/<major.minor.patch>/cli: {user_agent}"
        );
        assert_eq!(
            h.get("x-opencode-client").and_then(|v| v.to_str().ok()),
            Some("cli"),
        );
        let project = h
            .get("x-opencode-project")
            .and_then(|v| v.to_str().ok())
            .expect("project header");
        assert_eq!(project.len(), 40, "{project}");
        assert!(project.bytes().all(|b| b.is_ascii_hexdigit()), "{project}");
        let session = h
            .get("x-opencode-session")
            .and_then(|v| v.to_str().ok())
            .expect("session header");
        let tail = session.strip_prefix("ses_").expect("ses_ prefix");
        assert_eq!(tail.len(), 26, "{session}");
        assert!(
            tail[..12].bytes().all(|b| b.is_ascii_hexdigit()),
            "{session}"
        );
        assert!(
            tail[12..].bytes().all(|b| b.is_ascii_alphanumeric()),
            "{session}"
        );
        assert_eq!(h.get("x-session-affinity"), h.get("x-opencode-session"));
        assert_eq!(h.get("x-session-id"), h.get("x-opencode-session"));
        assert_eq!(
            h.get("authorization").and_then(|v| v.to_str().ok()),
            Some("Bearer public"),
        );
        for absent in ["x-opencode-request", "traceparent", "b3"] {
            assert!(
                !h.contains_key(absent),
                "{absent} is not part of the 2.x profile"
            );
        }
        // The mint happens at construction, so a second provider is a second
        // identity — never a per-request re-roll on the one already serving.
        let second = decorated(&zen_provider());
        assert_ne!(
            second.headers().get("x-opencode-session"),
            h.get("x-opencode-session")
        );
        assert_ne!(
            second.headers().get("x-opencode-project"),
            h.get("x-opencode-project")
        );
    }

    #[test]
    fn upstream_body_forces_stream_and_injects_decoys() {
        let req: ChatRequest = serde_json::from_value(json!({
            "model": "m", "messages": [{"role": "user", "content": "x"}]
        }))
        .unwrap();
        let body = provider("https://x.test/v1").to_upstream_body(&req);
        assert_eq!(body["stream"], true, "the gate 403s a buffered request");
        assert_eq!(body["stream_options"]["include_usage"], true);
        let names: Vec<&str> = body["tools"]
            .as_array()
            .expect("the gate reads tool names")
            .iter()
            .map(|tool| tool["function"]["name"].as_str().expect("chat spelling"))
            .collect();
        assert_eq!(
            names,
            ["bash", "read"],
            "exactly the gate names, appended after nothing"
        );
        assert_eq!(body["tool_choice"], "none", "decoys must be unreachable");
    }

    #[test]
    fn existing_tools_survive_and_keep_their_tool_choice() {
        let req: ChatRequest = serde_json::from_value(json!({
            "model": "m",
            "messages": [{"role": "user", "content": "x"}],
            "tools": [{"type": "function", "function": {"name": "get_weather"}}]
        }))
        .unwrap();
        let body = provider("https://x.test/v1").to_upstream_body(&req);
        let names: Vec<&str> = body["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .map(|tool| tool["function"]["name"].as_str().expect("chat spelling"))
            .collect();
        assert_eq!(names[0], "get_weather", "client tools keep their order");
        assert!(
            names.contains(&"bash") && names.contains(&"read"),
            "{names:?}"
        );
        assert!(
            body.get("tool_choice").is_none(),
            "a client with its own tools keeps its own tool_choice or its absence"
        );
    }

    #[test]
    fn responses_style_models_build_a_responses_body() {
        let req: ChatRequest = serde_json::from_value(json!({
            "model": "muse-spark-1.3-contributor-free",
            "messages": [
                {"role": "system", "content": "be terse"},
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
                {"role": "assistant", "content": null, "tool_calls": [
                    {"id": "call_1", "type": "function",
                     "function": {"name": "bash", "arguments": "{\"cmd\":\"ls\"}"}}
                ]},
                {"role": "tool", "tool_call_id": "call_1", "content": "done"}
            ],
            "max_tokens": 8,
            "temperature": 0.2,
            "frequency_penalty": 1.5
        }))
        .unwrap();
        let body = provider("https://x.test/v1").to_upstream_body(&req);
        assert!(
            body.get("messages").is_none(),
            "responses style has no messages key"
        );
        assert_eq!(body["stream"], true);
        assert!(
            body.get("stream_options").is_none(),
            "chat-only knob, it 400s here"
        );

        let input = body["input"].as_array().expect("input item array");
        assert_eq!(input[0]["role"], "system");
        assert_eq!(input[0]["content"][0]["type"], "input_text");
        assert_eq!(input[0]["content"][0]["text"], "be terse");
        assert_eq!(input[1]["content"][0]["type"], "input_text");
        assert_eq!(input[2]["role"], "assistant");
        assert_eq!(input[2]["content"][0]["type"], "output_text");
        let calls: Vec<&Value> = input
            .iter()
            .filter(|item| item["type"] == "function_call")
            .collect();
        assert_eq!(calls.len(), 1, "one function_call per tool call");
        assert_eq!(calls[0]["call_id"], "call_1");
        assert_eq!(calls[0]["name"], "bash");
        assert_eq!(calls[0]["arguments"], "{\"cmd\":\"ls\"}");
        let outputs: Vec<&Value> = input
            .iter()
            .filter(|item| item["type"] == "function_call_output")
            .collect();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0]["call_id"], "call_1");
        assert_eq!(outputs[0]["output"], "done");

        let tools = body["tools"].as_array().expect("flat tools");
        assert_eq!(tools[0]["type"], "function");
        assert_eq!(tools[0]["name"], "bash", "flat spelling");
        assert!(
            tools[0].get("function").is_none(),
            "flat spelling has no nesting"
        );
        assert_eq!(tools[1]["name"], "read");
        assert!(
            body.get("max_tokens").is_none(),
            "max_tokens is a chat spelling the responses API rejects"
        );
        assert!(
            body["max_output_tokens"].as_u64().expect("renamed field") >= 16,
            "the vendor floor is 16"
        );
        assert_eq!(body["temperature"], 0.2, "allowed knob passes through");
        assert!(
            body.get("frequency_penalty").is_none(),
            "chat-only knob dropped"
        );
        assert!(
            body.get("tool_choice").is_none(),
            "tool_choice is never pinned on this style"
        );
    }

    #[test]
    fn chat_style_models_keep_the_chat_body() {
        let req: ChatRequest = serde_json::from_value(json!({
            "model": "mimo-v2.6-flash-free",
            "messages": [{"role": "user", "content": "x"}],
            "temperature": 0.7
        }))
        .unwrap();
        let body = provider("https://x.test/v1").to_upstream_body(&req);
        assert!(body.get("messages").is_some(), "chat style keeps messages");
        assert!(body.get("input").is_none());
        let tools = body["tools"].as_array().expect("chat tools");
        assert_eq!(tools[0]["function"]["name"], "bash", "chat spelling");
        assert_eq!(tools[1]["function"]["name"], "read");
        assert_eq!(body["temperature"], 0.7, "chat extras pass through");
    }

    #[test]
    fn catalogue_lists_only_free_models() {
        let filtered = free_only(json!({
            "object": "list",
            "data": [
                {"id": "mimo-v2.5-free", "object": "model"},
                {"id": "claude-opus-5", "object": "model"},
                {"id": "ling-3.1-flash-free", "object": "model"}
            ]
        }));
        let ids: Vec<&str> = filtered["data"]
            .as_array()
            .expect("data array")
            .iter()
            .map(|model| model["id"].as_str().expect("id"))
            .collect();
        assert_eq!(ids, ["mimo-v2.5-free", "ling-3.1-flash-free"]);
        assert_eq!(filtered["object"], "list", "object wrapper intact");
    }

    /// The live answer for a dead model, envelope verbatim.
    const UNAVAILABLE_ENVELOPE: &str = r#"{"error":{"type":"server_error","message":"Error from provider (Console): Upstream request failed: Model is unavailable."}}"#;

    fn unavailable_error() -> ProviderError {
        let mut err = upstream(400, None);
        err.message =
            "Error from provider (Console): Upstream request failed: Model is unavailable.".into();
        err.error_body = Some(serde_json::from_str(UNAVAILABLE_ENVELOPE).expect("envelope parses"));
        err
    }

    fn listed_ids(value: &Value) -> Vec<&str> {
        value["data"]
            .as_array()
            .expect("data array")
            .iter()
            .map(|model| model["id"].as_str().expect("id"))
            .collect()
    }

    #[test]
    fn unavailable_models_are_withheld_from_the_catalogue_until_the_window_expires() {
        let now = std::time::Instant::now();
        let mut suppressed = HashMap::new();
        assert!(
            note_unavailable_model(&mut suppressed, &unavailable_error(), "mimo-v2.5-dead", now),
            "the live 400 envelope is detected and recorded"
        );
        let listed = json!({
            "object": "list",
            "data": [{"id": "mimo-v2.5-dead"}, {"id": "mimo-v2.5-free"}]
        });
        let hidden = withhold_unavailable(
            listed.clone(),
            &HashMap::new(),
            &suppressed,
            now + std::time::Duration::from_secs(1),
        );
        assert_eq!(listed_ids(&hidden), ["mimo-v2.5-free"]);
        // An expired entry reappears: the filter reads expiry at render time.
        let reappeared = withhold_unavailable(
            listed,
            &HashMap::new(),
            &suppressed,
            now + SUPPRESSION_WINDOW + std::time::Duration::from_secs(1),
        );
        assert_eq!(
            listed_ids(&reappeared),
            ["mimo-v2.5-dead", "mimo-v2.5-free"]
        );
    }

    #[test]
    fn further_unavailable_answers_extend_the_suppression_window() {
        let first = std::time::Instant::now();
        let mut suppressed = HashMap::new();
        assert!(note_unavailable_model(
            &mut suppressed,
            &unavailable_error(),
            "dead",
            first
        ));
        let original = suppressed["dead"];
        let repeat = first + std::time::Duration::from_secs(600);
        assert!(note_unavailable_model(
            &mut suppressed,
            &unavailable_error(),
            "dead",
            repeat
        ));
        assert!(
            suppressed["dead"] > original,
            "a repeat answer pushes the expiry forward"
        );
        let past_original = first + SUPPRESSION_WINDOW + std::time::Duration::from_secs(1);
        let listed = json!({"object": "list", "data": [{"id": "dead"}]});
        let still_hidden =
            withhold_unavailable(listed, &HashMap::new(), &suppressed, past_original);
        assert!(
            listed_ids(&still_hidden).is_empty(),
            "past the ORIGINAL expiry the extended window still hides it"
        );
    }

    #[test]
    fn suppression_reaches_alias_rows_when_model_map_is_set() {
        let mut model_map = HashMap::new();
        model_map.insert("friendly".to_string(), "dead-vendor".to_string());
        let now = std::time::Instant::now();
        let mut suppressed = HashMap::new();
        assert!(note_unavailable_model(
            &mut suppressed,
            &unavailable_error(),
            "dead-vendor",
            now
        ));
        let listed = json!({
            "object": "list",
            "data": [{"id": "dead-vendor"}, {"id": "friendly"}, {"id": "live-vendor"}]
        });
        let hidden = withhold_unavailable(
            listed,
            &model_map,
            &suppressed,
            now + std::time::Duration::from_secs(1),
        );
        assert_eq!(
            listed_ids(&hidden),
            ["live-vendor"],
            "both the vendor row and its alias row drop"
        );
    }

    #[test]
    fn unavailable_detection_reads_both_message_and_error_body() {
        let now = std::time::Instant::now();
        let mut suppressed = HashMap::new();
        // The marker in the message alone (body absent) is detected…
        let mut in_message = upstream(400, None);
        in_message.message =
            "Error from provider (Console): Upstream request failed: Model is unavailable.".into();
        assert!(note_unavailable_model(
            &mut suppressed,
            &in_message,
            "a",
            now
        ));
        // …and in the preserved vendor envelope alone (clean message) too.
        let mut in_body = upstream(400, None);
        in_body.error_body =
            Some(serde_json::from_str(UNAVAILABLE_ENVELOPE).expect("envelope parses"));
        assert!(note_unavailable_model(&mut suppressed, &in_body, "b", now));
        // A 400 without the marker is an ordinary client error.
        assert!(!note_unavailable_model(
            &mut suppressed,
            &upstream(400, None),
            "c",
            now
        ));
        // Status gate: the marker under a 5xx is a vendor fault, not a dead
        // model, and must never withhold anything.
        let mut server_fault = upstream(500, None);
        server_fault.message = "Upstream request failed: Model is unavailable.".into();
        server_fault.error_body =
            Some(serde_json::from_str(UNAVAILABLE_ENVELOPE).expect("envelope parses"));
        assert!(!note_unavailable_model(
            &mut suppressed,
            &server_fault,
            "d",
            now
        ));
    }

    #[test]
    fn the_catalogue_is_served_from_cache_within_the_window_and_refetched_after() {
        let mut cache = CatalogCache::default();
        assert!(
            cache.fresh(CATALOGUE_TTL).is_none(),
            "an empty cache is always a miss"
        );
        cache.store(json!({"object": "list", "data": [{"id": "mimo-v2.5-free"}]}));
        let hit = cache
            .fresh(std::time::Duration::from_secs(10 * 365 * 24 * 3600))
            .expect("within any real window the copy is fresh");
        assert_eq!(listed_ids(&hit), ["mimo-v2.5-free"]);
        assert!(
            cache.fresh(std::time::Duration::ZERO).is_none(),
            "a zero window is always a miss — the fetch path runs"
        );
    }

    #[test]
    fn stale_catalogue_survives_a_failed_refetch() {
        let empty = CatalogCache::default();
        assert!(
            empty.stale().is_none(),
            "before the first success there is nothing to fall back to"
        );
        let mut cache = CatalogCache::default();
        cache.store(json!({"object": "list", "data": [{"id": "mimo-v2.5-free"}]}));
        assert!(
            cache.fresh(std::time::Duration::ZERO).is_none(),
            "the refetch decision: stale by ttl"
        );
        let last_good = cache.stale().expect("last-good copy at any age");
        assert_eq!(listed_ids(&last_good), ["mimo-v2.5-free"]);
    }

    // ── verbatim-lane DONE trim ─────────────────────────────────────────

    /// Run the trim over pushes and flush at EOF: the exact byte stream a
    /// client would receive.
    fn trimmed(pieces: &[&[u8]]) -> Vec<u8> {
        let mut trim = DoneTrim::new();
        let mut out = Vec::new();
        for piece in pieces {
            if let Some(bytes) = trim.push(bytes::Bytes::copy_from_slice(piece)) {
                out.extend_from_slice(&bytes);
            }
        }
        if let Some(tail) = trim.finish() {
            out.extend_from_slice(&tail);
        }
        out
    }

    const TRIM_CHUNK: &[u8] =
        br#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"hi"}}]}"#;
    const TRIM_COST: &[u8] = br#"data: {"choices":[],"cost":"0"}"#;

    #[test]
    fn done_trim_forwards_through_the_terminator_and_drops_the_cost_frame_in_one_push() {
        let mut input = TRIM_CHUNK.to_vec();
        input.extend_from_slice(b"\n\ndata: [DONE]\n\n");
        input.extend_from_slice(TRIM_COST);
        input.extend_from_slice(b"\n\n");
        let mut expected = TRIM_CHUNK.to_vec();
        expected.extend_from_slice(b"\n\ndata: [DONE]\n\n");
        let out = trimmed(&[input.as_slice()]);
        assert_eq!(
            out, expected,
            "pre-marker bytes and the DONE record pass byte-identically; the cost frame drops"
        );
        assert!(!out.windows(TRIM_COST.len()).any(|w| w == TRIM_COST));
    }

    #[test]
    fn done_trim_resumes_a_marker_split_across_pushes() {
        let out = trimmed(&[
            b"hello\ndata: [DON".as_slice(),
            b"E]\n\n".as_slice(),
            b"data: {\"cost\":\"0\"}\n\n".as_slice(),
        ]);
        assert_eq!(
            out,
            b"hello\ndata: [DONE]\n\n".as_slice(),
            "nothing of the marker is lost, and the cost frame after it still drops"
        );
    }

    #[test]
    fn done_trim_flushes_an_unterminated_done_record_at_eof() {
        assert_eq!(
            trimmed(&[b"data: [DONE]".as_slice()]),
            b"data: [DONE]".as_slice(),
            "EOF flushes the held DONE record even with no blank line"
        );
        assert_eq!(
            trimmed(&[b"data: [DONE]\n".as_slice()]),
            b"data: [DONE]\n".as_slice(),
            "a line end alone is still forwarded; nothing after the marker ever is"
        );
        // Held bytes plus late content: the content after the marker drops,
        // the marker flushes.
        assert_eq!(
            trimmed(&[b"data: [DONE]".as_slice(), b"data: {\"cost\":0}".as_slice()]),
            b"data: [DONE]".as_slice()
        );
    }

    #[test]
    fn done_trim_passes_plain_streams_through_byte_for_byte() {
        let plain: &[u8] = b"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" there\"}}]}\n\n";
        // One push: nothing held back at all.
        assert_eq!(trimmed(&[plain]), plain.to_vec());
        // Split arbitrarily: outputs reassemble to the input exactly — the
        // trim only DELAYS bytes (at most marker.len()-1 of tail-hold),
        // never adds or removes them, while no marker exists.
        let pieces: [&[u8]; 4] = [&plain[..5], &plain[5..20], &plain[20..21], &plain[21..]];
        assert_eq!(trimmed(&pieces), plain.to_vec());
        // A tail that could START the marker is held, then flushed at EOF —
        // still byte-for-byte, because no complete marker ever arrived.
        let partial = b"payload then data: [D";
        assert_eq!(trimmed(&[partial.as_slice()]), partial.to_vec());
    }

    #[test]
    fn done_trim_drops_bytes_after_the_terminator_across_pushes() {
        let out = trimmed(&[
            b"data: [DONE]\n\n".as_slice(),
            TRIM_COST,
            b"data: another-frame\n\n".as_slice(),
            b"later pushes too".as_slice(),
        ]);
        assert_eq!(
            out,
            b"data: [DONE]\n\n".as_slice(),
            "everything after the terminator drops, in this push and every later one"
        );
    }

    /// The lane guard's verdict rides the TRIMMED bytes: a stream whose DONE
    /// (and only its DONE) reaches the decoder completes; a stream without
    /// one stays use-only, never a false success.
    #[test]
    fn trimmed_relay_completes_the_lane_only_when_done_reached_the_trim() {
        use x2api_kit::sse::SseDecoder;
        let transport = x2api_transport::Transport::direct(&x2api_kit::ServerConfig::default());
        let guard = || TouchOnDrop {
            lane: transport.lane().unwrap(),
            completed: false,
            transport_error: false,
        };

        let mut first = guard();
        let mut trim = DoneTrim::new();
        let mut decoder = SseDecoder::new();
        let mut events = Vec::new();
        let mut forwarded = Vec::new();
        for piece in [TRIM_CHUNK, b"\n\ndata: [DONE]\n\n".as_slice(), TRIM_COST] {
            if let Some(out) = trim.push(bytes::Bytes::copy_from_slice(piece)) {
                observe_terminal(&mut decoder, &mut events, &out, &mut first);
                forwarded.extend_from_slice(&out);
            }
        }
        if let Some(tail) = trim.finish() {
            observe_terminal(&mut decoder, &mut events, &tail, &mut first);
            forwarded.extend_from_slice(&tail);
        }
        assert!(first.completed, "DONE reached the decoder through the trim");
        assert!(
            !forwarded.windows(TRIM_COST.len()).any(|w| w == TRIM_COST),
            "the cost frame never reaches the decoder or a client"
        );

        // No DONE anywhere: use-only, exactly like a downstream cut.
        let mut second = guard();
        let mut trim = DoneTrim::new();
        let mut decoder = SseDecoder::new();
        let mut events = Vec::new();
        for piece in [TRIM_CHUNK, b"\n\n".as_slice()] {
            if let Some(out) = trim.push(bytes::Bytes::copy_from_slice(piece)) {
                observe_terminal(&mut decoder, &mut events, &out, &mut second);
            }
        }
        if let Some(tail) = trim.finish() {
            observe_terminal(&mut decoder, &mut events, &tail, &mut second);
        }
        assert!(!second.completed, "a stream without DONE is not a success");
        assert!(!second.transport_error, "and not a lane fault either");
    }

    #[tokio::test]
    async fn responses_sse_folds_to_ir_chunks_and_ends_at_the_terminal() {
        let records = [
            r#"{"type":"response.created","response":{"id":"resp_test123","model":"muse-spark-1.3-contributor-free","status":"in_progress"}}"#,
            r#"{"type":"response.in_progress"}"#,
            r#"{"type":"response.output_text.delta","delta":"Hello"}"#,
            r#"{"type":"response.output_text.delta","delta":" world"}"#,
            r#"{"type":"response.completed","response":{"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}"#,
            r#"{"type":"ping","cost":"0"}"#,
        ];
        let mut decoded = decode_responses_sse(
            sse_records(&records),
            64 * 1024,
            "vendor-fallback-model",
            touch_guard(),
        )
        .boxed();
        let mut items = Vec::new();
        while let Some(item) = decoded.next().await {
            items.push(item);
        }
        let chunks: Vec<ChatChunk> = items
            .into_iter()
            .collect::<Result<_, _>>()
            .expect("clean terminal");
        let text: String = chunks.iter().map(|chunk| chunk.text.as_str()).collect();
        assert_eq!(text, "Hello world");
        assert!(
            chunks.iter().all(|chunk| chunk.id == "resp_test123"),
            "every chunk carries the response id from the created event: {chunks:?}"
        );
        assert!(
            chunks
                .iter()
                .all(|chunk| { chunk.model == "muse-spark-1.3-contributor-free" }),
            "every chunk carries the response model from the created event: {chunks:?}"
        );
        assert_eq!(
            chunks.len(),
            3,
            "two text deltas plus exactly one terminal; the ping never yields"
        );
        let last = chunks.last().expect("terminal chunk");
        assert_eq!(last.finish_reason.as_deref(), Some("stop"));
        let usage = last.usage.expect("terminal usage");
        assert_eq!(usage.prompt_tokens, 5);
        assert_eq!(usage.completion_tokens, 2);
        assert_eq!(usage.total_tokens, Some(7));
        assert!(
            chunks.iter().all(|chunk| chunk.raw.is_none()
                && !chunk.text.contains("cost")
                && !chunk.reasoning.contains("cost")),
            "opencode's cost bookkeeping never reaches a client"
        );
    }

    #[tokio::test]
    async fn responses_tool_call_deltas_assemble_by_index() {
        let records = [
            r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"call_1","name":"bash"}}"#,
            r#"{"type":"response.function_call_arguments.delta","output_index":0,"delta":"{"}"#,
            r#"{"type":"response.function_call_arguments.delta","output_index":0,"delta":"\"ls\"}"}"#,
            r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":"call_1","name":"bash","arguments":"{\"ls\"}"}}"#,
        ];
        let mut decoded = decode_responses_sse(
            sse_records(&records),
            64 * 1024,
            "vendor-fallback-model",
            touch_guard(),
        )
        .boxed();
        let mut items = Vec::new();
        while let Some(item) = decoded.next().await {
            items.push(item);
        }
        let oks: Vec<ChatChunk> = items
            .iter()
            .filter_map(|item| item.as_ref().ok().cloned())
            .collect();
        assert_eq!(
            oks.len(),
            3,
            "added + two deltas; the done event must not re-emit arguments: {oks:?}"
        );
        let first = &oks[0].tool_calls[0];
        assert_eq!(first.id.as_deref(), Some("call_1"));
        assert_eq!(first.name.as_deref(), Some("bash"));
        let arguments: String = oks[1..]
            .iter()
            .flat_map(|chunk| &chunk.tool_calls)
            .map(|delta| delta.arguments.as_str())
            .collect();
        assert_eq!(arguments, r#"{"ls"}"#);
        assert!(
            oks.iter()
                .all(|chunk| chunk.model == "vendor-fallback-model"),
            "no response object ever appears here, so the caller's vendor id is every chunk's model: {oks:?}"
        );
        assert!(
            items.last().expect("stream ends").is_err(),
            "no terminal event: truncation, never a clean end"
        );
    }

    #[tokio::test]
    async fn responses_done_event_supplies_arguments_when_no_deltas_arrived() {
        let records = [
            r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"call_1","name":"bash"}}"#,
            r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":"call_1","name":"bash","arguments":"{\"cmd\":\"ls\"}"}}"#,
        ];
        let mut decoded = decode_responses_sse(
            sse_records(&records),
            64 * 1024,
            "vendor-fallback-model",
            touch_guard(),
        )
        .boxed();
        let mut items = Vec::new();
        while let Some(item) = decoded.next().await {
            items.push(item);
        }
        let oks: Vec<ChatChunk> = items
            .iter()
            .filter_map(|item| item.as_ref().ok().cloned())
            .collect();
        assert_eq!(oks.len(), 2, "added, then the done-supplied delta: {oks:?}");
        let tail = &oks[1].tool_calls;
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].arguments, r#"{"cmd":"ls"}"#);
    }

    #[tokio::test]
    async fn responses_failed_ends_without_a_terminal_and_keeps_vendor_text_out_of_the_error() {
        let records = [
            r#"{"type":"response.output_text.delta","delta":"partial"}"#,
            r#"{"type":"response.failed","response":{"error":{"message":"muse internals exploded"}}}"#,
        ];
        let mut decoded = decode_responses_sse(
            sse_records(&records),
            64 * 1024,
            "vendor-fallback-model",
            touch_guard(),
        )
        .boxed();
        let mut items = Vec::new();
        while let Some(item) = decoded.next().await {
            items.push(item);
        }
        let oks: Vec<ChatChunk> = items
            .iter()
            .filter_map(|item| item.as_ref().ok().cloned())
            .collect();
        assert_eq!(oks.len(), 1, "failure yields no finish_reason chunk");
        assert_eq!(oks[0].text, "partial");
        assert!(
            oks[0].finish_reason.is_none(),
            "truncation must not look complete"
        );
        let error = items
            .last()
            .expect("the failure terminates the stream")
            .as_ref()
            .expect_err("and it does so with an error, not a chunk");
        assert!(
            !error.message.contains("muse internals exploded"),
            "vendor text belongs in logs only: {}",
            error.message
        );
    }

    #[tokio::test]
    async fn complete_folds_a_forced_chat_stream() {
        let records = [
            r#"{"id":"c1","model":"m","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"plan "},"finish_reason":null}]}"#,
            r#"{"id":"c1","model":"m","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}"#,
            r#"{"id":"c1","model":"m","choices":[{"index":0,"delta":{"content":" world"},"finish_reason":null}]}"#,
            r#"{"id":"c1","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"length"}],"usage":{"prompt_tokens":9,"completion_tokens":4,"total_tokens":13}}"#,
            "[DONE]",
            r#"{"choices":[],"cost":"0.42"}"#,
        ];
        let mut decoded =
            decode_upstream_sse(sse_records(&records), 64 * 1024, None, touch_guard()).boxed();
        let mut items = Vec::new();
        while let Some(item) = decoded.next().await {
            items.push(item);
        }
        let chunks: Vec<ChatChunk> = items
            .into_iter()
            .collect::<Result<_, _>>()
            .expect("[DONE] ends clean");
        assert_eq!(
            chunks.len(),
            4,
            "the stream ends at [DONE]; the trailing cost frame never yields"
        );
        assert!(
            chunks.iter().all(|chunk| !String::from_utf8_lossy(
                chunk.raw.as_ref().expect("chat frames keep raw")
            )
            .contains("cost")),
            "nothing from the post-[DONE] cost frame may surface"
        );
        let completion = fold_completion(
            futures_util::stream::iter(chunks.into_iter().map(Ok::<_, ProviderError>)),
            None,
        )
        .await
        .expect("a [DONE]-terminated stream folds");
        assert_eq!(completion.text, "Hello world");
        assert_eq!(completion.reasoning.as_deref(), Some("plan "));
        assert_eq!(completion.finish_reason.as_deref(), Some("length"));
        let usage = completion.usage.expect("usage chunk");
        assert_eq!(usage.prompt_tokens, 9);
        assert_eq!(usage.completion_tokens, 4);
        assert_eq!(usage.total_tokens, Some(13));
        assert_eq!(completion.id, "c1");
        assert_eq!(completion.model, "m");
    }

    #[test]
    fn responses_buffered_json_folds_to_completion() {
        let completion = completion_from_responses(json!({
            "id": "resp_1",
            "model": "muse-spark-1.3-contributor-free",
            "status": "completed",
            "output": [
                {"type": "message", "role": "assistant",
                 "content": [{"type": "output_text", "text": "pong"}]},
                {"type": "function_call", "call_id": "call_2", "name": "read", "arguments": "{}"}
            ],
            "usage": {"input_tokens": 5, "output_tokens": 1}
        }),
            "muse-spark-1.3-contributor-free",
        )
        .expect("a buffered responses object parses");
        assert_eq!(completion.id, "resp_1");
        assert_eq!(completion.model, "muse-spark-1.3-contributor-free");
        assert_eq!(completion.text, "pong");
        assert_eq!(completion.tool_calls.len(), 1);
        assert_eq!(completion.tool_calls[0].id, "call_2");
        assert_eq!(completion.tool_calls[0].name, "read");
        assert_eq!(completion.tool_calls[0].arguments, "{}");
        let usage = completion.usage.expect("usage");
        assert_eq!(usage.prompt_tokens, 5);
        assert_eq!(usage.completion_tokens, 1);
        assert_eq!(completion.finish_reason.as_deref(), Some("stop"));

        let incomplete = completion_from_responses(
            json!({
                "id": "resp_2",
                "model": "muse-spark-1.3-contributor-free",
                "status": "incomplete",
                "output": [],
                "usage": {"input_tokens": 1, "output_tokens": 1}
            }),
            "muse-spark-1.3-contributor-free",
        )
        .expect("an incomplete response parses too");
        assert_eq!(incomplete.finish_reason.as_deref(), Some("length"));

        // An object without a model falls back to the id the caller asked
        // upstream for — an empty model must never reach a client.
        let fallback = completion_from_responses(
            json!({"id": "resp_3", "status": "completed", "output": []}),
            "muse-spark-1.2-contributor-free",
        )
        .expect("a model-less responses object still parses");
        assert_eq!(fallback.model, "muse-spark-1.2-contributor-free");
    }
}
