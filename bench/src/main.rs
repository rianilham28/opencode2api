//! x2api-bench — comparative throughput AND pace, not a load test.
//!
//! One mock upstream streams OpenAI-shape SSE; the SAME proxy (real router,
//! real relay, real reqwest/hyper over loopback TCP) is driven in its three
//! consumption modes, next to a no-proxy baseline. The point is the DELTA:
//! `mock` is what bytes-in-motion cost with no proxy at all; `chat` shows
//! what the verbatim relay adds (framing copies only — no parse); and
//! `messages`/`responses`/`gem` show what translating per chunk inherently
//! costs — one row per bridge dialect (Anthropic `messages`, OpenAI
//! `responses`, Gemini `generateContent`), because you cannot fold a frame
//! you refuse to read. A sanity probe runs before every timed pass and the
//! bench refuses to print numbers it cannot trust — a fast wrong path is
//! still wrong.
//!
//! What each column is for, because a throughput number alone hid a real bug
//! here once (the verbatim relay buffered whole streams and still posted the
//! best MiB/s in the table):
//! - **µs/frame** is the primary figure — median of `--repeats` passes, with
//!   the observed min–max, because one pass of this bench swings ±40%.
//! - **MiB/s** is per-variant only. The dialects emit different byte volumes
//!   for the same frame, so comparing it ACROSS rows compares verbosity.
//! - **ttfb** and **chunks/resp** are the pace columns: a proxy that
//!   accumulates instead of streaming keeps its throughput and loses these.
//! - **allocs/frame** counts allocations made ON THE PROXY'S OWN RUNTIME
//!   THREADS only (the client and mock live on the main runtime and are not
//!   counted). It is machine-independent and barely moves with load — the
//!   number to watch when optimizing the fold path.
//!
//! Read the timings as comparative ON THIS MACHINE (release build, mimalloc
//! like the shipped bins); they are not SLO figures.
//!
//!   cargo run --release -p x2api-bench -- [--frames N] [--responses N]
//!       [--repeats N] [-c N] [--pace] [--pace-us N] [--json]
//!
//! `--pace` makes the mock write one frame per `write` (what a real upstream
//! does); `--pace-us N` adds a delay between them, but the runtime's timer
//! granularity is the floor — on macOS a 50 µs request sleeps ~1 ms, so read
//! a delayed run's `µs/frame` as the MOCK's pacing and look at `ttfb` and
//! `chunks/resp` for the proxy's behavior.
//!
//! Legacy positional form (`-- 2000 25` = frames, responses) still works.

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

// ── allocation accounting ────────────────────────────────────────────────

thread_local! {
    /// Set once on each proxy-runtime worker thread. `const`-initialized so
    /// reading it inside the allocator can never itself allocate.
    static PROXY_THREAD: Cell<bool> = const { Cell::new(false) };
}

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

fn counting() -> bool {
    PROXY_THREAD.try_with(Cell::get).unwrap_or(false)
}

/// mimalloc (what the shipped bins use) plus a counter that only fires on
/// proxy worker threads. Counting the whole process would fold the bench's
/// own client and mock into the number and make it unreadable.
struct Counting;

unsafe impl std::alloc::GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        if counting() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { std::alloc::GlobalAlloc::alloc(&mimalloc::MiMalloc, layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        unsafe { std::alloc::GlobalAlloc::dealloc(&mimalloc::MiMalloc, ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new: usize) -> *mut u8 {
        if counting() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(new.saturating_sub(layout.size()) as u64, Ordering::Relaxed);
        }
        unsafe { std::alloc::GlobalAlloc::realloc(&mimalloc::MiMalloc, ptr, layout, new) }
    }
    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        if counting() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { std::alloc::GlobalAlloc::alloc_zeroed(&mimalloc::MiMalloc, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn alloc_snapshot() -> (u64, u64) {
    (
        ALLOCS.load(Ordering::Relaxed),
        ALLOC_BYTES.load(Ordering::Relaxed),
    )
}

// ── knobs ────────────────────────────────────────────────────────────────

struct Args {
    frames: usize,
    responses: usize,
    repeats: usize,
    /// In-flight requests per pass. `-c 1,8,64` sweeps: the whole matrix runs
    /// once per level, which is how you see SCALING rather than one point on
    /// a curve.
    concurrency: Vec<usize>,
    /// Write the mock's body one frame per `write`, the way a real upstream
    /// delivers it. Off, the whole body goes out in one `write_all` and the
    /// proxy sees ~40 KB reads — flattering every per-frame cost in the table.
    /// With `pace_us` the delay cannot go below the runtime timer's
    /// granularity (~1 ms on macOS), so a paced+delayed run measures the
    /// mock's clock, not the proxy's speed: read `ttfb`/`chunks` there.
    pace: bool,
    pace_us: u64,
    proxy_threads: usize,
    json: bool,
    /// Measure the BUFFERED path (`stream:false`) instead of the streamed
    /// one: parse, one upstream round trip, one rendered JSON document. It is
    /// a different code path end to end — `complete()` and each bridge's
    /// `completion_to_*` — and the streamed table says nothing about it.
    buffered: bool,
    payload: Payload,
    /// A previous `--json` run to compare against; regressions beyond
    /// `max_regress_pct` on `µs/unit` exit non-zero.
    baseline: Option<String>,
    /// Keep the historical skip behavior for exploratory shape-mismatched
    /// comparisons. The default is to fail the gate.
    allow_shape_mismatch: bool,
    max_regress_pct: f64,
    /// Handshake cost the bench's mock proxy charges per CONNECTION. 0 keeps
    /// the lane row a pure overhead measurement; turn it up (say 50) and the
    /// warm pool's whole reason to exist shows up in `cold ttfb`.
    proxy_connect_ms: u64,
    /// Build the lane WITHOUT pre-warming, so the first request pays the
    /// handshake instead of boot doing it.
    lane_cold: bool,
}

impl Args {
    fn parse() -> anyhow::Result<Self> {
        let mut a = Self {
            frames: 2000,
            responses: 25,
            repeats: 5,
            concurrency: vec![1],
            pace: false,
            pace_us: 0,
            proxy_threads: 2,
            json: false,
            buffered: false,
            payload: Payload::Ascii,
            baseline: None,
            allow_shape_mismatch: false,
            max_regress_pct: 10.0,
            proxy_connect_ms: 0,
            lane_cold: false,
        };
        let argv: Vec<String> = std::env::args().skip(1).collect();
        let mut positional = 0usize;
        let mut i = 0usize;
        while i < argv.len() {
            let arg = argv[i].clone();
            let value = |i: &mut usize| -> anyhow::Result<u64> {
                *i += 1;
                argv.get(*i)
                    .and_then(|v| v.parse::<u64>().ok())
                    .ok_or_else(|| anyhow::anyhow!("{arg} needs a number"))
            };
            match argv[i].as_str() {
                "--frames" => a.frames = value(&mut i)? as usize,
                "--responses" => a.responses = value(&mut i)? as usize,
                "--repeats" => a.repeats = value(&mut i)? as usize,
                "--concurrency" | "-c" => {
                    i += 1;
                    let raw = argv
                        .get(i)
                        .ok_or_else(|| anyhow::anyhow!("-c needs a level or a comma list"))?;
                    a.concurrency = raw
                        .split(',')
                        .map(|v| {
                            v.trim()
                                .parse::<usize>()
                                .map_err(|_| anyhow::anyhow!("-c: {v:?} is not a number"))
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    anyhow::ensure!(!a.concurrency.is_empty(), "-c needs at least one level");
                }
                "--proxy-threads" => a.proxy_threads = value(&mut i)? as usize,
                "--pace-us" => {
                    a.pace_us = value(&mut i)?;
                    a.pace = true;
                }
                "--pace" => a.pace = true,
                "--json" => a.json = true,
                "--buffered" => a.buffered = true,
                "--lane-cold" => a.lane_cold = true,
                "--proxy-connect-ms" => a.proxy_connect_ms = value(&mut i)?,
                "--payload" => {
                    i += 1;
                    a.payload =
                        Payload::parse(argv.get(i).map(String::as_str).unwrap_or_default())?;
                }
                "--baseline" => {
                    i += 1;
                    a.baseline = Some(
                        argv.get(i)
                            .cloned()
                            .ok_or_else(|| anyhow::anyhow!("--baseline needs a path"))?,
                    );
                }
                "--allow-shape-mismatch" => a.allow_shape_mismatch = true,
                "--max-regress-pct" => {
                    i += 1;
                    a.max_regress_pct = argv
                        .get(i)
                        .and_then(|v| v.parse().ok())
                        .ok_or_else(|| anyhow::anyhow!("--max-regress-pct needs a number"))?;
                }
                "-h" | "--help" => {
                    println!(
                        "x2api-bench [--frames N] [--responses N] [--repeats N] [-c N]\n\
                         \x20 [--pace] [--pace-us N] [--proxy-threads N] [--buffered]\n\
                         \x20 [--payload ascii|unicode|tools] [--json]\n\
                         \x20 [--proxy-connect-ms N] [--lane-cold]\n\
                         \x20 [--baseline run.json] [--max-regress-pct N]\n\
                         \x20 [--allow-shape-mismatch]\n\
                         allow-shape-mismatch: skip exploratory baseline comparisons\n\
                         with different frames/responses/mode/payload (default: fail)"
                    );
                    std::process::exit(0);
                }
                other => match (positional, other.parse::<usize>()) {
                    (0, Ok(v)) => {
                        a.frames = v;
                        positional += 1;
                    }
                    (1, Ok(v)) => {
                        a.responses = v;
                        positional += 1;
                    }
                    _ => anyhow::bail!("unrecognized argument {other:?}"),
                },
            }
            i += 1;
        }
        a.frames = a.frames.clamp(10, 200_000);
        a.responses = a.responses.clamp(3, 10_000);
        a.repeats = a.repeats.clamp(1, 100);
        for c in &mut a.concurrency {
            *c = (*c).clamp(1, 512).min(a.responses);
        }
        a.concurrency.dedup();
        a.proxy_threads = a.proxy_threads.clamp(1, 64);
        Ok(a)
    }

    /// What one measured unit IS: a streamed frame, or a whole buffered
    /// request. Every per-unit column below divides by this.
    fn units_per_response(&self) -> usize {
        if self.buffered { 1 } else { self.frames }
    }

    fn unit_label(&self) -> &'static str {
        if self.buffered {
            "µs/req"
        } else {
            "µs/frame"
        }
    }

    fn mock_label(&self) -> &'static str {
        match (self.pace, self.pace_us > 0) {
            (false, _) => "bulk",
            (true, false) => "paced",
            (true, true) => "paced+delay",
        }
    }
}

const PAD: &str = "the quick brown fox jumps over the lazy dog 0123456789";

/// What a delta frame carries. The default is plain ASCII, which is the
/// FRIENDLIEST case for every JSON path in the proxy — worth having, but a
/// bench that only measures it overstates the fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Payload {
    /// Unescaped ASCII: nothing to escape on the way out, nothing to unescape
    /// on the way in.
    Ascii,
    /// Non-Latin text, an em dash, an emoji and an embedded quote — so the
    /// serializer actually escapes and the decoder actually carries
    /// multi-byte characters across chunk boundaries.
    Unicode,
    /// Text alongside a `tool_calls` delta: the shape whose extra fields ride
    /// `raw` verbatim on the chat dialect and are invisible to the bridges.
    Tools,
}

impl Payload {
    fn parse(v: &str) -> anyhow::Result<Self> {
        Ok(match v {
            "ascii" => Self::Ascii,
            "unicode" => Self::Unicode,
            "tools" => Self::Tools,
            other => anyhow::bail!("unknown --payload {other:?} (ascii|unicode|tools)"),
        })
    }

    fn label(self) -> &'static str {
        match self {
            Self::Ascii => "ascii",
            Self::Unicode => "unicode",
            Self::Tools => "tools",
        }
    }

    /// The JSON-escaped text a delta carries.
    fn text(self) -> &'static str {
        match self {
            Self::Ascii | Self::Tools => PAD,
            // Already escaped as it will appear on the wire: the quote and
            // the multi-byte characters are the point.
            Self::Unicode => {
                // `\\ud83d\\ude80` is the escape as it travels on the wire (a
                // surrogate pair for an emoji); the em dash is a raw
                // multi-byte character, so a chunk boundary can land inside it.
                "halo dunia — ini teks dengan emoji \\ud83d\\ude80 dan tanda \\\"kutip\\\" 0123456789"
            }
        }
    }

    /// Extra delta fields, if this shape carries any.
    fn delta_extra(self) -> &'static str {
        match self {
            Self::Tools => {
                ",\"tool_calls\":[{\"index\":0,\"id\":\"call_bench\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"city\\\":\\\"jakarta\\\"}\"}}]"
            }
            _ => "",
        }
    }
}

/// The frames a response is made of, kept separate so the paced mock can
/// write them one at a time.
fn sse_frames(frames: usize, payload: Payload) -> Arc<Vec<Vec<u8>>> {
    let (text, extra) = (payload.text(), payload.delta_extra());
    let frame = format!(
        "data: {{\"id\":\"bench\",\"model\":\"bench\",\"object\":\"chat.completion.chunk\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{text}\"{extra}}},\"finish_reason\":null}}]}}\n\n"
    );
    let mut out: Vec<Vec<u8>> = (0..frames).map(|_| frame.clone().into_bytes()).collect();
    out.push(b"data: [DONE]\n\n".to_vec());
    // The post-[DONE] vendor-bookkeeping frame (a `cost` record): every
    // proxy variant must swallow it; the sanity checks grep for it in the
    // received tail.
    out.push(b"data: {\"cost\":0.001}\n\n".to_vec());
    Arc::new(out)
}

/// The buffered reply the mock serves a `stream:false` request: one
/// `chat.completion` whose text is the same volume the streamed variant
/// sends, so the two modes are comparable in bytes moved.
fn buffered_body(frames: usize, payload: Payload) -> Arc<Vec<u8>> {
    let mut text = String::with_capacity(frames * payload.text().len());
    for _ in 0..frames {
        text.push_str(payload.text());
    }
    Arc::new(
        format!(
            "{{\"id\":\"bench\",\"model\":\"bench\",\"object\":\"chat.completion\",\"choices\":[{{\"index\":0,\"message\":{{\"role\":\"assistant\",\"content\":\"{text}\"}},\"finish_reason\":\"stop\"}}],\"usage\":{{\"prompt_tokens\":7,\"completion_tokens\":{frames}}}}}"
        )
        .into_bytes(),
    )
}

// ── mock upstream ────────────────────────────────────────────────────────

/// Raw-socket mock: reads each full request (headers, then content-length
/// body) before answering — answering early aborts the client's upload —
/// then streams the body and closes. The body
/// is close-delimited (no content-length, no chunked framing), so the
/// connection genuinely must close; every response pays a fresh accept.
async fn mock_upstream(
    frames: Arc<Vec<Vec<u8>>>,
    whole: Arc<Vec<u8>>,
    buffered: Arc<Vec<u8>>,
    pace: bool,
    pace_us: u64,
) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            // Paced writes are only observable as pace if they leave now.
            let _ = sock.set_nodelay(true);
            let frames = frames.clone();
            let whole = whole.clone();
            let buffered = buffered.clone();
            tokio::spawn(async move {
                let mut buf: Vec<u8> = Vec::new();
                let mut header_end: Option<usize> = None;
                let mut want = 0usize;
                loop {
                    let mut tmp = [0u8; 8192];
                    let n = match sock.read(&mut tmp).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    buf.extend_from_slice(&tmp[..n]);
                    if header_end.is_none()
                        && let Some(pos) = memchr::memmem::find(&buf, b"\r\n\r\n")
                    {
                        let headers = String::from_utf8_lossy(&buf[..pos]).to_lowercase();
                        want = headers
                            .lines()
                            .find_map(|l| {
                                l.strip_prefix("content-length:")
                                    .and_then(|v| v.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        header_end = Some(pos + 4);
                    }
                    if let Some(he) = header_end
                        && buf.len() - he >= want
                    {
                        break;
                    }
                }
                // The client's own flag decides the mode, exactly as a real
                // upstream decides it: a `stream:false` body gets one JSON
                // document, with a content-length, on a keep-alive-shaped
                // reply that still closes (the pool is not what this measures).
                if memchr::memmem::find(&buf, br#""stream":false"#).is_some() {
                    let head = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        buffered.len()
                    );
                    let _ = sock.write_all(head.as_bytes()).await;
                    let _ = sock.write_all(&buffered).await;
                    let _ = sock.shutdown().await;
                    return;
                }
                let head = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-cache\r\nconnection: close\r\n\r\n";
                if sock.write_all(head).await.is_err() {
                    return;
                }
                if pace {
                    for f in frames.iter() {
                        if sock.write_all(f).await.is_err() {
                            return;
                        }
                        if pace_us > 0 {
                            tokio::time::sleep(Duration::from_micros(pace_us)).await;
                        }
                    }
                } else {
                    let _ = sock.write_all(&whole).await;
                }
                let _ = sock.shutdown().await;
            });
        }
    });
    addr
}

/// HTTP/1.1 chunked framing for one body: `<len hex>\r\n<data>\r\n` per
/// piece. Hand-rolled like every other mock here.
async fn write_chunks<'a>(
    sock: &mut tokio::net::TcpStream,
    pieces: impl Iterator<Item = &'a [u8]>,
) -> bool {
    for piece in pieces {
        if sock
            .write_all(format!("{:x}\r\n", piece.len()).as_bytes())
            .await
            .is_err()
            || sock.write_all(piece).await.is_err()
            || sock.write_all(b"\r\n").await.is_err()
        {
            return false;
        }
    }
    true
}

/// Mock HTTP proxy. Accepts the absolute-form request a proxied `reqwest`
/// sends, optionally spends `connect_ms` first, then answers the way the
/// upstream would: SSE for a streamed request, one JSON document for a
/// `stream:false` one.
///
/// It does not forward, because forwarding is not what this measures: the
/// question is what a LANE costs the proxy, and `connect_ms` stands in for
/// the CONNECT + TLS handshake a real vendor charges on every new
/// connection. Turn it up and the difference between a warm pool and a cold
/// one stops being an argument.
async fn mock_proxy(
    frames: Arc<Vec<Vec<u8>>>,
    whole: Arc<Vec<u8>>,
    buffered: Arc<Vec<u8>>,
    connect_ms: u64,
    pace: bool,
) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _ = sock.set_nodelay(true);
            let (frames, whole, buffered) = (frames.clone(), whole.clone(), buffered.clone());
            tokio::spawn(async move {
                // Charged once per CONNECTION, which is the whole point: a
                // pooled lane pays it once, a fresh one pays it per request.
                if connect_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(connect_ms)).await;
                }
                // KEEP-ALIVE, unlike the upstream mock: a close-delimited
                // body forces a new connection per request, which would make
                // every lane pay the handshake every time and leave this row
                // unable to say anything about pooling at all. Chunked
                // framing is also what real SSE upstreams use.
                //
                // One COMPLETE request — head and body both — is answered at a
                // time, because which reply is correct is decided by the
                // `stream` flag inside the body, and a keep-alive connection's
                // TCP segments do not line up with its requests. Bytes read
                // past the request being answered stay buffered for the next.
                let mut req: Vec<u8> = Vec::new();
                let mut tmp = [0u8; 8192];
                loop {
                    let (head_end, want) = loop {
                        let n = match sock.read(&mut tmp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        req.extend_from_slice(&tmp[..n]);
                        let Some(pos) = memchr::memmem::find(&req, b"\r\n\r\n") else {
                            continue;
                        };
                        let headers = String::from_utf8_lossy(&req[..pos]).to_lowercase();
                        let want = headers
                            .lines()
                            .find_map(|l| {
                                l.strip_prefix("content-length:")
                                    .and_then(|v| v.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if req.len() >= pos + 4 + want {
                            break (pos + 4, want);
                        }
                    };
                    let request = &req[..head_end + want];
                    let is_head = request.starts_with(b"HEAD ");
                    let unary = memchr::memmem::find(request, br#""stream":false"#).is_some();
                    req.drain(..head_end + want);
                    // A HEAD gets headers only, as HTTP requires — and as a
                    // real vendor's proxy does, which is what lets a warm
                    // lane's probe hand its connection back to the pool.
                    if is_head {
                        let head = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: 0\r\n\r\n";
                        if sock.write_all(head).await.is_err() {
                            return;
                        }
                        continue;
                    }
                    // An SSE body on a buffered request is unparsable JSON, so
                    // the proxy can only call it a bad gateway — the reply's
                    // shape is the request's choice, not this mock's default.
                    // Content-length (never `connection: close`) hands the
                    // connection back to the pool, so the buffered row prices
                    // a warm lane exactly like the streamed one does.
                    if unary {
                        let head = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                            buffered.len()
                        );
                        if sock.write_all(head.as_bytes()).await.is_err()
                            || sock.write_all(&buffered).await.is_err()
                        {
                            return;
                        }
                        continue;
                    }
                    let head = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n";
                    if sock.write_all(head).await.is_err() {
                        return;
                    }
                    let ok = if pace {
                        write_chunks(&mut sock, frames.iter().map(|f| f.as_slice())).await
                    } else {
                        write_chunks(&mut sock, std::iter::once(whole.as_slice())).await
                    };
                    if !ok || sock.write_all(b"0\r\n\r\n").await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    addr
}

// ── the fidelity lane's provider ─────────────────────────────────────────

/// Declares the chat dialect native and answers `relay_raw` by forwarding the
/// client's bytes to the same mock. Chat is not the lane's real use (that is a
/// native-Anthropic vendor), but it is the dialect whose FOLD path this bench
/// already measures — so the row below is a like-for-like comparison against
/// the verbatim relay, with the same mock, same body, same client.
struct NativeChat {
    inner: service::OpenAiProvider,
    client: reqwest::Client,
    /// Parsed once, like the provider's own endpoints: `post(&str)` would
    /// re-parse this on every request.
    url: reqwest::Url,
}

#[async_trait::async_trait]
impl x2api_kit::Provider for NativeChat {
    fn name(&self) -> &'static str {
        "native-chat"
    }

    fn native_dialects(&self) -> &'static [x2api_kit::Dialect] {
        &[x2api_kit::Dialect::Chat]
    }

    async fn relay_raw(
        &self,
        _dialect: x2api_kit::Dialect,
        client_body: bytes::Bytes,
        _ctx: &x2api_kit::CallContext<'_>,
    ) -> Result<x2api_kit::RawReply, x2api_kit::ProviderError> {
        let resp = self
            .client
            .post(self.url.clone())
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(client_body)
            .send()
            .await
            .map_err(x2api_kit::ProviderError::from)?;
        let status = resp.status().as_u16();
        let content_type = resp.headers().get(http::header::CONTENT_TYPE).cloned();
        let wrapped = x2api_kit::UpstreamResponse(resp);
        let frames = if wrapped.is_sse() {
            x2api_kit::RawFrames::Sse(wrapped.into_chunks())
        } else {
            x2api_kit::RawFrames::Buffered(
                wrapped
                    .into_response()
                    .bytes()
                    .await
                    .map_err(x2api_kit::ProviderError::from)?,
            )
        };
        Ok(x2api_kit::RawReply {
            status,
            content_type,
            headers: Default::default(),
            frames,
        })
    }

    async fn complete(
        &self,
        req: &x2api_kit::ChatRequest,
        ctx: &x2api_kit::CallContext<'_>,
    ) -> Result<x2api_kit::Completion, x2api_kit::ProviderError> {
        self.inner.complete(req, ctx).await
    }

    async fn stream(
        &self,
        req: &x2api_kit::ChatRequest,
        ctx: &x2api_kit::CallContext<'_>,
    ) -> Result<x2api_kit::ChatStream, x2api_kit::ProviderError> {
        self.inner.stream(req, ctx).await
    }
}

// ── proxy under test, on its own runtime ─────────────────────────────────

/// The proxy gets a dedicated runtime: its threads are the ones the
/// allocation counter watches, and keeping the bench's own client off them
/// stops the instrument from competing with the system under test for cores.
/// How this proxy instance reaches its upstream.
#[derive(Clone, Copy)]
enum Egress {
    /// Straight there: what every row but the lane one measures, because a
    /// proxy lane would put someone else's network in the numbers.
    Direct,
    /// Through an egress lane pointed at the bench's own mock proxy.
    /// `prewarm` decides whether the pool spends the handshake at boot or
    /// leaves it for the first request.
    Lane {
        proxy: std::net::SocketAddr,
        prewarm: bool,
    },
}

fn proxy_on(
    mock: std::net::SocketAddr,
    threads: usize,
    native: bool,
    egress: Egress,
) -> anyhow::Result<(std::net::SocketAddr, tokio::runtime::Runtime)> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .on_thread_start(|| PROXY_THREAD.with(|f| f.set(true)))
        .thread_name("proxy")
        .build()?;

    // Bind synchronously: `block_on` here would be inside the bench's own
    // runtime, which tokio refuses. The socket crosses over as a std listener.
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    std_listener.set_nonblocking(true)?;
    let addr = std_listener.local_addr()?;
    rt.spawn(async move {
        let mut server = x2api_kit::ServerConfig {
            bind: "127.0.0.1".into(),
            port: 0,
            sse_keepalive_secs: 0,
            stream_deadline_secs: 600,
            max_inflight: 1024,
            request_timeout_secs: 60,
            upstream_read_timeout_secs: 60,
            ..Default::default()
        };
        // A bench must fail loud, not retry noise into the averages.
        server.retry.max_attempts = 1;
        let provider = service::ServiceConfig {
            name: "openai".into(),
            base_url: format!("http://{mock}"),
            api_key: Some("bench".into()),
            model_map: Default::default(),
            api_keys: Vec::new(),
            max_response_bytes: None,
        };
        let (transport, lane_probe) = match egress {
            Egress::Direct => (x2api_transport::Transport::direct(&server), None),
            Egress::Lane { proxy, prewarm } => {
                let cfg = x2api_transport::ProxyConfig {
                    vendors: vec![x2api_transport::ProxyVendorConfig {
                        name: "bench".into(),
                        // A session template, so the lane exercises the real
                        // minting path rather than a fixed URL.
                        url: format!(
                            "http://bench-session-{{session}}-ttl-{{ttl_seconds}}:pw@{proxy}"
                        ),
                        lanes: 1,
                        min_lanes: 1,
                        session_ttl_secs: 3_600,
                        ..Default::default()
                    }],
                    prewarm,
                    ..Default::default()
                };
                let t = x2api_transport::Transport::with_proxy(&server, &cfg)
                    .expect("bench lane config is valid");
                (t, Some(format!("http://{mock}/v1/models")))
            }
        };
        let native_client = native.then(|| transport.client().expect("native egress has a client"));
        let url = reqwest::Url::parse(&format!("http://{mock}/v1/chat/completions"))
            .expect("bench mock url parses");
        let transport_for_start = transport.clone();
        let inner =
            service::OpenAiProvider::new(transport, provider).expect("bench base_url parses");
        let provider: Arc<dyn x2api_kit::Provider> = if native {
            Arc::new(NativeChat {
                inner,
                client: native_client.expect("native client was obtained above"),
                url,
            })
        } else {
            Arc::new(inner)
        };
        if let Some(probe) = lane_probe {
            // Warm (or deliberately do not warm) before anything serves.
            transport_for_start.start(Some(probe)).await;
        }
        let pipeline = x2api_server::Pipeline::new(provider, Arc::new(server), None);
        let listener = TcpListener::from_std(std_listener).unwrap();
        // serve() watches this sender; never flip it — the bench exits with
        // the process, exactly like the bins it mirrors.
        let (tx, _rx) = watch::channel(false);
        let _ = x2api_server::serve(listener, x2api_server::build_router(pipeline), 1, tx).await;
    });
    // Let the listener accept before the first timed request.
    std::thread::sleep(Duration::from_millis(50));
    Ok((addr, rt))
}

// ── measurement ──────────────────────────────────────────────────────────

/// One timed pass over `responses` responses.
#[derive(Default)]
struct Pass {
    secs: f64,
    bytes: u64,
    chunks: u64,
    ttfb_ms: Vec<f64>,
    /// Gaps between consecutive body chunks. TTFB says when a stream STARTED;
    /// this says whether it kept flowing — a proxy that stalls mid-generation
    /// has a healthy ttfb and an ugly tail here.
    gap_ms: Vec<f64>,
    allocs: u64,
    alloc_bytes: u64,
}

struct Run {
    name: &'static str,
    sanity: &'static str,
    passes: Vec<Pass>,
    units: usize,
    responses: usize,
    concurrency: usize,
    /// First-byte latency of the pass's one unwarmed request.
    cold_ttfb_ms: f64,
}

impl Run {
    /// Per-repeat µs per unit, sorted — the primary figure and its spread.
    fn us_per_frame(&self) -> Vec<f64> {
        let mut v: Vec<f64> = self
            .passes
            .iter()
            .map(|p| p.secs * 1e6 / (self.units * self.responses) as f64)
            .collect();
        v.sort_by(f64::total_cmp);
        v
    }

    fn mib_s(&self) -> f64 {
        let mut v: Vec<f64> = self
            .passes
            .iter()
            .map(|p| p.bytes as f64 / (1024.0 * 1024.0) / p.secs)
            .collect();
        v.sort_by(f64::total_cmp);
        median(&v)
    }

    /// Inter-chunk gap at a quantile. Meaningless for a buffered run (one
    /// chunk, no gaps) and reported as such.
    fn gap(&self, q: f64) -> f64 {
        let mut all: Vec<f64> = self
            .passes
            .iter()
            .flat_map(|p| p.gap_ms.iter().copied())
            .collect();
        all.sort_by(f64::total_cmp);
        quantile(&all, q)
    }

    fn ttfb(&self, q: f64) -> f64 {
        let mut all: Vec<f64> = self
            .passes
            .iter()
            .flat_map(|p| p.ttfb_ms.iter().copied())
            .collect();
        all.sort_by(f64::total_cmp);
        quantile(&all, q)
    }

    fn chunks_per_response(&self) -> f64 {
        let chunks: u64 = self.passes.iter().map(|p| p.chunks).sum();
        chunks as f64 / (self.passes.len() * self.responses) as f64
    }

    /// Proxy-thread allocations attributable to one frame. Zero for the
    /// no-proxy baseline, which has no proxy threads by construction.
    fn allocs_per_frame(&self) -> f64 {
        let n: u64 = self.passes.iter().map(|p| p.allocs).sum();
        n as f64 / (self.passes.len() * self.units * self.responses) as f64
    }

    fn alloc_bytes_per_frame(&self) -> f64 {
        let n: u64 = self.passes.iter().map(|p| p.alloc_bytes).sum();
        n as f64 / (self.passes.len() * self.units * self.responses) as f64
    }
}

fn median(sorted: &[f64]) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let mid = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    }
}

/// Nearest-rank quantile: at these sample counts an interpolating estimator
/// would invent precision the bench does not have.
fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((q * sorted.len() as f64).ceil() as usize).saturating_sub(1);
    sorted[idx.min(sorted.len() - 1)]
}

/// Consume one streamed response fully, recording when its first byte landed
/// and how many body chunks it arrived in.
struct Consumed {
    ttfb_ms: f64,
    bytes: u64,
    chunks: u64,
    gaps_ms: Vec<f64>,
}

async fn consume(
    client: &reqwest::Client,
    url: &str,
    body: &serde_json::Value,
) -> anyhow::Result<Consumed> {
    let started = Instant::now();
    let mut resp = client.post(url).json(body).send().await?;
    anyhow::ensure!(resp.status().is_success(), "status {}", resp.status());
    let mut out = Consumed {
        ttfb_ms: f64::NAN,
        bytes: 0,
        chunks: 0,
        gaps_ms: Vec::new(),
    };
    let mut last = started;
    while let Some(chunk) = resp.chunk().await? {
        let now = Instant::now();
        if out.chunks == 0 {
            out.ttfb_ms = now.duration_since(started).as_secs_f64() * 1e3;
        } else {
            out.gaps_ms
                .push(now.duration_since(last).as_secs_f64() * 1e3);
        }
        last = now;
        out.chunks += 1;
        out.bytes += chunk.len() as u64;
    }
    Ok(out)
}

/// One sanity probe (its bytes checked, its time discarded), then `repeats`
/// timed passes of `responses` responses each, `concurrency` in flight.
async fn measure(
    name: &'static str,
    client: &reqwest::Client,
    url: String,
    body: serde_json::Value,
    args: &Args,
    concurrency: usize,
    check: impl Fn(&[u8], &[u8]) -> &'static str,
) -> anyhow::Result<Run> {
    // The probe is the ONLY cold request in a pass, so its first byte is
    // what a client meets before anything is warm. On the lane row with a
    // handshake cost configured, this is the column that prices `prewarm`.
    let probe_started = Instant::now();
    let mut probe = client.post(&url).json(&body).send().await?;
    anyhow::ensure!(
        probe.status().is_success(),
        "sanity probe status {}",
        probe.status()
    );
    let mut cold_ttfb_ms = f64::NAN;
    let mut head: Vec<u8> = Vec::new();
    let mut tail: Vec<u8> = Vec::new();
    while let Some(chunk) = probe
        .chunk()
        .await
        .map_err(|e| anyhow::anyhow!("sanity probe read failed: {e}"))?
    {
        if cold_ttfb_ms.is_nan() {
            cold_ttfb_ms = probe_started.elapsed().as_secs_f64() * 1e3;
        }
        if head.len() < 512 {
            head.extend_from_slice(&chunk);
        }
        tail.extend_from_slice(&chunk);
        if tail.len() > 1024 {
            tail.drain(..tail.len() - 1024);
        }
    }
    anyhow::ensure!(head.len() > 50, "sanity probe received no frames");
    let sanity = check(&head, &tail);

    let url = Arc::new(url);
    let body = Arc::new(body);
    let mut passes = Vec::with_capacity(args.repeats);
    for _ in 0..args.repeats {
        let (a0, b0) = alloc_snapshot();
        let started = Instant::now();
        let mut workers = Vec::with_capacity(concurrency);
        for w in 0..concurrency {
            // Responses split as evenly as they divide; every lane keeps at
            // least its share so none sits idle skewing the wall clock.
            let share =
                args.responses / concurrency + usize::from(w < args.responses % concurrency);
            let (client, url, body) = (client.clone(), url.clone(), body.clone());
            workers.push(tokio::spawn(async move {
                let mut ttfbs = Vec::with_capacity(share);
                let mut gaps = Vec::new();
                let (mut bytes, mut chunks) = (0u64, 0u64);
                for _ in 0..share {
                    let c = consume(&client, &url, &body).await?;
                    ttfbs.push(c.ttfb_ms);
                    gaps.extend(c.gaps_ms);
                    bytes += c.bytes;
                    chunks += c.chunks;
                }
                Ok::<_, anyhow::Error>((ttfbs, gaps, bytes, chunks))
            }));
        }
        let mut pass = Pass::default();
        for w in workers {
            let (ttfbs, gaps, bytes, chunks) = w.await??;
            pass.ttfb_ms.extend(ttfbs);
            pass.gap_ms.extend(gaps);
            pass.bytes += bytes;
            pass.chunks += chunks;
        }
        pass.secs = started.elapsed().as_secs_f64();
        let (a1, b1) = alloc_snapshot();
        pass.allocs = a1 - a0;
        pass.alloc_bytes = b1 - b0;
        passes.push(pass);
    }

    Ok(Run {
        name,
        sanity,
        passes,
        units: args.units_per_response(),
        responses: args.responses,
        concurrency,
        cold_ttfb_ms,
    })
}

fn contains(hay: &[u8], needle: &str) -> bool {
    memchr::memmem::find(hay, needle.as_bytes()).is_some()
}

// ── reporting ────────────────────────────────────────────────────────────

fn print_table(rows: &[Run], args: &Args, failed: bool) {
    println!(
        "x2api-bench — mode={} payload={} frames/resp={} responses={} repeats={} \
         concurrency={:?} mock={} proxy-threads={}  {}",
        if args.buffered {
            "buffered"
        } else {
            "streamed"
        },
        args.payload.label(),
        args.frames,
        args.responses,
        args.repeats,
        args.concurrency,
        args.mock_label(),
        args.proxy_threads,
        if failed {
            "SANITY FAILED — numbers not trustworthy"
        } else {
            "sanity: all pass"
        }
    );
    for level in &args.concurrency {
        let group: Vec<&Run> = rows.iter().filter(|r| r.concurrency == *level).collect();
        if group.is_empty() {
            continue;
        }
        if args.concurrency.len() > 1 {
            println!("\n-- concurrency {level} --");
        }
        print_group(&group, args);
    }
    println!("\nsanity");
    for r in rows.iter().filter(|r| r.concurrency == args.concurrency[0]) {
        println!("  {:<36}{}", r.name, r.sanity);
    }
    println!(
        "\nread: {} is the comparison (median of {} passes); MiB/s is\n\
         per-variant only — dialects differ in bytes emitted per unit.\n\
         ttfb/gap/chunks are the pace columns: a proxy that accumulates instead\n\
         of streaming keeps its throughput and loses these, and gap p99 is the\n\
         mid-stream stall ttfb cannot show. allocs counts only the proxy\n\
         runtime's threads. Comparative on this machine, not SLOs.",
        args.unit_label(),
        args.repeats
    );
}

fn print_group(rows: &[&Run], args: &Args) {
    println!(
        "{:<36}{:>10}{:>16}{:>10}{:>10}{:>11}{:>9}{:>9}{:>9}{:>11}{:>10}",
        "variant",
        args.unit_label(),
        "(min–max)",
        "ttfb p50",
        "ttfb p99",
        "cold ttfb",
        "gap p99",
        "chunks",
        "MiB/s",
        if args.buffered {
            "allocs/req"
        } else {
            "allocs/f"
        },
        "bytes/u"
    );
    for r in rows {
        let us = r.us_per_frame();
        let gap = r.gap(0.99);
        println!(
            "{:<36}{:>10.2}{:>16}{:>10.2}{:>10.2}{:>11.2}{:>9}{:>9.0}{:>9.1}{:>11.1}{:>10.0}",
            r.name,
            median(&us),
            format!("{:.2}–{:.2}", us[0], us[us.len() - 1]),
            r.ttfb(0.50),
            r.ttfb(0.99),
            r.cold_ttfb_ms,
            if gap.is_nan() {
                "—".to_string()
            } else {
                format!("{gap:.2}")
            },
            r.chunks_per_response(),
            r.mib_s(),
            r.allocs_per_frame(),
            r.alloc_bytes_per_frame(),
        );
    }
}

/// NaN is not JSON. A metric that has no meaning for this mode says `null`
/// rather than a number that would be silently compared later.
fn nullable(v: f64) -> serde_json::Value {
    if v.is_nan() {
        serde_json::Value::Null
    } else {
        serde_json::json!(v)
    }
}

fn print_json(rows: &[Run], args: &Args, failed: bool) {
    let variants: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            let us = r.us_per_frame();
            serde_json::json!({
                "variant": r.name,
                "concurrency": r.concurrency,
                "us_per_frame_median": median(&us),
                "us_per_frame_min": us[0],
                "us_per_frame_max": us[us.len() - 1],
                "us_per_frame_passes": us,
                "cold_ttfb_ms": nullable(r.cold_ttfb_ms),
                "ttfb_ms_p50": r.ttfb(0.50),
                "ttfb_ms_p99": r.ttfb(0.99),
                "gap_ms_p50": nullable(r.gap(0.50)),
                "gap_ms_p99": nullable(r.gap(0.99)),
                "chunks_per_response": r.chunks_per_response(),
                "mib_per_sec_median": r.mib_s(),
                "proxy_allocs_per_frame": r.allocs_per_frame(),
                "proxy_alloc_bytes_per_frame": r.alloc_bytes_per_frame(),
                "sanity": r.sanity,
            })
        })
        .collect();
    let doc = serde_json::json!({
        "config": {
            "mode": if args.buffered { "buffered" } else { "streamed" },
            "payload": args.payload.label(),
            "unit": args.unit_label(),
            "frames": args.frames,
            "responses": args.responses,
            "repeats": args.repeats,
            "concurrency": args.concurrency,
            "mock": args.mock_label(),
            "pace_us": args.pace_us,
            "proxy_threads": args.proxy_threads,
        },
        "sanity_failed": failed,
        "variants": variants,
    });
    println!("{}", serde_json::to_string_pretty(&doc).unwrap());
}

/// Compare this run against a previous `--json` file and report regressions.
///
/// A shape mismatch is an error by default: comparing unlike units would turn
/// an invalid gate into a successful no-op.
fn compare_baseline(rows: &[Run], args: &Args, path: &str) -> anyhow::Result<Vec<String>> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("reading baseline {path}: {e}"))?;
    let doc: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| anyhow::anyhow!("parsing baseline {path}: {e}"))?;

    let cfg = &doc["config"];
    let current_mode = if args.buffered {
        "buffered"
    } else {
        "streamed"
    };
    let expected = [
        (
            "mode",
            serde_json::json!(cfg["mode"]),
            serde_json::json!(current_mode),
        ),
        (
            "payload",
            serde_json::json!(cfg["payload"]),
            serde_json::json!(args.payload.label()),
        ),
        (
            "frames",
            serde_json::json!(cfg["frames"]),
            serde_json::json!(args.frames),
        ),
        (
            "responses",
            serde_json::json!(cfg["responses"]),
            serde_json::json!(args.responses),
        ),
        (
            "concurrency",
            serde_json::json!(cfg["concurrency"]),
            serde_json::json!(args.concurrency),
        ),
    ];
    let mismatches: Vec<String> = expected
        .iter()
        .filter(|(_, baseline, current)| baseline != current)
        .map(|(field, baseline, current)| {
            format!("{field}: baseline {baseline}, current {current}")
        })
        .collect();
    if !mismatches.is_empty() {
        if args.allow_shape_mismatch {
            println!(
                "\nbaseline {path}: SKIPPED — different shape ({})",
                mismatches.join("; ")
            );
            return Ok(Vec::new());
        }
        anyhow::bail!(
            "baseline {path}: shape mismatch:\n  {}",
            mismatches.join("\n  ")
        );
    }

    let baseline_variants = doc["variants"].as_array().cloned().unwrap_or_default();
    println!("\nbaseline {path}");
    println!("{:<36}{:>12}{:>12}{:>10}", "variant", "was", "now", "delta");
    let mut mismatches = Vec::new();
    for r in rows {
        let Some(previous) = baseline_variants
            .iter()
            .find(|v| {
                v["variant"] == serde_json::json!(r.name)
                    && v["concurrency"] == serde_json::json!(r.concurrency)
            })
            .and_then(|v| v["us_per_frame_median"].as_f64())
        else {
            mismatches.push(format!(
                "{} [c{}]: missing from baseline",
                r.name, r.concurrency
            ));
            continue;
        };
        let now = median(&r.us_per_frame());
        let delta = (now - previous) / previous * 100.0;
        println!(
            "{:<36}{:>12.2}{:>12.2}{:>9.1}%",
            r.name, previous, now, delta
        );
        if delta > args.max_regress_pct {
            mismatches.push(format!(
                "{} [c{}]: {previous:.2} -> {now:.2} µs ({delta:+.1}%)",
                r.name, r.concurrency
            ));
        }
    }
    for previous in &baseline_variants {
        let Some(name) = previous["variant"].as_str() else {
            continue;
        };
        let Some(concurrency) = previous["concurrency"].as_u64() else {
            continue;
        };
        if !rows
            .iter()
            .any(|r| r.name == name && r.concurrency as u64 == concurrency)
        {
            mismatches.push(format!("{name} [c{concurrency}]: missing from current run"));
        }
    }
    Ok(mismatches)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse()?;

    let frames = sse_frames(args.frames, args.payload);
    let whole = Arc::new(frames.iter().flatten().copied().collect::<Vec<u8>>());
    let buffered = buffered_body(args.frames, args.payload);
    let (frames_for_proxy, whole_for_proxy, buffered_for_proxy) =
        (frames.clone(), whole.clone(), buffered.clone());
    let mock = mock_upstream(frames, whole, buffered, args.pace, args.pace_us).await;
    let via = mock_proxy(
        frames_for_proxy,
        whole_for_proxy,
        buffered_for_proxy,
        args.proxy_connect_ms,
        args.pace,
    )
    .await;
    let (proxy, proxy_rt) = proxy_on(mock, args.proxy_threads, false, Egress::Direct)?;
    // A second proxy, identical but for one declaration: its provider calls
    // the chat dialect native, so the same request takes the fidelity lane.
    let (native, native_rt) = proxy_on(mock, args.proxy_threads, true, Egress::Direct)?;
    // A third instance, identical but for its egress: every upstream call
    // rides a sticky proxy session through the mock proxy above.
    let (lane, lane_rt) = proxy_on(
        mock,
        args.proxy_threads,
        false,
        Egress::Lane {
            proxy: via,
            prewarm: !args.lane_cold,
        },
    )?;
    let outcome = bench_all(&args, mock, proxy, native, lane).await;
    // Dropping a runtime from inside another one's async context panics, and
    // a bench that dies on the way out looks like a proxy that died.
    proxy_rt.shutdown_background();
    native_rt.shutdown_background();
    lane_rt.shutdown_background();
    outcome
}

async fn bench_all(
    args: &Args,
    mock: std::net::SocketAddr,
    proxy: std::net::SocketAddr,
    native: std::net::SocketAddr,
    lane: std::net::SocketAddr,
) -> anyhow::Result<()> {
    // The instrument's client is deliberately plain: identical across
    // variants, so the deltas are the system under test's, not the tool's.
    let client = reqwest::Client::new();
    let mut rows: Vec<Run> = Vec::new();
    for &concurrency in &args.concurrency {
        rows.extend(measure_all(args, concurrency, &client, mock, proxy, native, lane).await?);
    }

    let failed = rows.iter().any(|r| r.sanity.starts_with("FAIL"));
    if args.json {
        print_json(&rows, args, failed);
    } else {
        print_table(&rows, args, failed);
    }
    if failed {
        anyhow::bail!("sanity failures above");
    }
    if let Some(path) = &args.baseline {
        let mismatches = compare_baseline(&rows, args, path)?;
        if !mismatches.is_empty() {
            anyhow::bail!("baseline comparison failed:\n  {}", mismatches.join("\n  "));
        }
    }
    Ok(())
}

/// Every variant at ONE concurrency level. Called once per `-c` level so a
/// sweep compares like with like: same mock, same bodies, same client.
async fn measure_all(
    args: &Args,
    concurrency: usize,
    client: &reqwest::Client,
    mock: std::net::SocketAddr,
    proxy: std::net::SocketAddr,
    native: std::net::SocketAddr,
    lane: std::net::SocketAddr,
) -> anyhow::Result<Vec<Run>> {
    let mut rows: Vec<Run> = Vec::new();
    // `stream` is the client's flag and the mock reads it back off the body,
    // so one boolean drives both ends of the measurement.
    let stream = !args.buffered;

    // 1. baseline: the mock itself, no proxy in the way.
    rows.push(
        measure(
            "mock (no proxy)",
            client,
            format!("http://{mock}/v1/chat/completions"),
            serde_json::json!({"model": "bench", "input": "x", "stream": stream}),
            args,
            concurrency,
            move |head, _tail| {
                if !stream {
                    return if contains(head, "\"object\":\"chat.completion\"") {
                        "ok: buffered document"
                    } else {
                        "FAIL: not a completion"
                    };
                }
                if contains(head, "data: {") {
                    "ok"
                } else {
                    "FAIL: no frames"
                }
            },
        )
        .await?,
    );

    // 2. chat dialect: must take the verbatim relay.
    rows.push(
        measure(
            "chat  /v1/chat/completions (relay)",
            client,
            format!("http://{proxy}/v1/chat/completions"),
            serde_json::json!({"model": "bench", "messages": [{"role": "user", "content": "x"}], "stream": stream}),
            args,
            concurrency,
            move |head, tail| {
                if !stream {
                    // Buffered chat renders `raw` back through a
                    // `serde_json::Value`, whose map is ordered — so the
                    // vendor's insertion order does NOT survive, and the
                    // document comes back key-sorted. That is the documented
                    // rule ("never promise byte-fidelity on buffered"), and
                    // this is where it becomes visible instead of asserted.
                    let ok = (contains(head, "chat.completion") || contains(tail, "chat.completion"))
                        && (contains(head, "\"finish_reason\":\"stop\"")
                            || contains(tail, "\"finish_reason\":\"stop\""));
                    let reordered = !contains(head, "\"id\":\"bench\",\"model\"");
                    return match (ok, reordered) {
                        (true, true) => "ok: document re-rendered (keys sorted, as documented)",
                        (true, false) => "ok: document relayed",
                        _ => "FAIL: buffered shape",
                    };
                }
                if contains(tail, "[DONE]") && !contains(tail, "cost") {
                    "ok: sentinel kept, post-[DONE] frame swallowed"
                } else {
                    "FAIL: sentinel/cost"
                }
            },
        )
        .await?,
    );

    // 3. the fidelity lane: same dialect, same bytes, no fold at all.
    rows.push(
        measure(
            "chat  /v1/chat/completions (fidelity)",
            client,
            format!("http://{native}/v1/chat/completions"),
            serde_json::json!({"model": "bench", "messages": [{"role": "user", "content": "x"}], "stream": stream}),
            args,
            concurrency,
            move |head, tail| {
                if !stream {
                    // The lane never parsed it, so the vendor's own key order
                    // is still there — the exact thing the relay row above
                    // loses. Same request, same upstream, different promise.
                    return if contains(head, "\"id\":\"bench\",\"model\":\"bench\",\"object\"") {
                        "ok: vendor bytes, key order intact"
                    } else {
                        "FAIL: lane altered the document"
                    };
                }
                // Fidelity means the vendor's trailer survives — the relay
                // row's sanity check demands the opposite, which is exactly
                // the difference between the two lanes.
                if contains(tail, "[DONE]") && contains(tail, "cost") {
                    "ok: vendor bytes verbatim, trailer included"
                } else {
                    "FAIL: lane altered the stream"
                }
            },
        )
        .await?,
    );

    // 4. the same relay, but reached through a sticky proxy session.
    rows.push(
        measure(
            "chat  (relay via proxy lane)",
            client,
            format!("http://{lane}/v1/chat/completions"),
            serde_json::json!({"model": "bench", "messages": [{"role": "user", "content": "x"}], "stream": stream}),
            args,
            concurrency,
            move |_head, tail| {
                if !stream {
                    return "ok: buffered through the lane";
                }
                if contains(tail, "[DONE]") {
                    "ok: relayed through a minted session"
                } else {
                    "FAIL: lane did not deliver"
                }
            },
        )
        .await?,
    );

    // 5.–7. bridge dialects: transcode is the product — sanity proves the
    // fold happened, not that bytes merely moved.
    rows.push(
        measure(
            "msg   /v1/messages (transcode)",
            client,
            format!("http://{proxy}/v1/messages"),
            serde_json::json!({"model": "bench", "max_tokens": 64, "stream": stream, "messages": [{"role": "user", "content": "x"}]}),
            args,
            concurrency,
            move |head, tail| {
                if !stream {
                    // The Anthropic fold's buffered render: a message object
                    // with text content, never the OpenAI shape it came from.
                    let has = |n: &str| contains(head, n) || contains(tail, n);
                    return if has("\"type\":\"message\"")
                        && has("\"role\":\"assistant\"")
                        && !has("chat.completion")
                    {
                        "ok: anthropic message document"
                    } else {
                        "FAIL: buffered fold"
                    };
                }
                if contains(head, "event: message_start") && contains(tail, "message_stop") {
                    "ok: anthropic event grammar"
                } else {
                    "FAIL: grammar"
                }
            },
        )
        .await?,
    );
    rows.push(
        measure(
            "resp  /v1/responses (transcode)",
            client,
            format!("http://{proxy}/v1/responses"),
            serde_json::json!({"model": "bench", "stream": stream, "input": "x"}),
            args,
            concurrency,
            move |head, tail| {
                if !stream {
                    let has = |n: &str| contains(head, n) || contains(tail, n);
                    return if has("\"object\":\"response\"") && has("\"output\"") {
                        "ok: responses document"
                    } else {
                        "FAIL: buffered fold"
                    };
                }
                if contains(head, "event: response.created") && contains(tail, "response.completed")
                {
                    "ok: responses lifecycle closed"
                } else {
                    "FAIL: lifecycle"
                }
            },
        )
        .await?,
    );

    // 7. the fourth dialect. Its model AND its stream flag ride the PATH verb
    // instead of the body, so this is the one row whose URL depends on mode.
    rows.push(
        measure(
            if stream {
                "gem   /v1beta/models/bench:streamGenerateContent (transcode)"
            } else {
                "gem   /v1beta/models/bench:generateContent (transcode)"
            },
            client,
            format!(
                "http://{proxy}/v1beta/models/bench:{}",
                if stream {
                    "streamGenerateContent?alt=sse"
                } else {
                    "generateContent"
                }
            ),
            serde_json::json!({"contents": [{"role": "user", "parts": [{"text": "x"}]}]}),
            args,
            concurrency,
            move |head, tail| {
                let has = |n: &str| contains(head, n) || contains(tail, n);
                if !stream {
                    return if has("\"candidates\"") && !has("chat.completion") {
                        "ok: gemini document"
                    } else {
                        "FAIL: buffered fold"
                    };
                }
                // And the one thing this dialect must NOT carry: Gemini has no
                // `[DONE]` sentinel, so the terminal `finishReason` frame is
                // its only terminator. A chat sentinel in the tail means bytes
                // moved without folding.
                if has("\"candidates\"")
                    && contains(tail, "\"finishReason\"")
                    && !contains(tail, "[DONE]")
                {
                    "ok: candidates + finishReason, no chat sentinel"
                } else {
                    "FAIL: gemini stream"
                }
            },
        )
        .await?,
    );

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    struct BaselineFile(std::path::PathBuf);

    impl BaselineFile {
        fn new(doc: serde_json::Value) -> Self {
            let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "x2api-bench-baseline-{}-{}.json",
                std::process::id(),
                id
            ));
            std::fs::write(&path, serde_json::to_vec(&doc).unwrap()).unwrap();
            Self(path)
        }

        fn path(&self) -> &str {
            self.0.to_str().unwrap()
        }
    }

    impl Drop for BaselineFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn args() -> Args {
        Args {
            frames: 10,
            responses: 1,
            repeats: 1,
            concurrency: vec![1],
            pace: false,
            pace_us: 0,
            proxy_threads: 1,
            json: true,
            buffered: false,
            payload: Payload::Ascii,
            baseline: None,
            allow_shape_mismatch: false,
            max_regress_pct: 10.0,
            proxy_connect_ms: 0,
            lane_cold: false,
        }
    }

    fn baseline_doc(args: &Args, variants: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "config": {
                "mode": if args.buffered { "buffered" } else { "streamed" },
                "payload": args.payload.label(),
                "frames": args.frames,
                "responses": args.responses
                ,"concurrency": args.concurrency
            },
            "variants": variants
        })
    }

    fn run(name: &'static str, us_per_frame: f64, allocs: u64) -> Run {
        Run {
            name,
            sanity: "ok",
            passes: vec![Pass {
                secs: us_per_frame * 10.0 / 1e6,
                bytes: 0,
                chunks: 0,
                ttfb_ms: vec![],
                gap_ms: vec![],
                allocs,
                alloc_bytes: 0,
            }],
            units: 10,
            responses: 1,
            concurrency: 1,
            cold_ttfb_ms: 0.0,
        }
    }

    #[test]
    fn same_shape_regression_names_offending_row() {
        let args = args();
        let doc = baseline_doc(
            &args,
            serde_json::json!([
                {"variant": "chat", "concurrency": 1, "us_per_frame_median": 100.0},
                {"variant": "messages", "concurrency": 1, "us_per_frame_median": 200.0}
            ]),
        );
        let baseline = BaselineFile::new(doc);
        let rows = [run("chat", 105.0, 0), run("messages", 250.0, 0)];

        let mismatches = compare_baseline(&rows, &args, baseline.path()).unwrap();

        assert_eq!(mismatches.len(), 1);
        assert!(mismatches[0].starts_with("messages [c1]:"));
    }

    #[test]
    fn shape_mismatches_fail_by_default_and_name_field_and_values() {
        let cases = [
            (
                "frames",
                serde_json::json!(11),
                "frames: baseline 11, current 10",
            ),
            (
                "responses",
                serde_json::json!(2),
                "responses: baseline 2, current 1",
            ),
            (
                "mode",
                serde_json::json!("buffered"),
                "mode: baseline \"buffered\", current \"streamed\"",
            ),
            (
                "payload",
                serde_json::json!("unicode"),
                "payload: baseline \"unicode\", current \"ascii\"",
            ),
            (
                "concurrency",
                serde_json::json!([8]),
                "concurrency: baseline [8], current [1]",
            ),
        ];
        for (field, baseline_value, expected) in cases {
            let args = args();
            let mut config = baseline_doc(&args, serde_json::json!([]));
            config["config"][field] = baseline_value;
            let baseline = BaselineFile::new(config);
            let error = compare_baseline(&[run("chat", 100.0, 0)], &args, baseline.path())
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "unexpected error: {error}");
        }
    }

    #[test]
    fn allowing_shape_mismatch_returns_empty_without_gate_results() {
        let mut args = args();
        args.allow_shape_mismatch = true;
        let mut config = baseline_doc(&args, serde_json::json!([]));
        config["config"]["frames"] = serde_json::json!(11);
        let baseline = BaselineFile::new(config);

        let mismatches = compare_baseline(&[run("chat", 100.0, 0)], &args, baseline.path())
            .expect("shape mismatch should be explicitly allowed");

        assert!(mismatches.is_empty());
    }

    #[test]
    fn missing_variants_are_matched_by_name_and_concurrency_in_both_directions() {
        let args = args();
        let baseline = BaselineFile::new(baseline_doc(
            &args,
            serde_json::json!([
                {"variant": "chat", "concurrency": 1, "us_per_frame_median": 100.0},
                {"variant": "chat", "concurrency": 8, "us_per_frame_median": 200.0}
            ]),
        ));
        let mut current = run("chat", 100.0, 0);
        current.concurrency = 4;

        let mismatches = compare_baseline(&[current], &args, baseline.path()).unwrap();

        assert!(
            mismatches
                .iter()
                .any(|m| m == "chat [c4]: missing from baseline")
        );
        assert!(
            mismatches
                .iter()
                .any(|m| m == "chat [c1]: missing from current run")
        );
        assert!(
            mismatches
                .iter()
                .any(|m| m == "chat [c8]: missing from current run")
        );
    }

    #[test]
    fn current_variant_missing_at_baseline_concurrency_is_reported() {
        let args = args();
        let baseline = BaselineFile::new(baseline_doc(
            &args,
            serde_json::json!([{"variant": "chat", "concurrency": 1, "us_per_frame_median": 100.0}]),
        ));
        let mut current = run("chat", 100.0, 0);
        current.concurrency = 8;

        let mismatches = compare_baseline(&[current], &args, baseline.path()).unwrap();

        assert!(mismatches.contains(&"chat [c8]: missing from baseline".to_string()));
        assert!(mismatches.contains(&"chat [c1]: missing from current run".to_string()));
    }

    #[test]
    fn median_odd_even_and_empty_samples_follow_contract() {
        assert!(median(&[]).is_nan());
        assert_eq!(median(&[1.0, 2.0, 3.0]), 2.0);
        assert_eq!(median(&[1.0, 2.0, 3.0, 5.0]), 2.5);
    }

    #[test]
    fn allocs_per_frame_uses_total_units_across_passes() {
        let mut row = run("chat", 100.0, 7);
        row.passes.push(Pass {
            secs: 100.0 * 10.0 / 1e6,
            bytes: 0,
            chunks: 0,
            ttfb_ms: vec![],
            gap_ms: vec![],
            allocs: 3,
            alloc_bytes: 0,
        });

        assert_eq!(row.allocs_per_frame(), 0.5);
    }
}
