//! Socket-level service test: the real `service` provider driven over a
//! production `axum::serve` router against a hand-rolled upstream mock
//! (raw `tokio::net::TcpListener` speaking HTTP/1.1). This is the layer the
//! scripted `oneshot` tests cannot reach: reqwest send, status-gate, the SSE
//! decoder, and `[DONE]` relay — end to end over real TCP.

use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use opencode2api_kit::ServerConfig;
use opencode2api_server::Pipeline;
use serde_json::{Value, json};
use service::{OpenAiProvider, ServiceConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

static PROXY_ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

const PROXY_ENV: [&str; 3] = [
    opencode2api_kit::config::ENV_PROXY_URL,
    opencode2api_kit::config::ENV_PROXY_LANES,
    opencode2api_kit::config::ENV_PROXY_SESSION_TTL_SECS,
];

fn proxy_env_lock() -> MutexGuard<'static, ()> {
    PROXY_ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct ProxyEnvGuard(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl ProxyEnvGuard {
    fn clear() -> Self {
        let prior = PROXY_ENV
            .iter()
            .map(|key| (*key, std::env::var_os(key)))
            .collect::<Vec<_>>();
        for key in PROXY_ENV {
            unsafe { std::env::remove_var(key) };
        }
        Self(prior)
    }
}

impl Drop for ProxyEnvGuard {
    fn drop(&mut self) {
        for (key, prior) in self.0.drain(..) {
            unsafe {
                if let Some(prior) = prior {
                    std::env::set_var(key, prior);
                } else {
                    std::env::remove_var(key);
                }
            }
        }
    }
}

/// One canned upstream reply.
#[derive(Clone)]
enum Reply {
    Buffered(&'static str),
    Sse(&'static str),
    /// One contiguous socket write, for tests that depend on record ordering
    /// and partial-record failure within a single upstream read.
    SseContiguous(String),
    Status(u16, &'static str),
    /// A status plus ONE extra header — enough to carry `retry-after` in any of
    /// the three spellings it arrives in, which is the whole point of the test
    /// that uses it.
    StatusWith(u16, &'static str, &'static str, &'static str),
    /// Headers and one data frame are written before `frame_written` opens the
    /// gate. The final `[DONE]` record waits until the test releases the body.
    GatedSse {
        frame: &'static str,
        frame_written: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    },
    TruncatedChunkedSse {
        frame: &'static str,
        frame_written: Arc<tokio::sync::Notify>,
    },
    /// A correct `content-length`, no `connection: close`, and the socket
    /// HELD open for the next request. Every connection-reuse proof needs a
    /// peer willing to reuse; a mock that closes after one reply cannot tell a
    /// probe that left its connection usable from one that did not.
    KeepAlive(&'static str),
    /// A `KeepAlive` that answers the prewarm `HEAD` as a HEAD — headers and
    /// `content-length: 0`, no body, which is what a `HEAD` means and all
    /// hyper will ever expose for one — and every other request as `request`.
    /// Split out because the prewarm proof needs to see BOTH on ONE
    /// connection: the socket the probe opened must be the socket the first
    /// real request rides. A peer that answers a HEAD with a body gets its
    /// connection discarded by hyper, and the test would then be measuring
    /// the mock's misbehaviour rather than connection reuse.
    KeepAliveProbe {
        request: &'static str,
    },
    /// Per-REQUEST reply script over a reusable connection. Connection-indexed
    /// replies are the wrong granularity the moment a socket is reused: the
    /// second request on a pooled connection would be handed the first
    /// request's reply. This one walks the script by the mock's global request
    /// count, so a scripted sequence survives pooling.
    KeepAliveScript(&'static [(u16, &'static str)]),
}

/// Mock upstream: reads each full request (headers + content-length body)
/// before replying (answering early aborts the client upload), captures
/// bodies, and serves `replies` in order, repeating the last forever.
async fn mock_upstream(replies: Vec<Reply>) -> (Socket, Arc<MockState>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(MockState {
        requests: std::sync::atomic::AtomicUsize::new(0),
        accepts: std::sync::atomic::AtomicUsize::new(0),
        bodies: parking_lot::Mutex::new(Vec::new()),
        conns: parking_lot::Mutex::new(Vec::new()),
        heads: parking_lot::Mutex::new(Vec::new()),
    });
    let st = state.clone();
    tokio::spawn(async move {
        let mut idx = 0usize;
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            // Which accepted socket each request rode: "shards multiply
            // connections, not sessions" and "the probe's connection was
            // reused" are both only observable if the mock can say which
            // requests shared one.
            let conn_id = st.accepts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let reply = replies[idx.min(replies.len() - 1)].clone();
            idx += 1;
            let st = st.clone();
            tokio::spawn(async move {
                // The pre-read stays where it always was: the one-shot arms
                // below (Buffered, Sse, GatedSse, Status, ...) have been
                // feeding `state.requests` / `state.heads` to the whole suite
                // this way since the harness was written.
                let (head, body) = read_request(&mut sock).await;
                let first_method = request_method(&head).to_owned();
                record(&st, conn_id, head, body);
                match reply {
                    // These three hold the socket, so the request that follows
                    // rides it with no second accept — which is what makes
                    // reuse observable at all.
                    Reply::KeepAlive(json) => loop {
                        let _ = sock.write_all(keep_alive_reply(json).as_bytes()).await;
                        let (head, body) = read_request(&mut sock).await;
                        if !record(&st, conn_id, head, body) {
                            break;
                        }
                    },
                    Reply::KeepAliveProbe { request } => {
                        // Answers the prewarm HEAD differently from a real
                        // request while HOLDING the socket, so the proof can
                        // see that the connection the probe opened is the one
                        // the first real request rides. A peer willing to
                        // reuse is what makes that visible at all.
                        //
                        // The probe's reply carries NO body, which is what a
                        // `HEAD` means: a peer that sends one anyway makes
                        // hyper discard the connection, and the test would
                        // then be measuring the mock's misbehaviour rather
                        // than connection reuse.
                        let mut probe_first = first_method == "HEAD";
                        loop {
                            let head = if probe_first {
                                probe_head_reply()
                            } else {
                                keep_alive_reply(request)
                            };
                            let _ = sock.write_all(head.as_bytes()).await;
                            let (head, body) = read_request(&mut sock).await;
                            probe_first = request_method(&head) == "HEAD";
                            if !record(&st, conn_id, head, body) {
                                break;
                            }
                        }
                    }
                    // Scripted by REQUEST, not by connection: once the client
                    // pools, request N rides whichever socket it likes, so a
                    // per-connection index would hand it the wrong reply the
                    // moment reuse began. Read, then answer, then repeat — the
                    // request just recorded IS the one being answered, so the
                    // `- 1` on the counter undoes the pre-read's own bump.
                    //
                    // `saturating_sub`, not `- 1`: a peer that connects and
                    // hangs up before the pre-read records anything leaves the
                    // counter at zero, and an underflow panic there would
                    // surface as a flake in an unrelated test.
                    Reply::KeepAliveScript(script) => {
                        let mut index = st
                            .requests
                            .load(std::sync::atomic::Ordering::SeqCst)
                            .saturating_sub(1);
                        loop {
                            let (code, body) = script[index.min(script.len() - 1)];
                            let _ = sock
                                .write_all(keep_alive_status(code, body).as_bytes())
                                .await;
                            let (head, body) = read_request(&mut sock).await;
                            if !record(&st, conn_id, head, body) {
                                break;
                            }
                            index += 1;
                        }
                    }
                    // The one-shot arms: one reply, then the socket closes.
                    Reply::Buffered(json) => {
                        let head = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                            json.len()
                        );
                        let _ = sock.write_all(head.as_bytes()).await;
                        let _ = sock.write_all(json.as_bytes()).await;
                    }
                    Reply::Sse(frames) => {
                        let _ = sock
                            .write_all(
                                b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n",
                            )
                            .await;
                        // split into the two data frames the fixtures carry
                        for line in frames.lines().filter(|l| !l.is_empty()) {
                            let _ = sock.write_all(line.as_bytes()).await;
                            let _ = sock.write_all(b"\n\n").await;
                            let _ = sock.flush().await;
                        }
                    }
                    Reply::SseContiguous(body) => {
                        let reply = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n{body}"
                        );
                        let _ = sock.write_all(reply.as_bytes()).await;
                    }
                    Reply::GatedSse {
                        frame,
                        frame_written,
                        release,
                    } => {
                        let _ = sock
                            .write_all(
                                b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n",
                            )
                            .await;
                        let _ = sock.write_all(frame.as_bytes()).await;
                        let _ = sock.write_all(b"\n\n").await;
                        let _ = sock.flush().await;
                        frame_written.notify_one();
                        release.notified().await;
                        let _ = sock.write_all(b"data: [DONE]\n\n").await;
                    }
                    Reply::TruncatedChunkedSse {
                        frame,
                        frame_written,
                    } => {
                        let _ = sock
                            .write_all(
                                b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
                            )
                            .await;
                        let payload = format!("{frame}\n\n");
                        let _ = sock
                            .write_all(format!("{:x}\r\n{}\r\n", payload.len(), payload).as_bytes())
                            .await;
                        let _ = sock.flush().await;
                        frame_written.notify_one();
                        // No terminating zero chunk: hyper reports a body error.
                    }
                    Reply::StatusWith(code, json, name, value) => {
                        let head = format!(
                            "HTTP/1.1 {code} ERR\r\ncontent-type: application/json\r\n{name}: {value}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                            json.len()
                        );
                        let _ = sock.write_all(head.as_bytes()).await;
                        let _ = sock.write_all(json.as_bytes()).await;
                    }
                    Reply::Status(code, json) => {
                        let head = format!(
                            "HTTP/1.1 {code} ERR\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                            json.len()
                        );
                        let _ = sock.write_all(head.as_bytes()).await;
                        let _ = sock.write_all(json.as_bytes()).await;
                    }
                }
            });
        }
    });
    (Socket(addr), state)
}

/// File one request against the mock's tallies, keyed by the connection that
/// carried it. `false` means the peer hung up: the request that would follow
/// is a new connection's business, not this one's.
fn record(st: &MockState, conn_id: usize, head: String, body: Vec<u8>) -> bool {
    if head.is_empty() {
        return false;
    }
    st.requests
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    st.bodies.lock().push(body);
    st.conns.lock().push(conn_id);
    st.heads.lock().push(head);
    true
}

/// A reply that leaves the connection reusable: an accurate `content-length`
/// and no `connection: close`, so a client that finished the body must put
/// the socket back in its pool.
fn keep_alive_reply(body: &'static str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    )
}

/// As `keep_alive_reply`, with the status a script asked for. Separate so the
/// 200 path stays the one-liner most tests read.
fn keep_alive_status(code: u16, body: &'static str) -> String {
    format!(
        "HTTP/1.1 {code} ERR\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    )
}

/// The prewarm probe's answer: headers, and no body — a `HEAD` that carries
/// one is a protocol violation, and hyper discards the socket rather than
/// reuse it. Sending a well-behaved HEAD is what lets the test observe the
/// probe's CONNECTION being reused by the next request, rather than measuring
/// a mock artefact.
fn probe_head_reply() -> String {
    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 0\r\n\r\n".to_string()
}

fn request_method(head: &str) -> &str {
    head.split_whitespace().next().unwrap_or_default()
}

struct Socket(std::net::SocketAddr);
struct MockState {
    requests: std::sync::atomic::AtomicUsize,
    /// Sockets accepted. Distinct from `requests`: one connection may carry
    /// several.
    accepts: std::sync::atomic::AtomicUsize,
    bodies: parking_lot::Mutex<Vec<Vec<u8>>>,
    /// The connection each request rode, in the same order as `heads`.
    conns: parking_lot::Mutex<Vec<usize>>,
    /// Raw request heads, so a test can assert what went ON THE WIRE — the
    /// proxy lane's credential lives here and nowhere else.
    heads: parking_lot::Mutex<Vec<String>>,
}

fn request_target(head: &str) -> &str {
    head.split_whitespace().nth(1).unwrap()
}

fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        (key.eq_ignore_ascii_case(name)).then(|| value.trim())
    })
}

fn header_value_count(head: &str, name: &str) -> usize {
    head.lines()
        .filter_map(|line| line.split_once(':'))
        .filter(|(key, _)| key.eq_ignore_ascii_case(name))
        .count()
}

async fn read_request(sock: &mut tokio::net::TcpStream) -> (String, Vec<u8>) {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    // headers
    let header_end;
    loop {
        let n = sock.read(&mut tmp).await.unwrap();
        if n == 0 {
            return (String::new(), Vec::new());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_double_crlf(&buf) {
            header_end = pos;
            break;
        }
    }
    let headers = String::from_utf8_lossy(&buf[..header_end]).to_lowercase();
    let len: usize = headers
        .lines()
        .find_map(|l| l.strip_prefix("content-length:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    while buf.len() - header_end - 4 < len {
        let n = sock.read(&mut tmp).await.unwrap();
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    (
        String::from_utf8_lossy(&buf[..header_end]).into_owned(),
        buf[header_end + 4..].to_vec(),
    )
}

/// Standard-alphabet base64 decode, for reading back a `Proxy-Authorization`
/// header. Hand-rolled like every other mock here — a test dependency for
/// twelve lines of table lookup would be the wrong trade.
fn base64_decode(input: &str) -> Vec<u8> {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in input.bytes().filter(|c| *c != b'=') {
        let Some(v) = TABLE.iter().position(|t| *t == c) else {
            continue;
        };
        acc = (acc << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    out
}

fn find_double_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

async fn serve(pipeline: Pipeline) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, opencode2api_server::build_router(pipeline))
            .await
            .unwrap();
    });
    addr
}

fn cfg_for(base: &str) -> (ServerConfig, ServiceConfig) {
    let mut server = ServerConfig {
        max_inflight: 4,
        ..Default::default()
    };
    server.retry.max_attempts = 3;
    server.retry.base_ms = 1;
    server.retry.cap_ms = 2;
    let provider = ServiceConfig {
        name: "openai".into(),
        base_url: base.to_string(),
        api_key: Some("test-key".into()),
        model_map: Default::default(),
        api_keys: Vec::new(),
        max_response_bytes: None,
    };
    (server, provider)
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

/// The provider under test must use the SAME tuned client production does
/// (pool rules, nodelay, timeouts documented in core::http) — otherwise the
/// socket suite exercises an untuned path the shipped binary never runs.
/// Direct egress: these tests measure the provider against a real socket, so
/// the transport is the no-proxy one (the pool's own behavior is covered by
/// opencode2api-kit's transport tests and the proxied test below).
fn client_for(cfg: &ServerConfig) -> std::sync::Arc<opencode2api_transport::Transport> {
    opencode2api_transport::Transport::direct(cfg)
}

/// Frames carry deliberate insertion order (`id,model,object` — not the
/// alphabetical `choices,id,model,object` a `serde_json::Value` round-trip
/// must swallow (a trailing `cost`-style vendor record).
const SSE_STREAM: &str = "data: {\"id\":\"c1\",\"model\":\"m\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\ndata: [DONE]\ndata: {\"cost\":0.9}";

#[tokio::test]
async fn chat_mode_and_request_id_reach_every_upstream_surface() {
    // The catalogue filter only lets `-free` ids through; chat requests for
    // other ids still forward (upstream's answer stays honest).
    const MODELS: &str = r#"{"object":"list","data":[{"id":"m-free","object":"model"}]}"#;
    let (mock, state) = mock_upstream(vec![
        Reply::Buffered(BUFFERED),
        Reply::Sse(SSE_STREAM),
        Reply::Buffered(MODELS),
    ])
    .await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let buffered = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .header("x-request-id", "7001")
        .json(&json!({"model":"m","messages":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(buffered.status(), 200);
    assert!(buffered.bytes().await.unwrap().starts_with(b"{"));

    let stream = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .header("x-request-id", "7002")
        .json(&json!({"model":"m","messages":[],"stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(stream.status(), 200);
    assert!(stream.text().await.unwrap().ends_with("data: [DONE]\n\n"));

    let models = client()
        .get(format!("http://{proxy}/v1/models"))
        .header("x-request-id", "7003")
        .send()
        .await
        .unwrap();
    assert_eq!(models.status(), 200);
    assert_eq!(
        models.json::<Value>().await.unwrap()["data"][0]["id"],
        "m-free"
    );

    let heads = state.heads.lock();
    let bodies = state.bodies.lock();
    assert_eq!(heads.len(), 3);
    assert_eq!(request_target(&heads[0]), "/v1/chat/completions");
    assert_eq!(request_target(&heads[1]), "/v1/chat/completions");
    assert_eq!(request_target(&heads[2]), "/v1/models");
    // The opencode profile answers every request with `*/*` — the gate does
    // not read Accept, so there is no per-mode choice to make.
    assert_eq!(header_value(&heads[0], "accept"), Some("*/*"));
    assert_eq!(header_value(&heads[1], "accept"), Some("*/*"));
    assert_eq!(header_value(&heads[0], "x-request-id"), Some("7001"));
    assert_eq!(header_value(&heads[1], "x-request-id"), Some("7002"));
    assert_eq!(header_value(&heads[2], "x-request-id"), Some("7003"));
    for (head, request_id) in [
        (&heads[0], "7001"),
        (&heads[1], "7002"),
        (&heads[2], "7003"),
    ] {
        assert_eq!(header_value_count(head, "x-request-id"), 1, "{request_id}");
    }
    // The vendor gate 403s a buffered upstream request, so even the buffered
    // client's turn is forwarded with stream forced on.
    assert_eq!(
        serde_json::from_slice::<Value>(&bodies[0]).unwrap()["stream"],
        true
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&bodies[1]).unwrap()["stream"],
        true
    );
}

#[tokio::test]
async fn models_without_configured_credentials_preserve_inbound_request_id() {
    // A models() listing is filtered to `-free` ids, so the fixture uses one.
    const MODELS: &str = r#"{"object":"list","data":[{"id":"m-free","object":"model"}]}"#;
    let (mock, state) = mock_upstream(vec![Reply::Buffered(MODELS)]).await;
    let (server, mut provider) = cfg_for(&format!("http://{}", mock.0));
    provider.api_key = None;
    provider.api_keys.clear();
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let models = client()
        .get(format!("http://{proxy}/v1/models"))
        .header("x-request-id", "7004")
        .send()
        .await
        .unwrap();
    assert_eq!(models.status(), 200);
    assert_eq!(
        models.json::<Value>().await.unwrap()["data"][0]["id"],
        "m-free"
    );

    let heads = state.heads.lock();
    assert_eq!(heads.len(), 1);
    assert_eq!(request_target(&heads[0]), "/v1/models");
    assert_eq!(header_value_count(&heads[0], "x-request-id"), 1);
    assert_eq!(header_value(&heads[0], "x-request-id"), Some("7004"));
}

#[tokio::test]
async fn custom_provider_name_prefixes_a_real_upstream_400() {
    let (mock, _state) = mock_upstream(vec![Reply::Status(400, "invalid model identifier")]).await;
    let (server, _) = cfg_for(&format!("http://{}", mock.0));
    let provider: ServiceConfig = serde_json::from_value(json!({
        "name": "compatible-vendor",
        "base_url": format!("http://{}", mock.0)
        , "api_key": "test-key"
    }))
    .unwrap();
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let response = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({"model":"m","messages":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let body: Value = response.json().await.unwrap();
    assert_eq!(
        body["error"]["message"],
        "compatible-vendor: 400: invalid model identifier"
    );
}

#[tokio::test]
async fn versioned_provider_bases_resolve_exact_chat_paths() {
    for (suffix, expected) in [
        ("/v1beta", "/v1beta/v1/chat/completions"),
        ("/api/v2", "/api/v2/v1/chat/completions"),
    ] {
        let (mock, state) = mock_upstream(vec![Reply::Buffered(BUFFERED)]).await;
        let (server, mut provider) = cfg_for(&format!("http://{}", mock.0));
        provider.base_url = format!("http://{}{suffix}", mock.0);
        let proxy = serve(Pipeline::new(
            Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
            Arc::new(server),
            None,
        ))
        .await;

        let response = client()
            .post(format!("http://{proxy}/v1/chat/completions"))
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200, "base {suffix}");
        assert_eq!(
            request_target(&state.heads.lock()[0]),
            expected,
            "base {suffix}"
        );
    }
}

#[test]
fn provider_name_defaults_to_openai_and_accepts_an_override() {
    let default: ServiceConfig = serde_json::from_value(json!({
        "base_url": "https://api.openai.com/v1"
    }))
    .unwrap();
    assert_eq!(default.name, "openai");

    let overridden: ServiceConfig = serde_json::from_value(json!({
        "name": "compatible-vendor",
        "base_url": "https://api.openai.com/v1"
    }))
    .unwrap();
    assert_eq!(overridden.name, "compatible-vendor");
}
const BUFFERED: &str = "{\"id\":\"c1\",\"model\":\"m\",\"object\":\"chat.completion\",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"hello\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"vendor_only\":42}}";
#[tokio::test]
async fn folded_response_reverses_vendor_model_to_client_alias() {
    let (mock, state) = mock_upstream(vec![Reply::Buffered(BUFFERED)]).await;
    let (server, mut provider) = cfg_for(&format!("http://{}", mock.0));
    provider
        .model_map
        .insert("friendly-model".into(), "m".into());
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let response = client()
        .post(format!("http://{proxy}/v1/messages"))
        .json(&json!({"model":"friendly-model","max_tokens":8,"messages":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["model"], "friendly-model");
    assert_eq!(body["content"][0]["text"], "hello");

    let bodies = state.bodies.lock();
    assert_eq!(
        serde_json::from_slice::<Value>(&bodies[0]).unwrap()["model"],
        "m",
        "the request must reach the vendor id"
    );
}

#[tokio::test]
async fn direct_vendor_model_is_not_rewritten_to_an_existing_alias() {
    let (mock, _state) = mock_upstream(vec![Reply::Buffered(BUFFERED)]).await;
    let (server, mut provider) = cfg_for(&format!("http://{}", mock.0));
    provider
        .model_map
        .insert("friendly-model".into(), "m".into());
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let response = client()
        .post(format!("http://{proxy}/v1/messages"))
        .json(&json!({"model":"m","max_tokens":8,"messages":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["model"], "m");
}

#[tokio::test]
async fn buffered_raw_chat_reply_keeps_vendor_model_bytes() {
    let (mock, _state) = mock_upstream(vec![Reply::Buffered(BUFFERED)]).await;
    let (server, mut provider) = cfg_for(&format!("http://{}", mock.0));
    provider
        .model_map
        .insert("friendly-model".into(), "m".into());
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let response = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({"model":"friendly-model","messages":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["model"], "m");
    assert_eq!(body["usage"]["vendor_only"], 42);
}

#[tokio::test]
async fn folded_stream_reverses_vendor_model_to_client_alias() {
    let (mock, _state) = mock_upstream(vec![Reply::Sse(SSE_STREAM)]).await;
    let (server, mut provider) = cfg_for(&format!("http://{}", mock.0));
    provider
        .model_map
        .insert("friendly-model".into(), "m".into());
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let response = client()
        .post(format!("http://{proxy}/v1/messages"))
        .json(&json!({
            "model":"friendly-model",
            "max_tokens":32,
            "messages":[],
            "stream":true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    let records: Vec<Value> = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|payload| serde_json::from_str(payload).ok())
        .collect();
    let start = records
        .iter()
        .find(|record| record["type"] == "message_start")
        .expect("folded stream start record");
    assert_eq!(start["message"]["model"], "friendly-model");
}

#[tokio::test]
async fn model_catalogue_lists_vendor_entry_and_alias_row() {
    // Both ids end in `-free`: `free_only` filters the listing, so a
    // non-free id would vanish before the alias-row behaviour under test
    // ever became observable.
    const MODELS: &str = r#"{"object":"list","data":[{"id":"m-free","object":"model"}]}"#;
    let (mock, _state) = mock_upstream(vec![Reply::Buffered(MODELS)]).await;
    let (server, mut provider) = cfg_for(&format!("http://{}", mock.0));
    provider
        .model_map
        .insert("friendly-model-free".into(), "m-free".into());
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let response = client()
        .get(format!("http://{proxy}/v1/models"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let catalogue: Value = response.json().await.unwrap();
    let ids: Vec<&str> = catalogue["data"]
        .as_array()
        .expect("model catalogue array")
        .iter()
        .filter_map(|model| model["id"].as_str())
        .collect();
    assert_eq!(ids.len(), 2, "one vendor row and one alias row: {ids:?}");
    assert!(ids.contains(&"m-free"), "vendor row missing: {ids:?}");
    assert!(
        ids.contains(&"friendly-model-free"),
        "alias row missing: {ids:?}"
    );
}

#[tokio::test]
async fn empty_model_map_preserves_an_unusual_catalogue_verbatim() {
    const VENDOR_CATALOGUE: &str = r#"{"models":[{"name":"vendor-specific"}]}"#;
    let (mock, _state) = mock_upstream(vec![Reply::Buffered(VENDOR_CATALOGUE)]).await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let response = client()
        .get(format!("http://{proxy}/v1/models"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        serde_json::from_str::<Value>(VENDOR_CATALOGUE).unwrap()
    );
}

#[tokio::test]
async fn raw_openai_sse_relay_keeps_vendor_model_bytes() {
    let (mock, _state) = mock_upstream(vec![Reply::Sse(SSE_STREAM)]).await;
    let (server, mut provider) = cfg_for(&format!("http://{}", mock.0));
    provider
        .model_map
        .insert("friendly-model".into(), "m".into());
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let response = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({
            "model":"friendly-model",
            "messages":[],
            "stream":true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    assert!(
        text.contains(r#""id":"c1","model":"m","object":"chat.completion.chunk""#),
        "the raw OpenAI relay must leave the vendor model bytes untouched: {text:?}"
    );
    assert!(!text.contains(r#""model":"friendly-model""#));
    assert!(text.contains(r#""content":"hi""#));
    assert!(text.ends_with("data: [DONE]\n\n"));
    assert!(!text.contains("cost"));
}

#[tokio::test]
async fn relay_streams_openai_sse_end_to_end() {
    let (mock, _state) = mock_upstream(vec![Reply::Sse(SSE_STREAM)]).await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let pipeline = Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    );
    let proxy = serve(pipeline).await;

    let resp = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({"model":"m","messages":[{"role":"user","content":"x"}],"stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert!(text.contains("\"content\":\"hi\""), "got {text:?}");
    assert!(
        text.contains(r#""id":"c1","model":"m","object":"chat.completion.chunk""#),
        "chat stream must be byte-relayed (key order proves no Value round-trip): {text:?}"
    );
    assert!(text.ends_with("data: [DONE]\n\n"), "got {text:?}");
    assert!(
        !text.contains("cost"),
        "post-[DONE] frames must not relay: {text:?}"
    );
}

#[tokio::test]
async fn anthropic_stream_reports_provider_sse_ceiling_after_prior_text() {
    const VALID: &str = "data: {\"id\":\"c1\",\"model\":\"m\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"before ceiling\"},\"finish_reason\":null}]}\n\n";
    let body = format!("{VALID}data: {}", "x".repeat(256));
    let (mock, _state) = mock_upstream(vec![Reply::SseContiguous(body)]).await;
    let (server, mut provider) = cfg_for(&format!("http://{}", mock.0));
    provider.max_response_bytes = Some(192);
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let resp = client()
        .post(format!("http://{proxy}/v1/messages"))
        .json(&json!({"model":"m","max_tokens":10,"messages":[],"stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    let content = text
        .find("before ceiling")
        .expect("complete earlier record must relay before failure");
    let failure = text
        .find("event: error")
        .expect("provider decoder failure must use Anthropic's terminal error event");
    assert!(content < failure, "content must precede failure: {text:?}");
    assert!(
        !text.contains("[DONE]"),
        "failed stream must not look complete: {text:?}"
    );

    let terminal: Value = serde_json::from_str(
        text[failure..]
            .split_once("data: ")
            .expect("Anthropic error event data")
            .1
            .trim_end(),
    )
    .expect("terminal provider failure is an Anthropic error envelope");
    assert_eq!(terminal["type"], "error");
    assert_eq!(terminal["error"]["type"], "api_error");
    assert_eq!(
        terminal["error"]["message"],
        "SSE record exceeds size limit"
    );
}

#[tokio::test]
async fn anthropic_stream_missing_done_reports_eof_after_prior_content() {
    const VALID: &str = "data: {\"id\":\"c1\",\"model\":\"m\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"before EOF\"},\"finish_reason\":null}]}\n\n";
    let (mock, _state) = mock_upstream(vec![Reply::SseContiguous(VALID.to_string())]).await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let resp = client()
        .post(format!("http://{proxy}/v1/messages"))
        .json(&json!({"model":"m","max_tokens":10,"messages":[],"stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    let content = text
        .find("before EOF")
        .expect("prior content must relay before EOF failure");
    let error = text
        .find("event: error")
        .expect("missing [DONE] must become an Anthropic error event");
    assert!(content < error, "content must precede failure: {text:?}");
    assert!(
        !text.contains("message_stop"),
        "failed stream must not stop cleanly: {text:?}"
    );
    let terminal: Value = serde_json::from_str(
        text[error..]
            .split_once("data: ")
            .expect("Anthropic error event data")
            .1
            .trim_end(),
    )
    .expect("EOF failure must be an Anthropic error envelope");
    assert_eq!(terminal["type"], "error");
    assert_eq!(terminal["error"]["type"], "api_error");
    assert_eq!(
        terminal["error"]["message"],
        "upstream stream ended without [DONE]"
    );
}

#[tokio::test]
async fn anthropic_stream_with_done_ends_cleanly_after_relayed_content() {
    const VALID: &str = "data: {\"id\":\"c1\",\"model\":\"m\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"complete\"},\"finish_reason\":null}]}\n\n";
    let body = format!("{VALID}data: [DONE]\n\n");
    let (mock, _state) = mock_upstream(vec![Reply::SseContiguous(body)]).await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let resp = client()
        .post(format!("http://{proxy}/v1/messages"))
        .json(&json!({"model":"m","max_tokens":10,"messages":[],"stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert!(text.contains("complete"), "content must relay: {text:?}");
    assert!(
        text.contains("event: message_stop"),
        "clean EOF must close the message: {text:?}"
    );
    assert!(
        !text.contains("event: error"),
        "a [DONE] stream must not fail: {text:?}"
    );
}

#[tokio::test]
async fn anthropic_stream_mid_record_eof_flushes_content_then_reports_missing_done() {
    const DATA_LINE: &str = "data: {\"id\":\"c1\",\"model\":\"m\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"flushed at EOF\"},\"finish_reason\":null}]}\n";
    let (mock, _state) = mock_upstream(vec![Reply::SseContiguous(DATA_LINE.to_string())]).await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let resp = client()
        .post(format!("http://{proxy}/v1/messages"))
        .json(&json!({"model":"m","max_tokens":10,"messages":[],"stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    let content = text
        .find("flushed at EOF")
        .expect("final record must be flushed before EOF failure");
    let error = text
        .find("event: error")
        .expect("mid-record EOF without [DONE] must produce an error event");
    assert!(
        content < error,
        "flushed content must precede failure: {text:?}"
    );
    assert!(
        !text.contains("message_stop"),
        "truncated stream must not stop cleanly: {text:?}"
    );
    let terminal: Value = serde_json::from_str(
        text[error..]
            .split_once("data: ")
            .expect("Anthropic error event data")
            .1
            .trim_end(),
    )
    .expect("EOF failure must be an Anthropic error envelope");
    assert_eq!(
        terminal["error"]["message"],
        "upstream stream ended without [DONE]"
    );
}

/// The whole hint path, end to end: a REAL upstream 429 carrying a REAL
/// `retry-after`, through the proxy's own gate, out to the client.
///
/// The unit test around `retry_hint` proves the parser; this proves it is
/// WIRED, which is the distinction that let the previous version of this file
/// claim to honour `Retry-After` while nothing ever read the header — every
/// existing test built its `ProviderError` by hand, so the gap was unobservable
/// from inside the suite.
///
/// `retry-after: 7` is deliberately below the 120 s cap so the assertion pins
/// pass-through rather than a clamp; the clamp has its own client-side test below.
#[tokio::test]
async fn a_vendor_retry_after_reaches_the_client_untouched() {
    let (mock, state) = mock_upstream(vec![Reply::StatusWith(
        429,
        "{\"error\":{\"message\":\"slow\",\"type\":\"rate_limit_error\"}}",
        "retry-after",
        "7",
    )])
    .await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let pipeline = Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    );
    let proxy = serve(pipeline).await;

    let resp = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({"model":"m","messages":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 429, "the vendor's status must survive");
    assert_eq!(
        resp.headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok()),
        Some("7"),
        "the hint the vendor sent is the hint the client gets"
    );
    assert!(
        state.requests.load(std::sync::atomic::Ordering::SeqCst) >= 1,
        "the upstream was actually asked"
    );
}

/// The other half of that seam, and the only place the CLAMP is observable.
///
/// `retry_hint` deliberately returns what the vendor said, and the clamp happens
/// at the attach site, so before this test the `.min(MAX_COOLDOWN_SECS)` could be
/// deleted and every test in the suite still passed — including the doc comment
/// above, which claimed the cap "has its own unit test" and did not. A free-tier
/// vendor advertising hours is the documented reason the cap exists, so the
/// behaviour it encodes is worth more than a comment asserting it.
#[tokio::test]
async fn a_vendor_hint_beyond_the_cap_is_clamped_for_everyone() {
    let (mock, _state) = mock_upstream(vec![Reply::StatusWith(
        429,
        "{\"error\":{\"message\":\"slow\",\"type\":\"rate_limit_error\"}}",
        "retry-after",
        "3600",
    )])
    .await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let pipeline = Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    );
    let proxy = serve(pipeline).await;

    let resp = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({"model":"m","messages":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 429, "the vendor's status still survives");
    assert_eq!(
        resp.headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok()),
        Some("120"),
        "an hour-long hint must reach the client as the cap, not the hour"
    );
}

#[tokio::test]
async fn relay_retries_429_then_returns_buffered() {
    let (mock, state) = mock_upstream(vec![
        Reply::Status(
            429,
            "{\"error\":{\"message\":\"slow\",\"type\":\"rate_limit_error\"}}",
        ),
        Reply::Buffered(BUFFERED),
    ])
    .await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let pipeline = Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    );
    let proxy = serve(pipeline).await;

    let resp = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({"model":"m","messages":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["usage"]["vendor_only"], 42, "raw must survive");
    assert_eq!(
        state.requests.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "exactly one retry"
    );
    // The forwarded body still carries the client's messages intact; only
    // the vendor gate's forced `stream: true` differs from what the client
    // sent.
    let sent: Value = serde_json::from_slice(state.bodies.lock().first().unwrap()).unwrap();
    assert_eq!(sent["stream"], true);
}

#[tokio::test]
async fn anthropic_bridge_over_real_upstream() {
    let (mock, _state) = mock_upstream(vec![Reply::Buffered(BUFFERED)]).await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let pipeline = Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    );
    let proxy = serve(pipeline).await;

    let resp = client()
        .post(format!("http://{proxy}/v1/messages"))
        .json(&json!({"model":"m","max_tokens":10,"messages":[{"role":"user","content":"x"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "message");
    assert_eq!(body["content"][0]["text"], "hello");
    assert_eq!(body["stop_reason"], "end_turn");
}

/// The fold only counts when it reaches a socket: a Claude client's `image`
/// and `document` blocks must arrive at a chat-speaking upstream as an
/// `image_url` part and a `file` part, with the base64 intact inside a data URL
/// and each block keeping its place in the array. No string test can prove
/// this — the provider passes `messages` through UNPARSED, so the part array
/// either survives the whole IR round-trip or it does not.
#[tokio::test]
async fn anthropic_media_reaches_the_upstream_as_image_and_file_parts() {
    let (mock, state) = mock_upstream(vec![Reply::Buffered(BUFFERED)]).await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let pipeline = Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    );
    let proxy = serve(pipeline).await;

    let resp = client()
        .post(format!("http://{proxy}/v1/messages"))
        .json(
            &json!({"model":"m","max_tokens":10,"messages":[{"role":"user","content":[
                {"type":"text","text":"what is in this?"},
                {"type":"image","source":{
                    "type":"base64","media_type":"image/png","data":"AANA"
                }}
            ]}]}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let sent: Value = serde_json::from_slice(state.bodies.lock().first().unwrap()).unwrap();
    let parts = sent["messages"][0]["content"]
        .as_array()
        .expect("the folded turn must be a part array");
    assert_eq!(parts[0]["type"], "text");
    assert_eq!(parts[0]["text"], "what is in this?");
    assert_eq!(parts[1]["type"], "image_url");
    assert_eq!(parts[1]["image_url"]["url"], "data:image/png;base64,AANA");
    assert!(
        parts[1]["image_url"].get("detail").is_none(),
        "Anthropic has no sizing field, so the upstream must not be told one"
    );

    // …and the document half, which must leave as chat's `file` part with the
    // neutral filename the fold supplies for a block Anthropic gives no name.
    let req = client()
        .post(format!("http://{proxy}/v1/messages"))
        .json(
            &json!({"model":"m","max_tokens":10,"messages":[{"role":"user","content":[
                {"type":"document","source":{
                    "type":"base64","media_type":"application/pdf","data":"JVBER"
                }}
            ]}]}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(req.status(), 200);
    let doc: Value =
        serde_json::from_slice(state.bodies.lock().get(1).expect("second request")).unwrap();
    let part = &doc["messages"][0]["content"][0];
    assert_eq!(part["type"], "file");
    assert_eq!(
        part["file"]["file_data"],
        "data:application/pdf;base64,JVBER"
    );
    assert_eq!(part["file"]["filename"], "document.pdf");
}

/// The chat dialect's half of the same promise: media parts ride the request
/// UNPARSED. Pinned at the wire because the one change that would break it is
/// a plausible-looking "normalize messages" in `to_upstream_body`, which has
/// no fold to notice it lost a client's image or PDF.
#[tokio::test]
async fn chat_media_parts_reach_the_upstream_untouched() {
    let (mock, state) = mock_upstream(vec![Reply::Buffered(BUFFERED)]).await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let pipeline = Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    );
    let proxy = serve(pipeline).await;

    let resp = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({"model":"m","messages":[{"role":"user","content":[
            {"type":"text","text":"and this?"},
            {"type":"image_url","image_url":{
                "url":"https://host/a.png","detail":"low","vendor_extension":true
            }},
            {"type":"file","file":{
                "filename":"client.pdf","file_data":"data:application/pdf;base64,JVBER"
            }}
        ]}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let sent: Value = serde_json::from_slice(state.bodies.lock().first().unwrap()).unwrap();
    let image = &sent["messages"][0]["content"][1];
    assert_eq!(image["image_url"]["url"], "https://host/a.png");
    assert_eq!(image["image_url"]["detail"], "low");
    assert_eq!(
        image["image_url"]["vendor_extension"], true,
        "unparsed means a field the IR never modelled still arrives"
    );
    let file = &sent["messages"][0]["content"][2];
    assert_eq!(file["type"], "file");
    assert_eq!(file["file"]["filename"], "client.pdf");
    assert_eq!(
        file["file"]["file_data"],
        "data:application/pdf;base64,JVBER"
    );
}

#[tokio::test]
async fn non_retryable_400_reaches_client_verbatim_after_one_attempt() {
    let (mock, state) = mock_upstream(vec![Reply::Status(
        400,
        "{\"error\":{\"message\":\"bad field\",\"code\":\"x_conflict\"}}",
    )])
    .await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let pipeline = Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    );
    let proxy = serve(pipeline).await;

    let resp = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({"model":"m","messages":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "x_conflict");
    assert_eq!(state.requests.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn buffered_non_json_200_is_a_deterministic_parse_verdict() {
    let (mock, state) = mock_upstream(vec![Reply::Buffered("upstream returned plain text")]).await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let pipeline = Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    );
    let proxy = serve(pipeline).await;

    let resp = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({"model":"m","messages":[]}))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        502,
        "a non-JSON 200 is a gateway parse failure"
    );
    assert_eq!(
        state.requests.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the deterministic parse verdict must not be retried"
    );
}

#[tokio::test]
async fn responses_dialect_over_real_upstream() {
    let (mock, _state) = mock_upstream(vec![Reply::Sse(SSE_STREAM)]).await;
    let (server, provider) = cfg_for(&format!("http://{}", mock.0));
    let pipeline = Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    );
    let proxy = serve(pipeline).await;

    // Responses-style client, chat-completions-only upstream: the bridge
    // folds input->messages on the way in and re-expands response.* events
    // on the way out.
    let resp = client()
        .post(format!("http://{proxy}/v1/responses"))
        .json(&json!({"model":"m","input":[{"role":"user","content":[{"type":"input_text","text":"x"}]}],"stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert!(text.contains("event: response.created"), "got {text:?}");
    assert!(text.contains("\"delta\":\"hi\""), "got {text:?}");
    assert!(text.contains("event: response.completed"), "got {text:?}");
    assert!(!text.contains("[DONE]"), "got {text:?}");
}

/// The README quick start is `cp config.example.json config.json` — if that
/// file is invalid JSON or drifts from the structs (likely both ways under
/// `deny_unknown_fields`: a renamed field, a dropped key, a comma), every
/// fork's first run is a startup error no compile-time gate sees. Both
/// shipped examples go through the REAL boot gates: serde against the
/// structs, `socket_addrs` (a non-resolvable bind must fail before the
/// drift test, not at first boot), and `ProxyConfig::from_doc` (which
/// validates, mirroring opencode2api-transport's own example test).
#[test]
fn example_config_parses_as_the_documented_shape() {
    let _env_lock = proxy_env_lock();
    let _proxy_env = ProxyEnvGuard::clear();
    for file in [
        "../../config.example.json",
        "../../config.production.example.json",
    ] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(file);
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
        let doc: Value =
            serde_json::from_str(&raw).unwrap_or_else(|e| panic!("{file} must be valid JSON: {e}"));
        let server: ServerConfig = serde_json::from_value(doc["server"].clone())
            .expect("server section must match ServerConfig");
        let provider: ServiceConfig = serde_json::from_value(doc["provider"].clone())
            .expect("provider section must match ServiceConfig (deny_unknown_fields!)");
        assert_eq!(server.port, 10080, "{file}");
        assert!(provider.model_map.is_empty(), "{file}");
        assert_eq!(provider.base_url, "https://api.openai.com/v1", "{file}");
        // Boot gate the same way the bin does: addresses resolve (extras
        // included), and the proxy section is either absent or valid.
        assert!(!server.socket_addrs().unwrap().is_empty(), "{file}");
        let proxy = opencode2api_transport::ProxyConfig::from_doc(&doc)
            .unwrap_or_else(|e| panic!("{file} proxy section must load: {e}"));
        assert_eq!(proxy.is_some(), doc.get("proxy").is_some(), "{file}");
    }
}

/// The egress pool's contract at the only level that proves it: a request
/// leaves through the PROXY, carrying a username this process minted, with
/// the session id and the vendor's own TTL spelling in it. Everything above
/// this line is a unit test of string formatting.
#[tokio::test]
async fn proxy_lane_puts_a_minted_session_on_the_wire() {
    // The mock plays the proxy. The upstream address is a black hole, so a
    // reply can only mean the request went through the lane.
    let (proxy_mock, state) = mock_upstream(vec![Reply::Sse(SSE_STREAM)]).await;
    let (server, provider) = cfg_for("http://192.0.2.1:9");
    let proxy = opencode2api_transport::ProxyConfig {
        vendors: vec![opencode2api_transport::ProxyVendorConfig {
            name: "testvendor".into(),
            url: format!(
                "http://user-acct-session-{{session}}-ttl-{{ttl_seconds}}:secret@{}",
                proxy_mock.0
            ),
            lanes: 1,
            session_ttl_secs: 3600,
            session_len: 7,
            ..Default::default()
        }],
        prewarm: false,
        ..Default::default()
    };
    let transport =
        opencode2api_transport::Transport::with_proxy(&server, &proxy).expect("pool builds");
    let pipeline = Pipeline::new(
        Arc::new(OpenAiProvider::new(transport, provider).unwrap()),
        Arc::new(server),
        None,
    );
    let proxy = serve(pipeline).await;

    let resp = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({"model":"m","messages":[{"role":"user","content":"x"}],"stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "the lane carried the request");
    let _ = resp.text().await;

    let heads = state.heads.lock().clone();
    let head = heads.first().expect("the proxy saw a request");
    // Absolute-form request line: this is a proxy hop, not a direct call.
    assert!(
        head.to_lowercase().starts_with("post http://192.0.2.1:9/"),
        "not proxied: {head}"
    );
    // Header NAMES are case-insensitive; the base64 value is not, so match
    // on a lowercased copy and take the value from the original line.
    let creds = head
        .lines()
        .find(|l| l.to_lowercase().starts_with("proxy-authorization: basic "))
        .map(|l| &l["proxy-authorization: basic ".len()..])
        .expect("lane credential missing");
    let decoded = String::from_utf8(base64_decode(creds.trim())).unwrap();
    let (user, pass) = decoded.split_once(':').expect("user:pass");
    assert_eq!(pass, "secret");
    let session = user
        .strip_prefix("user-acct-session-")
        .and_then(|r| r.strip_suffix("-ttl-3600"))
        .unwrap_or_else(|| panic!("template did not render: {user}"));
    assert_eq!(session.len(), 7, "vendor-shaped id: {session}");
    assert!(session.bytes().all(|c| c.is_ascii_digit()), "{session}");
}

/// One vendor, one lane, upstream a black hole: any reply can only have come
/// through the lane, and a single-lane pool cannot substitute a healthy exit
/// once that lane goes bad. Returns the lane the pool minted so a test reads
/// the pool's own verdict rather than a re-derived one.
///
/// A thin shim over `lane_pool` rather than a second pool builder: the two
/// differed only in the username template's spelling, and a near-identical
/// builder is a place for the two to drift apart. Callers that need a
/// different template pass it through `lane_pool`'s `tune` closure.
fn single_lane_pool(
    proxy: &Socket,
) -> (
    ServerConfig,
    ServiceConfig,
    Arc<opencode2api_transport::Transport>,
    Arc<opencode2api_transport::Lane>,
) {
    let (server, provider, transport) = lane_pool(proxy, |v| {
        v.url = format!("http://session-{{session}}:secret@{}", proxy.0)
    });
    let lane = transport.lane().expect("initial proxy lane");
    (server, provider, transport, lane)
}

/// A downstream stream that stops after a completed handoff records use without
/// claiming that a later poisoned lane recovered. The age assertions prove the
/// touch boundary; the health assertion proves use and health remain separate.
#[tokio::test]
async fn clean_stream_completion_restores_lane_health() {
    let frame_written = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let (proxy_mock, state) = mock_upstream(vec![Reply::GatedSse {
        frame: r#"data: {"id":"c1","model":"m","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}"#,
        frame_written: frame_written.clone(),
        release: release.clone(),
    }])
    .await;
    let (server, provider, transport, lane) = single_lane_pool(&proxy_mock);
    lane.note_success();
    // Let a header-time touch land in the next whole-second bucket; the
    // completion-only implementation must leave the lane's last touch at zero.
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let session = lane.session().to_owned();
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(transport, provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let frame_seen = frame_written.notified();
    let response = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({"model":"m","messages":[],"stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    frame_seen.await;
    assert!(
        lane.idle_age_for_test() >= std::time::Duration::from_secs(1),
        "headers alone must not touch the lane while the body is still in flight"
    );
    let in_flight_age = lane.idle_age_for_test();

    for _ in 0..3 {
        lane.note_failure();
    }
    assert!(
        format!("{lane:?}").contains("healthy: false"),
        "the body guard must still own completion while the SSE stream is suspended"
    );

    let head = state
        .heads
        .lock()
        .first()
        .expect("proxy captured the request")
        .clone();
    let credentials = header_value(&head, "proxy-authorization")
        .expect("proxy credential missing")
        .strip_prefix("Basic ")
        .expect("basic proxy credential");
    let user = String::from_utf8(base64_decode(credentials))
        .unwrap()
        .split_once(':')
        .expect("proxy credential shape")
        .0
        .to_owned();
    assert!(
        user.contains(&session),
        "stream used a different proxy session"
    );

    release.notify_one();
    assert!(response.text().await.unwrap().ends_with("data: [DONE]\n\n"));
    assert!(
        format!("{lane:?}").contains("healthy: true"),
        "clean upstream completion must restore lane health"
    );
    assert!(
        lane.idle_age_for_test() < in_flight_age,
        "body completion must reset the lane's idle age"
    );
}

/// The failure half of the outcome guard: an upstream body that dies MID-TRANSFER
/// is a transport fault, so the lane must be pulled from rotation once the
/// failures reach the pool's threshold — and must NOT be recorded as use.
/// The age assertion is what separates `note_failure` from `note_used`: both
/// leave health alone below the threshold, so only the touch (or its absence)
/// tells the two apart.
#[tokio::test]
async fn a_body_read_error_retires_the_lane_without_recording_use() {
    let frame_written = Arc::new(tokio::sync::Notify::new());
    let (proxy_mock, state) = mock_upstream(vec![Reply::TruncatedChunkedSse {
        frame: r#"data: {"id":"c1","model":"m","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}"#,
        frame_written: frame_written.clone(),
    }])
    .await;
    let (server, provider, transport, lane) = single_lane_pool(&proxy_mock);
    lane.note_success();
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(transport, provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let first = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({"model":"m","messages":[],"stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 200);
    let first_body = first.text().await.unwrap();
    assert!(
        first_body.contains(r#""content":"hi""#),
        "the complete frame before the cut must still reach the client: {first_body:?}"
    );
    assert!(
        !first_body.contains("data: [DONE]"),
        "a body that died mid-transfer is truncation, never a clean end: {first_body:?}"
    );
    let in_flight_age = lane.idle_age_for_test();
    assert!(
        in_flight_age >= std::time::Duration::from_secs(1),
        "a failed body must not have touched the lane: {in_flight_age:?}"
    );
    // Two more, and a whole second across which any completion-bound touch
    // would land, so the age comparison cannot pass on a same-second store.
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    for _ in 0..2 {
        let seen = frame_written.notified();
        let resp = client()
            .post(format!("http://{proxy}/v1/chat/completions"))
            .json(&json!({"model":"m","messages":[],"stream":true}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        seen.await;
        let body = resp.text().await.unwrap();
        assert!(
            !body.contains("data: [DONE]"),
            "every cut transfer must stay truncated: {body:?}"
        );
    }
    assert_eq!(
        state.requests.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "all three faults must have reached the upstream mock"
    );
    assert!(
        format!("{lane:?}").contains("healthy: false"),
        "three body read errors must pull the only lane from rotation: {lane:?}"
    );
    assert!(
        lane.idle_age_for_test() >= in_flight_age,
        "a transport error is a failure verdict, never a use: use would reset the idle age"
    );
}

/// The same completion contract on the FOLDED path, where the verdict is made
/// by the SSE decoder rather than by a byte scan: a lane poisoned mid-stream
/// must be restored by a folded `[DONE]`, and the dialect's own terminator
/// (`message_stop`) must reach the client.
#[tokio::test]
async fn a_folded_stream_with_done_restores_a_poisoned_lane() {
    let frame_written = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let (proxy_mock, _state) = mock_upstream(vec![Reply::GatedSse {
        frame: r#"data: {"id":"c1","model":"m","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"folded hi"},"finish_reason":null}]}"#,
        frame_written: frame_written.clone(),
        release: release.clone(),
    }])
    .await;
    let (server, provider, transport, lane) = single_lane_pool(&proxy_mock);
    lane.note_success();
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(transport, provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let seen = frame_written.notified();
    let resp = client()
        .post(format!("http://{proxy}/v1/messages"))
        .json(&json!({"model":"m","max_tokens":10,"messages":[],"stream":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    seen.await;
    assert!(
        lane.idle_age_for_test() >= std::time::Duration::from_secs(1),
        "the folded body is still in flight; the lane must not be touched yet"
    );
    let in_flight_age = lane.idle_age_for_test();
    for _ in 0..3 {
        lane.note_failure();
    }
    assert!(format!("{lane:?}").contains("healthy: false"));

    release.notify_one();
    let text = resp.text().await.unwrap();
    assert!(text.contains("folded hi"), "content must relay: {text:?}");
    assert!(
        text.contains("event: message_stop"),
        "a folded [DONE] must close the Anthropic message: {text:?}"
    );
    assert!(
        !text.contains("event: error"),
        "a completed fold must not report failure: {text:?}"
    );
    assert!(
        format!("{lane:?}").contains("healthy: true"),
        "the folded decoder's [DONE] must restore lane health: {lane:?}"
    );
    assert!(
        lane.idle_age_for_test() < in_flight_age,
        "folded completion must reset the lane's idle age"
    );
}

#[tokio::test]
async fn vendor_reply_over_the_ceiling_is_a_gateway_error() {
    // Response-side twin of max_body_bytes: an upstream that keeps sending
    // buffered JSON past provider.max_response_bytes must fail the request,
    // not make the proxy allocate. The rest of this suite proves the SAME
    // ~200-byte BUFFERED reply succeeds at the default ceiling — this test
    // only moves the cap, so it cannot pass vacuously.
    let (mock, state) = mock_upstream(vec![Reply::Buffered(BUFFERED)]).await;
    let (server, mut provider) = cfg_for(&format!("http://{}", mock.0));
    provider.max_response_bytes = Some(48);
    let proxy = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(client_for(&server), provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;
    let resp = client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({"model":"m","messages":[{"role":"user","content":"x"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502, "ceiling breach is a gateway failure");
    // DECISION, not accident: the breach is a deterministic local verdict, so
    // the fold must not re-fetch the same impossible request.
    assert_eq!(
        state.requests.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "deterministic local verdict is non-retryable"
    );
}

// ── W10f: the documented egress contracts, proved on the wire ─────────────
// Every test below points the upstream at a black hole and lets the MOCK play
// the proxy, so every reply can only have come through a lane, and every
// assertion is read off bytes the mock received. Nothing here re-derives a
// verdict the production code already knows; each one observes.

/// Read the `Proxy-Authorization` username off a captured request head. This
/// is the only place a lane's minted session is visible at all — the URL it
/// came from is never retained anywhere, by design.
fn wire_session(head: &str) -> String {
    let creds = header_value(head, "proxy-authorization")
        .expect("lane credential missing")
        .strip_prefix("Basic ")
        .expect("basic proxy credential");
    String::from_utf8(base64_decode(creds))
        .unwrap()
        .split_once(':')
        .expect("proxy credential is user:pass")
        .0
        .to_owned()
}

/// Requests the mock saw, with the connection each one rode.
fn wire_requests(state: &MockState) -> Vec<(usize, String)> {
    let heads = state.heads.lock().clone();
    let conns = state.conns.lock().clone();
    assert_eq!(
        heads.len(),
        conns.len(),
        "every head carries its connection"
    );
    conns.into_iter().zip(heads).collect()
}

/// A single-lane pool whose only vendor points at `proxy`, with a templated
/// username the test can read the session back out of.
///
/// `prewarm` is set as the VENDOR override on purpose, not as decoration:
/// `warm()` resolves `vendor.prewarm().unwrap_or(pool.prewarm)`, so a
/// `Some(false)` here silently wins over the pool-level knob and a test that
/// means to exercise pool prewarm must override it back through `tune`. Every
/// test that does not care leaves it off; the prewarm test sets it on
/// explicitly rather than relying on the pool default.
fn lane_pool(
    proxy: &Socket,
    tune: impl FnOnce(&mut opencode2api_transport::ProxyVendorConfig),
) -> (
    ServerConfig,
    ServiceConfig,
    Arc<opencode2api_transport::Transport>,
) {
    let (server, provider) = cfg_for("http://192.0.2.1:9");
    let mut vendor = opencode2api_transport::ProxyVendorConfig {
        name: "testvendor".into(),
        url: format!("http://user-session-{{session}}:secret@{}", proxy.0),
        min_lanes: 1,
        lanes: 1,
        session_ttl_secs: 3600,
        session_len: 7,
        prewarm: Some(false),
        ..Default::default()
    };
    tune(&mut vendor);
    let config = opencode2api_transport::ProxyConfig {
        vendors: vec![vendor],
        prewarm: false,
        ..Default::default()
    };
    let transport =
        opencode2api_transport::Transport::with_proxy(&server, &config).expect("pool builds");
    (server, provider, transport)
}

/// One buffered chat request through a proxy-egress pipeline. The status is
/// the test's to assert: some proofs are about a 200, and the upstream-status
/// proof is about a 429 arriving THROUGH a working lane.
async fn send_chat(proxy: std::net::SocketAddr) -> reqwest::Response {
    client()
        .post(format!("http://{proxy}/v1/chat/completions"))
        .json(&json!({"model":"m","messages":[{"role":"user","content":"x"}]}))
        .send()
        .await
        .unwrap()
}

/// As `send_chat`, for the tests whose claim is "the lane carried it".
async fn ask(proxy: std::net::SocketAddr) -> reqwest::Response {
    let resp = send_chat(proxy).await;
    assert_eq!(resp.status(), 200, "the lane carried the request");
    resp
}

/// (a) SHARDS multiply CONNECTIONS, never sessions.
///
/// Two shard clients on one lane are the whole feature: they lift a peer's
/// `MAX_CONCURRENT_STREAMS` ceiling, and every one of them carries the SAME
/// sticky session, so the exit IP does not move. The mock keeps its socket
/// alive between replies, so a round-robin over two pools shows up as two
/// distinct connections — while the credential on every request is byte-equal.
#[tokio::test]
async fn shards_open_more_connections_onto_one_session() {
    let (proxy_mock, state) = mock_upstream(vec![Reply::KeepAlive(BUFFERED)]).await;
    let (server, provider, transport) = lane_pool(&proxy_mock, |v| v.shards = Some(2));
    let lane = transport.lane().expect("lane");
    let addr = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(transport, provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    for _ in 0..6 {
        let _ = ask(addr).await.text().await;
    }

    let seen = wire_requests(&state);
    assert_eq!(seen.len(), 6, "every request reached the mock proxy");
    let sessions: Vec<String> = seen.iter().map(|(_, head)| wire_session(head)).collect();
    let first = &sessions[0];
    assert!(
        sessions.iter().all(|s| s == first),
        "shards must share one session, got {sessions:?}"
    );
    assert_eq!(
        first.strip_prefix("user-session-").map(str::to_owned),
        Some(lane.session().to_owned()),
        "the wire credential is the lane's own minted id"
    );
    // Two pools, so at least two connections: shards multiply connections.
    let distinct: std::collections::BTreeSet<usize> = seen.iter().map(|(c, _)| *c).collect();
    assert!(
        distinct.len() >= 2,
        "two shard pools must not share one connection, saw {distinct:?}"
    );
    assert_eq!(lane.shards(), 2, "the lane really did hold two pools");
}

/// (b) A prewarmed lane's first real request rides the connection the probe
/// opened.
///
/// WHAT THIS PROVES: the probe reached the vendor, and the socket it opened
/// was back in the client's pool by the time the first real request went out
/// — two requests, one accept, the same `conn_id`. That is the whole point of
/// spending a handshake off the request path, and it is a real regression
/// net: a probe whose response was never finished could not leave a reusable
/// connection behind.
///
/// WHAT IT DOES NOT PROVE, stated plainly because the tempting overclaim here
/// is "and the probe was DRAINED". It is not, and cannot be, here. A `HEAD`
/// response has no body by definition, and hyper exposes zero body bytes for
/// one to reqwest regardless of what a peer sends, so `drain()`'s `chunk()`
/// loop has nothing to consume and this test would still see `accepts == 1`
/// with `drain()` deleted outright. The historical prewarm bug needed a
/// body-carrying probe response, which a `HEAD` cannot produce; pinning
/// drain's effect means asserting at the pool that a completed probe's
/// connection returned to it, which is what the reuse assertion above
/// demonstrates indirectly and the only faithful shape available on this
/// stack.
///
/// The baseline is the same pool with prewarm off — the half that proves the
/// TOGGLE rather than whether the test called a function. Same peer, same
/// keep-alive, no probe at all, so the first request opens the connection
/// itself. The accept count is 1 in BOTH arms: the load-bearing number is
/// requests-per-connection (warm 2 over 1, cold 2 over 1 with zero `HEAD`s),
/// not accepts.
#[tokio::test]
async fn a_prewarm_probe_opens_the_connection_the_first_request_reuses() {
    // -- prewarm on: the probe opens the connection, the request rides it.
    let (warm_mock, warm_state) =
        mock_upstream(vec![Reply::KeepAliveProbe { request: BUFFERED }]).await;
    let (server, provider, transport) = lane_pool(&warm_mock, |v| v.prewarm = Some(true));
    // The floor lane was minted at CONSTRUCTION, which is not a prewarm — it
    // has never opened a connection. Poison it so the next maintenance pass
    // mints a replacement, and that replacement is warmed before it is
    // adopted. That warm pass is the probe this test is about, and the
    // replacement is the lane the request will actually be served by.
    let cold = transport.lane().expect("floor lane");
    for _ in 0..3 {
        cold.note_failure();
    }
    transport
        .maintain_once_for_test(Some("http://192.0.2.1:9/ready"))
        .await;
    assert_eq!(
        warm_state
            .requests
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "maintenance warmed exactly the replacement lane"
    );
    let addr = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(transport, provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;
    let _ = ask(addr).await.text().await;

    let warm = wire_requests(&warm_state);
    let probe = warm
        .iter()
        .find(|(_, head)| request_method(head) == "HEAD")
        .expect("prewarm sent its probe");
    let served = wire_requests(&warm_state)
        .into_iter()
        .find(|(_, head)| request_method(head) == "POST")
        .expect("the real request went to the wire");
    // The probe's HEAD is asserted PRESENT before the accept count: a missing
    // probe and a working probe both look like "one accept", so the count
    // only means something once the probe is known to have been sent.
    assert_eq!(
        probe.0, served.0,
        "the probe's connection must be the one the first real request rides"
    );
    assert_eq!(
        warm_state.accepts.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "probe and first request are one connection, not two"
    );

    // -- prewarm off: no probe, so the request itself opens the connection.
    //    This arm does two jobs. It proves the TOGGLE (same peer, same
    //    keep-alive, zero HEADs), and the `cold[0].0 == cold[1].0` check below
    //    proves the PEER reuses a socket it already holds — without that, the
    //    warm half's single accept could be a peer that never reuses at all.
    let (cold_mock, cold_state) = mock_upstream(vec![Reply::KeepAlive(BUFFERED)]).await;
    let (server, provider, transport) = lane_pool(&cold_mock, |v| v.prewarm = Some(false));
    let addr = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(transport, provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;
    // Two requests: the first must open the connection, and the second proves
    // the peer is willing to reuse it. If the peer were not, the warm half's
    // single-accept assertion would pass for the wrong reason.
    let _ = ask(addr).await.text().await;
    let _ = ask(addr).await.text().await;

    let cold = wire_requests(&cold_state);
    assert_eq!(cold.len(), 2, "both requests reached the mock proxy");
    assert!(
        cold.iter().all(|(_, head)| request_method(head) != "HEAD"),
        "prewarm off must not probe at all, saw {:?}",
        cold.iter()
            .map(|(_, h)| request_method(h))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        cold[0].0, cold[1].0,
        "the peer reuses its connection once one exists, so the warm half's \
         single accept is real reuse and not a peer that never reuses"
    );
    assert_eq!(
        cold_state.accepts.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "with no probe, the handshake is paid on the request path — but only \
         once, and the request still succeeds"
    );
}

/// (c) Every lane dead is a fail-CLOSED 503 the client can act on.
///
/// Falling back to direct egress here would leak the origin IP — the one
/// outcome the whole proxy subsystem exists to prevent — so the pool refuses,
/// and the refusal has to reach the client as a status plus a `Retry-After`,
/// not just as a `ProviderError` three layers down. This is the RENDER side of
/// the shed contract, which `Transport::lane` alone cannot prove.
#[tokio::test]
async fn a_dead_pool_renders_503_with_retry_after() {
    let (proxy_mock, state) = mock_upstream(vec![Reply::KeepAlive(BUFFERED)]).await;
    let (server, provider, transport) = lane_pool(&proxy_mock, |_| {});
    let pool = transport.clone();
    let lane = transport.lane().expect("lane");
    let addr = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(transport, provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    for _ in 0..3 {
        lane.note_failure();
    }

    let resp = client()
        .post(format!("http://{addr}/v1/chat/completions"))
        .json(&json!({"model":"m","messages":[{"role":"user","content":"x"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503, "a dead pool fails closed");
    assert_eq!(
        resp.headers()
            .get(reqwest::header::RETRY_AFTER)
            .map(|v| v.to_str().unwrap()),
        Some("5"),
        "a shed must tell the client when to come back"
    );
    let body: Value = resp.json().await.unwrap();
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("no healthy upstream proxy lane"),
        "the client must learn it was shed, not that the vendor failed: {body}"
    );
    assert_eq!(
        state.requests.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "nothing may be sent anywhere once the pool has shed"
    );
    assert!(
        !pool.is_ready(),
        "a pool with no healthy lane and no room left is not ready"
    );
}

/// (d) An upstream STATUS is the vendor's business, not the lane's.
///
/// A 429 and a 500 both arrived THROUGH a working tunnel, so attributing them
/// to the exit IP would pull a healthy lane out of rotation every time the
/// vendor rate-limited us — the pool would shed traffic for reasons that have
/// nothing to do with the proxy. The observable is the credential, not an
/// internal counter: the same lane is still selected afterwards, carrying the
/// same session. (`Lane::is_healthy` and the failure count are private, so a
/// src-side seam would be the alternative; see the report.)
#[tokio::test]
async fn an_upstream_429_and_500_leave_the_lane_in_rotation() {
    // The server retries a retryable status up to `max_attempts` (3), so each
    // scripted status occupies three consecutive script slots. Indexed by
    // request, not by connection: this lane's socket is pooled and reused.
    const RATE_LIMITED: (u16, &str) = (429, "{\"error\":{\"message\":\"slow down\"}}");
    const BROKEN: (u16, &str) = (500, "{\"error\":{\"message\":\"vendor exploded\"}}");
    let (proxy_mock, state) = mock_upstream(vec![Reply::KeepAliveScript(&[
        (200, BUFFERED),
        RATE_LIMITED,
        RATE_LIMITED,
        RATE_LIMITED,
        BROKEN,
        BROKEN,
        BROKEN,
        (200, BUFFERED),
    ])])
    .await;
    let (server, provider, transport) = lane_pool(&proxy_mock, |_| {});
    let lane = transport.lane().expect("lane");
    let addr = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(transport, provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    let _ = ask(addr).await.text().await;
    // Each of these burns `max_attempts` attempts before the status reaches
    // the client, which is why the script above gives each status three
    // slots. A 429 and a 500 are both RETRYABLE, so the retry loop is what
    // makes them lane-neutral: every attempt re-acquires the same lane.
    let throttled = send_chat(addr).await;
    assert_eq!(
        throttled.status(),
        429,
        "the vendor's status reaches the client"
    );
    let _ = throttled.text().await;
    let broken = send_chat(addr).await;
    assert_eq!(
        broken.status(),
        500,
        "the vendor's status reaches the client"
    );
    let _ = broken.text().await;

    // The lane is still selected — and still the SAME one, which is the whole
    // claim: three transport failures would have retired it and shed.
    let recovered = ask(addr).await;
    let _ = recovered.text().await;

    let seen = wire_requests(&state);
    let post: Vec<&String> = seen
        .iter()
        .filter(|(_, head)| request_method(head) == "POST")
        .map(|(_, head)| head)
        .collect();
    assert!(
        post.iter()
            .all(|head| wire_session(head) == format!("user-session-{}", lane.session())),
        "a vendor status must not rotate the lane: {:?}",
        post.iter().map(|h| wire_session(h)).collect::<Vec<_>>()
    );
    // `is_healthy` is private to the transport crate; the Debug rendering is
    // the lane's own verdict, and the wire credential above already proved
    // this same lane is still the one being selected.
    assert!(
        format!("{lane:?}").contains("healthy: true"),
        "a vendor status must not pull a working lane out of rotation: {lane:?}"
    );
}

/// (e) `ready()` is a promise about the FUTURE, so it must not spend anything.
///
/// A readiness probe that minted lanes would keep an otherwise idle pool warm
/// at the vendor's billed rate — the exact cost the adaptive pool exists to
/// avoid — and load balancers poll `/ready` on a timer whether or not anyone
/// is calling.
///
/// `min_lanes: 0` is the whole point, not a detail. With a floor lane held,
/// `is_ready()` returns early on "a healthy lane is held" and never reaches
/// the headroom branch — the only path on which a mutating `ready()` could
/// mint — so a floor lane would make this test unable to fail for the bug it
/// targets. Empty pool, `lanes: 1`: the pool reports ready by minting, and
/// must not.
///
/// THE WITNESS IS THE GAUGE, not the socket, and that took a measurement to
/// find rather than an argument. Minting is pure configuration: `mint_lane`
/// builds a reqwest client from a URL and opens nothing. A deliberately
/// mutating `is_ready()` was measured against the accept-count version of
/// this test and left it PASSING — zero accepts, zero requests, because
/// nothing connected. What a mint does emit is `publish_pool_metrics`, so
/// `opencode2api_proxy_lanes` is the only observable separating "did not mint" from
/// "minted a lane that never connected". The two wire counts are kept
/// alongside it because they prove the half no gauge can: readiness never
/// TOUCHES the vendor, so it costs no quota.
///
/// `#[test]`, not `#[tokio::test]`, and current-thread on purpose:
/// `with_local_recorder` installs a THREAD-LOCAL recorder whose guard drops
/// when the closure returns, so an async body would record onto the global
/// recorder instead and the snapshot would come back empty. A current-thread
/// runtime keeps the served router's work on this thread — the pattern
/// opencode2api-server's tests and transport's own lifecycle tests already use.
#[test]
fn readiness_probes_never_mint_a_lane_or_touch_the_vendor() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    const VENDOR: &str = "w10f-ready";

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let (proxy_mock, state) = rt.block_on(mock_upstream(vec![Reply::KeepAlive(BUFFERED)]));

    metrics::with_local_recorder(&recorder, || {
        // Snapshot-and-scan, the shape opencode2api-kit's pool gauge test uses: a
        // gauge holds one value per label set, so each phase reads the series
        // fresh instead of trusting an earlier reading.
        let lanes = |vendor: &str| -> Option<f64> {
            for (key, _, _, value) in snapshotter.snapshot().into_vec() {
                if key.key().name() != opencode2api_transport::names::LANES
                    || !key
                        .key()
                        .labels()
                        .any(|l| l.key() == "vendor" && l.value() == vendor)
                {
                    continue;
                }
                if let DebugValue::Gauge(g) = value {
                    return Some(g.into_inner());
                }
            }
            None
        };

        // Built inside the recorder so the boot-time publish is captured, and
        // on a private vendor label so no other test's lanes land in the
        // series being read.
        let (server, provider, transport) = lane_pool(&proxy_mock, |v| {
            v.name = VENDOR.to_string();
            v.min_lanes = 0;
        });
        assert_eq!(
            lanes(VENDOR),
            Some(0.0),
            "an idle pool with min_lanes 0 holds nothing before any traffic"
        );

        let addr = rt.block_on(serve(Pipeline::new(
            Arc::new(OpenAiProvider::new(transport, provider).unwrap()),
            Arc::new(server),
            None,
        )));

        // Through `routes::ready` -> `Provider::ready()` -> `is_ready()` on a
        // real socket: the path a load balancer actually takes. Calling
        // `is_ready()` directly would leave the render seam unproven.
        // Every socket leg is bounded. This test multiplexes the mock's accept
        // task, the axum server task, and the reqwest client on ONE thread —
        // a failure surface the rest of the suite does not have, since each
        // `#[tokio::test]` gets its own scheduler. Unbounded, a stall here
        // would hang with no diagnostic and take the whole CI job with it;
        // bounded, it is a named assertion failure.
        for i in 0..25 {
            let resp = rt
                .block_on(async {
                    // Built INSIDE the async block, not outside it:
                    // `tokio::time::timeout` reads the timer at construction,
                    // so building it on this thread but outside `block_on`
                    // panics with "no reactor running". The `#[test]` has no
                    // ambient runtime of its own to fall back on.
                    tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        client().get(format!("http://{addr}/ready")).send(),
                    )
                    .await
                })
                .unwrap_or_else(|_| panic!("probe {i}: /ready did not answer in 10s"))
                .unwrap();
            assert_eq!(resp.status(), 200, "probe {i}: a pool with room is ready");
        }

        // THE falsifiable claim. 25 readiness probes minted NOTHING.
        assert_eq!(
            lanes(VENDOR),
            Some(0.0),
            "a readiness probe must not mint a lane it would then pay rotations \
             and probes for"
        );
        // The half no gauge can show: readiness never reaches the vendor.
        assert_eq!(
            state.accepts.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a readiness probe must not open a connection to the vendor"
        );
        assert_eq!(
            state.requests.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "readiness is answered from configuration, not from a round trip"
        );

        // The control, and the reason the gauge above is not vacuous: the pool
        // really was willing and able to mint, and one real request does
        // exactly that. Without this leg a zero reading would also be what a
        // merely broken pool produced.
        let served = rt
            .block_on(async {
                // Inside the async block for the same reason as the probe
                // loop: the timer needs a reactor at construction, and a
                // bare `#[test]` has no ambient one.
                tokio::time::timeout(std::time::Duration::from_secs(10), ask(addr)).await
            })
            .expect("control request did not complete in 10s");
        let _ = rt.block_on(served.text());
        assert_eq!(
            lanes(VENDOR),
            Some(1.0),
            "the first real request mints the lane the 25 probes refused to"
        );
        assert_eq!(
            state.accepts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "and that minted lane is a real connection to the vendor"
        );
    });
}

/// (f) Two vendors, two session SPELLINGS, two exit identities.
///
/// The pool round-robins vendors, and each vendor's template decides where its
/// session id goes. If a lane's credential were ever rendered from the wrong
/// vendor's template, both vendors would answer with the same sticky session
/// and the process would silently lose the exit-IP diversity the rotation
/// exists to buy. The mock sees both vendors' usernames, so the spellings are
/// compared where they actually matter.
#[tokio::test]
async fn two_vendors_are_seen_on_the_wire_in_their_own_spellings() {
    let (proxy_mock, state) = mock_upstream(vec![Reply::KeepAlive(BUFFERED)]).await;
    let (server, provider) = cfg_for("http://192.0.2.1:9");
    let vendor = |name: &str, url: &str| opencode2api_transport::ProxyVendorConfig {
        name: name.into(),
        url: url.into(),
        min_lanes: 1,
        lanes: 1,
        session_ttl_secs: 3600,
        session_len: 7,
        prewarm: Some(false),
        ..Default::default()
    };
    let config = opencode2api_transport::ProxyConfig {
        vendors: vec![
            vendor(
                "alpha",
                &format!("http://a-{{session}}:secret@{}", proxy_mock.0),
            ),
            vendor(
                "beta",
                &format!("http://b-{{session}}-net-eco:secret@{}", proxy_mock.0),
            ),
        ],
        prewarm: false,
        ..Default::default()
    };
    let transport =
        opencode2api_transport::Transport::with_proxy(&server, &config).expect("pool builds");
    let addr = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(transport, provider).unwrap()),
        Arc::new(server),
        None,
    ))
    .await;

    for _ in 0..6 {
        let _ = ask(addr).await.text().await;
    }

    let seen = wire_requests(&state);
    let alpha: Vec<String> = seen
        .iter()
        .map(|(_, head)| wire_session(head))
        .filter(|u| u.starts_with('a'))
        .collect();
    let beta: Vec<String> = seen
        .iter()
        .map(|(_, head)| wire_session(head))
        .filter(|u| u.starts_with('b'))
        .collect();
    assert!(
        !alpha.is_empty() && !beta.is_empty(),
        "both vendors served traffic"
    );
    assert_eq!(
        alpha
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        1,
        "one vendor holds one session across every request: {alpha:?}"
    );
    assert_eq!(
        beta.iter().collect::<std::collections::BTreeSet<_>>().len(),
        1,
        "one vendor holds one session across every request: {beta:?}"
    );
    // Each vendor's template is stripped to the bare id, so a template that
    // stops rendering fails HERE with the offending value in the message
    // rather than as a bare unwrap panic naming neither the vendor nor the
    // string.
    let (a, b) = (alpha[0].clone(), beta[0].clone());
    let bare = |user: &str, prefix: &str, suffix: &str| -> String {
        user.strip_prefix(prefix)
            .and_then(|r| r.strip_suffix(suffix))
            .unwrap_or_else(|| panic!("template did not render for {user:?}"))
            .to_owned()
    };
    let a = bare(&a, "a-", "");
    let b = bare(&b, "b-", "-net-eco");
    assert_eq!(a.len(), 7, "alpha's own 7-digit id: {a:?}");
    assert!(
        a.bytes().all(|c| c.is_ascii_digit()),
        "alpha's template must render digits: {a:?}"
    );
    assert_eq!(
        b.len(),
        7,
        "beta's own 7-digit id, with its own trailing spelling intact: {b:?}"
    );
    assert!(
        b.bytes().all(|c| c.is_ascii_digit()),
        "beta's template must render digits: {b:?}"
    );
    assert_ne!(
        a, b,
        "two vendors must be two exit identities, not one shared session"
    );
}

/// (g) The egress family's `/metrics` exposition, rendered for real.
///
/// The counters here are published from inside the pool, and a family that
/// records without being DESCRIBED is exactly the failure this guards: series
/// that reach the exposition with no `# HELP`, so nobody can tell what they
/// mean. Driving it through the real `install_metrics` recorder and the real
/// `/metrics` route is the only way to see both halves at once.
#[tokio::test]
async fn the_egress_metric_family_is_exposed_with_help() {
    // A vendor label no other test uses. The recorder is process-global and
    // one-shot — `install_metrics` succeeds exactly once per binary — so every
    // other test in this file writes into it too, on whatever schedule. An
    // absolute count on a shared label is a coin flip, not an assertion; a
    // private label is the only way to pin a number here.
    const VENDOR: &str = "w10f-metrics";
    // Install BEFORE anything publishes: `with_proxy` writes the lane gauges
    // at construction, so a recorder installed afterwards renders a family
    // whose data went to the void while its HELP text still shows up. This is
    // the order the composition root uses (service/src/main.rs).
    let handle = opencode2api_kit::telemetry::install_metrics().expect("recorder installs");
    opencode2api_transport::describe_metrics();
    let (proxy_mock, _state) = mock_upstream(vec![Reply::KeepAlive(BUFFERED)]).await;
    let (server, provider, transport) = lane_pool(&proxy_mock, |v| v.name = VENDOR.to_string());
    let lane = transport.lane().expect("lane");
    let addr = serve(Pipeline::new(
        Arc::new(OpenAiProvider::new(transport, provider).unwrap()),
        Arc::new(server),
        Some(handle.clone()),
    ))
    .await;

    let read = || handle.render();
    let value = |text: &str, series: &str| -> Option<u64> {
        text.lines()
            .find_map(|l| l.strip_prefix(series)?.trim().parse::<u64>().ok())
    };
    let series = format!(r#"opencode2api_proxy_lane_failures_total{{vendor="{VENDOR}"}} "#);

    // Two failures this test causes and no other: a DELTA, so the assertion
    // survives both a recorder other tests have written into and any change to
    // what this test itself records. Presence of the line alone would pass on
    // another test's failures; an absolute count would flake on them.
    lane.note_failure();
    let before = value(&read(), &series).unwrap_or(0);
    lane.note_failure();
    let after = value(&read(), &series).unwrap_or(0);
    assert_eq!(
        after - before,
        1,
        "each transport failure adds exactly one to the egress counter, and no \
         other test contributes to this vendor's series"
    );

    let _ = ask(addr).await.text().await;
    let text = client()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let egress: Vec<&str> = text
        .lines()
        .filter(|l| l.contains("opencode2api_proxy"))
        .collect();
    // A family that RECORDS but is never DESCRIBED renders its data lines with
    // no help text at all, and a data-only assertion would stay green on it.
    for name in [
        opencode2api_transport::names::FAILURES,
        opencode2api_transport::names::HEALTHY,
        opencode2api_transport::names::LANES,
    ] {
        assert!(
            text.contains(&format!("# HELP {name} ")),
            "{name} reached /metrics without help text:\n{}",
            egress.join("\n")
        );
    }
    // Exposition renders labels in the order the source spells them, not
    // sorted. Match the name and the label values rather than one concatenated
    // literal, so a label-order change this test does not own cannot fail it.
    assert!(
        text.lines()
            .any(|l| l.starts_with("opencode2api_proxy_lanes{")
                && l.contains(&format!("vendor=\"{VENDOR}\""))
                && l.ends_with(" 1")),
        "lane counts are exposed per vendor:\n{}",
        egress.join("\n")
    );
    assert!(
        text.lines()
            .any(|l| l.starts_with("opencode2api_proxy_lanes_healthy{")
                && l.contains(&format!("vendor=\"{VENDOR}\""))
                && l.contains("state=\"healthy\"")
                && l.ends_with(" 1")),
        "lane health is exposed per vendor and state:\n{}",
        egress.join("\n")
    );
}
