//! Lifecycle tests for `x2api_server::serve` — the one part of the server
//! `oneshot` cannot reach: what happens to an in-flight SSE stream when
//! shutdown fires. These pin the two-stage drain (graceful first, force at
//! the cap): axum's graceful shutdown alone waits unbounded on streams, so
//! the cap is what bounds it.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::net::TcpListener;
use tokio::sync::watch;
use x2api_kit::{
    CallContext, ChatChunk, ChatRequest, ChatStream, Completion, Provider, ProviderError,
    ServerConfig,
};
use x2api_server::{Pipeline, serve};

/// Streams two chunks with a pause between, so shutdown can be raised while
/// the stream is genuinely mid-frames.
struct SlowStream;

#[async_trait::async_trait]
impl Provider for SlowStream {
    fn name(&self) -> &str {
        "slow"
    }
    async fn complete(
        &self,
        _r: &ChatRequest,
        _c: &CallContext<'_>,
    ) -> Result<Completion, ProviderError> {
        Err(ProviderError::unsupported("complete"))
    }
    async fn stream(
        &self,
        _r: &ChatRequest,
        _c: &CallContext<'_>,
    ) -> Result<ChatStream, ProviderError> {
        Ok(async_stream::stream! {
            for text in ["first ", "second"] {
                tokio::time::sleep(Duration::from_millis(40)).await;
                yield Ok(ChatChunk { refusal: String::new(),
                    id: "s1".into(),
                    model: "m".into(),
                    text: text.into(),
                    finish_reason: None,
                    usage: None,
                    tool_calls: Vec::new(),
                    raw: None,
                    reasoning: String::new(),
                });
            }
            yield Ok(ChatChunk { refusal: String::new(),
                id: "s1".into(),
                model: "m".into(),
                text: String::new(),
                finish_reason: Some("stop".into()),
                usage: None,
                tool_calls: Vec::new(),
                raw: None,
                reasoning: String::new(),
            });
        }
        .boxed())
    }
}

fn pipeline() -> Pipeline {
    let cfg = ServerConfig {
        sse_keepalive_secs: 0,
        stream_deadline_secs: 30,
        max_inflight: 4,
        ..Default::default()
    };
    Pipeline::new(Arc::new(SlowStream), Arc::new(cfg), None)
}

async fn start(
    drain_secs: u64,
) -> (
    std::net::SocketAddr,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, _rx) = watch::channel(false);
    let serve_tx = tx.clone();
    let handle = tokio::spawn(async move {
        let router = x2api_server::build_router(pipeline());
        serve(listener, router, drain_secs, serve_tx).await.unwrap();
    });
    (addr, tx, handle)
}

fn stream_body() -> serde_json::Value {
    serde_json::json!({"model": "m", "messages": [{"role": "user", "content": "x"}], "stream": true})
}

#[tokio::test]
async fn in_flight_stream_survives_graceful_shutdown() {
    let (addr, tx, server) = start(30).await;
    let client = reqwest::Client::new();
    let mut resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .json(&stream_body())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Read the first frame, THEN signal shutdown: the stream is mid-flight.
    let mut body = String::new();
    let chunk = resp.chunk().await.unwrap().unwrap();
    body.push_str(&String::from_utf8_lossy(&chunk));
    assert!(body.contains("first"), "got {body:?}");

    let _ = tx.send(true);

    // Drain the rest: the graceful phase must let it finish cleanly. A
    // client-side read error is DISTINCT from a clean end and is exactly
    // what a regressed drain would cause — surface it, don't exit silently.
    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => body.push_str(&String::from_utf8_lossy(&chunk)),
            Ok(None) => break,
            Err(e) => panic!("client read failed mid-drain (server cut early?): {e}"),
        }
    }
    assert!(
        body.ends_with("data: [DONE]\n\n"),
        "in-flight stream must complete through graceful drain: {body:?}"
    );
    assert!(body.contains("second"));
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("serve returns after drain")
        .unwrap();
}

#[tokio::test]
async fn zero_drain_caps_and_does_not_hang() {
    let (addr, tx, server) = start(0).await;
    let client = reqwest::Client::new();
    let mut resp = client
        .post(format!("http://{addr}/v1/chat/completions"))
        .json(&stream_body())
        .send()
        .await
        .unwrap();
    let chunk = resp.chunk().await.unwrap().unwrap();
    assert!(!chunk.is_empty(), "stream started");

    let _ = tx.send(true);
    // drain_secs = 0: the force arm fires immediately. The invariant tested
    // is NOT the client's fate (reset or short body — either is honest) but
    // that serve() terminates promptly instead of waiting on the open stream.
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("force-drop path must not hang")
        .unwrap();
}
