//! The wire types shared between providers and the server pipeline.
//!
//! A leaf module — it imports only external crates and `util` — so `provider`,
//! `errors`, and every service crate can depend on it without forming a cycle.
//! Note `http` is used directly for header/status types: this crate must never
//! name axum (that is `x2api-server`'s job).

use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, TryStreamExt, stream::BoxStream};
use http::HeaderMap;

use crate::util::error_chain;

/// Bytes still arriving from upstream. Transport errors are flattened to
/// strings at the edge: `error_chain` is what makes a reqwest error readable,
/// and the flattening lets the server relay loop be driven by a hand-built
/// stream in tests without a real transport.
pub type ChunkStream = BoxStream<'static, Result<Bytes, String>>;

/// A variant-neutral upstream response. Providers wrap their transport in
/// it so the typed handoff (`Completion`/`ChatChunk`/verbatim `ChunkStream`)
/// is all the server's retry loop ever sees — the vendor's HTTP details
/// never escape the provider crate.
pub struct UpstreamResponse(pub reqwest::Response);

impl UpstreamResponse {
    pub fn status(&self) -> http::StatusCode {
        self.0.status()
    }

    pub fn headers(&self) -> &HeaderMap {
        self.0.headers()
    }

    /// Content type as `str`, if present and valid — used for the
    /// `text/event-stream` fork.
    pub fn content_type(&self) -> Option<&str> {
        self.0
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
    }

    pub fn is_sse(&self) -> bool {
        self.content_type()
            .is_some_and(|v| v.starts_with("text/event-stream"))
    }
    /// Read the whole body. UNBOUNDED — prefer `body_bytes_capped` for
    /// anything whose size the upstream chooses.
    pub async fn body_bytes(self) -> Result<Bytes, String> {
        self.0.bytes().await.map_err(|e| error_chain(&e))
    }

    /// Read the body up to `max` bytes, failing the moment the running
    /// total crosses the ceiling. The inbound `max_body_bytes` guards the
    /// proxy against its OWN clients; this is the response-side twin — a
    /// broken or hostile upstream must not make us allocate unbounded.
    /// Accumulating chunk-by-chunk is the point: a 4 GiB reply costs at
    /// most ceiling + one chunk. A `bytes()`-then-check-length reading
    /// looks equivalent and is not — it allocates the whole body first.
    /// Streams need no size bound: their ceiling is the request deadline
    /// and the client's drain rate.
    pub async fn body_bytes_capped(self, max: usize) -> Result<Bytes, crate::ProviderError> {
        let resp = self.0;
        let mut stream = resp.bytes_stream();
        let mut buf = BytesMut::new();
        while let Some(chunk) = stream
            .try_next()
            .await
            .map_err(crate::ProviderError::from)?
        {
            if buf.len() + chunk.len() > max {
                return Err(crate::ProviderError::permanent_bad_gateway(format!(
                    "upstream reply exceeds the {max}-byte response ceiling"
                )));
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(buf.freeze())
    }

    /// The SSE body as a chunk stream, transport errors flattened to strings.
    pub fn into_chunks(self) -> ChunkStream {
        self.0
            .bytes_stream()
            .map_err(|e| error_chain(&e))
            .fuse()
            .boxed()
    }

    /// Consume the envelope and return the inner reqwest response (providers
    /// that need `bytes_stream()` with their own error mapping).
    pub fn into_response(self) -> reqwest::Response {
        self.0
    }
}
