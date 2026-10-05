//! Upstream HTTP client profile — one warm `reqwest::Client` per egress lane
//! or direct client-pool shard, built from `ServerConfig`.
//!
//! Egress connection policy belongs here because providers must obtain every
//! upstream client through `Transport`; that preserves lane selection, proxy
//! pinning, health, and sharding as one ownership boundary.
//!
//! Pool rules:
//! - `tcp_nodelay(true)`: axum's listener side needs it too (see server
//!   `serve`), and so does upstream — Nagle batches tiny SSE frames one RTT
//!   at a time off-box.
//! - `pool_idle_timeout(90s)`: under the LB idle-timeout of every plausible
//!   upstream so we stop reusing connections the peer has half-closed.
//! - `http2_adaptive_window(true)`: avoids WINDOW_UPDATE churn on large
//!   streamed bodies.
//! - `http2_keep_alive_{interval,timeout}`: h2 pings notice a half-dead
//!   pooled connection before a request lands on it.
//! - NO total timeout: it would cut long streams. Bound per-chunk idleness
//!   with `read_timeout` and wall-clock at the server layer instead.
//! - HTTP/2 is preferred wherever the peer will agree to it: over TLS that
//!   is automatic (rustls offers `h2` first in ALPN and reqwest takes it),
//!   and one h2 connection carries every concurrent stream — the handshake
//!   is paid once per lane instead of once per request. Plaintext h2c cannot
//!   be negotiated, only assumed, so it is the opt-in
//!   `server.http2_prior_knowledge` for upstreams known to speak it; turning
//!   it on against an HTTP/1.1 server breaks every request.

use std::time::Duration;

use x2api_kit::ServerConfig;

pub(super) fn build_client(cfg: &ServerConfig) -> reqwest::Client {
    build_client_with(cfg, |b| b)
}

/// The same profile with one deviation applied — the egress pool uses it to
/// pin a client to a proxy. Everything else about the connection stays
/// identical, so a proxied lane and a direct client differ in route only.
pub(super) fn build_client_with(
    cfg: &ServerConfig,
    tune: impl FnOnce(reqwest::ClientBuilder) -> reqwest::ClientBuilder,
) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(cfg.connect_timeout_secs))
        .read_timeout(Duration::from_secs(cfg.upstream_read_timeout_secs))
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_nodelay(true)
        .tcp_keepalive(Duration::from_secs(60))
        .http2_adaptive_window(true)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        // Room for every shard's connection to stay warm between requests.
        .pool_max_idle_per_host(cfg.upstream_shards.max(1) * 2);
    if cfg.http2_prior_knowledge {
        builder = builder.http2_prior_knowledge();
    }
    tune(builder)
        .build()
        .expect("reqwest client with these static settings cannot fail to build")
}
