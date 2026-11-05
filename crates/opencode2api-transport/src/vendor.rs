//! The vendor leg: TLS identity + HTTP/1.1 writer.
//!
//! Why this exists at all: the upstream answers a byte-identical request with
//! 200 when it arrives over Bun's BoringSSL and 429 when it arrives over
//! OpenSSL or rustls, so the handshake is the credential. `reqwest` cannot
//! carry it — its TLS backend here is rustls, and its HTTP/1.1 encoder neither
//! preserves header case nor emits the client's header order.
//!
//! What is copied, and what must stay copied:
//! - the TLS library is BoringSSL at the commit Bun pins, with the genuine
//!   client's settings: cipher order, groups including the ML-KEM hybrid,
//!   extension order, and Bun's own verification-signature-algorithm list
//!   ([`BUN_SIGALGS`] — BoringSSL's STOCK default is not what Bun sends, and
//!   the difference lives in extension 13's body, which JA3 cannot see).
//!   Nothing here sets a cipher list or a curve list, and GREASE stays at
//!   BoringSSL's default (off): every one of those moves the hello away from
//!   the genuine client, which is the invariant — not "stock defaults".
//! - per-connection decoration is what Bun itself adds: SNI, ALPN `http/1.1`
//!   only (no h2), OCSP stapling and SCT requests.
//! - the request head is written here, not by hyper: the caller's headers are
//!   emitted byte-sorted with their case preserved, then the client's default
//!   block (`Connection`, `Accept`, `Host`, `Accept-Encoding`,
//!   `Content-Length`). `b3`/`traceparent` are the genuine client's tracing
//!   headers and the PROVIDER mints them; this writer only places them where
//!   byte order puts them.
//!
//! What is NOT copied: TLS session resumption, and that matches the genuine
//! client rather than diverging from it — two consecutive Bun-fetch
//! connections captured 2026-11-05 carried no `pre_shared_key` extension
//! (usockets enables the client cache but leaves storage to a callback the
//! fetch path never feeds). This leg never resumes, so a NEW connection pays
//! the full handshake. Neither is the in-flight concurrency ceiling the
//! vendor applies per sandbox, which is unmeasured — `provider.max_inflight`
//! is the operator's ceiling, not a measurement of the vendor's.
//!
//! The tests at the bottom are the gate. JA3 covers the cipher list and the
//! extension IDS; `client_hello_matches_the_genuine_clients_full_shape`
//! covers what JA3 cannot see (extension order, body lengths, signature
//! algorithms, groups, ALPN, supported versions) — the axis on which this
//! module DID drift once.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_compression::tokio::bufread::{BrotliDecoder, DeflateDecoder, GzipDecoder, ZstdDecoder};
use boring::ssl::{
    SslConnector, SslConnectorBuilder, SslMethod, SslSignatureAlgorithm, SslVersion,
};
use boring::x509::{X509, store::X509StoreBuilder};
use bytes::{Bytes, BytesMut};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use opencode2api_kit::config::ServerConfig;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::{Lane, SendFault};

/// Where a lane's TCP connection is opened: the proxy's endpoint and the
/// credential for its CONNECT tunnel.
///
/// The composed proxy URL is never stored (it is a password), and this type
/// exists so the vendor dialer can reach the same exit IP the lane's warm
/// `reqwest` clients do. It is `Debug`-silent on purpose: a lane identifies
/// itself by vendor and session id only.
#[derive(Clone)]
pub(crate) struct ProxyTarget {
    host: String,
    port: u16,
    authorization: Option<HeaderValue>,
}

impl ProxyTarget {
    /// Parse a lane URL into the CONNECT endpoint. Only `http://` proxies are
    /// supported: a `CONNECT` to an `https://` proxy would need a TLS layer
    /// under the one this module is here to build.
    pub(crate) fn parse(url: &str) -> anyhow::Result<Self> {
        let parsed = reqwest::Url::parse(url).map_err(|e| {
            anyhow::anyhow!(
                "proxy url is not a usable proxy ({e}); percent-encode any @ : / in the credentials"
            )
        })?;
        anyhow::ensure!(
            parsed.scheme() == "http",
            "proxy scheme {:?} is unsupported by the vendor leg (an https proxy needs its own TLS layer)",
            parsed.scheme()
        );
        let host = parsed
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("proxy url has no host"))?
            .to_string();
        let port = parsed.port_or_known_default().unwrap_or(80);
        let authorization = match (parsed.username(), parsed.password()) {
            ("", _) => None,
            (user, password) => {
                let raw = format!("{user}:{}", password.unwrap_or(""));
                HeaderValue::from_str(&format!("Basic {}", base64(raw.as_bytes())))
                    .map_err(|_| anyhow::anyhow!("proxy credentials are not a legal header value"))
                    .map(Some)?
            }
        };
        Ok(Self {
            host,
            port,
            authorization,
        })
    }
}

/// Standard base64 with padding — what a `Proxy-Authorization: Basic` value
/// needs. Kept local rather than pulling a crate for sixteen lines.
fn base64(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        out.push(ALPHABET[(b[0] >> 2) as usize] as char);
        out.push(ALPHABET[(((b[0] & 0x03) << 4) | (b[1] >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(((b[1] & 0x0f) << 2) | (b[2] >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(b[2] & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// What the leg asks the origin to send.
///
/// The genuine client sends `gzip, deflate, br, zstd`, and the edge honours it
/// (measured: every JSON reply arrives `content-encoding: br`). That costs
/// CONNECTION REUSE: a codec stops at its own footer, the HTTP framing
/// terminal is left unread, and the socket cannot be handed to the next
/// request — so the leg paid a fresh TCP+TLS handshake per request. Measured
/// on `/inference/v1/models` (2026-10-25): ~760 ms per request with the four
/// codings (no pooling) vs ~325 ms with `identity` (pooled from request two),
/// both 200/200. Identity also removes the 0.4 s TLS + 0.2 s TCP per call.
///
/// Gate-neutrality of this header is MEASURED, not assumed: the extreme matrix
/// never varied `Accept-Encoding`, so an A/B was run on a fresh sandbox IP
/// (Bun 1.4.3 TLS, identical minimal body, only this header differing,
/// 2026-10-25): `identity` → 200 (01a171ac-ac0d…), the four codings → 200
/// (01a171ac-b937…), `identity` again → 200 (01a171ac-c596…). The decoders
/// stay: a server that ignores `identity` still gets handled.
const ACCEPT_ENCODING: &str = "identity";

/// A connection this module owns: a TCP socket, optionally inside a TLS
/// stream, optionally behind a proxy tunnel.
pub(crate) trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncReadWrite for T {}
pub(crate) type Conn = Box<dyn AsyncReadWrite>;

/// How long a parked connection may sit before it is retired unseen: under
/// every intermediary idle timeout this proxy knows of (Cloudflare closes an
/// idle origin connection well inside two minutes).
pub(crate) const IDLE_MAX: Duration = Duration::from_secs(90);

/// A connection parked between requests. Idle connections are dropped rather
/// than reused once they have been sitting long enough for an intermediary to
/// have closed them (Cloudflare's idle timeout is under two minutes).
pub(crate) struct IdleConn {
    pub(crate) conn: Conn,
    pub(crate) since: Instant,
}

/// The vendor-leg failure classes, named for the SAME lane accounting the
/// `reqwest` path uses: only a connect/tunnel failure is the lane's fault, a
/// silent post-connect peer is the vendor's.
pub struct VendorError {
    pub fault: SendFault,
    message: String,
}

impl VendorError {
    fn lane(message: impl Into<String>) -> Self {
        Self {
            fault: SendFault::Lane,
            message: message.into(),
        }
    }

    fn slow(message: impl Into<String>) -> Self {
        Self {
            fault: SendFault::Slow,
            message: message.into(),
        }
    }

    fn protocol(message: impl Into<String>) -> Self {
        Self {
            fault: SendFault::Other,
            message: message.into(),
        }
    }

    pub fn into_provider_error(self) -> opencode2api_kit::ProviderError {
        opencode2api_kit::ProviderError::transport(self.message)
    }
}

impl std::fmt::Debug for VendorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VendorError")
            .field("fault", &self.fault)
            .field("message", &self.message)
            .finish()
    }
}

impl std::fmt::Display for VendorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// One request as the wire should see it: caller headers in the order the
/// CALLER built them (the writer sorts), and a body that is always fully in
/// memory — every request this proxy sends is JSON the provider just built.
pub struct VendorRequest {
    method: &'static str,
    url: reqwest::Url,
    headers: Vec<(String, String)>,
    body: Bytes,
}

impl VendorRequest {
    pub fn post(url: reqwest::Url, headers: Vec<(String, String)>, body: Bytes) -> Self {
        Self {
            method: "POST",
            url,
            headers,
            body,
        }
    }

    pub fn get(url: reqwest::Url, headers: Vec<(String, String)>) -> Self {
        Self {
            method: "GET",
            url,
            headers,
            body: Bytes::new(),
        }
    }
}

/// The dialer. The TLS context inside is immutable once built and shared by
/// every handle; a handle differs only in its in-flight ceiling, which is why
/// the prewarmer and the provider can use the same connection profile under
/// different concurrency policies.
#[derive(Clone)]
pub struct VendorClient {
    inner: Arc<Inner>,
    /// Ceiling on requests in flight to the vendor. `None` = unmeasured, no
    /// cap; see `ServiceConfig::max_inflight`.
    cap: Option<Arc<Semaphore>>,
}

struct Inner {
    connector: SslConnector,
    connect_timeout: Duration,
    read_timeout: Duration,
}

/// Bun's verification-signature-algorithm list, verbatim from
/// `packages/bun-usockets/src/crypto/openssl.c`
/// (`us_ssl_ctx_set_verify_signature_algorithms`, bun-v1.4.4).
///
/// This is the ONE place the leg deliberately departs from a BoringSSL
/// default, and the departure is toward the genuine client, not away from it:
/// BoringSSL's `kVerifySignatureAlgorithms` has nine entries, while Bun sets
/// this eleven-entry list (adding ED25519 and ECDSA-SECP521R1-SHA512 before
/// the SHA-1 fallback). The difference lives in extension 13's BODY, which
/// JA3 does not hash — so the JA3 gate stayed green while the emitted hello
/// differed from opencode 2.2.2's (captured 2026-11-05). With this list the
/// two hellos are byte-identical once the per-connection randomness
/// (client_random, legacy session id, key-share key material) is set aside,
/// which is what the full-shape test below pins.
const BUN_SIGALGS: [SslSignatureAlgorithm; 11] = [
    SslSignatureAlgorithm::ECDSA_SECP256R1_SHA256,
    SslSignatureAlgorithm::RSA_PSS_RSAE_SHA256,
    SslSignatureAlgorithm::RSA_PKCS1_SHA256,
    SslSignatureAlgorithm::ECDSA_SECP384R1_SHA384,
    SslSignatureAlgorithm::RSA_PSS_RSAE_SHA384,
    SslSignatureAlgorithm::RSA_PKCS1_SHA384,
    SslSignatureAlgorithm::RSA_PSS_RSAE_SHA512,
    SslSignatureAlgorithm::RSA_PKCS1_SHA512,
    SslSignatureAlgorithm::ED25519,
    SslSignatureAlgorithm::ECDSA_SECP521R1_SHA512,
    SslSignatureAlgorithm::RSA_PKCS1_SHA1,
];

/// The client context every dial on this leg shares — and the SAME one the
/// ClientHello gate captures.
///
/// One constructor on purpose: the gate used to build its own copy of this
/// configuration, so a setting added to the dialer (or missing from it) could
/// not fail the test. The certificate store is the single thing the dialer
/// adds afterwards, because it is verification and cannot reach the hello.
fn client_connector() -> anyhow::Result<SslConnectorBuilder> {
    let mut builder = SslConnector::builder(SslMethod::tls())
        .map_err(|e| anyhow::anyhow!("BoringSSL client context: {e}"))?;
    builder
        .set_min_proto_version(Some(SslVersion::TLS1_2))
        .map_err(|e| anyhow::anyhow!("TLS 1.2 floor: {e}"))?;
    // h1 only: the genuine client's ALPN list is exactly `http/1.1`, and
    // the vendor answers it; offering h2 would change a field the origin
    // can read.
    builder
        .set_alpn_protos(b"\x08http/1.1")
        .map_err(|e| anyhow::anyhow!("ALPN http/1.1: {e}"))?;
    builder.enable_ocsp_stapling();
    builder.enable_signed_cert_timestamps();
    builder
        .set_verify_algorithm_prefs(&BUN_SIGALGS)
        .map_err(|e| anyhow::anyhow!("Bun's signature algorithms: {e}"))?;
    // No `set_grease_enabled`: BoringSSL ships GREASE off and Bun never turns
    // it on, so enabling it here would MOVE away from the genuine hello.
    Ok(builder)
}

impl VendorClient {
    pub fn new(server: &ServerConfig, max_inflight: Option<u32>) -> anyhow::Result<Self> {
        let mut builder = client_connector()?;
        // BoringSSL's default verify paths are a build-time OPENSSLDIR, which
        // on macOS points at nothing usable. The Mozilla bundle is what the
        // rustls path effectively verifies against, so it is what this one
        // verifies against too.
        let mut roots = X509StoreBuilder::new().map_err(|e| anyhow::anyhow!("X509 store: {e}"))?;
        for anchor in webpki_root_certs::TLS_SERVER_ROOT_CERTS {
            let cert = X509::from_der(anchor.as_ref())
                .map_err(|e| anyhow::anyhow!("webpki root is not DER: {e}"))?;
            roots
                .add_cert(cert)
                .map_err(|e| anyhow::anyhow!("webpki root rejected: {e}"))?;
        }
        builder.set_cert_store(roots.build());
        Ok(Self {
            inner: Arc::new(Inner {
                connector: builder.build(),
                connect_timeout: Duration::from_secs(server.connect_timeout_secs),
                read_timeout: Duration::from_secs(server.upstream_read_timeout_secs),
            }),
            cap: max_inflight
                .filter(|n| *n > 0)
                .map(|n| Arc::new(Semaphore::new(n as usize))),
        })
    }

    /// The same TLS context under a different concurrency ceiling.
    pub fn with_inflight_cap(&self, max_inflight: Option<u32>) -> Self {
        Self {
            inner: self.inner.clone(),
            cap: max_inflight
                .filter(|n| *n > 0)
                .map(|n| Arc::new(Semaphore::new(n as usize))),
        }
    }

    /// Open one connection per shard and park it, so the lane's first real
    /// request finds the TCP + TLS handshake already paid. No HTTP request is
    /// sent: the handshake is the cost, and a prewarm that also spent an
    /// upstream call would bill quota for warming.
    pub async fn prewarm(&self, lane: &Arc<Lane>, url: &reqwest::Url) -> Result<(), VendorError> {
        let shards = lane.shards();
        for slot in 0..shards {
            let conn = self
                .dial(lane, url, slot)
                .await
                .map_err(|e| VendorError::lane(e.message))?;
            lane.put_vendor_conn(
                slot,
                IdleConn {
                    conn,
                    since: Instant::now(),
                },
            );
        }
        Ok(())
    }

    /// Send one request over the lane's egress and hand back a response whose
    /// body streams as it arrives. Lane accounting is the CALLER's: this
    /// method only classifies its own failures.
    pub async fn send(
        &self,
        lane: &Arc<Lane>,
        req: VendorRequest,
    ) -> Result<opencode2api_kit::UpstreamResponse, VendorError> {
        let permit = match &self.cap {
            Some(cap) => Some(
                cap.clone()
                    .acquire_owned()
                    .await
                    .expect("the vendor in-flight semaphore is never closed"),
            ),
            None => None,
        };
        // An http:// target through a proxy is a plain proxy hop, written in
        // absolute form; an https:// target is tunnelled and the request line
        // carries only the origin path.
        let proxy = lane.vendor_proxy();
        let absolute_form = proxy.is_some() && req.url.scheme() != "https";
        let proxy_auth = proxy
            .as_ref()
            .filter(|_| absolute_form)
            .and_then(|p| p.authorization.clone());
        let head = build_head(&req, absolute_form, proxy_auth.as_ref())?;
        let mut attempt = 0;
        loop {
            attempt += 1;
            // A pooled socket the peer has since closed (or written an
            // unexpected trailing byte on) is discarded BEFORE the request:
            // a stale connection is cheap to replace here and impossible to
            // recover from once a POST has been half-written onto it.
            let pooled = match lane.take_vendor_conn() {
                Some((slot, mut conn)) => {
                    if stale(&mut conn).await {
                        None
                    } else {
                        Some((slot, conn))
                    }
                }
                None => None,
            };
            let reused = pooled.is_some();
            let (slot, mut conn) = match pooled {
                Some((slot, conn)) => (slot, conn),
                None => {
                    let slot = lane.next_vendor_slot();
                    (
                        slot,
                        self.dial(lane, &req.url, slot)
                            .await
                            .map_err(|e| match attempt {
                                1 => e,
                                _ => VendorError::lane(e.message),
                            })?,
                    )
                }
            };
            if let Err(e) = conn.write_all(&head).await {
                if reused && attempt == 1 && matches!(req.method, "GET" | "HEAD") {
                    // A pooled connection the peer closed between the
                    // liveness check and this write. Only an idempotent
                    // request may ride a fresh socket: re-sending a POST
                    // whose bytes may already have been received would
                    // double-bill the vendor.
                    continue;
                }
                return Err(VendorError::lane(format!(
                    "vendor request write failed: {e}"
                )));
            }
            if !req.body.is_empty()
                && let Err(e) = conn.write_all(&req.body).await
            {
                return Err(VendorError::lane(format!(
                    "vendor request write failed: {e}"
                )));
            }
            if let Err(e) = conn.flush().await {
                return Err(VendorError::lane(format!(
                    "vendor request write failed: {e}"
                )));
            }
            let mut reader = Reader {
                conn,
                buf: BytesMut::with_capacity(16 * 1024),
            };
            let response = match reader.read_head(self.inner.read_timeout).await {
                Ok(head) => head,
                Err(e)
                    if reused
                        && attempt == 1
                        && matches!(req.method, "GET" | "HEAD")
                        && e.kind() == io::ErrorKind::UnexpectedEof =>
                {
                    continue;
                }
                Err(e) if e.kind() == io::ErrorKind::TimedOut => {
                    return Err(VendorError::slow(format!(
                        "vendor response header timed out: {e}"
                    )));
                }
                Err(e) => {
                    return Err(VendorError::lane(format!(
                        "vendor response header failed: {e}"
                    )));
                }
            };
            return self
                .finish(lane, slot, reader, response, permit, req.method)
                .await;
        }
    }

    /// Open TCP (+ proxy tunnel) and complete the TLS handshake.
    async fn dial(
        &self,
        lane: &Arc<Lane>,
        url: &reqwest::Url,
        _slot: usize,
    ) -> Result<Conn, VendorError> {
        let host = url
            .host_str()
            .ok_or_else(|| VendorError::lane("vendor endpoint has no host"))?
            .to_string();
        let port = url.port_or_known_default().unwrap_or(443);
        let proxy = lane.vendor_proxy();
        let target = proxy
            .as_ref()
            .map_or((host.clone(), port), |p| (p.host.clone(), p.port));
        let tcp = self.connect_tcp(&target.0, target.1).await?;
        let _ = tcp.set_nodelay(true);
        let tcp: Conn = match proxy {
            None => Box::new(tcp),
            Some(proxy) if url.scheme() == "https" => Box::new(
                self.tunnel(tcp, &proxy, &host, port)
                    .await
                    .map_err(|e| VendorError::lane(format!("proxy tunnel failed: {e}")))?,
            ),
            // Cleartext through a proxy stays in absolute form on this
            // connection: no tunnel to open.
            Some(_) => Box::new(tcp),
        };
        // Plaintext endpoints exist for local/mock upstreams (the socket
        // suite) and for a deployment behind a terminating sidecar. The
        // handshake is the IDENTITY only where the scheme asks for TLS.
        if url.scheme() != "https" {
            return Ok(tcp);
        }
        let config = self
            .inner
            .connector
            .configure()
            .map_err(|e| VendorError::lane(format!("TLS setup failed: {e}")))?;
        let tls = tokio::time::timeout(
            self.inner.connect_timeout,
            tokio_boring::connect(config, &host, tcp),
        )
        .await
        .map_err(|_| VendorError::lane("TLS handshake timed out"))?
        .map_err(|e| VendorError::lane(format!("TLS handshake failed: {e}")))?;
        Ok(Box::new(tls))
    }

    /// Connect to the first address that answers, racing the resolved set.
    ///
    /// `TcpStream::connect((host, port))` walks the resolver's answers
    /// SEQUENTIALLY, so a host whose AAAA records are unroutable (a v4-only
    /// box, a container without v6) spends the entire connect budget on the
    /// first dead address and never reaches the working A record. Racing every
    /// address under ONE deadline is what makes the budget mean "the peer did
    /// not answer" rather than "the first six answers did not".
    async fn connect_tcp(
        &self,
        host: &str,
        port: u16,
    ) -> Result<tokio::net::TcpStream, VendorError> {
        let addrs = tokio::net::lookup_host((host, port))
            .await
            .map_err(|e| VendorError::lane(format!("vendor dns lookup failed: {e}")))?
            .collect::<Vec<_>>();
        if addrs.is_empty() {
            return Err(VendorError::lane(format!(
                "vendor host {host} has no addresses"
            )));
        }
        let attempts = addrs
            .into_iter()
            .map(|addr| Box::pin(tokio::net::TcpStream::connect(addr)))
            .collect::<Vec<_>>();
        match tokio::time::timeout(
            self.inner.connect_timeout,
            futures_util::future::select_ok(attempts),
        )
        .await
        {
            Ok(Ok((tcp, _))) => Ok(tcp),
            Ok(Err(e)) => Err(VendorError::lane(format!("vendor connect failed: {e}"))),
            Err(_) => Err(VendorError::lane("vendor connect timed out")),
        }
    }

    /// `CONNECT host:port` through an HTTP proxy, leaving the stream at the
    /// tunnel's mouth. Bytes the proxy sent past its own head belong to the
    /// TLS handshake and are kept in front of the socket.
    async fn tunnel(
        &self,
        mut tcp: tokio::net::TcpStream,
        proxy: &ProxyTarget,
        host: &str,
        port: u16,
    ) -> io::Result<Prefixed<tokio::net::TcpStream>> {
        let authority = format!("{host}:{port}");
        let mut head = format!(
            "CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nProxy-Connection: keep-alive\r\n"
        );
        if let Some(auth) = &proxy.authorization {
            head.push_str("Proxy-Authorization: ");
            head.push_str(auth.to_str().unwrap_or(""));
            head.push_str("\r\n");
        }
        head.push_str("\r\n");
        tokio::time::timeout(self.inner.connect_timeout, tcp.write_all(head.as_bytes()))
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "proxy CONNECT write timed out")
            })??;
        let mut buf = BytesMut::with_capacity(4096);
        let mut chunk = [0u8; 1024];
        let end = loop {
            if let Some(at) = find_head_end(&buf) {
                break at;
            }
            if buf.len() > 16 * 1024 {
                return Err(io::Error::other("proxy CONNECT head too large"));
            }
            let n = tokio::time::timeout(self.inner.connect_timeout, tcp.read(&mut chunk))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "proxy CONNECT read timed out")
                })??;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "proxy closed before answering CONNECT",
                ));
            }
            buf.extend_from_slice(&chunk[..n]);
        };
        let head = buf.split_to(end + 4);
        let status = std::str::from_utf8(&head)
            .ok()
            .and_then(|h| h.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
            .unwrap_or(0);
        if !(200..300).contains(&status) {
            return Err(io::Error::other(format!(
                "proxy answered CONNECT with {status}"
            )));
        }
        Ok(Prefixed {
            prefix: buf,
            inner: tcp,
        })
    }

    fn finish<'a>(
        &'a self,
        lane: &Arc<Lane>,
        slot: usize,
        reader: Reader,
        response: wire::Head,
        permit: Option<OwnedSemaphorePermit>,
        method: &'a str,
    ) -> impl Future<Output = Result<opencode2api_kit::UpstreamResponse, VendorError>> + 'a {
        wire::respond(
            reader,
            response,
            PoolReturn {
                lane: lane.clone(),
                slot,
                idle_ok: true,
                permit,
            },
            self.inner.read_timeout,
            method,
        )
    }
}

/// A stream that yields a prefix buffered from the proxy's CONNECT answer
/// before continuing with the socket.
struct Prefixed<T> {
    prefix: BytesMut,
    inner: T,
}

impl<T: AsyncRead + Unpin> AsyncRead for Prefixed<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.prefix.is_empty() {
            let take = self.prefix.len().min(buf.remaining());
            buf.put_slice(&self.prefix.split_to(take));
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Prefixed<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Whether a parked connection is no longer safe to write on.
///
/// Polls the socket once with a zero deadline: "not ready" is the healthy
/// idle state, while readability means the peer closed it (EOF) or left bytes
/// that belong to the PREVIOUS response. Both are fatal to the next request,
/// which is why they are detected here rather than after a failed send.
async fn stale(conn: &mut Conn) -> bool {
    let mut probe = [0u8; 1];
    tokio::time::timeout(Duration::ZERO, conn.read(&mut probe))
        .await
        .is_ok()
}

fn default_port(scheme: &str) -> Option<u16> {
    match scheme {
        "https" => Some(443),
        "http" => Some(80),
        _ => None,
    }
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// A connection plus whatever bytes have already been read from it.
struct Reader {
    conn: Conn,
    buf: BytesMut,
}

impl Reader {
    /// Read more bytes from the connection. `Ok(0)` is EOF.
    async fn fill(&mut self, timeout: Duration) -> io::Result<usize> {
        let n = tokio::time::timeout(timeout, self.conn.read_buf(&mut self.buf))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "read timed out"))??;
        Ok(n)
    }

    /// Read one CRLF-terminated line (without the CRLF), bounded so a hostile
    /// peer cannot grow the buffer without limit.
    async fn read_line(&mut self, timeout: Duration) -> io::Result<Vec<u8>> {
        loop {
            if let Some(at) = self.buf.windows(2).position(|w| w == b"\r\n") {
                return Ok(self.buf.split_to(at + 2)[..at].to_vec());
            }
            if self.buf.len() > 16 * 1024 {
                return Err(io::Error::other("line exceeds 16 KiB"));
            }
            if self.fill(timeout).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed mid-line",
                ));
            }
        }
    }

    async fn read_head(&mut self, timeout: Duration) -> io::Result<wire::Head> {
        loop {
            if let Some(at) = find_head_end(&self.buf) {
                let head = self.buf.split_to(at + 4);
                let parsed = wire::Head::parse(&head[..at])?;
                // A 1xx is not the answer. RFC 9110 allows any number of
                // interim heads before the final one, and an edge that sends
                // `103 Early Hints` must not be read as a final 103 response
                // — the stream would be handed to the caller with no body.
                // 1xx carries no body, so the next head starts immediately.
                if parsed.status.is_informational() {
                    continue;
                }
                return Ok(parsed);
            }
            if self.buf.len() > 64 * 1024 {
                return Err(io::Error::other("response head exceeds 64 KiB"));
            }
            if self.fill(timeout).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed inside the response head",
                ));
            }
        }
    }
}

/// The request head, exactly as the client writes it: the caller's headers
/// byte-sorted with their case preserved, then the client's default block.
fn build_head(
    req: &VendorRequest,
    absolute_form: bool,
    proxy_auth: Option<&HeaderValue>,
) -> Result<Bytes, VendorError> {
    let host = req
        .url
        .host_str()
        .ok_or_else(|| VendorError::protocol("vendor endpoint has no host"))?;
    let authority = match req.url.port_or_known_default() {
        Some(port) if Some(port) != default_port(req.url.scheme()) => {
            format!("{host}:{port}")
        }
        _ => host.to_string(),
    };
    let mut path = String::new();
    if absolute_form {
        path.push_str(req.url.scheme());
        path.push_str("://");
        path.push_str(&authority);
    }
    path.push_str(req.url.path());
    if path.is_empty() || path.ends_with("://") {
        path.push('/');
    }
    if let Some(query) = req.url.query() {
        path.push('?');
        path.push_str(query);
    }
    let mut headers: Vec<&(String, String)> = req.headers.iter().collect();
    // Byte order, case-sensitive: `Authorization` before `b3` before
    // `x-opencode-*`, which is what the genuine client's wire shows.
    headers.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut out = format!("{} {path} HTTP/1.1\r\n", req.method);
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("host")
            || name.eq_ignore_ascii_case("connection")
            || name.eq_ignore_ascii_case("accept-encoding")
            || name.eq_ignore_ascii_case("content-length")
        {
            return Err(VendorError::protocol(format!(
                "caller header {name:?} is written by the vendor leg itself"
            )));
        }
        if value.contains(['\r', '\n']) {
            return Err(VendorError::protocol(format!(
                "header {name:?} contains a line break"
            )));
        }
        out.push_str(name);
        out.push_str(": ");
        out.push_str(value);
        out.push_str("\r\n");
    }
    if let Some(auth) = proxy_auth {
        out.push_str("Proxy-Authorization: ");
        out.push_str(
            auth.to_str().map_err(|_| {
                VendorError::protocol("proxy credential is not a legal header value")
            })?,
        );
        out.push_str("\r\n");
    }
    out.push_str("Connection: keep-alive\r\n");
    out.push_str("Accept: */*\r\n");
    out.push_str(&format!("Host: {authority}\r\n"));
    out.push_str(&format!("Accept-Encoding: {ACCEPT_ENCODING}\r\n"));
    if !req.body.is_empty() {
        out.push_str(&format!("Content-Length: {}\r\n", req.body.len()));
    }
    out.push_str("\r\n");
    Ok(Bytes::from(out))
}

/// Where a finished body's connection goes back to.
struct PoolReturn {
    lane: Arc<Lane>,
    /// The shard slot this connection came from, and where it goes back.
    slot: usize,
    idle_ok: bool,
    permit: Option<OwnedSemaphorePermit>,
}

mod wire {
    use super::*;

    /// A parsed response head.
    pub(super) struct Head {
        pub(super) status: StatusCode,
        pub(super) headers: HeaderMap,
        framing: Framing,
        keep_alive: bool,
        encoding: Encoding,
    }

    enum Framing {
        Empty,
        Length(u64),
        Chunked,
        Close,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Encoding {
        Identity,
        Gzip,
        Deflate,
        Brotli,
        Zstd,
    }

    impl Head {
        pub(super) fn parse(head: &[u8]) -> io::Result<Self> {
            let text = std::str::from_utf8(head)
                .map_err(|_| io::Error::other("response head is not UTF-8"))?;
            let mut lines = text.split("\r\n");
            let status_line = lines
                .next()
                .ok_or_else(|| io::Error::other("empty response head"))?;
            let mut parts = status_line.split_whitespace();
            let version = parts.next().unwrap_or("");
            if !version.starts_with("HTTP/") {
                return Err(io::Error::other("response head has no HTTP version"));
            }
            let status = parts
                .next()
                .and_then(|code| code.parse::<u16>().ok())
                .ok_or_else(|| io::Error::other("response head has no status code"))?;
            let status = StatusCode::from_u16(status)
                .map_err(|_| io::Error::other("response status is out of range"))?;
            let mut headers = HeaderMap::new();
            for line in lines {
                if line.is_empty() {
                    continue;
                }
                let (name, value) = line
                    .split_once(':')
                    .ok_or_else(|| io::Error::other("malformed response header"))?;
                let Ok(name) = HeaderName::from_bytes(name.trim().as_bytes()) else {
                    continue;
                };
                let Ok(value) = HeaderValue::from_str(value.trim()) else {
                    continue;
                };
                headers.append(name, value);
            }
            let connection_close = headers
                .get("connection")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.eq_ignore_ascii_case("close"));
            let http10 = version.eq_ignore_ascii_case("HTTP/1.0");
            let keep_alive = !connection_close && !http10;
            let transfer_chunked = headers
                .get("transfer-encoding")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"));
            let content_length = headers
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<u64>().ok());
            let framing = match (status.as_u16(), transfer_chunked, content_length) {
                (204 | 304, _, _) => Framing::Empty,
                (_, true, _) => Framing::Chunked,
                (_, false, Some(0)) => Framing::Empty,
                (_, false, Some(n)) => Framing::Length(n),
                (_, false, None) => Framing::Close,
            };
            let encoding = match headers
                .get("content-encoding")
                .and_then(|v| v.to_str().ok())
                .map(|v| v.trim().to_ascii_lowercase())
                .as_deref()
            {
                None | Some("") | Some("identity") => Encoding::Identity,
                Some("gzip") | Some("x-gzip") => Encoding::Gzip,
                Some("deflate") => Encoding::Deflate,
                Some("br") => Encoding::Brotli,
                Some("zstd") => Encoding::Zstd,
                Some(other) => {
                    return Err(io::Error::other(format!(
                        "unsupported response content-encoding {other:?}"
                    )));
                }
            };
            Ok(Self {
                status,
                headers,
                framing,
                keep_alive,
                encoding,
            })
        }
    }

    /// Build the response the rest of the proxy consumes: status + headers,
    /// and a body stream that decodes framing and content-coding as it goes.
    pub(super) async fn respond(
        reader: Reader,
        head: Head,
        pool: PoolReturn,
        read_timeout: Duration,
        method: &str,
    ) -> Result<opencode2api_kit::UpstreamResponse, VendorError> {
        let Head {
            status,
            headers,
            framing,
            keep_alive,
            encoding,
        } = head;
        // An encoded body is decoded by a codec that stops at ITS end marker,
        // not at the HTTP body's end: the framing terminator after the coded
        // bytes can be left unread, and handing that socket to the next
        // request would feed it the stale tail of this response. So only
        // identity bodies are pooled; a compressed response closes its
        // connection, which costs a handshake and never a corrupted request.
        let idle_ok =
            keep_alive && pool.idle_ok && method != "HEAD" && encoding == Encoding::Identity;
        let raw = body_stream(
            reader,
            framing,
            read_timeout,
            PoolReturn {
                lane: pool.lane.clone(),
                slot: pool.slot,
                idle_ok,
                permit: pool.permit,
            },
        );
        let decoded: futures_util::stream::BoxStream<'static, Result<Bytes, io::Error>> =
            match encoding {
                Encoding::Identity => Box::pin(raw),
                Encoding::Gzip => Box::pin(ReaderStream::new(GzipDecoder::new(BufReader::new(
                    StreamReader::new(raw),
                )))),
                Encoding::Deflate => Box::pin(ReaderStream::new(DeflateDecoder::new(
                    BufReader::new(StreamReader::new(raw)),
                ))),
                Encoding::Brotli => Box::pin(ReaderStream::new(BrotliDecoder::new(
                    BufReader::new(StreamReader::new(raw)),
                ))),
                Encoding::Zstd => Box::pin(ReaderStream::new(ZstdDecoder::new(BufReader::new(
                    StreamReader::new(raw),
                )))),
            };
        let mut builder = http::Response::builder().status(status);
        for (name, value) in &headers {
            // Framing and coding describe the bytes ON THE WIRE; this envelope
            // re-frames and decodes them, so forwarding those headers would
            // describe bytes the caller is not receiving.
            if matches!(
                name.as_str(),
                "content-length" | "transfer-encoding" | "content-encoding" | "connection"
            ) {
                continue;
            }
            builder = builder.header(name.clone(), value.clone());
        }
        let body = reqwest::Body::wrap_stream(decoded);
        let response = builder
            .body(body)
            .map_err(|e| VendorError::protocol(format!("response envelope: {e}")))?;
        Ok(opencode2api_kit::UpstreamResponse(reqwest::Response::from(
            response,
        )))
    }

    /// The wire body as a byte stream: length-delimited, chunked, or
    /// close-delimited. On clean EOF the connection goes back to its lane;
    /// on any error, or if the consumer stops early, it is dropped.
    fn body_stream(
        mut reader: Reader,
        framing: Framing,
        read_timeout: Duration,
        pool: PoolReturn,
    ) -> impl futures_util::Stream<Item = Result<Bytes, io::Error>> + Send + 'static {
        async_stream::try_stream! {
            let PoolReturn { lane, slot, idle_ok, permit } = pool;
            let mut done = false;
            match framing {
                Framing::Empty => done = true,
                Framing::Length(mut remaining) => {
                    while remaining > 0 {
                        if reader.buf.is_empty() && reader.fill(read_timeout).await? == 0 {
                            Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "upstream request failed: body ended before Content-Length was met",
                            ))?;
                        }
                        let take = (reader.buf.len() as u64).min(remaining) as usize;
                        remaining -= take as u64;
                        yield reader.buf.split_to(take).freeze();
                    }
                    done = true;
                }
                Framing::Chunked => {
                  loop {
                    let line = reader.read_line(read_timeout).await?;
                    let size_text = std::str::from_utf8(&line).unwrap_or("").split(';').next().unwrap_or("");
                    let size = u64::from_str_radix(size_text.trim(), 16).map_err(|_| {
                        io::Error::other("upstream request failed: malformed chunk size")
                    })?;
                    if size == 0 {
                        // Trailers: read until the empty line.
                        loop {
                            if reader.read_line(read_timeout).await?.is_empty() {
                                break;
                            }
                        }
                        break;
                    }
                    let mut remaining = size;
                    while remaining > 0 {
                        if reader.buf.is_empty() && reader.fill(read_timeout).await? == 0 {
                            Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "upstream request failed: chunked body truncated",
                            ))?;
                        }
                        let take = (reader.buf.len() as u64).min(remaining) as usize;
                        remaining -= take as u64;
                        yield reader.buf.split_to(take).freeze();
                    }
                    let crlf = reader.read_line(read_timeout).await?;
                    if !crlf.is_empty() {
                        Err(io::Error::other(
                            "upstream request failed: chunk not terminated by CRLF",
                        ))?;
                    }
                  }
                  done = true;
                }
                Framing::Close => loop {
                    if !reader.buf.is_empty() {
                        yield reader.buf.split().freeze();
                    }
                    if reader.fill(read_timeout).await? == 0 {
                        break;
                    }
                },
            }
            if done && idle_ok {
                let reader = reader;
                lane.put_vendor_conn(slot, IdleConn {
                    conn: reader.conn,
                    since: Instant::now(),
                });
            }
            drop(permit);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    /// The fingerprint the genuine client presents, measured off Bun 1.4.4's
    /// `fetch()` against `openssl s_server -msg` (SNI present, OCSP+SCT on,
    /// GREASE off, ALPN http/1.1).
    const GENUINE_JA3: &str = "1523504b38f0fae0d881d4b6554aac1b";
    const GENUINE_JA3_STRING: &str = "771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49161-49171-49162-49172-156-157-47-53,0-23-65281-10-11-35-16-5-13-18-51-45-43,4588-29-23-24,0";

    /// Parse one ClientHello out of a TLS record and render the Salesforce JA3
    /// inputs. Deliberately a hand parser rather than a dependency: the gate
    /// must fail for a reason a reader can see (a changed cipher, extension,
    /// or curve), and the string is printed on failure.
    fn ja3_string(buf: &[u8]) -> String {
        assert_eq!(buf[0], 22, "not a handshake record");
        assert_eq!(buf[5], 1, "not a ClientHello");
        let mut p = 9; // record header + handshake type/len
        let version = u16::from_be_bytes([buf[p], buf[p + 1]]);
        p += 2 + 32; // client version + random
        let sid = buf[p] as usize;
        p += 1 + sid;
        let ciphers_len = u16::from_be_bytes([buf[p], buf[p + 1]]) as usize;
        p += 2;
        let ciphers = (0..ciphers_len / 2)
            .map(|i| u16::from_be_bytes([buf[p + i * 2], buf[p + i * 2 + 1]]).to_string())
            .collect::<Vec<_>>()
            .join("-");
        p += ciphers_len;
        let comp_len = buf[p] as usize;
        p += 1 + comp_len;
        let ext_total = u16::from_be_bytes([buf[p], buf[p + 1]]) as usize;
        p += 2;
        let end = p + ext_total;
        let mut exts = Vec::new();
        let mut groups = Vec::new();
        let mut point_formats = Vec::new();
        while p + 4 <= end {
            let kind = u16::from_be_bytes([buf[p], buf[p + 1]]);
            let len = u16::from_be_bytes([buf[p + 2], buf[p + 3]]) as usize;
            let body = &buf[p + 4..p + 4 + len];
            exts.push(kind.to_string());
            match kind {
                10 => {
                    let n = u16::from_be_bytes([body[0], body[1]]) as usize;
                    groups = (0..n / 2)
                        .map(|i| u16::from_be_bytes([body[2 + i * 2], body[3 + i * 2]]).to_string())
                        .collect();
                }
                11 => {
                    point_formats = body[1..].iter().map(u8::to_string).collect();
                }
                _ => {}
            }
            p += 4 + len;
        }
        format!(
            "{version},{ciphers},{},{},{}",
            exts.join("-"),
            groups.join("-"),
            point_formats.join("-")
        )
    }

    fn md5_hex(s: &str) -> String {
        // MD5 is what JA3 is defined over; it is a fingerprint, not a security
        // claim, and the value is compared against a published constant.
        let digest = md5::compute(s.as_bytes());
        digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    }

    fn connector() -> SslConnector {
        // The DIALER's own constructor, not a copy: a gate that rebuilds the
        // configuration can certify something the dialer does not send, which
        // is exactly how the signature-algorithm list went missing.
        client_connector()
            .expect("BoringSSL client context")
            .build()
    }

    /// Capture exactly the bytes the mutual handshake puts on the wire,
    /// without completing (or trusting) a handshake: the peer reads the
    /// ClientHello and never answers. That is the same record
    /// `openssl s_server -msg` prints, with no external binary in the gate.
    async fn capture_client_hello() -> Vec<u8> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let capture = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.expect("accept");
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let n = tokio::time::timeout(Duration::from_secs(5), sock.read(&mut chunk))
                    .await
                    .expect("client wrote its hello")
                    .expect("read");
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                if buf.len() >= 5 && buf.len() >= 5 + u16::from_be_bytes([buf[3], buf[4]]) as usize
                {
                    break;
                }
            }
            buf
        });
        let tcp = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect loopback");
        let cfg = connector().configure().expect("connector config");
        let handshake = tokio::spawn(async move {
            // Expected to fail/hang: the peer never answers. The hello is
            // already written by the time the task is aborted.
            let _ = tokio_boring::connect(cfg, "opencode.ai", tcp).await;
        });
        let hello = capture.await.expect("capture task");
        handshake.abort();
        hello
    }

    #[tokio::test]
    async fn client_hello_is_the_genuine_clients_ja3() {
        let hello = capture_client_hello().await;
        let inputs = ja3_string(&hello);
        assert_eq!(
            inputs, GENUINE_JA3_STRING,
            "ClientHello shape drifted from the measured genuine client"
        );
        assert_eq!(
            md5_hex(&inputs),
            GENUINE_JA3,
            "JA3 drifted from the measured genuine client"
        );
    }

    /// The ClientHello fields a fingerprint can read, minus the values that
    /// are random on every connection: `client_random`, the legacy session id,
    /// and the key-share key material. Extension ORDER and each body's LENGTH
    /// stay, because those are exactly what JA3 throws away — the
    /// signature-algorithm BODY is the field this module drifted on while the
    /// JA3 gate stayed green.
    #[derive(Debug, PartialEq, Eq)]
    struct HelloShape {
        version: u16,
        session_id_len: usize,
        ciphers: Vec<u16>,
        compression: Vec<u8>,
        extensions: Vec<(u16, usize)>,
        groups: Vec<u16>,
        point_formats: Vec<u8>,
        sigalgs: Vec<u16>,
        alpn: Vec<u8>,
        supported_versions: Vec<u16>,
        key_share: Vec<(u16, usize)>,
        server_name: Option<String>,
    }

    fn hello_shape(buf: &[u8]) -> HelloShape {
        assert_eq!(buf[0], 22, "not a handshake record");
        assert_eq!(buf[5], 1, "not a ClientHello");
        let mut p = 9; // record header + handshake type/len
        let version = u16::from_be_bytes([buf[p], buf[p + 1]]);
        p += 2 + 32;
        let session_id_len = buf[p] as usize;
        p += 1 + session_id_len;
        let ciphers_len = u16::from_be_bytes([buf[p], buf[p + 1]]) as usize;
        p += 2;
        let ciphers = (0..ciphers_len / 2)
            .map(|i| u16::from_be_bytes([buf[p + i * 2], buf[p + i * 2 + 1]]))
            .collect();
        p += ciphers_len;
        let compression = buf[p + 1..p + 1 + buf[p] as usize].to_vec();
        p += 1 + buf[p] as usize;
        let ext_total = u16::from_be_bytes([buf[p], buf[p + 1]]) as usize;
        p += 2;
        let end = p + ext_total;
        let mut shape = HelloShape {
            version,
            session_id_len,
            ciphers,
            compression,
            extensions: Vec::new(),
            groups: Vec::new(),
            point_formats: Vec::new(),
            sigalgs: Vec::new(),
            alpn: Vec::new(),
            supported_versions: Vec::new(),
            key_share: Vec::new(),
            server_name: None,
        };
        while p + 4 <= end {
            let kind = u16::from_be_bytes([buf[p], buf[p + 1]]);
            let len = u16::from_be_bytes([buf[p + 2], buf[p + 3]]) as usize;
            let body = &buf[p + 4..p + 4 + len];
            shape.extensions.push((kind, len));
            match kind {
                0 => {
                    let name_len = u16::from_be_bytes([body[3], body[4]]) as usize;
                    shape.server_name = String::from_utf8(body[5..5 + name_len].to_vec()).ok();
                }
                10 => {
                    let n = u16::from_be_bytes([body[0], body[1]]) as usize;
                    shape.groups = (0..n / 2)
                        .map(|i| u16::from_be_bytes([body[2 + i * 2], body[3 + i * 2]]))
                        .collect();
                }
                11 => shape.point_formats = body[1..].to_vec(),
                13 => {
                    let n = u16::from_be_bytes([body[0], body[1]]) as usize;
                    shape.sigalgs = (0..n / 2)
                        .map(|i| u16::from_be_bytes([body[2 + i * 2], body[3 + i * 2]]))
                        .collect();
                }
                16 => shape.alpn = body.to_vec(),
                51 => {
                    // The two-byte prefix is the LIST's length in BYTES
                    // (RFC 8446 §4.2.8), not an entry count: at 1258 bytes
                    // with two entries, counting entries walks off the end.
                    let n = u16::from_be_bytes([body[0], body[1]]) as usize;
                    let end = 2 + n;
                    let mut q = 2;
                    while q + 4 <= end {
                        let group = u16::from_be_bytes([body[q], body[q + 1]]);
                        let key_len = u16::from_be_bytes([body[q + 2], body[q + 3]]) as usize;
                        shape.key_share.push((group, key_len));
                        q += 4 + key_len;
                    }
                }
                43 => {
                    let n = body[0] as usize;
                    shape.supported_versions = (0..n / 2)
                        .map(|i| u16::from_be_bytes([body[1 + i * 2], body[2 + i * 2]]))
                        .collect();
                }
                _ => {}
            }
            p += 4 + len;
        }
        shape
    }

    /// The shape captured from the genuine client (`opencode` 2.2.2, which
    /// embeds Bun 1.4.4) on 2026-11-05, dialling SNI `opencode.ai`. Every
    /// value that varies per connection is absent from this struct rather
    /// than masked, so nothing random can fail the comparison.
    ///
    /// The signature list is Bun's, not BoringSSL's: Bun's usockets client
    /// calls `SSL_CTX_set_verify_algorithm_prefs` with these eleven entries
    /// when no `sigalgs` option is given (`us_ssl_ctx_set_verify_signature_
    /// algorithms`), which is why [`BUN_SIGALGS`] exists.
    fn genuine_shape() -> HelloShape {
        HelloShape {
            version: 0x0303,
            session_id_len: 32,
            ciphers: vec![
                4865, 4866, 4867, 49195, 49199, 49196, 49200, 52393, 52392, 49161, 49171, 49162,
                49172, 156, 157, 47, 53,
            ],
            compression: vec![0],
            extensions: vec![
                (0, 16),
                (23, 0),
                (65281, 1),
                (10, 10),
                (11, 2),
                (35, 0),
                (16, 11),
                (5, 5),
                (13, 24),
                (18, 0),
                (51, 1258),
                (45, 2),
                (43, 5),
            ],
            groups: vec![4588, 29, 23, 24],
            point_formats: vec![0],
            sigalgs: vec![
                1027, 2052, 1025, 1283, 2053, 1281, 2054, 1537, 2055, 1539, 513,
            ],
            alpn: b"\x00\x09\x08http/1.1".to_vec(),
            supported_versions: vec![0x0304, 0x0303],
            key_share: vec![(4588, 1216), (29, 32)],
            server_name: Some("opencode.ai".to_string()),
        }
    }

    #[tokio::test]
    async fn client_hello_matches_the_genuine_clients_full_shape() {
        let hello = capture_client_hello().await;
        assert_eq!(
            hello_shape(&hello),
            genuine_shape(),
            "ClientHello drifted outside JA3's view (extension bodies, order, \
             or lengths); the signature-algorithm list is BUN_SIGALGS"
        );
    }

    fn test_lane() -> Arc<Lane> {
        let server = ServerConfig::default();
        crate::Transport::direct(&server)
            .lane()
            .expect("a direct transport always has a lane")
    }

    fn test_vendor(server: &ServerConfig) -> VendorClient {
        VendorClient::new(server, None).expect("BoringSSL client context")
    }

    /// The request head is the client's, byte for byte: caller headers sorted
    /// with their case preserved, then the default block in the order the
    /// genuine client emits it. This is a wire contract, so the assertion is
    /// on whole bytes rather than on parsed header names.
    #[test]
    fn request_head_is_the_clients_bytes() {
        let url = reqwest::Url::parse("https://opencode.ai/inference/openai/v1/chat/completions")
            .unwrap();
        let headers = vec![
            ("x-session-id".to_string(), "ses_1".to_string()),
            ("Content-Type".to_string(), "application/json".to_string()),
            ("Authorization".to_string(), "Bearer public".to_string()),
            (
                "User-Agent".to_string(),
                "opencode/latest/2.0.27/cli".to_string(),
            ),
        ];
        let head = build_head(
            &VendorRequest::post(url, headers, Bytes::from_static(b"{}")),
            false,
            None,
        )
        .expect("head builds");
        assert_eq!(
            std::str::from_utf8(&head).unwrap(),
            concat!(
                "POST /inference/openai/v1/chat/completions HTTP/1.1\r\n",
                // Sorted by BYTES: uppercase names first, then lowercase.
                "Authorization: Bearer public\r\n",
                "Content-Type: application/json\r\n",
                "User-Agent: opencode/latest/2.0.27/cli\r\n",
                "x-session-id: ses_1\r\n",
                "Connection: keep-alive\r\n",
                "Accept: */*\r\n",
                "Host: opencode.ai\r\n",
                "Accept-Encoding: identity\r\n",
                "Content-Length: 2\r\n",
                "\r\n",
            )
        );
    }

    /// `http://` through a proxy is written in absolute form with the proxy's
    /// credential; a tunnel would be the wrong shape for a cleartext hop.
    #[test]
    fn proxied_cleartext_uses_absolute_form() {
        let url = reqwest::Url::parse("http://192.0.2.1:9/v1/models").unwrap();
        let auth = HeaderValue::from_static("Basic dTpw");
        let head =
            build_head(&VendorRequest::get(url, vec![]), true, Some(&auth)).expect("head builds");
        let head = std::str::from_utf8(&head).unwrap();
        assert!(
            head.starts_with("GET http://192.0.2.1:9/v1/models HTTP/1.1\r\n"),
            "{head}"
        );
        assert!(
            head.contains("Proxy-Authorization: Basic dTpw\r\n"),
            "{head}"
        );
        assert!(head.contains("Host: 192.0.2.1:9\r\n"), "{head}");
        assert!(
            !head.contains("Content-Length"),
            "a bodyless GET carries no length"
        );
    }

    /// Read one request head from a mock socket.
    async fn read_head(sock: &mut tokio::net::TcpStream) -> String {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = sock.read(&mut chunk).await.expect("request head");
            assert!(n > 0, "socket closed before a request head");
            buf.extend_from_slice(&chunk[..n]);
        }
        String::from_utf8_lossy(&buf).into_owned()
    }

    /// A chunked, content-encoded body is decoded as it streams. Its
    /// connection is NOT pooled (the codec ends before the HTTP framing
    /// terminator, so the socket's tail would be stale), which the accept
    /// count pins.
    #[tokio::test]
    async fn chunked_encoded_bodies_decode_and_their_connection_is_not_pooled() {
        use async_compression::tokio::write::GzipEncoder;
        use tokio::io::AsyncWriteExt;

        let payload = br#"{"object":"chat.completion.chunk","choices":[]}"#;
        let mut encoder = GzipEncoder::new(Vec::new());
        encoder.write_all(payload).await.unwrap();
        encoder.shutdown().await.unwrap();
        let gz = encoder.into_inner();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepts_srv = accepts.clone();
        let gz_srv = gz.clone();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut sock, _) = listener.accept().await.unwrap();
                accepts_srv.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let head = read_head(&mut sock).await;
                assert!(head.starts_with("GET /v1/models HTTP/1.1\r\n"), "{head}");
                // Split the chunk header and its data across writes: the
                // decoder must not depend on one recv() per frame.
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-encoding: gzip\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n",
                    gz_srv.len()
                );
                sock.write_all(response.as_bytes()).await.unwrap();
                sock.write_all(&gz_srv).await.unwrap();
                sock.write_all(b"\r\n0\r\n\r\n").await.unwrap();
            }
        });
        let server_cfg = ServerConfig::default();
        let vendor = test_vendor(&server_cfg);
        let lane = test_lane();
        let url = reqwest::Url::parse(&format!("http://{addr}/v1/models")).unwrap();
        for _ in 0..2 {
            let resp = vendor
                .send(&lane, VendorRequest::get(url.clone(), vec![]))
                .await
                .expect("the mock answered");
            assert_eq!(resp.status(), http::StatusCode::OK);
            assert_eq!(resp.body_bytes().await.unwrap().as_ref(), payload);
        }
        server.await.unwrap();
        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "a coded body's socket must not be reused"
        );
    }

    /// Plain chunked is pooled: the framing terminal is read by our own state
    /// machine, so the connection is genuinely at a request boundary when the
    /// second request takes it. This is the contrast case for the encoded test
    /// above — a compressed body must not be pooled, an identity one must.
    #[tokio::test]
    async fn plain_chunked_bodies_park_the_connection() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepts_srv = accepts.clone();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            accepts_srv.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            for _ in 0..2 {
                let _ = read_head(&mut sock).await;
                sock.write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n4\r\n{\"a\"\r\n3\r\n:1}\r\n0\r\n\r\n",
                )
                .await
                .unwrap();
            }
        });
        let server_cfg = ServerConfig::default();
        let vendor = test_vendor(&server_cfg);
        let lane = test_lane();
        let url = reqwest::Url::parse(&format!("http://{addr}/v1/models")).unwrap();
        for _ in 0..2 {
            let resp = vendor
                .send(&lane, VendorRequest::get(url.clone(), vec![]))
                .await
                .expect("the mock answered");
            assert_eq!(resp.body_bytes().await.unwrap().as_ref(), br#"{"a":1}"#);
        }
        server.await.unwrap();
        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "plain chunked must return its connection to the lane"
        );
    }

    /// A 1xx interim head is not the answer: the caller must see the FINAL
    /// head. Reading `103 Early Hints` as a final 103 would hand back an
    /// empty body and leave the real response bytes on a socket the pool
    /// could then reuse.
    #[tokio::test]
    async fn interim_1xx_heads_are_skipped() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _ = read_head(&mut sock).await;
            sock.write_all(
                b"HTTP/1.1 103 Early Hints\r\nlink: </x.css>; rel=preload\r\n\r\n\
                  HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok",
            )
            .await
            .unwrap();
        });
        let server_cfg = ServerConfig::default();
        let vendor = test_vendor(&server_cfg);
        let lane = test_lane();
        let url = reqwest::Url::parse(&format!("http://{addr}/v1/models")).unwrap();
        let resp = vendor
            .send(&lane, VendorRequest::get(url, vec![]))
            .await
            .expect("the mock answered");
        assert_eq!(resp.status(), http::StatusCode::OK);
        assert_eq!(resp.body_bytes().await.unwrap().as_ref(), b"ok");
        server.await.unwrap();
    }

    /// A body with neither a length nor chunking ends at EOF and must NOT be
    /// parked for reuse: the socket is gone, and a pooled half-closed
    /// connection would fail the next request.
    #[tokio::test]
    async fn close_delimited_bodies_end_at_eof_and_are_not_pooled() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepts_srv = accepts.clone();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut sock, _) = listener.accept().await.unwrap();
                accepts_srv.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let _ = read_head(&mut sock).await;
                sock.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\n\r\nhello")
                    .await
                    .unwrap();
                sock.shutdown().await.unwrap();
            }
        });
        let server_cfg = ServerConfig::default();
        let vendor = test_vendor(&server_cfg);
        let lane = test_lane();
        let url = reqwest::Url::parse(&format!("http://{addr}/v1/models")).unwrap();
        for _ in 0..2 {
            let resp = vendor
                .send(&lane, VendorRequest::get(url.clone(), vec![]))
                .await
                .expect("the mock answered");
            assert_eq!(resp.body_bytes().await.unwrap().as_ref(), b"hello");
        }
        server.await.unwrap();
        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "a close-delimited body must not be reused"
        );
    }

    /// The in-flight ceiling QUEUES: a second request waits for the first
    /// body's permit instead of being shed, which is the shape the vendor's
    /// undocumented concurrency cliff needs (latency grows, no 403).
    #[tokio::test]
    async fn the_inflight_cap_queues_rather_than_shedding() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _ = read_head(&mut sock).await;
            sock.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n")
                .await
                .unwrap();
            // Hold the body open until the test has tried the second send.
            let _ = released.await;
            sock.write_all(b"ok").await.unwrap();
            // The queued request rides the connection the first one parks.
            let head = read_head(&mut sock).await;
            assert!(head.starts_with("GET /v1/models HTTP/1.1\r\n"), "{head}");
            sock.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await
                .unwrap();
        });
        let server_cfg = ServerConfig::default();
        let vendor = test_vendor(&server_cfg).with_inflight_cap(Some(1));
        let lane = test_lane();
        let url = reqwest::Url::parse(&format!("http://{addr}/v1/models")).unwrap();
        let first = vendor
            .send(&lane, VendorRequest::get(url.clone(), vec![]))
            .await
            .expect("first send");
        // The first response's body has not finished, so its permit is alive.
        let second = tokio::time::timeout(
            Duration::from_millis(100),
            vendor.send(&lane, VendorRequest::get(url.clone(), vec![])),
        )
        .await;
        assert!(
            second.is_err(),
            "the second request must wait for the first body, not slip past the cap"
        );
        drop(release);
        assert_eq!(first.body_bytes().await.unwrap().as_ref(), b"ok");
        let second = tokio::time::timeout(
            Duration::from_secs(5),
            vendor.send(&lane, VendorRequest::get(url, vec![])),
        )
        .await
        .expect("the queued request proceeds once the cap frees")
        .expect("second send");
        assert_eq!(second.body_bytes().await.unwrap().as_ref(), b"ok");
        server.await.unwrap();
    }

    #[test]
    fn basic_proxy_authorization_encoding() {
        assert_eq!(base64(b"user:pass"), "dXNlcjpwYXNz");
        assert_eq!(base64(b"a"), "YQ==");
        assert_eq!(base64(b"ab"), "YWI=");
        assert_eq!(base64(b"abc"), "YWJj");
    }
}
