//! # opencode2api-server
//!
//! The generic HTTP half of the template: every service bin gets the chat
//! endpoints, the pipeline, and the operational behavior by depending on this
//! crate — and implements *only* `opencode2api_kit::Provider`.
//!
//! Boundary rules:
//! - The router holds only `Arc<dyn Provider>`; the concrete service type
//!   appears exactly once, in the bin's `main`.
//! - Error envelopes for both inbound formats render from the *same*
//!   `ProviderError` message fields (the Anthropic renderer reuses the
//!   OpenAI envelope so the safe-message logic cannot drift).
//! - There is ONE guarded-stream helper (`relay`), not one per surface —
//!   near-duplicate guards drift.
//! - A stream's retry unit ends at provider handoff; after the first frame
//!   there is no failing over left to do.
//! - Graceful shutdown is two-stage: axum's drain plus a hard cap, because
//!   `with_graceful_shutdown` alone waits unbounded on streams.

mod errors;
mod relay;
mod retry;
mod router;
mod routes;

#[cfg(test)]
mod testlog;

use std::sync::Arc;
use tokio::sync::Semaphore;

use opencode2api_kit::Provider;
pub use opencode2api_kit::ServerConfig;
pub use router::{build_router, run, serve};

/// Which client dialect a request arrived in. Drives parsing, envelope
/// rendering, and the SSE encoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundFormat {
    /// `/v1/chat/completions` (and `/v1/models`).
    OpenAi,
    /// `/v1/messages` via the the anthropic module bridge.
    Anthropic,
    /// `/v1/responses` via the the responses module bridge.
    Responses,
    /// `/v1beta/models/{model}:generateContent` via the gemini module bridge.
    Gemini,
}

impl InboundFormat {
    /// The provider-facing name for this same dialect.
    ///
    /// `Dialect` lives in `opencode2api-kit` (providers declare which ones their
    /// upstream speaks natively) and `InboundFormat` lives here (the server
    /// renders envelopes and picks sinks from it). They are the same four
    /// dialects seen from two sides, so the mapping belongs in ONE place:
    /// spelled by hand at each route, a mismatched pair would relay one
    /// dialect's bytes and render another's errors.
    pub(crate) const fn dialect(self) -> opencode2api_kit::Dialect {
        match self {
            Self::OpenAi => opencode2api_kit::Dialect::Chat,
            Self::Anthropic => opencode2api_kit::Dialect::Anthropic,
            Self::Responses => opencode2api_kit::Dialect::Responses,
            Self::Gemini => opencode2api_kit::Dialect::Gemini,
        }
    }

    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Anthropic => "anthropic",
            Self::Responses => "responses",
            Self::Gemini => "gemini",
        }
    }
}

/// Everything the generic pipeline shares. Cloning is Arc-bumps; the request
/// path never touches a lock outside admission.
#[derive(Clone)]
pub struct Pipeline {
    pub provider: Arc<dyn Provider>,
    pub cfg: Arc<ServerConfig>,
    /// `None` when the bin chose not to install the global recorder (tests:
    /// the recorder is process-global and install-twice fails).
    pub metrics: Option<metrics_exporter_prometheus::PrometheusHandle>,
    admission: Arc<Semaphore>,
}

impl Pipeline {
    pub fn new(
        provider: Arc<dyn Provider>,
        cfg: Arc<ServerConfig>,
        metrics: Option<metrics_exporter_prometheus::PrometheusHandle>,
    ) -> Self {
        let admission = Arc::new(Semaphore::new(cfg.max_inflight));
        Self {
            provider,
            cfg,
            metrics,
            admission,
        }
    }
}

/// Metric names, re-exported from the crate that DESCRIBES them at boot
/// (`opencode2api_kit::telemetry::names`) so a name and its help text cannot drift
/// apart. Handlers import from here; the literals live in exactly one place.
pub(crate) mod names {
    pub use opencode2api_kit::telemetry::names::{
        ADMISSION_QUEUE, DURATION, MEDIA, REQUESTS, STREAM_FIRST_TOKEN, STREAMS, TOKENS,
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two enums are the same dialects seen from two sides, and the routes
    /// spell their own pairs. A mismatch could relay one dialect's bytes while
    /// rendering another's error envelopes — silently, and only for a native
    /// upstream.
    ///
    /// Iterated over a list that MUST cover every variant: this test is the
    /// only thing standing between a fifth `InboundFormat` and an unpinned
    /// pairing, and it passed for a whole dialect's arrival because it
    /// enumerated three of four. A variant outside the list is now a failure,
    /// not a silent gap.
    #[test]
    fn every_inbound_format_maps_to_its_own_dialect() {
        use opencode2api_kit::Dialect;
        let all = [
            (InboundFormat::OpenAi, Dialect::Chat, "chat"),
            (InboundFormat::Anthropic, Dialect::Anthropic, "messages"),
            (InboundFormat::Responses, Dialect::Responses, "responses"),
            (
                InboundFormat::Gemini,
                Dialect::Gemini,
                opencode2api_kit::DIALECT_GENERATE_CONTENT,
            ),
        ];
        assert_eq!(
            all.len(),
            4,
            "every InboundFormat variant belongs in this table"
        );
        for (fmt, dialect, route_label) in all {
            assert_eq!(fmt.dialect(), dialect, "{}", fmt.label());
            // The label the ROUTE is gated by and the label METRICS carry are
            // different strings by design; both must exist for one dialect.
            assert!(
                opencode2api_kit::DIALECT_LABELS.contains(&route_label),
                "{route_label} gates no route this server knows about"
            );
        }
    }
}
