//! # service — THIS deployment's program
//!
//! One proxy process serves exactly one upstream, so this crate is not an
//! example to copy: it IS the program, and `src/provider.rs` is the only
//! file a fork has to write. What ships there today is an OpenAI-dialect
//! implementation — a starting point, not a fixture.
//!
//! That is why nothing here is named after a vendor. The crate is `service`,
//! the binary is `opencode2api` (rename it to whatever you deploy as; nothing
//! depends on it), and pointing this at a different upstream is a diff to one
//! file rather than a copy-and-rename of a crate.
//!
//! **Provider logic is CODE, config is not.** `ServiceConfig` below carries
//! secrets, base URL and model aliases — things an operator legitimately
//! changes per deployment. A behavioural knob appearing there is a mistake;
//! it belongs in `provider.rs`, where every translation that could differ for
//! your vendor has its own named seam:
//! - `chat_url` / `models_url` — endpoint construction
//! - `identity_headers` / `authenticate` — the identity every request carries
//! - `to_upstream_body` — IR -> vendor request (per-model renames like
//!   `max_tokens` vs `max_completion_tokens` go HERE, in code)
//! - `decode_upstream_sse` — vendor SSE -> IR chunks
//!
//! Optional seams, both defaulted so you can ignore them: the fidelity lane
//! (`native_dialects()` with `relay_raw()`, for an upstream that already
//! speaks a client dialect) and `ready()`, which wires readiness to egress
//! health.

pub mod provider;

pub use provider::OpenAiProvider;
use serde::Deserialize;
use serde_json::Value;

/// Operator-owned config: secrets and endpoints ONLY. The moment a logic
/// knob wants to live here, it is in the wrong place — put it in `provider`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    /// Stable operator-chosen identity used in upstream error prefixes and
    /// provider log/metric dimensions; it does not select request behavior.
    #[serde(default = "default_provider_name")]
    pub name: String,
    /// Serde-defaulted ONLY so the documented `OPENCODE2API_UPSTREAM_URL` env
    /// override gets a chance to supply it: the real gate is the `ensure!`
    /// in `from_doc`, which runs after the environment is read. Without
    /// this the parse died first and the env-only quick start the files
    /// advertise was unbootable.
    #[serde(default)]
    pub base_url: String,
    #[serde(default)]
    pub api_key: Option<String>,
    /// More than one upstream credential. ADDITIVE, not exclusive: `api_key`
    /// becomes slot 0, so an operator who already has `OPENCODE2API_UPSTREAM_KEY` in
    /// `.env` adds a second account by listing it here (or in
    /// `OPENCODE2API_UPSTREAM_KEYS`) without rewriting what works. The rotation policy
    /// — which vendor answer means "this credential is spent" — is code in
    /// `provider.rs`, because that is a vendor fact and `ServiceConfig` is
    /// documented to hold only secrets and endpoints.
    #[serde(default)]
    pub api_keys: Vec<String>,
    /// Optional: rewrite model aliases to upstream model ids in code-visible
    /// form (still a mapping, not behavior).
    #[serde(default)]
    pub model_map: std::collections::HashMap<String, String>,
    /// Response-side twin of `ServerConfig::max_body_bytes` — different
    /// direction AND scope: that one is the whole proxy's cap on what a
    /// CLIENT may send inbound; this is a per-service cap on what a VENDOR
    /// may return in a single buffered reply (streams are bounded by the
    /// request deadline, not by size). This is a resource ceiling, not a
    /// behavioural knob, so it may live here. Default:
    /// `opencode2api_kit::provider::DEFAULT_MAX_RESPONSE_BYTES`.
    #[serde(default)]
    pub max_response_bytes: Option<u64>,
    /// The model-catalogue endpoint, when it is NOT `{base_url}/models`.
    ///
    /// Measured 2026-10-25 against the live vendor: the catalogue advertises
    /// `/inference/openai/v1`, and chat works on BOTH that root and the legacy
    /// `/inference/v1`, but `GET /inference/openai/v1/models` answers 404
    /// while `GET /inference/v1/models` answers the list. A proxy that derives
    /// its catalogue URL from the chat base therefore never passes readiness
    /// on this deployment. An explicit value wins; otherwise the provider
    /// applies the vendor's own sibling rule (see `OpenAiProvider::new`).
    #[serde(default)]
    pub models_url: Option<String>,
    /// Ceiling on requests this sandbox keeps in flight to the vendor.
    ///
    /// A RESOURCE ceiling, like `max_response_bytes`, not a behavioural knob:
    /// upstream is reported to apply a per-sandbox concurrency cliff (one
    /// sibling proxy measured ~8 simultaneous requests turning into blanket
    /// `403 FreeTierError`, with 400 ms spacing passing), and the ceiling is
    /// what keeps the proxy under it. **The cliff is UNMEASURED on this
    /// deployment** — `None` means no cap, which is the honest default until
    /// someone runs the K ∈ {1, 4, 8, 12, 16} ladder against the genuine
    /// client and sets this to that largest all-ok K minus one. Once set,
    /// excess requests queue inside the vendor leg (latency grows, no 403);
    /// they are not shed.
    #[serde(default)]
    pub max_inflight: Option<u32>,
}

fn default_provider_name() -> String {
    "openai".to_string()
}

impl ServiceConfig {
    pub fn from_doc(doc: &Value) -> anyhow::Result<Self> {
        let section = doc
            .get("provider")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default()));
        let mut cfg: ServiceConfig = serde_json::from_value(section)
            .map_err(|e| anyhow::anyhow!("provider section: {e}"))?;
        if let Ok(v) = std::env::var(opencode2api_kit::config::ENV_UPSTREAM_URL) {
            cfg.base_url = v;
        }
        if let Ok(v) = std::env::var(opencode2api_kit::config::ENV_UPSTREAM_KEY) {
            cfg.api_key = Some(v);
        }
        if let Ok(v) = std::env::var(opencode2api_kit::config::ENV_UPSTREAM_MODELS_URL) {
            cfg.models_url = Some(v);
        }
        if let Ok(v) = std::env::var(opencode2api_kit::config::ENV_UPSTREAM_KEYS) {
            cfg.api_keys.extend(
                v.split(',')
                    .map(str::trim)
                    .filter(|k| !k.is_empty())
                    .map(String::from),
            );
        }
        anyhow::ensure!(
            !cfg.base_url.is_empty(),
            "provider.base_url is required — set provider.base_url in the \
             config file or OPENCODE2API_UPSTREAM_URL in the environment"
        );
        cfg.base_url = cfg.base_url.trim_end_matches('/').to_string();
        Ok(cfg)
    }

    /// Every credential this service may use, in slot order, de-duplicated.
    ///
    /// Deduplicated because the single key and the list are separate surfaces
    /// that easily hold the same value (an operator who moves `api_key` into
    /// `api_keys` but leaves `.env` set), and a duplicate slot is not a second
    /// account: it would double the traffic that key sees and halve the point of
    /// the pool.
    pub fn credentials(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::with_capacity(self.api_keys.len() + 1);
        for key in self.api_key.iter().chain(self.api_keys.iter()) {
            let key = key.trim();
            if !key.is_empty() && !out.iter().any(|seen| seen == key) {
                out.push(key.to_string());
            }
        }
        out
    }

    /// The buffered-reply ceiling every whole-body read on this service's
    /// upstream must pass through.
    pub fn response_ceiling(&self) -> usize {
        self.max_response_bytes
            .and_then(|b| usize::try_from(b).ok())
            .unwrap_or(opencode2api_kit::provider::DEFAULT_MAX_RESPONSE_BYTES)
    }
}
