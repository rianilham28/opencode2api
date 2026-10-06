use clap::Parser;
use opencode2api_kit::{ServerConfig, telemetry};
use opencode2api_server::Pipeline;
use opencode2api_transport::{ProxyConfig, build};
use service::{OpenAiProvider, ServiceConfig};
use std::path::PathBuf;
use std::sync::Arc;

/// Example opencode2api service: OpenAI-dialect provider (the copy-me bin).
#[derive(Parser, Debug)]
#[command(name = "service", version, about)]
struct Args {
    /// JSON config file (see config.example.json).
    #[arg(short, long, env = opencode2api_kit::ENV_CONFIG)]
    config: Option<PathBuf>,
}

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Before anything reads the environment: `.env` fills in what the shell
    // did not export, and never overrides what it did.
    let env_file = opencode2api_kit::load_dotenv()?;
    let args = Args::parse();
    let (server_cfg, doc) = ServerConfig::load(args.config.as_deref())?;
    let provider_cfg = ServiceConfig::from_doc(&doc)?;

    let _telemetry = telemetry::init(&server_cfg, "service")?;
    if let Some(path) = env_file {
        // Logged only after telemetry exists; the file's VALUES never are.
        tracing::info!(path = %path.display(), "loaded env file");
    }
    // Recorder install is process-global and must precede serving; failing to
    // install (double boot, or tests that already set one) must never stop
    // the proxy from serving — /metrics answers 501 until it works.
    let metrics = match telemetry::install_metrics() {
        Ok(handle) => {
            // Transport owns the egress family; the composition root installs
            // its descriptions only after a recorder actually owns the data.
            opencode2api_transport::describe_metrics();
            Some(handle)
        }
        Err(e) => {
            tracing::warn!(error = %e, "metrics recorder not installed");
            None
        }
    };

    // Egress first: with `server.proxy` configured this builds the sticky
    // lanes, warms each one against the upstream's cheapest endpoint so the
    // CONNECT + TLS handshake is spent off the request path, and starts the
    // rotation that replaces a session before the vendor expires it.
    let proxy_cfg = ProxyConfig::from_doc(&doc)?;
    let transport = build(&server_cfg, proxy_cfg.as_ref())?;
    let provider = Arc::new(OpenAiProvider::new(transport.clone(), provider_cfg)?);
    // The provider owns the endpoint shape, so the probe comes from it.
    transport
        .start(Some(provider.probe_url().to_string()))
        .await;
    let pipeline = Pipeline::new(provider, Arc::new(server_cfg), metrics);
    opencode2api_server::run(pipeline).await
}
