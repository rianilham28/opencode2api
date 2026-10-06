//! Test-only log capture for this crate: install the SAME json layout
//! `telemetry::init` installs, emit, and hand back the parsed lines.
//!
//! Why a shared module rather than a helper inside each test module: the thing
//! these tests defend is the shape of a log line, and two independent renderings
//! of "how our json layer writes" is how one module's test passes while the
//! other's pins a format nobody ships.
//!
//! The subscriber is installed as a THREAD-LOCAL default (`set_default`), never
//! `set_global_default`: a tracing dispatcher cannot be unset, so a global would
//! leak into every other test in the binary. Thread-local has a consequence the
//! async tests depend on — a task migrated between workers would lose the
//! subscriber mid-await and read back an EMPTY capture as a pass — so callers
//! must stay on a current-thread runtime, which is what `#[tokio::test]` gives
//! them by default.

use std::path::PathBuf;

use tracing_subscriber::layer::SubscriberExt as _;

/// Holds the dispatcher guard; the writer is synchronous so a line is visible
/// as soon as the event returns and parallel tests cannot lose a warning to a
/// background flush race.
pub(crate) struct Capture {
    dir: PathBuf,
    _default: tracing::dispatcher::DefaultGuard,
}

impl Capture {
    /// Installs the capture for the lifetime of the returned value. `tag` keeps
    /// concurrent tests in different directories.
    pub(crate) fn start(tag: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("opencode2api-capture-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir for captured lines");
        let appender = tracing_appender::rolling::never(&dir, "out.log");
        let sub = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("info"))
            .with(crate::telemetry::capture_json_layer(appender));
        Self {
            dir,
            _default: tracing::subscriber::set_default(sub),
        }
    }

    /// Await `body` under the capture, then return its lines.
    ///
    /// Only sound because the default is THREAD-LOCAL and `#[tokio::test]` runs
    /// on a current-thread runtime: an `await` that migrated workers would
    /// resume where no subscriber is installed and the test would read back an
    /// empty capture as a pass. Keeping this in one `async fn` means the await
    /// happens while `self` — and therefore the guard — is still alive.
    pub(crate) async fn collect_async(
        self,
        body: impl std::future::Future<Output = ()>,
    ) -> Vec<serde_json::Value> {
        body.await;
        self.finish()
    }

    /// Finish without emitting, then read, parse, and clean up.
    pub(crate) fn finish(self) -> Vec<serde_json::Value> {
        let path = self.dir.join("out.log");
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        let lines = text
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();
        let _ = std::fs::remove_dir_all(&self.dir);
        lines
    }
}
