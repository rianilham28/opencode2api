//! Test-only log capture: run `body` under the SAME json layout the shipped
//! `log_json` layer uses and hand back the parsed lines.
//!
//! The terminal line's field vocabulary is a published contract — operators
//! grep it — so it is asserted where it is
//! built rather than eyeballed in a manual run. A capture sink keeps that
//! assertion honest: no global subscriber is installed (tests run in parallel
//! and would otherwise attribute each other's lines), and nothing is written to
//! disk.

use std::io::Write;
use std::sync::Arc;

use parking_lot::Mutex;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt as _;

#[derive(Clone)]
struct Sink(Arc<Mutex<Vec<u8>>>);

impl<'a> MakeWriter<'a> for Sink {
    type Writer = Buf;

    fn make_writer(&'a self) -> Buf {
        Buf(self.0.clone())
    }
}

struct Buf(Arc<Mutex<Vec<u8>>>);

impl Write for Buf {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Every json line `body` emitted, in order, under the `info` filter the
/// shipped default produces — the level is part of what these tests defend.
pub(crate) fn capture(body: impl FnOnce()) -> Vec<serde_json::Value> {
    let shared = Arc::new(Mutex::new(Vec::new()));
    let layer = x2api_kit::telemetry::capture_json_layer(Sink(shared.clone()));
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(layer);
    tracing::subscriber::with_default(subscriber, body);
    let bytes = std::mem::take(&mut *shared.lock());
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}
