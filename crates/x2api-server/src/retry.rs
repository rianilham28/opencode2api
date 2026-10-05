//! The retry loop: generic over the attempt because the provider seam is a
//! trait object. The unit of retry is one *attempt*, and an attempt is only
//! over when its outcome is committable — a buffered response fully read, or
//! a stream handed off after its status gate (the invariant documented on
//! `Provider::stream`).

use std::future::Future;
use std::time::Duration;

use x2api_kit::ProviderError;
use x2api_kit::backoff::{full_jitter, splitmix64};

use crate::Pipeline;

/// What the loop learned, not just the value: the terminal record carries
/// `retries`, because "why did this take 4 s" should be one field rather than a
/// join on `request_id` between the retry warning and the record.
pub(crate) struct Retried<T> {
    pub(crate) outcome: Result<T, ProviderError>,
    /// Attempts actually made; 1 means no retry happened.
    pub(crate) attempts: u32,
}

impl<T> Retried<T> {
    /// The count worth logging: zero when the first attempt carried the day, so
    /// the field can stay off the line in the common case.
    pub(crate) fn retries(&self) -> u64 {
        u64::from(self.attempts.saturating_sub(1))
    }
}

pub(crate) async fn with_retry<T, F, Fut>(
    p: &Pipeline,
    request_id: u64,
    op: &'static str,
    mut attempt_fn: F,
) -> Retried<T>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, ProviderError>>,
{
    let r = p.cfg.retry;
    let mut last: Option<ProviderError> = None;
    for attempt in 1..=r.max_attempts {
        match attempt_fn(attempt).await {
            Ok(v) => {
                return Retried {
                    outcome: Ok(v),
                    attempts: attempt,
                };
            }
            Err(e) => {
                if !e.is_retryable() {
                    return Retried {
                        outcome: Err(e),
                        attempts: attempt,
                    };
                }
                last = Some(e);
                if attempt == r.max_attempts {
                    break;
                }
                let e = last.as_ref().expect("set above");
                let delay = full_jitter(
                    attempt,
                    Duration::from_millis(r.base_ms),
                    Duration::from_millis(r.cap_ms),
                    splitmix64(request_id ^ u64::from(attempt)),
                );
                tracing::warn!(
                    request_id,
                    op,
                    attempt,
                    status = e.status,
                    delay_ms = delay.as_millis() as u64,
                    "retryable upstream failure"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
    Retried {
        outcome: Err(
            last.unwrap_or_else(|| ProviderError::internal("retry loop exited without an error"))
        ),
        attempts: r.max_attempts,
    }
}
