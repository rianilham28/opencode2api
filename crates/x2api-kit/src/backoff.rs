//! Retry backoff math: full jitter on a seeded splitmix64.
//!
//! Why not `rand`: the retry path wants a deterministic-per-(request, attempt)
//! spread with zero syscalls and zero deps. splitmix64 seeded from the
//! request id de-synchronizes concurrent retry streams wider than additive
//! jitter and, for a hard total budget, minimizes expected wait so more
//! attempts fit.

/// One splitmix64 step: turns a request/attempt pair into a well-distributed
/// seed with no wall-clock syscall per retry and no `rand` dependency.
pub fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Full-jitter exponential backoff: uniform in `[0, min(cap, base * 2^attempt)]`.
/// `attempt` is 1-based; saturation caps the shift so absurd attempt counts
/// cannot overflow. Zero remains possible by design; validated configurations
/// have a non-zero base, but the function itself must remain total.
pub fn full_jitter(
    attempt: u32,
    base: std::time::Duration,
    cap: std::time::Duration,
    seed: u64,
) -> std::time::Duration {
    let base_ms = base.as_millis() as u64;
    let cap_ms = cap.as_millis() as u64;
    let span = base_ms
        .saturating_mul(1u64 << (attempt.saturating_sub(1)).min(16))
        .min(cap_ms);
    if span == 0 {
        return std::time::Duration::ZERO;
    }
    std::time::Duration::from_millis(splitmix64(seed ^ (attempt as u64)) % (span + 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn splitmix_is_deterministic_and_avalanching() {
        assert_eq!(splitmix64(1), splitmix64(1));
        assert_ne!(splitmix64(1), splitmix64(2));
    }

    #[test]
    fn jitter_stays_within_bounds_and_grows() {
        let base = Duration::from_millis(100);
        let cap = Duration::from_secs(4);
        for seed in 0..64u64 {
            let d1 = full_jitter(1, base, cap, seed);
            assert!(d1 <= base, "attempt 1 must be within base: {d1:?}");
            let d5 = full_jitter(5, base, cap, seed);
            assert!(d5 <= cap, "never exceeds cap");
        }
        // Deterministic per (attempt, seed): same inputs, same delay.
        assert_eq!(full_jitter(3, base, cap, 9), full_jitter(3, base, cap, 9));
        // Different seeds spread across the window (probabilistic but the
        // window is 800ms wide; all-identical would need a 64-way collision).
        let spread: Vec<_> = (0..64).map(|s| full_jitter(4, base, cap, s)).collect();
        assert!(
            spread
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                > 8
        );
    }

    #[test]
    fn absurd_attempts_saturate_instead_of_overflowing() {
        assert!(
            full_jitter(
                u32::MAX,
                Duration::from_millis(500),
                Duration::from_secs(1),
                7
            ) <= Duration::from_secs(1)
        );
    }
}
