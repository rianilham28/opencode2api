//! Slot-based cooldown for a set of interchangeable upstream credentials.
//!
//! The pool holds NO credential. Its state is a vector of "ready at" instants
//! indexed by slot, and `pick` hands back a NUMBER — the secret stays in the
//! owner's own `Vec<String>`. That split is deliberate rather than
//! stylistic: the thing a caller most plausibly does with a pool is log it, put
//! it in a `Debug` impl, or render it on a status page, and any of those would
//! print a credential if the pool held one. An owned `Debug` is therefore not
//! needed (and the derive would be safe anyway), but a type that CANNOT leak is
//! better than one that must be remembered not to.
//!
//! It is also deliberately not a load balancer: no weights, no health probes,
//! no circuit-breaker state machine. The only question it answers is "which of
//! these has not been told to slow down yet", and it makes NO judgement about
//! how long a slot is gone — it is handed a duration. Reading `Retry-After` and
//! deciding what it means is the owner's job, because that is a fact about a
//! particular vendor (which header spelling it uses, and how far its "wait 24
//! hours" can be trusted), and this type has to stay vendor-blind.

use parking_lot::Mutex;
use std::time::{Duration, Instant};

/// Why a credential was taken out of rotation, without exposing vendor status
/// codes to the pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CooldownReason {
    Quota,
    Auth,
    Other,
}

impl CooldownReason {
    fn label(self) -> &'static str {
        match self {
            Self::Quota => "quota",
            Self::Auth => "auth",
            Self::Other => "other",
        }
    }
}

/// A set of interchangeable credentials, addressed by slot.
///
/// `Locking:` one non-reentrant `Mutex` over the whole state, and no guard is
/// ever held across an `await`. `publish` borrows the caller's guard so metric
/// publication cannot re-enter and self-deadlock while the pool state is held.
#[derive(Debug)]
pub struct Pool {
    inner: Mutex<State>,
}

#[derive(Debug)]
struct State {
    /// Round-robin cursor: equal-cost keys should be used evenly, because
    /// free tiers are usually per-credential and burning one while others sit
    /// idle shortens the window before the whole set is cooling.
    next: usize,
    /// Per slot: the instant it may be used again. `None` = never cooled.
    ready_at: Vec<Option<Instant>>,
}

impl Pool {
    /// A pool of `slots` interchangeable credentials, all available.
    pub fn new(slots: usize) -> Self {
        Self {
            inner: Mutex::new(State {
                next: 0,
                ready_at: vec![None; slots],
            }),
        }
    }

    /// Publish the pool's state as `x2api_credentials{state=ready|cooling}`.
    ///
    /// Recorded here, by the type that owns the state, for the same reason the
    /// egress family lives in `x2api_transport`: a fork's provider cannot forget
    /// to report it, and the number cannot drift from the pool's own view of
    /// which deadlines have passed. Called on the paths that already hold the
    /// lock rather than from a timer, which bounds what the series means: it is
    /// FRESH WHILE TRAFFIC FLOWS (every `pick` re-publishes, so an expired
    /// cooldown stops being counted as spent as soon as anything asks), and a
    /// pool that has gone quiet keeps reporting its last observed state. Never
    /// with a credential in hand, because the labels are counts.
    fn publish(&self, st: &State, now: Instant) {
        let total = st.ready_at.len();
        if total == 0 {
            return; // passthrough deployment: there is no pool to report
        }
        let cooling = st.ready_at.iter().flatten().filter(|at| *at > &now).count();
        metrics::gauge!(crate::telemetry::names::CREDENTIALS, "state" => "cooling")
            .set(cooling as f64);
        metrics::gauge!(crate::telemetry::names::CREDENTIALS, "state" => "ready")
            .set((total - cooling) as f64);
    }

    pub fn len(&self) -> usize {
        self.inner.lock().ready_at.len()
    }

    /// The next usable slot, or `None` when every slot is cooling.
    ///
    /// Round-robin among the AVAILABLE ones, not "first available": always
    /// taking the lowest index would concentrate all traffic on slot 0 until it
    /// cools, then slot 1, which is precisely the uneven burn a pool exists to
    /// avoid.
    pub fn pick(&self, now: Instant) -> Option<usize> {
        // Observing the pool is also the moment a cooldown may have silently
        // ended, so this is where the gauge learns the truth rather than holding
        // the last failure's picture forever.
        let mut st = self.inner.lock();
        self.publish(&st, now);
        let n = st.ready_at.len();
        for step in 0..n {
            let slot = (st.next + step) % n;
            if st.ready_at[slot].is_none_or(|at| at <= now) {
                // Advance past the slot handed out, so the next caller starts
                // at the following one.
                st.next = (slot + 1) % n;
                return Some(slot);
            }
        }
        // A pool of ONE cannot be "exhausted": there is nowhere to divert the
        // traffic to, so honouring the cooldown would replace the vendor's own
        // retryable 429 with a 503 we invented, and would break the retry loop
        // that exists to ride a 429 out. Cooldown is a routing decision, and
        // routing needs at least two routes.
        (n == 1).then_some(0)
    }

    /// Take a slot out of rotation, and say whether this STARTED an episode.
    ///
    /// The `bool` is what keeps the caller's reporting honest. A 429 is not one
    /// event per outage but one per request, so a provider that warns on every
    /// `cool` call turns a rate-limited key into a flood of identical lines —
    /// exactly the volume this crate has been shrinking. `true` means "ready
    /// before, cooling now", which is the one moment worth a WARN and a metric
    /// increment; extending an existing cooldown returns `false` and says nothing
    /// twice. An out-of-range slot also reports `false`, so the ignore-don't-panic
    /// behaviour stays observable rather than silent.
    ///
    /// A zero/spurious `for_` is clamped to one tick, so a vendor answering
    /// `Retry-After: 0` still costs the caller something.
    pub fn cool(&self, slot: usize, for_: Duration, reason: CooldownReason) -> bool {
        let now = Instant::now();
        let mut st = self.inner.lock();
        let started = match st.ready_at.get_mut(slot) {
            Some(at) => {
                let was_ready = at.is_none_or(|at| at <= now);
                *at = Some(now + for_.max(Duration::from_millis(1)));
                was_ready
            }
            None => false,
        };
        self.publish(&st, now);
        if started {
            metrics::counter!(
                crate::telemetry::names::CREDENTIAL_COOLDOWNS,
                "reason" => reason.label()
            )
            .increment(1);
        }
        started
    }

    /// When a slot was last put out of rotation. Test-only surface: the
    /// zero-cooldown case cannot be observed through `pick`/`ready_in` without
    /// sleeping, and a test that sleeps to prove a 1 ms floor is worse than the
    /// bug it claims to catch.
    #[cfg(test)]
    pub fn expiry_of(&self, slot: usize) -> Option<Instant> {
        *self.inner.lock().ready_at.get(slot)?
    }

    /// Seconds until the earliest slot frees, for a caller-facing `Retry-After`
    /// when the whole set is cooling. `None` means something is available now —
    /// which is also what it means when only SOME of the set is cooling, since a
    /// caller that can proceed should not be told a wait.
    /// when the whole set is cooling. `None` means something is available now.
    ///
    /// Rounded UP: telling a client to wait 0.2 s and having it return before
    /// the slot is ready would re-enter the same all-cooling state, which reads
    /// as the pool ignoring its own advice.
    pub fn ready_in(&self, now: Instant) -> Option<u64> {
        let st = self.inner.lock();
        let mut waits: Vec<Duration> = Vec::with_capacity(st.ready_at.len());
        for at in st.ready_at.iter().flatten() {
            if let Some(remaining) = at.checked_duration_since(now) {
                waits.push(remaining);
            }
        }
        // A deadline already in the past is not a negative wait — that slot is
        // usable NOW, so there is no wait to report and it outranks every longer
        // deadline still in the list. Comparing counts says so without an early
        // return hiding inside the loop.
        if waits.len() != st.ready_at.len() {
            return None;
        }
        // Rounded UP: telling a client to wait 0 when the slot frees in 0.2 s
        // makes it return before the pool can serve it, which reads as the pool
        // ignoring its own advice.
        waits.into_iter().min().map(|d| d.as_secs().max(1))
    }

    /// How many slots are currently out of rotation.
    ///
    /// Lives on the pool because only this type knows what "cooling" means, and
    /// an EXPIRED deadline must count as ready — otherwise a gauge would keep
    /// reporting a key as spent after its cooldown ended and the operator would
    /// chase a credential that is already back.
    pub fn cooling(&self, now: Instant) -> usize {
        self.inner
            .lock()
            .ready_at
            .iter()
            .flatten()
            .filter(|at| *at > &now)
            .count()
    }

    /// Whether a slot exists at all — i.e. whether this is a credential pool or
    /// a passthrough deployment. Named for the caller that asks (`provider.rs`
    /// branches on it), not as a mirror of `Vec::is_empty`.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().ready_at.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picks_round_robin_across_slots() {
        let p = Pool::new(3);
        let now = Instant::now();
        assert_eq!(p.pick(now), Some(0));
        assert_eq!(p.pick(now), Some(1));
        assert_eq!(p.pick(now), Some(2));
        assert_eq!(p.pick(now), Some(0), "wraps, so no slot is privileged");
        assert_eq!(p.len(), 3);
    }

    #[test]
    fn a_cooling_slot_is_skipped_not_avoided_forever() {
        let p = Pool::new(2);
        let now = Instant::now();
        assert!(
            p.cool(0, Duration::from_secs(30), CooldownReason::Other),
            "a new episode reports"
        );
        assert!(
            !p.cool(0, Duration::from_secs(30), CooldownReason::Other),
            "an extension does not"
        );
        assert_eq!(p.pick(now), Some(1), "the other key carries traffic");
        // `None` here is not "no wait known": a slot is usable RIGHT NOW, which
        // is the only answer worth giving a caller that could just proceed.
        assert_eq!(p.ready_in(now), None, "the free slot is the answer");
        // Expiry, not a timer: the slot becomes usable on the next look.
        let later = now + Duration::from_secs(31);
        assert_eq!(
            p.pick(later),
            Some(0),
            "the cooled slot comes back by itself"
        );
        assert_eq!(p.ready_in(later), None);
    }

    /// One credential cannot be routed around. Refusing it would turn the
    /// vendor's retryable 429 into our own 503 and disable the retry loop for
    /// every single-key deployment — which is the normal case, not the edge one.
    #[test]
    fn a_single_credential_is_still_used_while_cooling() {
        let p = Pool::new(1);
        let now = Instant::now();
        assert!(p.cool(0, Duration::from_secs(60), CooldownReason::Other));
        assert_eq!(
            p.pick(now),
            Some(0),
            "no other slot exists, so the retry loop must still be able to try"
        );
        assert_eq!(p.ready_in(now), Some(60), "but the wait is still known");
    }

    #[test]
    fn all_cooling_reports_a_wait_instead_of_a_slot() {
        let p = Pool::new(2);
        let now = Instant::now();
        assert!(p.cool(0, Duration::from_secs(10), CooldownReason::Other));
        assert!(p.cool(1, Duration::from_secs(45), CooldownReason::Other));
        assert_eq!(p.pick(now), None);
        assert_eq!(
            p.ready_in(now),
            Some(10),
            "the SHORTEST wait, so the caller retries when any key is back"
        );
    }

    #[test]
    fn a_zero_retry_after_still_costs_something() {
        // Two slots, both cooled: with one there would be nothing to route
        // around, and `a_single_credential_is_still_used_while_cooling` covers
        // that case. The point under test is that a zero-length cooldown is not
        // the same as no cooldown — `Retry-After: 0` must still cost a tick.
        let p = Pool::new(2);
        let now = Instant::now();
        assert!(p.cool(0, Duration::ZERO, CooldownReason::Other));
        assert!(p.cool(1, Duration::ZERO, CooldownReason::Other));
        assert!(
            now < p.expiry_of(1).expect("a zero cooldown still expires later"),
            "`Retry-After: 0` was a no-op"
        );
        assert_eq!(
            p.ready_in(now),
            Some(1),
            "and the advice rounds up to a second rather than 0"
        );
    }

    #[test]
    fn out_of_range_slots_are_ignored_not_panicked() {
        // A vendor status handler computes slot numbers from a `Vec` that can be
        // reconfigured at boot; a panic in the error path would turn one bad
        // answer into a down proxy.
        let p = Pool::new(1);
        p.cool(7, Duration::from_secs(5), CooldownReason::Other);
        // Out of range is ignored, not panicked, and does not corrupt the pool:
        // the one real slot is still handed out.
        assert_eq!(p.pick(Instant::now()), Some(0));
        assert!(!p.is_empty());
        assert!(
            Pool::new(0).is_empty(),
            "a passthrough deployment has no slots"
        );
    }

    /// The gauge is the only way an operator learns a credential is spent while
    /// traffic still SUCCEEDS, so it is asserted against a real recorder rather
    /// than trusted from the code. Snapshotted after each step: a gauge holds one
    /// value per label set, so a single final snapshot would show only the last
    /// write and prove nothing about the transitions that matter.
    #[test]
    fn the_pool_publishes_ready_and_cooling_counts() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let seen = || -> (Option<f64>, Option<f64>) {
            let mut ready = None;
            let mut cooling = None;
            for (key, _, _, value) in snapshotter.snapshot().into_vec() {
                if key.key().name() != "x2api_credentials" {
                    continue;
                }
                let DebugValue::Gauge(g) = value else {
                    continue;
                };
                let is_cooling = key
                    .key()
                    .labels()
                    .any(|l| l.key() == "state" && l.value() == "cooling");
                if is_cooling {
                    cooling = Some(g.into_inner());
                } else {
                    ready = Some(g.into_inner());
                }
            }
            (ready, cooling)
        };
        metrics::with_local_recorder(&recorder, || {
            let p = Pool::new(3);
            let now = Instant::now();
            assert_eq!(seen(), (None, None), "nothing observed, nothing published");
            p.pick(now);
            assert_eq!(seen(), (Some(3.0), Some(0.0)), "all three ready");
            assert!(p.cool(0, Duration::from_secs(30), CooldownReason::Quota));
            assert_eq!(seen(), (Some(2.0), Some(1.0)), "one spent");
            assert!(p.cool(1, Duration::from_secs(30), CooldownReason::Auth));
            assert_eq!(seen(), (Some(1.0), Some(2.0)), "two spent");
            // Expiry has to be published as ready: remembering a spent slot after
            // its cooldown ended is how a gauge becomes a lie that outlives the
            // problem.
            p.pick(now + Duration::from_secs(31));
            assert_eq!(seen(), (Some(3.0), Some(0.0)), "expiry counted as ready");
        });
    }

    /// A passthrough deployment has no pool and must not grow a zero-valued
    /// series that looks like "3 credentials, all ready".
    #[test]
    fn an_empty_pool_publishes_nothing() {
        use metrics_util::debugging::DebuggingRecorder;
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let p = Pool::new(0);
            assert!(p.pick(Instant::now()).is_none());
            p.cool(0, Duration::from_secs(5), CooldownReason::Other);
            p.publish(&p.inner.lock(), Instant::now());
        });
        let n = snapshotter
            .snapshot()
            .into_vec()
            .iter()
            .filter(|(key, _, _, _)| key.key().name() == "x2api_credentials")
            .count();
        assert_eq!(n, 0, "no pool, no series");
    }

    #[test]
    fn cooldown_counter_counts_only_started_episodes() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let p = Pool::new(1);
            assert!(p.cool(0, Duration::from_secs(30), CooldownReason::Quota));
            assert!(!p.cool(0, Duration::from_secs(45), CooldownReason::Auth));
            assert!(!p.cool(7, Duration::from_secs(30), CooldownReason::Quota));
        });
        let mut counts = std::collections::BTreeMap::new();
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            if key.key().name() != crate::telemetry::names::CREDENTIAL_COOLDOWNS {
                continue;
            }
            let DebugValue::Counter(c) = value else {
                continue;
            };
            let reason = key
                .key()
                .labels()
                .find_map(|l| (l.key() == "reason").then_some(l.value().to_owned()));
            counts.insert(reason, c);
        }
        assert_eq!(
            counts,
            std::collections::BTreeMap::from([(Some("quota".to_owned()), 1)])
        );
    }
}
