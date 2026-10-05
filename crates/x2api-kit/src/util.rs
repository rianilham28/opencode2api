//! Small shared utilities. Leaf module: std + deps only.

use std::fmt::Write as _;

/// The full `Display` + `source()` chain of an error, `"a: b: c"`.
/// reqwest's top-level message is just "error sending request"; the chain is
/// what makes a failure diagnosable in logs.
pub fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut s = err.to_string();
    let mut src = err.source();
    while let Some(e) = src {
        let _ = write!(s, ": {e}");
        src = e.source();
    }
    s
}

/// Char-boundary-safe truncation to `max` characters, adding an ellipsis.
/// Never panics on multibyte content (a hard requirement: vendor bodies are
/// frequently UTF-8 with emoji/CJK and we truncate them for logs and relays).
pub fn truncate_chars(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((idx, _)) => {
            let mut s = String::with_capacity(idx + 3);
            s.push_str(&text[..idx]);
            s.push('…');
            s
        }
        None => text.to_string(),
    }
}

/// Seconds since the Unix epoch (OpenAI's `created` field).
pub fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// Monotonic-ish unique id seed: `(millis << 12) | counter` fed through one
/// splitmix64 round, so ids are well-distributed without `rand` or a syscall
/// per request. Counter guarantees uniqueness within the process; the clock
/// mix guarantees uniqueness across restarts *of the same seed space*
/// (documented limitation: two processes, same counter start, within the same
/// millisecond, can collide — fine for request ids, not for credentials).
pub fn next_request_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default();
    let seed = (nanos << 12) | (COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFFF);
    crate::backoff::splitmix64(seed)
}

/// The id one request is known by in THIS process: the caller's own
/// `x-request-id` when it parses as an unsigned integer (a client that
/// already runs a correlation spine gets its numbers reused), otherwise a
/// fresh [`next_request_id`].
///
/// It lives in the shared crate because THREE surfaces must agree on one
/// value: the request span (which is what attributes every log line the
/// request produces), the handler's `CallContext::request_id`, and the retry
/// backoff seed. A second mint anywhere would put the log line and the
/// provider call under different ids — the exact failure this function
/// exists to make impossible. `stamp_request_id` in the server writes the
/// value into the request headers before routing, so every reader below the
/// middleware sees the same one.
pub fn request_id_of(headers: &http::HeaderMap) -> u64 {
    headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        // Zero is rejected for the same reason a mint must never be zero, with
        // one more teeth: this branch reads CALLER input, so `x-request-id: 0`
        // would let any client collapse all of its traffic onto one key — and
        // that key also seeds the minted message ids (`x2api-{id:016x}`) and the
        // retry-jitter seed.
        .filter(|id| *id != 0)
        .unwrap_or_else(next_request_id)
}

/// Compare secrets without leaking equality position via timing. The length
/// side channel is not removed (public constants here are same-length by
/// construction; hashes pre-compare when lengths differ).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let n = a.len().max(b.len());
    let mut diff = a.len() ^ b.len();
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= (x ^ y) as usize;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_is_char_safe() {
        let s = "日本語のテキストです";
        let t = truncate_chars(s, 5);
        assert_eq!(t, "日本語のテ…");
        assert_eq!(truncate_chars("abc", 10), "abc");
    }

    #[test]
    fn request_ids_are_unique() {
        let a = next_request_id();
        let b = next_request_id();
        assert_ne!(a, b);
    }

    #[test]
    fn constant_time_eq_basics() {
        assert!(constant_time_eq(b"token", b"token"));
        assert!(!constant_time_eq(b"token", b"toke"));
        assert!(!constant_time_eq(b"", b"x"));
    }

    #[test]
    fn error_chain_walks_sources() {
        #[derive(Debug)]
        struct Inner;
        impl std::fmt::Display for Inner {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("inner")
            }
        }
        impl std::error::Error for Inner {}
        #[derive(Debug)]
        struct Outer(Inner);
        impl std::fmt::Display for Outer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("outer")
            }
        }
        impl std::error::Error for Outer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        assert_eq!(error_chain(&Outer(Inner)), "outer: inner");
    }

    #[test]
    fn request_id_of_reuses_a_usable_client_id_and_mints_otherwise() {
        let mut h = http::HeaderMap::new();
        let minted = request_id_of(&h);
        assert_ne!(request_id_of(&h), minted, "no header means a fresh id");

        h.insert("x-request-id", http::HeaderValue::from_static("4242"));
        assert_eq!(request_id_of(&h), 4242, "a numeric client id is reused");

        // Garbage ids are the common real case (a UUID from an edge proxy):
        // mint rather than reject the request.
        h.insert(
            "x-request-id",
            http::HeaderValue::from_static("8f2c-uuid-ish"),
        );
        assert_ne!(request_id_of(&h), 0);

        // And zero specifically: this branch reads CALLER input, so accepting 0
        // would let one client collapse all of its traffic onto one correlation
        // key — the same key that seeds the minted message ids and the jitter.
        h.insert("x-request-id", http::HeaderValue::from_static("0"));
        let fresh = request_id_of(&h);
        assert_ne!(fresh, 0, "a caller may not pin the key");
        assert_ne!(fresh, minted, "a rejected 0 is replaced by a real mint");
    }
}
