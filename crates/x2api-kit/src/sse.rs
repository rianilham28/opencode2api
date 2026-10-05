//! SSE framing: decode an upstream byte stream into events, frame events for
//! the downstream body. Byte-level on purpose — no `String::from_utf8_lossy`
//! on partial chunks (it would corrupt multi-byte characters split across a
//! chunk boundary).
//!
//! Records end at a blank line; `\n`/`\r\n` line endings mix freely; multiple
//! `data:` lines concatenate with `\n`; and `event:` names are carried with the
//! record they terminate. `id:` and `retry:` are deliberately discarded. The
//! OpenAI-shaped service provider currently ignores event names, so an
//! Anthropic-native provider must consume the name channel for dispatch.

use bytes::{BufMut, Bytes, BytesMut};
use memchr::memchr;

/// One decoded SSE event.
///
/// `Data` is a slice OF THE DECODER'S OWN BUFFER, not a copy: single-line
/// records — every OpenAI-shaped frame — reach the caller without the payload
/// being moved even once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SseEvent {
    /// Joined `data:` payload bytes (no trailing newline) and the record's
    /// event name, if one was declared.
    Data { event: Option<Bytes>, data: Bytes },
    /// A `data: [DONE]` sentinel was seen.
    Done,
}

/// A malformed or overlong record could not be decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SseDecodeError {
    message: &'static str,
}

impl SseDecodeError {
    /// Maximum number of bytes retained while waiting for a record boundary.
    pub const RECORD_TOO_LARGE: Self = Self {
        message: "SSE record exceeds size limit",
    };

    pub const fn message(self) -> &'static str {
        self.message
    }
}

impl std::fmt::Display for SseDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message)
    }
}

impl std::error::Error for SseDecodeError {}

/// Incremental decoder. Feed raw chunks; drain events.
#[derive(Default)]
pub struct SseDecoder {
    buf: BytesMut,
    /// `data:` payload lines of the record currently being assembled.
    parts: Vec<Bytes>,
    event: Option<Bytes>,
    max_record: Option<usize>,
    record_len: usize,
    /// True once [DONE] was emitted: later records are still parsed (some
    /// vendors append trailing bookkeeping) but the caller decides whether to
    /// care; the server stops relaying past Done.
    done_emitted: bool,
}

impl SseDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a decoder that rejects records whose raw bytes exceed
    /// `max_record` before their terminating blank line.
    pub fn with_max_record(max_record: usize) -> Self {
        Self {
            max_record: Some(max_record),
            ..Self::default()
        }
    }

    /// Consume a chunk, returning whole events found in it.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<SseEvent>, SseDecodeError> {
        let mut out = Vec::new();
        self.push_into(chunk, &mut out)?;
        Ok(out)
    }

    /// `push` into a caller-owned sink. The relay reuses one `Vec` for the
    /// life of a stream, so a frame costs no allocation for its own event.
    pub fn push_into(
        &mut self,
        chunk: &[u8],
        out: &mut Vec<SseEvent>,
    ) -> Result<(), SseDecodeError> {
        if self.max_record.is_none() {
            self.buf.extend_from_slice(chunk);
            while let Some(nl) = memchr(b'\n', &self.buf) {
                let mut line = self.buf.split_to(nl + 1);
                line.truncate(line.len() - 1);
                if line.last() == Some(&b'\r') {
                    line.truncate(line.len() - 1);
                }
                self.handle_line(line, out);
            }
            return Ok(());
        }

        let max = self.max_record.expect("bounded branch");
        let mut rest = chunk;
        while !rest.is_empty() {
            let line_len = memchr(b'\n', rest).map_or(rest.len(), |at| at + 1);
            let Some(retained) = self.record_len.checked_add(line_len) else {
                return Err(self.reject_too_large());
            };
            if retained > max {
                return Err(self.reject_too_large());
            }
            self.record_len = retained;
            self.buf.extend_from_slice(&rest[..line_len]);
            rest = &rest[line_len..];
            if memchr(b'\n', &self.buf).is_some() {
                let mut line = self.buf.split();
                line.truncate(line.len() - 1);
                if line.last() == Some(&b'\r') {
                    line.truncate(line.len() - 1);
                }
                self.handle_line(line, out);
            }
        }
        Ok(())
    }

    fn reject_too_large(&mut self) -> SseDecodeError {
        self.buf = BytesMut::new();
        self.parts.clear();
        self.parts.shrink_to_fit();
        self.event = None;
        self.record_len = 0;
        SseDecodeError::RECORD_TOO_LARGE
    }

    #[cfg(test)]
    fn retained_bytes(&self) -> usize {
        self.record_len
    }

    /// EOF: flush a record that was not terminated by a blank line. The server
    /// treats a flushed partial as *truncated* unless it completes with
    /// `[DONE]` semantics of its own.
    pub fn finish(&mut self) -> Vec<SseEvent> {
        let mut out = Vec::new();
        if !self.buf.is_empty() {
            let line = self.buf.split();
            self.handle_line(line, &mut out);
        }
        // A final unterminated record: emit whatever data lines accumulated.
        self.flush_record(&mut out);
        out
    }

    pub fn done_seen(&self) -> bool {
        self.done_emitted
    }

    /// Takes the line BY VALUE: splitting the payload off it and freezing
    /// hands the caller a view of the buffer already allocated for the read,
    /// instead of a fresh `Vec` per line.
    fn handle_line(&mut self, mut line: BytesMut, out: &mut Vec<SseEvent>) {
        if line.is_empty() {
            self.flush_record(out);
            return;
        }
        if line.starts_with(b"event:") {
            let start = if line.get(6) == Some(&b' ') { 7 } else { 6 };
            self.event = match line.split_off(start).freeze() {
                bytes if bytes.is_empty() => None,
                bytes => Some(bytes),
            };
            return;
        }
        // A colon-prefixed line is a comment; id and retry are deliberately
        // discarded because neither affects the payload carried by this API.
        if line.starts_with(b"data:") {
            let start = if line.get(5) == Some(&b' ') { 6 } else { 5 };
            let payload = line.split_off(start);
            if payload[..] == b"[DONE]"[..] {
                // [DONE] always ends the current record first.
                self.flush_record(out);
                self.done_emitted = true;
                out.push(SseEvent::Done);
                return;
            }
            self.parts.push(payload.freeze());
        }
    }

    fn flush_record(&mut self, out: &mut Vec<SseEvent>) {
        self.record_len = 0;
        let event = self.event.take();
        let data = match self.parts.len() {
            0 => return,
            // The overwhelmingly common shape: one `data:` line per record,
            // which travels on as the slice it already is.
            1 => self.parts.pop().unwrap(),
            _ => {
                let len: usize =
                    self.parts.iter().map(Bytes::len).sum::<usize>() + self.parts.len() - 1;
                let mut joined = BytesMut::with_capacity(len);
                for (i, p) in self.parts.drain(..).enumerate() {
                    if i > 0 {
                        joined.put_u8(b'\n');
                    }
                    joined.put_slice(&p);
                }
                joined.freeze()
            }
        };
        out.push(SseEvent::Data { event, data });
    }
}

/// Append a `data: <payload>\n\n` frame.
pub fn append_data_frame(dst: &mut BytesMut, payload: &[u8]) {
    dst.put_slice(b"data: ");
    dst.put_slice(payload);
    dst.put_slice(b"\n\n");
}

/// Append a named-event frame (`event: <name>\ndata: <payload>\n\n`) — the
/// Anthropic wire needs the `event:` field.
pub fn append_event_frame(dst: &mut BytesMut, event: &str, payload: &[u8]) {
    dst.put_slice(b"event: ");
    dst.put_slice(event.as_bytes());
    dst.put_slice(b"\ndata: ");
    dst.put_slice(payload);
    dst.put_slice(b"\n\n");
}

/// Where a bridge sends the events it produces.
///
/// The bridges must not write SSE bytes (framing is the server's job) and the
/// server must not know a dialect's event shapes — so a bridge hands typed
/// payloads to this sink and something else decides what they become. The
/// streaming server passes a `BytesMut` and gets frames; a caller that wants
/// documents passes a collector and gets `Value`s. One state machine either
/// way, which is the point: a second rendering path is a second thing to keep
/// in sync.
pub trait EventSink {
    /// `name: None` emits an anonymous `data:` event.
    fn event<T: serde::Serialize + ?Sized>(&mut self, name: Option<&'static str>, payload: &T);
}

/// The server's sink: events become frames, in place.
impl EventSink for BytesMut {
    fn event<T: serde::Serialize + ?Sized>(&mut self, name: Option<&'static str>, payload: &T) {
        append_event(self, name, payload);
    }
}

/// Frame a value that serializes to the event's payload, writing it INTO the
/// frame buffer — no payload `Vec` in between. On a serialization failure the
/// half-written payload is rolled back and the neutral `{}` takes its place,
/// so a bad value can never leave a malformed record on the wire.
pub fn append_event<T: serde::Serialize + ?Sized>(
    dst: &mut BytesMut,
    event: Option<&str>,
    value: &T,
) {
    match event {
        Some(name) => {
            dst.put_slice(b"event: ");
            dst.put_slice(name.as_bytes());
            dst.put_slice(b"\ndata: ");
        }
        None => dst.put_slice(b"data: "),
    }
    let mark = dst.len();
    let mut writer = dst.writer();
    if serde_json::to_writer(&mut writer, value).is_err() {
        let dst = writer.into_inner();
        dst.truncate(mark);
        dst.put_slice(b"{}");
    }
    dst.put_slice(b"\n\n");
}

/// The terminal sentinel for OpenAI-format streams.
pub const DONE_FRAME: &[u8] = b"data: [DONE]\n\n";
/// Comment line, safe to interleave between records (never inside one).
pub const KEEPALIVE_FRAME: &[u8] = b": keepalive\n\n";

/// Freeze a scratch buffer into an owned frame batch, resetting it for reuse
/// (one BytesMut across all frames: `split()` keeps the amortized cost at
/// one allocation per emitted batch).
pub fn take_batch(buf: &mut BytesMut) -> Option<Bytes> {
    if buf.is_empty() {
        return None;
    }
    Some(buf.split().freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(v: &str) -> SseEvent {
        SseEvent::Data {
            event: None,
            data: Bytes::copy_from_slice(v.as_bytes()),
        }
    }

    fn named(event: &str, v: &str) -> SseEvent {
        SseEvent::Data {
            event: Some(Bytes::copy_from_slice(event.as_bytes())),
            data: Bytes::copy_from_slice(v.as_bytes()),
        }
    }

    #[test]
    fn frames_split_across_chunks_are_reassembled() {
        let mut d = SseDecoder::new();
        let mut out = Vec::new();
        out.extend(d.push(b"data: {\"a\"").expect("unbounded decoder"));
        assert!(out.is_empty());
        out.extend(d.push(b":1}\n\ndata: two\n").expect("unbounded decoder"));
        assert_eq!(out, vec![data("{\"a\":1}")]);
        out.extend(d.push(b"\n").expect("unbounded decoder"));
        assert_eq!(out, vec![data("{\"a\":1}"), data("two")]);
    }

    #[test]
    fn crlf_and_multiline_data_follow_the_spec() {
        let mut d = SseDecoder::new();
        let evs = d
            .push(b"event: foo\r\ndata: l1\r\ndata: l2\r\n\r\n")
            .expect("unbounded decoder");
        assert_eq!(evs, vec![named("foo", "l1\nl2")]);
    }

    #[test]
    fn done_terminates_and_is_flagged() {
        let mut d = SseDecoder::new();
        let evs = d
            .push(b"data: x\n\ndata: [DONE]\n\ndata: trailing-bookkeeping\n\n")
            .expect("unbounded decoder");
        assert_eq!(
            evs,
            vec![data("x"), SseEvent::Done, data("trailing-bookkeeping")]
        );
        assert!(d.done_seen());
    }

    #[test]
    fn comments_are_not_payloads() {
        let mut d = SseDecoder::new();
        let evs = d
            .push(b": keepalive\n\ndata: x\n\n")
            .expect("unbounded decoder");
        assert_eq!(evs, vec![data("x")]);
    }

    #[test]
    fn eof_flushes_partial_record() {
        let mut d = SseDecoder::new();
        assert!(d.push(b"data: half").expect("unbounded decoder").is_empty());
        assert_eq!(d.finish(), vec![data("half")]);
    }

    #[test]
    fn record_ceiling_rejects_a_newline_free_record() {
        let mut d = SseDecoder::with_max_record(1024);
        let record = vec![b'x'; 2 * 1024 * 1024];
        for chunk in record.chunks(512) {
            if d.push(chunk) == Err(SseDecodeError::RECORD_TOO_LARGE) {
                assert_eq!(
                    d.retained_bytes(),
                    0,
                    "the rejected bytes must not be retained"
                );
                return;
            }
        }
        panic!("2 MiB without a newline must exceed the 1 KiB ceiling");
    }

    #[test]
    fn event_names_are_record_scoped_and_replaced() {
        let mut d = SseDecoder::new();
        let events = d
            .push(b"event: first\ndata: one\n\nevent: second\ndata: two\n\ndata: three\n\n")
            .expect("unbounded decoder");
        assert_eq!(
            events,
            vec![named("first", "one"), named("second", "two"), data("three")]
        );
    }

    #[test]
    fn record_ceiling_includes_many_data_lines() {
        let mut d = SseDecoder::with_max_record(1024);
        let line = b"data: x\n";
        let mut out = Vec::new();
        let mut error = None;
        for _ in 0..256 {
            if let Err(err) = d.push_into(line, &mut out) {
                error = Some(err);
                break;
            }
        }
        assert_eq!(error, Some(SseDecodeError::RECORD_TOO_LARGE));
        assert_eq!(d.retained_bytes(), 0);
    }

    #[test]
    fn overflow_keeps_completed_events_from_the_same_push() {
        let mut d = SseDecoder::with_max_record(64);
        let mut out = Vec::new();
        let result = d.push_into(b"data: first\n\ndata: 0123456789012345678901234567890123456789012345678901234567890123456789\n", &mut out);
        assert_eq!(result, Err(SseDecodeError::RECORD_TOO_LARGE));
        assert_eq!(out, vec![data("first")]);
        assert_eq!(d.retained_bytes(), 0);
    }

    #[test]
    fn event_only_record_resets_the_next_record_name() {
        let mut d = SseDecoder::new();
        let events = d
            .push(b"event: stale\n\nevent: current\ndata: payload\n\ndata: next\n\n")
            .expect("unbounded decoder");
        assert_eq!(events, vec![named("current", "payload"), data("next")]);
    }

    #[test]
    fn empty_event_name_resets_to_none() {
        let mut d = SseDecoder::new();
        let events = d
            .push(b"event: stale\ndata: one\n\nevent:\ndata: two\n\n")
            .expect("unbounded decoder");
        assert_eq!(events, vec![named("stale", "one"), data("two")]);
    }

    /// A corpus that exercises every rule the decoder claims: multi-line
    /// `data:`, mixed line endings, comments, the sentinel, a trailing
    /// bookkeeping record after it, and a multi-byte character whose UTF-8
    /// bytes a split can land inside.
    const CORPUS: &[u8] = concat!(
        ": keepalive\n\n",
        "event: named\r\ndata: l1\r\ndata: l2\r\n\r\n",
        "data: {\"text\":\"halo — dunia 🚀\"}\n\n",
        "id: 7\nretry: 100\ndata: after-fields\n\n",
        "data: [DONE]\n\n",
        "data: {\"cost\":0.1}\n\n",
    )
    .as_bytes();

    fn decode_in_pieces(input: &[u8], cuts: &[usize]) -> Vec<SseEvent> {
        let mut d = SseDecoder::new();
        let mut out = Vec::new();
        let mut prev = 0usize;
        for &cut in cuts {
            let cut = cut.min(input.len());
            if cut > prev {
                out.extend(d.push(&input[prev..cut]).expect("unbounded decoder"));
                prev = cut;
            }
        }
        out.extend(d.push(&input[prev..]).expect("unbounded decoder"));
        out.extend(d.finish());
        out
    }

    /// The decoder is the one parser fed bytes we do not control, and a panic
    /// in it is NOT containable: the relay's `catch_unwind` guards the sink,
    /// while a panic inside the provider's stream poll aborts the generator
    /// mid-await. So: every single-byte split of the corpus must decode to
    /// exactly what the unsplit parse produced. This is the case a real
    /// upstream produces constantly and a fixed test fixture never does.
    #[test]
    fn every_split_decodes_identically_to_the_whole() {
        let whole = decode_in_pieces(CORPUS, &[]);
        assert!(whole.len() > 4, "corpus should yield several events");
        for cut in 1..CORPUS.len() {
            assert_eq!(
                decode_in_pieces(CORPUS, &[cut]),
                whole,
                "a boundary at byte {cut} changed the decode"
            );
        }
    }

    /// …and so must many splits at once, including several inside one record.
    /// Offsets come from the same splitmix64 the backoff uses, so a failure
    /// reproduces from the printed seed instead of being a coin toss.
    #[test]
    fn arbitrary_multi_splits_decode_identically() {
        let whole = decode_in_pieces(CORPUS, &[]);
        for seed in 0..400u64 {
            let mut state = crate::backoff::splitmix64(seed | 1);
            let mut cuts: Vec<usize> = (0..6)
                .map(|_| {
                    state = crate::backoff::splitmix64(state);
                    (state as usize) % CORPUS.len()
                })
                .collect();
            cuts.sort_unstable();
            assert_eq!(
                decode_in_pieces(CORPUS, &cuts),
                whole,
                "seed {seed} (cuts {cuts:?}) changed the decode"
            );
        }
    }

    /// A record cut in half at EOF is truncation, not an event — the relay
    /// keys its `truncated` outcome on exactly this.
    #[test]
    fn a_partial_tail_flushes_once_and_only_at_finish() {
        let mut d = SseDecoder::new();
        assert!(
            d.push(b"data: {\"half\":")
                .expect("unbounded decoder")
                .is_empty(),
            "no record yet"
        );
        let flushed = d.finish();
        assert_eq!(flushed.len(), 1, "the partial surfaces exactly once");
        assert!(matches!(&flushed[0], SseEvent::Data { data, .. } if data.ends_with(b":")));
        assert!(d.finish().is_empty(), "and never again");
    }

    #[test]
    fn writers_produce_exact_bytes() {
        let mut b = BytesMut::new();
        append_data_frame(&mut b, b"{\"a\":1}");
        assert_eq!(&b[..], b"data: {\"a\":1}\n\n");
        b.clear();
        append_event_frame(&mut b, "content_block_delta", b"{}");
        assert_eq!(&b[..], b"event: content_block_delta\ndata: {}\n\n");
    }

    #[test]
    fn serializing_writer_matches_the_byte_writer() {
        let value = serde_json::json!({"a": 1, "text": "quote \" and \n newline"});
        let payload = serde_json::to_vec(&value).unwrap();

        let mut direct = BytesMut::new();
        append_event_frame(&mut direct, "delta", &payload);
        let mut written = BytesMut::new();
        append_event(&mut written, Some("delta"), &value);
        assert_eq!(direct, written);

        direct.clear();
        written.clear();
        append_data_frame(&mut direct, &payload);
        append_event(&mut written, None, &value);
        assert_eq!(direct, written);
    }

    /// The rollback contract: a value that fails mid-serialization leaves a
    /// complete, neutral record rather than a truncated one.
    #[test]
    fn failed_serialization_rolls_back_to_a_neutral_payload() {
        struct Boom;
        impl serde::Serialize for Boom {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                use serde::ser::{Error, SerializeMap};
                let mut m = s.serialize_map(Some(1))?;
                m.serialize_entry("half", "written")?;
                Err(S::Error::custom("boom"))
            }
        }
        let mut b = BytesMut::new();
        append_event(&mut b, Some("delta"), &Boom);
        assert_eq!(&b[..], b"event: delta\ndata: {}\n\n");
    }

    /// A relay-shaped stream: a plain record, an OpenAI `choices` record, the
    /// terminal sentinel, a comment line that must stay silent, and a tail the
    /// upstream never terminated. Built once at compile time so every chunking
    /// below feeds identical bytes.
    const RELAY_PAYLOAD: &[u8] = concat!(
        "data: hello\n\n",
        r#"data: {"id":"chatcmpl-9","choices":"#,
        r#"["index":0,"delta":{"content":"hi"},"finish_reason":null}]}"#,
        "\n\n",
        "data: [DONE]\n\n",
        ": keepalive\r\n\r\n",
        r#"data: {"usage":{"prompt_tokens"#,
    )
    .as_bytes();

    const DONE_SENTINEL: &[u8] = b"[DONE]";

    /// Normalise events to strings so a chunking regression prints the events
    /// that diverged instead of a byte-slice diff the reader has to decode by
    /// hand.
    fn rendered(events: &[SseEvent]) -> Vec<String> {
        events
            .iter()
            .map(|e| match e {
                SseEvent::Data { data: p, .. } => format!("data:{}", String::from_utf8_lossy(p)),
                SseEvent::Done => "done".to_string(),
            })
            .collect()
    }

    /// The stream the unsplit payload must produce, spelled out so the
    /// invariance loop cannot pass vacuously on a decoder that emits nothing at
    /// all. The payloads are read from the corpus's own `data:` fields, so the
    /// reference can never drift from the bytes it is judging — while the fixed
    /// shape below still asserts the comment line stays silent and the sentinel
    /// record becomes `Done`, not a payload.
    fn relay_expectation() -> Vec<String> {
        const FIELD: &str = "data: ";
        let text = std::str::from_utf8(RELAY_PAYLOAD).expect("payload is ascii");
        let records: Vec<String> = text
            .lines()
            .filter(|l| l.starts_with(FIELD))
            .map(|l| l[FIELD.len()..].to_string())
            .collect();
        assert_eq!(
            records.len(),
            4,
            "corpus: plain record, choices record, sentinel, unterminated tail"
        );
        assert_eq!(records[2], "[DONE]", "the sentinel must stay a sentinel");
        vec![
            format!("data:{}", records[0]),
            format!("data:{}", records[1]),
            "done".to_string(),
            format!("data:{}", records[3]),
        ]
    }

    fn offsets_of(hay: &[u8], needle: &[u8]) -> Vec<usize> {
        hay.windows(needle.len())
            .enumerate()
            .filter(|(_, w)| *w == needle)
            .map(|(i, _)| i)
            .collect()
    }

    /// Chunk-invariance is the decoder's whole contract: a network read can
    /// break a stream at ANY byte, and a boundary that changes the event
    /// sequence is a corrupted answer, not a recoverable error. Exhaustive over
    /// single boundaries plus deterministic multi-boundaries (the payload
    /// divided in thirds from each start), so no offset escapes.
    #[test]
    fn decoding_is_invariant_to_chunk_boundaries() {
        let baseline = rendered(&decode_in_pieces(RELAY_PAYLOAD, &[]));
        assert_eq!(
            baseline,
            relay_expectation(),
            "the unsplit decode is the reference every split is judged against"
        );

        let third = RELAY_PAYLOAD.len() / 3;
        let len = RELAY_PAYLOAD.len();
        for i in 0..=len {
            let j = (i + third).min(len);
            let k = (j + third).min(len);
            assert_eq!(
                rendered(&decode_in_pieces(RELAY_PAYLOAD, &[i])),
                baseline,
                "a boundary at byte {i} changed the decode"
            );
            assert_eq!(
                rendered(&decode_in_pieces(RELAY_PAYLOAD, &[i, j])),
                baseline,
                "boundaries at {i} and {j} changed the decode"
            );
            assert_eq!(
                rendered(&decode_in_pieces(RELAY_PAYLOAD, &[i, j, k])),
                baseline,
                "boundaries at {i}, {j} and {k} changed the decode"
            );
        }
    }

    /// The sentinel is only recognised once all six of its bytes have arrived:
    /// a boundary inside `[DONE]` must not flag the stream as finished and must
    /// not leak the fragment as a data payload. A decoder that matched on a
    /// prefix would end the answer after the first half-token.
    #[test]
    fn a_split_inside_the_done_sentinel_is_not_a_done() {
        let at = offsets_of(RELAY_PAYLOAD, DONE_SENTINEL)
            .pop()
            .expect("payload contains the sentinel");
        let expect = relay_expectation();
        for off in 1..DONE_SENTINEL.len() {
            let cut = at + off;
            let mut d = SseDecoder::new();
            let head = d.push(&RELAY_PAYLOAD[..cut]).expect("unbounded decoder");
            assert!(
                head.iter().all(|e| !matches!(e, SseEvent::Done)),
                "a {off}-byte fragment of the sentinel was treated as Done"
            );
            assert!(!d.done_seen(), "done_seen() flipped at byte {cut}");
            assert_eq!(
                rendered(&head),
                expect[..2],
                "the fragment produced no complete record before the token finished"
            );

            let mut tail = d.push(&RELAY_PAYLOAD[cut..]).expect("unbounded decoder");
            tail.extend(d.finish());
            assert_eq!(
                rendered(&tail),
                expect[2..],
                "the completed sentinel at byte {cut} must resume the expected sequence"
            );
            assert!(d.done_seen(), "the finished sentinel went unflagged");
        }
    }

    /// A boundary inside the `data:` field NAME (not the payload) must still
    /// yield the record, and the bytes held in the buffer must never be handed
    /// to the caller as one. Dropping the record would silently lose a token of
    /// the answer rather than error.
    #[test]
    fn a_split_inside_a_data_field_name_does_not_drop_the_record() {
        let expect = relay_expectation();
        let fields = offsets_of(RELAY_PAYLOAD, b"data:");
        assert_eq!(fields.len(), 4, "one `data:` field per record");
        for at in fields {
            for off in 1..b"data:".len() {
                let cut = at + off;
                let mut d = SseDecoder::new();
                let head = d.push(&RELAY_PAYLOAD[..cut]).expect("unbounded decoder");
                // A record surfaces only past its blank line, so what the
                // caller may hold at this point is exactly the records already
                // terminated before `cut` — no early fragment, no duplicate.
                let terminated = String::from_utf8_lossy(&RELAY_PAYLOAD[..cut])
                    .matches("\n\n")
                    .count();
                assert_eq!(
                    rendered(&head),
                    expect[..terminated],
                    "a boundary inside `data:` at byte {cut} delivered the wrong prefix"
                );
                assert_eq!(
                    d.done_seen(),
                    expect[..terminated].iter().any(|e| e == "done"),
                    "done_seen() at byte {cut} disagrees with the events emitted"
                );
                let mut tail = d.push(&RELAY_PAYLOAD[cut..]).expect("unbounded decoder");
                tail.extend(d.finish());
                let mut all = rendered(&head);
                all.extend(rendered(&tail));
                assert_eq!(
                    all, expect,
                    "a boundary inside `data:` at byte {cut} changed the decode"
                );
            }
        }
    }
}
