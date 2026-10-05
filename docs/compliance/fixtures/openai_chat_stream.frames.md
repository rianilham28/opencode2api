# openai_chat_stream.frames.jsonl

**Hand-authored capture of the documented `chat.completion.chunk` stream
shape** (`docs/compliance/openai.md`; the `stream_options.include_usage`
usage-only final chunk is OpenAI's documented streaming grammar). Each line is
the RAW payload of one `data:` frame — the `data: ` prefix and blank-line
record separator are SSE framing, not content, and the test re-frames them.
NOT lifted from a live vendor.

Pins:

- the IR chunk decode (`ChatChunk::from_wire`) reads text deltas, the
  `finish_reason` frame, and the trailing usage chunk as expected — a vendor
  that moved `usage` into `choices[]` or renamed `delta.content` fails here;
- identity relay: `CompatStream` must re-emit these exact bytes, so any
  re-serialization drift in the chat dialect is caught;
- `[DONE]` is the terminal sentinel on the clean path and is ABSENT on the
  truncated path (`done_seen()` false, and `fail()`'s error frame carries no
  `[DONE]`).
