# responses_stream.events.jsonl

**Hand-authored capture of the documented `/v1/responses` SSE event
sequence** — one JSON payload per line, `event:` framing omitted (it mirrors
`type`). The lifecycle grammar is the one the module docs verify against real
captures: `response.created` at `sequence_number` **0**, strictly increasing,
created → in_progress → output_item.added → content_part.added →
output_text.delta* → output_text.done → content_part.done → output_item.done
→ terminal object event; usage appears ONLY on the terminal event; there is
NO `[DONE]` sentinel on this dialect. NOT lifted from a live vendor.

Pins:

- every payload carries `type` + `sequence_number`; numbers are contiguous
  from 0 with no gaps or duplicates (a lifecycle client refuses either);
- exactly one terminal event (`response.completed`/`response.incomplete`/
  `error`), and on the clean capture it is `response.completed` carrying the
  full text + `usage` whose `total_tokens` equals input+output;
- the bridge's `ResponsesStream`, driven by the equivalent IR chunks, emits
  the same (type, sequence_number) ledger and the same final text/usage —
  parity between what we claim to render and what we actually render;
- `fail()` yields exactly one `error` event with NO preamble and no completed
  event ever follows: a cut Responses stream cannot be read as done.
- event types, `sequence_number`, and the terminal `output_text` +
  `usage.total_tokens` — nothing else. This capture is ONE text-only message
  item at `output_index` 0, so it does not pin reasoning/tool item ordering
  (that rule lives in `responses.rs`, keyed to announcement order), and
  request-echo envelope fields (e.g. `parallel_tool_calls`, `temperature`) are
  static placeholders here because the `Completion` IR does not retain the
  inbound request.
