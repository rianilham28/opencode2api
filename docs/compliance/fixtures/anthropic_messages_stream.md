# anthropic_messages_stream.sse

**Hand-authored capture of the documented `/v1/messages?stream=true` event
grammar** (`docs/compliance/anthropic.md` §(b): named `event:` lines whose
payload always carries a matching `type`, the
message_start → ping → content_block_start → content_block_delta* →
content_block_stop → message_delta → message_stop chain, and the terminal
`message_delta` owning `stop_reason` + output usage). NOT lifted from a live
vendor.

Pins:

- per record, `event:` == payload `type` — the invariant every Anthropic SDK
  client keys on; a vendor renaming one side fails here;
- the chain has exactly one `message_stop`, last, and `stop_reason`
  `end_turn` with non-null `output_tokens` on `message_delta`;
- the bridge's own `AnthropicStream`, driven by the equivalent IR chunks,
  produces the SAME event-name sequence, accumulated text, and stop_reason —
  so a fold change that drifts from the documented grammar trips the parity
  assert before a Claude client ships into it;
- the failure path (`error()`) ends the stream with `anthropic_error_overloaded.json`
  and NO `message_stop` ever follows.
