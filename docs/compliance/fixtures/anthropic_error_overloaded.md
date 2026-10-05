# anthropic_error_overloaded.json

**Hand-authored capture of the documented Anthropic error document** — the
same `{type:"error", error:{type,message}}` envelope the API returns as a
buffered body (HTTP 529 `overloaded_error`) and as the in-stream
`event: error` payload (`docs/compliance/anthropic.md`). NOT lifted from a
live vendor.

Pins:

- `AnthropicStream::error("overloaded_error", "Overloaded")` renders EXACTLY
  this document as its single event payload — the envelope clients parse to
  decide retry-vs-fail;
- the error event is terminal: `finish()` afterwards emits nothing, so no
  `message_stop` can follow an error and a cut stream cannot read as clean;
- the shape survives a stream already carrying text (error arrives
  mid-message, the case a vendor overload actually produces).
