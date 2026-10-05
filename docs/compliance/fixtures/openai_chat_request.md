# openai_chat_request.json

**Hand-authored capture of the documented `/v1/chat/completions` request
shape** (`docs/compliance/openai.md`, §2; official API reference). NOT lifted
from a live vendor — field names, nesting, and the tool-call/`role:"tool"`
history grammar follow the documented wire, but nobody should mistake this for
a transcript of a real request.

Pins (see `crates/x2api-dialects/tests/vendor_fixtures.rs`):

- the chat fold accepts it verbatim (`parse_request` → IR, no error),
- `tools`/`tool_choice`/`stream_options`/`temperature` ride `extra`
  untouched — the chat dialect's contract is pass-through, and a vendor that
  renames or nests these fields would break the test before it broke a client,
- the assistant `tool_calls` + `tool` reply history survives the fold
  (dropping it would answer from a conversation that never happened).
