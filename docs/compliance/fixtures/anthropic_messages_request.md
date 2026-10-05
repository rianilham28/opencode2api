# anthropic_messages_request.json

**Hand-authored capture of the documented `/v1/messages` request shape**
(`docs/compliance/anthropic.md`; flat `tools[].input_schema`, block-content
`tool_use`/`tool_result`, and `{type:"auto"}` tool choice are the documented
grammar). NOT lifted from a live vendor.

Pins:

- the Anthropic fold accepts it (`AnthropicRequest` → `to_chat_request`, no
  error) and orders the IR system message first, then user → assistant → tool;
- a `tool_use` block's `input` OBJECT becomes the chat wire's JSON **string**
  (the one place the two wires genuinely disagree), with the id preserved;
- `tools`/`tool_choice` land in the chat dialect's nested shape, `max_tokens`
  and `temperature` forward, and Anthropic-only fields the chat wire has no
  spelling for (`metadata`) drop — the documented accepted-and-dropped
  posture, pinned so a rename of `input_schema` fails a test, not a client.
