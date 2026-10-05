# openai_chat_buffered.json

**Hand-authored capture of the documented buffered `chat.completion` shape**
(`docs/compliance/openai.md`; content and a co-occurring `tool_calls` array on
one message is the shape OpenAI emits when the model speaks and calls at
once). NOT lifted from a live vendor.

Pins:

- `Completion::from_openai` reads text, BOTH tool calls, `finish_reason`
  `"tool_calls"`, and `usage.prompt_tokens`/`completion_tokens` out of the
  buffered document — a vendor that renamed `message.tool_calls` or nested
  `usage` per-choice fails here first;
- `chat::completion_to_wire` keeps it VERBATIM (the `raw` fast path): the
  fields a chat client reads (`system_fingerprint`, `total_tokens`) must
  survive untouched, because re-shaping them is precisely what the relay
  promises not to do.
