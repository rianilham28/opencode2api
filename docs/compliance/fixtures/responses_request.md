# responses_request.json

**Hand-authored capture of the documented `/v1/responses` request shape**
(`docs/compliance/openai.md` §Responses: flat `tools[]`, `input` item array
with `function_call`/`function_call_output` history, `instructions`,
`max_output_tokens`, `reasoning.effort`). NOT lifted from a live vendor.

Pins:

- the Responses fold accepts it (`ResponsesRequest` → `to_chat_request`, no
  error) and emits system → user → assistant(tool_calls) → tool in that order;
- flat `tools[].name/parameters` nest into the chat shape;
  `max_output_tokens`→`max_tokens`, `reasoning.effort`→`reasoning_effort`;
- a `function_call`'s `arguments` is ALREADY a JSON string on this wire, so
  it must arrive at the IR byte-identical (re-encoding it is the drift this
  fold has been burned by before).
