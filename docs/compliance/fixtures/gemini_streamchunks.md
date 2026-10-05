# gemini_streamchunks.sse

**Hand-authored capture of the documented `:streamGenerateContent?alt=sse`
frame sequence** — anonymous `data:` records, each a full
`GenerateContentResponse`; NO `[DONE]` sentinel on this dialect: the terminal
frame carrying `finishReason` ends the stream. `createTime`/`responseId` are
the vendor's own per-chunk identity fields. NOT lifted from a live vendor.

Pins:

- non-terminal frames carry no `finishReason`; exactly ONE frame does, and it
  is last — a client walking `candidates[0]` relies on that terminator;
- `functionCall.args` is an OBJECT on this wire (the chat wire's JSON-string
  spelling is converted at the fold, and `GeminiStream::finish` re-parses
  accumulated argument fragments into an object — a vendor that started
  streaming `args` as a string would be caught by the parity assert);
- `usageMetadata.totalTokenCount` == prompt + candidates counts, present only
  on the terminal frame;
- the bridge's `GeminiStream` driven by the equivalent IR chunks projects the
  same text, function call (name + args object + id), `finishReason` and usage
  — and `fail()` emits a Google `{error:{code,message,status}}` frame with NO
  `finishReason` anywhere: a cut Gemini stream cannot read as a finished turn.
