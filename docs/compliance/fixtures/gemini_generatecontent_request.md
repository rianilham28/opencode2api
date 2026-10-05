# gemini_generatecontent_request.json

**Hand-authored capture of the documented `:generateContent` body shape**
(camelCase protobuf JSON, `contents[].parts[]`, a separate
`systemInstruction`, `generationConfig`, `tools[].functionDeclarations`,
`toolConfig.functionCallingConfig` — the grammar recorded in the Gemini
dialect module header). NOT lifted from a live vendor. The model name lives in
the URL path, not in this body.

Pins:

- the Gemini fold accepts it (`GeminiRequest` → `to_chat_request`, no error)
  with system → user → assistant(tool_calls) → tool ordering, the
  `functionCall.args` OBJECT re-spelled as the chat wire's JSON string, and
  the call `id` preserved on both the call and its `functionResponse`;
- `generationConfig` sampling lands on OpenAI names (`temperature`,
  `maxOutputTokens`→`max_tokens`, `stopSequences`→`stop`);
- `functionDeclarations` nest into `tools[].function`, and
  `functionCallingConfig.mode:"AUTO"` folds to `tool_choice:"auto"`.
