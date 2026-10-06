# omp (oh-my-pi) inbound-dialect audit vs the x2api template

Sources: `omp://models.md`, `omp://providers.md`, `omp://provider-endpoint-constraints.md`, `omp://provider-streaming-internals.md`, `omp://provider-compat-reference.md`, `omp://provider-quirks.md`, `omp://adding-a-provider.md`, `omp://auth-broker-gateway.md`, `omp://non-compaction-retry-policy.md`; live `~/.omp/agent/models.yml` (provider names + `api` styles only, no secrets read out); repo `docs/compliance/*.md` (2026-09-06) + template tree. Read-only; no edits, no cargo.

## 0. The decisive fact: omp ships 9 wire APIs + 1 native transport

`omp://models.md` → *Allowed provider/model `api` values*, verbatim:

```yaml
- openai-completions      # POST {baseUrl}/chat/completions
- openai-responses        # POST {baseUrl}/responses
- openai-codex-responses  # ChatGPT backend /responses (SSE + WebSocket)
- azure-openai-responses  # {base}/responses?api-version=…, api-key header
- anthropic-messages      # POST {base}/v1/messages (?beta=true), X-Api-Key
- bedrock-converse-stream # bedrock-runtime {region} …/converse-stream, SigV4, AWS eventstream
- google-generative-ai    # POST /v1beta/models/{model}:streamGenerateContent?alt=sse, x-goog-api-key
- google-gemini-cli       # POST /v1internal:streamGenerateContent?alt=sse (Cloud Code Assist)
- google-vertex           # {loc}-aiplatform …/models/{id}:streamGenerateContent?alt=sse (ADC)
```

Plus, verbatim from the same section: `transport: pi-native` → “every model under that provider is sent to an `omp auth-gateway` compatible `baseUrl` via `POST /v1/pi/stream`; `apiKey` is the gateway bearer”. Extension-registered custom APIs are also dispatchable (`adding-a-provider.md`: “custom stream handler registration for new API IDs”); “Adding a *new wire protocol* (a new `KnownApi`) … also touches `stream.ts` dispatch, `api-registry.ts`, and the catalog `types.ts`.”

Strong corroboration that three dialects is the *canonical* server surface: omp's own auth-gateway (`omp://auth-broker-gateway.md` → Endpoints) exposes exactly `POST /v1/chat/completions`, `POST /v1/messages`, `POST /v1/responses`, `POST /v1/pi/stream`, `GET /v1/models`, `GET /v1/usage`, `GET /healthz` — and “re-encodes the result to the inbound format (SSE for streamed responses)”. The template's route table (`crates/opencode2api-server/src/router.rs:38-43`) is the same three minus pi-native.

## 1. What the user's live `models.yml` actually asks for (names + api styles only)

| Provider id | `api` | baseUrl shape | Other provider fields |
|---|---|---|---|
| `axon-compat` | `openai-completions` | `http://<tailscale-ip>:8822/v1` | `auth: apiKey` |
| `axon-responses` | `openai-responses` | same host `…/v1` | `auth: apiKey` |
| `axon-gemini` | `google-generative-ai` | same host `…/v1beta` | `auth: apiKey` |
| `glm-bridge` | `openai-completions` | `http://localhost:3001/v1` | `auth: apiKey` |

So in production omp is pointed at **one** gateway host through **three different dialects** (chat, responses, gemini-native) — i.e. exactly the multi-dialect-per-host shape the template is built for, with a fourth (Gemini native) the template does not serve. Per-model `compat`/`thinking`/`tokenizer` metadata that real omp traffic carries on these wires: `supportsReasoningEffort`, `reasoningEffortMap`, `reasoningContentField: reasoning_content`, `requiresReasoningContentForToolCalls`, `thinkingFormat: zai`, `supportsToolChoice: false` + `disableReasoningOnToolChoice`, `supportsStore: false`, `thinking: {mode: effort, efforts, defaultLevel, requiresEffort}`, `tokenizer: qwen3`, and full `cost: {input, output, cacheRead, cacheWrite}`.

Local engines are also wire-shaped (no models.yml needed): implicit `ollama` and `llama.cpp` → `api: openai-responses` on `/v1`; `lm-studio` → `openai-completions`; `vllm` → OpenAI-compat; native Ollama discovery uses `/api/tags` + `POST /api/show`, “not OpenAI `/v1/models`” (`models.md` → Implicit discovery; `provider-quirks.md` → Ollama).

## 2. Matrix A — can omp use the template as its SERVER, per omp `api`?

Cells: **NATIVE** (route speaks the dialect) · **IR-BRIDGE** (folded to the chat IR) · **PROVIDER-CRATE** (needs vendor wire work beyond chat-shape) · **GAP** (why).

| omp `api` (evidence) | Template route | Cell | Why / what breaks |
|---|---|---|---|
| `openai-completions` (`provider-quirks` §OpenAI Chat Completions; workhorse for dozens of gateways) | `POST /v1/chat/completions` | **NATIVE** | Identity with the IR; `ChatRequest.extra` is `#[serde(flatten)]` so `tools`, `tool_choice`, `reasoning_effort`, `thinking`, `chat_template_kwargs`, `stream_options` ride unchanged; `ChatChunk.raw`/`Completion.raw` are relayed verbatim (`opencode2api-dialects/src/chat.rs`) → `tool_calls`, `reasoning_content`, vendor extras survive. Agent-viable today. |
| `openai-responses` (`provider-quirks` §OpenAI Responses; “stateful `/v1/responses` … SSE”) | `POST /v1/responses` | **IR-BRIDGE (agent-capable)** | Bridge emits spec-correct named events + `sequence_number` + `response.completed`/`incomplete` + `error` frame, folds `instructions`/`input`/flat `tools`/`tool_choice`/`text.format`/`reasoning.effort`/`max_output_tokens`, AND the tool loop: `function_call`/`function_call_output` items fold both ways and the stream emits `response.output_item.added{type:"function_call"}` + `response.function_call_arguments.delta`/`.done`, so omp's agent loop gets its calls and its results back to the model. User-turn `input_image`/`input_file` fold to the chat media parts (a `file_id` passes through — on this dialect the id IS the upstream's namespace) and a `refusal` part keeps its text. `reasoning`/`item_reference` items still refuse (no IR reasoning field). `previous_response_id`/`store` accepted-and-dropped (harmless: omp only chains on official OpenAI hosts, `PI_OPENAI_STATEFUL` + `hostMatchesUrl`); the state itself is gap G3. Fold lives in `opencode2api-dialects/src/responses.rs`. |
| `anthropic-messages` (`provider-quirks` §Anthropic Messages: “HTTPS POST to `/v1/messages` (or `/v1/messages?beta=true`)”) | `POST /v1/messages` | **IR-BRIDGE (agent-capable)** | Stream/event grammar is faithful (`docs/compliance/anthropic.md` §(b) “Our bridge's current contract”), and so is the agent loop now: `tools`/`tool_choice` and `tool_use`/`tool_result` fold both ways, `stop_reason: "tool_use"` is reported only from blocks the bridge actually EMITTED (the item-1 worst case is gone, pinned by a test), and user-turn `image`/`document` blocks fold to the chat media parts in the client's order. One grammar qualification: tool argument frames are NOT live `input_json_delta` — every streamed tool fragment is held and written by `AnthropicStream::finish` in one terminal burst, one block at a time, refusal first, then each held call complete (start, its `input_json_delta` fragments, stop). Live deltas were dropped because Anthropic never has two content blocks open: a refusal arriving mid-call could not be interleaved without breaking that one-open-block contract, and holding is what also keeps one call to exactly one `tool_use` block, so buffered parity wins. `AnthropicStream::error` drops the pending fragments. The gate accepts the SDK's `x-api-key` (item 8 is gone — a stock Claude client is no longer 401'd). Still true: `thinking` refused (no IR reasoning field, item 2), `metadata` dropped, non-`assistant` roles → `user`, `usage` is input/output counters only (item 6), `cache_control` dropped (chat has no counterpart, so it is said-and-dropped rather than refused), and `/v1/models` answers OpenAI-shaped (item 7). Fold lives in `opencode2api-dialects/src/anthropic.rs`; the hold is `crates/opencode2api-dialects/src/anthropic.rs::AnthropicStream::on_chunk_into` / `crates/opencode2api-dialects/src/anthropic.rs::AnthropicStream::on_tool_delta` / `crates/opencode2api-dialects/src/anthropic.rs::AnthropicStream::finish` / `crates/opencode2api-dialects/src/anthropic.rs::AnthropicStream::error`. |
| `openai-codex-responses` (`provider-quirks` §OpenAI Codex) | — | **GAP / PROVIDER-CRATE upstream** | Not “Responses with a different URL”: ChatGPT OAuth, `ChatGPT-Account-Id`, `x-codex-turn-state`, `responsesLite`, websocket-vs-SSE, Harmony control tokens, `/wham/usage`, zstd. Template has neither `/responses`-at-backend-api semantics nor OAuth/websocket/SigV4 machinery. omp's transport *is* Responses-shaped, so a template **provider crate** that holds ChatGPT OAuth could expose codex models through `/v1/responses` — the IR prerequisite is met (`tool_calls` are typed and folded), so nothing on the fold side blocks it. |
| `azure-openai-responses` (`provider-quirks` §Azure OpenAI) | `POST /v1/responses` (partial) | **IR-BRIDGE (dial in upstream) / GAP inbound** | Inbound: omp-as-Azure-client needs the `api-key` header (“never `Authorization: Bearer`”) and `${baseUrl}/responses?api-version=`. The gate is one function accepting `Authorization: Bearer`, `x-api-key`, `x-goog-api-key` or `?key=` — Azure's `api-key` spelling is NOT among them, and the router has no `/responses` base-URL flexibility for the query form, so point omp at the template as plain `openai-responses`, not `azure-openai-responses`. Upstream: relay-compatible if the vendor speaks Responses at a `/v1`-suffixed base. `strictResponsesPairing` (Azure/Copilot) is a fold-away problem (bridge can't pair calls it drops). |
| `google-generative-ai` (`provider-quirks` §Google Gemini) | `POST /v1beta/models/{model}:generateContent` and its `:streamGenerateContent` twin | **IR-BRIDGE (delivered)** | `dialects::gemini` + a wildcard path route (`/v1beta/models/{*tail}`, matchit 0.8 brace syntax). The model id and stream verb arrive in the PATH, so `build_router` needed the path-param route this row called for; `systemInstruction`/`functionDeclarations`/`parametersJsonSchema`/`usageMetadata`/`functionCall` all fold, and user-turn `inlineData` folds to the chat media parts. This client's exact URL — `:streamGenerateContent?alt=sse` with `x-goog-api-key` — is served: the stream verb REQUIRES `alt=sse` (the fold frames SSE, never Google's JSON-array default), `x-goog-api-key` and `?key=` both satisfy the gate, and `GET /v1beta/models[/{model}]` answers `listModels`/`getModel`. Still refused: `thinkingConfig`, built-in server tools, audio/video, `fileData`. |
| `google-vertex` (`provider-quirks` §Google Vertex AI) | — | **GAP (PROVIDER-CRATE upstream)** | ADC/OAuth ladder + regional `aiplatform` host interpolation + `:streamGenerateContent`, and `functionCall.id`/`functionResponse.id` must be **stripped** for Vertex (400 INVALID_ARGUMENT). No chat-shape fold. |
| `google-gemini-cli` / `google-antigravity` (`provider-quirks` §Google Gemini CLI / Antigravity) | — | **GAP (provider crate)** | `POST /v1internal:streamGenerateContent?alt=sse` on `cloudcode-pa.googleapis.com`, CCA envelope (`project`, `requestId`, `requestType`, `labels`), OAuth onboarding, planning-leak filtering, `normalizeSchemaForCCA`. |
| `bedrock-converse-stream` (`provider-quirks` §Amazon Bedrock) | — | **GAP (provider crate)** | SigV4/bearer AWS creds, `…/model/{id}/converse-stream`, `inferenceConfig`/`toolConfig`/`additionalModelRequestFields`, and **binary `application/vnd.amazon.eventstream`** framing with CRCs. Nothing in the template's SSE layer (`opencode2api_kit::sse`) speaks it. |
| `transport: pi-native` (`models.md`; `provider-quirks` §Pi Native) | — | **GAP (would be a `pi` module in `opencode2api-dialects`)** | `POST {baseUrl}/v1/pi/stream` with body `{modelId: "provider/id", context, options, stream:true}` and *canonical pi-ai `Context`/`AssistantMessageEvent`* blocks — “lossless … without foreign-wire quantization”. Only omp's own auth-gateway speaks it; a template that wanted to be a credential-less omp sidecar backend would need a fourth dialect module. Low value: only useful omp→omp. |
| local engines: implicit `ollama` / `llama.cpp` = `openai-responses` on `/v1`; `lm-studio` / `vllm` = `openai-completions`; native ollama-chat = `POST /api/chat` NDJSON (`provider-quirks` §Ollama) | `/v1/chat/completions`, `/v1/responses` | **NATIVE (chat) / IR-BRIDGE (responses) / GAP (ollama native)** | The template can *be* an ollama/llama.cpp- or LM-Studio-like OpenAI endpoint. What it cannot be is a native Ollama: discovery would need `/api/tags` + `POST /api/show` (model_info `.context_length`, `capabilities` `thinking`/`vision`) and `/api/chat` NDJSON with `think`, `num_predict`, `done_reason`. |

### Root cause of the two agent-blocking cells (worth stating once)
The IR is deliberately text-shaped: `ChatChunk` = `{id, model, text, finish_reason, usage, raw}` and `Usage` = `{prompt_tokens, completion_tokens}` only (`crates/opencode2api-kit/src/types.rs:58-135`); tool calls exist only inside `raw`, which **only** the chat bridge can use. README states it as policy — “Bridges are text-first by design.” Consequence: any omp api whose history needs structured tool/reasoning/image items is structurally unservable until `ChatChunk`/`Completion` grow typed `tool_calls` (+ reasoning + cache usage) and both bridges fold them. That single change converts Matrix A's two “IR-BRIDGE → GAP” cells into real IR-BRIDGE and is the prerequisite for a gemini bridge too (Google also needs structured `functionCall`).

## 3. Matrix B — template as client toward the upstreams omp names (outbound cell view)

| Upstream | Best template surface | Cell | Cite |
|---|---|---|---|
| OpenAI `/v1` | `service` relay, `POST {base}/v1/chat/completions` | **NATIVE** | `docs/compliance/openai.md` §2, §7 (plain body relays unchanged; `[DONE]`, `stream_options.include_usage` chunk before `[DONE]`) |
| DeepSeek | same relay, base `…/v1` | **NATIVE** | `docs/compliance/deepseek.md` §2 (`max_tokens` kept, `thinking`/`reasoning_effort` verbatim in `raw`), §7 config-only path |
| GLM v4 (`open.bigmodel.cn/api/paas/v4`) | same relay (`normalized_base` keeps `/v4`) | **NATIVE** | `docs/compliance/glm.md` §1.1, §7.1 “zero-code path” |
| DeepSeek/GLM `/v1/responses` (DeepSeek ships Responses since 2026-08-13; GLM has `%s/api/v1/responses`) | IR passthrough is chat-shaped → would need a responses-speaking provider | **PROVIDER-CRATE / IR-BRIDGE-by-design** | `deepseek.md` §1 (Responses endpoint, “no `[DONE]`”, `stream_options` unsupported), `glm.md` §1.1 third-party corroboration |
| Anthropic `api.anthropic.com` (`x-api-key`, `anthropic-version`, strict unknown-field rejection, `content[]` envelope) | cannot be served by chat relay | **PROVIDER-CRATE** | `anthropic.md` §(a) 1-2, §7 diff list, “Verdict rationale: provider-crate-needed — confirmed” |
| Z.AI/DeepSeek Anthropic-compatible faces (`/api/anthropic/v1/messages`) | reachable by a provider crate; unnecessary for Anthropic-inbound→OpenAI-IR | **PROVIDER-CRATE** | `glm.md` §7.3; `deepseek.md` §1 |
| Gemini native / Vertex / CCA / Bedrock / Codex-ChatGPT | no chat-shape fold | **PROVIDER-CRATE each** | §2 rows above |

## 4. List 1 — dialects omp uses that the template already covers

1. **`openai-completions` — fully.** Route + identity IR + verbatim `raw` (tools, `reasoning_content`, vendor extras, usage-only trailing chunk, `[DONE]`). This is omp's primary transport: `provider-quirks` §OpenAI Chat Completions lists Groq, Cerebras, Mistral, DeepSeek, Fireworks, Zhipu, Qwen/DashScope, Kimi, Synthetic, GitLab Duo, OpenRouter, Vercel AI Gateway, CoreWeave, HuggingFace, Nvidia NIM, Novita, GMI Cloud, Baseten, NanoGPT and Sakana as riding it. Live `models.yml` uses it for 2 of 4 providers.
2. **`openai-responses` — wire-compatible, text-only.** Grammar, sequence numbers, terminal `completed`/`incomplete`, `error` frame, flat→nested tools, `instructions`, `reasoning.effort`, `max_output_tokens`, `text.format`→`response_format`, `store`/`previous_response_id` dropped. Sufficient for *non-agent* clients (omp's `tiny`/title-style calls, one-shot completions); not for tool loops/images.
3. **`anthropic-messages` — grammar-faithful, text-only.** Named events, `message_start`→`ping`→`content_block_start/delta/stop`→`message_delta`→`message_stop`, buffered envelope, `max_tokens`-required 400, terminal `event: error` with no `message_stop`, closed error-kind table. Same caveat: no tools/thinking/images.
4. **`/v1/models` (OpenAI shape) — covered** via `Provider::models()`; `service` proxies upstream verbatim (`provider.rs:166-178`). The trait default is a typed 501 (`opencode2api-kit/src/provider.rs:78-81`) → a service crate that doesn't implement it kills omp discovery.
5. **Local-engine surfaces** the template can impersonate: an `openai-completions` `/v1` server (LM-Studio/vLLM-style, discoverable) or an `openai-responses` `/v1` server (Ollama/llama.cpp-style per omp's implicit defaults).

## 5. List 2 — dialects omp uses that the template does NOT cover

| # | Missing | What it needs |
|---|---|---|
| 1 | **Tool-calls/images through `/v1/messages` and `/v1/responses`** | **DELIVERED** — a typed IR did it: `ChatChunk`/`Completion` carry `tool_calls`, and every bridge folds tools AND user-turn media in both directions, so the omp agent loop runs on the non-chat dialects. What that item also asked for and the IR still lacks is a **reasoning channel** and **cache/server-tool usage** (`Usage` is two counters), which is what G7/§1b rows 4-5 now hold open. |
| 2 | **SHIPPED — Gemini native inbound** (`api: google-generative-ai`, live in `models.yml`) | `dialects::gemini` + the path-parameter route (`/v1beta/models/{model}:streamGenerateContent?alt=sse`, `:generateContent`, plus the `GET` listing pair), `x-goog-api-key`/`?key=` accepted by the gate, `systemInstruction`/`functionDeclarations`/`usageMetadata` fold, `functionCall` as a whole object, user-turn `inlineData` folded. `thinkingConfig` remains refused: it asks the upstream to reason, which is spelled per vendor and is therefore provider-crate work, not a missing IR field (reasoning text itself now has one). |
| 3 | **Vertex / Cloud Code Assist (gemini-cli, antigravity)** | Provider crates: ADC/OAuth token ladders, regional hosts, `:streamRawPredict`/`v1internal` envelopes, `id`-stripping, `normalizeSchemaForCCA`, planning-leak filters. Not IR-foldable. |
| 4 | **Bedrock Converse-stream** | Provider crate: SigV4 + `application/vnd.amazon.eventstream` binary framing (new framing codec beside `opencode2api_kit::sse`), `toolConfig`/`inferenceConfig`, `cachePoint`, `NO_TOOLS_SENTINEL`. |
| 5 | **OpenAI Codex (ChatGPT subscription Responses)** | Provider crate: PKCE/device OAuth + account headers + websocket-or-SSE + `responsesLite` + Harmony escaping + `/wham/usage`. |
| 6 | **`pi-native` `/v1/pi/stream`** | A fourth bridge that speaks canonical pi-ai `Context`/events (lossless, no wire quantization). Only worth it to make the template an omp auth-gateway stand-in. |
| 7 | **Anthropic-side extras if you want real Claude clients:** `anthropic-version` validation, `x-api-key` gate, `request-id` response header, `529 overloaded_error`/`402 billing_error`/`409 conflict_error`/`504 timeout_error` kinds, `pause_turn`/`refusal` stop reasons, Anthropic-shaped `GET /v1/models` (`display_name`/`created_at`/`type:"model"` + `has_more`/`first_id`/`last_id`), `thinking` blocks, `cache_*` usage | Bridge/server edits enumerated in `anthropic.md` §(b) items 3-10. |
| 8 | **Ollama native provider face** (`/api/chat` NDJSON, `/api/tags`, `/api/show`) | Provider crate (NDJSON ≠ SSE) if you want `discovery.type: ollama` against the template instead of OpenAI-compat. |
| 9 | **Azure-specific inbound** (`api-key` header, `?api-version=`) | Small server-side change, or just use `openai-responses` from omp (recommended). |

## 6. List 3 — omp client expectations worth freezing in the template README

**Streaming usage**
- omp sends `stream_options: {include_usage: true}` by default (`supportsUsageInStreaming` defaults `true`; `provider-compat-reference` §Chat-completions-only flags) and **waits 2,500 ms after `finish_reason`** for a trailing usage-only chunk (`OPENAI_COMPLETIONS_POST_FINISH_GRACE_MS`, `awaitTrailingUsageDetails`, `provider-quirks` §OpenAI Chat Completions stream behavior). So: after `finish_reason`, emit the usage chunk, then `[DONE]` — and don't close the stream earlier. DeepSeek/GLM fixtures confirm “the terminating chunk carries `usage`” (`deepseek.md` sources #19).
- Usage detail omp reads and would love to see: `prompt_tokens_details.cached_tokens`, `completion_tokens_details.reasoning_tokens`, `cache_write_tokens`, OpenRouter `prompt_tokens_details.cache_write_tokens`, DeepSeek `prompt_cache_miss_tokens` (`provider-quirks` §Usage Chunk Parsing / §8 endpoint-constraints). IR `Usage` only has prompt/completion → `cacheRead`/`cacheWrite` in omp's cost line will read 0 through the template. Known, document it.
- Keepalive: omp **ignores** SSE comment frames as progress (generic-compat rule: keepalives are not progress). `sse_keepalive_secs: 15` is fine but does not by itself satisfy a thinking host — omp's own `streamIdleTimeoutMs` floor covers GLM (600s), DeepSeek/Kimi/MiMo/local (300s), and `streamFirstEventTimeoutMs = 0` for local backends (`provider-compat-reference` §Stream parsing/watchdogs). Consequence for the template's own knobs: `stream_deadline_secs` (default 3600) must stay ≥ omp's floors or the truncation frame outruns the client's patience; `upstream_read_timeout_secs: 120` must conversely stay *under* them, since the client's watchdogs are idle-time caps. Truncation must **not** look complete: template withholds `[DONE]` on failure — matches.
- “Chat Completions can break after `finish_reason` plus usage” (`endpoint-constraints` §7). Empty `choices: []` and role-only deltas are not progress — the template's raw relay preserves them verbatim, good.

**Error JSON parsing**
- OpenAI envelope `{"error":{"message","type","param","code"}}` is what omp parses; template 4xx-shaped pass-through keeps vendor `code` (“vendor code must survive”, `opencode2api-kit/src/errors.rs` test) → omp's `context_length_exceeded` detection, reasoning-effort 400 fallback (`resolveOpenAIReasoningEffortFallback`), strict-tool 400 fallback and `AIError.finalize` all keep working. **Therefore: preserve 4xx bodies verbatim; collapse 5xx only** (already template policy).
- Responses inbound shares the chat envelope (template comment cites `docs/compliance/openai.md`) — fine for omp, since its Responses parser treats an SSE `error` event as terminal failure too.
- Anthropic inbound must use the closed kind set and `event: error`; missing kinds listed in Matrix B/gap #7.
- `not_supported_error`/501: omp treats it as a hard error (not in its retryable set) → typed 501s are the right way to say “capability missing” (e.g. `Provider::models` default). Don't fake a 200.

**Retry / status handling**
- omp's `TurnRecovery` retries 429/500/502/503/504 + network/timeout wording, `retry.maxRetries: 10`, backoff `min(500ms·2^(n-1), 8000ms)` × 75-100% jitter, `retry.maxDelayMs: 300000`, and honours `retry-after-ms` / `retry-after` / `x-ratelimit-reset(-ms)` (`non-compaction-retry-policy` §Backoff). Template's 503-with-`Retry-After: 5` on admission shed is exactly right; its refusal to copy upstream `Retry-After` blindly is right too (omp would sleep on a bogus value; note `errors.rs` NOTE about vendors advertising hours).
- Template's own retries default `max_attempts: 3` (`RetryConfig`) → effective worst-case amplification is 3×10 omp attempts; say so, and say 501 is never retried.
- Stop retrying after visible output: omp requires replay-safety (`isRetryableError` requires no replay-unsafe output) — template's “retry unit ends at provider handoff” invariant matches; document that after-handoff failures become a truncation frame / `event: error`, never a retry.

**`/v1/models` needs**
- `discovery.type: openai-models-list` GETs `{baseUrl}/models` with `/v1` injected unless `discovery.injectV1: false`; payload envelopes omp accepts: `data`, `models`, `result`, `items` (`provider-quirks` §Catalog Discovery).
- `discovery.type: proxy` (built for hosts exposing **both** `/v1/messages` and `/v1/chat/completions`) hits `GET /v1/models` (10 s, OpenAI-style) and reads each entry's **`supported_endpoint_types`** → `"anthropic"` ⇒ `/v1/messages`, `"openai"` ⇒ `/v1/chat/completions`, else provider-level `api` or drop. A template that can synthesize `supported_endpoint_types: ["openai","anthropic","responses"]` per model gets omp's per-model dialect auto-routing for free. Note the Anthropic client strips a trailing `/v1` from `baseUrl` before appending `/v1/messages`, so one `…/v1` baseUrl round-trips both wires.
- Also useful: vLLM-style `max_model_len` (generic fallback `context_length`) is read as the context window by discovery (`models.md` §Practical examples).
- `Provider::models()` must actually be implemented per service crate, else omp discovery is a 501 (trait default).

**Validation & auth handshake**
- Key-validation probes omp issues against OpenAI-compatible bases: `POST /chat/completions` `{messages:[{role:"user",content:"ping"}], max_tokens:1, temperature:0}` + Bearer (`provider-quirks` §API-Key Validation; `validate "chat-completions"` rules, 15 s timeouts), and `validate "models-endpoint"` variants (`/v1/models`, `/v1/accounts/.../models`) — so the template must answer the ping with 200 and 401 correctly when `client_api_key` is set.
- The gate is ONE function (`routes.rs::client_auth_gate`) and any one of the credential spellings the served dialects document satisfies it: `Authorization: Bearer`, `x-api-key`, `x-goog-api-key`, or `?key=`. omp's `anthropic-messages` transport sending `X-Api-Key` without `Authorization` therefore needs NO workaround any more, and neither does a Gemini client's `x-goog-api-key`; `authHeader: true` remains valid, just no longer required. A `?key=` credential travels in a URL, which is why the request span logs the path and never the query.
- `api` per provider is the dispatch key, not provider name: “stream dispatch keys on `model.api`, not `model.provider`” (`adding-a-provider.md` Scope) → the template's four chat-surface routes each just need a distinct provider id in `models.yml`, all pointed at the same `baseUrl` host (exactly what the live config does with AxonHub).
- omp request shaping you should expect to see arrive and be preserved: `developer` vs `system` role, coalesced vs separate leading system messages (KV-cache), `store: false`, `max_tokens` vs `max_completion_tokens`, `tool_choice` spellings, `reasoning: {effort}`/`thinking:{type}`/`enable_thinking`/`chat_template_kwargs`/`reasoning_effort` per `thinkingFormat`, `cache_control: {type:"ephemeral"}` Anthropic-style markers even on chat payloads (`cacheControlFormat: "anthropic"`), and `reasoning_content` replay on assistant turns (local/loopback bases auto-enable `replayReasoningContent`; DeepSeek thinking+tools **400s** without it). All of these are top-level/message fields the template's flattened `extra` + unparsed `messages` already relay verbatim on the chat dialect — state that as a contract: *do not normalize or strip client body fields in the shared crates.*

## 7. Therefore: point omp at this template via …

Provider-entry shapes only (no secrets; `apiKey` is env-var-name-or-literal in omp). Assume template on `:10080`, `server.client_api_key` set.

```yaml
# ~/.omp/agent/models.yml
providers:
  # (1) TODAY: fully agent-viable. api = the chat route; baseUrl must end /v1.
  opencode2api:
    baseUrl: http://127.0.0.1:10080/v1
    api: openai-completions
    auth: apiKey
    apiKey: OPENCODE2API_CLIENT_KEY            # env var name, or literal
    discovery:
      type: openai-models-list          # needs Provider::models() implemented
    models:
      - id: some-relay-model
        name: Some Relay Model
        reasoning: true
        input: [text]
        contextWindow: 200000
        maxTokens: 8192
        compat:
          maxTokensField: max_tokens          # DeepSeek/GLM keep max_tokens; OpenAI wants completion form
          supportsStore: false                # avoid store on non-standard upstreams
          supportsUsageInStreaming: true      # relay emits include_usage + trailing usage chunk
          reasoningContentField: reasoning_content
          replayReasoningContent: true        # loopback base auto-detects this; pin for clarity

  # (2) Responses dialect: works text-only; NO tools/images until the IR carries them.
  opencode2api-resp:
    baseUrl: http://127.0.0.1:10080/v1
    api: openai-responses
    auth: apiKey
    apiKey: OPENCODE2API_CLIENT_KEY
    models: [ … ]                         # same ids; text-only roles (e.g. tiny/smol)

  # (3) Anthropic dialect: works with the SDK's own `x-api-key` or with a bearer.
  opencode2api-msg:
    baseUrl: http://127.0.0.1:10080/v1     # omp strips trailing /v1, re-appends /v1/messages
    api: anthropic-messages
    auth: apiKey
    authHeader: true                      # optional: sends Authorization: Bearer; the gate also accepts x-api-key
    apiKey: OPENCODE2API_CLIENT_KEY
    disableStrictTools: true              # bridge has no strict-schema path at all
    models: [ … ]

  # (4) If the service crate can emit supported_endpoint_types, one provider covers all faces:
  #     discovery: {type: proxy}   -> per-model anthropic/openai auto-routing
```

Settings side: `modelRoles: {default: opencode2api/some-relay-model, task: …, tiny: opencode2api-resp/cheap-text-model}`, and note `thinking: {mode: effort, efforts: [...], requiresEffort}` + `reasoningEffortMap` per model are how omp encodes the upstream's effort ladder — the template must pass those body fields through untouched.

**Verdict for the parent report:** all four inbound dialects exist and omp can be pointed at each. The highest-leverage fix was not a new dialect but typing tool calls into `opencode2api-kit`'s `ChatChunk`/`Completion` and folding them in every bridge — done: `ToolCall`/`ToolCallDelta` are IR types, all three non-chat folds translate function tools in both directions, user-turn media folds in all three, and `openai-responses`, `anthropic-messages` and `gemini` are genuine IR-BRIDGE cells for agent traffic. (Cache usage remains untyped; reasoning now has an IR field, and `thinkingConfig`/top-level `thinking` still refuse because they are per-vendor REQUEST knobs, not history.) The `dialects::gemini` module + path-param route this verdict closed on — the one surface the user's live config already demanded from a single-host gateway — shipped on that typed foundation, with its own `?alt=sse` framing rule, credential spellings, and `listModels`/`getModel` read surface.
