# Zhipu AI / Z.AI — GLM chat API compliance report

Investigated 2026-09-06 against: CN docs `docs.bigmodel.cn` (+ its live `openapi/openapi.json`, 796,939 B), intl docs `docs.z.ai` (+ `openapi.json`, 146,793 B), official Python SDKs on PyPI (`zhipuai 2.1.5.20250825`, `zai-sdk 0.2.3`, wheel source read directly), and third-party consumers (new-api, LiteLLM 1.100.0, glm-launch, SillyTavern/OpenClaw issues). Judged against `local://provider-seam.md` and the template's reference provider `crates/service/src/provider.rs`.

**Verdict up front: GLM's OpenAI-compatible `/api/paas/v4` surface is servable by the reference `service` provider with config only** (`base_url` ending in `/v4` is already handled by `normalized_base()`, plain `Authorization: Bearer`, `{base}/models`, `[DONE]`, comment-frame-tolerant decoder). Two translation risks need a live probe, not a new crate (Q7).

---

## 1. Endpoint + auth

### 1.1 Base URLs (all product surfaces, both regions)

| Surface | Path (append `chat/completions` / `models`) | Auth |
|---|---|---|
| **General API — CN** | `https://open.bigmodel.cn/api/paas/v4/` | `Authorization: Bearer <API_KEY>` |
| **General API — intl (Z.AI)** | `https://api.z.ai/api/paas/v4/` | same, + optional `Accept-Language: en-US,en` |
| Coding Plan — OpenAI Chat, CN | `https://open.bigmodel.cn/api/coding/paas/v4` | Bearer (Coding-Plan key) |
| Coding Plan — OpenAI Chat, intl | `https://api.z.ai/api/coding/paas/v4` | Bearer |
| Coding Plan — Anthropic Messages, CN | `https://open.bigmodel.cn/api/anthropic` (+`/v1/messages`) | `x-api-key` **or** bearer |
| Coding Plan — Anthropic Messages, intl | `https://api.z.ai/api/anthropic` (+`/v1/messages`) | `x-api-key` **or** bearer |
| Coding Plan — OpenAI Responses, CN/intl | `https://open.bigmodel.cn/api/v1`, `https://api.z.ai/api/v1` | Bearer |

- CN: “请求端点(通用API) `https://open.bigmodel.cn/api/paas/v4/`” — https://docs.bigmodel.cn/cn/guide/develop/http/introduction
- Intl: “General API Endpoint `https://api.z.ai/api/paas/v4/`” — https://docs.z.ai/guides/develop/http/introduction
- Coding-plan endpoint table (Anthropic / OpenAI Chat / OpenAI Response bases) — https://docs.bigmodel.cn/cn/coding-plan/quick-start and https://docs.z.ai/devpack/quick-start
- Anthropic-compatible base: “替换您访问的 **base_url** 为 `https://open.bigmodel.cn/api/anthropic`” — https://docs.bigmodel.cn/cn/guide/develop/claude/introduction
- CN OpenAPI `servers` = `{"url": "https://open.bigmodel.cn/api/"}` with paths `/paas/v4/chat/completions`; intl spec `servers: - url: https://api.z.ai/api` — https://docs.bigmodel.cn/openapi/openapi.json, https://docs.z.ai/openapi.json
- Third-party corroboration: `fmt.Sprintf("%s/api/paas/v4/chat/completions", baseURL)`, claude branch `%s/api/anthropic/v1/messages`, responses branch `%s/api/v1/responses` — `relay/channel/zhipu_4v/adaptor.go` in https://github.com/QuantumNous/new-api; `constant/channel.go` special bases map `glm-coding-plan` → `{ClaudeBaseURL: https://open.bigmodel.cn/api/anthropic, OpenAIBaseURL: https://open.bigmodel.cn/api/coding/paas/v4}` and `glm-coding-plan-international` → `api.z.ai` equivalents.

**Drift notes / dates.** The only live major today is `v4`; neither `docs.z.ai/llms.txt` nor `docs.bigmodel.cn/llms.txt` publishes a `/v3` or `/v2` page. Coding Plan keys are **not** interchangeable with general keys and the general endpoint answers a coding-plan key with `429 code 1113 “余额不足或无可用资源包”` — https://docs.bigmodel.cn/cn/api/api-code, https://github.com/sipeed/picoclaw/issues/1652, https://patricklerner.com/writing/routing-z-ai-glm-through-headroom-in-opencode (“the general endpoint returns ‘insufficient balance’ on a coding-plan key”). Model-code dates: GLM-5 Feb 2026, GLM-5.1 Apr 2026, GLM-5.2 Jun 2026, GLM-5.3/-5.3-Flash 2026-08-19 (https://github.com/QwenLM/qwen-code/issues/5393, https://github.com/SillyTavern/SillyTavern/issues/5984).

### 1.2 Auth: plain bearer vs JWT — plain `Authorization: Bearer <api-key>` is the documented, SDK-default scheme

1. Docs: “Authorization `string` header required — 标准的 HTTP Bearer 认证方式，在 API Keys 页面获取密钥” (https://docs.bigmodel.cn/api-reference/%E6%A8%A1%E5%9E%8B-api/%E5%AF%B9%E8%AF%9D%E8%A1%A5%E5%85%A8); OpenAPI `securitySchemes: {bearerAuth: {type: http, scheme: bearer}}` (both specs). JWT is presented as an *alternative*: “支持的鉴权方式: API Key 鉴权 / JWT Token 鉴权”.
2. Official SDKs (read from wheels): default = raw key bearer; JWT only if you opt out of the default.
   ```python
   # zhipuai 2.1.5.20250825  zhipuai/_client.py
   _disable_token_cache: bool = True
   def auth_headers(self):
       if self._disable_token_cache:
           return {"Authorization": f"Bearer {api_key}", "x-source-channel": source_channel}
       else:
           return {"Authorization": f"Bearer {_jwt_token.generate_token(api_key)}", ...}
   # base_url default: f"https://open.bigmodel.cn/api/paas/v4"
   ```
   ```python
   # zai-sdk 0.2.3  zai/_client.py: disable_token_cache: bool = True
   class ZaiClient(BaseClient):      default_base_url = 'https://api.z.ai/api/paas/v4'   # + 'Accept-Language': 'en-US,en'
   class ZhipuAiClient(BaseClient):  default_base_url = 'https://open.bigmodel.cn/api/paas/v4'
   ```
   JWT recipe (what the optional path signs): payload `{api_key: id, exp, timestamp}` from `apikey.split(".")`, HS256, header `sign_type: SIGN` — http/introduction page.
3. Gateway corroboration: `req.Set("Authorization", "Bearer "+info.ApiKey)` — new-api `zhipu_4v/adaptor.go`.

**Relay consequence:** `CallContext::client_authorization()` passthrough is fully viable — the inbound `Authorization: Bearer <glm-key>` forwards verbatim; no token store, no OAuth, no JWT management.

### 1.3 Request headers
`Content-Type: application/json`; `Authorization: Bearer`; intl recommends `Accept-Language: en-US,en` (its only accepted enum value). No `anthropic-version` on the v4 surface. The relay's `send()` sets `Accept: application/json` even for streams — GLM chooses SSE from the body `stream` flag (docs curls set no `Accept`), and the relay mode-converts a non-SSE reply, so this is safe.

### 1.4 Model ids
- CN text enum (对话补全 page): `glm-5.3, glm-5.2, glm-5.1, glm-5-turbo, glm-5, glm-4.7, glm-4.7-flash, glm-4.7-flashx, glm-4.6, glm-4.5-air, glm-4.5-airx, glm-4.5-flash, glm-4-flash-250414, glm-4-flashx-250414` (default `glm-5.3`).
- Intl text enum (`ChatCompletionTextRequest`): `glm-5.3, glm-5.2, glm-5.1, glm-5, glm-4.7, glm-4.7-flash, glm-4.7-flashx, glm-4.6, glm-4.5, glm-4.5-air, glm-4.5-x, glm-4.5-airx, glm-4.5-flash, glm-4-32b-0414-128k`.
- Vision enum (intl): `glm-5.3-flash, glm-4.6v, autoglm-phone-multilingual, glm-4.6v-flash, glm-4.6v-flashx, glm-4.5v`.
- Codes are lowercase with dots/hyphens; **no `+` form exists in the current v4 dialect.** Legacy `glm-4-plus/glm-4-air/glm-4-flash/glm-4-flashx` still appear in the `max_tokens` table (max 4095) at https://docs.bigmodel.cn/cn/guide/start/concept-param but are absent from live request enums. Unknown/unentitled code ⇒ `400 1211 “模型不存在，请检查模型代码”`; the anthropic surface answers `400 modelCode: does not exist` (https://github.com/jefftriplett/glm-launch README, `bench --all`).
- `[1m]` tier suffix (`glm-5.3[1m]`, `glm-5.2[1m]`) is a **Claude-Code-side / Anthropic-endpoint convention**: documented for the Anthropic base (https://docs.bigmodel.cn/cn/coding-plan/tool/claude) but reported rejected on the OpenAI-compat endpoint (https://github.com/BerriAI/litellm/issues/32218; glm-launch live probe). Use `model_map` instead of `[1m]` ids.

---

## 2. Request conformance (does a plain OpenAI body relay unchanged?)

**Mostly yes — a documented OpenAI-compatible subset plus a GLM extension set; the field universe is closed in the spec.**

`ChatCompletionTextRequest` (exact property list from `openapi.json`): `model*`, `messages*`, `stream`, `max_tokens`, `temperature`, `top_p`, `do_sample`, `thinking`, `reasoning_effort`, `tools`, `tool_choice`, `tool_stream`, `stop`, `response_format`, `request_id`, `user_id` (vision variant drops `response_format`/`tool_stream`; audio variant drops tools/thinking, adds `watermark_enabled`). Required: `model`, `messages` (minItems 1; “不能只包含系统消息或助手消息”).

| OpenAI field | GLM v4 behaviour | Evidence |
|---|---|---|
| `model, messages, temperature, top_p, stop, stream, tools, tool_choice, response_format, max_tokens` | Accepted, same names | OpenAPI schemas; API-ref page |
| `max_tokens` | **The documented name.** Range `1–131072`; per-model default 65536 (glm-5.x), 16384 (4.6v), 4095-max legacy glm-4-* | API ref + concept-param |
| `max_completion_tokens` | Not in spec. Community-verified as **accepted**: “Headroom renames `max_tokens` to `max_completion_tokens` on the way out. z.ai accepts it.” | https://patricklerner.com/writing/routing-z-ai-glm-through-headroom-in-opencode — official acceptance UNVERIFIED |
| `stream_options` | Not in spec. LiteLLM lists it as supported passthrough for `zai`; GLM emits usage without it | https://github.com/BerriAI/litellm `litellm/llms/zai/chat/transformation.py` (`get_supported_openai_params` = `max_tokens, stream, stream_options, temperature, top_p, stop, tools, tool_choice` [+`thinking`]) — **needs live probe** |
| `n, presence_penalty, frequency_penalty, seed, logprobs, parallel_tool_calls, store, metadata, user` | **Absent from every GLM schema.** Ignore-vs-`400 1210` UNVERIFIED; indirect evidence says ignored | OpenAPI (0 occurrences in both specs) |
| `temperature` / `top_p` ranges | `0.0 ≤ temperature ≤ 1.0`, `0.01 ≤ top_p ≤ 1.0`, both “限两位小数”. **OpenAI clients sending temperature 1.2–2.0 will 400** (`1214 参数非法`). new-api clamps defensively: `if top_p >= 1 { top_p = 0.99 }` | API ref page; new-api `ConvertOpenAIRequest` |
| `response_format` | `{type: text \| json_object}` only — no `json_schema`/strict | CN schema |
| `tools` | OpenAI function shape (`additionalProperties:false`; name pattern `^[a-zA-Z0-9_-]+$`, ≤64), max 128 functions; intl `tool_choice` documents **only `auto`**; extra tool types `retrieval`, `web_search`, `mcp` (CN); GLM-only `tool_stream`. FAQ: “tools 支持传多个函数，但每次调用只能命中一个”; function/retrieval/web_search mutually exclusive by priority | https://docs.z.ai/api-reference/llm/chat-completion.md, https://docs.bigmodel.cn/cn/faq/api-issues |
| `stop` | `string[]`, maxItems 4, but intl: “Currently, only one stop word is supported” | z.ai chat-completion.md |
| GLM-only extras | `thinking:{type,clear_thinking}`, `reasoning_effort`, `do_sample`, `tool_stream`, `request_id`, `user_id` | API ref / thinking-mode page |

`request_id` (6–64 chars) is client-supplied and echoed-shaped — a natural slot for our `CallContext::request_id`.

---

## 3. Buffered response conformance

Documented 200 body (`ChatCompletionResponse`, CN spec): `id`, `request_id`, `created` (unix s), `model`, `choices[]`, `usage`, plus GLM extras `video_result[]`, `web_search[]`, `content_filter[]`.

- `choices[i]`: `index`, `message{role, content, reasoning_content, tool_calls[], audio{id,data,expires_at}}`, `finish_reason`. **No `logprobs`.**
- `usage`: `prompt_tokens`, `completion_tokens`, `total_tokens`, `prompt_tokens_details.cached_tokens`; the SDK model adds `completion_tokens_details.reasoning_tokens` (`zai/types/chat/chat_completion_chunk.py`). Optional per spec.
- **`object` is absent from both OpenAPI response schemas and every official sample** (e.g. the thinking-mode sample starts `{"created":…, "model":"glm-5.3", "choices":[…], "usage":{…}}` — https://docs.bigmodel.cn/cn/guide/capabilities/thinking.md). Real pass-through responses carry it: `{"id":"20251224113151a94620120f9e4ebf","model":"glm-4.7","object":"chat.completion","request_id":"…","usage":{…}}` and `"object":"chat.completion"` for glm-5 — https://docs.aimlapi.com/api-references/text-models-llm/zhipu/glm-4.7 and .../glm-5. Treat as **present in practice, undocumented in schema (UNVERIFIED)**; our parser must not require it.
- No `service_tier`, no `system_fingerprint`.
- **Usage needs no opt-in on the buffered path** (“调用结束时返回的 Token 使用统计”).
- Template fit: `Completion::from_openai` reads `choices[0].message.content` ✓, `finish_reason` ✓, `usage.{prompt_tokens,completion_tokens}` ✓ (unknown keys ignored; `#[serde(default)]`), `raw: Some(value)` keeps `request_id`/`reasoning_content`/`web_search` verbatim for OpenAI clients ✓.

---

## 4. Streaming conformance

| Aspect | GLM v4 |
|---|---|
| Trigger | body `"stream": true` (no `Accept` requirement) |
| Content-Type | `text/event-stream` — declared as a 200 media type in the CN spec (`responses.200.content` = `application/json`, `text/event-stream`) |
| Frames | plain `data:` JSON chunks, **no named `event:` lines** |
| Terminal sentinel | **`data: [DONE]` — explicit.** “流式输出结束时会返回 `data: [DONE]` 消息” (CN) / “When the Event Stream ends, a `data: [DONE]` message will be returned” (intl schema description) |
| Chunk shape | `{id, created, model, choices:[{index, delta:{role?, content, reasoning_content?, tool_calls?}, finish_reason}], usage?}`; sample `data: {"id":"1","created":1677652288,"model":"glm-5.2","choices":[{"index":0,"delta":{"content":"春"},"finish_reason":null}]}`; `object` absent in official sample (§3) |
| Usage | **On the last chunk, without `stream_options`**: “`usage`: 令牌使用统计（仅在最后一个chunk中出现）”; the documented terminal frame carries `finish_reason:"stop"` **and** `usage` together: `data: {…"choices":[{"index":0,"finish_reason":"stop","delta":{"role":"assistant","content":""}}],"usage":{"prompt_tokens":8,"completion_tokens":262,"total_tokens":270,"prompt_tokens_details":{"cached_tokens":0}}}` |
| Empty-`choices` frames | Exist — both official SDK samples guard `if not chunk.choices: continue` (streaming + stream-tool pages), so usage may also arrive OpenAI-style |
| Keepalive / comments | The official SSE parser skips lines starting with `:` (`zai/core/_streaming.py`: `if line.startswith(':') or not line: return`), i.e. comment/heartbeat lines must be ignored. Whether GLM actively sends them: UNVERIFIED |
| Mid-stream errors | Documented: **no error codes mid-stream** — “使用流式（SSE）调用时，如果 API 在推理过程中异常终止，不会返回上述错误码，而是在响应体的 `finish_reason` 参数中返回异常原因”. The SDK *also* treats an in-band `data: {"error": …}` frame as terminal (`_streaming.py`) → handle both |
| `finish_reason` | enum `stop \| length \| tool_calls \| sensitive \| network_error` (CN chunk schema) + `model_context_window_exceeded` named in descriptions — non-OpenAI values |
| Tool-call deltas | `delta.tool_calls[].index/function.name/function.arguments` accumulation; `tool_stream=true` required for streamed arguments (GLM-5.3/5.2/5.1/5/5-Turbo/4.7/4.6) |
| Reliability caveat | Upstream bug report: GLM-5.1 on `api.z.ai/api/coding/paas/v4` intermittently emits **truncated JSON inside an SSE `data:` frame** on long tool-heavy sessions; the model also hallucinates literal `data: {…}` text into `content` with unescaped inner quotes — https://github.com/zai-org/GLM-5/issues/66. Our decoder maps that to a terminal `ProviderError` (truncation frame, `[DONE]` withheld) — correct behaviour, but expect these on GLM |

Sources: https://docs.bigmodel.cn/cn/guide/capabilities/streaming, https://docs.z.ai/guides/capabilities/streaming, https://docs.bigmodel.cn/cn/guide/capabilities/stream-tool, `zai-sdk 0.2.3` wheel source.

---

## 5. Errors

- Wire envelope (both regions): **`{"error": {"code": "1001", "message": "…"}}`** — business code as a **string**; HTTP status out-of-band. Verbatim: `HTTP/2 401 … {"error":{"code":"1001","message":"Header 中未收到 Authentication 参数，无法进行身份验证"}}`. **No `error.type`, no `error.param`.**
- Spec drift to flag: the intl `openapi.json` `Error` schema is *flat* (`{code: int32, message}`, both required) and the intl API-ref template renders `{ "code": 123, "message": "" }`, while the intl **Errors page** shows the nested string-code form (`"error": {"code": "1214", …}`). The nested/`error.code`-string form is corroborated by captured bodies in both regions ⇒ treat that as truth; the flat integer form is a spec-doc bug (UNVERIFIED which the gateway emits on non-chat endpoints).
- Code/status map (identical tables both regions): `1000/1001/1003/1005`→401; `1220`→403; `1210 1211 1212 1213 1214 1215 1221 1222 1261 1301`→400; `1113`→429 (arrears/no resource pack); `1200 1230 1234`→500; `1302 1305 1308 1309 1310 1311 1313 1314 1315 1316–1321`→429. Notable: **`1305` = platform overload signalled as 429**, `1301` = safety block as 400, and `1308/1310/1316-1321` embed `next_flush_time` **in the message text** (no structured reset).
- **Retry-After: not documented, not observed.** Docs give prose guidance only (“请您控制请求频率”, “稍后再试”, “限额将在 ${next_flush_time} 重置”). `gate_response` will have nothing to read; `ProviderError.retry_after` stays `None` and our `RetryConfig` backoff must drive cadence. Also reported: **Cloudflare-fronted 429/code 1305 masquerading as a content-filter rejection** on the coding endpoint — https://github.com/NousResearch/hermes-agent/issues/47685.
- Rate model: concurrency-based (per model, per entitlement tier; dynamic peak throttling), not rpm — https://docs.bigmodel.cn/cn/api/rate-limit.
- Template mapping: bodies are already `"error"`-keyed ⇒ pass through verbatim per the seam rule; we must synthesize `error_type`; `1200/1230/1234` 5xx collapse to canned messages as designed.

---

## 6. Models endpoint

- **Exists on the general v4 base**: `GET {base}/models` → `https://open.bigmodel.cn/api/paas/v4/models`, `https://api.z.ai/api/paas/v4/models`, `Authorization: Bearer <key>`. **Not documented on either docs site** (absent from both `llms.txt` indexes and both OpenAPI `paths`) — undocumented-but-live.
- Live-verified shape: OpenAI list — `{"object":"list","data":[{"id":"glm-4.5"}, …]}` (https://github.com/openclaw/openclaw/issues/14352). Verified with a live key 2026-08-27, “identical on `api.z.ai` and `open.bigmodel.cn`”: `glm-5.3, glm-5.3-flash, glm-5.2, glm-5.1, glm-5, glm-5-turbo, glm-4.7, glm-4.6, glm-4.5, glm-4.5-air` (https://github.com/SillyTavern/SillyTavern/issues/5984) — **10 core text models only; vision models (`glm-4.6v*`, `glm-5v-turbo`) are omitted**, so it is not a complete catalog. Per-item fields beyond `id`: UNVERIFIED.
- Coding-plan base exposes the same route: `https://api.z.ai/api/coding/paas/v4/models` with `Authorization: Bearer` (https://github.com/jefftriplett/glm-launch README: “The live endpoint is the OpenAI-compatible coding PaaS base … and uses `Authorization: Bearer`”). Health check lists Z.AI models endpoint as “Auth Required” (https://llm24.net/model-api-check).
- Auth caveat: the relay's `models()` sends a bearer only when `cfg.api_key` is set (no client passthrough) — fine for GLM, but a coding-plan key must not hit the general base.
- No quota/usage API (“there is no API for quota data” — glm-launch README).

---

## 7. Provider-adapter diff list + verdict

### 7.1 Zero-code path (recommended)

The reference relay already satisfies GLM's dialect — verified in source:
- `normalized_base()` keeps a base untouched when it `ends_with("/v4")` (`crates/service/src/provider.rs`) → `chat_url = https://open.bigmodel.cn/api/paas/v4/chat/completions`, `models_url = …/v4/models`.
- `decorate()` = `req.bearer_auth(token)`; `bearer()` prefers `cfg.api_key`, else strips `Bearer ` from `ctx.client_authorization()` → **client-key passthrough works** (§1.2).
- `decode_upstream_sse` + `x2api_kit::sse::SseDecoder`: stops at `[DONE]`, drops `:` comment lines and `event:`/`id:`/`retry:` → matches §4.
- `ChatChunk::from_openai` / `Completion::from_openai` are key-optional (missing `object`/`id`, `choices: []` usage frames, extra `request_id`/`reasoning_content`/`web_search` survive via `raw`).
- A non-SSE reply to a stream request is mode-converted, not error-relayed.
- `ServiceConfig {base_url, api_key, model_map}` + `X2API_UPSTREAM_URL`/`X2API_UPSTREAM_KEY` ⇒ GLM is a config row:
  ```json
  {"provider": {"base_url": "https://open.bigmodel.cn/api/paas/v4", "api_key": "<32hex>.<16alnum>"}}
  ```

**No new provider crate is needed for GLM chat.** What config-only loses is enumerated below; two items are correctness risks to probe first.

### 7.2 Concrete wire diffs (what `crates/service/src/provider.rs` does on a fork)

| # | Direction | Transform |
|---|---|---|
| D1 | out | URL = `<base>/chat/completions`; base `/api/paas/v4` (general) or `/api/coding/paas/v4` (coding key) |
| D2 | out | `Authorization: Bearer <raw key>` — never JWT |
| D3 | out | Clamp `temperature` into `[0,1]` (2 decimals) and `top_p` into `[0.01,1)`. new-api precedent `top_p >= 1 → 0.99`. Without this, any client sending `temperature: 2` gets `400 1214` |
| D4 | out | **Suppress `stream_options`** — GLM emits usage itself; the relay currently injects `{"include_usage": true}` unconditionally (probe §7.4-1) |
| D5 | out | Drop fields absent from GLM's closed schema (`n`, `presence_penalty`, `frequency_penalty`, `seed`, `logprobs`, `parallel_tool_calls`, `store`, `metadata`, `user`); map `max_completion_tokens → max_tokens`; collapse `response_format` to `text`/`json_object`; cap `stop` at 1 entry; downgrade non-`auto` `tool_choice` |
| D6 | out | Optional GLM wins: `thinking:{type,clear_thinking}`, `reasoning_effort`, `do_sample`, `tool_stream`, `request_id = <our request id>` (6–64 chars) |
| D7 | in | Usage can arrive on the terminal content chunk **or** as an empty-`choices` frame → reducer must take whichever carries it and never treat `choices: []` as EOF |
| D8 | in | `finish_reason` superset: the bridge's `stop_reason()` maps `sensitive`/`network_error`/`model_context_window_exceeded` to `end_turn` today; faithful would be `model_context_window_exceeded → max_tokens`, `network_error → error`, `sensitive → stop_sequence`/flag |
| D9 | in | `reasoning_content` (deltas + message) is dropped by the IR → Anthropic clients lose chain-of-thought (bridge is text-first by design); OpenAI clients keep it via `raw` |
| D10 | in | Errors: `error.code` is a string and `error.type` is missing → synthesize `type` from HTTP status |
| D11 | in | `object` may be missing (schema silent, resellers see it) → re-synthesize for strict OpenAI clients |
| D12 | in | Malformed/truncated SSE frames occur upstream (zai-org/GLM-5#66) → terminal error, withhold `[DONE]` (already the relay's behaviour) |

### 7.3 Anthropic-side note (GLM as an Anthropic upstream)
GLM also speaks Anthropic Messages natively at `{base}/api/anthropic` (`/v1/messages`, `x-api-key`, `stream:true`, effort-tier mapping low/medium/high/xhigh/max → `reasoning_effort`, `[1m]` ids). Using that surface *faithfully* (thinking blocks, `tool_use` blocks, Anthropic error semantics) **does** need a provider crate — `decorate()` would send `x-api-key` and the body would be Anthropic-shaped. For the current design (Anthropic inbound → OpenAI IR → GLM v4) it is unnecessary; the asymmetry is that GLM's Anthropic surface exposes capabilities (`[1m]` tiers, native thinking blocks) the v4 relay path cannot express.

---

## 8. Anthropic-inbound requirements

Q8 targets the Anthropic service itself; for GLM only the *provider-side* Anthropic-compat requirements matter, and they are:
- Path `<base>/v1/messages`; base `https://open.bigmodel.cn/api/anthropic` (CN) / `https://api.z.ai/api/anthropic` (intl) — official docs + glm-launch (README: “Z.AI exposes an Anthropic-compatible endpoint at `https://api.z.ai/api/anthropic`, so no local proxy is needed”; `bench` sends a 32-token `/v1/messages` round-trip → `OK (200) in 412ms`).
- Headers: `x-api-key: YOUR_API_KEY` + `content-type: application/json` (official cURL). Bearer also works (Claude Code configured with `ANTHROPIC_AUTH_TOKEN`; glm-launch sets both) → both accepted; whether `anthropic-version` is validated: UNVERIFIED (not documented as required; Claude Code doesn't send it here).
- `max_tokens` required (official example passes `max_tokens: 1024`). Coding models are **text-only** (“The GLM coding models are text-only — pasting images into Claude Code won't work through Z.AI”, glm-launch README); vision comes via their Vision MCP server.
- Errors on this surface are Anthropic-flavoured strings: unknown/unentitled model ⇒ `400 modelCode: does not exist` (glm-launch `bench --all`).
- For our *inbound* Anthropic faithfulness the only GLM-imposed items are: single text block per turn is fine; `stop_reason` must never leak `sensitive`/`network_error` (D8); no Anthropic `thinking` content block is derivable from the v4 path (D9).

---

## Open items to settle with one live probe each (Main)
1. **`stream_options` tolerance** — does `{"stream":true,"stream_options":{"include_usage":true}}` return 200 on `/api/paas/v4/chat/completions`? Decides whether D4 must be code (the relay injects it today).
2. **Unknown-field strictness** — send `n`, `presence_penalty`, `max_completion_tokens`, `metadata` together: 200 (ignored) vs `400 1210` sets D5's severity.
3. **`object` presence** in buffered + chunk payloads (schema silent, resellers see it).
4. **`GET {base}/models`** availability per region + per-item fields (community-verified, not officially documented).
5. Whether any 429 carries `Retry-After` (never documented; note Cloudflare can answer 429 before GLM does).
