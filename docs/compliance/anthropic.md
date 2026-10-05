# Anthropic Messages API — chat-endpoint compliance report

**Investigated:** official Claude Platform docs (markdown variants of `docs.claude.com`/`platform.claude.com`, fetched 2026-09-06) + `anthropic-sdk-python` @ `1.4.0` (commit `62de60b`, 2026-09-04) + `anthropic-sdk-typescript` @ `0.124.0` (commit `ba14b1f`), including the Python SDK's *recorded live API fixtures* (`tests/lib/tools/__inline_snapshot__/...json`) which are real response bodies. Judged against `local://provider-seam.md` and the live template tree.

> ⚠️ Tree moved twice since this research. Crates are now `x2api-kit`,
> `x2api-server`, `x2api-transport`, `x2api-dialects` (the three former
> `x2api-bridge-*` crates, now modules: `chat` / `anthropic` / `responses`)
> and `service` (THE program — one upstream per process, so it is no longer
> named after a vendor). Paths below were mechanically updated; **line
> numbers predate the move** and the tool-calling findings in §(b) predate
> the folds gaining tool support — see `dialect-matrix.md` G2/G3.

---

## (a) Anthropic as an UPSTREAM

### 1. Endpoint + auth

| Item | Value | Source |
|---|---|---|
| Chat endpoint | `POST https://api.anthropic.com/v1/messages` | [api overview](https://docs.claude.com/en/api/overview), [create endpoint](https://docs.claude.com/en/api/messages/create) |
| Beta namespace | `POST /v1/messages?beta=true` (same path + query) | py SDK `resources/beta/messages/messages.py:1136` |
| Token counting | `POST /v1/messages/count_tokens` | api overview |
| Models | `GET /v1/models`, `GET /v1/models/{model_id}` | [models-list](https://docs.claude.com/en/api/models/list) |
| Legacy | `POST /v1/complete` — Text Completions, marked **`[Legacy]`** in the reference; Python SDK 1.0 deleted `client.completions` | [completions ref](https://platform.claude.com/docs/en/api/completions/create) |

**Headers (docs API-overview table, verbatim):**

| Header | Value | Required |
|---|---|---|
| `Authorization` | `Bearer <token>`, where `<token>` is **your API key** or a short-lived token from `POST /v1/oauth/token` (Workload Identity Federation) | **Yes, unless `x-api-key` is set** |
| `x-api-key` | Console API key. **"Legacy fallback for `Authorization`, still supported"** | No |
| `anthropic-version` | e.g. `2023-06-01` | **Yes** |
| `content-type` | `application/json` | **Yes** |
| `anthropic-workspace-id` | `wrkspc_…` | Required with a multi-workspace key |
| `anthropic-beta` | comma-joined dated tokens (`claude-code-20250219`, `thinking-binding-controls-2026-08-01`, …) | Optional |

**Drift that matters:** as of 2026 the *primary* auth form is `Authorization: Bearer sk-ant-api…`; `x-api-key` is demoted to a supported legacy alias ([authentication](https://docs.claude.com/en/manage-claude/authentication): "Send it as `Authorization: Bearer <key>` … The legacy `x-api-key` header is still supported in place of `Authorization`"). Both SDKs still emit `X-Api-Key` for `apiKey=` and `Authorization: Bearer` only for `authToken=`/OAuth creds (py `_client.py:360-373`, ts `client.ts:944-955`), and default `anthropic-version: 2023-06-01` (py `_client.py:381`, ts `client.ts:1594`). Version values: `2023-06-01` (current), `2023-01-01` (initial, deprecated). Docs domain also moved: `docs.anthropic.com` → `platform.claude.com/docs` (301 observed) with `docs.claude.com` as alias. The exact 400 body for a missing `anthropic-version` is **UNVERIFIED** (docs state the requirement only).

**Client-key passthrough (`CallContext::client_authorization`): VIABLE, no token management.** An OpenAI-dialect client's `Authorization: Bearer <key>` can be forwarded verbatim as Anthropic bearer auth when the key is an Anthropic key; `x-api-key` is reachable too (`CallContext::client_headers` is public). Server-side static key is equally fine. OAuth/WIF (`POST /v1/oauth/token`, refreshable short-lived tokens, `TokenCache` in the SDKs) is only needed if you want WIF credentials — not for the default path.

### 2. Request conformance — an OpenAI body does NOT relay

Required (py SDK `message_create_params.py`, generated from the OpenAPI spec): `max_tokens: Required[int]`, `messages: Required[Iterable[MessageParam]]`, `model: Required[ModelParam]`. Anthropic validates strictly and **rejects unknown fields**, e.g. `"block_binding: Extra inputs are not permitted"` (docs errors page) and in the wild `max_retries: Extra inputs are not permitted`, `context_management: Extra inputs are not permitted` — so an unmodified OpenAI body 400s.

| OpenAI field | Anthropic | Transform |
|---|---|---|
| `max_completion_tokens` | `max_tokens` (required; `0` allowed = cache pre-warm) | rename |
| `stop` (str\|array) | `stop_sequences: [string]` | rename |
| `messages[].role:"system"` | top-level `system: string \| [TextBlockParam]` — docs: "there is no `system` role for input messages in the Messages API" | lift; but the SDK `MessageParam.role` literal *does* include `"system"` and Opus 5 documents mid-conversation `role:"system"` messages immediately after a user turn ([migration guide](https://docs.claude.com/en/models/opus-5/migration-guide)) |
| `messages[].content` string | string **or** content-block array (`text`/`image`/`document`/`tool_use`/`tool_result`/`thinking`/`redacted_thinking`/server-tool blocks) | pass-through for text; block mapping otherwise |
| `tools[]` `{type:"function",function:{name,description,parameters}}` | `{name, description, input_schema}` (+ versioned server tools `web_search_20250305`, `text_editor_20260728`…) | reshape |
| `tools[].function.name` in assistant `tool_calls` | `tool_use` block `{id,name,input}`; results come back as `tool_result` blocks (`tool_use_id`) inside a **`user`** message (no `tool` role) | structural |
| `tool_choice:"required"/"none"/`{type:"function",function:{name}}` | `{type:"any"|"none"|"auto"|"tool", name}` | rewrite |
| `response_format:{type:"json_schema",json_schema:{schema}}` | `output_config.format` (`type:"json_schema"`) — deprecated alias `output_format` | rewrite |
| `temperature`,`top_p`,`top_k` | **deprecated & value-gated**: post-Opus-4.6 models 400 on non-default (`temperature` must be exactly `1.0`, `top_p ≥ 0.99`, `top_k` any value rejected); range documented as `0.0–1.0` (OpenAI is `0–2`); Python SDK ≥1.0 removed the params | **drop** for modern models |
| `n`, `stream_options`, `logprobs`, `seed`, `presence/frequency_penalty` | none | drop |
| `user` | `metadata.user_id` | rename |
| — | `thinking`, `service_tier`, `cache_control`, `container`, `inference_geo`, `workspace_id`, `output_config.effort/task_budget` | Anthropic-only additions |

Anthropic also 400s assistant prefill on Opus 4.6+/Sonnet 4.6+/Opus 5 (`"This model does not support assistant message prefill. The conversation must end with a user message."`) — relevant because OpenAI clients legitimately end a turn with an assistant message.

### 3. Buffered response conformance

No `object`, no `created`, no `choices`. Envelope (`Message`, py `types/message.py`; docs Returns section):

```json
{
  "id": "msg_013Zva2CMHLNnXjNJJKqJ2EF",
  "type": "message",
  "role": "assistant",
  "model": "claude-opus-5",
  "content": [ { "type": "text", "text": "Hi! My name is Claude.", "citations": [] } ],
  "stop_reason": "end_turn",
  "stop_sequence": null,
  "stop_details": null,
  "container": null,
  "usage": { "input_tokens": 2095, "output_tokens": 503,
    "cache_creation_input_tokens": 2051, "cache_read_input_tokens": 2051,
    "cache_creation": {"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0},
    "server_tool_use": {"web_search_requests":0,"web_fetch_requests":2},
    "output_tokens_details": {"thinking_tokens":0},
    "service_tier": "standard", "inference_geo": "global" }
}
```

- `stop_reason` set (SDK `stop_reason.py` + docs): `end_turn` · `max_tokens` · `stop_sequence` · `tool_use` · `pause_turn` · `refusal` · `model_context_window_exceeded`. **Non-null always in buffered mode**; null only in `message_start`.
- `usage` required; `input_tokens`/`output_tokens` required ints, all others optional/nullable. Billing total input = `input_tokens + cache_creation_input_tokens + cache_read_input_tokens`.
- **No `stream_options.include_usage` equivalent** — usage is always returned (buffered and streamed).
- `content` is an array of typed blocks; text is *not* a single field → `Completion.text` = concatenation of `type=="text"` blocks; `raw: Some(verbatim Anthropic JSON)` or a synthesized OpenAI envelope.

### 4. Streaming conformance

- **Content-type:** `text/event-stream; charset=utf-8`, plus `cache-control: no-cache`, `connection: keep-alive` (recorded live response, py SDK inline snapshot).
- **Named events only.** Versioning doc, verbatim, for `2023-06-01`: "All events are **named events** … rather than data-only events. **Removed unnecessary `data: [DONE]` event.**" Each event's `event:` name is duplicated as `type` inside the data object.
- **`[DONE]` is never sent — CONFIRMED.** The terminal sentinel is `event: message_stop` followed by connection close.
- Grammar (verbatim, [streaming docs](https://docs.claude.com/en/build-with-claude/streaming) + SDK `raw_*_event.py` + live fixture):

```
event: message_start
data: {"type":"message_start","message":{"id":"msg_…","type":"message","role":"assistant","content":[],"model":"claude-opus-5","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":770,"output_tokens":8}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: ping
data: {"type": "ping"}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: content_block_start
data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_01T1…","name":"get_weather","input":{}}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"location\":"}}

event: content_block_stop
data: {"type":"content_block_stop","index":1}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":89}}

event: message_stop
data: {"type":"message_stop"}
```

- Flow contract: `message_start` → per block (`content_block_start`, ≥1 `content_block_delta`, `content_block_stop`) → ≥1 `message_delta` → `message_stop`. `index` **corresponds to the block's index in the final `Message.content` array** (docs). Zero-delta blocks do occur (server-side-fallback emits a start/stop pair with no deltas; `display:"omitted"` thinking emits only `signature_delta`).
- Delta types: `text_delta{text}` · `input_json_delta{partial_json}` · `thinking_delta{thinking}` · `signature_delta{signature}` · `citations_delta{citation}`.
- **Usage is cumulative on every `message_delta`** (docs warning: "The token counts shown in the `usage` field of the `message_delta` event are *cumulative*"). Spec marks `usage` **required** on `message_delta` (`MessageDeltaUsage.output_tokens: int` required); one docs thinking example omits it → treat as abbreviated docs, always emit it.
- Keepalives: named `ping` events (any number), **not** `:` comment lines. Comments are ignored by both SDK decoders (py `_streaming.py:453`, ts `streaming.ts:381`), so they are harmless but off-dialect.
- Mid-stream error: `event: error` + `data: {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`, then close — **no `message_stop`**.
- Provider must status-gate before handoff and translate `error` events into a terminal `Err`; Anthropic's own retries (5xx/429, 2 attempts, honoring `retry-after`) are the client's job — our `with_retry` covers pre-handoff only, which matches the seam rule.

### 5. Errors

`{"type":"error","error":{"type":…, "message":…},"request_id":"req_011CSHoEeqs5C35K2UUqR7Fy"}` — status→kind table, verbatim from [docs.claude.com/en/api/errors](https://docs.claude.com/en/api/errors):

| Status | `error.type` |
|---|---|
| 400 | `invalid_request_error` (also used for other undocumented 4XX) |
| 401 | `authentication_error` |
| 402 | `billing_error` |
| 403 | `permission_error` |
| 404 | `not_found_error` |
| 409 | `conflict_error` |
| 413 | `request_too_large` (32 MB Messages limit, Cloudflare-enforced) |
| 429 | `rate_limit_error` |
| 500 | `api_error` |
| 504 | `timeout_error` |
| 529 | `overloaded_error` |

- **`retry-after`: yes**, on 429 rate limits and fast-mode 429 — seconds to wait ("Earlier retries will fail"). **Absent** on the tier-spend-cap 429 (distinguish via `error.details.error_code == "enforced_spend_limit_reached"`). A user-set spend limit returns **400 `invalid_request_error`**, not 429.
- Every response carries a `request-id` header (`req_…`), mirrored as `request_id` in error bodies; also `anthropic-ratelimit-{requests,tokens,input-tokens,output-tokens}-{limit,remaining,reset}` and `anthropic-organization-id`/`anthropic-workspace-id`.
- Kinds may grow over time (versioning policy) → treat unknown kinds as retryable-by-status, not fatal.

### 6. Models endpoint

`GET https://api.anthropic.com/v1/models` (+ `GET /v1/models/{model_id}`), same auth/version headers; query `limit` (default 20, 1–1000), `after_id`, `before_id` (id-cursor scheme, **not** the `page`/`next_page` scheme used by newer Anthropic lists). Shape is **not** OpenAI's:

```json
{ "data": [ { "id": "claude-opus-5", "type": "model",
    "display_name": "Claude Opus 5", "created_at": "2026-07-24T00:00:00Z",
    "max_tokens": 0, "max_input_tokens": 0,
    "capabilities": { "thinking": {"supported": true, "types": {"adaptive": {"supported": true}, "enabled": {"supported": true}}}, "effort": {…}, "image_input": {"supported": true}, "structured_outputs": {"supported": true}, … } } ],
  "first_id": "…", "last_id": "…", "has_more": true }
```

→ `Provider::models()` must translate this into the OpenAI `{"object":"list","data":[{"id","object":"model","created","owned_by":"anthropic"}]}` shape the template's `GET /v1/models` serves (RFC-3339 → epoch seconds; `display_name`/`capabilities` dropped or parked in a vendor field).

### 7. Provider-adapter diff list (what the Anthropic-facing fork's `service` crate —
    `crates/service/src/provider.rs` — must do)

1. **URL:** base `https://api.anthropic.com` + `/v1/messages` (no `/chat/completions`); the reference `service::normalized_base` (`/v1`,`/v4` heuristics) is wrong for Anthropic — hardcode `/v1`.
2. **`decorate`:** set `content-type: application/json`, `anthropic-version: 2023-06-01`, and auth = forwarded client bearer **or** `x-api-key`; optional `anthropic-workspace-id`, `anthropic-beta`. (The reference `decorate` doc-comment already flags this: *"A vendor needing `x-api-key`, JWT, or OAuth refresh grows its logic here"*.)
3. **`to_upstream_body`:** lift IR `role:"system"` messages → top-level `system` string; `stop`→`stop_sequences`; **drop `temperature`/`top_p`/`top_k`** for post-Opus-4.6 models (they 400); rename `max_completion_tokens`→`max_tokens` and default it (Anthropic requires it; OpenAI doesn't); map OpenAI `tools`/`tool_choice`/`response_format` into `tools[].input_schema` / `{type,name}` / `output_config.format`; translate `tool_calls`/`tool`-role messages into `tool_use`/`tool_result` blocks; strip `n`, `stream_options`, penalties, `logprobs`, `user`→`metadata.user_id`; never end the turn with an `assistant` message (prefill 400).
4. **Buffered parse:** `content[]` text join → `Completion.text`; `stop_reason`→`finish_reason` inverse map (`max_tokens`→`length`, `tool_use`→`tool_calls`, else `stop`); `usage.input_tokens`→`prompt_tokens`, `output_tokens`→`completion_tokens`; cache fields need a home — either an `Usage` extension or verbatim `raw`.
5. **Stream parse:** Anthropic has no `choices`; parse by `data["type"]` (our `SseDecoder` yields only `Data`/`Done` and drops `event:` names — sufficient, since Anthropic repeats the type in the payload), accumulate `text_delta` into `ChatChunk.text`, treat `input_json_delta`/`thinking_delta`/`signature_delta` as non-text (park in `raw`), take cumulative `usage` from `message_delta`, terminal `finish_reason` from `message_delta.delta.stop_reason`, **stop at `message_stop`** (never `[DONE]` → the reference loop's `SseEvent::Done` arm and its post-EOF flush assumption must be replaced), ignore `ping`, and turn `event: error` into a terminal `ProviderError` with the mapped Anthropic kind.
6. **Errors:** Anthropic 4xx bodies are `{"type":"error","error":{…}}`, not `{"error":{…}}` → `gate_response`'s verbatim-passthrough test (`openai_error_object`) won't recognize them, so Anthropic-specific errors get sanitized into `"anthropic: 400: …"` unless the crate pre-normalizes the envelope; parse `retry-after` explicitly (`with_retry_after`, capped) since `gate_response` deliberately ignores it.
7. **`models()`:** shape translation (see §6) + id-cursor paging if we ever expose pagination.

**Zero-code relay? No.** `service` cannot reach Anthropic with base_url/key config: wrong path, wrong body keys, strict unknown-field rejection, bearer-vs-api-key header, non-OpenAI response envelope, and a non-OpenAI SSE grammar with no `[DONE]` terminator. → **`provider-crate-needed` (confirmed)**. It is a *small* crate though: the credential story is static-key passthrough, so no token manager, no OAuth, no browser flow — the cost is entirely in body/SSE translation, not auth.

---

## (b) Anthropic as the INBOUND dialect: what `x2api-dialects::anthropic` must not violate

### What the real SDKs check client-side (source-verified)

| Rule | Python 1.4.0 | TypeScript 0.124.0 |
|---|---|---|
| Events are dispatched **only by the SSE `event:` name**; anonymous `data:` frames are **silently dropped** | `_streaming.py:75-127` (`sse.event == "message_start" or …`); `ServerSentEvent.event` defaults `None` | `core/streaming.ts:79-124`; `SSEDecoder.event` defaults `null` |
| `type` must be inside the JSON data | not strictly (SDK backfills `data["type"] = sse.event`) | **required** (`JSON.parse(sse.data)` then `event.type`) |
| First event must be `message_start` | `RuntimeError: Unexpected event order, got X before "message_start"` (`lib/streaming/_messages.py:460`) | `AnthropicError: … before "message_start"` (`lib/MessageStream.ts:578`), **and** a second `message_start` before `message_stop` throws (`:572`) |
| Blocks must be opened before their deltas; `index` must equal position in `content` | `content_block_start` **appends** ("TODO: check index", `:462-470`); `content_block_delta`/`content_block_stop` do `snapshot.content[event.index]` → **IndexError** if out of range (`:471,:511`) | `content.push(...)`; `content.at(event.index)` → out-of-range is silently ignored (text loss, no throw) |
| Required payload fields (pydantic validates at runtime; TS types are compile-time only) | `Message`: `id`,`type:"message"`,`role:"assistant"`,`model`,`content[]`,`usage` with `input_tokens`+`output_tokens` **required ints**; `RawMessageDeltaEvent.usage` required with `output_tokens` required; `TextBlock`: `type`+`text`; `RawContentBlockStartEvent.content_block` is a discriminated union | same fields, non-optional in the interfaces |
| `stop_reason` | `null` in `message_start`, non-null thereafter (docs) | same |
| `ping` / unknown events | skipped / tolerated (`if sse.event == "ping": continue`; `assert_never` only under `TYPE_CHECKING`) | skipped (`streaming.ts:135`) |
| `event: error` | raises via `_make_status_error` | `throw new APIError(undefined, body, …, type)` — **`type` string is surfaced to the caller** (`streaming.ts:139-142`) |
| `:` comment lines | ignored (`_streaming.py:453`) | ignored (`streaming.ts:381`) |
| Tool loop driver | `stop_reason` + presence of `tool_use` blocks (`lib/tools/_beta_runner.py:204-276`) | same |
| Non-streaming timeout guard | SDK validates `max_tokens` against a 10-min budget (`MODEL_NONSTREAMING_TOKENS`) | same |

### Our bridge's current contract (verified against the tree)

Faithful today: `message_start` (with `content: []`, `stop_reason/stop_sequence: null`, zeroed `usage`) → `ping` → lazily-opened single `content_block_start` index 0 → `text_delta`s → `content_block_stop` → `message_delta{stop_reason, stop_sequence:null, usage}` → `message_stop`, all with `event:` names via `sse::append_event_frame`; empty-text streams still get a start/stop pair; buffered reply carries `id/type/role/model/content/stop_reason/stop_sequence/usage`; `max_tokens` missing → 400 `invalid_request_error` (`routes.rs` "Faithful Anthropic servers reject this"); terminal error → `event: error` with no `message_stop`; `Sink` threads the last chunk's `finish_reason` into `finish(last)` (`relay.rs:87-100`), so streaming `stop_reason` is no longer hardcoded `end_turn`.

### Where it fails a real Anthropic client

1. **Any tool-using client.** `to_chat_request` forwards only `temperature/top_p/top_k/max_tokens/stop_sequences` — `tools`, `tool_choice`, `thinking`, `metadata` are dropped, and `tool_result`/`tool_use` blocks are flattened to text (role map sends everything non-`assistant` to `user`). Outbound, the bridge emits exactly one `text` block, so a client that declared `tools` gets no `tool_use` block, no `input_json_delta`, and no `tool_use` `stop_reason`. Even the *partial* case is broken: `stop_reason("tool_calls") == "tool_use"` while `content` holds only a text block — the SDK's accumulator/`get_final_message()` then reports `stop_reason: "tool_use"` with zero `tool_use` blocks, and the tool runner loops/aborts. Same for images/documents: silently dropped inbound with no error.
2. **Extended thinking.** `thinking_delta`/`signature_delta`/`thinking` blocks are never emitted; on Opus 4.7+/Opus 5 thinking is on by default and responses *start* with `thinking` blocks — clients written against the documented "select blocks by `type`" guidance see content that never arrived. Upstream reasoning has no IR field at all.
3. **Error-kind table is off-vendor.** `anthropic_kind` emits `overloaded_server_error` (503) — a string in **neither** the docs table **nor** the SDK unions (`shared.ts:40-49` = `invalid_request_error, authentication_error, permission_error, not_found_error, rate_limit_error, timeout_error, overloaded_error, api_error, billing_error`; py `beta_error.py` union adds no `request_too_large`/`conflict_error` variant, so the pydantic discriminated union can't match those two either). Anthropic's overloaded status is **529 → `overloaded_error`**; ours maps 529 to `api_error` (falls through `_`) and invents 503. Missing: `timeout_error` (504), `billing_error` (402), `conflict_error` (409). The `anthropic_renders_closed_kind_set` test currently *pins* the non-vendor literal. The TS stream path forwards `body.error.type` straight to the thrown error, so clients switching on `type` misbehave.
4. **Error/response metadata.** Error body lacks `request_id`; our correlation header is `x-request-id`, while Anthropic's is `request-id` (both SDKs expose `_request_id` from `request-id`). No `retry-after` is derived from an upstream 429 by the renderers unless the provider set `with_retry_after`.
5. **`stop_reason` coverage.** Only `end_turn`/`max_tokens`/`tool_use` are producible. `stop_sequence` is never reported (and `stop_sequence` is always `null` even when a stop matched), nor are `pause_turn`, `refusal`, `model_context_window_exceeded`; `stop_details` and `container` are never emitted.
6. **Usage fidelity.** `usage` carries only `input_tokens`/`output_tokens`; cache accounting (`cache_creation_input_tokens`, `cache_read_input_tokens`, `cache_creation`, `server_tool_use`, `service_tier`) is unrepresentable in IR `Usage` and never reaches the Anthropic bridge, and `message_start` always reports zeros (Anthropic reports real prompt tokens there, `message_delta` is cumulative).
7. **`GET /v1/models` is OpenAI-shaped and format-blind** (`routes.rs::models` hardcodes `InboundFormat::OpenAi`). A real Anthropic SDK `client.models.list()` fails pydantic validation: `ModelInfo` requires `display_name`, `created_at` (RFC-3339) and `type: "model"`, and the page requires `data`/`has_more`/`first_id`/`last_id` — OpenAI's `object`/`created`/`owned_by` items don't satisfy it.
8. **`anthropic-version` and `x-api-key` inbound are not honored.** The version header is never validated (docs: "you **must** send an `anthropic-version` request header"), and `client_auth_gate` reads only `Authorization: Bearer` — so with `server.client_api_key` set, a default-configured Anthropic SDK client (which sends `X-Api-Key` for `apiKey=`) is rejected 401. CORS already allow-lists both headers, so this is a gate gap, not a browser gap. Also unmodeled: `anthropic-workspace-id` (a 400 this proxy would otherwise never reproduce) and `anthropic-beta` (a beta client expects beta features to exist).
9. **Keepalive style.** We interleave `: keepalive` comments; Anthropic sends named `ping` events. Both SDK decoders tolerate ours, so this is cosmetic — but a strict non-SDK SSE parser keyed on `event:` seeing nothing for a long time is the reason Anthropic chose pings.
10. **ID shape.** We mint `msg_x2api-<hex16>`; Anthropic ids are `msg_`-prefixed opaque strings (fine), but tool ids/results are absent, so no client-side correlation is possible.

### Verdict rationale

`provider-crate-needed` — **confirmed**, and *also* required on the inbound side for anything beyond a text echo. Anthropic is a distinct wire in both directions: different path, different required keys (`max_tokens`), strict unknown-field rejection, a different response envelope (`content[]` + `stop_reason`), a different stream grammar (named events, cumulative usage, `message_stop` terminator, no `[DONE]`), and a different error taxonomy (529 `overloaded_error`, `retry-after` semantics). None of that is expressible as `base_url`+key config on `service`. The saving grace versus a vendor like Google/Bedrock: **auth is a static key passthrough** — the new crate needs no token store, only `decorate` for `anthropic-version` + bearer/x-api-key and translation code in `to_upstream_body`, the buffered parser, and the named-event decoder.

**Sources:** [API overview](https://docs.claude.com/en/api/overview) · [Create a Message](https://docs.claude.com/en/api/messages/create) · [Streaming](https://docs.claude.com/en/build-with-claude/streaming) · [Errors](https://docs.claude.com/en/api/errors) · [Versions](https://docs.claude.com/en/api/versioning) · [List Models](https://docs.claude.com/en/api/models/list) · [Rate limits](https://docs.claude.com/en/api/rate-limits) · [Authentication](https://docs.claude.com/en/manage-claude/authentication) · [Stop reasons](https://docs.claude.com/en/build-with-claude/handling-stop-reasons) · [Opus 5 migration](https://docs.claude.com/en/models/opus-5/migration-guide) · `anthropics/anthropic-sdk-python@1.4.0` (`types/message_create_params.py`, `types/stop_reason.py`, `types/usage.py`, `types/message.py`, `types/raw_*_event.py`, `lib/streaming/_messages.py`, `_streaming.py`, `_client.py`, live fixture `tests/lib/tools/__inline_snapshot__/…555fb399…json`) · `anthropics/anthropic-sdk-typescript@0.124.0` (`src/resources/shared.ts`, `src/resources/messages/messages.ts`, `src/core/streaming.ts`, `src/lib/MessageStream.ts`, `src/client.ts`).
