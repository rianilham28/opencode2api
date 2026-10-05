# opencode2api — OpenCode Zen behind the x2api template

This repo is a fork of the `rust-translate-proxy` **x2api template**, deployed
as **opencode2api**: one process fronting OpenCode's Zen API and exposing its
**free tier — and only it** — as standard chat endpoints over all four client
dialects. All vendor behavior lives, as code, in exactly one file:
`crates/service/src/provider.rs` (one process, one upstream).

## This deployment

| Fact | Value |
|---|---|
| Listen | `127.0.0.1:10081` (`server.port` in `config.json`) |
| Upstream | `https://opencode.ai/zen/v1` — chat style at `…/chat/completions`, responses style at `…/responses` |
| Model surface | **`-free` ids only**: `/v1/models` keeps ids ending `-free`; the catalogue is fetched from upstream at most once per 900s (cached, stale-served on upstream error), and a model upstream reports `Model is unavailable` is withheld from the list for 900s, extended by every repeat — still forwarded if a client asks for it directly |
| Style split | `muse-spark-1.2-contributor-free` / `muse-spark-1.3-contributor-free` answer **only** over the Responses upstream (chat 500s upstream, live-probed); every other free id rides `/chat/completions` |
| Upstream identity | the opencode 2.x fingerprint: `User-Agent: opencode/latest/<semver>/cli` **adopted dynamically** from `GET /update/api/latest/cli/npm` at boot, on a 60s-throttled catalogue tick, and on 426; `x-opencode-client: cli`; a per-process 40-hex project and `ses_`-shaped session mirrored into `x-session-affinity`/`x-session-id`; `Authorization: Bearer public` — the documented, non-secret free-tier token (`config.json` `provider.api_key`) |
| Free-tier gate | every upstream request forces `stream: true` and carries `tools` naming **`bash` and `read`** (decoys appended when missing; `tool_choice: "none"` only when the client sent no tools) — a buffered or tool-less body is 403 `FreeTierError`. Buffered clients are served by folding the forced stream back; the post-`[DONE]` `cost` frame never reaches clients |
| Client dialects | `/v1/chat/completions`, `/v1/messages`, `/v1/responses`, `/v1beta/models/…` — all fold through the IR |
| Debugging | a Proxyman **reverse proxy** entry `opencode2api` (`127.0.0.1:10099 → 127.0.0.1:10081`) captures every client request; recording on, `opencode.ai` in the SSL-proxying list |
| Config | `config.json` only — no `.env` needed for the quick start |

Requests for non-free model ids are still forwarded: the catalogue is advisory
and upstream's own answer stays the honest one.

The remainder of this README documents the underlying template — its request
flow, workspace layout, contracts, and runbooks (in `AGENTS.md`).

## The request flow — two translations, opposite directions, one IR

```
        client dialects (x2api-dialects)                upstream vendor (yours)
┌──────────────────────────────────────────────────┐     ┌────────────────────────┐
│ POST /v1/chat/completions            ─ chat      │     │                        │
│ POST /v1/messages                    ─ anthropic │────▶│ ② IR ──► vendor wire   │
│ POST /v1/responses                   ─ responses │     │ ③ vendor ──► IR chunks │
│ POST /v1beta/models/{model}:…Content ─ gemini    │     │                        │
└──────────────────────────────────────────────────┘     └────────────────────────┘
                  x2api-server: pipeline, SSE relay, retry, envelopes
```

1. **① inbound fold (`x2api-dialects`, vendor-blind).** Each client dialect
   has a module: request → IR (`ChatRequest`), and IR → that dialect's wire for
   responses AND streams. `-compat` is the identity fold (the IR *is*
   chat-completions-shaped, so its relay is verbatim and free).
2. **②③ outbound translation (`crates/service/src/provider.rs`,
   dialect-blind).** One handoff of IR out to the vendor, vendor JSON/SSE
   back into IR (`Completion`/`ChatChunk`). This is *the* customization file.
3. The server never knows a vendor; the provider never knows a dialect. The
   IR is the only shared tongue — that's why one chat-completions provider
   serves **all four** client styles with zero extra code (a Responses or a
   Gemini client against a chat-only upstream just works: fold in, relay,
   re-expand into that dialect's events on the way out).

## Workspace

```
crates/
  x2api-kit               IR · Provider trait · ProviderError · SSE framing
                           (memchr) · backoff · ServerConfig · telemetry · wire
  x2api-server             router · pipeline (auth gate, admission, retry) ·
                           guarded stream relay · per-dialect error render ·
                           graceful two-stage drain · build_router() / run()
  x2api-transport          upstream egress: warm lanes over sticky proxy
                           sessions, rotation, shards (optional — a service
                           with direct egress uses `Transport::direct`)
  x2api-dialects           the inbound folds, one module each: chat
                           completions (identity), Anthropic Messages,
                           OpenAI Responses, Gemini generateContent
  service                  THE program (lib+bin): this deployment's provider.
                           Ships an OpenAI-dialect implementation — OpenAI,
                           DeepSeek, GLM v4, … — as its starting point
bench/                     comparative harness + fold benches (root member, dev
                           tool — a fork's `cargo new` story skips it)
config.example.json        server + proxy + provider sections (no secrets)
config.production.example.json
                           systemd-shape policy; drift-pinned like its dev twin
.env.example               the secrets those sections leave out
deploy/x2api.service       hardened systemd unit (runbook: docs/production.md)
docs/compliance/           source-verified provider reports (2026-09-06)
AGENTS.md                  boundary doctrine — read before editing
```

## Quick start

```sh
cp config.example.json config.json      # the SHAPE: urls, lanes, limits
cp .env.example .env                    # the SECRETS: keys, proxy passwords
cargo run -p service -- --config config.json

H='content-type: application/json'
curl :10080/v1/chat/completions -H "$H" -d '{"model":"…","messages":[{"role":"user","content":"hi"}]}'
curl :10080/v1/responses       -H "$H" -d '{"model":"…","input":"hi","stream":true}'
```

`.env` is read first and never overrides an already-exported variable, so an
injected container secret beats a stale file; `X2API_ENV_FILE` points it
elsewhere. Credentials go there rather than in `config.json` — a proxy URL
carries a password. And `config.json` is secret too once
`server.client_api_key` is set, so keep it `0640` (see `docs/production.md`).

<!-- env-overrides:start -->
Env overrides are pinned to the implementation. Service loading uses
`X2API_CONFIG`, `X2API_ENV_FILE`, `X2API_BIND`, `X2API_PORT`,
`X2API_EXTRA_BINDS`, `X2API_MAX_INFLIGHT`, `X2API_DRAIN_SECS`,
`X2API_LOG_LEVEL`, `X2API_LOG_JSON`, `X2API_LOG_DIR`, `X2API_LOG_PREFIX`,
`X2API_LOG_ROTATE`, `X2API_LOG_KEEP_FILES`, `X2API_LOG_STDOUT`,
`X2API_CLIENT_API_KEY`, `X2API_RETRY_ATTEMPTS`, `X2API_HTTP2_PRIOR_KNOWLEDGE`,
and `X2API_UPSTREAM_SHARDS`; `RUST_LOG` additionally selects per-crate logging.
Provider credentials and endpoints use `X2API_UPSTREAM_URL`, `X2API_UPSTREAM_KEY`,
and `X2API_UPSTREAM_KEYS` (rotated after 429/401). Egress uses
`X2API_PROXY_URL`, `X2API_PROXY_LANES`, and `X2API_PROXY_SESSION_TTL_SECS`.
<!-- env-overrides:end -->
Every completed request writes ONE line, and it is the short one: `request_id`,
endpoint, format, `stream`, `result`, `status`, `duration_ms`, the model, and the
token counts — plus `images`/`files` when media actually arrived and, on a
counted stream, `ttfb_ms` + `frames`. Fields a request did not measure are ABSENT
(`null`-free by construction): that is how the fidelity lane's by-design silence
reads. `format` stays because `/v1/models` and `/v1beta/models` share one
`endpoint="models"`, and the answer must not depend on a span the operator can
filter away. `request_id` also rides the request SPAN (which carries `path` and
nothing else), so the provider's error warning and the retry warnings of the same
call are attributable in either layout.
Config split is
enforced: `server.*` is the template's surface, `proxy.*` belongs to
`x2api-transport`, and `provider.*` is parsed by your service crate — never
by core.

## Pointing it at your upstream

One process serves one upstream, so there is no crate to copy and nothing to
rename: `crates/service` **is** the program, and retargeting it is a diff to
`src/provider.rs`. What ships there is an OpenAI-dialect implementation
(OpenAI, DeepSeek, GLM v4, …) — a starting point, not a fixture.

Say to your coding agent: *"make it talk to glm"* — `AGENTS.md` Runbook R1 is
the recipe. By hand, it is three files, in order:

1. **`src/lib.rs`** — `ServiceConfig`: what an operator may configure.
   Secrets and endpoints ONLY; a logic knob appearing here is a mistake
   (it belongs as code in step 2).
2. **`src/provider.rs`** — the four seams: endpoint construction, `decorate`
   (auth/headers), `to_upstream_body`, stream decode. Optional: declare
   `native_dialects()` + implement `relay_raw` for a vendor with a native
   face (fidelity lane).
3. **`src/main.rs`** — parse your config, wire your provider. Done:
   `cargo run -p service -- --config config.json` serves all four client
   dialects, metrics, drain, retry, envelopes.

Everything not in those three files is the frozen template — the correct
edit for a new vendor is never `x2api-server`.

**Forking the whole template as its own repo** is Runbook R2: three sed
patterns (env prefix, crate/metric/id prefix, repo name) carry every
namespace the template owns — verified to zero residue with the copy
compiling `--workspace --all-targets` afterward. The `service` crate name
survives the fork: it carries no template token, so the global sed cannot
reach it, and it names the deployment, not this template.

**Worked example — the opencode port.** Against the real `../opencode-free`
(12k LoC across 12 modules), the port is ~200 lines because each vendor
quirk it had already has exactly one home on the template:

```rust
// opencode-free smeared its profile over proxy.rs/retry.rs/server.rs.
// Here (provider.rs) it's three functions:

fn decorate(&self, req: RequestBuilder, ctx: &CallContext) -> Result<…> {
    // the client header profile that IS the service's identity
    req.bearer_auth(&self.cfg.api_key)              // "public" for the free tier
        .header(USER_AGENT, OPENCODE_UA)
        .header("x-opencode-client", "cli")
        .header("x-opencode-project", "global")
        .header("x-opencode-request", opencode_id()) // ordered clock + base62
}

fn to_upstream_body(&self, req: &ChatRequest, stream: bool) -> Value {
    let mut body = req.to_value();
    if let Some(m) = &self.cfg.force_model {        // OPENCODE_FORCE_MODEL
        body["model"] = json!(m);
    }
    body
}
// …and the `cost` bookkeeping frame OpenCode appends AFTER [DONE] never
// reaches clients — the shared SSE decoder stops at Done (the original
// needed relay_body byte surgery for it). Free.
```

Also free on the port: `/v1/messages` + `/v1/responses` + the Gemini
`/v1beta/models/{model}:{generate,streamGenerate}Content` surface the original
never had, typed 429/400/5xx envelopes, full-jitter retry, admission shed,
`x-request-id` stamping, Prometheus metrics, graceful drain.

## Operational surface

| Route | Answers |
|---|---|
| `/health` | 200 while the process lives, and **nothing more**. A live proxy whose upstream is gone should be pulled from rotation, not restarted into an outage nobody can diagnose. |
| `/ready` | 200 when this proxy can actually serve; **503 + `Retry-After`** when it cannot — with a proxy pool configured, that means no healthy egress lane and no room to mint one. This is the route a load balancer should poll. It never mints a lane or sends anything upstream, so probing it every second costs nothing and cannot keep an idle pool alive at your vendor's expense. |
| `/metrics` | Prometheus text, when the bin installed a recorder. |
| `/v1/models` | The upstream's list, if the provider implements `models()`. |
| `/v1beta/models` | The same list in Gemini's grammar — `models/<id>` resources carrying this surface's verbs — for a client whose SDK discovers with `listModels`. Rendered by `gemini::models_page` from the SAME `Provider::models()` the row above answers from. |
| `/v1beta/models/{model}` | One model resource (`getModel`): `models/{id}` and the bare id name the same model, and a miss is a 404 rather than an empty answer. |

The Gemini pair is gated by the `generate-content` dialect like the rest of
that surface, and it makes a stream name its framing: `:streamGenerateContent`
requires `?alt=sse`, because Google's default for that verb is a JSON-array
stream and this proxy frames SSE only. Without `alt=sse` the fold answers a
typed 400 that says what to send, rather than replying in a shape nobody asked
for. A provider that declares Gemini native answers on the fidelity lane
before that check and relays whatever framing the vendor itself used.

`server.dialects` narrows the whole dialect surface — every route a label owns,
the Gemini listing included: `["chat"]` serves `/v1/chat/completions` and
leaves the others **absent**, 404 through the same envelope as any unknown path,
rather than registered-and-refusing, which reads to a client as a broken
endpoint instead of one that was never offered. Empty (the default) serves all
four. It is a routing decision rather than a
Cargo feature on purpose: compile-time gating would thread `cfg` through the
`InboundFormat` variants and every `match` over them — routes, envelopes, the
relay's sinks — to avoid compiling ~2300 lines that cost nothing to carry.

Metrics worth alerting on:

- `x2api_requests_total{endpoint,format,status}` — `status` is `200-committed`
  for a stream, since a stream's real outcome is not known when the headers go
  out. `x2api_streams_total{endpoint,format,result}` carries that outcome:
  `ok` / `failed` / `truncated` / `panicked` / `dropped` (client vanished).
- `x2api_request_duration_seconds{endpoint,stream}` — request lifetime from
  request admission through the terminal outcome.
- `x2api_tokens_total{endpoint,format,kind}` — what the UPSTREAM reported;
  `kind` is `prompt`, `completion`, `cached`, `reasoning`, `cache_creation`, or
  `cache_read`. The four detail kinds are emitted only when the upstream reports
  that counter. Counted on transcoded paths and on the verbatim relay (one field
  behind a byte scan, so frames without usage are never parsed). **Not counted
  on the fidelity lane:** its vendor-dialect bytes remain unparsed. Use the
  vendor's billing there, or fold instead of relaying.
- `x2api_media_total{endpoint,format,kind}` — media parts the proxy ACCEPTED,
  `kind` being `image` or `file`. Counted on the folded paths, where the parts
  are already in memory (the chat identity fold included), and **never on the
  fidelity lane**: reading media out of relayed bytes would give the lane the
  vendor knowledge it exists to avoid, exactly like its tokens. It answers "is
  the multimodal traffic actually arriving" — a vendor 400 only answers that
  after the request has failed — not "what did media cost".
- `x2api_admission_queue_seconds{endpoint}` —
  admission wait for every successful permit acquisition, including immediate
  acquisitions. `x2api_stream_first_token_seconds{endpoint,format}` — TTFB for
  counted streams that relayed a frame. NDJSON `queued_ms` and `ttfb_ms` remain
  useful per-request correlates.
- `x2api_credentials{state}` — upstream keys by state, `ready` or `cooling`.
  Published by `Pool` so a fork cannot forget it, and exported only when a pool
  exists: passthrough publishes no series, so an absent name and `ready 0` are
  different facts. Traffic refreshes it via `pick`, so an idle pool reports its
  last observed state. `x2api_credential_cooldowns_total{reason}` records the
  cooldown episodes that produce those states.
- `x2api_proxy_lanes{vendor}`, `x2api_proxy_lanes_desired`,
  `x2api_proxy_lane_retirements_total{vendor,reason}` with reason
  `idle|surplus|spent`, `x2api_proxy_lane_mint_failures_total{vendor}`,
  `x2api_proxy_lane_shed_total{vendor="all"}`,
  `x2api_proxy_lane_oldest_age_seconds{vendor}`,
  `x2api_proxy_maintain_last_run_timestamp_seconds`, and
  `x2api_proxy_lane_rotation_failures_total{vendor}` — pool capacity, demand,
  upkeep, and health. `x2api_proxy_lanes_healthy{vendor,state}` uses state
  `healthy|unhealthy`. Pool-only families are absent in direct mode.
- `x2api_proxy_lane_rotations_total{vendor}`,
  `x2api_proxy_lane_failures_total{vendor}`, and
  `x2api_proxy_prewarm_total{vendor}` — rotation, transport-failure, and
  prewarm counters; prewarms are the number to multiply when pricing a metered
  warm pool.
- `x2api_proxy_lane_timeouts_total{vendor}` — response-header timeouts, counted
  and never charged to the lane. `reqwest` cannot tell a dead tunnel from a
  slow generation, so a timeout labels the VENDOR; only a connect fault (or a
  body that dies mid-flight) can retire an exit IP. A spike here is a slow
  upstream, not a dead proxy — retiring lanes on it is how three slow
  generations used to cost three healthy sessions.

What the template refuses to generalize is *policy*, and in both pools the
seam for it is code in the provider crate, not a fork of the core. The egress
bullets above are the mechanism, and the mechanism SHIPS: rotating proxy-session
lanes, sharding, health, and prewarming live in `x2api-transport`. What it cannot
know is a vendor's session syntax when no URL template can spell it (the
`ProxyVendor` seam), what that vendor's TTL unit actually means, or which
failures were the LANE's fault rather than the vendor's — get those wrong and a
healthy exit IP is retired on someone else's 429.

Upstream *credential* pools draw the same line on the same terms: the mechanism
is general (`x2api_kit::pool` — round-robin over secret-free slot indices,
per-slot cooldown, counts by state), the policy is not (`provider.rs` decides
which statuses spend a key, for how long, and how far a vendor's `Retry-After`
is trusted before being clamped).

## Vendor specials — where each quirk lives

The compliance docs surfaced real, live-behavior quirks. The rule beats the
list: **a quirk is CODE in the crate that owns the side it bites** — inbound
side → its bridge, upstream side → its provider crate, both sides → the IR
grows a typed field first. Never config knobs, never shared-crate branches.

| Special (source-verified) | Where it lands |
|---|---|
| **DeepSeek thinking mode with `tools` 400s unless EVERY prior assistant turn carries `reasoning_content`** — not only the ones with `tool_calls` (`deepseek.md` §2) | **Chat dialect: automatic** — messages relay unparsed, so a client that replays it just works (pinned by `deepseek_reasoning_content_and_tools_replay_verbatim`). Anthropic and Responses bridges now replay a present `reasoning_content`; no bridge invents the key. A present-but-empty field is vendor/provider work (G7): inject it in a provider crate's `to_upstream_body`, never in a bridge that must not know which upstream answered. |
| DeepSeek keeps `max_tokens`, adds `thinking{type}` + `reasoning_effort` (`medium`→`high` mapped), `reasoning_content` rides in stream deltas | Chat dialect: `extra` flattening passes all through verbatim; the relay already injects `stream_options.include_usage`. |
| **OpenAI deprecates `max_tokens` for o-series** (use `max_completion_tokens`); non-US regions reject `store=true` (`openai.md`) | `to_upstream_body` in a provider crate — a per-model rename in code when you care about o-series; NOT a ServerConfig knob. |
| GLM: plain-bearer works (JWT is opt-in legacy), coding-plan keys 429 `1113` on the general endpoint, text-only models (`glm.md`) | Two `config.json` bases (`/api/paas/v4` vs `/api/coding/paas/v4`) = deployment; a faithful coding-plan *Anthropic* face = its own provider crate per `dialect-matrix`. |
| **Anthropic upstream**: `max_tokens` required, `system` top-level, strict unknown-field rejection, `anthropic-version` mandatory, `content[]` envelopes (`anthropic.md` §a) | A dedicated service (R1 retarget of `crates/service`, forked per R2 as `anthropic2api` — one process, one upstream, so the vendor is the repo name, not a crate name; `provider-crate-needed` verdict; full transform list in `docs/compliance/anthropic.md` §7). Its natural shape: declare `Dialect::Anthropic` native and let Claude-style clients take the fidelity lane (full fidelity, no fold); OpenAI-style clients still go through its `complete`/`stream` translation. The chat relay alone cannot serve it. |
| omp waits 2.5 s after `finish_reason` for a trailing usage chunk (`client-omp-audit` §6) | Already satisfied: relay asks upstream for `include_usage` and the raw usage chunk rides through before `[DONE]`. |
| **Vision is a MODEL choice, not an endpoint capability.** DeepSeek gates images behind `deepseek-v4-flash-vision-exp` and 400s an image outside a `user` message (48 MiB body cap, `deepseek.md` §2); Z.AI's coding-plan models are text-only (`glm.md` §7.1) while `glm-4.6v*` takes `image_url:{url}` and its OpenAPI forbids `detail` (`additionalProperties:false`); Kimi accepts a base64 data URL but rejects https image URLs | A folded image that reaches a text-only upstream is the **vendor's** 400 — no fold can predict that, and it is deployment/`model_map`, not bridge logic. A per-vendor `detail` strip or URL rewrite is `to_upstream_body` code; the folds emit only what the client actually sent, in the chat dialect's shape. |
| **Documents fold inline only.** Anthropic `document` / Responses `input_file` become chat's `{"type":"file","file":{filename,file_data}}` — `file_data` forwards in either documented form (a `data:application/pdf;base64,` URL, or the raw base64 the guide's curl sample shows), and with a `filename`: Anthropic has no name field, so the fold derives `document.pdf`/`document.txt` from a media type it can see, and a raw payload with no name 400s rather than emitting a shape no OpenAI example shows. A `url`/`file_url` source has no chat field at all; a block CLAIMING something the wire cannot honour refuses (`citations` enabled, a non-empty `title`/`context`, `oversized_image: "error"`) while defaults fold; a Responses `file_id` passes through (the `file` object has the field and the id is the upstream's own) while an Anthropic one refuses (different namespace) | Vendor document support is narrower than vision: GPT-4o-class reads PDFs, `deepseek.md:44` documents a `file` block, most vLLM-style OpenAI-compat endpoints do not implement `type:"file"` at all — and that is the vendor's 400, not a fold decision. Real PDFs also blow past the 16 MiB inbound default: raise `max_body_bytes` (see `docs/production.md`). |
| **Two fold limits that are deployment facts, not bugs.** (1) A folded `file_id` is only meaningful in the store the upstream can actually see — a Responses `file_id` is the OpenAI-namespaced id its own Files API minted, so it passes through and works against OpenAI; pointed at DeepSeek/GLM it is someone else's identifier and answers a vendor 400. (2) Cache hints split by whether the target wire defines them: OpenAI's `prompt_cache_breakpoint` is FORWARDED onto the folded `image_url`/`file` part (chat's own parts carry that exact field), while Anthropic's `cache_control` (`{type:"ephemeral",ttl}`) has no chat counterpart and is dropped — neither is ever refused, since both would 400 ordinary cached traffic | (1) is deployment: serve a Responses client's `file_id` traffic from an OpenAI-namespaced upstream, or declare the dialect native and take the fidelity lane (where the id is never re-pointed anywhere). It is NOT a fold change — a bridge that did I/O to dereference a file would stop being a bridge. (2) is stated at both call sites (`with_cache_hint` in the responses module, `reject_unfolding_claims` in the anthropic module) so the forward and the drop are each a written decision. |
| **Gemini inbound**: the model lives in the PATH (`/v1beta/models/{model}:generateContent`), camelCase protobuf JSON (so `inlineData` and `inline_data` both arrive), `:streamGenerateContent` has **no `[DONE]`** (the terminal `finishReason` frame ends it) and is a JSON-array stream unless `?alt=sse`, `x-goog-api-key` **or** `?key=` auth, built-in tools (`googleSearch`/`codeExecution`), discovery by `listModels`/`getModel` | A wildcard path route (`/v1beta/models/{*tail}` — matchit 0.8 brace syntax, not `:param`) shared by the POST verbs and the GET listing, with the handler splitting `{model}:{verb}`. `dialects::gemini` folds text, `functionDeclarations` and **user-turn `inlineData`** through the IR — image → the chat `image_url` part, `application/pdf`/`text/plain` → its `file` part, the client's part order kept — and rejects everything the chat dialect cannot hold: built-in server tools, reasoning, `fileData`, audio/video and other inline types, media outside a user turn, a part carrying two oneof arms. `?alt=sse` is required for the stream verb (typed 400 naming it). The GET pair renders `Provider::models()` through `gemini::models_page`. A *Gemini upstream* (native `generativelanguage.googleapis.com`) is its own provider crate, not this fold: the lane hands `relay_raw` the inbound path (`CallContext::request_path`) because the model is in the URL, never in the body. The auth gate already takes both Google forms (§Client integration). |

## Services built on this template

Two exist today, each a separate program rather than a variant of this one — that
is R1's rule (`one process, one upstream`) and R2's recipe (`dest="../${slug}"`),
which is why they sit BESIDE this repo with their own git history and are not
workspace members: nothing here compiles or tests them.

Neither is hosted on GitHub and neither has a remote configured, so no hosted
copy of what they carry exists (the vendor seams are one commit in each).

| Service | What it closes that the template deliberately does not |
|---|---|
| `../deepseek2api` | G7 — injects `reasoning_content: ""` on assistant turns when the request declares `tools` (the template replays the field, never invents it) |
| `../glm2api` | G8 — reads the 200-wrapped `{code,msg,success:false}` envelope; G6 — maps GLM's entitlement codes `1113`/`1211` to a legible non-retryable refusal |

**Path notation:** a leading `../<slug>` anywhere in these docs means a SIBLING of
this repo, i.e. `projects/<slug>` — not a directory inside it (`docs/…` files are
written repo-root-relative, so `../glm2api` from `docs/dialect-matrix.md` is not a
file-relative path).

## Adding a dialect (Responses and Gemini are the proofs)

One new module in `x2api-dialects` exposing `parse → IR`, `completion →
wire`, and a stream state machine (`start/on_chunk/finish/fail` emitting
that dialect's exact event grammar). Server touches are enumerated in
AGENTS.md — the relay loop, retry, framing, and telemetry are never edited, and
the dialect label a deployment configures lives in one shared const that the
validator, the router, and the tests all read.

## Contracts worth knowing before you edit
- `Provider::stream()` / `stream_relay()` status-gate **before** handoff —
  the retry unit ends there. Post-handoff failure = terminal in-stream item,
  rendered by the
  dialect's bridge: chat = truncation chunk **without** `[DONE]`, anthropic
  = `event: error` (no `message_stop`), responses = `error` event (no
  `response.completed`). A truncated answer must never look complete.
- Client-facing errors never carry raw vendor prose: valid `{"error":{…}}`
  envelopes pass through verbatim (4xx); 5xx collapse to canned messages (full body
  → logs only). Capability gaps are typed 501s.
- `raw` carries the OpenAI-shape JSON so chat-dialect clients lose nothing
  (`reasoning_content`, vendor usage extras, tool calls). On the chat
  `stream_relay` path frames are **payload-relayed (framing normalized)**; on the buffered/`stream()`
  path they pass through a `serde_json::Value`, so structure survives but key
  ORDER is normalized — never promise byte-fidelity on buffered. The other
  dialects see normalized fields and fold text, function tools and USER-TURN
  media (an `image`/`input_image`/`inlineData` image becomes the chat
  `image_url` part, a `document`/`input_file` or a PDF-or-text `inlineData`
  its `file` part); audio, a hosted-file reference, and any media the chat
  dialect has no position for still reject, loudly.

Non-chat bridges carry typed `Usage` details without inventing unsupported
positions: Responses renders the vendor's own total (or a fallback), cached and
reasoning details; Anthropic renders cache creation/read counters; Gemini puts
cached/reasoning counters in `usageMetadata`. Refusals are successful outputs:
Responses emits a refusal content part, Anthropic plain text with its honest
terminal, and Gemini text with `SAFETY` when reported.

## Client integration (oh-my-pi) — read before wiring a client

The route table (`/v1/chat/completions`, `/v1/messages`, `/v1/responses`,
`/v1beta/models/{model}:generateContent` and its `:streamGenerateContent` twin
(`?alt=sse` required), the GET pair behind them (`/v1beta/models`,
`/v1beta/models/{model}`), `/v1/models`) is exactly the surface a single-host
gateway should expose — verified in `docs/client-omp-audit.md` (Gemini is the
newest fold on top of it). Three facts to plan around:

1. **`/v1/chat/completions` is agent-viable today.** omp's flattened
   extras + the verbatim `raw` relay carry `tools`, `tool_choice`,
   `reasoning_content`, and the post-`finish_reason` usage chunk omp waits
   for. This is the dialect to point an agent at.
2. **`/v1/messages` and `/v1/responses` are grammar-faithful and carry
   function tools through the FOLD.** The IR types tool calls
   (`ChatChunk{ …, tool_calls: Vec<ToolCallDelta> }`,
   `Completion{ …, tool_calls: Vec<ToolCall> }`), so both bridges translate
   them in both directions: inbound `tool_use`/`tool_result` blocks and
   `function_call`/`function_call_output` items become the chat dialect's
   `tool_calls` + `role:"tool"` messages; outbound they become `tool_use`
   blocks with `input_json_delta` fragments in a terminal burst (streamed
   tool fragments are held until the terminal event so a refusal keeps its
   buffered position), or `function_call` items with
   `function_call_arguments.delta`. A tool-using Claude or Responses client
   is served from an OpenAI-dialect upstream with no native face.
   What still rejects loudly: audio, `thinking`, and
   `reasoning`/`item_reference` items — plus every media part the chat dialect
   has no position for (a non-user turn, a screenshot a tool RETURNED, an
   image `file_id`, a hosted-`url` or `citations`-bearing document).
   Rejected, never silently degraded.
   For those, declare the dialect native and take the **fidelity lane**
   (zero code between a Claude-style client and a vendor that speaks its
   dialect: a DeepSeek `/anthropic` or GLM coding-plan provider).
3. **`dialects::gemini` is the fourth fold.** It folds `contents[].parts[]`
   text and `functionDeclarations`/`functionCall`/`functionResponse` through
   the IR exactly like the Responses and Anthropic bridges — a Gemini client
   against a chat-only upstream works with zero extra code. User-turn
   `inlineData` folds too: an image becomes the chat `image_url` part and an
   `application/pdf`/`text/plain` document its `file` part, in the client's
   part order. What still rejects, loudly: `fileData` (a `files/…` URI is
   Google's namespace and the chat file object has no url field), audio/video
   and every other inline media type, media outside a user turn, a part
   carrying two oneof arms, built-in server tools, and reasoning. Stream with
   `?alt=sse`, authenticate with `x-goog-api-key` or `?key=`, and
   `GET /v1beta/models[/{model}]` answers from `Provider::models()` — that one
   method gives a Gemini client discovery as well as chat. The remaining lever
   for fully faithful fold traffic is the IR growth that retires the rest of
   those rejections: typed reasoning and cache usage.

Client-side notes: ONE gate (`client_auth_gate`) is satisfied by any of the
credential spellings the served dialects document — `Authorization: Bearer`,
`x-api-key` (Anthropic's SDK default), `x-goog-api-key` or `?key=` (Google's
two forms), each compared constant-time — and CORS allows `x-goog-api-key`, so
a stock SDK passes without a bearer workaround. A credential can ride the
query, which is why the request span logs the path and never the query.
Implement `Provider::models()` or the trait's default 501 kills omp's
`/v1/models` discovery — the same call backs `GET /v1beta/models`; emitting
`supported_endpoint_types` per model gets omp's per-model dialect routing
free. `docs/client-omp-audit.md` §6-7 has the full expectation list and a
copy-pasteable `models.yml`.

## Docs map

`docs/compliance/*.md` — source-verified per-provider reports (2026-09-06).
`docs/dialect-matrix.md` — upstream × inbound-dialect NATIVE/BRIDGE/CRATE/GAP.
`docs/client-omp-audit.md` — the client side: what omp requests and what each
dialect can serve it today.
`docs/production.md` — the systemd runbook: bring-up, multi-bind, the drain
contract, simple security posture (`deploy/x2api.service` is the unit).

## Egress through proxies — warm lanes, not a client per call

Upstreams reached through a rotating proxy are the case where *connection
reuse*, not microseconds, is the whole performance story: a rotating endpoint
(`…-ttl-0`) hands out a new exit IP per CONNECTION, so every request pays
CONNECT + TLS-to-proxy + TLS-to-upstream — 100–400 ms on a WAN, which dwarfs
everything else this repo measures.

`x2api-transport` answers that with **lanes**: one long-lived
`reqwest::Client` per sticky proxy session, warmed once and rotated on a timer
while it is idle. You get rotation (N exit IPs, cycled) *and* warm
connections, instead of trading one for the other.

Its own top-level config section, parsed by the crate that owns it (the same
rule `provider.*` follows — the shared `ServerConfig` is not where optional
subsystems grow):

```json
"proxy": {
  "vendors": [
    {"name": "plainproxies",
     "url": "http://ACCOUNT-session-{session}-ttl-{ttl_seconds}:PW@dc.us-pr.plainproxies.com:1338",
     "lanes": 4, "session_ttl_secs": 600, "session_len": 6},
    {"name": "proxiware",
     "url": "http://user-ACCOUNT-network-eco-sid-{session}-ttl-{ttl_minutes}:PW@proxy.proxiware.com:1337",
     "lanes": 4, "session_ttl_secs": 3600, "session_len": 7}
  ],
  "rotate_at_pct": 80, "prewarm": true
}
```

What each part is defending:

- **`{session}` is required.** Without it every lane shares one exit IP and
  rotation does nothing, so the config is rejected at boot rather than
  quietly degraded.
- **`{ttl_seconds}` / `{ttl_minutes}`** exist because the unit is per vendor
  (plainproxies spells seconds, proxiware minutes). `session_ttl_secs` is the
  one authoritative number; the template renders it in the vendor's own unit
  and rotation uses the seconds — so the URL and the timer cannot drift.
- **`rotate_at_pct`** is how much of a session's life to spend before
  replacing it. At 80 % of a 3600 s TTL a lane retires at 48 min, and its
  replacement is minted **and pre-warmed before the swap** — no request ever
  meets an expired session or a cold handshake. A failed pre-warm keeps the
  current lane: expired-but-working beats fresh-but-dead.
- **Health is transport-only.** Three consecutive connect/timeout failures
  retire a lane. An upstream 429 or 500 came *through* a working tunnel and
  never counts, or the vendor's rate limiting would retire your whole pool.
- **No healthy lane ⇒ 503 + `Retry-After`**, never a fall back to direct
  egress: hiding the origin IP is usually the reason the proxy is there, so a
  silent bypass is worse than a retryable error.
- **Multiple vendors ride one pool.** Lanes round-robin across every healthy
  lane of every vendor, so one vendor's blocked IPs degrade capacity instead
  of stopping traffic, and health/metrics are per vendor.
- **Warming is metered traffic, so it is a `HEAD`** against the endpoint the
  provider itself resolves (never a URL rebuilt here — a base that already
  carries `/v1` would probe `/v1/v1/models` and pay for a 404). It still
  repeats on every rotation, which is the pool's standing cost on a
  bandwidth-billed plan: `x2api_proxy_prewarm_total` is there to be
  multiplied, and `prewarm` is switchable globally or per vendor.
- **Credentials never leave `mint_lane`.** The URL holds a password; a lane
  identifies itself by vendor + session id in every log line, and `Lane`'s
  `Debug` is hand-written to keep it that way.

**The pool sizes itself, because upkeep is billed.** A garrisoned pool pays
`lanes × 3600 ÷ (session_ttl_secs × rotate_at_pct/100)` rotations an hour
whether or not a request arrives — with a 60-second TTL and 4 lanes that is
~300/hour, roughly 2 MB of handshakes and probes for nothing. So `lanes` is a
**ceiling**, not a garrison:

- `min_lanes` (default **0**) is what stays up in silence. Zero means an idle
  proxy holds no sessions, pays no rotations, and sends no probes.
- The maintenance loop measures requests per minute and asks for
  `ceil(rate ÷ requests_per_lane)` lanes, clamped to `[min_lanes, lanes]`.
  Below one lane's worth of traffic that is the floor — the "don't do that
  when it's quiet" rule, in one expression (`desired_lanes`, pinned by test).
- A lane nobody has taken for `idle_retire_secs` is **retired rather than
  rotated**: rotating buys a session for traffic that is not arriving, and
  the request that eventually shows up mints its own.
- Growth is free at the moment it happens, which is what makes this safe:
  minting a lane builds a `reqwest::Client`, which is configuration, not a
  connection. The handshake still lands on the first request that uses it —
  the same handshake that request was going to pay anyway.

`session_ttl_secs` should still be the longest the vendor honours, since it
sets the rotation floor for lanes that *are* busy. The pool pays for itself
exactly when your rate is high enough that those handshakes would otherwise
be paid per request — which is what a rotating `ttl-0` endpoint charges you
unconditionally.

`server.upstream_shards` (per-vendor override: `shards`) stays on the server
surface, because it tunes the client profile `x2api-transport` builds for
direct and proxied egress alike. It is the concurrency
escape valve. HTTP/2 multiplexes every stream onto ONE connection per lane —
excellent for handshakes, but it inherits the peer's
`MAX_CONCURRENT_STREAMS` as your ceiling, and a streaming proxy holds one
stream open per in-flight generation. Sizing rule:
`lanes × shards × peer_stream_limit ≥ max_inflight`. Default is 1 and it is
usually right (4 lanes × ~100 streams already covers `max_inflight: 256`);
raise it only when lanes report healthy and requests still queue. Every shard
of a lane carries the same session, so sharding multiplies connections, never
exit IPs.

The response direction has a ceiling of its own:
`provider.max_response_bytes` caps a single buffered vendor reply per
service (default 64 MiB). Inbound `max_body_bytes` stops CLIENTS making the
proxy allocate without bound; this stops UPSTREAMS doing it. Streams need no
size bound — the request deadline is their ceiling.

## Performance defaults (structure proven here; figures inherited)

mimalloc global allocator (opt in per bin); `lto = "fat"` + `codegen-units
= 1`; HTTP/2 wherever the peer agrees (automatic over TLS via ALPN — one
handshake per lane carries every concurrent stream; plaintext h2c cannot be
negotiated, only assumed, so it is the opt-in `server.http2_prior_knowledge`);
upstream endpoints parsed ONCE at boot (`reqwest` validates a URL on every
`post(&str)`, and a bad `base_url` now fails at startup instead of failing
every request identically); SSE framing on `memchr` with one reused `BytesMut` (`split().freeze()`
≈ one alloc per frame batch); full-jitter backoff on splitmix64 seeded per
`(request, attempt)` — no `rand`, no clock syscall per retry;
`tap_io(set_nodelay)` so SSE frames don't crawl out one RTT at a time;
admission sheds overload as 503 + `Retry-After` instead of queueing into a
timeout cliff; h2 adaptive window + keepalive pings; two-stage drain (axum
graceful + hard cap) so a stuck SSE client can't pin shutdown.

Which of the above is *proven by this tree*: no per-chunk parse on the chat
relay (key-order survival test), post-`[DONE]` suppression, panic-frame
recovery, drain survival, and — since the bench grew an allocation counter —
the framing claim, at 0.2 proxy-side allocations per relayed frame. Which is
*inherited*: the Nagle RTT cost and the histogram-upkeep leak, measured in
`../opencode-free` / `../cline-proxy`, not re-measured here. Every other
local number is the bench below.

## Measured here — `bench/`

Two instruments, because one number cannot answer both questions a proxy
raises.

**`x2api-bench`** (the binary) runs the real proxy over loopback TCP against a
raw-socket mock upstream, in all three consumption modes plus a no-proxy
floor. Every timed pass is gated by a sanity probe — the bench refuses to
print numbers for a path that isn't correct.

**`cargo bench`** (criterion, `bench/benches/fold.rs`) runs the translation
with no I/O at all and decomposes the per-frame cost, because at ~1 µs/frame
end-to-end the transport noise is the same order as the work.

```sh
cargo run --release -p x2api-bench -- [--frames N] [--responses N] [--repeats N]
      [-c 1,8,64] [--pace] [--pace-us N] [--buffered]
      [--payload ascii|unicode|tools] [--proxy-connect-ms N] [--lane-cold]
      [--json] [--baseline run.json] [--max-regress-pct N]
cargo bench -p x2api-bench
```

Four axes, because one number per variant answers one question:

- **`--payload`** decides what a frame carries. `ascii` is the friendliest
  case for every JSON path in the proxy; `unicode` carries escapes and
  multi-byte characters (so serde must UNESCAPE into owned strings instead of
  borrowing, and a chunk boundary can land mid-character); `tools` adds a
  `tool_calls` delta. Measured on the same machine, `/v1/messages` costs 0.94
  µs/frame on ascii, **1.07 on unicode (+14 %, and 9.2 allocations instead of
  7.2)** and **1.34 on tools (+43 %)**. An ASCII-only bench understates the
  fold for anything not written in English.
- **`--buffered`** measures the `stream:false` path — `complete()` plus each
  bridge's `completion_to_*` — which the streamed table says nothing about,
  and which is a different code path end to end.
- **`gap p99`** is the mid-stream stall `ttfb` cannot show: when the stream
  STARTED versus whether it kept flowing.
- **`-c 1,8,64`** sweeps concurrency, printing one sub-table per level, which
  shows scaling instead of one point on a curve. Measured here: per-frame cost
  falls ~2.9× from `-c 1` to `-c 8` while `ttfb` climbs — the queueing
  trade-off, visible in one run.
- **`--proxy-connect-ms N`** charges the bench's own mock proxy a handshake
  per connection, so the `relay via proxy lane` row can price the warm pool.
  With a 50 ms handshake: **`cold ttfb` 0.39 ms prewarmed against 53.12 ms
  with `--lane-cold`** — the first request either meets a warm session or pays
  for one, and that is a 136× difference on first-byte latency. Steady-state
  `ttfb p50` is 0.3 ms either way, which is precisely why throughput alone
  could never have shown it.
- **`--baseline run.json`** compares against a previous `--json` run and exits
  non-zero on a regression beyond `--max-regress-pct` — but only when the new
  median is worse than the baseline's own WORST pass, so a busy laptop does
  not make the gate cry wolf.

Sample (M1 Pro-class, release, 2000 frames × 25 responses, median of 5
passes):

```
variant                               µs/frame       (min–max)  ttfb p50  ttfb p99   chunks    MiB/s  allocs/f   bytes/f
mock (no proxy)                           0.14       0.08–0.24      0.15      0.27       48   1351.4       0.0         0
chat  /v1/chat/completions (relay)        0.38       0.38–0.41      0.23      0.43       49    487.3       0.2       322
chat  /v1/chat/completions (fidelity)     0.30       0.29–0.32      0.22      0.32       49    619.1       0.1       289
msg   /v1/messages (transcode)            0.91       0.89–0.93      0.25      0.34       35    176.6       7.2       762
resp  /v1/responses (transcode)           1.17       1.14–1.18      0.27      0.37       41    403.7       7.3      1729
```

The `fidelity` row is the same dialect and the same bytes through the fidelity
lane (a provider declaring `Dialect::Chat` native), so the two chat rows differ
in exactly one thing: whether anything parsed the stream.

Read: `µs/frame` is the comparison and it is a median with its spread, because a
single pass swings by more than any change of interest: ±40 % is the usual bound
on an ascii row, and a `unicode` `resp` row measured 1.71 then 3.26 µs/frame —
**the same revision, the same binary, minutes apart**. `MiB/s` is per-variant
only — the dialects emit different byte volumes per frame, so across rows it
compares verbosity, not speed (responses looks 2.6× "faster" than messages at
the same per-frame cost). `ttfb` and `chunks/resp` are the *pace* columns.
`allocs/f` counts allocations on the proxy's own runtime threads only (the bench
client and mock run on a separate runtime and are not counted); it is
machine-independent and barely moves with load, which makes it the number to
watch when changing the fold.

The lazy message-item announcement (`ensure_message_opened`, plus the reasoning
item it makes room for) was therefore A/B'd on `allocs/f`, same machine and
back-to-back, at both payloads: ascii 7.3 → 7.3, unicode 9.3 → 9.4. That is the
only statement the data supports. No per-frame delta exceeds what UNCHANGED code
does on this machine: `fold/chat_frame_verbatim` moved 96.6 → 73.1 ns between two
runs of the same tree, and `sink/compat` — whose source (`CompatStream` plus the
decoder) is byte-identical across the two revisions — moved 20.2 → 16.9 ns ACROSS
them. Unchanged paths drifting 16–32 % in opposite directions, between repeats of
one build and between builds, is what makes every cross-revision percentage
unreadable in either direction. Criterion's own `p = 0.00` lines compared against
stale baselines from earlier sessions and are not evidence either. So: the added
per-chunk branch costs a branch and no allocation, and it is NOT an optimization —
it fixed a shape divergence.

`--pace` makes the mock write one frame per `write` (a real upstream's shape,
against the default single 40 KB `write_all`); `--pace-us N` adds a delay,
floored by the runtime's timer granularity — at ~1 ms/frame the µs/frame
column measures the mock's clock, and `ttfb`/`chunks` are what to read. Under
that mode the relay delivers 1001 chunks for 1000 frames at 1.8 ms ttfb: one
frame out per frame in, which is the property the fix below restored.

*Why the pace columns exist* — two bugs, neither visible in throughput:

1. The verbatim relay used to accumulate its frames in a `Vec<Bytes>` and
   flush them all at `[DONE]`. Same bytes, same order, every relay test
   green — and the best MiB/s in the table, since batching amortizes the
   syscalls the other rows pay per frame. A client saw nothing until the
   generation finished. Only `ttfb` (growing with stream length: 1.9 ms at
   2k frames, 4.4 ms at 20k) and `chunks/resp` (50 for 2000 frames, against
   2003 on the transcode rows) could see it.
2. Every streamed request then showed a flat **1.3 ms** of first-byte
   latency that the fidelity lane did not. It was one line: the relay
   generator consumed the keepalive interval's first tick before its first
   frame, and tokio's timer rounds an immediate deadline up to its ~1 ms
   granularity. The tick is only needed when keepalives are ON (the select
   arm is disabled otherwise), so it is now conditional. Per-request cost
   1390 → 188 µs; `ttfb` 1.49 → 0.18 ms; it also accounts for ~0.65 µs/frame
   of every 2000-frame row above.

Both are pinned by tests that hold an upstream open and demand the frame
(`verbatim_relay_yields_before_upstream_completes`,
`transcoded_stream_yields_before_the_provider_finishes`). A throughput
number would have shipped either bug.

Per-frame fold cost, with no transport in the way (criterion, ns/frame):

```
decode/per_frame_read      49   SSE decode, one frame per read (64-frame batch / 64)
to_ir/wire_to_chunk       348   wire bytes -> IR chunk (borrowed parse)
sink/anthropic            162   IR chunk -> Anthropic event frames
sink/responses            201   IR chunk -> Responses event frames
sink/compat                14   IR chunk -> chat frame (verbatim bytes)
fold/anthropic_frame      555   the whole /v1/messages path for one frame
fold/chat_frame_verbatim   57   the relay path: decode + reframe, no parse
```

The verbatim path is ~10× cheaper per frame *as translation* (57 vs 555 ns),
which is the fast path's reason to exist stated as a number instead of an
assertion. End-to-end that ratio compresses to ~2.4× (0.38 vs 0.91 µs/frame),
because transport, not translation, sets the floor here — and against a real
upstream over TLS (or through a proxy lane) it compresses further still.
Single client, loopback, no TLS: comparative on the author's machine, not SLO
figures — run your own before trusting any of it.

*What the lane row found on its first run:* `prewarm` was establishing the
connection and then failing to hand it back — a `send()` whose body is never
consumed leaves `reqwest` unable to reuse that connection, so the first real
request opened a second one and paid the handshake the probe was meant to
have spent. `cold ttfb` sat at the mock proxy's full 53 ms with prewarming
ON. The probe now drains its response, and a `HEAD` that a peer answers with
a body can no longer cost the pool its whole purpose.

*What the buffered mode showed on its first run:* the chat dialect's buffered
answer goes back through a `serde_json::Value`, so what the client receives
depends on that map's ordering rather than the vendor's — while the fidelity
lane, which never parsed it, returns the vendor's key order intact. The two
rows' sanity strings now report which happened, so the "never promise
byte-fidelity on buffered" rule is visible in the output rather than asserted
in a doc.

*What is left, and why it stops here.* The three parse shapes in
`bench/benches/fold.rs` bound the remaining headroom: the IR's current
borrowed struct costs 287 ns, dropping `id`/`model` (which only the FIRST
chunk of a stream reads) costs 252 ns, and leaving the text as an unparsed
`RawValue` span costs 228 ns. So `wire_to_chunk`'s 348 ns is ~287 ns of serde
and ~60 ns of IR — the parse IS the wall, and beating it means either
carrying raw spans through the IR (which would also delete the text
allocation and the sink's re-escape) or hand-rolling a scanner for one
vendor's frame shape. The second is what this bench's own motto forbids: a
fast wrong path is still wrong.

What the two instruments bought when they were pointed at the fold (same
machine, before → after): per-frame allocations 37 → 7 on the transcode rows
and 2.3 → 0.2 on the relay; `fold/anthropic_frame` 1327 → 555 ns; end-to-end
`/v1/messages` 2.93 → 0.91 µs/frame. The changes: the SSE decoder
hands out `Bytes` slices of its own buffer instead of owned `Vec`s; `ChatChunk`
keeps the vendor's frame as wire bytes and parses it through a borrowed struct
instead of a `serde_json::Value`; the chat dialect writes those bytes back
verbatim (614 → 14 ns) instead of re-serializing a document it had just
parsed; and both bridges emit their per-frame event through an `EventSink`
straight into the frame buffer, so a streamed token never becomes a `Value`.
The transcoded loop then folds every chunk the provider ALREADY has into one
write (`now_or_never`, so it never waits for more), which is the batching the
verbatim relay gets for free from reading upstream in chunks: 2003
writes/response → 35, without holding a single frame back.

(What is NOT claimed: the per-request `status.to_string()` label in
`record()` sits below measurement noise on a path that just did an upstream
round-trip; static labels are cleanliness, not a performance win.)

*The earlier rejection, revisited.* A previous pass built a single-copy SSE
framer (payload byte-ranges instead of the decoder's owned `Vec`s), measured
the relay row at 1.22–1.43 µs/frame against a 1.13–1.19 baseline, and threw it
away as noise. That verdict was correct **for the instrument it had**: the
relay's end-to-end µs/frame is set by loopback transport and cannot see a
payload copy. With `allocs/f` and the criterion fold benches the same idea is
measurable and it lands — decode −47%, relay allocations 2.3 → 0.2 per frame —
while the relay's µs/frame indeed stays put, exactly as the earlier pass
found. The lesson keeps its edge and gains a caveat: don't spend complexity on
a number your instrument cannot see, and when a change looks right but
measures flat, check whether you are measuring the level it lives on.

## Gates

```sh
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
```

The same three run as a git hook — **for commits that touch Rust**. Every step
globs `**/*.rs`, so a docs-or-config-only commit passes vacuously, by design: a
cold workspace would otherwise pay a full compile for a typo in a `.md`.
`hk check --all` is the command that runs the whole gate regardless of what is
staged, and it is the one to reach for after editing the hook itself. One
command wires the hook up on a fresh clone:

```sh
mise trust && mise install     # installs the pinned tools; its postinstall registers hk's hooks
```

`mise.toml` pins `hk` to the release `hk.pkl` amends — the two must agree, since
a floating binary lets a new release redefine a builtin default and silently
change what the gate accepts — and pins `rust` to the channel
`rust-toolchain.toml` names. That second pin is not duplication for its own
sake: measured on the machine this was written on, an unpinned mise inherited a
machine-level `rust = "latest"`, its shim outranked rustup on PATH, and the
repo's 1.98.0 became a comment while clippy ran 1.98.1. Two ways to skip the
gate exist: `HK=0 git commit …` is the deliberate one (the hook checks that
variable, so it stays installed and the bypass is at least an intentional act),
and `git commit --no-verify` is the blunt one, which also skips any hook someone
adds later. Neither is a reason the gate is decorative — an untrusted
`mise.toml` is: mise warns and moves on, `postinstall` never runs, and the repo
looks set up while every commit is ungated.

The suite needs no network: raw-TcpListener upstream mocks (the opencode-free
harness) in service tests, scripted-provider + `serve()` lifecycle tests in
the server, and grammar tests in each bridge. `docs/compliance/` holds the
dated, source-verified provider reports that future provider crates should
re-check before implementing.

## License

MIT — see [LICENSE](LICENSE).
