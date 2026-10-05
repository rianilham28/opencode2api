# AGENTS.md — working rules for the x2api template

This repo is a **template**: its value is the boundary discipline below. Every
rule traces to a concrete failure in the sibling projects
(`../opencode-free`, `../cline-proxy`, `../aisix`) or to a source-verified
finding in `docs/compliance/`.

## Crate boundaries (the load-bearing part)

```
crates/
  x2api-kit                IR + Provider trait + errors + SSE + config/telemetry
  x2api-transport          egress: proxy-session lanes, rotation, shards
  x2api-dialects           the inbound folds: chat / anthropic / responses / gemini
  x2api-server             axum surface: routes, pipeline, relay, envelopes
  service                  THE program: this deployment's provider (+ thin bin)
```

1. **Bridges translate INBOUND dialects. Providers translate OUTBOUND
   vendors. Never the reverse.** A dialect module must not name a vendor
   (no `deepseek`, no `glm`); a provider crate must not know which client
   dialect arrived. The IR (`x2api-kit::types`) is the only shared tongue.
2. **The router holds only `Arc<dyn Provider>`.** A concrete provider type
   appears exactly once per service: in its `main`. Never import a provider
   crate from `x2api-server`.
3. **No vendor-specific code in the shared crates.** This workspace must stay
   generic: vendor quirks (header profiles, model forcing, JWT schemes,
   field renames) live in `crates/service/src/provider.rs`, as CODE — not as
   branches in core/server, and not as config-shaped logic knobs.
4. `x2api-kit` never depends on axum/tower (`reqwest` is allowed: the wire
   envelope is transport, not framework). `IntoResponse` lives in server.
5. **Egress is transport, not a provider, and its own crate.** A provider
   holds `Arc<Transport>` and asks it for a lane per call — never its own
   `reqwest::Client`. Proxy sessions, rotation, sharding and health belong to
   `x2api-transport` (which depends on core, never the reverse; a fork with
   direct egress still uses `Transport::direct`, and its `proxy` config
   section is parsed by that crate, not by `ServerConfig`), because `reqwest`
   bakes proxy config into the client
   (so a proxy identity cannot vary per request) and because the lanes are
   ONE warm pool shared by every provider in the process. The provider's only
   say in it: reporting which failures were the LANE's fault.
6. **A proxy vendor's session syntax is data until it isn't.** Every vendor
   seen so far puts the session id in the proxy URL's username
   (`…-session-{session}-ttl-60`, `…-network-eco-sid-{session}-ttl-60`), so a
   URL template covers them from config — and the TTL UNIT differs per vendor
   (one spells seconds, another minutes), which is why the template renders
   `{ttl_seconds}`/`{ttl_minutes}` from ONE authoritative `session_ttl_secs`.
   Implement the `ProxyVendor` trait only for a vendor that cannot be spelled
   as a URL at all (session header, port pool) — then it is code, like every
   other vendor quirk here.

## Adding things (the exhaustive lists)

**Retarget the proxy** = edit `crates/service/src/provider.rs` (endpoints,
auth `decorate`, `to_upstream_body`, stream decode). There is no crate to
copy: one process, one upstream. An upstream that natively speaks a client
dialect may additionally declare it via `native_dialects()` + `relay_raw()`
— the fidelity lane (see Contracts).

**Add an inbound dialect** = new module in `x2api-dialects` with
`parse → IR`, `completion → wire`, a stream state machine
(`start/on_chunk/finish/fail`), then exactly these touches:
`InboundFormat` variant + label, a `DIALECT_LABELS` entry, route registration,
handler, buffered-render arm in `chat_core`, error-envelope arm in
`errors::render`, `Sink` arms in `relay`,
`crates/x2api-kit/src/provider.rs::Dialect`, and the client-header spellings in
`crates/x2api-kit/src/provider.rs::relayable_headers`. The relay LOOP, retry,
SSE framing, admission, telemetry are never edited — if you're touching them for
a dialect, you're doing it wrong.

**Dialect labels are ONE list.** `x2api_kit::DIALECT_LABELS` (with the
`DIALECT_CHAT`/`DIALECT_MESSAGES`/`DIALECT_RESPONSES`/`DIALECT_GENERATE_CONTENT`
shorthands) is what `ServerConfig::validate` accepts, what `build_router` gates
each route on, and what the tests enumerate. A fifth dialect adds its label
THERE: the fourth shipped while two of the config tests still enumerated three
dialects, and a shared const is what makes a reader drift like that impossible.

## Runbook — agent-executed procedures (each run-verified or parser-verified on this tree)

### R1. Point this proxy at a different upstream

Triggers: "add a <vendor> service", "port <project> onto the template",
"make it talk to <vendor>".

**There is no copy step.** One process serves one upstream, so the program is
`crates/service` and retargeting it is a diff to `src/provider.rs` — not a
new crate, not a rename. (This runbook used to be a six-site `sed` script;
that ceremony existed only because the crate was named after a vendor.)

Edit three files, in this order:

1. **`src/lib.rs`** — `ServiceConfig`: what an operator may configure.
   Secrets and endpoints ONLY. A behavioural knob appearing here is a
   mistake; it belongs in step 2, as code.
2. **`src/provider.rs`** — the four seams: endpoint construction,
   `decorate` (auth/headers), `to_upstream_body` (IR -> vendor request),
   `decode_upstream_sse` (vendor SSE -> IR). Plus the optional ones, both
   defaulted: `native_dialects()`/`relay_raw()` for the fidelity lane, and
   `ready()` for readiness.
3. **`src/main.rs`** — config wiring only.

Anything else you "need" to edit is a design smell — reread Contracts.

Renaming is now optional and local: the binary is `[[bin]] name = "x2api"`
and the provider type is `OpenAiProvider` because that is what it implements
today. Rename either freely; nothing in the workspace depends on them.

`main.rs` wiring is two stages and both matter: `build(&server_cfg, proxy_cfg)?`
constructs the transport, then `OpenAiProvider::new(transport, cfg)?` creates the
provider. The provider owns the upstream endpoint shape, so its `probe_url()`
is passed to `transport.start(Some(probe_url)).await`; start warms and begins
rotating lanes and must complete before the server serves requests. A `base_url`
that cannot form a URL is a BOOT failure, not a 500 repeated per request.

### R2. Fork the whole template as a new project

Triggers: "new 2api project", "stand this up as its own repo".
Three patterns cover every namespace the template owns — bare `x2api` is
SAFE to replace globally because no service identifier contains it
(`service` notwithstanding: it has no `x2api` substring). Verified: zero
case-insensitive residue, and the copy compiles AND passes its own suite
from a fresh target — `cargo test`, not just `check`, because a rename must
carry BOTH sides of every equality assertion (fixture constants and their
asserts) together, and only running proves it. The residue gate is case-SENSITIVE on
purpose: a fork's own prefix (`GLM2API_`, `DEEPSEEK2API_`) contains `X2API_` under
`-i`, so an insensitive sweep passes every real fork vacuously:

```sh
slug=myproxy; upper=$(printf '%s' "$slug" | tr a-z A-Z); dest="../${slug}"
# The slug IS the project name and follows the ecosystem convention already on disk:
# every sibling is `<vendor>2api`, no hyphen (atomcode2api, gcli2api, grok2api,
# qwenwork2api, freebuff2api-rs); none is `<vendor>-2api`.
# THIS FILE IS NEVER MAPPED. AGENTS.md is procedure, not description: the slug guard,
# the residue gate's pattern, and the hazard examples below all name the TEMPLATE tokens
# because they are instructions for running this recipe, and a fork's copy must stay
# runnable. Per-line restoration of those literals was tried and failed twice (an
# example mapped into a fork says the wrong thing about what the residue is), so the
# whole file is excluded instead — which also means the check is an empty diff, not an
# exception list a future edit can silently undo. The fork's IDENTITY is mapped where it
# faces an operator: README and docs/. CONSEQUENCE to read before pasting: the runbook's
# own commands keep the TEMPLATE's package names (`cargo test -p x2api-dialects`,
# `cargo run -p x2api-bench`, `crates/x2api-kit`) because they are written to be run in the
# template; in a fork substitute this project's own prefix, i.e. `-p <prefix>-dialects`
# for whatever `<prefix>` names the repo, or the command fails as "package with part of
# that name not a member". RESOLVE REAL NAMES FROM THE BUILD (`cargo metadata` / Cargo.toml),
# never from this prose. Two severities, both covered by the same rule: `-p x2api-bench`
# is a command a fork operator pastes TODAY and
# would break, while `crates/x2api-*` appears only inside R2's own loop, which is reached
# just when someone re-runs R2 from a fork -- already unsupported. `crates/service` needs no
# caveat: it carries no template token, which is why the program crate was named that way.
# No real project name is written in this file at all: it is copied verbatim into every fork,
# so any name here becomes a foreign token in every other fork's doctrine -- the same reason
# the examples above stay template-relative.
case "$slug" in ""|*x2api*) echo "slug: non-empty, no 'x2api' inside" >&2; exit 1;; esac
rsync -a --exclude target --exclude .git --exclude local \
      --exclude Cargo.lock --include .env.example --exclude '.env*' \
      "$PWD"/ "$dest"/   # component-wise; .env* secrets never ride (include the
                         # example FIRST — rsync is first-match-wins)
for d in "$dest"/crates/x2api-*; do
  mv "$d" "$(dirname "$d")/$(basename "$d" | sed "s/^x2api-/${slug}-/")"
done
# The extension list MUST include `*.service`, `*.tmpl` and `*.pkl`: a unit that
# still exports X2API_ENV_FILE silently never loads `.env` (the prefix is derived
# from the CRATE name), askama templates are COMPILED, so a stale title that still
# says the template's old package name can never fail the build, and `hk.pkl` names
# the template in its first line while pinning the `package://` release whose
# schema it amends — the file an operator reads when a hook refuses their commit.
# None of these classes is caught by fmt, clippy, or the suite; the residue grep
# below is what makes a
# miss impossible to ship rather than something you learn from a broken
# deployment, and note that an omitted extension here fails the FORK rather than
# silently surviving: `test $? -eq 1` aborts on the match.
find "$dest" -type f ! -name AGENTS.md \( -name '*.rs' -o -name '*.toml' -o -name '*.md' \
     -o -name '*.json' -o -name '*.service' -o -name '*.tmpl' -o -name '*.pkl' \
     -o -name '.env*' -o -name '*.example' \) \
  -print0 | xargs -0 sed -i.bak -e "s/X2API_/${upper}_/g" -e "s/x2api/${slug}/g" \
      -e "s/rust-translate-proxy/${slug}/g"
find "$dest" -name '*.bak' -delete
for u in "$dest"/deploy/x2api*.service; do
  [ -e "$u" ] && mv "$u" "$(dirname "$u")/$(basename "$u" | sed "s/^x2api/${slug}/")"
done
# The residue gate MUST be able to fail: it is the only check that sees either
# class, so it is proven against a directory holding live residue and one that is
# clean before being trusted. No slug containing the template token is reachable
# (rejected above), so the pattern needs no case folding.

# EXEMPT prose by NAME rather than enumerating surfaces: a gate that greps a list
# of paths goes SILENT when a fork deletes one (grep exits 2, and `! grep` inverts
# a missing directory into a pass), while template provenance in README/AGENTS/docs
# is kept on purpose. So: whole tree, minus the files that legitimately name the
# template. A missing exclusion can only ever make the gate louder, never quiet.
# Require exit 1 EXPLICITLY: `! grep` turns a broken invocation (exit 2 — missing
# $dest, typo'd flag) into a PASS, which is the same silent-green failure as an
# over-narrow path list. Measured on this shell: no-match exit 1, bad-path exit 2.
# 0 = residue, 1 = clean, anything else = the gate itself is wrong.
grep -rqE 'x2api|X2API_|rust-translate-proxy' "$dest" \
    --exclude=README.md --exclude=AGENTS.md --exclude-dir=docs \
    --exclude-dir=target --exclude-dir=.git --exclude=Cargo.lock
test $? -eq 1   # only "no match" may continue
# The runbook is unchanged in the copy — the assertion, not the exception list:
cmp -s "$dest"/AGENTS.md "$PWD"/AGENTS.md && echo "runbook identical"   || { echo "runbook was mapped — R2 must exclude AGENTS.md"; exit 1; }
cd "$dest" && cargo test --workspace   # the gate; run it BEFORE any commit
```

What the fork must then hand-edit (the script can't have taste): README h1 +
intro in the project's own words; the first commit (`git init -b main`,
conventional lowercase subject). What it must NOT touch: the `service` crate NAME
(it carries no template token, so the global sed cannot hit it, and R1 points every
retarget at `crates/service/src/provider.rs` — there is no vendor-named crate to
rename), and sibling-project provenance in DOCS (README history,
compliance-report citations — factual history survives a rename; per
§Style it lives only in docs, never in code comments).

### R3. Point a client at the running proxy

The fidelity-lane + env-override surface changes per fork; see README
"Quick start" + `docs/client-omp-audit.md` §7 for the omp `models.yml`
shape (three provider entries, one host — the gate accepts `Authorization:
Bearer`, `x-api-key`, `x-goog-api-key` and `?key=`, so a stock SDK's own
credential header needs no workaround).

### R4. Deploy it under systemd

Triggers: "production setup", "put this on a server".
Artifacts, not scripts: `deploy/x2api.service` (parsed by
`systemd-analyze verify`; directives clean), `config.production.example.json`
(drift-pinned), `.env.example` (secrets), `docs/production.md` (the runbook:
bring-up, verify curl set, multi-bind, drain contract, simple security
posture — admission + ceilings, deliberately NO application rate limiting).
The two invariants the unit encodes are RUN-verified on this tree: SIGTERM →
drain → exit 0 (TimeoutStopSec MUST exceed `drain_secs`), and bind-all-before-
serve (occupied extra ⇒ boot error, nothing served). Ops knobs also take
`X2API_*` env overrides — `apply_env` (x2api-kit/config.rs) is the source of
truth for which; README "Quick start" lists the common ones.

## Contracts that must not regress

- **Retry unit ends at provider handoff.** `Provider::stream()` and
  `Provider::stream_relay()` status-gate before returning their stream; a
  post-handoff failure cannot be retried (the stream contract). Separately,
  `complete()` turns deterministic post-200 local parse failures into
  `ProviderError::permanent_bad_gateway`, so `is_retryable()` is false (pinned
  by `buffered_non_json_200_is_a_deterministic_parse_verdict`). Tests:
  `retry_re_sends_retryables_until_success`,
  `non_retryable_fails_fast_one_attempt`.
- **Truncated answers never look complete.** Chat dialect: error-carrying
  chunk WITHOUT `[DONE]`; Anthropic: `event: error`, no `message_stop`;
  Responses: `error` event, no `response.completed`. Grammar lives in the
  the dialect modules, not the server.
- **Client-facing error text is derived, not copied.** On folded paths, valid
  vendor `{"error":{"message":…}}` envelopes with HTTP status below 500 pass
  through verbatim; other 4xx bodies are wrapped
  `"{provider}: {status}: {snippet}"`, while 5xx bodies are canned because they
  leak shard/ARN internals (`ProviderError::from_upstream`). A post-200 in-band
  envelope follows `types.rs::in_band_error`: a trustworthy 4xx-class envelope
  is preserved verbatim and non-retryable, while 5xx-class or ambiguous values
  become a canned permanent 502 and vendor text reaches logs only. The fidelity
  lane deliberately relays non-2xx vendor envelopes untouched; capability gaps
  are typed 501s, never message-text parsing.
- **`raw` is shape-preserving.** OpenAI-shape clients receive vendor
  extras untouched in STRUCTURE (key order is normalized by the
  `serde_json::Value` round-trip on the buffered/`stream()` path); the chat
  `stream_relay()` path preserves payload BYTES (framing normalized). Tests
    assert structure survives
  (`openai_buffered_relays_raw_verbatim`,
  `relay_streams_openai_sse_end_to_end` — the latter's key-order assertion
  proves no Value round-trip on the relay path). IR's text/usage fields
  feed the OTHER dialects only. Do not promise byte-fidelity on buffered.
- **Quirks live in the crate that owns the side they bite.** The canonical
  example: DeepSeek thinking models 400 unless assistant `tool_calls` turns
  replay `reasoning_content` — on the chat dialect that survives for free
  (messages relay unparsed; pinned by
  `deepseek_reasoning_content_and_tools_replay_verbatim`), and any rename a
  vendor demands (`max_tokens` ↔ `max_completion_tokens`) is
  `to_upstream_body` code in the provider crate. Never a config knob,
  never an `if vendor == …` branch in core/server. README "Vendor specials"
  is the current map; keep it updated when a provider crate lands.
- **One guarded-stream helper.** All terminal stream accounting (outcome,
  duration, permit release) goes through `relay::StreamOutcome`'s Drop;
  outcomes are `ok|failed|truncated|panicked|dropped` (`panicked` = a first-party
  exception caught by `catch_unwind` in a folded sink or the verbatim lane's
  sink/poll/decode path, or in the fidelity lane's poll path (no decoder exists);
  converted to the dialect's failure frame where a sink exists, else
  truncation-with-prefix). `relay.rs::tests` pins the sink and both byte-lane
  shapes with `guarded_recovers_earned_frames_after_sink_panic`,
  `verbatim_poll_panic_reports_panicked_and_preserves_prefix`,
  `verbatim_in_body_panic_preserves_multiline_record_and_drops_partial`, and
  `fidelity_poll_panic_reports_panicked`. Do not add
  per-endpoint guards.
- **Metric names are defined once, per family.** Request-path names live in
  `x2api_kit::telemetry::names` — in the crate that INSTALLS the recorder,
  because `install_metrics` must describe the same strings it serves, and a
  description spelled apart from its counter gives a dashboard a series with
  help text and no data. `x2api_server::names` is a `pub(crate)` re-export for
  the handlers, an alias and not a second definition (a provider crate imports
  the public kit module); the egress family is `x2api_transport::names`. What
  is forbidden is a re-spelled literal on a RECORDING path.
- **SSE keepalive yields only between complete frames** (the relay batches
  complete frames before any comment can interleave).
- **Bridges are REJECTING, not lossy.** The folds carry text, function tools,
  and USER-TURN MEDIA: `tools`/`tool_choice`, `tool_use`/`tool_result` blocks
  and `function_call`/`function_call_output` items map to the chat dialect's
  `tool_calls` / `role:"tool"` shapes, an Anthropic `image`, a Responses
  `input_image` or a Gemini user-turn `inlineData` image becomes the chat
  `image_url` part, and an Anthropic `document`, a Responses `input_file` or a
  Gemini `application/pdf`/`text/plain` `inlineData` becomes its `file` part —
  all built by `x2api_kit::{image_part, file_part, file_ref_part, data_url,
  parts_content}`, the ONE definition of the IR's media shapes (a fold
  hand-rolling its own is how several dialects end up with incompatible "IR
  images"). Everything still unrepresentable — audio, `thinking`,
  `reasoning`/`item_reference` items,
  media in a NON-user turn, media a tool RETURNED, a hosted-`url` document
  (Gemini's `fileData`: Google's namespace, and the chat file object has no
  url field), a part carrying two oneof arms at once, a
  block CLAIMING what the wire cannot honour (`citations` enabled, a non-empty
  `title`/`context`, `oversized_image: "error"` — defaults fold), or a
  `file_id` from a namespace the upstream
  does not own — returns `Err(400)`, never a silent skip that answers from a
  history whose traffic vanished. Those limits are the chat dialect's own, not
  a preference: `system`/`assistant`/`tool` content is `string | text parts`,
  so a computer-use screenshot has no legal position and the FIDELITY LANE is
  the answer rather than an invented user turn. The one asymmetry worth
  knowing: `image_url` has no `file_id` field, so an image reference cannot
  fold at all, while the `file` object does have one — so a Responses
  `file_id` passes through and an Anthropic one is refused, because relaying
  another vendor's namespace guarantees a "not found" the client cannot
  attribute.
  A terminal `tool_use` / `function_call` is a PROMISE of blocks, so it is
  reported from what was actually EMITTED, not from the upstream's word for
  it: an upstream claiming `tool_calls` while sending none still collapses to
  `end_turn`. State both rules in any new bridge's crate doc.
- **The fidelity lane is byte fidelity, not a faster fold.** When the
  provider's `native_dialects()` contains the inbound dialect, the server
  calls `relay_raw` BEFORE any fold: client bytes go to the vendor, vendor
  bytes come back — no IR, no bridge, no lossy shapes. Rules (each pinned
  by a test in `x2api-server/tests/api.rs`): no injected keepalive comments
  (a comment between two un-parsed chunks can split an event); no
  synthesized or withheld terminators — whether the stream "completed" is
  the VENDOR dialect's semantics, so even post-`[DONE]` trailers pass
  (the chat fold's suppression rule deliberately does NOT apply here);
  non-2xx vendor envelopes relay untouched (the 4xx-passthrough doctrine
  at byte level) — with the vendor's `retry-after`, rate-limit and
  request-id headers, which `x2api_kit::relayable_headers` allowlists and the
  lane forwards. Buffered replies are read whole under the service's
  `max_response_bytes` ceiling (`UpstreamResponse::body_bytes_capped`:
  accumulate-and-fail at the crossing — a `bytes()`-then-check-length
  reading would allocate the body first and make the cap decorative).
  Framing headers (`content-length`, `content-encoding`,
  `transfer-encoding`) are NEVER forwarded: this proxy re-frames the body, so
  they would describe bytes the client is not receiving; exactly one attempt —
  a committed vendor stream must never be re-sent, so no retry wrapper on this
  lane; auth gate + admission are shared with the fold via `admit`, and the
  lane is the only caller that fills `CallContext::request_path`
  (`CallContext::for_lane`) — a dialect that puts request state in the URL
  cannot be relayed from body+headers alone (Gemini's model AND its stream flag
  live in `models/{model}:{verb}`), while the fold paths pass `None` because
  there the server parses the model out of the path and hands it down through
  the IR. A provider that needs to REWRITE
  native bodies (model aliases, quota re-scopes) does it inside its own
  `relay_raw`, in code — never by detouring through the IR. The lane is
  declared per dialect and checked first, and the dialect is DERIVED from the
  inbound format (`InboundFormat::dialect()`) rather than passed beside it —
  a mismatched pair would relay one dialect's bytes while rendering another's
  errors. A request the fold 400s on must reach the lane when the vendor is
  native (pinned by
  `fidelity_lane_keeps_tool_bodies_the_fold_must_reject`).

- **A dialect you do not serve is ABSENT, not refusing.** `server.dialects`
  filters route registration in `build_router`, so an unserved surface 404s
  through the normal envelope. Do NOT reach for Cargo features here: gating
  at compile time means `cfg` on the `InboundFormat` variants and on every
  match over them (routes, envelopes, relay sinks) to save compiling ~2300
  lines that cost nothing to carry.
- **Prewarming must DRAIN its probe.** A `send()` whose body is never
  consumed leaves the connection unreturnable to `reqwest`'s pool, so the
  next request opens another and pays the handshake the probe already spent —
  prewarming that prewarms nothing. The bench's lane row is what caught it
  (`cold ttfb` unchanged with `prewarm` on); keep that row honest.
- **`/health` and `/ready` answer different questions.** `/health` is the
  process; `/ready` is "can I serve", and with a pool configured it reports
  the egress. Keep `Provider::ready()` OBSERVATIONAL — a probe runs every
  few seconds, so it must never mint a lane, connect, or send anything
  upstream, or readiness itself keeps an idle pool alive and billable.
- **Token counts follow parsing.** `x2api_tokens_total` is fed wherever a
  path already reads the frame: both transcoded folds, and the verbatim relay
  via `usage_from_frame` (a byte scan first — `"usage":null` rides EVERY
  chunk when `include_usage` is set, so matching the field name alone would
  parse every frame and defeat the fast path). The fidelity lane stays
  uncounted BY DESIGN: its bytes are in the vendor's dialect, and reading
  them would give the lane the vendor knowledge it exists to avoid. Say so
  rather than quietly adding a parse there.
- **Media counts follow folding.** `x2api_media_total{endpoint,format,kind}` is
  incremented in `chat_core`, after a fold has produced the IR, over the parts
  the counting walk already visited — bridges never learn that an operator is
  watching. The fidelity lane stays uncounted, by the same rule as its tokens;
  the chat dialect is counted too, because its parts are already `Value`s by
  the time `chat_core` runs.
- **Egress lanes fail closed.** With a proxy pool configured and no healthy
  lane, the provider gets a 503 + `Retry-After` (same shed as admission) and
  the request dies there. Never fall back to direct egress: for most
  deployments hiding the origin IP is the entire reason the proxy exists, so
  a silent bypass is worse than an error the client can retry.
- **Admission covers listings too.** `/v1/models` and `/v1beta/models` acquire
  an admission permit before their provider fetch, so they can queue behind
  generation by design. `request_timeout_secs` covers pre-handoff stream setup,
  `relay_raw` handoff, and listings; after handoff, `stream_deadline_secs`
  remains the stream ceiling.
- **Only transport failures are the lane's fault.** `Lane::note_failure` is
  for CONNECT and tunnel errors, plus a body that dies mid-flight. An upstream
  429 or 500 arrived THROUGH a working lane and says nothing about the exit IP
  — attributing those would retire healthy lanes every time the vendor
  rate-limits you. Three consecutive transport failures retire a lane; the
  maintenance loop replaces it.
- **A slow upstream is not a dead exit IP.** `reqwest` reports the read
  timeout (the 120 s header wait) as a timeout and cannot tell it from a dead
  one, so `classify_send` splits them: a connect fault is the lane's, a
  post-connect silence is the VENDOR's and goes to `Lane::note_slow`, which
  only counts `x2api_proxy_lane_timeouts_total{vendor}`. Charging those to the
  lane retires three healthy sessions for three slow generations and buys
  three fresh ones each time.
- **Warming costs quota.** The probe is a `HEAD` on an endpoint the PROVIDER
  resolves (`probe_url()`), never a URL rebuilt in the bin — that is how the
  first version paid for `/v1/v1/models` 404s. It repeats per rotation, and it
  runs once per SHARD (`Lane::client()` round-robins, so warming one shard
  leaves the rest paying the handshake the prewarm exists to remove). So on a
  bandwidth-billed proxy it is a standing cost of `lanes × shards` probes per
  rotation: count them (`x2api_proxy_prewarm_total` counts PROBES, not lanes),
  and let a short-TTL vendor opt out — per vendor, honoured by both the boot
  sweep and the rotation warm.
- **Lanes follow demand; `lanes` is a ceiling.** The pool holds `min_lanes`
  (default 0) in silence, sizes itself from observed requests per minute
  (`desired_lanes`, pure and tested), mints on demand when a request finds
  nothing healthy, and RETIRES an idle lane instead of rotating it. This is
  safe only because minting is configuration, not a connection — keep it that
  way, and never move I/O into `mint_lane`.
- **Rotate before expiry, warm before serving.** A lane is replaced at
  `rotate_at_pct` of its TTL, and the replacement is pre-warmed BEFORE it is
  swapped in, so no request ever meets an expired session or pays a CONNECT +
  TLS handshake. A failed pre-warm keeps the current lane: expired-but-
  working beats fresh-but-dead.
- **A proxy URL is a credential.** It is composed on demand inside
  `mint_lane` and never stored, logged, or attached to an error. A lane
  identifies itself by vendor + session id — `Lane`'s hand-written `Debug`
  exists for exactly this reason, so don't derive it.
- **Sharding multiplies connections, never sessions.** Every shard of a lane
  carries the SAME session, so the exit IP is one IP however many connections
  serve it (pinned by `shards_multiply_connections_not_sessions`). Shards
  exist because one h2 connection multiplexes every stream and inherits the
  peer's `MAX_CONCURRENT_STREAMS` as our ceiling; default 1 and raise it only
  when lanes are healthy and requests still queue.

- **Secrets go in `.env`, shapes go in JSON.** `load_dotenv()` runs FIRST in
  every `main`, fills only what the shell did not export (a real env var
  always wins), and treats a missing file as silence. Never log a value it
  loaded — the path only. A proxy URL is a credential, so a `proxy.vendors`
  array with live passwords does not belong in a committed `config.json`.


- **The terminal record is the SHORT one, by default and with no knob.** Its
  fields are `request_id endpoint format stream result status duration_ms model
  prompt_tokens completion_tokens`, plus `queued_ms` and `retries` when they were
  non-zero (who waited for a slot, and how many extra upstream attempts it took —
  the two questions `duration_ms` alone cannot answer), `images files` when media
  actually arrived, and `ttfb_ms frames` on a counted stream. Two things were cut because
  they cost bytes and answered nothing: zero media counts, and `model: "-"` for a
  route that parsed no model. `format` was a candidate and is NOT:
  `/v1/models` (OpenAI) and `/v1beta/models` (Gemini) share one
  `endpoint="models"`, and although the text layer does print the span's
  `dialect`, surviving that answer must not depend on a span the operator can
  filter away — at `log_level: "warn"` the span is gone and `format` is the only
  thing left that says which listing was called.

  `relay::Done` is the only place that says a request ended: the buffered paths
  call `emit` (metrics + line), a stream's line comes from `StreamOutcome`'s
  Drop through the SAME struct. A second per-endpoint log call, or a different
  field name for the same fact, is the drift a reviewer cannot see in a diff.
  An unmeasured key is ABSENT, never
  `null` and never `-`: the fidelity lane reports no model/tokens/frames by the
  same BY-DESIGN rule as its metrics, and `tracing` drops an unrecorded `Option`
  rather than printing null (`every_lane_writes_the_same_core_and_only_what_it_measured`,
  `an_unmeasured_field_is_absent_rather_than_zero_or_dash`).
- **`request_id` is derived ONCE and carried by the span.**
  `x2api_kit::util::request_id_of` is the single derivation (the caller's
  numeric `x-request-id`, else a mint): the middleware puts it on the request
  header, `PathOnlyMakeSpan` puts it on the request SPAN, and `CallContext` plus
  the retry backoff seed take that same number, so the retry warnings and the
  lines a handler logs need no field threaded through the call graph. The ONE
  exception is deliberate: `errors::gate_response` takes `request_id` as an
  argument, because it logs at WARN and therefore survives a level that disables
  the span — the line holding the vendor's own explanation must not be the one
  that loses its attribution. The span is TWO fields, `path` and `request_id`:
  everything else on it was the event's data restated (145 bytes of a measured
  456-byte line), and `record` on an undeclared field is a silent no-op, so
  `the_request_span_carries_id_and_path_and_nothing_else` asserts the removed keys
  stay absent. The span MUST stay `info`-level: a span the filter disables has
  nothing to attribute
  (`an_info_span_attributes_every_json_line_and_a_debug_span_does_not` exists to
  stop someone demoting it). The converse is decided too: `log_level: "warn"`
  disables that span ON PURPOSE and the span-borne attribution goes with it —
  that is the trade, not a bug, and "fixing" it with `add_disabled` or level
  games would fight EnvFilter's precedence. The terminal record is the line that
  still identifies a request at any level, because it prints the fields itself.
- **Upstream credentials rotate in the provider, and only quota answers spend
  them.** `x2api_kit::pool::Pool` holds slot INDICES and cooldown instants — never
  a key, because the plausible future use of a pool is to log or Debug it. The
  policy (429 → its `Retry-After`, 401/403 → longer, 400/500 → nothing) is CODE in
  `provider.rs::cooldown`, same rule as every other vendor fact; a shared crate
  must not learn which status means "this account". Three invariants each have a
  test because each is a plausible-looking "improvement" that is wrong: a pool of
  ONE must still be used while cooling (routing around a route that does not exist
  turns the vendor's retryable 429 into our own invented 503 and disables the
  retry loop), a PASSTHROUGH caller token has no slot to spend (cooling it punishes
  this client for another client's quota), and `decorate` returns the slot it used —
  a second pick in the error path cools an innocent key while the guilty one keeps
  taking traffic, silently and forever.
- **A cost figure is a projection of the record, never a second sink.** The
  per-model usage card reads the same parsed `request completed` lines the table
  renders, summed across EVERY file and filtered on the record's own date: a
  separate `usage.jsonl` would be a second source of truth with its own rotation
  question, and a month total taken from the newest file alone reads as
  authoritative while under-reporting. Partial must be reported (`~partial`) rather
  than summed quietly.

- **The log file's name is configuration, not the crate.** The program crate is
  `service` in every fork (R2 renames only the `x2api-*` crates), so all of them
  would write `service.<period>.log`, and pruning is prefix-scoped — two
  deployments sharing a `log_dir` cull each other's files. `server.log_prefix`
  is the knob; empty or path-shaped values fail boot, and `log_rotate: "never"`
  bounds NOTHING (the boot line reports `ignored (never)` rather than repeating
  a cap that is ignored).
- **A malformed `.env` stops the boot and never quotes the line.** Absent file =
  silence (documented above); a file that exists but cannot be parsed must fail
  loudly, because the alternative is a process serving upstream-auth 5xx forever
  with a config the operator believes is fine. dotenvy's own error interpolates
  the offending TEXT — in this file, a credential — so `load_env_file` reports
  the line NUMBER and withholds the text
  (`an_unparseable_env_line_fails_boot_without_revealing_its_text`).

## Testing

- A proxy lane is proved at the WIRE, not in string tests: point the upstream
  at a black hole, make the mock the proxy, and decode its
  `Proxy-Authorization` (`proxy_lane_puts_a_minted_session_on_the_wire`). A
  template that renders correctly but never reaches a socket is not evidence.
- Upstream fakes are **raw `tokio::net::TcpListener` mocks** (zero mock libs)
  in service-crate tests; server tests script the `Provider` trait directly
  over `build_router + oneshot`. Don't introduce wiremock/mockito.
- Every behavior added to a bridge or provider needs one test at the seam it
  defends; the suite must pass with no network: `cargo test --workspace`.
- `bench/` (workspace member, dev tool) is two instruments; run BOTH after
  changes to the relay loop, framing, or bridges:
  - `cargo run --release -p x2api-bench` — the real proxy over loopback TCP.
    Its sanity probe gates every timed pass (numbers for an incorrect path are
    refused, not printed). Read `µs/frame` as a median with its spread, never
    `MiB/s` across rows, and treat `ttfb`/`chunks/resp` as the pace check —
    they are what caught a relay that buffered whole streams while posting the
    table's best throughput. `allocs/f` is proxy-thread-only and stable; it is
    the number to watch when touching the fold.
    Vary `--payload` before trusting a fold number: `unicode` (escapes force
    serde into owned strings) costs ~14 % more than `ascii` and `tools` ~43 %,
    so an ascii-only reading understates real traffic. `--buffered` measures
    the `stream:false` path the streamed table never touches. `--baseline
    run.json` turns the whole thing into a gate.
  - `cargo bench -p x2api-bench` — criterion, no I/O, decomposing the
    per-frame fold. Wall-clock end-to-end cannot resolve a change to the
    translation; this can. A change that moves neither is not an optimization.
  Treat outputs as comparative deltas on your own machine before writing them
  into any document; the README's performance table is a snapshot, and the
  structural claims it supports live here.
## Style

- `parking_lot::{Mutex, RwLock}` when guards are never held across `.await`;
  `tokio::sync` otherwise.
- Comments explain WHY: state the DURABLE technical reason (or cite the
  compliance doc) — never sibling-project history; "`opencode-free did X`"
  goes temporal the moment the template is forked, the reason does not.
  Never restate WHAT the code does. Never delete a comment unless proven
  false.
- Config split is law: `server.*` = template surface (parsed by core),
  `provider.*` = service-owned (parsed in the service crate). New shared-knob
  needs go through `ServerConfig`; vendor knobs NEVER do.

## Docs

- `README.md` — the flow diagram + how to add a service; keep the diagram in
  sync with `crates/` on every dialect/service addition.
- `docs/compliance/*.md` — source-verified provider reports; treat claims as
  dated (fetched 2026-09-06); when implementing a provider, re-check drifted
  facts (model ids, endpoints) against the live docs before coding around them.
