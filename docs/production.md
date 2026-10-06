# Production setup (systemd)

The repository ships one process: the proxy, with graceful drain, admission,
retry and metrics. Production is the OS glue around it: one unit, one JSON
file, and the env file. The behaviours
below are **measured on this tree** (multi-bind, 401 gate, upstream-failure
path, metrics render, SIGTERM drain → exit 0), not aspirational.

## Files and their single jobs

| File | Holds | Mode | Owner |
|---|---|---|---|
| `/etc/opencode2api/config.json` | policy: binds, timeouts, `client_api_key`, log/rotate | `0640` | `opencode2api` |
| `/etc/opencode2api/.env` | secrets: `OPENCODE2API_UPSTREAM_KEY`, `OPENCODE2API_UPSTREAM_KEYS`, upstream URL override | `0600` | `opencode2api` |
| `/var/log/opencode2api/` | the proxy's own rotating log (via `LogsDirectory`) | `0750` | `opencode2api` |
| `deploy/opencode2api.service` → `/etc/systemd/system/` | the unit | `0644` | root |

`config.json` holds `server.client_api_key`, so it is a secret too:
**0640 group `opencode2api`**, like `.env` at 0600 — the FILES carry the protection.
The directories are created explicitly because systemd does not retroactively
take ownership or mode of an existing `ConfigurationDirectory`. With
`ConfigurationDirectoryMode=0750`, an existing config directory keeps the
ownership/mode from step 1; if it were absent, systemd would create it
root-owned, which `User=opencode2api` cannot read. `StateDirectoryMode=0750` and
`LogsDirectoryMode=0750` make `/var/lib/opencode2api` and `/var/log/opencode2api`
`opencode2api:opencode2api` `0750`.
The loader (`opencode2api_kit::load_dotenv`) reads the path in `OPENCODE2API_ENV_FILE`
first and never overwrites a variable the environment already set — so a
one-off `OPENCODE2API_UPSTREAM_KEY=… systemctl edit` override wins over the file.

## Bring-up (run as root on the target box)

The unit is authored-**and-parsed**: `systemd-analyze verify` flagged no
directive, and that parse is re-run on every push by `gate.yml`'s `unit` job
rather than living as a one-off human memory (see the end of "What is not
automated"). It is not boot-verified from this macOS box (no systemd here);
the binary behaviours it encodes ARE run-verified: SIGTERM drain → exit 0,
bind-all-before-serve (occupied extra ⇒ boot error `binding …: Address already
in use`, no socket served), and gated 401. First action on the target after
install: `systemd-analyze verify /etc/systemd/system/opencode2api.service`.

```sh
# 1. service user (the unit pins User/Group) and explicitly-owned managed
#    dirs. ConfigurationDirectory is load-bearing: systemd does not chown/chmod
#    an existing one, and a missing one would be created root-owned 0750.
useradd --system --no-create-home --shell /usr/sbin/nologin opencode2api
install -d -m 0750 -o opencode2api -g opencode2api /etc/opencode2api /var/lib/opencode2api /var/log/opencode2api

# 2. build and install the binary; install the two shipped config
#    templates. Configs are 0640 because the proxy config holds client_api_key.
cargo build --release -p service
install -m 0755 target/release/opencode2api /usr/local/bin/
install -m 0640 config.production.example.json /etc/opencode2api/config.json
install -m 0600 .env.example /etc/opencode2api/.env

# 3. EDIT before starting: client_api_key in config.json; OPENCODE2API_UPSTREAM_URL
#    AND OPENCODE2API_UPSTREAM_KEY in .env. The env URL overrides provider.base_url,
#    so both sources must agree. The .env comments carry the other deployment
#    knobs; the full override list is apply_env in opencode2api-kit and README lists
#    the common ones. Then hand the service user its files — the unit uses a
#    stable User, not DynamicUser, so 0600 on .env is readable:
chown opencode2api:opencode2api /etc/opencode2api/config.json /etc/opencode2api/.env /var/lib/opencode2api /var/log/opencode2api

# 4. install the unit (+ the runbook), reload systemd, and enable it.
install -m 0644 deploy/opencode2api.service /etc/systemd/system/
install -D -m 0644 docs/production.md /usr/share/doc/opencode2api/production.md
systemctl daemon-reload
systemctl enable --now opencode2api
```

## Verify (the exact checks run on this tree)

```sh
curl -fs localhost:10080/health          # {"status":"ok"} — liveness, no auth
curl -fs localhost:10080/ready           # {"status":"ready"} — direct mode; lane health if pooled
curl -s localhost:10080/metrics | head   # opencode2api_requests_total … after first req
# authed call: correct key reaches upstream, wrong/missing key is 401
curl -o /dev/null -w '%{http_code}\n' -X POST localhost:10080/v1/chat/completions \
     -H 'content-type: application/json' -d '{"model":"m","messages":[]}'   # 401
curl -fs -X POST localhost:10080/v1/chat/completions \
     -H 'authorization: Bearer <client_api_key>' -H 'content-type: application/json' \
     -d '{"model":"m","messages":[{"role":"user","content":"x"}]}'          # upstream reply / 502
curl -s localhost:10080/v1beta/models -H 'x-goog-api-key: <client_api_key>'  # listModels, in Gemini's grammar
```

`/health` and `/metrics` answer on **every** bind without the client key —
that is why a public bind must not be the one you scrape. Keep the primary
`bind` on `127.0.0.1` (or a tailscale IP) and put any public face behind a
TLS reverse proxy that forwards to it.

## Multi-bind

`server.extra_binds` is a list of **full `host:port`** addresses; the same
router, admission semaphore, retry, and drain serve on all of them, and all
resolve + bind **before** the first socket listens — a bad extra fails boot
rather than half-starting the process. They do not inherit `port` on purpose:
a bare `"8081"` is rejected at validation so a typo can't silently bind the
wrong socket.

The common shape — private control plane, public data plane in front of TLS:

```jsonc
"bind": "127.0.0.1", "port": 10080,            // loopback: /health, /metrics, reverse proxy target
"extra_binds": ["100.101.102.103:10080"]       // tailscale-only second face
```

Prefer two proxy units (one per socket) if you want per-face isolation via
`ProtectSystem`/network namespaces; one proxy unit + `extra_binds` when they share
policy (they almost always do). Env override for quick tests, no file edit:

```sh
OPENCODE2API_EXTRA_BINDS="127.0.0.1:8081,[::1]:8081"   # comma list, blanks dropped
```

## Drain / restart contract (why TimeoutStopSec=40)

`systemctl restart` sends SIGTERM; the binary stops accepting, drains
in-flight streams for `server.drain_secs` (prod example: 25), then exits 0.
`TimeoutStopSec` **must exceed** `drain_secs` or systemd SIGKILLs mid-stream
and truncates live generations. Confirmed on this tree: stop → `exit=0`,
"log … draining in-flight requests".

## Security posture, deliberately simple

The user's call for this deployment: **keep security simple, no application
rate limiting.** What actually protects the box:
- `client_api_key` — one shared secret, constant-time, gates every dialect
  route on every bind. Unset = open proxy (fine behind an authenticating
  gateway or on a private network). ANY of the credential spellings the served
  dialects document satisfies it — `Authorization: Bearer`, `x-api-key` (the
  Anthropic SDK's default), `x-goog-api-key`, or `?key=` (the last two read on
  the Gemini routes, whose vendor documents them there) — and it stays ONE
  function (`client_auth_gate`) on purpose: a fork needing another form extends
  that function rather than adding a second gate. A credential can arrive in the
  query, which is why the request span logs the path and never the query
  (`PathOnlyMakeSpan`) — the key cannot reach this proxy's own logs, but a
  reverse proxy or a browser history still sees the URL, so prefer a header.
- `max_inflight` + `admission_wait_secs` — a semaphore, not a limiter: it
  bounds concurrent admitted requests, answering 503 after the wait. Listings
  share this permit and can therefore queue behind generation, by design. Rate
  limiting per-client is not wired for this shape and adding it is a separate
  design, not a checkbox here. The production example deliberately pins **8**
  permits below the library/example default of 256 to fit its 2 GiB cgroup.
- `max_body_bytes` is the request-body ceiling enforced by the router.
  `max_response_bytes` bounds buffered provider replies. The shipped values are
  16 MiB and 64 MiB. The production sizing allowance is 160 MiB/permit
  (`2 × (16 + 64)`), so `8 permits × 160 MiB = 1280 MiB`. The factor of two is
  an engineering assumption for raw+parsed overlap, **not an RSS measurement
  or strict copy ceiling**; folded IR can hold additional response copies.
  Size with
  `permits = floor(MemoryMax × 0.8 / [2 × (max_body_bytes + max_response_bytes)])`
  or lower. The 2 GiB formula permits 10, but this deployment steps down to 8
  to stay below `MemoryHigh=1536M` (9 × 160 MiB); `MemoryHigh` throttles
  allocations, it does not SIGKILL, and `MemoryMax=2G` is the cgroup last
  resort. For a box step, recompute the formula and choose a power-of-two
  permit count below it (4/8 GiB ⇒ 16/32 permits with the 20% reserve), then
  update config and both memory directives together. No RSS benchmark was
  taken on this tree, so this is a conservative capacity envelope rather than
  proof about allocator/runtime overhead.
- Streams are bounded after handoff by `stream_deadline_secs`; the
  `request_timeout_secs` wall clock bounds pre-handoff stream setup, buffered
  generation, fidelity handoff, and model listings. A listing acquires
  admission before its provider fetch and shares the same request timeout.
  Media traffic is what actually meets the request ceiling: base64 inflates a
  payload by ~4/3, so 16 MiB carries a single Anthropic-sized inline image
  (10 MB base64, their documented per-image cap) but not two. A PDF is a
  different order of magnitude entirely (Anthropic allows a 32 MB request / 600
  pages; OpenAI's image-input guide says 512 MB), so deployments that fold
  documents must raise this value deliberately and revisit the memory budget.
- systemd `ProtectSystem=strict` + no capabilities + private /tmp + device
  isolation. The process can write only `/var/lib/opencode2api`, `/var/log/opencode2api`,
  and read `/etc/opencode2api`.

## What is not automated, and why

There is no CD pipeline, and the deployment shape is the reason: the build host IS
the run host (step 2 installs `target/release/opencode2api` where it will execute), so no
artifact ever crosses a machine and a distribution matrix would have nothing to
grade. The axes that do exist are covered by CI: the compiler's floor by
`gate.yml`'s `msrv` job, the shipping profile by its `release` job
(`hk run release -c`, which is what turned `lto = "fat"` from an untested claim into
a build), and the unit files themselves by its `unit` job, described at the end of
this section. Multi-OS release binaries are not merely unused here: the drain
contract is `#[cfg(unix)]` (`router.rs`'s SIGTERM arm), so shipping a Windows or
macOS artifact would distribute something that violates a documented invariant.

Three things stay on a human, each for a reason:

- **Key provisioning.** `client_api_key` is generated on the box (step 3) and vendor
  credentials live in a `0600` `.env`. A pipeline that owns either becomes the secret
  issuer, and a pipeline that ships the config ships a client-facing secret inside a
  build artifact. R2's rsync excluding `.env*` applies the same rule to forks.
- **Ownership and modes.** Pre-created config directory `opencode2api:opencode2api` `0750`,
  state/log directories systemd-managed at `0750`, config `0640`, `.env` `0600`,
  and files handed to a stable `User=`. Wrong in either direction and the
  service either reads nothing or the key is world-readable.
- **The restart and rollback** under `## Upgrade / rollback`. A pool-backed occupied extra bind can make `/ready` fail closed with
  503 + literal `Retry-After: 5`; direct egress has no lanes and reports ready
  once the process is alive. Never use `/health` as the data-plane assertion.

The `systemd-analyze verify` claim is now a job rather than a memory. This file used
to assert the units were parsed clean in a fedora container, by hand, once — and
`StateDirectory`/`LogsDirectory`/`ConfigurationDirectory`/`ConfigurationDirectoryMode`
are genuinely the only parts of the repo whose behaviour depends on the image's
systemd, so a dated human step was the wrong owner for it. `gate.yml`'s `unit` job
runs the parser over the shipped unit in `fedora:43`, the image named above: it
installs `systemd` (the base image ships no `systemd-analyze`), stubs the
`ExecStart` path (the verifier resolves them; with nothing there it exits 1 on
`Command … is not executable`, so the stub is load-bearing rather than cosmetic), and
demands empty output. That the demand can bite is grounded in what the tool actually
says: an unknown directive is reported as `Unknown key … ignoring` — a message only a
directive typo would produce, and exactly the regression the "directives clean" claim
covers. Whether that line moves `systemd-analyze`'s exit code was NOT measured (the
observation was made without the stub, so the exit 1 there is explained by the missing
binary), which is why the job fails on the text rather than on the status: the text is
the proven half, the status is a bonus.

What stays the operator's decision is the distro SET. Automating against a second
image is a one-line matrix once someone names what they actually deploy; guessing a
distribution list buys a green on a system nobody runs, which is the thing this
section keeps having to retract.

## Upgrade / rollback

```sh
# upgrade: preserve the installed binary, then install the rebuilt one
cp -p /usr/local/bin/opencode2api /usr/local/bin/opencode2api.old
install -m 0755 target/release/opencode2api /usr/local/bin/
systemctl restart opencode2api
journalctl -u opencode2api -n 50 --no-pager

# rollback: restore the preserved binary
install -m 0755 /usr/local/bin/opencode2api.old /usr/local/bin/opencode2api
systemctl restart opencode2api
```

Config and `.env` survive restarts untouched. `deny_unknown_fields` means a
new binary reading an old config that dropped a required field fails boot —
`systemctl status` shows it immediately, which is the point: no silent
half-boot.

## Reading the logs

Two layouts, same fields: `log_json: true` writes NDJSON (machine-greppable,
and the only one that carries the request SPAN's fields), `false` writes the
human text the shipped `config.example.json` starts with.

The shape that answers questions:

- **One line per completed request**, on every lane, from one short field set:
  `request_id endpoint format stream result status duration_ms model
  prompt_tokens completion_tokens`, plus `images files` when media arrived and
  `ttfb_ms frames` on a counted stream. `result` is the whole story of a stream
  in one word (`ok|failed|truncated|panicked|dropped`) and `dropped` means the
  client hung up, not that the vendor failed.
- **`request_id` is the spine.** It is derived from the caller's numeric
  `x-request-id` when present (else minted), stamped on the response header,
  put on the request span, and used as the retry backoff seed — so
  `select(.span.request_id == N)` returns the provider's `upstream error`
  warning, the `retryable upstream failure` warnings, and the terminal line of
  the same call. BOTH layouts publish that context — json as a `"span"` object,
  the text layout as a `request{… request_id=N …}:` prefix standing where the
  target would be. What removes it is the LEVEL, never the format — see the
  `log_level` paragraph below this list.
- **Scrape noise is not logged; every client-visible refusal is.** A miss on
  `/health` or `/metrics` writes nothing (a poller would drown the file), while a
  404 or a wrong-method probe records `endpoint="unmatched"` — in the log AND in
  `opencode2api_requests_total{endpoint="unmatched"}`, because "am I being scanned" is a
  question the counter answers and the "mysteriously can't list models" ticket is
  one the log answers.
- **Absence means "not measured", or nothing happened.** An unrecorded field is
  omitted rather than written as null or `-`: a fidelity-lane stream shows no
  `frames`/`ttfb_ms`/tokens (that lane relays vendor bytes untouched by design,
  and reading them to count them is the thing it exists to avoid), a listing
  route shows no model, and a request that carried no media and never waited for
  a slot shows no `images`/`queued_ms`/`retries`. Every one of those reads as the
  same honest statement: this line says what was measured.

Timestamps are RFC 3339 to the millisecond, and with `log_tz` unset (the default)
a UTC stamp renders `+00:00`, not `Z` — one suffix regex anchored on `Z` would
miss every line, so anchor on the offset instead.

The level knob is the volume control for all of this, and it is the only one:
`server.log_level` sets the BASE level for every target (`RUST_LOG` is the
per-crate lever and accepts directives). Measured against the binary: with
`"warn"` and the same traffic that produced records at `"info"`, the rotating
file was EMPTY — not because logging broke, but because every line this proxy
writes on a healthy request is `info` — which means a restart at `warn` also
drops the `configuration` boot record and `opencode2api listening`, so the journal shows
nothing about what the process is set to do. `RUST_LOG=info` for one restart when
you need that line, rather than turning the level back on for good.
So `warn` buys quiet by giving up correlation as well: the request span is an
`info`-level span, and with it gone the provider and retry lines lose their
span-borne `request_id`. The per-request record keeps its own copy of the id
precisely so the answer survives that setting; `"off"` is available and means
what it says.


A refused start is also a log-shape fact: an unparseable line in `.env` aborts
boot naming its LINE NUMBER and nothing else (the text is a credential), and
dotenvy applies what it read before that line — so "partially loaded, then
refused" is the state, which is exactly why refusing beats warning.

Bounds, honestly stated:

- `log_keep_files` caps the NUMBER of period files, never their size —
  rotation is by UTC boundary, so a `RUST_LOG=debug` day is one huge file until
  the next one. With `log_rotate: "never"` the cap is ignored entirely (one
  unstamped file, unbounded); the boot line reports `ignored (never)` rather
  than repeating a number that does nothing.
- **`log_stdout: true` is on in both shipped examples, and the production one
  keeps it on for a reason**: the appender worker swallows every write error
  (a full disk included), so the mirror is the only witness that the file
  stream died. It is also what `journalctl -u opencode2api` shows — with `log_dir`
  configured and `log_stdout: false`, a systemd service logs NOTHING to its
  journal after the subscriber installs, and every "check the service" step in
  this runbook silently returns empty while the proxy is behaving perfectly.
  That is a real operational failure mode, not a cosmetic one: a machine with
  `/var/log` full loses the file mid-stream, and the journal is where an
  operator looks. If you do turn it off to stop the duplication, change the
  verification commands to `tail /var/log/opencode2api/opencode2api.<period>.log`.
- The journal half is bounded by journald, not by this proxy: `log_keep_files`
  counts period FILES only, and a unit-level quota does not apply to the shared
  journal, so `SystemMaxUse=` in `journald.conf` is the knob that makes
  retention mean something on that side.
- Nothing in the log is a secret by construction: a malformed `.env` aborts
  boot naming the line NUMBER only (dotenvy's own message would print the line,
  and in this file the line is a credential), request spans carry the path and
  never the query (`?key=`), lanes report vendor + session id and never a proxy
  URL, and the boot `configuration` line reports `auth=configured|open`
  instead of any key.

## What to point a monitoring system at

- `GET /health` for liveness (restarts the unit; `Restart=always` already
  does that locally — remote liveness for a load balancer).
- `GET /ready` for pool-backed egress: 503 when the lane pool cannot currently
  hand off or mint a healthy lane, so pull the instance from rotation without
  restarting it. In direct-egress mode, which the shipped production example
  uses, readiness is intentionally true whenever the process is running and
  does not probe upstream.
- Alert on the 503 ratio `opencode2api_requests_total{status="503"}` divided by all
  `opencode2api_requests_total` series. For a pool-backed deployment, sustained
  `/ready` 503 is also a signal. Labels cannot distinguish the three 503
  producers: inspect the terminal NDJSON `result` (`shed`, `failed`), its
  distinct error message, and `Retry-After` (literal `5` for admission shed,
  derived wait for exhausted credentials). `status` is not uniformly numeric:
  committed streams use `status="200-committed"` when the provider hands back a
  stream, before its first frame; this counts streams STARTED, not successes.
  Their outcomes are `opencode2api_streams_total{result=...}`.
- Alert on `opencode2api_requests_total{status="504"}`, then confirm the terminal log
  `result="timeout"`: a vendor-originated 504 uses the same labels. This catches
  proxy breaches of `request_timeout_secs` (pre-handoff stream setup, buffered
  generation, fidelity handoff, and model listings). Post-handoff
  `stream_deadline_secs` truncation appears in
  `opencode2api_streams_total{result="truncated"}` instead.
- For proxy-pool deployments only, alert on
  `opencode2api_proxy_lane_failures_total{vendor}`,
  `opencode2api_proxy_lane_rotations_total{vendor}`, and the labeled
  `opencode2api_proxy_lanes_healthy{vendor,state}` gauge; use
  `opencode2api_proxy_prewarm_total{vendor}` rate × per-probe cost for warm-pool
  pricing. `opencode2api_proxy_lane_timeouts_total{vendor}` is a VENDOR-slowness
  signal, not a lane-health one: a rate there means the upstream stopped
  answering within the read timeout, and no lane was retired for it. A
  rotation-failure rate that is not accompanied by successful rotations means
  the vendor is down — the pool then backs off from 30 s to 10 min per attempt
  rather than probing every tick, so the prewarm rate flattens on its own.
  The shipped config has no `proxy` section. Direct mode can record
  `opencode2api_proxy_lane_failures_total{vendor="direct"}` on transport failure, but
  has no pool maintenance: healthy, rotation, and prewarm families are absent.
- For credential pools with at least two keys, alert on
  `opencode2api_credential_cooldowns_total{reason}` and the separate
  `opencode2api_credentials{state="ready"}` / `{state="cooling"}` gauges as a leading
  capacity signal. A one-key pool deliberately keeps trying its sole cooled key;
  passthrough publishes no gauge; and a quiet pool retains its last published
  state rather than expiring it on a timer.
- Alert on sustained `opencode2api_admission_queue_seconds{endpoint}` p95 above the
  deployment's tolerable queue delay, and on
  `opencode2api_stream_first_token_seconds{endpoint,format}` p99 for stream latency.
  Admission samples include immediate acquisitions, so the full distribution is
  visible; first-token samples exist only for counted streams that relayed a
  frame. The buckets are 50 ms, 250 ms, 1 s, 5 s, and 15 s for admission, and
  50 ms through 3600 s for first-token latency. NDJSON `queued_ms` and `ttfb_ms`
  remain useful per-request correlates.
- `opencode2api_requests_total{endpoint,format,status}` and
  `opencode2api_streams_total{endpoint,format,result}` provide traffic and outcomes;
  `opencode2api_request_duration_seconds{endpoint,stream}` carries real duration
  buckets. `opencode2api_tokens_total` and `opencode2api_media_total` provide cost and
  accepted-media counters.
