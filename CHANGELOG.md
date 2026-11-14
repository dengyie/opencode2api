# Changelog

All notable changes to this maintained edition are documented here. The
upstream project (jasonxu114514/opencode2api) has its own release notes; this
file tracks what this repository adds on top of the imported baseline.

## v1.7.3 (2026-11-14) — hot-path performance and memory optimization

Pure performance and observability release; no external behavior changes.
Benchmarked results: the stream usage observer allocates 95.6% less per long
stream (35.5 MB → 1.58 MB on a ~150 KB, 512-frame SSE stream) at 4.1× the
throughput, and the v1 Anthropic beta header is derived once per body instead
of once per key attempt.

### Changed

- **Adaptive encode-buffer pooling** (`internal/jsonutil/encode.go`): `Marshal`
  borrows its buffer from a `sync.Pool` and copies the result out for buffers
  up to 256 KiB; larger buffers travel with the returned bytes instead of
  being pooled (copying a megabyte payload per call costs more than a fresh
  allocation, and pooling one pins megabytes per concurrent request). Small
  encodes drop one allocation and 20% of bytes per call.
- **Lazy anonymous-lane shaping** (`internal/gateway/upstream.go`,
  `internal/gateway/warp.go`): the anonymous exit walk shapes each upstream
  gate's body on first use (`sync.Once`) instead of pre-shaping every gate
  before the walk starts, so a walk that stops early never pays for bodies it
  never sent.
- **Beta-header cache** (`internal/gateway/upstream.go`): the v1 Anthropic
  lane's `anthropic-beta` derivation is memoized over the shaped body (a
  SHA-256-prefix keyed map bounded at 64 entries, least-recently-seen
  eviction); key rotation replays the same body once per attempt and only the
  first attempt pays the full typed decode.
- **Zero-copy stream tracking** (`internal/protocol/stream.go`): the usage
  observer scans SSE frames in place over the retained buffer instead of
  copying every frame out and re-reading it through a fresh scanner; the
  buffer holds only the unterminated tail, so a long stream costs O(frame)
  memory instead of O(stream).

### Added

- **pprof endpoints** (`internal/admin/server.go`): when
  `performance.pprof_enabled` is true, the net/http/pprof handlers mount under
  `/debug/pprof/` on the WebUI admin listener only, behind the same session
  auth as every other API route. Off by default.
- **Soft memory limit** (`cmd/opencode2api/main.go`): when
  `performance.memory_limit_bytes` is positive, the runtime applies
  `debug.SetMemoryLimit` at startup so GC works harder as total memory
  approaches the ceiling. 0 keeps the Go default.

## v1.7.2 (2026-11-14) — client masquerade version sync

Tracks the official OpenCode client releases and keeps the v2 lane's
masquerade version aligned with them, the dynamic-version pattern from
zcode2api's version tracker. Detection is always on; applying is opt-in.

### Added

- **Client-version tracker** (`internal/gateway/versiontracker.go`): a
  background loop (default 12h, `performance.version_sync.interval_seconds`)
  polls the official release feed, parses the newest stable release (exact
  `v<numeric semver>` tags only — component tags like
  `process-daemon-v0.5.2`, drafts, and prereleases are ignored), and records
  the outcome in `/healthz` under `version_sync`.
- **Atomic v2 version** (`httpx`): `v2ClientVersion` moved behind an atomic
  with `V2ClientVersion`/`SetV2ClientVersion`; `V2UserAgent` reads it on
  every render, so an applied update reaches new requests without a restart.
- **Ceiling clamp**: an auto-applied release never crosses
  `performance.version_sync.ceiling` (empty = the compile-time claim). The
  gateway's other fingerprint constants — the v1 client version, Bun
  runtime, and per-protocol provider-utils pairings — were captured from one
  release with no public mapping, so a newer claim is unverified. Above-
  ceiling releases are logged as `version_update_above_ceiling` and marked
  `pending` in healthz; the operator verifies the release against the v2
  gates and then raises the ceiling or rebuilds.
- **Config** (`performance.version_sync`): `interval_seconds` (0 = 12h),
  `auto_sync` (default false — detect and log only), `ceiling` (must be
  numeric; a `v` prefix is stripped). Invalid ceilings fail config load
  rather than silently disabling the clamp.

### Unchanged

- The v1 lane's masquerade is deliberately not auto-synced: its client /
  Bun / provider-utils version pairing has no public mapping and only a
  captured request of a real release can provide the next triple.

## v1.7.1 (2026-11-14) — credential quarantine hardening from the production review

Follow-up to v1.7.0's production code review. No behavior change on the
production shape (zen-only pools); the fixes close conditional paths and
lock them with tests.

### Fixed

- **Sentinel priority across tiers** (`doUpstreamTiers`): when the first key
  tier ends in the credential sentinel and a later tier fails with a bare
  transport error (no HTTP response), the sentinel was overwritten in
  `lastErr` and the caller saw the generic 502 `upstream_error` instead of
  the credential-exhausted 503 + code `key_pool_credential_exhausted`. The
  sentinel now survives cross-tier failover (a later tier's real response
  still wins — the response-over-error convention is intact, and the 401
  never leaked either way).
- **Quarantine saw-tooth** (`RepinCredentialQuarantine`): a key the upstream
  keeps rejecting expired out of the wall between two probes, re-entered
  rotation, and the next live request re-took the 401 that synthesizes a
  503. A probe that still sees the key rejected while the quarantine is
  within one probe interval of expiry now rolls the wall forward, so a
  dead key stays out until a probe (or a real request) sees the upstream
  accept it again. A successful probe clears everything as before.

### Tests

- `TestE2ECredentialSentinelSurvivesGoTierFailure`: zen (all 401 → sentinel)
  + go (transport-level dead POST) two-tier walk; asserts the 503 with the
  credential-exhausted code — verified to fail with 502 without the fix.
- WARP lane guards (`TestWARPResult*`): a 429 or network-abuse 403 on the
  WARP lane must not mark the key at all (exit-scoped), a real 401
  quarantines exactly like the direct lane, and a success clears it.
- `TestRepinCredentialQuarantine`: near-expiry rolls forward and keeps
  `cooldownUntil` covering `credUntil`; far horizons, expired walls and the
  disabled (`0`) quarantine are untouched.
- The v1.7.0 E2E now pins the Retry-After range (`1..quarantine_seconds`)
  instead of only its existence.

## v1.7.0 (2026-11-14) — key credential quarantine: 401/403 handled inside the pool

The 2026-11-14 production incident: a zen key had been rejected by the
upstream with `401 Invalid credential` since 11-09, but the gateway only
applied the short exponential cooldown (≤120s), so the dead key re-entered
rotation every two minutes, kept getting 401'd, and — when it was the last
key standing — the upstream 401 reached the fronting gateway (New-API),
which auto-bans a channel on a forwarded 401. The channel went dark while
five healthy keys existed one rotation away.

The 429 quota wall already has a complete closed loop (quotaUntil +
probe recovery + all-cooled pre-check); v1.7.0 gives credential rejections
the same semantics.

### Added

- `performance.credential_quarantine_seconds` (default **86400**, range
  0–2592000): how long a key stays out of rotation after the upstream
  rejects *the key itself* with a 401/403. `0` restores the legacy
  plain-cooldown behavior.
- Credential pre-check: when every key of a tier is quarantined, requests
  are answered with a synthesized `503` + `Retry-After` (earliest quarantine
  expiry) and code `key_pool_credential_exhausted` with **zero upstream
  calls** — the upstream 401 never reaches a caller.
- Probe recovery: quarantined keys are re-tested by the quota probe loop
  (`ProbeDueNodes` now covers both the 429 and the 401/403 classes); a
  probe that reaches 200 clears the quarantine immediately.
- healthz `keys.credential_quarantined` count (informational; a
  quarantined key does not affect readiness).
- Events: `key_credential_cooled` (live 401/403, both lanes incl. WARP),
  `quota_probe_still_credential_dead` (probe against a still-dead key),
  `credential_precheck_blocked` (zero-attempt rejection).

### Changed

- `MarkFailure` quarantines on 401/403 instead of the exponential cooldown:
  `credUntil` pins `cooldownUntil` to the quarantine horizon (an upstream
  `Retry-After` is meaningless for a dead key and is ignored), the failure
  streak resets so the key returns clean, and `quotaUntil` is untouched —
  the two failure classes stay independent.
- `MarkSuccess` and `MarkListingSuccess` clear `credUntil` (a listing
  proves the credential authenticates).
- When the last upstream response on a key tier was a 401/403 (no other
  key left to try), the request exits with a synthesized 503 instead of
  forwarding the 401.

## v1.6.1 (2026-11-10) — event names that survive the log redactor

Deploying v1.6.0 surfaced a logging trap: the TetherPool gateway's
conventional proxy password is the literal string `tetherpool`, and the
log redactor loads proxy userinfo passwords as secrets — so every event
named `tetherpool_*` was written to the log as `***_*`. The events fired
(healthz proved the provision) but were ungreppable by name, which defeats
the log-driven operations this gateway is run by.

### Changed

- Events renamed to a `tp_` prefix that shares no substring with the
  proxy credential: `tp_provisioned`, `tp_fallback`, `tp_reprovision`,
  `tp_reprovision_failed`, `tp_rebind`, `tp_rebind_failed`,
  `tp_duplicate_key_id`. Messages reworded to avoid the credential too
  ("bindings provisioned", "egress resolve failed; keeping configured
  proxies", ...). The `account_id`/`key_id` attributes keep their values:
  the redactor scrubs whole userinfo strings, not the short display id.

## v1.6.0 (2026-11-10) — TetherPool Mode B integration (optional)

Deep integration with the TetherPool (IP-Pool v2.1) control plane as an
*optional* egress manager. When enabled, every Zen key gets its own sticky
egress binding allocated over the REST API and a quota wall on a key can be
reported back so the control plane rotates that binding to a fresh exit.
Every failure path degrades to the statically configured proxies, so the
integration is an enhancement, never a dependency.

### Added

- `tetherpool.*` configuration section (default **disabled**): `base_url`
  (default `http://127.0.0.1:9100`), `service` (default `opencode2api`),
  `token` (optional bearer, redacted from logs), `timeout_seconds` (default
  10, bounds the whole resolve), `rebind_on_wall` (default **false**),
  `rebind_min_interval_seconds` (default 600) and `regions` (cycled by key
  position). Range checks bind only while enabled.
- Startup provisioning (P1): `RuntimeController.build` allocates one binding
  per Zen key (`oc2api_<display id>`) and leads the proxy list with the
  returned gateway URLs, so the positional nodePool binding pairs every key
  with its own egress and the configured proxies trail as failover spares.
  The resolve is all-or-nothing — a partial list would shift every later
  key's binding. Bindings are cached per (key, region) for the process
  lifetime, so config applies do not re-hit the control plane.
- Static fallback + self-heal (P1): an unreachable control plane keeps the
  configured proxies (`/healthz` renders `tetherpool.mode =
  "static_fallback"`) and a background worker retries the resolve every five
  minutes until it succeeds. A build never fails because of TetherPool.
- Wall feedback (P1, behind `rebind_on_wall`): a 429 observed on a Zen key —
  on live traffic or, while a fully walled pool pre-check-blocks every
  request, on the quota probe's still-walled branch — queues one battery
  report per key per `rebind_min_interval_seconds`; the dispatch worker POSTs
  it (`rate_limited_429`, `force_rebind`) so the control plane rotates the
  binding to a fresh egress. The free-tier wall is scoped per account AND per
  egress IP, so a rotation lifts only the IP-side component.
- Observability: events `tetherpool_provisioned`, `tetherpool_fallback`,
  `tetherpool_reprovision(_failed)`, `tetherpool_rebind(_failed)` and an
  optional `tetherpool` section in `/healthz` (`mode` plus per-key
  account/node/region/exit_ip/cached). `static_fallback` is informational
  and does not affect readiness.

### Behavior compatibility

- With the section absent or `enabled: false`, nothing changes: no new
  goroutines, no control-plane calls, no healthz section. A config carrying
  the section requires this binary (`DisallowUnknownFields`), so deploy the
  binary before enabling.
- Provisioned entries are injected into the resolved proxy list only; they
  are never persisted to config.json and the anonymous lane keeps sharing
  the transports as before.

### Hardening (post-review)

- `Acquire` builds its query with `url.Values` — the account id derives from
  arbitrary key material, so characters that are not query-safe must arrive
  escaped.
- `tetherpool.rebind_on_wall` without `tetherpool.enabled` is rejected at
  load: the dispatch worker only exists while the integration is enabled, so
  the combination would drop every report into an unread channel.
- The provision snapshot is stored only after the runtime build succeeds; a
  failed apply no longer overwrites the worker's and `/healthz`'s view.
- Duplicate key display ids (two keys sharing one account and egress) are
  warned about at resolve time instead of silently sharing a binding.
- `Shutdown` takes `updateMu`, so its cancel-field reads cannot interleave
  with an apply (this also closes the pre-existing window around
  `quotaProbeCancel`). The re-provision interval moved from a package
  variable into the controller.

### Tests

- Hermetic coverage end to end: config defaults/validation, client contract
  (acquire/battery/token/errors), per-key 1:1 provisioning shape, region
  cycling, all-or-nothing fallback, cache reuse, build fallback on an
  unreachable plane, wall-report gating/deduplication and the dispatch
  worker's battery body, query escaping, the config round-trip dropping
  provisioned entries, the duplicate-display-id warning and the worker's
  self-heal reload from `static_fallback` to `api`. `go test ./... -race
  -count=1` green.

## v1.5.7 (2026-11-07) — bound the walled-lane probe

Follow-up to v1.5.6 from the next day's request log (644 routed, 91.3%
200). The fast failover works — 418 events, one anon attempt then the key —
but the probe itself was left at the full 30s attempt budget, and a *walled*
exit is not a fast exit: measured 429s from the lane took 1.7–21.4s. A single
request paid 17.1s for the probe and 12.8s for the key, 29.9s of a 30s
first-token budget, and the caller's gate cut it with 429/499.

### Changed

- `performance.wall_probe_timeout_seconds` (default **5**, 0 = off): the
  attempt timeout for the one anonymous probe made while the wall fast
  failover is armed, applied on both the proxy-backed and WARP walks. The
  probe is now abandoned about 5s in and the key tiers keep ~25s of the
  caller's budget instead of competing for what is left of it. Requests where
  the walk is not capped are untouched, so serial exits still get the full
  `attempt_timeout_seconds`.
- The `anon_wall_fast_failover` event carries `attempt_timeout` so the
  remaining budget is visible next to `exits`.
- `anon_wall_fast_failover` reads "capping the exit walk for a key
  failover".

### Not addressed

The ~30s 499s with no key leg in `legs` are a different shape: the walk ran
uncapped (2–5 exits), so by the time the anonymous walk finished no key was
usable, and the caller's gate took the request first. That is the degenerate
wall (free tier walling keys and exits together), which the full cap still
covers by design — this release only stops a *slow probe* from eating the
key's share when a key can serve.

## v1.5.6 (2026-11-05) — anonymous-wall fast failover to the key tiers

Production traffic analysis showed the anonymous lane's exit walk defeating
the key fallback: while the lane is 429-walled, each exit attempt costs
seconds (observed 1.7–26s), so a request could spend its entire
fronting-gateway first-token budget walking up to six exits before the key
tiers were ever consulted — the key fallback then never completes and the
caller cancels.

### Changed

- The anonymous exit walk (proxy-backed and WARP paths alike) caps itself at
  one attempt when the empirical wall signal is armed (≥2 lane 429s within
  the 10-minute window) **and** at least one Zen key is currently usable.
  One attempt still catches the wall lifting; with no usable key the walk
  keeps its full cap, because the exits are then the only lane that can
  answer.

### Added

- `config_attempt_timeout_covers_request` (WARN at startup): fired when
  `performance.attempt_timeout_seconds >= retry.timeout_seconds` — the
  misconfiguration that cut established streams at exactly 30s in production
  before the 2026-11-05 timeout split.

## v1.5.5 (2026-11-05) — quota state visibility

An 11-hour production soak exposed three diagnostic blind spots, all of them
in log coverage rather than routing: a fully quota-walled key pool produced no
trace at all, the probe loop's skip paths were silent, and a 502 caused by a
body read cut short by the caller's deadline was indistinguishable from a real
upstream failure.

### Added

- `key_quota_cooled` (INFO): logged when a keyed 429 marks the key
  quota-cooled, with `key_id`, `tier` and the adopted `retry_after_seconds`.
  A walled pool is now visible the moment it walls, not when the next caller
  pays for the discovery.
- `quota_probe_skipped` (INFO): the probe loop's three silent reschedule
  paths — `no_probe_model`, `no_proxy`, `not_attempted` — now log their
  reason instead of leaving the operator with a loop that appears dead.
- `upstream_body_read_failed` (WARN): the non-streaming body read failure now
  carries the underlying error and upstream status. Production showed 15.0s
  reads ending in 502 — the caller's own time-to-first-token budget cutting
  the connection, not an upstream fault.

### Notes

- No routing, quota or retry semantics changed; this release only makes the
  existing state observable.

## v1.5.4 (2026-11-05) — empirical reasoning observation

Production verification of the capability catalog showed its `reasoning`
flag cannot be trusted: every free model is flagged reasoning=true, while
live probing proved crouton-alpha-free answers with zero reasoning payload.
That left the v1.5.2 probe-model preference permanently dormant.

### Added

- **Live reasoning observation**: every successful availability/quota probe
  and the models they serve record whether the response actually carried a
  reasoning payload (`wire.DetectReasoning` marker scan on the collapsed
  document, in-memory per model).
- `quotaProbeModel` selection order is now: (1) models **empirically
  observed** to answer without reasoning — overriding the catalog, (2) models
  the catalog marks non-reasoning, (3) fallback. Models observed to reason
  are skipped entirely. In-memory by design: the hourly availability probe
  re-learns the map within one cycle after a restart.

### Tests

- `DetectReasoning` marker matrix across all three protocols; probe-model
  selection ordering with observed overrides (fallback → observed pick →
  observed exclusion).

## v1.5.3 (2026-11-05) — wall-aware quota pre-check

From continued production testing of v1.5.2: the free-pool capability
catalog marks **every** free model `reasoning=true` (so the v1.5.2 probe
model preference has no non-reasoning candidate to pick and stays on its
fallback — kept for the day upstream ships one), and free-model routes start
on the anonymous lane (`route.Anonymous=true`), which made the v1.5.2
pre-check unreachable for exactly the traffic shape it was built for: wall
periods burned 3 upstream calls per live request (anonymous walk + two key
attempts) with `quota_precheck_blocked` never firing.

### Fixed

- **The quota pre-check now covers anonymous-first routes.** The
  `route.Anonymous` bypass is replaced by an empirical wall signal: the
  anonymous lane observed answering 429 twice within 10 minutes counts as
  walled (its breaker being disabled without `anonymous_backoff` no longer
  blocks the pre-check). Any 2xx on the lane or a successful quota probe
  stands the signal down, and a lapsed window disarms it — the pre-check
  re-arms itself on the next real walk, so recovery is never blocked.

### Effect

During a free-pool wall with the default config, live requests now cost 0
upstream calls after the first two walks (previously 3 per request for as
long as the wall lasted), while New-API still gets `429 insufficient_quota`
+ `Retry-After` for its smooth degradation to paid channels.

## v1.5.2 (2026-11-05) — test-driven iteration

Findings from the 2026-11-05 production test pass (probe events 43
recovered / 45 still-walled, inflight saturation sampled externally, zero
quota pre-check events during the wall).

### Added

- **Queue observability**: the inflight gate now counts every request that
  had to wait (`queued_total`, exposed in `healthz` `load`) and logs an
  `inflight_queued` event with the wait duration — saturation is visible in
  the request log instead of only by polling `healthz.load` during a burst.
- **Probe model prefers non-reasoning free models.** Production probes
  through deepseek streamed reasoning before answering, spending part of the
  probe's token budget on thinking; with capability metadata the probe now
  picks the first non-reasoning free model and falls back to the first free
  model only when no such candidate exists.

### Documented

- The quota pre-check fires only when the anonymous lane cannot take a
  request. On deployments that keep the anonymous lane enabled **without**
  `models.anonymous_backoff`, its circuit breaker is disabled (upstream
  semantics), so the pre-check stays dormant during free-pool walls — the
  observed production behavior. Operators who want wall-time rejection with
  zero upstream calls should configure `anonymous_backoff` to arm the
  breaker; probe loop and key cooldowns are unaffected.

## v1.5.1 (2026-11-04) — post-review hardening

Findings from the production code review of v1.5.0, all resolved.

### Fixed

- **Admission control now covers the dedicated `/v1/systemone` endpoint.** The
  inflight gate moved into a `gated` wrapper applied to all four POST
  endpoints, so `max_inflight` bounds them uniformly (v1.5.0's gate lived
  inside `handleInference` only, and the comment claiming SystemOne coverage
  was wrong).
- **GEO classification stops key rotation within a tier.** The check moved
  down into `doKeyUpstream`'s attempt loop (returning `errGeoBlocked`), so a
  region-blocked model no longer burns `max_attempts` keys on the same egress
  before the 404 mapping; the now-dead tier-level GEO branch was removed.
- **Quota probes leave through a WARP exit when the model's lane uses WARP**,
  matching the live traffic path instead of measuring a different egress.
- **The quota probe loop re-arms on every config apply**, so
  `performance.quota_probe_seconds` is hot-reloadable (enabling it after
  starting with the knob off no longer requires a restart; repeated applies
  never stack loops).
- **Idle-aborted streams record `upstream_idle_timeout` as the request
  outcome** instead of being misattributed to `client_canceled` (both surface
  as `context.Canceled` on the upstream read).
- Stale comment: the keepalive tick floor is 50ms, not the documented 500ms.

### Tests

- **E2E is hermetic**: the fake upstream serves the capability catalog and
  `models.CapabilitiesURL` is a package variable the tests point at it — the
  suite no longer depends on `models.opencode.ai` (whose free roster rotates)
  or on external network at all. Keepalive unit tests got wider timing
  margins for slow CI runners.

## v1.5.0 (2026-11-04) — stability & quota governance enhancements

Base: jasonxu114514/opencode2api v1.4.6 (58f5667). Design document:
[docs/DEVELOPMENT-PLAN-v1.5.0.zh-CN.md](docs/DEVELOPMENT-PLAN-v1.5.0.zh-CN.md).

### Added

- **P1-1 SSE keepalive** (`performance.heartbeat_seconds`, default 15, 0=off):
  committed streams emit a client-invisible frame after N seconds of upstream
  silence — an SSE comment line (`: keepalive`) for Chat and Responses, the
  standard `event: ping` for Anthropic — so intermediaries and client idle
  watchdogs stop cutting thinking streams.
- **P1-2 post-commit idle watchdog** (`performance.idle_timeout_seconds`,
  default 0=off): when the upstream produces nothing for N seconds, the
  gateway emits an in-band `upstream_idle_timeout` error, cancels the upstream
  request (no more tokens burned on a wedged stream) and ends the turn instead
  of waiting out the whole request budget.
- **P1-3 quota probes for cooled keys** (`performance.quota_probe_seconds`,
  default 600, 0=off): a key rejected with 429 free-usage-limit is probed with
  one minimal real request per interval (fresh identity, one in flight
  gateway-wide); a success clears its cooldown immediately
  (`quota_probe_recovered`) instead of waiting passively for Retry-After.
- **P2-4 quota pre-check**: when every configured key lane is fully
  quota-cooled and the anonymous lane cannot take the request, inference
  answers `429 insufficient_quota` + `Retry-After` with ZERO upstream calls
  (event `quota_precheck_blocked`). Playground/diagnostic requests bypass it.
- **P2-5 global inflight gate** (`performance.max_inflight` +
  `performance.queue_wait_seconds`, default 0=off): bounds concurrent
  inference turns gateway-wide; overflow queues, then `429 rate_limit_error
  gateway_busy` with `Retry-After: 5`. `healthz` grows an optional `load`
  section.
- **P3-6 geo-block classification**: region-availability 4xx ("not available
  in your country/region", …) stop burning attempts across keys and tiers and
  answer `404 invalid_request_error model_not_available_in_region`.

### Fixed

- Model-catalog listings answered 200 while inference was quota-walled, and
  the refresh cycle cleared a key's quota state every 5 minutes
  (`MarkSuccess`). Listings now record key/transport health only
  (`MarkListingSuccess`); only a real inference success or a quota probe
  clears the quota state.
- `rawSSE` writes each event with a single `Write`, so a supervised stream
  can never interleave an event with a keepalive frame.

### Behavior compatibility

Old `config.json` files load unchanged. With every new knob at its default
the only visible change is the 15s keepalive on streaming responses; set
`heartbeat_seconds: 0` to get the exact pre-v1.5 wire behavior.
