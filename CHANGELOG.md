# Changelog

All notable changes to this maintained edition are documented here. The
upstream project (jasonxu114514/opencode2api) has its own release notes; this
file tracks what this repository adds on top of the imported baseline.

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
