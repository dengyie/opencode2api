# Changelog

All notable changes to this maintained edition are documented here. The
upstream project (jasonxu114514/opencode2api) has its own release notes; this
file tracks what this repository adds on top of the imported baseline.

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
