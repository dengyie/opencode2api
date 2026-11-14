package gateway

import (
	"context"
	"sync/atomic"
	"time"

	"net/http"

	"opencode2api/internal/config"
	wire "opencode2api/internal/protocol"
)

// The quota probe loop (v1.5.0) gives quota-cooled keys an active way back.
// A key that answered 429 free-usage-limit is cooled until its quota window
// reopens; upstream does not expose a quota endpoint, so the only honest way
// to learn whether the window reopened is one minimal real request through
// that key. Waiting for the next live request to find out means the first
// caller after the window pays for the discovery — and, worse, a fully walled
// pool stays invisible until then. The loop probes each quota-cooled key at
// most once per quota_probe_seconds, one probe in flight at a time, and
// clears the cooldown the moment a probe succeeds.
// Since v1.7.0 the loop also re-tests credential-quarantined keys (401/403):
// a quarantined key needs the same active recovery, since the quarantine
// stretch otherwise outlives any evidence the credential was revived.

// startQuotaProbes runs the quota probe loop on the controller's root context
// so it survives runtime swaps. Disabled when quota_probe_seconds is zero.
func (m *RuntimeController) startQuotaProbes() {
	ctx, cancel := context.WithCancel(m.root)
	m.quotaProbeCancel = cancel
	go func() {
		// Tick at half the configured probe interval (bounded to [1s, 1m]) so
		// a due probe fires close to its time at any interval, and the test
		// suite can exercise the loop without minute-scale sleeps.
		period := time.Minute
		if interval := m.quotaProbeIntervalForTick(); interval > 0 {
			if half := interval / 2; half < period {
				period = half
			}
		}
		if period < time.Second {
			period = time.Second
		}
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			m.probeQuotaCooledKeys(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// quotaProbeIntervalForTick reads the configured probe interval from the
// current runtime; zero (disabled) means the loop should never have started.
func (m *RuntimeController) quotaProbeIntervalForTick() time.Duration {
	r := m.current.Load()
	if r == nil || r.gateway == nil || r.gateway.cfg.Performance.QuotaProbeSeconds <= 0 {
		return 0
	}
	return time.Duration(r.gateway.cfg.Performance.QuotaProbeSeconds) * time.Second
}

// probeQuotaCooledKeys collects due quota-cooled keys and probes them one at
// a time, so the loop can never stampede the upstream even with many keys
// walled at once.
func (m *RuntimeController) probeQuotaCooledKeys(ctx context.Context) {
	r := m.current.Load()
	if r == nil || r.gateway == nil {
		return
	}
	g := r.gateway
	for _, pool := range []*upstreamNodes{g.zenNodes, g.goNodes} {
		for _, node := range pool.ProbeDueNodes() {
			if ctx.Err() != nil {
				return
			}
			if !quotaProbeInFlight.CompareAndSwap(false, true) {
				return // one probe at a time; the rest wait for the next tick
			}
			func() {
				defer quotaProbeInFlight.Store(false)
				gctx, cancel := context.WithTimeout(ctx, 90*time.Second)
				defer cancel()
				g.probeQuotaKey(gctx, pool, node)
			}()
		}
	}
}

// probeQuotaKey sends one minimal real request through the node's key and
// current proxy. On success the key's cooldowns are cleared immediately; on
// failure the next probe is rescheduled without touching the cooldown
// semantics MarkFailure established.
func (g *Gateway) probeQuotaKey(ctx context.Context, nodes *upstreamNodes, node *upstreamNode) {
	interval := nodes.quotaProbeInterval
	if interval <= 0 {
		return
	}
	reschedule := func() {
		node.probeNextAt.Store(time.Now().Add(interval).UnixNano())
	}
	model, protocol, ok := g.quotaProbeModel(nodes == g.zenNodes)
	if !ok {
		// Nothing to probe with yet (catalog still empty at startup); try
		// again on the next tick.
		reschedule()
		g.logger.Info("quota probe skipped", "component", "upstream", "event", "quota_probe_skipped",
			"tier", tierOf(nodes, g), "key_id", config.KeyDisplayID(node.key), "reason", "no_probe_model")
		return
	}
	proxy := nodes.Proxy(node)
	if proxy == nil {
		reschedule()
		g.logger.Info("quota probe skipped", "component", "upstream", "event", "quota_probe_skipped",
			"tier", tierOf(nodes, g), "key_id", config.KeyDisplayID(node.key), "reason", "no_proxy")
		return
	}
	ids := probeRequestIDs()
	baseURL := g.cfg.Upstream.Zen
	if nodes == g.goNodes {
		baseURL = g.cfg.Upstream.Go
	}
	// Match the live traffic path: when this model's lane egresses over WARP,
	// the probe must leave through a WARP exit too, or it measures a different
	// egress than production and can be answered by the free tier's IP gate
	// instead of the key's quota.
	tier := tierOf(nodes, g)
	overWARP := g.usesWARP(model, tier, false)
	started := time.Now()
	result := g.probeAttempt(ctx, g.inferenceClient(proxy, baseURL, model), baseURL, node.key, model, protocol, ids, "", overWARP)
	if !result.attempted {
		// No request reached the upstream (transport/proxy down): probe again
		// next interval without changing any cooldown state.
		reschedule()
		g.logger.Info("quota probe skipped", "component", "upstream", "event", "quota_probe_skipped",
			"tier", tier, "key_id", config.KeyDisplayID(node.key), "reason", "not_attempted")
		return
	}
	if result.success {
		nodes.MarkSuccess(node)
		// The probe itself is an observation of the model's real behavior:
		// feed the reasoning-learning map (v1.5.4).
		g.noteReasoningObservation(model, result.sawReasoning)
		// The pool reopened: drop the empirical anonymous-wall signal so the
		// pre-check stands down immediately instead of riding out its window.
		g.anonWallCount.Store(0)
		g.logger.Info("quota probe recovered a key", "component", "upstream", "event", "quota_probe_recovered", "tier", tier, "key_id", config.KeyDisplayID(node.key), "model", model, "probe_ms", time.Since(started).Milliseconds())
		return
	}
	if result.reason == "http_429" {
		// Still walled; the next probe is due after the interval. quotaUntil
		// already reflects the last real Retry-After, so leave it alone.
		g.logger.Info("quota probe still walled", "component", "upstream", "event", "quota_probe_still_walled", "tier", tier, "key_id", config.KeyDisplayID(node.key), "model", model)
		// TetherPool Mode B (v1.6.0): the probe is the only wall observer
		// while a fully walled pool pre-check-blocks every live request, so
		// the rebind report rides this branch too (same per-key spacing).
		g.observeQuotaWall(node, tier, "probe_still_walled")
	} else if result.reason == "http_401" || result.reason == "http_403" {
		// Still credential-dead (v1.7.0); the quarantine stays as MarkFailure
		// left it and the next probe is due after the interval. A dedicated
		// event keeps a long-quarantined key visible instead of reading as
		// inconclusive churn.
		nodes.RepinCredentialQuarantine(node)
		g.logger.Info("quota probe still credential-dead", "component", "upstream", "event", "quota_probe_still_credential_dead", "tier", tier, "key_id", config.KeyDisplayID(node.key), "model", model, "reason", result.reason)
	} else {
		g.logger.Debug("quota probe inconclusive", "component", "upstream", "event", "quota_probe_inconclusive", "tier", tier, "key_id", config.KeyDisplayID(node.key), "reason", result.reason)
	}
	reschedule()
}

// noteReasoningObservation records live reasoning behavior for one model,
// overriding the capability catalog's reasoning flag for probe-model
// selection. In-memory by design: the hourly availability probe re-learns the
// map within one cycle after a restart.
func (g *Gateway) noteReasoningObservation(model string, sawReasoning bool) {
	g.observedReasoning.Store(model, sawReasoning)
}

// quotaProbeModel picks the model a quota probe should use: the first model
// the catalog considers free, supported and native on the key lane. A
// non-reasoning model is preferred when one exists — production probes
// (2026-11-05) showed a reasoning model spends part of its probe budget
// thinking before answering. Selection order: (1) models empirically observed
// to answer WITHOUT reasoning (live observation overrides the catalog, which
// has flagged every free model reasoning=true even for models that answer
// with none), (2) models the catalog marks non-reasoning, (3) fallback to the
// first acceptable free model. The quota that gates a key is not
// model-specific, so any free model works.
func (g *Gateway) quotaProbeModel(zen bool) (string, wire.Protocol, bool) {
	tier := config.TierZen
	if !zen {
		tier = config.TierGo
	}
	var fallbackModel string
	var fallbackProtocol wire.Protocol
	for _, model := range g.catalog.List() {
		if !g.catalog.IsFreeModel(model) || !g.catalog.Supported(model) {
			continue
		}
		d := g.catalog.Diagnostic(model, wire.Chat, len(g.cfg.ZenKeys) > 0, len(g.cfg.GoKeys) > 0, g.cfg.Anonymous)
		protocol, ok := d.NativeProtocols[tier]
		if !ok {
			continue
		}
		if observed, has := g.observedReasoning.Load(model); has {
			if observed.(bool) {
				continue // empirically a reasoning model: never probe with it
			}
			return model, protocol, true // empirically non-reasoning: best
		}
		if !g.catalog.MetadataForTier(model, tier).Reasoning {
			return model, protocol, true
		}
		if fallbackModel == "" {
			fallbackModel, fallbackProtocol = model, protocol
		}
	}
	if fallbackModel != "" {
		return fallbackModel, fallbackProtocol, true
	}
	return "", wire.Chat, false
}

func tierOf(nodes *upstreamNodes, g *Gateway) config.Tier {
	if nodes == g.goNodes {
		return config.TierGo
	}
	return config.TierZen
}

// quotaProbeInFlight ensures at most one quota probe runs gateway-wide.
var quotaProbeInFlight atomic.Bool

// anonymousLaneAvailable reports, read-only, whether the anonymous lane could
// currently take a request: enabled and its breaker not open. It never arms
// the breaker's half-open probe — AllowAttempt does that, and the pre-check
// must stay side-effect free. WARP-backed anonymous traffic counts as
// available here: its breaker is this same breaker.
func (g *Gateway) anonymousLaneAvailable() bool {
	if !g.cfg.Anonymous || g.anonymous == nil {
		return false
	}
	if g.anonymous.backoff.threshold >= 1 {
		if until := g.anonymous.breakerUntil.Load(); until != 0 && time.Now().UnixNano() < until {
			return false
		}
	}
	return true
}

// anonWallWindow is how long recent anonymous-lane 429s count toward the
// empirical wall signal.
const anonWallWindow = 10 * time.Minute

// observeAnonymousWall feeds the empirical wall signal with one anonymous-lane
// outcome (v1.5.3): 429s accumulate within the window; any 2xx proves the lane
// can serve again and clears it. The anonymous lane serves only free models,
// so its 429 is the free-usage wall by definition.
func (g *Gateway) observeAnonymousWall(status int) {
	if status/100 == 2 {
		g.anonWallCount.Store(0)
		return
	}
	if status != http.StatusTooManyRequests {
		return
	}
	now := time.Now().UnixNano()
	last := g.anonWallLastAt.Load()
	if last == 0 || now-last > int64(anonWallWindow) {
		g.anonWallCount.Store(1)
	} else {
		g.anonWallCount.Add(1)
	}
	g.anonWallLastAt.Store(now)
}

// anonymousLaneWalled reports whether the anonymous lane answered 429 often
// enough recently to be treated as empirically walled. Two observations within
// the window keep the signal robust against a single fluke while staying
// cheap: the second request's walk arms it for everything that follows.
func (g *Gateway) anonymousLaneWalled() bool {
	last := g.anonWallLastAt.Load()
	if last == 0 || time.Since(time.Unix(0, last)) > anonWallWindow {
		return false
	}
	return g.anonWallCount.Load() >= 2
}

// anonWallFastFailover reports whether the anonymous walk should hand the
// request to the key tiers after a single attempt: the lane is empirically
// 429-walled (two within the window) and at least one Zen key is currently
// usable. Walking every exit through a walled lane burns the caller's
// first-token budget before the keys ever see the request (each 429 costs
// 1.7–26s in production), so the fronting gateway cancels at its TTFT gate
// and the key fallback never completes. One attempt still catches the wall
// lifting; with no usable key the walk keeps its full cap, because the exits
// are then the only lane that can answer.
func (g *Gateway) anonWallFastFailover() bool {
	return g.anonymousLaneWalled() && g.zenNodes.HasUsableNode()
}

// wallProbeAttemptTimeout bounds the single anonymous attempt a wall fast
// failover leaves in place. Production shows a walled exit can take 2–17s to
// answer 429; when the keys are expected to carry the request, that delay is
// spent out of the caller's first-token budget and can push an otherwise
// serviceable request past it (a 17s probe plus a 13s key answer is 30s).
// performance.wall_probe_timeout_seconds caps the probe so the keys keep the
// rest of the budget; 0 keeps the full attempt timeout.
func (g *Gateway) wallProbeAttemptTimeout() time.Duration {
	probe := time.Duration(g.cfg.Performance.WallProbeTimeoutSeconds) * time.Second
	if probe <= 0 {
		return g.attemptTimeout
	}
	if g.attemptTimeout > 0 && g.attemptTimeout < probe {
		return g.attemptTimeout
	}
	return probe
}

// quotaWall reports whether the request's possible lanes are all quota-cooled,
// and when the first key reopens. The anonymous lane must be unable to take
// the request — disabled, paused by its breaker, or empirically walled (its
// own recent 429s) — because it is either the route's primary lane
// (route.Anonymous free-model traffic, the tebi shape) or the key lanes'
// fallback. A lane that might still serve the model never pre-checks a
// request away. (v1.5.3 replaces the v1.5.2 route.Anonymous bypass, which
// made the pre-check unreachable for anonymous-first traffic.)
func (g *Gateway) quotaWall() (time.Time, bool) {
	if g.anonymousLaneAvailable() && !g.anonymousLaneWalled() {
		return time.Time{}, false
	}
	zenConfigured := len(g.cfg.ZenKeys) > 0
	goConfigured := len(g.cfg.GoKeys) > 0
	if !zenConfigured && !goConfigured {
		return time.Time{}, false
	}
	walled := false
	earliest := time.Time{}
	if zenConfigured {
		until, ok := g.zenNodes.AllQuotaCooled()
		if !ok {
			return time.Time{}, false
		}
		walled = true
		earliest = until
	}
	if goConfigured {
		until, ok := g.goNodes.AllQuotaCooled()
		if !ok {
			return time.Time{}, false
		}
		walled = true
		if earliest.IsZero() || until.Before(earliest) {
			earliest = until
		}
	}
	if !walled || earliest.IsZero() {
		return time.Time{}, false
	}
	return earliest, true
}

// credentialWall is quotaWall's credential-class mirror (v1.7.0): whether the
// request's possible lanes are all credential-quarantined (401/403), and when
// the first key's quarantine ends. Same lane logic — a possibly usable
// anonymous lane never pre-checks a request away. Rejecting here costs zero
// upstream calls, where walking into doKeyUpstream would spend attempts
// re-selecting keys the upstream already rejected.
func (g *Gateway) credentialWall() (time.Time, bool) {
	if g.anonymousLaneAvailable() && !g.anonymousLaneWalled() {
		return time.Time{}, false
	}
	zenConfigured := len(g.cfg.ZenKeys) > 0
	goConfigured := len(g.cfg.GoKeys) > 0
	if !zenConfigured && !goConfigured {
		return time.Time{}, false
	}
	walled := false
	earliest := time.Time{}
	if zenConfigured {
		until, ok := g.zenNodes.AllCredentialQuarantined()
		if !ok {
			return time.Time{}, false
		}
		walled = true
		earliest = until
	}
	if goConfigured {
		until, ok := g.goNodes.AllCredentialQuarantined()
		if !ok {
			return time.Time{}, false
		}
		walled = true
		if earliest.IsZero() || until.Before(earliest) {
			earliest = until
		}
	}
	if !walled || earliest.IsZero() {
		return time.Time{}, false
	}
	return earliest, true
}
