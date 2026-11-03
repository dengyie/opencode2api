package gateway

import (
	"context"
	"sync/atomic"
	"time"

	"opencode2api/internal/config"
	models "opencode2api/internal/models"
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

// startQuotaProbes runs the quota probe loop on the controller's root context
// so it survives runtime swaps. Disabled when quota_probe_seconds is zero.
func (m *RuntimeController) startQuotaProbes() {
	ctx, cancel := context.WithCancel(m.root)
	m.quotaProbeCancel = cancel
	go func() {
		ticker := time.NewTicker(time.Minute)
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
		for _, node := range pool.QuotaCooledNodes() {
			if ctx.Err() != nil {
				return
			}
			if time.Now().UnixNano() < node.probeNextAt.Load() {
				continue
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
		return
	}
	proxy := nodes.Proxy(node)
	if proxy == nil {
		reschedule()
		return
	}
	ids := probeRequestIDs()
	baseURL := g.cfg.Upstream.Zen
	if nodes == g.goNodes {
		baseURL = g.cfg.Upstream.Go
	}
	started := time.Now()
	result := g.probeAttempt(ctx, g.inferenceClient(proxy, baseURL, model), baseURL, node.key, model, protocol, ids, "", false)
	if !result.attempted {
		// No request reached the upstream (transport/proxy down): probe again
		// next interval without changing any cooldown state.
		reschedule()
		return
	}
	if result.success {
		nodes.MarkSuccess(node)
		g.logger.Info("quota probe recovered a key", "component", "upstream", "event", "quota_probe_recovered", "tier", tierOf(nodes, g), "key_id", config.KeyDisplayID(node.key), "model", model, "probe_ms", time.Since(started).Milliseconds())
		return
	}
	if result.reason == "http_429" {
		// Still walled; the next probe is due after the interval. quotaUntil
		// already reflects the last real Retry-After, so leave it alone.
		g.logger.Info("quota probe still walled", "component", "upstream", "event", "quota_probe_still_walled", "tier", tierOf(nodes, g), "key_id", config.KeyDisplayID(node.key), "model", model)
	} else {
		g.logger.Debug("quota probe inconclusive", "component", "upstream", "event", "quota_probe_inconclusive", "tier", tierOf(nodes, g), "key_id", config.KeyDisplayID(node.key), "reason", result.reason)
	}
	reschedule()
}

// quotaProbeModel picks the model a quota probe should use: the first model
// the catalog considers free, supported and native on the key lane. The quota
// that gates a key is not model-specific, so the first available answer wins;
// a stable choice keeps consecutive probes comparable.
func (g *Gateway) quotaProbeModel(zen bool) (string, wire.Protocol, bool) {
	tier := config.TierZen
	if !zen {
		tier = config.TierGo
	}
	for _, model := range g.catalog.List() {
		if !g.catalog.IsFreeModel(model) || !g.catalog.Supported(model) {
			continue
		}
		d := g.catalog.Diagnostic(model, wire.Chat, len(g.cfg.ZenKeys) > 0, len(g.cfg.GoKeys) > 0, g.cfg.Anonymous)
		protocol, ok := d.NativeProtocols[tier]
		if !ok {
			continue
		}
		return model, protocol, true
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

// quotaWallFor reports whether the request's possible lanes are all
// quota-cooled, and when the first key reopens. Conservative by design: it
// fires only when EVERY configured key lane is fully walled AND the anonymous
// lane cannot take the request (disabled, breaker open, or the route starts
// there anyway). A lane that might still serve the model never pre-checks a
// request away.
func (g *Gateway) quotaWallFor(route models.Route) (time.Time, bool) {
	if route.Anonymous || g.anonymousLaneAvailable() {
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
