package gateway

import (
	"context"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/tetherpool"
)

// The TetherPool integration (v1.6.0) has two halves. Provisioning runs
// inside RuntimeController.build: one sticky egress binding per Zen key, with
// the static proxies kept behind the allocations as failover spares. Feedback
// runs here: a battery worker drains wall-triggered reports so the control
// plane can rotate a walled key to a fresh egress. Both halves are best-effort
// — every failure keeps the gateway on the statically configured proxies.

// rearmTetherPool (re)starts the battery dispatch worker so changes to the
// tetherpool section take effect on config apply. Any previous worker is
// canceled first, so repeated applies never stack workers. While the current
// provision fell back to the static proxies, the worker also retries the
// resolve every tpRetryInterval (5 minutes when unset) by re-applying the
// config file, so a control plane that was down at startup is picked up
// without operator action.
func (m *RuntimeController) rearmTetherPool(cfg config.Config) {
	if m.tpCancel != nil {
		m.tpCancel()
		m.tpCancel = nil
	}
	if !cfg.TetherPool.Enabled {
		return
	}
	ctx, cancel := context.WithCancel(m.root)
	m.tpCancel = cancel
	go m.runTetherPoolWorker(ctx, cfg)
}

// runTetherPoolWorker drains the rebind channel and re-provisions after a
// startup fallback. It runs on the controller's root context and survives
// runtime swaps, like the quota probe loop.
func (m *RuntimeController) runTetherPoolWorker(ctx context.Context, cfg config.Config) {
	client := tetherpool.NewClient(cfg.TetherPool.BaseURL, cfg.TetherPool.Token, time.Duration(cfg.TetherPool.TimeoutSeconds)*time.Second)
	service := cfg.TetherPool.Service
	retryInterval := m.tpRetryInterval
	if retryInterval <= 0 {
		retryInterval = 5 * time.Minute
	}
	retry := time.NewTicker(retryInterval)
	defer retry.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-retry.C:
			if snapshot, ok := m.tpSnapshot.Load().(tetherpool.Snapshot); ok && snapshot.Mode == tetherpool.ModeStaticFallback {
				if m.logger != nil {
					m.logger.Info("tetherpool re-provision retry", "component", "gateway", "event", "tetherpool_reprovision")
				}
				if _, err := m.Reload(); err != nil && m.logger != nil {
					m.logger.Warn("tetherpool re-provision failed", "component", "gateway", "event", "tetherpool_reprovision_failed", "error", err)
				}
			}
		case ev := <-m.tpRebind:
			reportCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.TetherPool.TimeoutSeconds)*time.Second)
			err := client.BatteryEvent(reportCtx, tetherpool.BatteryEvent{
				AccountID:     ev.AccountID,
				EventType:     "rate_limited_429",
				TargetService: service,
				ForceRebind:   true,
			})
			cancel()
			if m.logger != nil {
				if err != nil {
					m.logger.Warn("tetherpool rebind report failed", "component", "gateway", "event", "tetherpool_rebind_failed", "key_id", ev.KeyID, "reason", ev.Reason, "error", err)
				} else {
					m.logger.Info("tetherpool rebind reported", "component", "gateway", "event", "tetherpool_rebind", "key_id", ev.KeyID, "account_id", ev.AccountID, "reason", ev.Reason)
				}
			}
		}
	}
}

// observeQuotaWall forwards one per-key quota wall to the battery dispatcher.
// The rebindFiredAt timestamp on the node spaces reports at least
// tetherpool.rebind_min_interval_seconds apart, so a wall episode observed by
// many concurrent requests still costs one event. The send is non-blocking:
// the report is an optimization, never a request-path dependency, and a full
// channel drops the report while keeping the timestamp so the next window —
// not the next 429 — retries.
func (g *Gateway) observeQuotaWall(node *upstreamNode, tier config.Tier, reason string) {
	if g.tpRebind == nil || !g.cfg.TetherPool.RebindOnWall || tier != config.TierZen {
		return
	}
	now := time.Now().UnixNano()
	minInterval := int64(time.Duration(g.cfg.TetherPool.RebindMinIntervalSeconds) * time.Second)
	fired := node.rebindFiredAt.Load()
	if fired != 0 && now-fired < minInterval {
		return
	}
	if !node.rebindFiredAt.CompareAndSwap(fired, now) {
		return
	}
	select {
	case g.tpRebind <- tetherpool.RebindRequest{AccountID: tetherpool.AccountID(node.key), KeyID: config.KeyDisplayID(node.key), Reason: reason}:
	default:
	}
}
