package gateway

import (
	"context"
	"strings"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/tetherpool"
)

// exitRebindRegionDropAfter is how many consecutive failed exit-rebind
// acquires for one account it takes before the next attempt drops the region
// pin: a pinned rebind into a whole down region can never succeed.
const exitRebindRegionDropAfter = 3

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
					m.logger.Info("egress re-provision retry", "component", "gateway", "event", "tp_reprovision")
				}
				if _, err := m.Reload(); err != nil && m.logger != nil {
					m.logger.Warn("egress re-provision failed", "component", "gateway", "event", "tp_reprovision_failed", "error", err)
				}
			}
		case ev := <-m.tpRebind:
			if ev.Kind == tetherpool.RebindKindFailure {
				m.handleExitFailureRebind(ctx, client, cfg, service, ev)
				continue
			}
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
					m.logger.Warn("egress rebind report failed", "component", "gateway", "event", "tp_rebind_failed", "key_id", ev.KeyID, "reason", ev.Reason, "error", err)
				} else {
					m.logger.Info("egress rebind reported", "component", "gateway", "event", "tp_rebind", "key_id", ev.KeyID, "account_id", ev.AccountID, "reason", ev.Reason)
				}
			}
		}
	}
}

// handleExitFailureRebind soft-rebinds one TetherPool account whose exit
// failed its health verification: an acquire with force_rebalance moves the
// binding to a different node without any battery penalty (the account did
// nothing wrong — its exit died), and the same gateway proxy URL keeps
// serving from the new exit. The snapshot's account entry is updated in place
// so /healthz renders the fresh binding immediately.
//
// The read-modify-write of the snapshot runs under snapshotMu so it cannot
// lose against a concurrent build() Store: a rebuild that lands mid-rebind
// keeps its fresh snapshot, and the rebind retries against it. After
// exitRebindRegionDropAfter consecutive failures for one account the region
// pin is dropped for the next attempt — a whole region can be down, and a
// pinned rebind into a dead region can never succeed.
func (m *RuntimeController) handleExitFailureRebind(ctx context.Context, client *tetherpool.Client, cfg config.Config, service string, ev tetherpool.RebindRequest) {
	reportCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.TetherPool.TimeoutSeconds)*time.Second)
	defer cancel()
	region, oldNode, oldIP := "", "", ""
	m.snapshotMu.Lock()
	if snapshot, ok := m.tpSnapshot.Load().(tetherpool.Snapshot); ok {
		for _, a := range snapshot.Accounts {
			if a.AccountID == ev.AccountID {
				region = a.Region
				oldNode, oldIP = a.NodeName, a.ExitIP
				break
			}
		}
	}
	// A dying region must not wedge the rebind: past the streak threshold the
	// next acquire asks for any region, and a success re-pins to where it
	// actually landed.
	if m.rebindFailStreak[ev.AccountID] >= exitRebindRegionDropAfter {
		if m.logger != nil && region != "" {
			m.logger.Warn("dropping the region pin after repeated exit-rebind failures", "component", "gateway", "event", "tp_exit_rebind_region_dropped",
				"account_id", ev.AccountID, "region", region, "streak", m.rebindFailStreak[ev.AccountID])
		}
		region = ""
	}
	m.snapshotMu.Unlock()
	alloc, err := client.AcquireRebind(reportCtx, service, ev.AccountID, "", region)
	m.snapshotMu.Lock()
	if m.rebindFailStreak == nil {
		m.rebindFailStreak = make(map[string]int)
	}
	if err != nil {
		m.rebindFailStreak[ev.AccountID]++
		m.snapshotMu.Unlock()
		if m.logger != nil {
			m.logger.Warn("exit-failure rebind failed", "component", "gateway", "event", "tp_exit_rebind_failed",
				"key_id", ev.KeyID, "account_id", ev.AccountID, "reason", ev.Reason, "error", err)
		}
		return
	}
	delete(m.rebindFailStreak, ev.AccountID)
	if snapshot, ok := m.tpSnapshot.Load().(tetherpool.Snapshot); ok {
		for i := range snapshot.Accounts {
			if snapshot.Accounts[i].AccountID == ev.AccountID {
				snapshot.Accounts[i].NodeName = alloc.NodeName
				snapshot.Accounts[i].ExitIP = alloc.ExitIP
				snapshot.Accounts[i].Region = alloc.Region
			}
		}
		m.tpSnapshot.Store(snapshot)
	}
	m.snapshotMu.Unlock()
	if m.logger != nil {
		m.logger.Warn("dead exit rebound to a fresh egress", "component", "gateway", "event", "tp_exit_rebound",
			"key_id", ev.KeyID, "account_id", ev.AccountID, "reason", ev.Reason,
			"old_node", oldNode, "old_exit_ip", oldIP, "new_node", alloc.NodeName, "new_exit_ip", alloc.ExitIP, "region", alloc.Region)
	}
}

// observeProxyExitFailure queues one exit-failure rebind for a TetherPool-bound
// proxy whose health verification failed. The rebindFiredAt timestamp spaces
// reports at least tetherpool.rebind_min_interval_seconds apart, so a dying
// exit observed by many checks still costs one rebind per window. The send is
// non-blocking: the rebind is a self-heal, never a request-path dependency.
func (g *Gateway) observeProxyExitFailure(proxy *proxyTransport, source string) {
	if g.tpRebind == nil || proxy == nil || proxy.tpAccountID == "" || !g.cfg.TetherPool.RebindOnFailure {
		return
	}
	now := time.Now().UnixNano()
	minInterval := int64(time.Duration(g.cfg.TetherPool.RebindMinIntervalSeconds) * time.Second)
	fired := proxy.rebindFiredAt.Load()
	if fired != 0 && now-fired < minInterval {
		return
	}
	if !proxy.rebindFiredAt.CompareAndSwap(fired, now) {
		return
	}
	select {
	case g.tpRebind <- tetherpool.RebindRequest{Kind: tetherpool.RebindKindFailure, AccountID: proxy.tpAccountID, KeyID: strings.TrimPrefix(proxy.tpAccountID, tetherpool.AccountPrefix), Reason: source}:
	default:
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
