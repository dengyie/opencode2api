package gateway

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"opencode2api/internal/config"
)

// evictorTick paces the eviction sweep. Evictions are armed with a 24h
// observation window (the credential quarantine), so a minute-level cadence
// is plenty precise and keeps the sweep off the hot path.
const evictorTick = time.Minute

// Sentinel outcomes of the eviction callback. errEvictionKeyGone means the
// key was already removed (a concurrent apply raced the sweep): the eviction
// is done, not failed. errEvictionLastKey refuses to strand the gateway
// without any upstream credential when the anonymous lane is off; the key
// stays quarantined and the sweep retries until the operator intervenes.
var (
	errEvictionKeyGone  = errors.New("key already removed from the config")
	errEvictionLastKey  = errors.New("refusing to evict the last key of the tier while the anonymous lane is off")
	errEvictionDisabled = errors.New("no eviction callback installed")
)

// installEvictionLogging wires the pool-level eviction callbacks (v1.8.0) to
// the gateway logger. The pools stay log-free; the evictor itself logs the
// removal and the RuntimeController logs config persistence. A nil logger
// (tests) keeps the callbacks silent.
func installEvictionLogging(logger *slog.Logger, pools ...*upstreamNodes) {
	for _, pool := range pools {
		if pool == nil {
			continue
		}
		pool.onEvictionScheduled = func(keyID string, streak uint32, evictAt time.Time) {
			logEviction(logger, "key_eviction_scheduled", keyID, streak, evictAt,
				"automatic eviction scheduled for a credential-dead key")
		}
		pool.onEvictionCanceled = func(keyID string) {
			logEviction(logger, "key_eviction_canceled", keyID, 0, time.Time{},
				"automatic eviction canceled: the key authenticated again")
		}
	}
}

// logEviction emits one eviction lifecycle event. nil logger (tests constructing
// pools directly) stays silent.
func logEviction(logger *slog.Logger, event, keyID string, streak uint32, evictAt time.Time, msg string) {
	if logger == nil {
		return
	}
	logger.Warn(msg, "component", "upstream", "event", event, "key_id", keyID,
		"streak", streak, "evict_at", evictAt.UTC().Format(time.RFC3339))
}

// StartEvictor launches the background sweep (v1.8.0) that removes
// credential-dead keys once their observation window lapses. It is a no-op
// when the death streak is disabled or no eviction callback is installed.
// One sweep runs per runtime; RuntimeController.start stops it with the
// runtime's context when a config apply swaps the runtime out.
func (g *Gateway) StartEvictor(ctx context.Context) {
	if g.cfg.Performance.CredentialDeathStreak <= 0 || g.evictKey == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(evictorTick)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				g.evictDueKeys()
			}
		}
	}()
}

// evictDueKeys removes one key whose eviction deadline has lapsed, preferring
// the zen pool. The eviction callback rebuilds the runtime, which stops this
// sweep when it swaps in, so exactly one key is handled per tick and the new
// runtime's sweep takes over the rest.
func (g *Gateway) evictDueKeys() {
	for _, tierPool := range []struct {
		tier config.Tier
		pool *upstreamNodes
	}{
		{config.TierZen, g.zenNodes},
		{config.TierGo, g.goNodes},
	} {
		due := tierPool.pool.EvictionDueNodes()
		if len(due) == 0 {
			continue
		}
		node := due[0]
		keyID := config.KeyDisplayID(node.key)
		if err := g.evictKey(tierPool.tier, node.key); err != nil {
			if errors.Is(err, errEvictionKeyGone) {
				continue // already removed by a concurrent apply; nothing to do
			}
			if g.logger != nil {
				g.logger.Warn("automatic eviction failed; the key stays quarantined and the sweep retries next tick",
					"component", "gateway", "event", "key_eviction_failed",
					"tier", string(tierPool.tier), "key_id", keyID, "error", err)
			}
			return
		}
		if g.logger != nil {
			g.logger.Warn("credential-dead key evicted from the config",
				"component", "gateway", "event", "key_evicted",
				"tier", string(tierPool.tier), "key_id", keyID)
		}
		return // the runtime was rebuilt under us; the new sweep takes over
	}
}
