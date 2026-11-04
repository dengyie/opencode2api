package gateway

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// The inflight gate (v1.5.0) bounds how many inference turns run at once.
// Every free-tier request spends real upstream quota, and a burst of parallel
// turns (a client fanning out, or a retrying frontend) spends it at once —
// the wall hits everywhere simultaneously and every in-flight turn is lost.
// Serializing through a bounded queue turns that burst into a steady drip.
// Disabled when performance.max_inflight is zero, which is the default: the
// pre-v1.5 behavior has no admission control.

// errInflightBusy reports a request that found the gate full and could not
// obtain a slot within queue_wait_seconds.
var errInflightBusy = errors.New("inflight limit reached")

type inflightGate struct {
	slots    chan struct{}
	wait     time.Duration
	inflight atomic.Int64
	queued   atomic.Int64
	// queuedTotal counts every request that had to wait for a slot, so
	// saturation stays visible cumulatively (healthz load.queued_total) and
	// not only as the instantaneous queued gauge.
	queuedTotal atomic.Int64
}

func newInflightGate(maxInflight, queueWaitSeconds int) *inflightGate {
	if maxInflight <= 0 {
		return nil
	}
	return &inflightGate{
		slots: make(chan struct{}, maxInflight),
		wait:  time.Duration(queueWaitSeconds) * time.Second,
	}
}

// acquire occupies one slot for the whole turn. A full gate queues the caller
// for up to wait (0 rejects immediately) before giving up with
// errInflightBusy. The returned release function must be called exactly once;
// waited reports how long the caller was queued (0 when a slot was free), so
// the caller can log saturation it would otherwise only see by polling.
func (g *inflightGate) acquire(ctx context.Context) (func(), time.Duration, error) {
	if g == nil {
		return func() {}, 0, nil
	}
	release := func() {
		<-g.slots
		g.inflight.Add(-1)
	}
	select {
	case g.slots <- struct{}{}:
		g.inflight.Add(1)
		return release, 0, nil
	default:
	}
	queuedAt := time.Now()
	g.queued.Add(1)
	g.queuedTotal.Add(1)
	defer g.queued.Add(-1)
	if g.wait <= 0 {
		return nil, 0, errInflightBusy
	}
	timer := time.NewTimer(g.wait)
	defer timer.Stop()
	select {
	case g.slots <- struct{}{}:
		g.inflight.Add(1)
		return release, time.Since(queuedAt), nil
	case <-ctx.Done():
		return nil, time.Since(queuedAt), ctx.Err()
	case <-timer.C:
		return nil, time.Since(queuedAt), errInflightBusy
	}
}
