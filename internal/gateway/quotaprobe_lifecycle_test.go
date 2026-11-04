package gateway

import (
	"context"
	"testing"
	"time"
)

// The probe loop lifecycle: rearmQuotaProbes arms the loop for an enabled
// config (and the armed loop actually probes a walled key against the fake
// upstream), cancels it for a disabled config, and never stacks loops across
// repeated applies. This is the regression guard for making
// quota_probe_seconds hot-reloadable.
func TestRearmQuotaProbesLifecycle(t *testing.T) {
	upstream := fakeUpstream(t, nil)
	// Pools carry the probe interval, so build the gateway with the knob on;
	// the controller decides whether the LOOP runs.
	g := gatewayFor(t, upstream.URL, `{"max_attempts": 1, "timeout_seconds": 30}`, `{"quota_probe_seconds": 1}`)
	root, cancelRoot := context.WithCancel(context.Background())
	t.Cleanup(cancelRoot)
	m := &RuntimeController{root: root, logger: g.logger}
	m.current.Store(&gatewayRuntime{gateway: g, config: g.cfg})

	// Knob off: nothing armed.
	disabled := g.cfg
	disabled.Performance.QuotaProbeSeconds = 0
	m.rearmQuotaProbes(disabled)
	if m.quotaProbeCancel != nil {
		t.Fatal("disabled config must not arm the probe loop")
	}

	// Enabled: loop armed, and a quota-walled key is probed through the fake
	// upstream, whose probe model answers 200 — the cooldown must clear
	// without any live traffic.
	walled := g.zenNodes.nodes[0]
	g.zenNodes.MarkFailure(walled, quotaResponse("3600"), nil)
	if walled.quotaUntil.Load() == 0 {
		t.Fatal("precondition: key must be quota-cooled")
	}
	m.rearmQuotaProbes(g.cfg)
	if m.quotaProbeCancel == nil {
		t.Fatal("enabled config must arm the probe loop")
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if walled.cooldownUntil.Load() == 0 && walled.quotaUntil.Load() == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if walled.cooldownUntil.Load() != 0 || walled.quotaUntil.Load() != 0 {
		t.Fatal("the re-armed loop never probed the walled key back to health")
	}

	// Disabled again: the loop is canceled, and a second enable never stacks
	// on top of a previous loop.
	m.rearmQuotaProbes(disabled)
	if m.quotaProbeCancel != nil {
		t.Fatal("disabled re-arm must cancel the loop")
	}
	m.rearmQuotaProbes(g.cfg)
	if m.quotaProbeCancel == nil {
		t.Fatal("re-enabling must arm the loop again")
	}
}
