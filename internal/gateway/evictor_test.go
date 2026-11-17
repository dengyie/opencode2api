package gateway

import (
	"errors"
	"testing"

	"opencode2api/internal/config"
	"time"
)

// testEvictPool builds a pool like testKeyPool with the v1.8.0 death-streak
// knob and the observation window (credential quarantine) set.
func testEvictPool(n, streak int, quarantine time.Duration) *upstreamNodes {
	p := testKeyPool(n, 10*time.Minute)
	p.credentialDeathStreak = streak
	p.credentialQuarantine = quarantine
	return p
}

// Five consecutive 401s arm the eviction at the end of the observation window
// (= the credential quarantine stretch).
func TestMarkFailure401StreakArmsEviction(t *testing.T) {
	pool := testEvictPool(1, 5, time.Hour)
	node := pool.nodes[0]
	for i := 1; i <= 5; i++ {
		pool.MarkFailure(node, credResponse(401, ""), nil)
	}
	if node.credStreak.Load() != 5 {
		t.Fatalf("streak must be 5 after five 401s, got %d", node.credStreak.Load())
	}
	evictAt := node.evictAt.Load()
	if evictAt == 0 {
		t.Fatal("reaching the death streak must arm evictAt")
	}
	if remaining := time.Until(time.Unix(0, evictAt)); remaining < 55*time.Minute {
		t.Fatalf("evictAt must sit one quarantine stretch out, got %v", remaining)
	}
	if pool.EvictionDueNodes() != nil && len(pool.EvictionDueNodes()) != 0 {
		t.Fatal("eviction must not be due before the window lapses")
	}
	if pool.EvictionPending() != 1 {
		t.Fatalf("health must report one pending eviction, got %d", pool.EvictionPending())
	}
}

// A streak below the threshold does not arm anything.
func TestMarkFailure401BelowThresholdLeavesEvictionUnarmed(t *testing.T) {
	pool := testEvictPool(1, 5, time.Hour)
	node := pool.nodes[0]
	for i := 0; i < 4; i++ {
		pool.MarkFailure(node, credResponse(401, ""), nil)
	}
	if node.evictAt.Load() != 0 {
		t.Fatal("below the threshold no eviction may be armed")
	}
	if pool.EvictionPending() != 0 {
		t.Fatal("no pending eviction below the threshold")
	}
}

// Zero disables the feature entirely, matching the pre-v1.8 behavior.
func TestMarkFailure401DisabledFeatureNeverArms(t *testing.T) {
	pool := testEvictPool(1, 0, time.Hour)
	node := pool.nodes[0]
	for i := 0; i < 10; i++ {
		pool.MarkFailure(node, credResponse(401, ""), nil)
	}
	if node.evictAt.Load() != 0 || pool.EvictionPending() != 0 {
		t.Fatal("credential_death_streak=0 must disable the feature")
	}
}

// A real inference success before the deadline cancels the eviction and clears
// the streak; a listing success deliberately does neither.
func TestEvictionCancelSemantics(t *testing.T) {
	pool := testEvictPool(1, 5, time.Hour)
	node := pool.nodes[0]
	for i := 0; i < 5; i++ {
		pool.MarkFailure(node, credResponse(401, ""), nil)
	}
	if node.evictAt.Load() == 0 {
		t.Fatal("precondition: eviction armed")
	}
	// A listing success (the catalog refresher, seen live on vJ7MJ answering
	// 200 while inference 401s) clears the quarantine but not the streak.
	pool.MarkListingSuccess(node)
	if node.credStreak.Load() != 5 || node.evictAt.Load() == 0 {
		t.Fatal("MarkListingSuccess must not touch the streak or the eviction")
	}
	// A real inference success cancels it.
	pool.MarkSuccess(node)
	if node.credStreak.Load() != 0 || node.evictAt.Load() != 0 {
		t.Fatal("MarkSuccess must clear the streak and cancel the eviction")
	}
	if pool.EvictionPending() != 0 {
		t.Fatal("no pending eviction after a real success")
	}
}

// A 403 (the other credential class) counts toward the same streak.
func TestMarkFailure403CountsTowardStreak(t *testing.T) {
	pool := testEvictPool(1, 5, time.Hour)
	node := pool.nodes[0]
	for i := 0; i < 3; i++ {
		pool.MarkFailure(node, credResponse(401, ""), nil)
	}
	pool.MarkFailure(node, credResponse(403, ""), nil)
	pool.MarkFailure(node, credResponse(403, ""), nil)
	if node.evictAt.Load() == 0 {
		t.Fatal("403s must count toward the credential streak")
	}
}

// A lapsed deadline plus a streak still at the threshold means the sweep
// removes the key.
func TestEvictionDueNodesAfterWindowLapses(t *testing.T) {
	pool := testEvictPool(2, 5, time.Hour)
	dead, alive := pool.nodes[0], pool.nodes[1]
	for i := 0; i < 5; i++ {
		pool.MarkFailure(dead, credResponse(401, ""), nil)
	}
	// Pretend the window lapsed.
	dead.evictAt.Store(time.Now().Add(-time.Minute).UnixNano())
	due := pool.EvictionDueNodes()
	if len(due) != 1 || due[0] != dead {
		t.Fatalf("exactly the dead key must be due, got %v", due)
	}
	// A recovered key is never due even with a lapsed deadline.
	pool.MarkSuccess(alive)
	alive.evictAt.Store(time.Now().Add(-time.Minute).UnixNano())
	alive.credStreak.Store(5)
	alive.evictAt.Store(0) // MarkSuccess cleared it; simulate the aftermath
	if len(pool.EvictionDueNodes()) != 1 {
		t.Fatal("the recovered key must stay out of the due set")
	}
}

// The quota-probe path never counts toward the streak: RepinCredentialQuarantine
// does not touch credStreak or evictAt.
func TestQuotaProbeRepinDoesNotCountTowardStreak(t *testing.T) {
	pool := testEvictPool(1, 5, time.Hour)
	node := pool.nodes[0]
	node.credUntil.Store(time.Now().Add(30 * time.Minute).UnixNano())
	pool.RepinCredentialQuarantine(node)
	if node.credStreak.Load() != 0 || node.evictAt.Load() != 0 {
		t.Fatal("probe repins must not count toward the death streak")
	}
}

// evictDueKeys calls the callback exactly once per tick with the due key and
// honors callback failures.
func TestEvictDueKeysInvokesCallback(t *testing.T) {
	g := &Gateway{logger: nil}
	pool := testEvictPool(2, 5, time.Hour)
	g.zenNodes = pool
	g.goNodes = testEvictPool(0, 5, time.Hour)
	dead := pool.nodes[0]
	for i := 0; i < 5; i++ {
		pool.MarkFailure(dead, credResponse(401, ""), nil)
	}
	dead.evictAt.Store(time.Now().Add(-time.Minute).UnixNano())

	var calls []string
	g.evictKey = func(_ config.Tier, key string) error {
		calls = append(calls, key)
		return nil
	}
	g.evictDueKeys()
	if len(calls) != 1 || calls[0] != dead.key {
		t.Fatalf("the sweep must evict exactly the dead key, got %v", calls)
	}
}

func TestEvictDueKeysCallbackFailureStopsSweep(t *testing.T) {
	g := &Gateway{logger: nil}
	pool := testEvictPool(1, 5, time.Hour)
	g.zenNodes = pool
	g.goNodes = testEvictPool(0, 5, time.Hour)
	dead := pool.nodes[0]
	for i := 0; i < 5; i++ {
		pool.MarkFailure(dead, credResponse(401, ""), nil)
	}
	dead.evictAt.Store(time.Now().Add(-time.Minute).UnixNano())

	called := 0
	g.evictKey = func(_ config.Tier, key string) error {
		called++
		return errors.New("apply failed")
	}
	g.evictDueKeys()
	if called != 1 {
		t.Fatalf("a failed callback must be retried next tick, got %d calls", called)
	}
}
