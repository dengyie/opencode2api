package gateway

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"opencode2api/internal/config"
	models "opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

// testKeyPool builds a pool with n keys against a nil-transport-free stub.
// nodePool requires transports, but MarkFailure paths only touch the nodes,
// so a minimal pool via the zero-value struct keeps these tests focused.
func testKeyPool(n int, probeInterval time.Duration) *upstreamNodes {
	p := &upstreamNodes{cooldown: time.Second, quotaProbeInterval: probeInterval}
	for i := 0; i < n; i++ {
		p.nodes = append(p.nodes, &upstreamNode{key: string(rune('a' + i)), index: i, preferredProxy: 0})
	}
	return p
}

func quotaResponse(retryAfter string) *http.Response {
	header := http.Header{}
	if retryAfter != "" {
		header.Set("Retry-After", retryAfter)
	}
	return &http.Response{StatusCode: http.StatusTooManyRequests, Header: header}
}

// A 429 marks the key quota-cooled with the same instant as its cooldown.
func TestMarkFailure429MarksQuota(t *testing.T) {
	pool := testKeyPool(2, 10*time.Minute)
	node := pool.nodes[0]
	pool.MarkFailure(node, quotaResponse(""), nil)
	if node.quotaUntil.Load() == 0 {
		t.Fatal("429 must mark quotaUntil")
	}
	if node.quotaUntil.Load() != node.cooldownUntil.Load() {
		t.Fatal("quotaUntil must match cooldownUntil for a 429")
	}
	if time.Until(time.Unix(0, node.probeNextAt.Load())) < 9*time.Minute {
		t.Fatalf("probeNextAt must be one interval out, got %v", time.Until(time.Unix(0, node.probeNextAt.Load())))
	}
	// The other key is untouched.
	if pool.nodes[1].quotaUntil.Load() != 0 {
		t.Fatal("unrelated key must stay unmarked")
	}
}

// Retry-After dominates the cooldown instant.
func TestMarkFailureRetryAfterAdopted(t *testing.T) {
	pool := testKeyPool(1, 10*time.Minute)
	node := pool.nodes[0]
	pool.MarkFailure(node, quotaResponse("12564"), nil)
	until := time.Unix(0, node.quotaUntil.Load())
	if remaining := time.Until(until); remaining < 12560*time.Second {
		t.Fatalf("Retry-After 12564s must be adopted, got %v", remaining)
	}
}

// A 401/403/5xx cools the key but does not mark quota.
func TestMarkFailureNonQuotaNoQuotaMark(t *testing.T) {
	pool := testKeyPool(1, 10*time.Minute)
	node := pool.nodes[0]
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusInternalServerError} {
		pool.MarkFailure(node, &http.Response{StatusCode: status, Header: http.Header{}}, nil)
		if node.quotaUntil.Load() != 0 {
			t.Fatalf("status %d must not mark quotaUntil", status)
		}
		if node.cooldownUntil.Load() == 0 {
			t.Fatalf("status %d must cool the key", status)
		}
		node.cooldownUntil.Store(0)
	}
}

// MarkSuccess and quota recovery clear the quota state.
func TestMarkSuccessClearsQuota(t *testing.T) {
	pool := testKeyPool(1, 10*time.Minute)
	node := pool.nodes[0]
	pool.MarkFailure(node, quotaResponse(""), nil)
	pool.MarkSuccess(node)
	if node.quotaUntil.Load() != 0 || node.cooldownUntil.Load() != 0 || node.probeNextAt.Load() != 0 {
		t.Fatal("MarkSuccess must clear quota state")
	}
}

// AllQuotaCooled reports the earliest recovery and refuses partially free pools.
func TestAllQuotaCooled(t *testing.T) {
	pool := testKeyPool(3, 0)
	if _, ok := pool.AllQuotaCooled(); ok {
		t.Fatal("empty-ish pool without keys marked cannot be walled")
	}
	now := time.Now()
	pool.nodes[0].quotaUntil.Store(now.Add(time.Hour).UnixNano())
	if _, ok := pool.AllQuotaCooled(); ok {
		t.Fatal("one cooled key of three is not a wall")
	}
	pool.nodes[1].quotaUntil.Store(now.Add(2 * time.Hour).UnixNano())
	pool.nodes[2].quotaUntil.Store(now.Add(30 * time.Minute).UnixNano())
	earliest, ok := pool.AllQuotaCooled()
	if !ok {
		t.Fatal("all keys cooled must report a wall")
	}
	if want := now.Add(30 * time.Minute); earliest.Sub(want) > time.Minute || want.Sub(earliest) > time.Minute {
		t.Fatalf("earliest = %v, want about %v", earliest, want)
	}
}

// Empty and nil pools never report a wall (nothing to pre-check).
func TestAllQuotaCooledEmpty(t *testing.T) {
	pool := testKeyPool(0, 0)
	if _, ok := pool.AllQuotaCooled(); ok {
		t.Fatal("an empty pool must not report a wall")
	}
	var nilPool *upstreamNodes
	if _, ok := nilPool.AllQuotaCooled(); ok {
		t.Fatal("a nil pool must not report a wall")
	}
}

// The GEO classifier matches the region wordings on 4xx only.
func TestGeoBlockClassification(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   bool
	}{
		{http.StatusForbidden, `{"error":{"message":"This model is not available in your country"}}`, true},
		{http.StatusBadRequest, `{"error":{"message":"region not supported"}}`, true},
		{http.StatusTooManyRequests, `{"error":{"message":"not available in your region right now"}}`, true},
		{http.StatusForbidden, `{"error":{"message":"free tier can only be used from within OpenCode"}}`, false},
		{http.StatusForbidden, `{"error":{"message":"Free usage limit reached"}}`, false},
		{http.StatusInternalServerError, `{"error":{"message":"region not supported"}}`, false},
		{http.StatusForbidden, `{}`, false},
	}
	for _, tc := range cases {
		if got := isGeoBlockResponse(tc.status, []byte(tc.body)); got != tc.want {
			t.Errorf("isGeoBlockResponse(%d, %q) = %v, want %v", tc.status, tc.body, got, tc.want)
		}
	}
	if !strings.Contains(geoBlockedMessage, "region") {
		t.Fatal("the client message must name the region cause")
	}
}

// The empirical anonymous-wall signal: two 429s within the window arm it, a
// 2xx clears it, non-429 failures do not feed it, and a stale window
// disarms it.
func TestAnonymousWallSignal(t *testing.T) {
	g := &Gateway{}
	if g.anonymousLaneWalled() {
		t.Fatal("no observations must not arm the wall")
	}
	g.observeAnonymousWall(http.StatusTooManyRequests)
	if g.anonymousLaneWalled() {
		t.Fatal("a single 429 must not arm the wall")
	}
	g.observeAnonymousWall(http.StatusTooManyRequests)
	if !g.anonymousLaneWalled() {
		t.Fatal("two 429s within the window must arm the wall")
	}
	g.observeAnonymousWall(http.StatusOK)
	if g.anonymousLaneWalled() {
		t.Fatal("a 2xx must clear the wall signal")
	}
	g.observeAnonymousWall(http.StatusForbidden)
	if g.anonymousLaneWalled() {
		t.Fatal("a non-429 failure must not arm the wall")
	}
	g.observeAnonymousWall(http.StatusTooManyRequests)
	g.observeAnonymousWall(http.StatusTooManyRequests)
	if !g.anonymousLaneWalled() {
		t.Fatal("re-armed after the clear")
	}
	g.anonWallLastAt.Store(time.Now().Add(-11 * time.Minute).UnixNano())
	if g.anonymousLaneWalled() {
		t.Fatal("a stale window must disarm the wall")
	}
}

// The pre-check covers anonymous-first routes (the tebi free-model shape):
// with the anonymous lane enabled and empirically walled, walled keys
// pre-check the request; a healthy anonymous lane stands it down; a 2xx
// observation clears it.
func TestQuotaWallCoversAnonymousRoute(t *testing.T) {
	upstream := fakeUpstream(t, nil)
	g := gatewayFor(t, upstream.URL, `{"max_attempts": 1, "timeout_seconds": 30}`, `{"heartbeat_seconds": 0}`)
	// Flip the config to the tebi shape: anonymous lane enabled. The gateway
	// was built with anonymous=false, so patch the field the wall logic reads.
	g.cfg.Anonymous = true
	g.cfg.Upstream.Anonymous = upstream.URL
	node := g.zenNodes.nodes[0]
	g.zenNodes.MarkFailure(node, quotaResponse("3600"), nil)

	// No anonymous observations yet: the lane might still serve, stand down.
	if _, walled := g.quotaWall(); walled {
		t.Fatal("healthy anonymous lane must stand down the pre-check")
	}
	// Two anonymous 429s: empirically walled, keys walled → pre-check fires.
	g.observeAnonymousWall(http.StatusTooManyRequests)
	g.observeAnonymousWall(http.StatusTooManyRequests)
	until, walled := g.quotaWall()
	if !walled {
		t.Fatal("walled keys + empirically walled anonymous lane must pre-check")
	}
	if time.Until(until) < 3590*time.Second {
		t.Fatalf("retry-after must track the key recovery instant, got %v", time.Until(until))
	}
	// A 2xx on the anonymous lane stands the pre-check back down.
	g.observeAnonymousWall(http.StatusOK)
	if _, walled := g.quotaWall(); walled {
		t.Fatal("a 2xx observation must stand down the pre-check")
	}
}

// Live observation overrides the capability catalog's reasoning flag: with
// every free model flagged reasoning=true upstream, a model empirically
// observed to answer without reasoning becomes the preferred probe target.
func TestQuotaProbeModelPrefersObservedNonReasoning(t *testing.T) {
	upstream := fakeUpstream(t, nil)
	g := gatewayFor(t, upstream.URL, `{"max_attempts": 1, "timeout_seconds": 30}`, `{"quota_probe_seconds": 1}`)
	g.catalog.ReplaceWithCapabilities(
		[]string{"deepseek-v4.1-flash-free", "crouton-alpha-free"},
		nil,
		map[config.Tier]map[string]wire.Protocol{config.TierZen: {
			"deepseek-v4.1-flash-free": wire.Chat,
			"crouton-alpha-free":       wire.Chat,
		}},
		nil,
		// Upstream marks BOTH reasoning=true (observed in production).
		map[config.Tier]map[string]models.Metadata{config.TierZen: {
			"deepseek-v4.1-flash-free": {Reasoning: true},
			"crouton-alpha-free":       {Reasoning: true},
		}},
	)
	// No observation yet: both are metadata-reasoning, so the fallback picks
	// the alphabetically first free model.
	model, _, ok := g.quotaProbeModel(true)
	if !ok || model != "crouton-alpha-free" {
		t.Fatalf("unobserved fallback = %q ok=%v, want crouton", model, ok)
	}
	// Live observation: crouton answers WITHOUT reasoning → the observed flag
	// overrides the catalog and crouton becomes the pick.
	g.noteReasoningObservation("crouton-alpha-free", false)
	model, _, ok = g.quotaProbeModel(true)
	if !ok || model != "crouton-alpha-free" {
		t.Fatalf("observed-non-reasoning preference = %q ok=%v, want crouton", model, ok)
	}
	// The reverse observation excludes crouton entirely: the probe falls back
	// to the other model despite its metadata claiming reasoning=true.
	g.noteReasoningObservation("crouton-alpha-free", true)
	model, _, ok = g.quotaProbeModel(true)
	if !ok || model != "deepseek-v4.1-flash-free" {
		t.Fatalf("crouton excluded by observation = %q ok=%v, want deepseek", model, ok)
	}
}
