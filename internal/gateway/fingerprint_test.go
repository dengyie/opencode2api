package gateway

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
	"opencode2api/internal/identity"
	wire "opencode2api/internal/protocol"
)

// setTestFingerprintPool restores the per-key version pool after a test that
// rewrote it: like the claimed-version atomic, the pool is package-global and
// a leaked pool would bleed into unrelated tests.
func setTestFingerprintPool(t *testing.T, versions []string) {
	t.Helper()
	previous := httpx.V2FingerprintPool()
	httpx.SetV2FingerprintPool(versions)
	t.Cleanup(func() { httpx.SetV2FingerprintPool(previous) })
}

// V2VersionForKey distributes keys across the pool deterministically and
// collapses to the global claim when the pool is absent or single-version.
func TestV2VersionForKeyDeterministicAndPooled(t *testing.T) {
	setTestV2Version(t, "2.1.1")
	setTestFingerprintPool(t, []string{"2.1.1", "2.2.7", "2.3.0"})

	first := httpx.V2VersionForKey("aaaa1111bbbb2222")
	if first == "" {
		t.Fatal("a pooled fingerprint must yield a version")
	}
	for i := 0; i < 50; i++ {
		if got := httpx.V2VersionForKey("aaaa1111bbbb2222"); got != first {
			t.Fatalf("same fingerprint yielded %q then %q; must be deterministic", first, got)
		}
	}
	if !slices.Contains([]string{"2.1.1", "2.2.7", "2.3.0"}, first) {
		t.Fatalf("yielded version %q is outside the pool", first)
	}
	// With enough fingerprints both non-compile pool entries get hit; with 3
	// versions and 24 samples a uniform hash misses a bucket with p ~ (2/3)^24.
	seen := map[string]bool{}
	for i := 0; i < 24; i++ {
		seen[httpx.V2VersionForKey(config.Fingerprint("key-"+string(rune('a'+i))))] = true
	}
	if len(seen) < 2 {
		t.Fatalf("24 fingerprints collapsed to %v; pool distribution is broken", seen)
	}
}

func TestV2VersionForKeyFallsBackToGlobal(t *testing.T) {
	setTestV2Version(t, "2.1.1")
	setTestFingerprintPool(t, nil)
	if got := httpx.V2VersionForKey("aaaa1111bbbb2222"); got != "2.1.1" {
		t.Fatalf("empty pool must fall back to the global claim, got %q", got)
	}
	setTestFingerprintPool(t, []string{"2.1.1"})
	if got := httpx.V2VersionForKey("aaaa1111bbbb2222"); got != "2.1.1" {
		t.Fatalf("single-version pool must fall back to the global claim, got %q", got)
	}
}

// The pool seeding (split out of newVersionTracker in v1.7.5 so gateway.New
// can run it before the node pools) prunes to the ceiling: no key ever claims
// above the verified ceiling. An empty ceiling maps to the compile version:
// out of the box the pool holds only the verified compile claim.
func TestVersionTrackerSeedsFingerprintPoolUnderCeiling(t *testing.T) {
	setTestV2Version(t, "2.1.1")

	seedFingerprintPool("2.2.7")
	pool := httpx.V2FingerprintPool()
	want := []string{"2.1.1", "2.2.7"}
	if !slices.Equal(pool, want) {
		t.Fatalf("pool under ceiling 2.2.7 = %v; want %v (2.3.0 pruned)", pool, want)
	}

	seedFingerprintPool("2.3.0")
	pool = httpx.V2FingerprintPool()
	want = []string{"2.1.1", "2.2.7", "2.3.0"}
	if !slices.Equal(pool, want) {
		t.Fatalf("pool under ceiling 2.3.0 = %v; want %v", pool, want)
	}

	seedFingerprintPool("")
	pool = httpx.V2FingerprintPool()
	want = []string{"2.1.1"}
	if !slices.Equal(pool, want) {
		t.Fatalf("pool under the compile ceiling = %v; want %v (nothing above the compile claim)", pool, want)
	}
}

// An auto-synced release joins the fingerprint pool, so keys distribute
// across every verified claim; the ceiling still gates entry.
func TestVersionAutoSyncWidensFingerprintPool(t *testing.T) {
	g := testVersionGateway(t, "2.4.0", true)

	g.applyVersionState("2.4.0", time.Now(), "test")
	if got := httpx.V2ClientVersion(); got != "2.4.0" {
		t.Fatalf("auto-sync must claim 2.4.0, got %q", got)
	}
	pool := httpx.V2FingerprintPool()
	if !slices.Contains(pool, "2.4.0") {
		t.Fatalf("auto-synced 2.4.0 missing from pool %v", pool)
	}

	// Above the ceiling: pending only, pool untouched.
	g = testVersionGateway(t, "2.3.0", true)
	g.applyVersionState("2.5.0", time.Now(), "test")
	pool = httpx.V2FingerprintPool()
	if slices.Contains(pool, "2.5.0") {
		t.Fatalf("above-ceiling release 2.5.0 must not enter pool %v", pool)
	}
	if got := httpx.V2ClientVersion(); got != "2.1.1" {
		t.Fatalf("above-ceiling release must not be claimed, got %q", got)
	}
}

// Two keys render different User-Agents when the pool assigns them different
// releases; an empty uaVersion (anonymous lane) keeps the global claim.
func TestUpstreamRequestUAVariesByKey(t *testing.T) {
	setTestV2Version(t, "2.1.1")
	setTestFingerprintPool(t, []string{"2.2.7", "2.3.0"})
	ids := identity.RequestIDs{Session: "ses_000000000000AAAAAAAAAAAAAA", Project: "prj_test"}
	build := func(version string) *http.Request {
		t.Helper()
		req, err := upstreamRequest(context.Background(), "https://api.example.com", wire.Chat, []byte(`{}`), ids, "key", version)
		if err != nil {
			t.Fatal(err)
		}
		return req
	}

	// Find two fingerprints the pool sends to different buckets.
	var uaA, uaB string
	for i := 0; uaA == "" || uaB == ""; i++ {
		fpA := config.Fingerprint("keyA-" + string(rune('a'+i)))
		fpB := config.Fingerprint("keyB-" + string(rune('a'+i)))
		vA, vB := httpx.V2VersionForKey(fpA), httpx.V2VersionForKey(fpB)
		if vA != vB {
			uaA, uaB = vA, vB
			break
		}
		if i > 100 {
			t.Fatal("no divergent pair found in 100 samples; distribution broken")
		}
	}

	if got := build(uaA).Header.Get("User-Agent"); got != "opencode/latest/"+uaA+"/cli" {
		t.Fatalf("key A UA = %q; want opencode/latest/%s/cli", got, uaA)
	}
	if got := build(uaB).Header.Get("User-Agent"); got != "opencode/latest/"+uaB+"/cli" {
		t.Fatalf("key B UA = %q; want opencode/latest/%s/cli", got, uaB)
	}
	if got := build("").Header.Get("User-Agent"); got != "opencode/latest/2.1.1/cli" {
		t.Fatalf("anonymous UA = %q; want the global claim opencode/latest/2.1.1/cli", got)
	}
}

// nodePool derives each node's claimed release from the key fingerprint, so
// new keys join with their own identity and rebuilt pools keep unchanged
// keys' versions stable.
func TestNodePoolDerivesPerKeyUAVersions(t *testing.T) {
	setTestV2Version(t, "2.1.1")
	setTestFingerprintPool(t, []string{"2.1.1", "2.2.7", "2.3.0"})
	transports, err := transportPool([]string{"direct", "direct"}, config.PerformanceConfig{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{"oc_sk_test_aaaa1111", "oc_sk_test_bbbb2222", "oc_sk_test_cccc3333"}
	pool, err := nodePool(keys, transports, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for i, node := range pool.nodes {
		want := httpx.V2VersionForKey(config.Fingerprint(keys[i]))
		if node.uaVersion != want {
			t.Fatalf("node %d uaVersion = %q; want %q", i, node.uaVersion, want)
		}
		if !slices.Contains([]string{"2.1.1", "2.2.7", "2.3.0"}, node.uaVersion) {
			t.Fatalf("node %d claims %q; outside the pool", i, node.uaVersion)
		}
	}

	// Rebuild: same keys, same versions.
	rebuilt, err := nodePool(keys, transports, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for i := range pool.nodes {
		if rebuilt.nodes[i].uaVersion != pool.nodes[i].uaVersion {
			t.Fatalf("rebuild changed node %d from %q to %q; versions must be stable",
				i, pool.nodes[i].uaVersion, rebuilt.nodes[i].uaVersion)
		}
	}
}

// gateway.New must seed the fingerprint pool before it builds the node pools:
// the nodes materialize their claimed release from the pool once, at build
// time. This pins the v1.7.5 ordering fix — a cold start with a multi-version
// pool must yield per-key claims, not the global compile version frozen into
// every node.
func TestNewSeedsFingerprintPoolBeforeNodePools(t *testing.T) {
	setTestV2Version(t, "2.1.1")
	doc := `{
		"listen": "127.0.0.1:0",
		"server_keys": ["sk-local"],
		"zen_keys": ["oc_sk_test_aaaa1111", "oc_sk_test_bbbb2222", "oc_sk_test_cccc3333"],
		"anonymous": false,
		"proxies": ["direct"],
		"upstream": {"zen": "https://zen.example.com", "go": "https://go.example.com"},
		"performance": {"version_sync": {"ceiling": "2.3.0"}}
	}`
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	g, err := New(cfg, quietLogger(), nil, nil)
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	pool := httpx.V2FingerprintPool()
	want := []string{"2.1.1", "2.2.7", "2.3.0"}
	if !slices.Equal(pool, want) {
		t.Fatalf("cold-start pool = %v; want %v", pool, want)
	}
	keys := cfg.ZenKeys
	if len(keys) != 3 {
		t.Fatalf("expected 3 zen keys, got %d", len(keys))
	}
	seen := map[string]bool{}
	for i, node := range g.zenNodes.nodes {
		wantVersion := httpx.V2VersionForKey(config.Fingerprint(keys[i]))
		if node.uaVersion != wantVersion {
			t.Fatalf("cold-start node %d uaVersion = %q; want the pool assignment %q", i, node.uaVersion, wantVersion)
		}
		seen[node.uaVersion] = true
	}
	if len(seen) < 2 {
		t.Fatalf("cold start collapsed all nodes to %v; per-key isolation did not engage", seen)
	}
}

// The anonymous lane must render one identity everywhere. The pool cannot
// special-case the anonymous token — consistency is the callers' contract:
// anonymous inference, probes and model listings pass uaVersion="" (the
// global claimed version), keyed lanes pass the node's own claim. The pool
// seeded for this test holds only entries other than the compile claim, so
// any caller that derived a claim from the anonymous token would render a
// UA that differs from what anonymous inference sends; the anonymous render
// here must stay on the global version (v1.7.5).
func TestAnonymousLaneSharesGlobalClaim(t *testing.T) {
	setTestV2Version(t, "2.1.1")
	setTestFingerprintPool(t, []string{"2.2.7", "2.3.0"})
	if got := httpx.V2UserAgentForVersion(""); got != "opencode/latest/2.1.1/cli" {
		t.Fatalf("anonymous UA = %q; want the global claim opencode/latest/2.1.1/cli", got)
	}
	for _, version := range httpx.V2FingerprintPool() {
		if got := httpx.V2UserAgentForVersion(version); got == httpx.V2UserAgentForVersion("") {
			t.Fatalf("pool version %q renders the anonymous lane's UA; a leaked per-key claim would be invisible", version)
		}
	}
}
