package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
)

// setTestV2Version restores the compile-time claim after a test that rewrote
// it: the atomic is package-global, so a leaked version would bleed into
// unrelated tests.
func setTestV2Version(t *testing.T, version string) {
	t.Helper()
	previous := httpx.V2ClientVersion()
	httpx.SetV2ClientVersion(version)
	t.Cleanup(func() { httpx.SetV2ClientVersion(previous) })
}

// parseReleaseTag accepts only bare numeric releases.
func TestParseReleaseTag(t *testing.T) {
	accept := map[string]string{
		"v2.3.0":    "2.3.0",
		"2.3.0":     "2.3.0",
		"V2.3.0":    "2.3.0",
		"v2.3":      "2.3",
		" v2.10.0 ": "2.10.0",
	}
	for tag, want := range accept {
		got, ok := parseReleaseTag(tag)
		if !ok || got != want {
			t.Fatalf("parseReleaseTag(%q) = %q,%v; want %q,true", tag, got, ok, want)
		}
	}
	reject := []string{"", "process-daemon-v0.5.2", "v2.3.0-beta.1", "v2.3.0+build", "latest", "v2.x.0", "v2.3.0.1"}
	for _, tag := range reject {
		if _, ok := parseReleaseTag(tag); ok {
			t.Fatalf("parseReleaseTag(%q) must be rejected", tag)
		}
	}
}

func TestVersionNewer(t *testing.T) {
	if !versionNewer("2.3.0", "2.1.1") {
		t.Fatal("2.3.0 must be newer than 2.1.1")
	}
	if versionNewer("2.3", "2.3.0") {
		t.Fatal("padded forms compare equal: 2.3 == 2.3.0, not newer")
	}
	if versionNewer("2.1.1", "2.3.0") {
		t.Fatal("older must not be newer")
	}
	if versionNewer("2.1.1", "2.1.1") {
		t.Fatal("equal versions are not newer")
	}
	if versionNewer("garbage", "2.1.1") || versionNewer("2.1.1", "") {
		t.Fatal("malformed operands must never claim an upgrade")
	}
}

// feedServer serves a GitHub-shaped release list with tags that mimic the
// real repository's mix of client and component releases.
func feedServer(t *testing.T, entries []versionFeedEntry) *httptest.Server {
	t.Helper()
	body, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	}))
	t.Cleanup(server.Close)
	return server
}

// fetchLatestVersion skips component tags, drafts, and prereleases.
func TestFetchLatestVersionFiltersTags(t *testing.T) {
	server := feedServer(t, []versionFeedEntry{
		{TagName: "process-daemon-v0.5.2"},
		{TagName: "v2.4.0-rc.1", Prerelease: true},
		{TagName: "v2.3.0"},
		{TagName: "v2.2.7"},
	})
	version, err := fetchLatestVersion(context.Background(), server.Client(), server.URL)
	if err != nil {
		t.Fatalf("fetchLatestVersion: %v", err)
	}
	if version != "2.3.0" {
		t.Fatalf("latest stable release = %q, want 2.3.0", version)
	}
}

func TestFetchLatestVersionNoStableRelease(t *testing.T) {
	server := feedServer(t, []versionFeedEntry{{TagName: "process-daemon-v0.5.2"}, {TagName: "v2.4.0-rc.1", Prerelease: true}})
	if _, err := fetchLatestVersion(context.Background(), server.Client(), server.URL); err == nil {
		t.Fatal("a feed with no stable client release must error")
	}
}

// testVersionGateway builds a Gateway with only what applyVersionState needs.
// It pins the claimed version to the compile-time one so the ceiling
// assertions below stay meaningful regardless of test order.
func testVersionGateway(t *testing.T, ceiling string, autoSync bool) *Gateway {
	t.Helper()
	setTestV2Version(t, "2.1.1")
	return &Gateway{
		logger:         testLogger(t),
		cfg:            config.Config{Performance: config.PerformanceConfig{VersionSync: config.VersionSyncConfig{AutoSync: autoSync, Ceiling: ceiling}}},
		versionTracker: newVersionTracker(ceiling, autoSync),
	}
}

func TestVersionStateTracksFeed(t *testing.T) {
	g := testVersionGateway(t, "", false)
	now := time.Now()
	g.applyVersionState("2.1.1", now, "test")
	state := g.versionTracker.state
	if !state.Synced || state.Pending {
		t.Fatalf("same-version feed must be synced and not pending: %+v", state)
	}
	g.applyVersionState("2.3.0", now, "test")
	state = g.versionTracker.state
	if state.Latest != "2.3.0" || state.Synced {
		t.Fatalf("detection must record the newer release without syncing: %+v", state)
	}
	if state.Active != "2.1.1" {
		t.Fatalf("detection must not change the claimed version: %q", state.Active)
	}
}

func TestVersionAutoSyncAppliesBelowCeiling(t *testing.T) {
	g := testVersionGateway(t, "2.9.0", true)
	g.applyVersionState("2.3.0", time.Now(), "test")
	if got := httpx.V2ClientVersion(); got != "2.3.0" {
		t.Fatalf("auto-sync below the ceiling must apply the release, got %q", got)
	}
	if !g.versionTracker.state.Synced || g.versionTracker.state.Pending {
		t.Fatalf("applied state must be synced and not pending: %+v", g.versionTracker.state)
	}
	if ua := httpx.V2UserAgent(); ua != "opencode/latest/2.3.0/cli" {
		t.Fatalf("V2UserAgent must reflect the applied version, got %q", ua)
	}
}

// The ceiling holds even with auto-sync on: a release the operator has not
// verified is recorded as pending, never claimed.
func TestVersionAutoSyncClampedToCeiling(t *testing.T) {
	g := testVersionGateway(t, "2.2.0", true)
	g.applyVersionState("2.3.0", time.Now(), "test")
	if got := httpx.V2ClientVersion(); got != "2.1.1" {
		t.Fatalf("above-ceiling release must not be applied, claimed %q", got)
	}
	if !g.versionTracker.state.Pending {
		t.Fatalf("above-ceiling release must be recorded as pending: %+v", g.versionTracker.state)
	}
	if g.versionTracker.state.Synced {
		t.Fatalf("above-ceiling release must not be recorded as synced: %+v", g.versionTracker.state)
	}
}

func TestVersionDetectionWithoutAutoSync(t *testing.T) {
	g := testVersionGateway(t, "2.9.0", false)
	g.applyVersionState("2.3.0", time.Now(), "test")
	if got := httpx.V2ClientVersion(); got != "2.1.1" {
		t.Fatalf("detection without auto-sync must not change the claim, got %q", got)
	}
	if g.versionTracker.state.Synced {
		t.Fatalf("state must record the drift: %+v", g.versionTracker.state)
	}
}

func TestVersionTrackerSeedClampsToCompileVersion(t *testing.T) {
	setTestV2Version(t, "2.1.1")
	tracker := newVersionTracker("", true)
	if tracker.ceiling != "2.1.1" {
		t.Fatalf("empty ceiling must seed from the compile-time claim, got %q", tracker.ceiling)
	}
}

// The v2 UA reads the atomic on every render, so a swap is visible to new
// requests without a restart, and V1UserAgent is untouched by the tracker.
func TestUserAgentAtomicSwap(t *testing.T) {
	setTestV2Version(t, "2.1.1")
	if ua := httpx.V2UserAgent(); ua != "opencode/latest/2.1.1/cli" {
		t.Fatalf("baseline UA mismatch: %q", ua)
	}
	httpx.SetV2ClientVersion("2.3.0")
	if ua := httpx.V2UserAgent(); ua != "opencode/latest/2.3.0/cli" {
		t.Fatalf("UA must follow the atomic: %q", ua)
	}
	if ua := httpx.V1UserAgent(); ua != "opencode/1.19.3" {
		t.Fatalf("v1 UA must be untouched by the v2 tracker: %q", ua)
	}
}

// The v1 lane's header render must keep using the captured SDK pairing; the
// tracker deliberately does not cover it.
func TestV1HeadersKeepCapturedSDKPairing(t *testing.T) {
	httpx.SetV2ClientVersion("9.9.9")
	t.Cleanup(func() { httpx.SetV2ClientVersion("2.1.1") })
	if httpx.V1ChatSDK != "4.0.23" || httpx.V1ResponsesSDK != "4.0.40" || httpx.V1AnthropicSDK != "4.0.46" {
		t.Fatal("the v1 SDK pairings are part of the masquerade fingerprint and must not drift with the v2 tracker")
	}
}
