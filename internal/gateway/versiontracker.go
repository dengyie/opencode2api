package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"opencode2api/internal/httpx"
)

// The version tracker (v1.7.2) keeps the v2 lane's masquerade version aligned
// with the official OpenCode client releases, the zcode2api dynamic-version
// pattern: poll a public release feed, compare semvers, and atomically swap
// the claimed version behind httpx.V2UserAgent. Two locks keep it from
// creating a fingerprint upstream never shipped: a release feed must parse as
// a bare numeric version, and an auto-applied version never crosses the
// configured ceiling (empty = the compile-time version), because everything
// else the gateway claims — the v1 client, Bun runtime, and the per-protocol
// provider-utils pairings — was captured from one specific release and has no
// public mapping to a newer one.

const (
	// versionSyncInterval is the default poll cadence, matching the model
	// catalog's "hours, not minutes" refresh philosophy. A release matters
	// at most when upstream starts rejecting old claims.
	versionSyncInterval = 12 * time.Hour
	// versionSyncTimeout bounds one feed fetch. The feed is GitHub's public
	// API; a slow answer must not delay anything else.
	versionSyncTimeout = 15 * time.Second
	// versionFeedURL lists the official client's releases, newest first.
	// Overridden in tests.
	versionFeedURL = "https://api.github.com/repos/anomalyco/opencode/releases?per_page=30"
)

// versionFeedEntry is one release of the feed. The tag carries the version
// ("v2.3.0"); prerelease marks drafts the official client does not ship.
type versionFeedEntry struct {
	TagName    string `json:"tag_name"`
	Prerelease bool   `json:"prerelease"`
	Draft      bool   `json:"draft"`
}

// versionState is the tracker's shared snapshot: the newest release the feed
// reported and the release currently claimed on the v2 lane.
type versionState struct {
	Latest   string     `json:"latest"`
	Active   string     `json:"active"`
	Ceiling  string     `json:"ceiling"`
	Synced   bool       `json:"synced"`
	Pending  bool       `json:"pending"`
	CheckAt  *time.Time `json:"checked_at,omitempty"`
	Source   string     `json:"source,omitempty"`
	AutoSync bool       `json:"auto_sync"`
}

// versionTracker is the gateway's client-version sync loop. State is guarded
// by mu because ApplyConfig may rebuild the gateway while a fetch is in
// flight.
type versionTracker struct {
	mu       sync.Mutex
	state    versionState
	ceiling  string
	autoSync bool
	client   *http.Client
}

// newVersionTracker seeds the state from the config. ceiling empty means the
// compile-time version, so out of the box auto-sync can never claim a release
// the operator has not verified.
func newVersionTracker(ceiling string, autoSync bool) *versionTracker {
	if ceiling == "" {
		ceiling = httpx.V2ClientVersion()
	}
	return &versionTracker{
		state: versionState{
			Latest:   httpx.V2ClientVersion(),
			Active:   httpx.V2ClientVersion(),
			Ceiling:  ceiling,
			Synced:   true,
			AutoSync: autoSync,
		},
		ceiling:  ceiling,
		autoSync: autoSync,
		client:   &http.Client{Timeout: versionSyncTimeout},
	}
}

// StartVersionSync checks the feed at once and then every
// performance.version_sync.interval_seconds until ctx is canceled, mirroring
// StartModelRefresh's startup-then-tick shape.
func (g *Gateway) StartVersionSync(ctx context.Context) {
	interval := versionSyncInterval
	if g.cfg.Performance.VersionSync.IntervalSeconds > 0 {
		interval = time.Duration(g.cfg.Performance.VersionSync.IntervalSeconds) * time.Second
	}
	go func() {
		g.checkVersionFeed(ctx)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				g.checkVersionFeed(ctx)
			}
		}
	}()
}

// checkVersionFeed fetches the release feed once and applies the outcome.
// Failures only log: a stale claim is the state every pre-1.7.2 build ran
// with, and the next tick retries.
func (g *Gateway) checkVersionFeed(ctx context.Context) {
	if g.versionTracker == nil {
		return
	}
	fetchCtx, cancel := context.WithTimeout(ctx, versionSyncTimeout)
	defer cancel()
	version, err := fetchLatestVersion(fetchCtx, g.versionTracker.client, versionFeedURL)
	checked := time.Now()
	if err != nil {
		if ctx.Err() == nil {
			g.logger.Warn("client version feed check failed", "component", "models", "event", "version_check_failed", "error", err)
		}
		return
	}
	g.applyVersionState(version, checked, versionFeedURL)
}

// applyVersionState records the feed result and, when auto-sync is on, moves
// the claimed v2 version. The Gateway's logger and config carry the events;
// the tracker only guards its own state.
func (g *Gateway) applyVersionState(latest string, checked time.Time, source string) {
	tracker := g.versionTracker
	tracker.mu.Lock()
	previous := tracker.state.Latest
	tracker.state.Latest = latest
	tracker.state.CheckAt = &checked
	tracker.state.Source = source
	tracker.state.Active = httpx.V2ClientVersion()
	tracker.state.Ceiling = tracker.ceiling
	aboveCeiling := versionNewer(latest, tracker.ceiling)
	tracker.state.Pending = aboveCeiling
	tracker.state.Synced = !versionNewer(latest, tracker.state.Active)
	autoSync := tracker.autoSync
	switch {
	case !versionNewer(latest, tracker.state.Active):
		g.logger.Debug("client version feed is current", "component", "models", "event", "version_check_current", "active", tracker.state.Active, "latest", latest)
	case aboveCeiling:
		// Never claim a release the operator has not verified: the
		// compile-time pairing of this build's other fingerprint
		// constants ends at the ceiling.
		g.logger.Warn("newer client release is above the version-sync ceiling", "component", "models", "event", "version_update_above_ceiling", "latest", latest, "active", tracker.state.Active, "ceiling", tracker.ceiling,
			"hint", "verify the release passes the v2 gates, then raise performance.version_sync.ceiling or rebuild")
	case !autoSync:
		g.logger.Info("newer client release detected", "component", "models", "event", "version_update_detected", "latest", latest, "active", tracker.state.Active, "ceiling", tracker.ceiling)
	default:
		httpx.SetV2ClientVersion(latest)
		tracker.state.Active = latest
		tracker.state.Synced = true
		g.logger.Info("client masquerade version updated", "component", "models", "event", "version_synced", "from", previous, "to", latest, "ceiling", tracker.ceiling)
	}
	tracker.mu.Unlock()
}

// fetchLatestVersion returns the newest stable release tag from the feed.
// The repository mixes client releases ("v2.3.0") with component tags
// ("process-daemon-v0.5.2"), so anything that is not exactly
// "v<numeric semver>" (or the bare numeric form) is ignored, as are drafts
// and prereleases the official client does not distribute.
func fetchLatestVersion(ctx context.Context, client *http.Client, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "opencode2api-version-tracker")
	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("version feed returned %d", res.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return "", err
	}
	var entries []versionFeedEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		return "", fmt.Errorf("decode version feed: %w", err)
	}
	for _, entry := range entries {
		if entry.Draft || entry.Prerelease {
			continue
		}
		if version, ok := parseReleaseTag(entry.TagName); ok {
			return version, nil
		}
	}
	return "", errors.New("version feed contained no stable client release")
}

// parseReleaseTag accepts "v2.3.0" and "2.3.0" and rejects everything else,
// including loose prerelease suffixes: a claim upstream does not ship is a
// fingerprint divergence, not an upgrade.
func parseReleaseTag(tag string) (string, bool) {
	version := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(tag), "v"), "V")
	if version == "" || strings.ContainsAny(version, "-+") {
		return "", false
	}
	parts := strings.Split(version, ".")
	if len(parts) < 2 || len(parts) > 3 {
		return "", false
	}
	for _, part := range parts {
		if part == "" || len(part) > 9 {
			return "", false
		}
		for _, r := range part {
			if r < '0' || r > '9' {
				return "", false
			}
		}
	}
	return version, true
}

// versionNewer reports whether a is a strictly newer numeric release than b.
// Malformed operands never claim an upgrade.
func versionNewer(a, b string) bool {
	pa, oka := splitNumericVersion(a)
	pb, okb := splitNumericVersion(b)
	if !oka || !okb {
		return false
	}
	for i := 0; i < 3; i++ {
		if pa[i] != pb[i] {
			return pa[i] > pb[i]
		}
	}
	return false
}

// splitNumericVersion parses a bare numeric release into up to three
// components, right-padding with zeros ("2.3" == "2.3.0").
func splitNumericVersion(version string) ([3]int, bool) {
	var out [3]int
	if version == "" || strings.ContainsAny(version, "-+") {
		return out, false
	}
	parts := strings.Split(version, ".")
	if len(parts) > 3 {
		return out, false
	}
	for i, part := range parts {
		if part == "" || len(part) > 9 {
			return out, false
		}
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			return out, false
		}
		out[i] = n
	}
	return out, true
}

// VersionState snapshots the tracker for /healthz.
func (g *Gateway) VersionState() *versionState {
	if g.versionTracker == nil {
		return nil
	}
	g.versionTracker.mu.Lock()
	defer g.versionTracker.mu.Unlock()
	state := g.versionTracker.state
	state.Active = httpx.V2ClientVersion()
	return &state
}
