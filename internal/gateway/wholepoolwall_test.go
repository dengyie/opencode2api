package gateway

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"opencode2api/internal/config"
	models "opencode2api/internal/models"
)

// v1.8.2: the whole-pool unavailability judgment now counts credential-dead
// keys. Production (2026-11-18 00:14) held seven 429-walled keys plus one
// 401-dead key (quotaUntil=0, credUntil set): the old judgment kept reporting
// "a key is usable", so the entry pre-check and the mid-walk fast fail never
// fired and the anonymous lane walked 6–7 exits into the caller's 30s budget
// (499). These tests pin the pool predicate, the pre-check, and both walk
// caps against that exact shape.

func credQuarantinePool(n int, quarantine time.Duration) *upstreamNodes {
	p := testKeyPool(n, 10*time.Minute)
	p.credentialQuarantine = quarantine
	return p
}

// AllUnavailable: a cred-quarantined key counts as unavailable with its own
// horizon; the quota flag separates wall classes so an all-credential pool
// stands the quota verdict down (credentialWall owns the 503).
func TestAllUnavailableCountsCredentialQuarantine(t *testing.T) {
	pool := credQuarantinePool(2, time.Hour)
	quota, cred := pool.nodes[0], pool.nodes[1]
	pool.MarkFailure(quota, quotaResponse("3600"), nil)
	pool.MarkFailure(cred, credResponse(401, ""), nil)
	if cred.quotaUntil.Load() != 0 {
		t.Fatal("precondition: a 401 must not set quotaUntil")
	}
	until, all, quotaWalled := pool.AllUnavailable()
	if !all || !quotaWalled {
		t.Fatalf("quota-walled + cred-quarantined pool must report a whole-pool quota wall, got all=%v quota=%v", all, quotaWalled)
	}
	if got := time.Until(until); got < 55*time.Minute || got > time.Hour {
		t.Fatalf("the horizon must be the soonest reopening (the quota wall here), got %v", got)
	}
	// Lapse the quota wall and quarantine the second key too: an
	// all-credential pool must stand the quota verdict down (credentialWall
	// owns the 503 classification) and report the credential wall itself.
	quota.quotaUntil.Store(time.Now().Add(-time.Second).UnixNano())
	quota.cooldownUntil.Store(time.Now().Add(-time.Second).UnixNano())
	pool.MarkFailure(quota, credResponse(403, ""), nil)
	if _, all, quotaWalled := pool.AllUnavailable(); !all || quotaWalled {
		t.Fatalf("an all-credential pool must report all=true quota=false, got all=%v quota=%v", all, quotaWalled)
	}
	if _, walled := pool.AllCredentialQuarantined(); !walled {
		t.Fatal("an all-credential pool must report a credential wall")
	}
	// Re-wall the first key with 429: the quota verdict returns, with the
	// horizon at the soonest reopening across both unavailability classes.
	pool.MarkSuccess(quota)
	pool.MarkFailure(quota, quotaResponse("1800"), nil)
	var rewalled, requota bool
	until, rewalled, requota = pool.AllUnavailable()
	if !rewalled || !requota {
		t.Fatalf("quota-walled + cred-quarantined pool must report a whole-pool wall again, got all=%v quota=%v", rewalled, requota)
	}
	if time.Until(until) > 30*time.Minute {
		t.Fatalf("the horizon must track the fresh 30m quota wall, got %v", time.Until(until))
	}
}

// The 00:14:54 production shape end to end: the wall signal armed, every
// healthy key 429-walled, one key credential-dead. The entry pre-check must
// reject with zero upstream calls instead of walking the anonymous lane.
func TestE2EWholePoolWallWithCredentialDeadKey(t *testing.T) {
	var upstreamCalls atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"mimo-v2.6-flash-free"}]}`))
	})
	mux.HandleFunc("GET /capabilities", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"opencode":{"id":"opencode","api":"https://opencode.ai/zen","npm":"@ai-sdk/openai-compatible","models":{"mimo-v2.6-flash-free":{"id":"mimo-v2.6-flash-free"}}}}`))
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"Free usage limit reached","type":"upstream_error"}}`))
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	doc := `{
		"listen": "127.0.0.1:0",
		"server_keys": ["sk-local"],
		"zen_keys": ["sk-zen-1", "sk-zen-2"],
		"anonymous": true,
		"proxies": ["direct"],
		"upstream": {"zen": "` + server.URL + `", "go": "` + server.URL + `", "anonymous": "` + server.URL + `"},
		"retry": {"max_attempts": 6, "timeout_seconds": 30},
		"models": {"refresh_seconds": 300},
		"performance": {"heartbeat_seconds": 0, "quota_probe_seconds": 0}
	}`
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	cfg.Models.RefreshSeconds = 1
	origCapabilitiesURL := models.CapabilitiesURL
	models.CapabilitiesURL = server.URL + "/capabilities"
	t.Cleanup(func() { models.CapabilitiesURL = origCapabilitiesURL })
	g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g.StartModelRefresh(ctx)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if len(g.catalog.List()) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(g.catalog.List()) == 0 {
		t.Fatal("catalog never populated")
	}

	// The production state: one key 429-walled, one key 401-dead, anonymous
	// lane empirically walled (two observations within the window).
	g.zenNodes.MarkFailure(g.zenNodes.nodes[0], quotaResponse("3600"), nil)
	g.zenNodes.MarkFailure(g.zenNodes.nodes[1], credResponse(401, ""), nil)
	g.observeAnonymousWall(http.StatusTooManyRequests)
	g.observeAnonymousWall(http.StatusTooManyRequests)

	apiServer := httptest.NewServer(g.Handler())
	defer apiServer.Close()
	req, _ := http.NewRequest(http.MethodPost, apiServer.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"mimo-v2.6-flash-free","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("x-api-key", "sk-local")
	resp, err := apiServer.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusTooManyRequests || !strings.Contains(string(raw), "insufficient_quota") {
		t.Fatalf("the whole-pool wall must pre-check a 429, got %d body=%q", resp.StatusCode, raw)
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Fatalf("the pre-check must reject with zero upstream calls, got %d", got)
	}
}

// The 01:16:13 production shape end to end: the wall armed at entry, the
// probe-recovered keys hold a probation cooldown (quotaUntil already clear),
// so no key is ready by the cursor's definition yet they can still serve via
// the earliest-cooling fallback. The anonymous walk must spend exactly one
// probe attempt and hand the request to the probation key instead of burning
// every exit.
func TestE2EWallProbeWithProbationKeys(t *testing.T) {
	var anonCalls, keyCalls atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"mimo-v2.6-flash-free"}]}`))
	})
	mux.HandleFunc("GET /capabilities", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"opencode":{"id":"opencode","api":"https://opencode.ai/zen","npm":"@ai-sdk/openai-compatible","models":{"mimo-v2.6-flash-free":{"id":"mimo-v2.6-flash-free"}}}}`))
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("Authorization"), "public") {
			anonCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "3600")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"message":"Free usage limit reached","type":"upstream_error"}}`))
			return
		}
		keyCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"OK\"},\"finish_reason\":null}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	doc := `{
		"listen": "127.0.0.1:0",
		"server_keys": ["sk-local"],
		"zen_keys": ["sk-zen"],
		"anonymous": true,
		"proxies": ["direct"],
		"upstream": {"zen": "` + server.URL + `", "go": "` + server.URL + `", "anonymous": "` + server.URL + `"},
		"retry": {"max_attempts": 6, "timeout_seconds": 30},
		"models": {"refresh_seconds": 300},
		"performance": {"heartbeat_seconds": 0, "quota_probe_seconds": 0}
	}`
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	cfg.Models.RefreshSeconds = 1
	origCapabilitiesURL := models.CapabilitiesURL
	models.CapabilitiesURL = server.URL + "/capabilities"
	t.Cleanup(func() { models.CapabilitiesURL = origCapabilitiesURL })
	g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g.StartModelRefresh(ctx)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if len(g.catalog.List()) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(g.catalog.List()) == 0 {
		t.Fatal("catalog never populated")
	}
	// Two anonymous exits, so an uncapped walk would take two 429 legs.
	transports2, err := transportPool([]string{"direct", "direct"}, cfg.Performance, 0)
	if err != nil {
		t.Fatal(err)
	}
	g.anonymous = anonymousPool(true, transports2, 15*time.Second, anonymousBackoff{})

	// The production state: the key just recovered from a probe and holds the
	// v1.8.1 probation cooldown (quotaUntil clear), the anonymous wall armed.
	node := g.zenNodes.nodes[0]
	g.zenNodes.MarkFailure(node, quotaResponse("3600"), nil)
	g.zenNodes.MarkSuccess(node)
	g.zenNodes.MarkProbation(node, time.Minute)
	if node.quotaUntil.Load() != 0 {
		t.Fatal("precondition: MarkSuccess must clear the quota wall")
	}
	g.observeAnonymousWall(http.StatusTooManyRequests)
	g.observeAnonymousWall(http.StatusTooManyRequests)

	apiServer := httptest.NewServer(g.Handler())
	defer apiServer.Close()
	req, _ := http.NewRequest(http.MethodPost, apiServer.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"mimo-v2.6-flash-free","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("x-api-key", "sk-local")
	resp, err := apiServer.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), "OK") {
		t.Fatalf("the probation key must serve the request, got %d body=%q", resp.StatusCode, raw)
	}
	if got := anonCalls.Load(); got != 1 {
		t.Fatalf("the walled walk must spend exactly one probe attempt, got %d", got)
	}
	if got := keyCalls.Load(); got != 1 {
		t.Fatalf("the probation key must serve the request, got %d key calls", got)
	}
}

// The cross-pool blind spot (v1.8.3): the zen pool fully quota-walled while
// the go pool is fully credential-quarantined. The old per-pool boolean kept
// reporting "a key is usable" (the go pool has no quota wall), so the entry
// pre-check never fired; the tri-state verdict must pre-check a 429 with
// zero upstream calls.
func TestE2ECrossPoolMixedClassWall(t *testing.T) {
	var upstreamCalls atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"mimo-v2.6-flash-free"}]}`))
	})
	mux.HandleFunc("GET /capabilities", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"opencode":{"id":"opencode","api":"https://opencode.ai/zen","npm":"@ai-sdk/openai-compatible","models":{"mimo-v2.6-flash-free":{"id":"mimo-v2.6-flash-free"}}},` +
			`"opencode-go":{"id":"opencode-go","api":"https://opencode.ai/go","npm":"@ai-sdk/openai-compatible","models":{"mimo-v2.6-flash-free":{"id":"mimo-v2.6-flash-free"}}}}`))
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"Free usage limit reached","type":"upstream_error"}}`))
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	doc := `{
		"listen": "127.0.0.1:0",
		"server_keys": ["sk-local"],
		"zen_keys": ["sk-zen"],
		"go_keys": ["sk-go"],
		"anonymous": true,
		"proxies": ["direct"],
		"upstream": {"zen": "` + server.URL + `", "go": "` + server.URL + `", "anonymous": "` + server.URL + `"},
		"retry": {"max_attempts": 6, "timeout_seconds": 30},
		"models": {"refresh_seconds": 300},
		"performance": {"heartbeat_seconds": 0, "quota_probe_seconds": 0}
	}`
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	cfg.Models.RefreshSeconds = 1
	origCapabilitiesURL := models.CapabilitiesURL
	models.CapabilitiesURL = server.URL + "/capabilities"
	t.Cleanup(func() { models.CapabilitiesURL = origCapabilitiesURL })
	g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g.StartModelRefresh(ctx)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if len(g.catalog.List()) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(g.catalog.List()) == 0 {
		t.Fatal("catalog never populated")
	}

	// The cross-pool shape: zen quota-walled, go credential-dead, anonymous
	// lane empirically walled.
	g.zenNodes.MarkFailure(g.zenNodes.nodes[0], quotaResponse("3600"), nil)
	g.goNodes.MarkFailure(g.goNodes.nodes[0], credResponse(401, ""), nil)
	g.observeAnonymousWall(http.StatusTooManyRequests)
	g.observeAnonymousWall(http.StatusTooManyRequests)

	apiServer := httptest.NewServer(g.Handler())
	defer apiServer.Close()
	req, _ := http.NewRequest(http.MethodPost, apiServer.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"mimo-v2.6-flash-free","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("x-api-key", "sk-local")
	resp, err := apiServer.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusTooManyRequests || !strings.Contains(string(raw), "insufficient_quota") {
		t.Fatalf("the cross-pool wall must pre-check a 429, got %d body=%q", resp.StatusCode, raw)
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Fatalf("the pre-check must reject with zero upstream calls, got %d", got)
	}
}

// The classification guard end to end: every key credential-quarantined and
// the anonymous lane walled must yield the credential pre-check's 503
// key_pool_credential_exhausted — never the quota pre-check's 429.
func TestE2EAllCredentialPoolClassifies503(t *testing.T) {
	var upstreamCalls atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"mimo-v2.6-flash-free"}]}`))
	})
	mux.HandleFunc("GET /capabilities", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"opencode":{"id":"opencode","api":"https://opencode.ai/zen","npm":"@ai-sdk/openai-compatible","models":{"mimo-v2.6-flash-free":{"id":"mimo-v2.6-flash-free"}}}}`))
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"message":"Invalid credential","type":"authentication_error"}}`))
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	doc := `{
		"listen": "127.0.0.1:0",
		"server_keys": ["sk-local"],
		"zen_keys": ["sk-zen-1", "sk-zen-2"],
		"anonymous": true,
		"proxies": ["direct"],
		"upstream": {"zen": "` + server.URL + `", "go": "` + server.URL + `", "anonymous": "` + server.URL + `"},
		"retry": {"max_attempts": 6, "timeout_seconds": 30},
		"models": {"refresh_seconds": 300},
		"performance": {"heartbeat_seconds": 0, "quota_probe_seconds": 0}
	}`
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	cfg.Models.RefreshSeconds = 1
	origCapabilitiesURL := models.CapabilitiesURL
	models.CapabilitiesURL = server.URL + "/capabilities"
	t.Cleanup(func() { models.CapabilitiesURL = origCapabilitiesURL })
	g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g.StartModelRefresh(ctx)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if len(g.catalog.List()) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(g.catalog.List()) == 0 {
		t.Fatal("catalog never populated")
	}

	// Both keys credential-quarantined, anonymous lane empirically walled.
	g.zenNodes.MarkFailure(g.zenNodes.nodes[0], credResponse(401, ""), nil)
	g.zenNodes.MarkFailure(g.zenNodes.nodes[1], credResponse(403, ""), nil)
	g.observeAnonymousWall(http.StatusTooManyRequests)
	g.observeAnonymousWall(http.StatusTooManyRequests)

	apiServer := httptest.NewServer(g.Handler())
	defer apiServer.Close()
	req, _ := http.NewRequest(http.MethodPost, apiServer.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"mimo-v2.6-flash-free","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("x-api-key", "sk-local")
	resp, err := apiServer.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(raw), "key_pool_credential_exhausted") {
		t.Fatalf("the all-credential pool must pre-check a 503, got %d body=%q", resp.StatusCode, raw)
	}
	if strings.Contains(string(raw), "insufficient_quota") {
		t.Fatalf("the all-credential pool must not read as a quota wall: %q", raw)
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Fatalf("the pre-check must reject with zero upstream calls, got %d", got)
	}
}

// Fix C on a route without key tiers (anonymous-only deployment): the entry
// cap does not apply (nothing to hand over to), but the mid-walk arm check
// still stops the walk once this request's own 429s arm the wall — one leg.
func TestE2EAnonOnlyMidWalkArm(t *testing.T) {
	var anonCalls atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"mimo-v2.6-flash-free"}]}`))
	})
	mux.HandleFunc("GET /capabilities", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"opencode":{"id":"opencode","api":"https://opencode.ai/zen","npm":"@ai-sdk/openai-compatible","models":{"mimo-v2.6-flash-free":{"id":"mimo-v2.6-flash-free"}}}}`))
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, _ *http.Request) {
		anonCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"Free usage limit reached","type":"upstream_error"}}`))
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	doc := `{
		"listen": "127.0.0.1:0",
		"server_keys": ["sk-local"],
		"zen_keys": [],
		"anonymous": true,
		"proxies": ["direct"],
		"upstream": {"zen": "` + server.URL + `", "go": "` + server.URL + `", "anonymous": "` + server.URL + `"},
		"retry": {"max_attempts": 6, "timeout_seconds": 30},
		"models": {"refresh_seconds": 300},
		"performance": {"heartbeat_seconds": 0, "quota_probe_seconds": 0}
	}`
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	cfg.Models.RefreshSeconds = 1
	origCapabilitiesURL := models.CapabilitiesURL
	models.CapabilitiesURL = server.URL + "/capabilities"
	t.Cleanup(func() { models.CapabilitiesURL = origCapabilitiesURL })
	g, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g.StartModelRefresh(ctx)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if len(g.catalog.List()) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(g.catalog.List()) == 0 {
		t.Fatal("catalog never populated")
	}
	// Two anonymous exits so an uncapped walk would take two 429 legs.
	transports2, err := transportPool([]string{"direct", "direct"}, cfg.Performance, 0)
	if err != nil {
		t.Fatal(err)
	}
	g.anonymous = anonymousPool(true, transports2, 15*time.Second, anonymousBackoff{})
	// The wall armed before the request: the mid-walk check must stop the
	// walk after the first exit's 429.
	g.observeAnonymousWall(http.StatusTooManyRequests)
	g.observeAnonymousWall(http.StatusTooManyRequests)

	apiServer := httptest.NewServer(g.Handler())
	defer apiServer.Close()
	req, _ := http.NewRequest(http.MethodPost, apiServer.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"mimo-v2.6-flash-free","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("x-api-key", "sk-local")
	resp, err := apiServer.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("the anonymous-only walk must forward the upstream 429, got %d body=%q", resp.StatusCode, raw)
	}
	if strings.Contains(string(raw), "insufficient_quota") {
		t.Fatalf("without keys there is no wall synthesis; the upstream body must be forwarded: %q", raw)
	}
	if got := anonCalls.Load(); got != 1 {
		t.Fatalf("the mid-walk arm must stop the walk after one exit, got %d anon calls", got)
	}
}
