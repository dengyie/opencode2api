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

// E2E quota-wall fast fail (v1.8.1): the request starts while the entry
// pre-check passes (a key is still usable), but the fake upstream 429s every
// key attempt with a long Retry-After. Instead of rotating through
// MaxAttempts keys and burning the caller's budget, the walk must stop after
// the wall is confirmed and the handler must synthesize the pre-check's
// 429+Retry-After with the OpenAI-canonical insufficient_quota type.
func TestE2EQuotaWallFastFail(t *testing.T) {
	var keyCalls atomic.Int64
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
		keyCalls.Add(1)
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
		"zen_keys": ["sk-zen-1", "sk-zen-2", "sk-zen-3"],
		"anonymous": false,
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
	g, err := New(cfg, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})), nil, nil)
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
		t.Fatalf("the fast fail must synthesize a 429, got %d body=%q", resp.StatusCode, raw)
	}
	if ra := resp.Header.Get("Retry-After"); ra == "" || ra == "0" {
		t.Fatalf("Retry-After must be present and positive, got %q", ra)
	}
	if !strings.Contains(string(raw), "insufficient_quota") {
		t.Fatalf("the error type must be the OpenAI-canonical insufficient_quota, got %q", raw)
	}
	// The walk must stop at the wall confirmation, not exhaust 6 attempts
	// across 3 keys (18 legs). The first 429 walls the key; the re-check sees
	// every key walled only once all keys took a 429 — 3 calls, then stop.
	if got := keyCalls.Load(); got > 4 {
		t.Fatalf("the walk must cut short well before MaxAttempts, got %d upstream calls", got)
	}
	// Every walled key must carry the adopted Retry-After horizon.
	if _, all, _ := g.zenNodes.AllUnavailable(); !all {
		t.Fatal("all keys must be unavailable after the walk")
	}
}

// A single-key pool that gets walled mid-walk also fast-fails: quotaWall is
// true as soon as the only key is walled, so the second attempt never fires.
func TestE2EQuotaWallFastFailSingleKey(t *testing.T) {
	var keyCalls atomic.Int64
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
		keyCalls.Add(1)
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"Free usage limit reached","type":"upstream_error"}}`))
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	doc := `{
		"listen": "127.0.0.1:0",
		"server_keys": ["sk-local"],
		"zen_keys": ["sk-zen-1"],
		"anonymous": false,
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
	g, err := New(cfg, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})), nil, nil)
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
		t.Fatalf("the single walled key must fast-fail with the pre-check 429, got %d body=%q", resp.StatusCode, raw)
	}
	// One attempt walls the only key, the re-check confirms the wall, done:
	// no second leg on the same walled key.
	if got := keyCalls.Load(); got != 1 {
		t.Fatalf("the walk must stop after the first 429 confirms the wall, got %d calls", got)
	}
}
