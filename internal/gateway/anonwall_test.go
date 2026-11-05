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
	"opencode2api/internal/models"
)

// The v1.5.6 empirical-wall fast failover: while the anonymous lane answers
// 429 and a Zen key can serve, the anonymous exit walk is capped at one
// attempt so the key fallback gets its turn inside the caller's first-token
// budget. These tests cover the decision helper, the pool predicate it reads,
// and the end-to-end walk cap against a fake upstream.

func TestHasUsableNode(t *testing.T) {
	transports, err := transportPool([]string{"direct", "direct"}, config.PerformanceConfig{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := nodePool([]string{"k1", "k2"}, transports, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !nodes.HasUsableNode() {
		t.Fatal("a fresh pool must report a usable node")
	}
	nodes.MarkFailure(nodes.nodes[0], quotaResponse("3600"), nil)
	if !nodes.HasUsableNode() {
		t.Fatal("one cooled key must not unusable the whole pool")
	}
	nodes.MarkFailure(nodes.nodes[1], quotaResponse("3600"), nil)
	if nodes.HasUsableNode() {
		t.Fatal("every key cooled: the pool must report no usable node")
	}
	var empty *upstreamNodes
	if empty.HasUsableNode() {
		t.Fatal("a nil pool must report no usable node")
	}
}

func TestAnonWallFastFailover(t *testing.T) {
	upstream := fakeUpstream(t, nil)
	g := gatewayFor(t, upstream.URL, `{"max_attempts": 1, "timeout_seconds": 30}`, `{"heartbeat_seconds": 0}`)
	if g.anonWallFastFailover() {
		t.Fatal("an unarmed wall must not trigger the fast failover")
	}
	g.anonWallCount.Store(2)
	g.anonWallLastAt.Store(time.Now().UnixNano())
	if !g.anonWallFastFailover() {
		t.Fatal("an armed wall with a usable key must trigger the fast failover")
	}
	g.zenNodes.MarkFailure(g.zenNodes.nodes[0], quotaResponse("3600"), nil)
	if g.anonWallFastFailover() {
		t.Fatal("no usable key: the walk must keep its full cap")
	}
}

// End-to-end walk cap: the config's proxy list is deduplicated, so the test
// rebuilds the anonymous pool with three direct transports by hand. The first
// request (wall unarmed) walks all three exits; the second — wall armed, key
// usable — must spend exactly one anon attempt and then be served by the key.
func TestE2EAnonWallFastFailover(t *testing.T) {
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
		"retry": {"max_attempts": 4, "timeout_seconds": 30},
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
	g, err := New(cfg, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})), nil, nil)
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
	// Three distinct anonymous exits (config dedup collapses duplicates, so
	// this pool is built by hand over fresh transports). All direct: every
	// attempt reaches the fake upstream and its 429.
	transports3, err := transportPool([]string{"direct", "direct", "direct"}, cfg.Performance, 0)
	if err != nil {
		t.Fatal(err)
	}
	g.anonymous = anonymousPool(true, transports3, 15*time.Second, anonymousBackoff{})

	apiServer := httptest.NewServer(g.Handler())
	defer apiServer.Close()
	post := func() (int, string) {
		req, _ := http.NewRequest(http.MethodPost, apiServer.URL+"/v1/chat/completions",
			strings.NewReader(`{"model":"mimo-v2.6-flash-free","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
		req.Header.Set("x-api-key", "sk-local")
		resp, err := apiServer.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(raw)
	}

	// First request: the wall is not armed yet, so all three exits are walked
	// and the key serves. The 429s arm the signal.
	status, body := post()
	if status != http.StatusOK || !strings.Contains(body, "OK") {
		t.Fatalf("first request must be served by the key: status=%d body=%q", status, body)
	}
	if got := anonCalls.Load(); got != 3 {
		t.Fatalf("uncapped walk must use all three exits, got %d anon calls", got)
	}
	if got := keyCalls.Load(); got != 1 {
		t.Fatalf("the key must serve the first request, got %d key calls", got)
	}
	// Second request: wall armed + key usable → the walk is capped at one
	// anon attempt and the key serves again.
	status, body = post()
	if status != http.StatusOK || !strings.Contains(body, "OK") {
		t.Fatalf("second request must be served by the key: status=%d body=%q", status, body)
	}
	if got := anonCalls.Load(); got != 4 {
		t.Fatalf("the capped walk must use exactly one more anon attempt (3 -> 4), got %d", got)
	}
	if got := keyCalls.Load(); got != 2 {
		t.Fatalf("the key must serve the capped request, got %d key calls", got)
	}
}
