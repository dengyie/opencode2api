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
	"testing"
	"time"

	"opencode2api/internal/config"
)

// The E2E in this file drives the REAL gateway Handler against a fake v2
// upstream: model discovery fills the catalog, a streamed chat request goes
// through pool routing, and the v1.5.0 stream supervision (keepalive frames
// during upstream silence, idle watchdog aborting a wedged stream) and the
// quota pre-check are observed on the client side of the wire.

// fakeUpstream serves the two v2 shapes the gateway touches: GET /v1/models
// for discovery and POST /v1/chat/completions for inference. The behavior
// mode arrives in the model name: "test-free" streams one delta and wedges;
// "test-quota" answers 429 with the free-usage-limit wording.
func fakeUpstream(t *testing.T, wedged chan struct{}) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"test-free"},{"id":"test-quota"}]}`))
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 4096)
		n, _ := r.Body.Read(body)
		if strings.Contains(string(body[:n]), `"test-quota"`) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "3600")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"message":"Free usage limit reached","type":"upstream_error"}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n"))
		flusher.Flush()
		// Wedge: hold the stream open without another byte until the gateway
		// cancels the request (the idle watchdog) or the test ends.
		<-r.Context().Done()
		if wedged != nil {
			close(wedged)
		}
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func gatewayFor(t *testing.T, upstream, retryJSON, performanceJSON string) *Gateway {
	t.Helper()
	doc := `{
		"listen": "127.0.0.1:0",
		"server_keys": ["sk-local"],
		"zen_keys": ["sk-zen"],
		"anonymous": false,
		"proxies": ["direct"],
		"upstream": {"zen": "` + upstream + `", "go": "` + upstream + `"},
		"retry": ` + retryJSON + `,
		"models": {"refresh_seconds": 300},
		"performance": ` + performanceJSON + `
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
	g, err := New(cfg, slog.New(slog.NewTextHandler(os.Stderr, nil)), nil, nil)
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g.StartModelRefresh(ctx)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if g.catalog.Supported("test-free") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !g.catalog.Supported("test-free") {
		t.Fatal("catalog never learned the fake upstream model")
	}
	return g
}

// chatStream POSTs one streaming chat completion and returns the raw SSE body
// once the connection ends.
func chatStream(t *testing.T, handler http.Handler, model string) string {
	t.Helper()
	server := httptest.NewServer(handler)
	defer server.Close()
	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"`+model+`","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("x-api-key", "sk-local")
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(raw)
}

// A wedged upstream: the client sees the first delta, then keepalive frames
// during the silence, then the idle watchdog error and a clean end; and the
// wedged upstream request is actually canceled.
func TestE2EKeepaliveAndIdleWatchdog(t *testing.T) {
	wedged := make(chan struct{})
	upstream := fakeUpstream(t, wedged)
	g := gatewayFor(t, upstream.URL, `{"max_attempts": 1, "timeout_seconds": 30}`, `{"heartbeat_seconds": 1, "idle_timeout_seconds": 2}`)
	body := chatStream(t, g.Handler(), "test-free")
	if !strings.Contains(body, `"content":"hello"`) {
		t.Fatalf("first delta missing: %q", body)
	}
	if !strings.Contains(body, ": keepalive") {
		t.Fatalf("keepalive comment frame missing: %q", body)
	}
	if !strings.Contains(body, "upstream_idle_timeout") {
		t.Fatalf("idle watchdog error missing: %q", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("stream must end with [DONE] after the idle error: %q", body)
	}
	select {
	case <-wedged:
	case <-time.After(3 * time.Second):
		t.Fatal("the gateway never canceled the wedged upstream request")
	}
}

// With supervision disabled no keepalive frames appear; the wedge is cut by
// the request budget alone (short budgeted here), reproducing pre-v1.5.
func TestE2ESupervisionDisabled(t *testing.T) {
	upstream := fakeUpstream(t, nil)
	g := gatewayFor(t, upstream.URL, `{"max_attempts": 1, "timeout_seconds": 2}`, `{"heartbeat_seconds": 0, "idle_timeout_seconds": 0}`)
	done := make(chan string, 1)
	go func() { done <- chatStream(t, g.Handler(), "test-free") }()
	select {
	case body := <-done:
		if strings.Contains(body, ": keepalive") {
			t.Fatalf("disabled supervision must not emit keepalive frames: %q", body)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the stream outlived its request budget")
	}
}

// A fully quota-cooled key lane with the anonymous lane disabled rejects the
// request before any upstream call (zero-attempt pre-check).
func TestE2EQuotaPrecheck(t *testing.T) {
	upstream := fakeUpstream(t, nil)
	g := gatewayFor(t, upstream.URL, `{"max_attempts": 1, "timeout_seconds": 30}`, `{"heartbeat_seconds": 0}`)
	for _, node := range g.zenNodes.nodes {
		g.zenNodes.MarkFailure(node, quotaResponse("3600"), nil)
	}
	server := httptest.NewServer(g.Handler())
	defer server.Close()
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"test-quota","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("x-api-key", "sk-local")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got == "" || got == "0" {
		t.Fatalf("Retry-After missing: %q", got)
	}
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "insufficient_quota") || !strings.Contains(string(raw), "quota-cooled") {
		t.Fatalf("error body must carry the quota semantics: %q", string(raw))
	}
}
