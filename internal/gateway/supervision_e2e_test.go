package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"opencode2api/internal/models"

	"opencode2api/internal/config"
)

// The E2E in this file drives the REAL gateway Handler against a fake v2
// upstream: model discovery fills the catalog, a streamed chat request goes
// through pool routing, and the v1.5.0 stream supervision (keepalive frames
// during upstream silence, idle watchdog aborting a wedged stream) and the
// quota pre-check are observed on the client side of the wire.

// fakeUpstream serves the v2 shapes the gateway touches: GET /v1/models for
// discovery, GET /capabilities for the protocol catalog (hermetic — without it
// the gateway would fetch the real models.opencode.ai catalog, whose free
// roster rotates), and POST /v1/chat/completions for inference. The behavior
// mode arrives in the model name: "big-pickle" streams one delta and wedges;
// "deepseek-v4.1-flash-free" is the probe's properly terminated stream;
// "mimo-v2.6-flash-free" answers 429 with the free-usage-limit wording.
func fakeUpstream(t *testing.T, wedged chan struct{}) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"deepseek-v4.1-flash-free"},{"id":"big-pickle"},{"id":"mimo-v2.6-flash-free"}]}`))
	})
	mux.HandleFunc("GET /capabilities", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"opencode":{"id":"opencode","api":"https://opencode.ai/zen","npm":"@ai-sdk/openai-compatible","models":{` +
			`"big-pickle":{"id":"big-pickle"},` +
			`"deepseek-v4.1-flash-free":{"id":"deepseek-v4.1-flash-free"},` +
			`"mimo-v2.6-flash-free":{"id":"mimo-v2.6-flash-free"}}}}`))
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 4096)
		n, _ := r.Body.Read(body)
		payload := string(body[:n])
		if strings.Contains(payload, `"mimo-v2.6-flash-free"`) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "3600")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"message":"Free usage limit reached","type":"upstream_error"}}`))
			return
		}
		if strings.Contains(payload, `"deepseek-v4.1-flash-free"`) {
			// The quota probe's model: a complete, properly terminated stream.
			w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			flusher := w.(http.Flusher)
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n"))
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"OK\"},\"finish_reason\":null}]}\n\n"))
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"))
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			flusher.Flush()
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
	// Point the capability catalog at the fake before the refresh loop starts,
	// so the E2E never touches the real models.opencode.ai.
	origCapabilitiesURL := models.CapabilitiesURL
	models.CapabilitiesURL = upstream + "/capabilities"
	t.Cleanup(func() { models.CapabilitiesURL = origCapabilitiesURL })
	g, err := New(cfg, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})), nil, nil)
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g.StartModelRefresh(ctx)
	// Wait for the catalog to actually POPULATE (a permissive empty catalog
	// supports everything, which would make the wait vacuous).
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
	body := chatStream(t, g.Handler(), "big-pickle")
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
	go func() { done <- chatStream(t, g.Handler(), "big-pickle") }()
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
		strings.NewReader(`{"model":"mimo-v2.6-flash-free","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
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

// The quota probe loop end-to-end: a key marked quota-cooled by a 429 with a
// long Retry-After is probed through its own key; the upstream answering 200
// clears the cooldown immediately, so the key routes again.
func TestE2EQuotaProbeRecoversKey(t *testing.T) {
	upstream := fakeUpstream(t, nil)
	g := gatewayFor(t, upstream.URL, `{"max_attempts": 1, "timeout_seconds": 30}`, `{"quota_probe_seconds": 1}`)
	node := g.zenNodes.nodes[0]
	g.zenNodes.MarkFailure(node, quotaResponse("3600"), nil)
	if node.quotaUntil.Load() == 0 {
		t.Fatal("precondition: key must be quota-cooled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	g.probeQuotaKey(ctx, g.zenNodes, node)
	if node.cooldownUntil.Load() != 0 || node.quotaUntil.Load() != 0 || node.probeNextAt.Load() != 0 {
		t.Fatalf("a successful probe must clear all cooldown state, got cooldown=%d quota=%d probeNext=%d",
			node.cooldownUntil.Load(), node.quotaUntil.Load(), node.probeNextAt.Load())
	}
	// And the key is usable again: a live non-stream request through it gets
	// the upstream's 200 instead of the wall.
	body := chatStream(t, g.Handler(), "deepseek-v4.1-flash-free")
	if !strings.Contains(body, `"content":"OK"`) {
		t.Fatalf("recovered key must route live traffic, got %q", body)
	}
}

// A probe against a still-walled upstream reschedules without clearing.
func TestE2EQuotaProbeStillWalled(t *testing.T) {
	// Every model this fake lists answers 429 with a long Retry-After, so the
	// probe picks the walled model and must reschedule instead of clearing.
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
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"Free usage limit reached","type":"upstream_error"}}`))
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	g := gatewayFor(t, server.URL, `{"max_attempts": 1, "timeout_seconds": 30}`, `{"quota_probe_seconds": 1}`)
	node := g.zenNodes.nodes[0]
	g.zenNodes.MarkFailure(node, quotaResponse("3600"), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	g.probeQuotaKey(ctx, g.zenNodes, node)
	if node.cooldownUntil.Load() == 0 || node.quotaUntil.Load() == 0 {
		t.Fatal("a still-walled probe must leave cooldown state intact")
	}
	if time.Until(time.Unix(0, node.probeNextAt.Load())) <= 0 {
		t.Fatal("a still-walled probe must reschedule the next probe")
	}
}

// End-to-end wall behavior with the tebi traffic shape: the anonymous lane is
// the route's primary lane (route.Anonymous), everything upstream is 429-walled,
// and the anonymous breaker is disabled (no anonymous_backoff). The first two
// requests walk the lanes and arm the empirical wall signal; from the third the
// quota pre-check answers with zero upstream calls.
func TestE2EQuotaPrecheckAnonymousRoute(t *testing.T) {
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
		"zen_keys": ["sk-zen"],
		"anonymous": true,
		"proxies": ["direct"],
		"upstream": {"zen": "` + server.URL + `", "go": "` + server.URL + `", "anonymous": "` + server.URL + `"},
		"retry": {"max_attempts": 1, "timeout_seconds": 30},
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

	// First two walks: anonymous lane and key lane both 429, arming the signal.
	for i := 0; i < 2; i++ {
		status, body := post()
		if status != http.StatusTooManyRequests {
			t.Fatalf("walk request %d: status = %d, want 429", i+1, status)
		}
		if strings.Contains(body, "quota-cooled") {
			t.Fatalf("walk request %d must reach the upstream, got the pre-check: %q", i+1, body)
		}
	}
	walkCalls := upstreamCalls.Load()
	if walkCalls == 0 {
		t.Fatal("the walks must reach the upstream")
	}
	// Third request: pre-checked with zero upstream calls.
	status, body := post()
	if status != http.StatusTooManyRequests {
		t.Fatalf("pre-checked status = %d, want 429", status)
	}
	if !strings.Contains(body, "insufficient_quota") || !strings.Contains(body, "quota-cooled") {
		t.Fatalf("pre-check body missing: %q", body)
	}
	if got := upstreamCalls.Load(); got != walkCalls {
		t.Fatalf("the pre-checked request must not touch the upstream: calls %d -> %d", walkCalls, got)
	}
}

// credentialFakeUpstream serves the discovery shapes and one inference
// endpoint that answers 401 "Invalid credential" — the exact wording a dead
// zen key produces — while counting every inference call. When healthy is
// true the endpoint instead answers 200 with a complete stream, so a test can
// flip the upstream and watch the probe recover the quarantined key.
func credentialFakeUpstream(t *testing.T, healthy *atomic.Bool, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"deepseek-v4.1-flash-free"}]}`))
	})
	mux.HandleFunc("GET /capabilities", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"opencode":{"id":"opencode","api":"https://opencode.ai/zen","npm":"@ai-sdk/openai-compatible","models":{"deepseek-v4.1-flash-free":{"id":"deepseek-v4.1-flash-free"}}}}`))
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if !healthy.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"message":"Invalid credential","type":"authentication_error"}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"OK\"},\"finish_reason\":null}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
		_ = r
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

// postChat is chatStream for non-stream requests, returning status and body.
func postChat(t *testing.T, handler http.Handler, model string) (int, string) {
	t.Helper()
	server := httptest.NewServer(handler)
	defer server.Close()
	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"`+model+`","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("x-api-key", "sk-local")
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(raw)
}

// The v1.7.0 credential closed loop end-to-end: an upstream that 401s the key
// (the my8Lw production shape) must NOT hand the 401 to the caller — New-API
// auto-bans a channel on a forwarded 401. Instead the gateway quarantines the
// key, synthesizes a 503 with a Retry-After, reports the quarantine in
// healthz, pre-checks later requests with zero upstream calls, and the probe
// loop revives the key once the upstream starts answering 200 again.
func TestE2ECredentialQuarantineLoop(t *testing.T) {
	var healthy atomic.Bool
	var calls atomic.Int64
	upstream := credentialFakeUpstream(t, &healthy, &calls)
	g := gatewayFor(t, upstream.URL, `{"max_attempts": 3, "timeout_seconds": 30}`, `{"heartbeat_seconds": 0, "quota_probe_seconds": 1, "credential_quarantine_seconds": 7200}`)

	// Walk: the only key gets 401'd across all attempts; the caller sees 503
	// with the quarantine semantics, never the upstream 401.
	status, body := postChat(t, g.Handler(), "deepseek-v4.1-flash-free")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (401 must not pass through), body: %q", status, body)
	}
	if strings.Contains(body, "Invalid credential") || status == http.StatusUnauthorized {
		t.Fatalf("the upstream 401 leaked through: %q", body)
	}
	if !strings.Contains(body, "key_pool_credential_exhausted") {
		t.Fatalf("error body must carry the credential-exhausted code: %q", body)
	}
	if calls.Load() == 0 {
		t.Fatal("the walk must reach the upstream")
	}
	node := g.zenNodes.nodes[0]
	if node.credUntil.Load() == 0 {
		t.Fatal("the 401 must quarantine the key")
	}
	retryAfter := respRetryAfter(t, g, "deepseek-v4.1-flash-free")
	if retryAfter <= 0 {
		t.Fatalf("Retry-After must name the quarantine horizon, got %d", retryAfter)
	}

	// healthz reports the quarantined key (informational, not a readiness block).
	_, healthBody := getHealth(t, g)
	keys, _ := healthBody["keys"].(map[string]any)
	if got := keys["credential_quarantined"]; got != float64(1) {
		t.Fatalf("credential_quarantined = %v, want 1", got)
	}

	// Pre-check: a second request while the wall holds answers with zero
	// further upstream calls.
	before := calls.Load()
	status, body = postChat(t, g.Handler(), "deepseek-v4.1-flash-free")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("pre-checked status = %d, want 503, body: %q", status, body)
	}
	if got := calls.Load(); got != before {
		t.Fatalf("the pre-checked request must not touch the upstream: calls %d -> %d", before, got)
	}

	// Probe recovery: the upstream starts answering 200; the probe loop clears
	// the quarantine and the key routes live traffic again.
	healthy.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	g.probeQuotaKey(ctx, g.zenNodes, node)
	if node.credUntil.Load() != 0 || node.cooldownUntil.Load() != 0 || node.probeNextAt.Load() != 0 {
		t.Fatalf("a successful probe must clear the quarantine, got cooldown=%d cred=%d probeNext=%d",
			node.cooldownUntil.Load(), node.credUntil.Load(), node.probeNextAt.Load())
	}
	status, body = postChat(t, g.Handler(), "deepseek-v4.1-flash-free")
	if status != http.StatusOK || !strings.Contains(body, `"content":"OK"`) {
		t.Fatalf("recovered key must route live traffic, got %d: %q", status, body)
	}
}

// respRetryAfter issues a chat request and returns the parsed Retry-After.
func respRetryAfter(t *testing.T, g *Gateway, model string) int {
	t.Helper()
	server := httptest.NewServer(g.Handler())
	defer server.Close()
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"`+model+`","stream":false,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("x-api-key", "sk-local")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	ra := resp.Header.Get("Retry-After")
	if ra == "" {
		return 0
	}
	n, err := strconv.Atoi(ra)
	if err != nil {
		return 0
	}
	return n
}

// getHealth calls the healthz endpoint and returns the decoded document.
func getHealth(t *testing.T, g *Gateway) (int, map[string]any) {
	t.Helper()
	server := httptest.NewServer(g.Handler())
	defer server.Close()
	resp, err := server.Client().Get(server.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("healthz body: %q", string(raw))
	}
	return resp.StatusCode, doc
}

// With credential_quarantine_seconds=0 the legacy behavior holds: no
// credUntil marking, so the pre-check and the wall never engage, and the
// last upstream 401 passes through exactly as before v1.7.0.
func TestE2ECredentialQuarantineDisabled(t *testing.T) {
	var healthy atomic.Bool
	var calls atomic.Int64
	upstream := credentialFakeUpstream(t, &healthy, &calls)
	g := gatewayFor(t, upstream.URL, `{"max_attempts": 3, "timeout_seconds": 30}`, `{"heartbeat_seconds": 0, "quota_probe_seconds": 0, "credential_quarantine_seconds": 0}`)
	status, _ := postChat(t, g.Handler(), "deepseek-v4.1-flash-free")
	if status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (legacy passthrough when quarantine is disabled)", status)
	}
	if node := g.zenNodes.nodes[0]; node.credUntil.Load() != 0 {
		t.Fatal("disabled quarantine must not mark credUntil")
	}
}
