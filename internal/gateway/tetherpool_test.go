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
	"strings"
	"sync"
	"testing"
	"time"

	"opencode2api/internal/config"
	"opencode2api/internal/tetherpool"
)

// tetherCP is a hermetic fake of the TetherPool control plane for the gateway
// tests: it answers every acquire with a stable per-account allocation and
// records the battery events it receives.
type tetherCP struct {
	mu       sync.Mutex
	bodies   []string
	acquires []string
}

func (cp *tetherCP) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		if r.URL.Path == "/v1/battery/event" {
			body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
			cp.bodies = append(cp.bodies, string(body))
			w.WriteHeader(http.StatusOK)
			return
		}
		cp.acquires = append(cp.acquires, r.URL.Query().Get("account_id"))
		accountID := r.URL.Query().Get("account_id")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"account_id":        accountID,
			"node_name":         "fake-warp-01",
			"gateway_proxy_url": "http://" + accountID + "__type_warp_ipv6__service_opencode2api:tp@127.0.0.1:9101",
			"exit_ip":           "2a09:bac5::1",
			"region":            r.URL.Query().Get("region"),
			"egress_type":       "warp_ipv6",
			"tier":              "standard",
		})
	})
}

func (cp *tetherCP) batteryCount() int {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return len(cp.bodies)
}

func tetherConfig(t *testing.T, baseURL string) config.Config {
	t.Helper()
	doc := `{
		"server_keys": ["sk-local"],
		"zen_keys": ["oc_sk_aaa_11111", "oc_sk_bbb_22222"],
		"proxies": ["http://static-spare:9101"],
		"tetherpool": {
			"enabled": true,
			"base_url": "` + baseURL + `",
			"timeout_seconds": 5,
			"rebind_on_wall": true,
			"rebind_min_interval_seconds": 60,
			"regions": ["singapore"]
		}
	}`
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return cfg
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// A build with a reachable control plane leads the transports with one
// allocated gateway URL per Zen key (in key order) and keeps the configured
// proxies as spares; /healthz renders the provision.
func TestBuildProvisionsTetherPoolBindings(t *testing.T) {
	cp := &tetherCP{}
	server := httptest.NewServer(cp.handler())
	defer server.Close()

	cfg := tetherConfig(t, server.URL)
	m := &RuntimeController{root: context.Background(), logger: quietLogger(), tp: tetherpool.NewProvisioner()}
	runtime, err := m.build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	g := runtime.gateway
	if got := len(g.transports.items); got != 3 {
		t.Fatalf("expected 2 allocated + 1 static transport, got %d", got)
	}
	for i, want := range []string{"oc2api_11111", "oc2api_22222"} {
		node := g.zenNodes.nodes[i]
		proxy := g.zenNodes.Proxy(node)
		if proxy == nil || !strings.Contains(proxy.name, want+"__type_warp_ipv6") {
			t.Fatalf("zen key %d must bind its own allocation %s, got %q", i, want, proxyName(proxy))
		}
	}
	// The static spare trails the allocations.
	if spare := g.transports.items[2].name; spare != "http://static-spare:9101" {
		t.Fatalf("static spare must trail the allocations, got %q", spare)
	}
	// /healthz renders the provision (the catalog is pending, so the document
	// answers 503 starting — the tetherpool section is what the test reads).
	res := httptest.NewRecorder()
	g.handleHealth(res, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	var health struct {
		TetherPool *struct {
			Mode     string `json:"mode"`
			Accounts []struct {
				AccountID string `json:"account_id"`
				Node      string `json:"node"`
			} `json:"accounts"`
		} `json:"tetherpool"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &health); err != nil {
		t.Fatalf("healthz did not decode: %v", err)
	}
	if health.TetherPool == nil || health.TetherPool.Mode != "api" || len(health.TetherPool.Accounts) != 2 {
		t.Fatalf("healthz must render the api provision, got %s", res.Body.String())
	}
}

// A build against an unreachable control plane keeps the configured proxies
// and reports static_fallback — the degradation, never a failed build.
func TestBuildFallsBackWhenControlPlaneDown(t *testing.T) {
	cfg := tetherConfig(t, "http://127.0.0.1:1") // nothing listens here
	m := &RuntimeController{root: context.Background(), logger: quietLogger(), tp: tetherpool.NewProvisioner()}
	runtime, err := m.build(cfg)
	if err != nil {
		t.Fatalf("build must not fail on an unreachable control plane: %v", err)
	}
	g := runtime.gateway
	if got := len(g.transports.items); got != 1 || g.transports.items[0].name != "http://static-spare:9101" {
		t.Fatalf("fallback must use only the configured proxies, got %+v", g.transports.items)
	}
	if g.tpSnapshot == nil || g.tpSnapshot.Mode != tetherpool.ModeStaticFallback {
		t.Fatalf("health snapshot must be static_fallback, got %+v", g.tpSnapshot)
	}
}

// A later build reuses the process cache, so a control plane that was down
// after a successful first provision does not regress the bindings.
func TestBuildReusesCachedBindings(t *testing.T) {
	cp := &tetherCP{}
	server := httptest.NewServer(cp.handler())
	cfg := tetherConfig(t, server.URL)
	m := &RuntimeController{root: context.Background(), logger: quietLogger(), tp: tetherpool.NewProvisioner()}
	if _, err := m.build(cfg); err != nil {
		t.Fatalf("first build: %v", err)
	}
	server.Close()
	runtime, err := m.build(cfg)
	if err != nil {
		t.Fatalf("second build: %v", err)
	}
	if runtime.gateway.tpSnapshot == nil || runtime.gateway.tpSnapshot.Mode != tetherpool.ModeCache {
		t.Fatalf("cached rebuild must report mode cache, got %+v", runtime.gateway.tpSnapshot)
	}
	if got := len(runtime.gateway.transports.items); got != 3 {
		t.Fatalf("cached rebuild must keep the allocated + spare shape, got %d", got)
	}
}

// A live 429 on a Zen key queues exactly one battery report per rebind
// interval, addressed to the key's TetherPool account.
func TestObserveQuotaWallFiresBatteryReport(t *testing.T) {
	cfg := tetherConfig(t, "http://127.0.0.1:1")
	g, err := New(cfg, quietLogger(), nil, nil)
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	ch := make(chan tetherpool.RebindRequest, 8)
	g.tpRebind = ch

	node := g.zenNodes.nodes[0]
	proxy := g.zenNodes.Proxy(node)
	g.observeKeyResult(context.Background(), g.zenNodes, node, proxy, quotaResponse("600"), nil)
	select {
	case ev := <-ch:
		if ev.AccountID != "oc2api_11111" || ev.Reason != "live_429" {
			t.Fatalf("unexpected rebind request: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("a live 429 must queue a battery report")
	}
	// A second wall observation inside the interval is deduplicated.
	g.observeKeyResult(context.Background(), g.zenNodes, node, proxy, quotaResponse("600"), nil)
	select {
	case ev := <-ch:
		t.Fatalf("the interval must suppress a second report, got %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}
	// Backdating the node's last-fire time starts a new episode.
	node.rebindFiredAt.Store(time.Now().Add(-2 * time.Minute).UnixNano())
	g.observeKeyResult(context.Background(), g.zenNodes, node, proxy, quotaResponse("600"), nil)
	select {
	case <-ch:
	default:
		t.Fatal("a lapsed interval must allow a new report")
	}
}

// The report is gated on the config switch and on the Zen tier: Go keys and
// disabled configurations never enqueue.
func TestObserveQuotaWallGatedByConfig(t *testing.T) {
	cfg := tetherConfig(t, "http://127.0.0.1:1")
	cfg.TetherPool.RebindOnWall = false
	g, err := New(cfg, quietLogger(), nil, nil)
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	ch := make(chan tetherpool.RebindRequest, 8)
	g.tpRebind = ch
	node := g.zenNodes.nodes[0]
	g.observeQuotaWall(node, config.TierZen, "live_429")
	select {
	case ev := <-ch:
		t.Fatalf("rebind_on_wall=false must not report, got %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}

	g2, err := New(cfg, quietLogger(), nil, nil)
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	g2.tpRebind = ch
	// The tier gate, not pool membership, is what filters Go keys: a wall
	// reported with the Go tier never enqueues.
	g2.observeQuotaWall(g2.zenNodes.nodes[0], config.TierGo, "live_429")
	select {
	case ev := <-ch:
		t.Fatalf("go-tier walls must not report, got %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}
}

// The dispatch worker drains the channel and posts the battery event the
// control plane contract defines.
func TestRebindWorkerPostsBatteryEvent(t *testing.T) {
	cp := &tetherCP{}
	server := httptest.NewServer(cp.handler())
	defer server.Close()

	cfg := tetherConfig(t, server.URL)
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &RuntimeController{root: root, logger: quietLogger(), tp: tetherpool.NewProvisioner(), tpRebind: make(chan tetherpool.RebindRequest, 8)}
	m.rearmTetherPool(cfg)
	t.Cleanup(m.Shutdown)

	m.tpRebind <- tetherpool.RebindRequest{AccountID: "oc2api_11111", KeyID: "11111", Reason: "live_429"}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cp.batteryCount() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if cp.batteryCount() != 1 {
		t.Fatalf("worker must deliver one battery event, got %d", cp.batteryCount())
	}
	cp.mu.Lock()
	body := cp.bodies[0]
	cp.mu.Unlock()
	var ev tetherpool.BatteryEvent
	if err := json.Unmarshal([]byte(body), &ev); err != nil {
		t.Fatalf("battery body did not decode: %v", err)
	}
	if ev.AccountID != "oc2api_11111" || ev.EventType != "rate_limited_429" || ev.TargetService != "opencode2api" || !ev.ForceRebind {
		t.Fatalf("unexpected battery event: %+v", ev)
	}
}

func proxyName(proxy *proxyTransport) string {
	if proxy == nil {
		return ""
	}
	return proxy.name
}
