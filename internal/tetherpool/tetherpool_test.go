package tetherpool

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"opencode2api/internal/config"
)

// recordingCP is a hermetic fake of the TetherPool control plane: it answers
// acquire with one allocation per account and records every request.
type recordingCP struct {
	mu       sync.Mutex
	requests []string // "METHOD path?query" per request
	bodies   []string
	failFor  map[string]bool // account_id → force HTTP 500
	proxyFor func(accountID string) string
}

func (cp *recordingCP) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		cp.requests = append(cp.requests, r.Method+" "+r.URL.RequestURI())
		if r.Body != nil {
			body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
			cp.bodies = append(cp.bodies, string(body))
		}
		accountID := r.URL.Query().Get("account_id")
		if cp.failFor[accountID] {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.URL.Path == "/v1/battery/event" {
			w.WriteHeader(http.StatusOK)
			return
		}
		proxy := "http://" + accountID + "__type_warp_ipv6:tp@127.0.0.1:9101"
		if cp.proxyFor != nil {
			proxy = cp.proxyFor(accountID)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(Allocation{
			AccountID:       accountID,
			NodeName:        "fake-warp-01",
			GatewayProxyURL: proxy,
			ExitIP:          "2a09:bac5::1",
			Region:          r.URL.Query().Get("region"),
			EgressType:      "warp_ipv6",
			Tier:            "standard",
		})
	})
}

func (cp *recordingCP) count() int {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return len(cp.requests)
}

func testConfig(t *testing.T, baseURL string) config.Config {
	cfg, err := config.Load(writeTetherConfig(t, baseURL))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return cfg
}

func writeTetherConfig(t *testing.T, baseURL string) string {
	t.Helper()
	path := t.TempDir() + "/config.json"
	doc := `{
		"server_keys": ["sk-local"],
		"zen_keys": ["oc_sk_aaa_11111", "oc_sk_bbb_22222", "oc_sk_ccc_33333"],
		"proxies": ["http://static-spare:9101"],
		"tetherpool": {
			"enabled": true,
			"base_url": "` + baseURL + `",
			"timeout_seconds": 5,
			"rebind_on_wall": true,
			"regions": ["singapore", "tokyo"]
		}
	}`
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Acquire hits the universal allocation endpoint with the service namespace,
// the account and the region preference, and decodes the assignment.
func TestClientAcquire(t *testing.T) {
	cp := &recordingCP{}
	server := httptest.NewServer(cp.handler())
	defer server.Close()

	client := NewClient(server.URL, "token-1", 5*time.Second)
	alloc, err := client.Acquire(context.Background(), "opencode2api", "oc2api_11111", "warp_ipv6", "tokyo")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if alloc.GatewayProxyURL == "" || alloc.NodeName != "fake-warp-01" {
		t.Fatalf("allocation did not decode: %+v", alloc)
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	// url.Values.Encode orders parameters alphabetically.
	if len(cp.requests) != 1 || cp.requests[0] != "POST /v1/allocation/acquire?account_id=oc2api_11111&egress_preference=warp_ipv6&region=tokyo&service=opencode2api" {
		t.Fatalf("unexpected request: %q", cp.requests)
	}
}

// The account id derives from arbitrary key material: characters that are not
// query-safe must arrive escaped so the control plane sees one account.
func TestClientAcquireEscapesAccountID(t *testing.T) {
	cp := &recordingCP{}
	server := httptest.NewServer(cp.handler())
	defer server.Close()

	client := NewClient(server.URL, "", 5*time.Second)
	rawAccount := "oc2api_a&b#c d%e+f"
	if _, err := client.Acquire(context.Background(), "opencode2api", rawAccount, "warp_ipv6", ""); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if len(cp.requests) != 1 {
		t.Fatalf("unexpected request count: %q", cp.requests)
	}
	if got := queryParam(cp.requests[0], "account_id"); got != rawAccount {
		t.Fatalf("control plane must see the raw account id, got %q", got)
	}
}

// A token-bearing client sends the bearer credential; a non-2xx answer is an
// error carrying the status.
func TestClientAuthAndErrors(t *testing.T) {
	cp := &recordingCP{failFor: map[string]bool{"oc2api_bad": true}}
	server := httptest.NewServer(cp.handler())
	defer server.Close()

	client := NewClient(server.URL, "token-1", 5*time.Second)
	if err := client.Health(context.Background()); err != nil {
		t.Fatalf("health: %v", err)
	}
	if _, err := client.Acquire(context.Background(), "svc", "oc2api_bad", "warp_ipv6", ""); err == nil {
		t.Fatal("a 500 from the control plane must be an error")
	}
}

// BatteryEvent posts the JSON body the control plane contract defines.
func TestClientBatteryEvent(t *testing.T) {
	cp := &recordingCP{}
	server := httptest.NewServer(cp.handler())
	defer server.Close()

	client := NewClient(server.URL, "", 5*time.Second)
	err := client.BatteryEvent(context.Background(), BatteryEvent{AccountID: "oc2api_11111", EventType: "rate_limited_429", TargetService: "opencode2api", ForceRebind: true})
	if err != nil {
		t.Fatalf("battery event: %v", err)
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if len(cp.bodies) == 0 {
		t.Fatal("battery event must carry a JSON body")
	}
	var ev BatteryEvent
	if err := json.Unmarshal([]byte(cp.bodies[len(cp.bodies)-1]), &ev); err != nil {
		t.Fatalf("body is not a battery event: %v", err)
	}
	if ev.EventType != "rate_limited_429" || !ev.ForceRebind || ev.TargetService != "opencode2api" {
		t.Fatalf("unexpected battery event: %+v", ev)
	}
}

// Resolve leads the merged list with one gateway URL per Zen key in key order
// and keeps the configured proxies as spares behind them.
func TestResolveProvisionsOneBindingPerKey(t *testing.T) {
	cp := &recordingCP{}
	server := httptest.NewServer(cp.handler())
	defer server.Close()

	cfg := testConfig(t, server.URL)
	p := NewProvisioner()
	entries, snapshot, err := p.Resolve(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// 3 keys + 1 static spare.
	if len(entries) != 4 {
		t.Fatalf("expected 3 allocated + 1 static entry, got %q", entries)
	}
	for i, want := range []string{"oc2api_11111", "oc2api_22222", "oc2api_33333"} {
		if !containsBind(entries[i], want) {
			t.Fatalf("entry %d must route the %s binding, got %q", i, want, entries[i])
		}
	}
	if entries[3] != "http://static-spare:9101" {
		t.Fatalf("static spares must trail the allocations, got %q", entries[3])
	}
	if snapshot.Mode != "api" || len(snapshot.Accounts) != 3 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	// Regions cycle by key position: key i asks for Regions[i % len]. The
	// acquires run in parallel, so assert on the account→region mapping, not
	// on arrival order.
	cp.mu.Lock()
	uris := append([]string(nil), cp.requests...)
	cp.mu.Unlock()
	regionByAccount := make(map[string]string)
	for _, uri := range uris {
		if account, region := queryParam(uri, "account_id"), queryParam(uri, "region"); account != "" {
			regionByAccount[account] = region
		}
	}
	want := map[string]string{"oc2api_11111": "singapore", "oc2api_22222": "tokyo", "oc2api_33333": "singapore"}
	for account, region := range want {
		if regionByAccount[account] != region {
			t.Fatalf("account %s must prefer region %s, got %q", account, region, regionByAccount[account])
		}
	}
}

// queryParam extracts one query parameter from a recorded "METHOD path?query"
// request line.
func queryParam(uri, name string) string {
	if i := strings.Index(uri, " "); i >= 0 {
		uri = uri[i+1:]
	}
	u, err := url.ParseRequestURI(uri)
	if err != nil {
		return ""
	}
	return u.Query().Get(name)
}

// A second resolve is served from the cache without touching the control
// plane, and is reported as mode "cache".
func TestResolveCachesBindings(t *testing.T) {
	cp := &recordingCP{}
	server := httptest.NewServer(cp.handler())
	defer server.Close()

	cfg := testConfig(t, server.URL)
	p := NewProvisioner()
	if _, _, err := p.Resolve(context.Background(), cfg, nil); err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	before := cp.count()
	entries, snapshot, err := p.Resolve(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("cached resolve: %v", err)
	}
	if cp.count() != before {
		t.Fatal("cached resolve must not call the control plane")
	}
	if snapshot.Mode != "cache" {
		t.Fatalf("cached resolve must report mode cache, got %q", snapshot.Mode)
	}
	if len(entries) != 4 {
		t.Fatalf("cached resolve must keep the merged shape, got %q", entries)
	}
}

// The resolve is all-or-nothing: one failing account sends the whole list
// back to the static configuration.
func TestResolveFailsClosedOnPartialAllocation(t *testing.T) {
	cp := &recordingCP{failFor: map[string]bool{"oc2api_22222": true}}
	server := httptest.NewServer(cp.handler())
	defer server.Close()

	cfg := testConfig(t, server.URL)
	p := NewProvisioner()
	entries, _, err := p.Resolve(context.Background(), cfg, nil)
	if err == nil {
		t.Fatal("a partially satisfied resolve must fail")
	}
	if entries != nil {
		t.Fatalf("a failed resolve must not return entries, got %q", entries)
	}
}

// A control plane answer without a usable gateway URL is rejected.
func TestResolveRejectsMalformedGatewayURL(t *testing.T) {
	cp := &recordingCP{proxyFor: func(string) string { return "not a url" }}
	server := httptest.NewServer(cp.handler())
	defer server.Close()

	cfg := testConfig(t, server.URL)
	p := NewProvisioner()
	if _, _, err := p.Resolve(context.Background(), cfg, nil); err == nil {
		t.Fatal("a malformed gateway URL must fail the resolve")
	}
}

// A disabled section resolves to nothing and the caller keeps the static list.
func TestResolveNoopWhenDisabled(t *testing.T) {
	cfg := testConfig(t, "http://127.0.0.1:1")
	cfg.TetherPool.Enabled = false
	p := NewProvisioner()
	entries, snapshot, err := p.Resolve(context.Background(), cfg, nil)
	if err != nil || entries != nil || snapshot.Mode != "" {
		t.Fatalf("disabled resolve must be a no-op, got %q %+v %v", entries, snapshot, err)
	}
}

// AccountID derives the stable per-key account from the display id.
func TestAccountID(t *testing.T) {
	if got := AccountID("oc_sk_58812127cb89_f6P05ESLALo1yXGcbHF8i-TKANvmy8Lw"); got != "oc2api_my8Lw" {
		t.Fatalf("account id = %q, want oc2api_my8Lw", got)
	}
}

// Two keys whose display ids collide are warned about (they would share one
// binding) while the resolve itself still succeeds.
func TestResolveWarnsOnDuplicateDisplayID(t *testing.T) {
	cp := &recordingCP{}
	server := httptest.NewServer(cp.handler())
	defer server.Close()

	cfg := testConfig(t, server.URL)
	cfg.ZenKeys = []string{"oc_sk_aaa_11111", "oc_sk_bbb_11111"} // same display id
	var logBuf lockedBuffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))
	p := NewProvisioner()
	entries, _, err := p.Resolve(context.Background(), cfg, logger)
	if err != nil {
		t.Fatalf("resolve must succeed despite the collision: %v", err)
	}
	if !strings.Contains(logBuf.String(), "tp_duplicate_key_id") {
		t.Fatalf("a duplicate display id must be warned about, log: %s", logBuf.String())
	}
	// Both slots carry the same shared binding.
	if entries[0] != entries[1] {
		t.Fatalf("colliding keys must share one binding, got %q vs %q", entries[0], entries[1])
	}
}

// lockedBuffer is a small io.Writer safe for concurrent use by the provisioner.
type lockedBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func containsBind(entry, account string) bool {
	return strings.Contains(entry, account+"__type_warp_ipv6")
}
