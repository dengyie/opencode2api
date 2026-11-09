package tetherpool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"opencode2api/internal/config"
)

// AccountPrefix is prepended to every Zen key display id so the bindings of
// this gateway are recognizable in TetherPool metrics and dashboards.
const AccountPrefix = "oc2api_"

// AccountID returns the TetherPool account a Zen key binds to. The display id
// (the key's last five runes) is stable for the lifetime of the key, so the
// account — and with it the sticky binding — survives restarts.
func AccountID(key string) string {
	return AccountPrefix + config.KeyDisplayID(key)
}

// RebindRequest is one wall-triggered battery report queued for the dispatch
// worker. KeyID carries the display id for logging only.
type RebindRequest struct {
	AccountID string
	KeyID     string
	Reason    string
}

// KeyBinding is the resolved state of one Zen key's egress.
type KeyBinding struct {
	AccountID string `json:"account_id"`
	NodeName  string `json:"node"`
	Region    string `json:"region"`
	ExitIP    string `json:"exit_ip"`
	ProxyURL  string `json:"-"`
	Cached    bool   `json:"cached"`
}

// Snapshot is the provision state the gateway renders in /healthz. Mode is
// "api" when every binding was freshly acquired, "cache" when at least one
// came from the process cache, and "static_fallback" when the control plane
// could not satisfy every key.
type Snapshot struct {
	Mode      string       `json:"mode"`
	Accounts  []KeyBinding `json:"accounts,omitempty"`
	UpdatedAt time.Time    `json:"updated_at,omitempty"`
}

// Provisioner allocates Zen key bindings and caches their resolved proxy URLs
// for the lifetime of the process, so config applies rebuild the gateway
// without re-hitting the control plane for keys it already knows.
type Provisioner struct {
	mu    sync.Mutex
	cache map[string]KeyBinding
}

// NewProvisioner returns an empty provisioner.
func NewProvisioner() *Provisioner {
	return &Provisioner{cache: make(map[string]KeyBinding)}
}

// Resolve returns the proxy list for cfg: one TetherPool gateway URL per Zen
// key, in ZenKeys order, followed by the statically configured entries as
// failover spares. nodePool binds key i to transport i modulo the transport
// count, so leading the list with one entry per key pairs every key with its
// own binding and keeps the static entries as spares.
//
// The resolve is all-or-nothing: if any key cannot be filled from the cache
// or the control plane, the static list alone is good enough and Resolve
// returns an error — a partially provisioned list would shift the positional
// bindings of every key after the gap. Callers must treat an error as "keep
// the configured proxies", never as a gateway build failure.
func (p *Provisioner) Resolve(ctx context.Context, cfg config.Config, logger *slog.Logger) ([]string, Snapshot, error) {
	keys := cfg.ZenKeys
	if !cfg.TetherPool.Enabled || len(keys) == 0 {
		return nil, Snapshot{}, nil
	}
	client := NewClient(cfg.TetherPool.BaseURL, cfg.TetherPool.Token, time.Duration(cfg.TetherPool.TimeoutSeconds)*time.Second)
	bindings := make([]KeyBinding, len(keys))
	errs := make([]error, len(keys))
	// The whole resolve shares the configured call budget: it runs during a
	// build, which must not hang a config apply on an unreachable plane.
	ctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.TetherPool.TimeoutSeconds)*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i, key := range keys {
		displayID := config.KeyDisplayID(key)
		region := ""
		if len(cfg.TetherPool.Regions) > 0 {
			region = strings.TrimSpace(cfg.TetherPool.Regions[i%len(cfg.TetherPool.Regions)])
		}
		cacheKey := displayID + "|" + region
		p.mu.Lock()
		cached, ok := p.cache[cacheKey]
		p.mu.Unlock()
		if ok {
			bindings[i] = cached
			bindings[i].Cached = true
			continue
		}
		wg.Add(1)
		go func(i int, key, region, cacheKey string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			alloc, err := client.Acquire(ctx, cfg.TetherPool.Service, AccountID(key), "warp_ipv6", region)
			if err == nil {
				err = validateGatewayURL(alloc.GatewayProxyURL)
			}
			if err != nil {
				errs[i] = err
				return
			}
			binding := KeyBinding{AccountID: AccountID(key), NodeName: alloc.NodeName, Region: alloc.Region, ExitIP: alloc.ExitIP, ProxyURL: alloc.GatewayProxyURL}
			bindings[i] = binding
			p.mu.Lock()
			p.cache[cacheKey] = binding
			p.mu.Unlock()
		}(i, key, region, cacheKey)
	}
	wg.Wait()

	entries := make([]string, 0, len(keys))
	cached := 0
	for i := range bindings {
		if bindings[i].ProxyURL == "" {
			reason := errs[i]
			if reason == nil {
				reason = errors.New("no binding")
			}
			return nil, Snapshot{}, fmt.Errorf("tetherpool: %s: %w", AccountID(keys[i]), reason)
		}
		if bindings[i].Cached {
			cached++
		}
		entries = append(entries, bindings[i].ProxyURL)
	}
	entries = append(entries, cfg.RuntimeProxies()...)
	mode := "api"
	if cached > 0 {
		mode = "cache"
	}
	return entries, Snapshot{Mode: mode, Accounts: bindings, UpdatedAt: time.Now().UTC()}, nil
}

// validateGatewayURL accepts the gateway proxy URLs the control plane hands
// out. They must parse as http(s) with a host, matching the schemes
// transportPool builds transports for.
func validateGatewayURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (strings.ToLower(u.Scheme) != "http" && strings.ToLower(u.Scheme) != "https") {
		return fmt.Errorf("invalid gateway proxy URL %q", raw)
	}
	return nil
}
