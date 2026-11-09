package tetherpool

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"opencode2api/internal/httpx"
)

// Allocation is the assignment the control plane returns for an account: the
// node it bound, the direct proxy URL of that node, and the gateway proxy URL
// that routes through the binding regardless of which node currently serves
// it. The gateway consumes only GatewayProxyURL — the control plane keeps the
// binding authoritative and re-homes it server-side when a node dies.
type Allocation struct {
	AccountID       string `json:"account_id"`
	NodeName        string `json:"node_name"`
	ProxyURL        string `json:"proxy_url"`
	GatewayProxyURL string `json:"gateway_proxy_url"`
	ExitIP          string `json:"exit_ip"`
	Region          string `json:"region"`
	EgressType      string `json:"egress_type"`
	Tier            string `json:"tier"`
}

// BatteryEvent reports a downstream business outcome so the control plane can
// isolate or migrate the account's binding. EventType mirrors the control
// plane vocabulary; the gateway only ever sends rate_limited_429.
type BatteryEvent struct {
	AccountID     string `json:"account_id"`
	EventType     string `json:"event_type"`
	TargetService string `json:"target_service"`
	ForceRebind   bool   `json:"force_rebind"`
}

// Client is a minimal REST client for the TetherPool control plane. The zero
// value is not usable; build one with NewClient.
type Client struct {
	base   string
	token  string
	client *http.Client
}

// NewClient returns a client for the control plane at baseURL. An empty token
// skips the Authorization header (the control plane only requires one when
// IP_POOL_API_TOKEN is set server-side). timeout bounds every call.
func NewClient(baseURL, token string, timeout time.Duration) *Client {
	return &Client{
		base:   strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		token:  token,
		client: &http.Client{Timeout: timeout},
	}
}

// Acquire allocates (or idempotently returns) the sticky binding for
// accountID in service, preferring the requested egress type and region. The
// control plane creates the binding on first sight and returns the same
// assignment for every later call, so repeated resolves are safe. Parameters
// are query-encoded: the account id derives from arbitrary key material, so
// it may contain characters that are not query-safe.
func (c *Client) Acquire(ctx context.Context, service, accountID, egress, region string) (Allocation, error) {
	query := url.Values{}
	query.Set("service", service)
	query.Set("account_id", accountID)
	query.Set("egress_preference", egress)
	if region != "" {
		query.Set("region", region)
	}
	var alloc Allocation
	if err := c.do(ctx, http.MethodPost, "/v1/allocation/acquire?"+query.Encode(), "", &alloc); err != nil {
		return Allocation{}, err
	}
	return alloc, nil
}

// BatteryEvent posts a business outcome for accountID. With forceRebind the
// control plane immediately migrates the account to a different node.
func (c *Client) BatteryEvent(ctx context.Context, ev BatteryEvent) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, "/v1/battery/event", string(body), nil)
}

// Health checks control-plane reachability.
func (c *Client) Health(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/health", "", nil)
}

// do performs one REST call and decodes a 2xx JSON response into out (which
// may be nil for endpoints without a body of interest). Non-2xx responses
// become errors carrying the status and a trimmed body excerpt.
func (c *Client) do(ctx context.Context, method, pathAndQuery, body string, out any) error {
	var reader io.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+pathAndQuery, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", httpx.V2UserAgent())
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		excerpt, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("tetherpool %s returned HTTP %d: %s", pathAndQuery, resp.StatusCode, strings.TrimSpace(string(excerpt)))
	}
	if out == nil {
		// Drain (bounded) so the connection is reusable.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return nil
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, 1<<20))
	return dec.Decode(out)
}
