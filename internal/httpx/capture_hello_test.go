package httpx

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"
)

// TestCaptureClientHello drives one request through a BunTransport against a
// local capture endpoint, so the raw ClientHello the gateway sends can be
// diffed against live captures of the official client. Skipped unless
// CAPTURE_HELLO_URL is set; the handshake itself is expected to fail there.
func TestCaptureClientHello(t *testing.T) {
	endpoint := os.Getenv("CAPTURE_HELLO_URL")
	if endpoint == "" {
		t.Skip("CAPTURE_HELLO_URL not set")
	}
	tr, err := NewBunTransport(BunOptions{ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The capture endpoint records the ClientHello and drops the connection;
	// any error here is expected.
	_, _ = tr.RoundTrip(req)
	time.Sleep(300 * time.Millisecond)
}

// TestBunLiveUpstreamHandshake proves the trimmed hello still completes a
// real handshake against the actual upstream edge (skipped unless BUN_LIVE=1).
func TestBunLiveUpstreamHandshake(t *testing.T) {
	if os.Getenv("BUN_LIVE") == "" {
		t.Skip("BUN_LIVE not set")
	}
	tr, err := NewBunTransport(BunOptions{ConnectTimeout: 10 * time.Second, ResponseHeaderTimeout: 15 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://opencode.ai/zen/v1/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatalf("live handshake through the trimmed hello failed: %v", err)
	}
	defer resp.Body.Close()
	t.Logf("live upstream answered status %d", resp.StatusCode)
	if resp.StatusCode == 0 {
		t.Fatal("no status")
	}
}
