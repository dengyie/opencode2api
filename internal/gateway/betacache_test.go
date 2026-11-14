package gateway

import (
	"fmt"
	"sync"
	"testing"
)

// cachedAnthropicBetas must return exactly what v1AnthropicBetas derives: the
// cache is a decode memo, not a behavior change. The body below exercises the
// tool-driven, mcp, context-management and speed branches.
func TestCachedAnthropicBetasMatchesDirect(t *testing.T) {
	body := []byte(`{"model":"claude-sonnet-4","speed":"fast","mcp_servers":[{}],` +
		`"context_management":{"edits":[{"type":"compact_20260112"}]},` +
		`"tools":[{"type":"web_fetch_20260209"},{"type":"custom","input_examples":[{}]}]}`)
	want := v1AnthropicBetas(body)
	if want == "" {
		t.Fatal("precondition: the body must derive at least one beta")
	}
	for round := 0; round < 3; round++ {
		if got := cachedAnthropicBetas(body); got != want {
			t.Fatalf("cachedAnthropicBetas = %q, want the direct derivation %q", got, want)
		}
	}
	// A different body must not be served the first body's answer.
	other := []byte(`{"model":"claude-sonnet-4","tools":[{"type":"advisor_20260301"}]}`)
	otherWant := v1AnthropicBetas(other)
	if otherWant == want {
		t.Fatal("precondition: the bodies must derive different beta sets")
	}
	if got := cachedAnthropicBetas(other); got != otherWant {
		t.Fatalf("cachedAnthropicBetas(other) = %q, want %q", got, otherWant)
	}
}

// Eviction keeps the cache bounded: filling past maxCachedBetaBodies must
// evict down instead of growing forever, and the surviving entries stay
// correct.
func TestCachedAnthropicBetasEviction(t *testing.T) {
	seen := map[string]string{}
	for i := 0; i < maxCachedBetaBodies+8; i++ {
		body := []byte(fmt.Sprintf(`{"model":"m%d","speed":"fast"}`, i))
		want := v1AnthropicBetas(body)
		if got := cachedAnthropicBetas(body); got != want {
			t.Fatalf("body %d: cache = %q, want %q", i, got, want)
		}
		seen[string(body)] = want
	}
	if len(betaCache) > maxCachedBetaBodies {
		t.Fatalf("cache grew past its bound: %d entries", len(betaCache))
	}
	// Re-derive one survivor through the cache and one fresh body.
	for body, want := range seen {
		if got := cachedAnthropicBetas([]byte(body)); got != want {
			t.Fatalf("survivor cache mismatch: got %q, want %q", got, want)
		}
		break
	}
}

// Concurrent callers on the same body must all see the same derivation.
func TestCachedAnthropicBetasConcurrent(t *testing.T) {
	body := []byte(`{"model":"claude-sonnet-4","speed":"fast"}`)
	want := v1AnthropicBetas(body)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < 50; round++ {
				if got := cachedAnthropicBetas(body); got != want {
					t.Errorf("cached = %q, want %q", got, want)
					return
				}
			}
		}()
	}
	wg.Wait()
}
