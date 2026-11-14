package jsonutil

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

// Marshal must keep its escaping semantics (no HTML escaping, trailing
// newline trimmed) and hand back a byte slice the caller owns outright.
func TestMarshalSemantics(t *testing.T) {
	got, err := Marshal(map[string]any{"text": "a<b&c>d"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"text":"a<b&c>d"}`
	if string(got) != want {
		t.Fatalf("Marshal = %s, want %s (HTML must not be escaped, newline trimmed)", got, want)
	}
	if strings.HasSuffix(string(got), "\n") {
		t.Fatal("Marshal must trim the encoder's trailing newline")
	}
}

// The pooled buffer must never leak into the returned bytes: callers keep the
// result across further Marshal calls, so the result must be a private copy.
func TestMarshalResultIndependentOfPool(t *testing.T) {
	first, err := Marshal(map[string]any{"k": "first"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// Reuse is very likely to hand back the same buffer.
	for i := 0; i < 10; i++ {
		if _, err := Marshal(map[string]any{"k": strings.Repeat("x", 100)}); err != nil {
			t.Fatalf("Marshal: %v", err)
		}
	}
	if !bytes.Contains(first, []byte("first")) || bytes.Contains(first, []byte("xxx")) {
		t.Fatalf("first result was corrupted by later Marshal calls: %s", first)
	}
}

// Concurrent Marshal calls must not interleave: a shared pooled buffer would
// show up as garbled output under -race and this assertion.
func TestMarshalConcurrent(t *testing.T) {
	const workers = 8
	var wg sync.WaitGroup
	results := make([][]byte, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := strings.Repeat(string(rune('a'+i)), 512)
			value := map[string]any{"worker": payload}
			for round := 0; round < 50; round++ {
				got, err := Marshal(value)
				if err != nil {
					t.Errorf("Marshal: %v", err)
					return
				}
				results[i] = got
			}
		}(i)
	}
	wg.Wait()
	for i, got := range results {
		var decoded map[string]string
		if err := json.Unmarshal(got, &decoded); err != nil {
			t.Fatalf("worker %d output is not valid JSON: %v (%s)", i, err, got)
		}
	}
}

func BenchmarkMarshalSmall(b *testing.B) {
	value := map[string]any{"model": "claude-sonnet-4", "stream": true}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Marshal(value); err != nil {
			b.Fatal(err)
		}
	}
}

// A 1MB tool-description-scale payload: the pooled buffer means the encode
// buffer itself is reused instead of reallocated per request.
func BenchmarkMarshalLarge(b *testing.B) {
	value := map[string]any{
		"model": "claude-sonnet-4",
		"tools": []map[string]string{{"description": strings.Repeat("code snippet <a>&b>", 60000)}},
	}
	b.ReportAllocs()
	b.SetBytes(int64(len("tools") * 60000))
	for i := 0; i < b.N; i++ {
		if _, err := Marshal(value); err != nil {
			b.Fatal(err)
		}
	}
}
