package gateway

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	wire "opencode2api/internal/protocol"
)

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// acquire returns immediately while slots are free.
func TestInflightAcquireFreeSlot(t *testing.T) {
	gate := newInflightGate(2, 0)
	if gate == nil {
		t.Fatal("gate should exist for max=2")
	}
	release, err := gate.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got := gate.inflight.Load(); got != 1 {
		t.Fatalf("inflight = %d, want 1", got)
	}
	release()
	if got := gate.inflight.Load(); got != 0 {
		t.Fatalf("inflight after release = %d, want 0", got)
	}
}

// A full gate with wait=0 rejects immediately.
func TestInflightImmediateBusy(t *testing.T) {
	gate := newInflightGate(1, 0)
	release, err := gate.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	started := time.Now()
	if _, err := gate.acquire(context.Background()); err != errInflightBusy {
		t.Fatalf("err = %v, want errInflightBusy", err)
	}
	if time.Since(started) > 50*time.Millisecond {
		t.Fatal("wait=0 must reject immediately")
	}
}

// A full gate queues for up to wait and hands over the slot on release.
func TestInflightQueueHandover(t *testing.T) {
	gate := newInflightGate(1, 2)
	release, err := gate.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	acquired := make(chan struct{}, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		r, err := gate.acquire(context.Background())
		if err != nil {
			t.Errorf("queued acquire: %v", err)
			return
		}
		defer r()
		acquired <- struct{}{}
	}()
	time.Sleep(50 * time.Millisecond)
	if got := gate.queued.Load(); got != 1 {
		t.Fatalf("queued = %d, want 1", got)
	}
	release()
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("queued acquire never got the released slot")
	}
	wg.Wait()
}

// A queued acquire times out with errInflightBusy when no slot frees up.
func TestInflightQueueTimeout(t *testing.T) {
	gate := newInflightGate(1, 1) // 1 second wait
	release, err := gate.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	started := time.Now()
	if _, err := gate.acquire(context.Background()); err != errInflightBusy {
		t.Fatalf("err = %v, want errInflightBusy", err)
	}
	if elapsed := time.Since(started); elapsed < 900*time.Millisecond {
		t.Fatalf("the queued acquire must wait out the queue budget, returned after %v", elapsed)
	}
}

// A queued acquire honors context cancellation.
func TestInflightQueueContextCanceled(t *testing.T) {
	gate := newInflightGate(1, 5)
	release, _ := gate.acquire(context.Background())
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	if _, err := gate.acquire(ctx); err == nil {
		t.Fatal("canceled wait must return an error")
	}
}

// A zero limit disables the gate entirely.
func TestInflightDisabled(t *testing.T) {
	if newInflightGate(0, 0) != nil {
		t.Fatal("max=0 must disable the gate")
	}
	gate := newInflightGate(0, 0)
	release, err := gate.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
}

// The gated wrapper bounds every handler it wraps: a full gate answers 429
// gateway_busy with Retry-After, and a freed slot passes through to the
// handler. This is the regression guard for the /v1/systemone admission
// bypass found in review — the wrapper is what the dedicated endpoint hangs
// off, so it must reject on its own.
func TestGatedWrapperBoundsHandler(t *testing.T) {
	g := &Gateway{
		logger:   testLogger(t),
		inflight: newInflightGate(1, 0),
	}
	release, err := g.inflight.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	handler := g.gated(wire.SystemOne, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("handler must not run while the gate is full")
	})
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodPost, "/v1/systemone", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want 5", got)
	}
	// WriteError carries the taxonomy in the type field (no code field):
	// the gateway_busy token lives in the structured log event.
	if !strings.Contains(rec.Body.String(), "rate_limit_error") || !strings.Contains(rec.Body.String(), "concurrent inference limit") {
		t.Fatalf("body must carry the busy semantics: %q", rec.Body.String())
	}
	// A freed slot lets a DIFFERENT handler run to completion (the busy-path
	// handler above must never run in either call).
	release()
	passed := false
	passthrough := g.gated(wire.SystemOne, func(w http.ResponseWriter, _ *http.Request) {
		passed = true
	})
	rec = httptest.NewRecorder()
	passthrough(rec, httptest.NewRequest(http.MethodPost, "/v1/systemone", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("passthrough status = %d, want 200", rec.Code)
	}
	if !passed {
		t.Fatal("passthrough handler did not run with a free slot")
	}
}
