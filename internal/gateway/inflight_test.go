package gateway

import (
	"context"
	"sync"
	"testing"
	"time"
)

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
