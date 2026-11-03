package protocol

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// captureWriter records every write so tests can assert on the emitted
// frames, and serializes access because the supervisor writes concurrently
// with the pump.
type captureWriter struct {
	mu     sync.Mutex
	frames []string
}

func (w *captureWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.frames = append(w.frames, string(data))
	return len(data), nil
}

func (w *captureWriter) all() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.frames...)
}

func (w *captureWriter) joined() string { return strings.Join(w.all(), "") }

func testFlusher() http.Flusher { return httptest.NewRecorder() }

// keepalive emits comment frames for Chat after the silence threshold.
func TestSupervisorChatKeepalive(t *testing.T) {
	w := &captureWriter{}
	sup := startStreamSupervisor(context.Background(), w, testFlusher(), Chat, "m", &StreamSupervision{
		Heartbeat: 60 * time.Millisecond,
	})
	if sup == nil {
		t.Fatal("supervisor should start")
	}
	time.Sleep(400 * time.Millisecond)
	sup.Stop()
	frames := w.all()
	if len(frames) == 0 {
		t.Fatal("expected keepalive frames during silence")
	}
	for _, f := range frames {
		if !strings.HasPrefix(f, ": ") {
			t.Fatalf("chat keepalive must be an SSE comment line, got %q", f)
		}
	}
}

// Anthropic keepalive is the standard ping event.
func TestSupervisorAnthropicKeepalive(t *testing.T) {
	w := &captureWriter{}
	sup := startStreamSupervisor(context.Background(), w, testFlusher(), Anthropic, "m", &StreamSupervision{Heartbeat: 30 * time.Millisecond})
	defer sup.Stop()
	time.Sleep(300 * time.Millisecond)
	if !strings.Contains(w.joined(), "event: ping") || !strings.Contains(w.joined(), `"type":"ping"`) {
		t.Fatalf("anthropic keepalive must be event: ping, got %q", w.joined())
	}
}

// Data writes reset the heartbeat clock: a stream that keeps producing gets
// no keepalive frames at all.
func TestSupervisorNoKeepaliveWhileWriting(t *testing.T) {
	w := &captureWriter{}
	sup := startStreamSupervisor(context.Background(), w, testFlusher(), Chat, "m", &StreamSupervision{Heartbeat: 30 * time.Millisecond})
	defer sup.Stop()
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := sup.Writer().Write([]byte("data: x\n\n")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := w.joined(); strings.Contains(got, ": keepalive") {
		t.Fatalf("active stream must not receive keepalive frames, got %q", got)
	}
}

// The idle watchdog fires once: it emits an in-band error for the protocol,
// calls OnIdle, and swallows residual data writes.
func TestSupervisorIdleWatchdog(t *testing.T) {
	w := &captureWriter{}
	fired := make(chan time.Duration, 1)
	sup := startStreamSupervisor(context.Background(), w, testFlusher(), Chat, "m", &StreamSupervision{
		Idle:   40 * time.Millisecond,
		OnIdle: func(silence time.Duration) { fired <- silence },
	})
	defer sup.Stop()
	select {
	case <-fired:
	case <-time.After(3 * time.Second):
		t.Fatal("idle watchdog never fired")
	}
	if !strings.Contains(w.joined(), "upstream_idle_timeout") && !strings.Contains(w.joined(), "went idle") {
		t.Fatalf("idle fire must emit an error frame, got %q", w.joined())
	}
	// Residual data after the watchdog is swallowed, so nothing follows the
	// error frame.
	if _, err := sup.Writer().Write([]byte("data: late\n\n")); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(w.joined(), "data: late") {
		t.Fatal("data writes after the idle fire must be swallowed")
	}
}

// Upstream output defers the watchdog: MarkData keeps it quiet.
func TestSupervisorIdleDefersOnData(t *testing.T) {
	w := &captureWriter{}
	fired := make(chan struct{}, 1)
	sup := startStreamSupervisor(context.Background(), w, testFlusher(), Chat, "m", &StreamSupervision{
		Idle:   150 * time.Millisecond,
		OnIdle: func(time.Duration) { fired <- struct{}{} },
	})
	defer sup.Stop()
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		sup.MarkData()
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-fired:
		t.Fatal("idle watchdog fired despite steady upstream output")
	default:
	}
}

// Zero supervision or a nil pointer installs nothing.
func TestSupervisorDisabled(t *testing.T) {
	if startStreamSupervisor(context.Background(), &captureWriter{}, testFlusher(), Chat, "m", nil) != nil {
		t.Fatal("nil supervision must disable the supervisor")
	}
	if startStreamSupervisor(context.Background(), &captureWriter{}, testFlusher(), Chat, "m", &StreamSupervision{}) != nil {
		t.Fatal("zero supervision must disable the supervisor")
	}
}

// Concurrent writes from pump and loop must not produce interleaved frames:
// every captured frame is a complete SSE unit.
func TestSupervisorFramesStayAtomic(t *testing.T) {
	w := &captureWriter{}
	sup := startStreamSupervisor(context.Background(), w, testFlusher(), Chat, "m", &StreamSupervision{Heartbeat: 10 * time.Millisecond})
	defer sup.Stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			_, _ = sup.Writer().Write([]byte("data: {\"chunk\":" + "\"aaaaaaaaaa\"}\n\n"))
			time.Sleep(time.Millisecond)
		}
	}()
	<-done
	for _, f := range w.all() {
		if !(strings.HasPrefix(f, ": ") || strings.HasPrefix(f, "data: ")) || !strings.HasSuffix(f, "\n\n") {
			t.Fatalf("frame lost its shape: %q", f)
		}
	}
}
