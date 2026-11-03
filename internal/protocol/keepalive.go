package protocol

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// StreamSupervision configures the per-stream keepalive and idle watchdog
// installed on committed SSE streams. A nil supervision, or one with both
// durations non-positive, installs nothing and the stream behaves exactly as
// it did before v1.5.0.
type StreamSupervision struct {
	// Heartbeat is the downstream keepalive threshold: when nothing has been
	// written to the client for this long, emit one client-invisible keepalive
	// frame (an SSE comment line for Chat and Responses, the standard
	// "event: ping" frame for Anthropic) so intermediaries and client idle
	// watchdogs do not cut a stream that is merely thinking. 0 disables it.
	Heartbeat time.Duration
	// Idle is the post-commit idle watchdog: when the UPSTREAM has produced
	// nothing for this long, emit an in-band error event, then call OnIdle so
	// the gateway can cancel the upstream request instead of waiting out the
	// whole request budget on a wedged stream. The watchdog measures upstream
	// output, not downstream writes, so keepalive frames never reset it.
	// 0 disables it.
	Idle time.Duration
	// OnIdle is called after the idle error frame has been written. The
	// gateway uses it to cancel the upstream request context and log.
	OnIdle func(silence time.Duration)
}

// streamSupervisor serializes every downstream write of one committed SSE
// stream (data frames from the pump, keepalive frames and the idle error frame
// from the background loop) behind one mutex, so a frame can never interleave
// with another. It owns two clocks: lastWrite, the downstream write clock that
// paces heartbeats, and lastUpstream, the upstream output clock that drives
// the idle watchdog.
type streamSupervisor struct {
	ctx         context.Context
	w           io.Writer
	flusher     http.Flusher
	protocol    Protocol // downstream protocol: selects the keepalive frame
	model       string   // model (or rotation alias) reported on the idle error frame
	supervision StreamSupervision

	mu           sync.Mutex
	lastWrite    atomic.Int64
	lastUpstream atomic.Int64
	idleFired    atomic.Bool
	stopOnce     sync.Once
	stop         chan struct{}
}

// startStreamSupervisor launches the background loop for one committed stream.
// It returns nil when supervision is disabled, so call sites can treat a nil
// supervisor as "no change in behavior".
func startStreamSupervisor(ctx context.Context, w io.Writer, flusher http.Flusher, downstream Protocol, model string, supervision *StreamSupervision) *streamSupervisor {
	if supervision == nil || (supervision.Heartbeat <= 0 && supervision.Idle <= 0) {
		return nil
	}
	s := &streamSupervisor{
		ctx:         ctx,
		w:           w,
		flusher:     flusher,
		protocol:    downstream,
		model:       model,
		supervision: *supervision,
		stop:        make(chan struct{}),
	}
	now := time.Now().UnixNano()
	s.lastWrite.Store(now)
	s.lastUpstream.Store(now)
	go s.run()
	return s
}

// run ticks faster than the shortest configured threshold (bounded to
// [500ms, 2s]) and fires the idle watchdog or a keepalive frame when the
// corresponding clock has run over.
func (s *streamSupervisor) run() {
	period := 2 * time.Second
	for _, threshold := range []time.Duration{s.supervision.Heartbeat, s.supervision.Idle} {
		if threshold > 0 {
			if candidate := threshold / 3; candidate < period {
				period = candidate
			}
		}
	}
	if period < 50*time.Millisecond {
		period = 50 * time.Millisecond
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
		now := time.Now().UnixNano()
		if s.supervision.Idle > 0 && !s.idleFired.Load() {
			if silence := time.Duration(now - s.lastUpstream.Load()); silence >= s.supervision.Idle {
				s.fireIdle(silence)
				return
			}
		}
		if s.supervision.Heartbeat > 0 {
			if quiet := time.Duration(now - s.lastWrite.Load()); quiet >= s.supervision.Heartbeat {
				s.writeKeepalive()
			}
		}
	}
}

// fireIdle emits the in-band idle error frame once, then hands control to the
// gateway's OnIdle (which cancels the upstream request and logs). Data frames
// that raced in are swallowed from here on, so nothing follows the error.
func (s *streamSupervisor) fireIdle(silence time.Duration) {
	s.idleFired.Store(true)
	message := fmt.Sprintf("upstream_idle_timeout: upstream stream produced no output for %s; the request was aborted instead of waiting out the budget on a wedged stream", silence.Round(time.Second))
	_ = s.idleEmitter().Emit(bridgeStreamEvent{Kind: "error", Error: message, ErrorType: "upstream_error"})
	if s.supervision.OnIdle != nil {
		s.supervision.OnIdle(silence)
	}
}

// writeKeepalive emits one client-invisible frame: an SSE comment line for
// Chat and Responses (parsers must ignore lines starting with ':') and the
// standard ping event for Anthropic.
func (s *streamSupervisor) writeKeepalive() {
	frame := ": keepalive\n\n"
	if s.protocol == Anthropic {
		frame = "event: ping\ndata: {\"type\":\"ping\"}\n\n"
	}
	_ = s.writeFrame([]byte(frame))
}

// writeThrough writes data under the lock, flushes, and counts it as
// downstream progress. bypassIdle lets the supervisor's own idle error frame
// through after the watchdog fired; the data pump's writes set it false so
// residual upstream bytes are swallowed and nothing follows the error frame.
func (s *streamSupervisor) writeThrough(data []byte, bypassIdle bool) (int, error) {
	if !bypassIdle && s.idleFired.Load() {
		return len(data), nil
	}
	s.mu.Lock()
	n, err := s.w.Write(data)
	if n > 0 {
		s.flusher.Flush()
		s.lastWrite.Store(time.Now().UnixNano())
	}
	s.mu.Unlock()
	return n, err
}

// writeFrame writes one self-contained SSE frame under the lock and flushes.
func (s *streamSupervisor) writeFrame(frame []byte) error {
	_, err := s.writeThrough(frame, false)
	return err
}

// supervisedWriter is the io.Writer the data pump writes through: every chunk
// is serialized against the background loop's frames, flushed, and counted as
// downstream progress for the heartbeat clock. After the idle watchdog fired,
// residual upstream bytes are swallowed so nothing follows the error frame.
type supervisedWriter streamSupervisor

func (s *supervisedWriter) Write(data []byte) (int, error) {
	return (*streamSupervisor)(s).writeThrough(data, false)
}

// Writer returns the io.Writer data pumps must use for downstream writes.
func (s *streamSupervisor) Writer() io.Writer { return (*supervisedWriter)(s) }

// supervisorFrameWriter is the io.Writer the supervisor's own idle error
// frame goes through: identical to the pump writer except that the idle
// swallow does not apply — without the bypass the error frame would swallow
// itself, since idleFired is set before the frame is emitted.
type supervisorFrameWriter struct{ s *streamSupervisor }

func (w supervisorFrameWriter) Write(data []byte) (int, error) {
	return w.s.writeThrough(data, true)
}

// MarkData records upstream output for the idle clock. Call it for every
// upstream chunk, whether or not it produced a downstream write.
func (s *streamSupervisor) MarkData() { s.lastUpstream.Store(time.Now().UnixNano()) }

// Stop ends the background loop. Safe to call multiple times; the deferred
// Stop at the stream call site is what bounds the goroutine's life even when
// the loop already returned after firing the watchdog.
func (s *streamSupervisor) Stop() { s.stopOnce.Do(func() { close(s.stop) }) }

// idleEmitter delivers the idle error in the downstream protocol. It builds a
// fresh encoder writing through the supervisor's bypass writer, so the frame
// is serialized like any other but survives the idle swallow; a fresh encoder
// for a standalone error event matches the established
// emitUnexpectedStreamError pattern.
func (s *streamSupervisor) idleEmitter() *bridgeStreamEncoder {
	return bridgeStreamEmitter(supervisorFrameWriter{s}, s.flusher, s.protocol, s.model)
}

// markDataObserver wraps the usage observer so every upstream chunk read by
// io.Copy also feeds the idle clock.
type markDataObserver struct {
	inner *streamUsageTracker
	sup   *streamSupervisor
}

func (o *markDataObserver) Write(data []byte) (int, error) {
	if o.sup != nil {
		o.sup.MarkData()
	}
	return o.inner.Write(data)
}
