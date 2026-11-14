package protocol

import (
	"strings"
	"testing"
)

// feed relays data through the tracker the way ForwardStream's TeeReader
// does: one Write per relay chunk.
func feed(t *testing.T, observer *streamUsageTracker, chunks ...string) {
	t.Helper()
	for _, chunk := range chunks {
		n, err := observer.Write([]byte(chunk))
		if err != nil || n != len(chunk) {
			t.Fatalf("Write(%q) = %d, %v", chunk, n, err)
		}
	}
}

// A complete Chat stream split across Writes at awkward boundaries (mid-frame,
// mid-line, mid-JSON) reports usage once and normal termination, and usage is
// merged before Finish.
func TestStreamTrackerSplitWrites(t *testing.T) {
	observer := streamUsageObserver(Chat)
	feed(t, observer,
		"data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n",
		"data: {\"choices\":[{\"del",
		"ta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}",
		"]}\n\ndata: {\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":7}}\n\ndata: [DONE]\n\n",
	)
	usage := observer.Finish()
	if !observer.NormalTermination() {
		t.Fatal("the [DONE] frame must mark normal termination")
	}
	if !observer.Reported() {
		t.Fatal("usage must be reported")
	}
	if usage.Input != 11 || usage.Output != 7 {
		t.Fatalf("usage = %+v, want input 11 output 7", usage)
	}
	if observer.ParseError() != nil {
		t.Fatalf("unexpected parse error: %v", observer.ParseError())
	}
}

// CRLF-terminated frames (the alternative SSE terminator) are consumed the
// same way.
func TestStreamTrackerCRLF(t *testing.T) {
	observer := streamUsageObserver(Chat)
	feed(t, observer,
		"data: {\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4}}\r\n\r\n",
		"data: [DONE]\r\n\r\n",
	)
	usage := observer.Finish()
	if usage.Input != 3 || usage.Output != 4 {
		t.Fatalf("usage = %+v, want input 3 output 4", usage)
	}
	if !observer.NormalTermination() || observer.ErrorTermination() {
		t.Fatalf("termination flags wrong: normal=%v error=%v", observer.NormalTermination(), observer.ErrorTermination())
	}
}

// An upstream error event marks error termination, and an Anthropic
// message_start usage is merged even when the stream later fails.
func TestStreamTrackerErrorAndAnthropicUsage(t *testing.T) {
	observer := streamUsageObserver(Anthropic)
	feed(t, observer,
		"event: message_start\ndata: {\"message\":{\"id\":\"msg_1\",\"usage\":{\"input_tokens\":20,\"output_tokens\":1}}}\n\n",
		"event: error\ndata: {\"error\":{\"message\":\"boom\",\"type\":\"upstream_error\"}}\n\n",
	)
	usage := observer.Finish()
	if !observer.ErrorTermination() {
		t.Fatal("the error event must mark error termination")
	}
	if usage.Input != 20 {
		t.Fatalf("usage = %+v, want input 20", usage)
	}
	if !observer.Reported() {
		t.Fatal("message_start usage must count as reported")
	}
}

// An unterminated tail (no blank line) is never parsed as a frame: usage that
// arrives in the last unterminated record must not count.
func TestStreamTrackerUnterminatedTailNotConsumed(t *testing.T) {
	observer := streamUsageObserver(Chat)
	feed(t, observer, "data: {\"usage\":{\"prompt_tokens\":99,\"completion_tokens\":99}}\n")
	usage := observer.Finish()
	if observer.Reported() {
		t.Fatal("usage in an unterminated tail must not be reported")
	}
	if usage.Input != 0 || usage.Output != 0 {
		t.Fatalf("usage = %+v, want zero", usage)
	}
	// The tail survives into a later Write and parses when terminated.
	feed(t, observer, "\ndata: [DONE]\n\n")
	if !observer.NormalTermination() {
		t.Fatal("the completed tail must mark normal termination")
	}
}

// An invalid JSON frame is remembered as a parse error but does not stop the
// tracker.
func TestStreamTrackerParseErrorRemembered(t *testing.T) {
	observer := streamUsageObserver(Chat)
	feed(t, observer, "data: {not json}\n\n", "data: [DONE]\n\n")
	if observer.ParseError() == nil {
		t.Fatal("the invalid frame must be remembered as a parse error")
	}
	if !observer.NormalTermination() {
		t.Fatal("the tracker must keep consuming after a parse error")
	}
}

// The zero-copy scan must not let a long stream accumulate: after Writes that
// complete many frames, the retained buffer holds only the unterminated tail.
func TestStreamTrackerBufferStaysSmall(t *testing.T) {
	observer := streamUsageObserver(Chat)
	feed(t, observer, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
	for i := 0; i < 1000; i++ {
		_, _ = observer.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"chunk\"}}]}\n\n"))
	}
	// Every frame was complete, so nothing but an empty tail may remain.
	if len(observer.buffer) > 0 {
		t.Fatalf("fully consumed stream retained %d bytes", len(observer.buffer))
	}
	// A trailing partial frame stays bounded by the partial itself.
	_, _ = observer.Write([]byte("data: {\"partial\":"))
	if len(observer.buffer) != len("data: {\"partial\":") {
		t.Fatalf("partial tail = %d bytes, want exactly the partial", len(observer.buffer))
	}
}

func BenchmarkStreamTrackerLongStream(b *testing.B) {
	frame := "data: {\"choices\":[{\"delta\":{\"content\":\"" + strings.Repeat("x", 256) + "\"}}]}\n\n"
	payload := strings.Repeat(frame, 512) // ~150KB, 512 frames
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		observer := streamUsageObserver(Chat)
		// 8 relay chunks per stream, the shape io.Copy produces on a real socket.
		chunk := payload[:len(payload)/8]
		for c := 0; c < 8; c++ {
			observer.Write([]byte(chunk))
		}
		observer.Finish()
	}
}
