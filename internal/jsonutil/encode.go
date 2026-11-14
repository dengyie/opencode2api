package jsonutil

import (
	"bytes"
	"encoding/json"
	"sync"
)

// bufferPool recycles the encode buffers of small and mid-size marshals, the
// per-key and per-value encodes of the v2 chat body writer and the protocol
// bridges. A buffer leaves the pool only when its bytes are copied out, so a
// returned buffer never aliases pooled memory.
var bufferPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// poolableBufferCap caps the buffers worth pooling. A larger buffer takes the
// plain path instead: its backing array would either be copied (paying a
// full-payload memcpy on every call) or retained by the pool (pinning
// megabytes per concurrent request). One-shot allocation is cheaper there.
const poolableBufferCap = 256 << 10

// Marshal encodes value the way the JavaScript clients this gateway imitates
// do. encoding/json escapes '<', '>' and '&' as \u003c, \u003e and \u0026 by
// default; JSON.stringify never does. A body carrying those escapes is
// byte-different from a real client's on every request whose text contains an
// angle bracket or ampersand, and tool descriptions and code snippets are full
// of both, so upstream request bodies go through here instead.
func Marshal(value any) ([]byte, error) {
	buffer := bufferPool.Get().(*bytes.Buffer)
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		buffer.Reset()
		bufferPool.Put(buffer)
		return nil, err
	}
	encoded := buffer.Bytes()
	if buffer.Cap() <= poolableBufferCap {
		// Copy out of the pooled buffer: callers keep the result past this
		// call, and the buffer is about to be reused by another request.
		encoded = append([]byte(nil), encoded...)
		buffer.Reset()
		bufferPool.Put(buffer)
	}
	// A buffer past the cap is dropped, not pooled: its backing array travels
	// with the returned bytes, so the next Marshal allocates fresh.
	return bytes.TrimSuffix(encoded, []byte("\n")), nil
}
