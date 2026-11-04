package protocol

import "testing"

// The marker scan must match exactly what the response encoders emit, per
// protocol, and never flag an empty document.
func TestDetectReasoning(t *testing.T) {
	cases := []struct {
		protocol Protocol
		doc      string
		want     bool
	}{
		{Chat, `{"choices":[{"message":{"content":"hi"}}]}`, false},
		{Chat, `{"choices":[{"message":{"content":"","reasoning_content":"thinking..."}}]}`, true},
		{Anthropic, `{"content":[{"type":"text","text":"hi"}]}`, false},
		{Anthropic, `{"content":[{"type":"thinking","thinking":"hmm"}]}`, true},
		{Anthropic, `{"content":[{"type":"redacted_thinking","data":"..."}]}`, true},
		{Responses, `{"output":[{"type":"message","content":[]}]}`, false},
		{Responses, `{"output":[{"type":"reasoning","summary":[]}]}`, true},
		{Chat, ``, false},
	}
	for _, tc := range cases {
		if got := DetectReasoning(tc.protocol, []byte(tc.doc)); got != tc.want {
			t.Errorf("DetectReasoning(%v, %q) = %v, want %v", tc.protocol, tc.doc, got, tc.want)
		}
	}
}
