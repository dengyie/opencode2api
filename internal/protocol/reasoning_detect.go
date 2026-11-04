package protocol

import "bytes"

// DetectReasoning reports whether a collapsed (or raw JSON) response document
// for the given protocol actually carries a reasoning payload. The upstream
// capability catalog has been observed to flag every free model
// reasoning=true regardless of real behavior (crouton-alpha-free answers with
// none, 2026-11-05), so downstream cheap-probe selection relies on live
// observation instead of the catalog. The markers match exactly what this
// package's response encoders emit, so a false positive requires our own
// encoder to have written a reasoning block.
func DetectReasoning(protocol Protocol, doc []byte) bool {
	if len(doc) == 0 {
		return false
	}
	switch protocol {
	case Anthropic:
		return bytes.Contains(doc, []byte(`"type":"thinking"`)) ||
			bytes.Contains(doc, []byte(`"type": "thinking"`)) ||
			bytes.Contains(doc, []byte(`"type":"redacted_thinking"`))
	case Responses:
		return bytes.Contains(doc, []byte(`"type":"reasoning"`)) ||
			bytes.Contains(doc, []byte(`"type": "reasoning"`))
	default:
		return bytes.Contains(doc, []byte(`"reasoning_content"`))
	}
}
