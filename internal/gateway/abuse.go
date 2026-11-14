package gateway

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

// errFreeTierIPBlocked reports that OpenCode's free gateway refused the request
// because it flagged the egress IP, not the request, the key or the session.
// Unlike the shape gates, another lane, key or retry cannot fix it: the same
// IP gets the same answer, and retrying only extends the penalty. The request
// must therefore leave the anonymous lane instead of walking its upstreams,
// and the exit must stay out of the cursor until the block expires.
var errFreeTierIPBlocked = errors.New("free tier blocked the egress IP")

// freeTierBlockedMessage is what a client receives while its exit is blocked.
// The upstream message tells people to turn off a VPN, which is wrong for a
// server that egresses through a proxy; this one names the real cause and the
// two ways out.
const freeTierBlockedMessage = "OpenCode is blocking free-model traffic from the IP this gateway leaves from, so retrying now cannot succeed. Route the request through another exit or wait for the block to expire."

// errAnonymousLanePaused reports that the anonymous lane's circuit breaker has
// paused the whole lane. Unlike an upstream rejection, another exit or a retry
// cannot succeed while the lane is paused; the caller should use an
// authenticated tier or report the pause itself.
var errAnonymousLanePaused = errors.New("anonymous lane paused by its circuit breaker")

// anonymousLanePausedMessage is what a client receives while the circuit
// breaker holds the anonymous lane shut. It names the cause so the operator is
// not sent looking for a credential or model problem.
const anonymousLanePausedMessage = "The anonymous free tier is paused after repeated upstream failures. Configure an authenticated key, wait for the pause to expire, or restart the gateway after confirming the upstream block has lifted."

// freeTierBlockTTL keeps a blocked exit out of the anonymous cursor. Upstream
// does not publish the block duration, so this is a conservative backoff: long
// enough to stop the retry storm, short enough that an unblocked IP recovers
// without a restart.
const freeTierBlockTTL = 15 * time.Minute

// freeTierBlockTTL returns the backoff applied to an exit the free tier flagged.
func (g *Gateway) freeTierBlockTTL() time.Duration {
	if g != nil && g.freeTierBlock > 0 {
		return g.freeTierBlock
	}
	return freeTierBlockTTL
}

// abuseProbeLimit bounds how much of a 403 body is read to classify it.
const abuseProbeLimit = 16 << 10

// isNetworkAbuseBlock reports whether status and body are the upstream's IP
// abuse rejection. The response is a 403 whose message names the egress
// network, in one of the wordings the gateway has been served:
//
//   - "…free models aren't available from your current network because we've
//     detected abusive traffic from this IP address…"
//   - "Forbidden, OpenCode Free Model Gateway Detect Abuse Traffic"
//   - "Abuse of traffic"
//
// The shape gate's 403 ("free tier can only be used from within OpenCode") is
// deliberately not matched: that one is a request-shape problem, which another
// lane or a refreshed body can still fix.
func isNetworkAbuseBlock(status int, body []byte) bool {
	if status != http.StatusForbidden {
		return false
	}
	text := strings.ToLower(string(body))
	if strings.Contains(text, "abusive traffic") ||
		strings.Contains(text, "abuse traffic") ||
		strings.Contains(text, "abuse of traffic") {
		return true
	}
	return strings.Contains(text, "free models aren't available") &&
		strings.Contains(text, "from your current network")
}

// peekResponseBody reads the start of a response body and puts it back in
// front of the rest, so classification and downstream error handling both see
// the full payload.
func peekResponseBody(resp *http.Response) []byte {
	if resp == nil || resp.Body == nil {
		return nil
	}
	prefix, err := io.ReadAll(io.LimitReader(resp.Body, abuseProbeLimit))
	resp.Body = replayBody(prefix, resp.Body)
	if err != nil {
		return nil
	}
	return prefix
}

// peekNetworkAbuseBlock classifies a response without consuming its body.
func peekNetworkAbuseBlock(resp *http.Response) bool {
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		return false
	}
	return isNetworkAbuseBlock(resp.StatusCode, peekResponseBody(resp))
}

// errDeterministicClientRejection reports that the upstream rejected the
// request itself: the body is malformed, too long for the model, or fails the
// same validation on every credential. Retrying on another key, lane or tier
// cannot change that, so the request must end and the original 4xx must reach
// the client. Without this, the first tier's real error is replaced by
// whatever the next tier answers — typically a 401 from an unconfigured key,
// which tells the client to re-authenticate instead of compacting.
var errDeterministicClientRejection = errors.New("upstream rejected the request deterministically")

// deterministicMarkers are the error types and codes an upstream uses for a
// request that will fail identically everywhere. The status must be 400 or 422
// as well: a 401, 403 or 429 can be specific to the credential or the moment.
var deterministicMarkers = []string{
	"invalid_request_error",
	"invalid_request",
	"context_length_exceeded",
	"maximum context length",
	"string_above_max_length",
	"prompt is too long",
}

// isDeterministicClientResponse reports whether status and body describe a
// request-level rejection rather than a credential or availability problem.
func isDeterministicClientResponse(status int, body []byte) bool {
	if status != http.StatusBadRequest && status != http.StatusUnprocessableEntity {
		return false
	}
	text := strings.ToLower(string(body))
	for _, marker := range deterministicMarkers {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

// errGeoBlocked reports that the upstream does not offer the model in the
// region the gateway egresses from (v1.5.0). The region is a property of the
// egress, not of the request, the key or the credential, so no other key or
// lane can serve it and retrying only spends attempts on the same answer.
// The model is deliberately not marked unavailable: a different exit region
// may serve it fine.
var errGeoBlocked = errors.New("model not available in this egress region")

// geoBlockedMessage is what a client receives for errGeoBlocked. It names the
// cause and the one thing that can change it, instead of leaving a 403 that
// reads like an authentication problem to retry against.
const geoBlockedMessage = "the upstream does not offer this model in the region this gateway egresses from; egressing through another region (a different proxy or WARP exit) may make it available"

// errKeyCredentialsExhausted (v1.7.0) reports that the last upstream response
// on a key tier was a 401/403 — the upstream rejected the key itself — and no
// other key was left to try. The 401 must not reach the caller: fronting
// gateways treat a 401 as a dead channel and disable it, while every other
// key's rejection was already absorbed by the in-tier rotation. The handler
// synthesizes a 503 with the earliest quarantine expiry instead.
var errKeyCredentialsExhausted = errors.New("upstream keys failed credential validation")

// keyPoolCredentialExhaustedMessage is what a client receives when every key
// is credential-quarantined (v1.7.0). It names the cause and the recovery
// path so the caller knows the 503 is transient — the probe loop re-tests
// the keys — and not an invitation to re-send credentials it does not have.
const keyPoolCredentialExhaustedMessage = "all configured upstream keys failed credential validation (401/403); the gateway quarantined them and keeps probing, so retry after the Retry-After interval or fix the upstream keys"

// geoBlockMarkers are the region-availability wordings the upstream uses for
// a model this egress may not call. Matched only on 4xx bodies; the wordings
// are specific enough that a quota or shape rejection never trips them.
var geoBlockMarkers = []string{
	"not available in your country",
	"not available in your region",
	"unsupported country",
	"unsupported region",
	"region not supported",
	"country not supported",
	"not available in this country",
	"not available in this region",
}

// isGeoBlockResponse reports whether status and body are a region
// availability rejection. It identifies WHY a 4xx happened so the gateway can
// answer 404 model_not_available_in_region instead of a retryable 403.
func isGeoBlockResponse(status int, body []byte) bool {
	if status < 400 || status > 499 {
		return false
	}
	text := strings.ToLower(string(body))
	for _, marker := range geoBlockMarkers {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

// replayBody returns a body that yields prefix and then the rest of original.
func replayBody(prefix []byte, original io.ReadCloser) io.ReadCloser {
	return &prefixBody{Reader: io.MultiReader(bytes.NewReader(prefix), original), closer: original}
}

type prefixBody struct {
	io.Reader
	closer io.Closer
}

func (b *prefixBody) Close() error { return b.closer.Close() }
