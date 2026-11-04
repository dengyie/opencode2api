package gateway

import (
	"testing"

	"opencode2api/internal/config"
	models "opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

// The probe prefers a non-reasoning free model: production probes through a
// reasoning model spend their token budget thinking before answering. With
// metadata marking deepseek reasoning and beam not, the probe must pick beam.
func TestQuotaProbeModelPrefersNonReasoning(t *testing.T) {
	upstream := fakeUpstream(t, nil)
	g := gatewayFor(t, upstream.URL, `{"max_attempts": 1, "timeout_seconds": 30}`, `{"quota_probe_seconds": 1}`)
	g.catalog.ReplaceWithCapabilities(
		[]string{"deepseek-v4.1-flash-free", "beam-free"},
		nil,
		map[config.Tier]map[string]wire.Protocol{config.TierZen: {
			"deepseek-v4.1-flash-free": wire.Chat,
			"beam-free":                wire.Chat,
		}},
		nil,
		map[config.Tier]map[string]models.Metadata{config.TierZen: {
			"deepseek-v4.1-flash-free": {Reasoning: true},
			"beam-free":                {},
		}},
	)
	model, _, ok := g.quotaProbeModel(true)
	if !ok {
		t.Fatal("quotaProbeModel must find a probe model")
	}
	if model != "beam-free" {
		t.Fatalf("probe model = %q, want the non-reasoning beam-free", model)
	}
	// With no non-reasoning candidate left, the fallback keeps probing.
	g2 := gatewayFor(t, upstream.URL, `{"max_attempts": 1, "timeout_seconds": 30}`, `{"quota_probe_seconds": 1}`)
	g2.catalog.ReplaceWithCapabilities(
		[]string{"deepseek-v4.1-flash-free"},
		nil,
		map[config.Tier]map[string]wire.Protocol{config.TierZen: {"deepseek-v4.1-flash-free": wire.Chat}},
		nil,
		map[config.Tier]map[string]models.Metadata{config.TierZen: {
			"deepseek-v4.1-flash-free": {Reasoning: true},
		}},
	)
	model, _, ok = g2.quotaProbeModel(true)
	if !ok || model != "deepseek-v4.1-flash-free" {
		t.Fatalf("fallback probe model = %q ok=%v, want deepseek-v4.1-flash-free", model, ok)
	}
}
