package config

import (
	"os"
	"path/filepath"
	"testing"
)

// writeConfig is the test helper that persists one config document.
func writeConfig(t *testing.T, doc string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// A config with none of the v1.5.0 fields loads and gets the built-in
// defaults: keepalive on at 15s, the watchdog and the gates off.
func TestLoadDefaultsForV150Fields(t *testing.T) {
	cfg, err := Load(writeConfig(t, `{
		"server_keys": ["k"],
		"zen_keys": ["z"]
	}`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Performance.HeartbeatSeconds != 15 {
		t.Fatalf("heartbeat default = %d, want 15", cfg.Performance.HeartbeatSeconds)
	}
	if cfg.Performance.IdleTimeoutSeconds != 0 || cfg.Performance.QuotaProbeSeconds != 0 || cfg.Performance.MaxInflight != 0 || cfg.Performance.QueueWaitSeconds != 0 {
		t.Fatal("watchdog/probe/gates must default to zero (off)")
	}
}

// The new fields are accepted and validated.
func TestLoadV150Fields(t *testing.T) {
	cfg, err := Load(writeConfig(t, `{
		"server_keys": ["k"],
		"zen_keys": ["z"],
		"performance": {
			"heartbeat_seconds": 30,
			"idle_timeout_seconds": 240,
			"quota_probe_seconds": 600,
			"max_inflight": 12,
			"queue_wait_seconds": 10
		}
	}`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Performance.HeartbeatSeconds != 30 || cfg.Performance.IdleTimeoutSeconds != 240 ||
		cfg.Performance.QuotaProbeSeconds != 600 || cfg.Performance.MaxInflight != 12 || cfg.Performance.QueueWaitSeconds != 10 {
		t.Fatalf("fields did not round-trip: %+v", cfg.Performance)
	}
}

// Explicit zeros are valid and mean "off".
func TestLoadV150ExplicitZero(t *testing.T) {
	cfg, err := Load(writeConfig(t, `{
		"server_keys": ["k"],
		"zen_keys": ["z"],
		"performance": {"heartbeat_seconds": 0, "max_inflight": 0}
	}`))
	if err != nil {
		t.Fatalf("explicit zero must load: %v", err)
	}
	if cfg.Performance.HeartbeatSeconds != 0 || cfg.Performance.MaxInflight != 0 {
		t.Fatal("explicit zero must survive")
	}
}

// Negative and out-of-range values are rejected.
func TestLoadV150Validation(t *testing.T) {
	bad := []string{
		`{"server_keys":["k"],"zen_keys":["z"],"performance":{"heartbeat_seconds":-1}}`,
		`{"server_keys":["k"],"zen_keys":["z"],"performance":{"idle_timeout_seconds":-5}}`,
		`{"server_keys":["k"],"zen_keys":["z"],"performance":{"quota_probe_seconds":90000}}`,
		`{"server_keys":["k"],"zen_keys":["z"],"performance":{"max_inflight":-2}}`,
		`{"server_keys":["k"],"zen_keys":["z"],"performance":{"queue_wait_seconds":4000}}`,
		`{"server_keys":["k"],"zen_keys":["z"],"performance":{"wall_probe_timeout_seconds":-1}}`,
		`{"server_keys":["k"],"zen_keys":["z"],"performance":{"wall_probe_timeout_seconds":301}}`,
	}
	for i, doc := range bad {
		if _, err := Load(writeConfig(t, doc)); err == nil {
			t.Fatalf("case %d must be rejected", i)
		}
	}
}

// The wall probe bound (v1.5.7) defaults to 5s and explicit 0 keeps the full
// attempt timeout.
func TestLoadWallProbeTimeout(t *testing.T) {
	cfg, err := Load(writeConfig(t, `{"server_keys":["k"],"zen_keys":["z"]}`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Performance.WallProbeTimeoutSeconds != 5 {
		t.Fatalf("default wall_probe_timeout_seconds must be 5, got %d", cfg.Performance.WallProbeTimeoutSeconds)
	}
	cfg, err = Load(writeConfig(t, `{"server_keys":["k"],"zen_keys":["z"],"performance":{"wall_probe_timeout_seconds":0}}`))
	if err != nil {
		t.Fatalf("explicit zero must load: %v", err)
	}
	if cfg.Performance.WallProbeTimeoutSeconds != 0 {
		t.Fatalf("explicit zero must survive, got %d", cfg.Performance.WallProbeTimeoutSeconds)
	}
}

// A config with no tetherpool section loads and gets the built-in defaults:
// the integration is off, and the defaulted knobs are ready for a plain flip
// of enabled.
func TestLoadDefaultsForTetherPool(t *testing.T) {
	cfg, err := Load(writeConfig(t, `{
		"server_keys": ["k"],
		"zen_keys": ["z"]
	}`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.TetherPool.Enabled {
		t.Fatal("tetherpool must default to disabled")
	}
	if cfg.TetherPool.BaseURL != "http://127.0.0.1:9100" || cfg.TetherPool.Service != "opencode2api" || cfg.TetherPool.TimeoutSeconds != 10 || cfg.TetherPool.RebindMinIntervalSeconds != 600 {
		t.Fatalf("defaults did not apply: %+v", cfg.TetherPool)
	}
}

// The tetherpool fields are accepted and validated, and an enabled section
// with out-of-range or malformed values is rejected.
func TestLoadTetherPoolFields(t *testing.T) {
	cfg, err := Load(writeConfig(t, `{
		"server_keys": ["k"],
		"zen_keys": ["z"],
		"tetherpool": {
			"enabled": true,
			"base_url": "http://10.144.144.9:9100",
			"service": "opencode2api",
			"token": "secret-token",
			"timeout_seconds": 15,
			"rebind_on_wall": true,
			"rebind_min_interval_seconds": 900,
			"regions": ["singapore", "tokyo", ""]
		}
	}`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !cfg.TetherPool.Enabled || cfg.TetherPool.BaseURL != "http://10.144.144.9:9100" || cfg.TetherPool.Token != "secret-token" || !cfg.TetherPool.RebindOnWall {
		t.Fatalf("fields did not round-trip: %+v", cfg.TetherPool)
	}
	if len(cfg.TetherPool.Regions) != 2 {
		t.Fatalf("empty region entries must be dropped, got %q", cfg.TetherPool.Regions)
	}
}

func TestLoadTetherPoolValidation(t *testing.T) {
	bad := []string{
		`{"server_keys":["k"],"zen_keys":["z"],"tetherpool":{"enabled":true,"base_url":"not-a-url"}}`,
		`{"server_keys":["k"],"zen_keys":["z"],"tetherpool":{"enabled":true,"base_url":"ftp://10.144.144.9:9100"}}`,
		`{"server_keys":["k"],"zen_keys":["z"],"tetherpool":{"enabled":true,"timeout_seconds":500}}`,
		`{"server_keys":["k"],"zen_keys":["z"],"tetherpool":{"enabled":true,"rebind_min_interval_seconds":30}}`,
		`{"server_keys":["k"],"zen_keys":["z"],"tetherpool":{"enabled":true,"rebind_min_interval_seconds":100000}}`,
		// The wall feedback loop has no consumer while the integration is off.
		`{"server_keys":["k"],"zen_keys":["z"],"tetherpool":{"enabled":false,"rebind_on_wall":true}}`,
	}
	for i, doc := range bad {
		if _, err := Load(writeConfig(t, doc)); err == nil {
			t.Fatalf("case %d must be rejected", i)
		}
	}
	// A disabled section is inert: leftover values must not reject the config.
	cfg, err := Load(writeConfig(t, `{"server_keys":["k"],"zen_keys":["z"],"tetherpool":{"enabled":false,"timeout_seconds":500}}`))
	if err != nil {
		t.Fatalf("disabled section must not be validated: %v", err)
	}
	if cfg.TetherPool.TimeoutSeconds != 500 {
		t.Fatalf("disabled section values must survive untouched, got %d", cfg.TetherPool.TimeoutSeconds)
	}
}

// WithRuntimeProxies feeds the resolved list without touching the configured
// one, and Clone keeps the provisioned list and regions from aliasing.
func TestWithRuntimeProxiesAndClone(t *testing.T) {
	cfg, err := Load(writeConfig(t, `{
		"server_keys": ["k"],
		"zen_keys": ["z"],
		"proxies": ["http://static:1"],
		"tetherpool": {"regions": ["tokyo"]}
	}`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	merged := cfg.WithRuntimeProxies([]string{"http://tp-a:1", "http://tp-b:2", "http://static:1"})
	got := merged.RuntimeProxies()
	if len(got) != 3 || got[0] != "http://tp-a:1" {
		t.Fatalf("provisioned proxies must lead the resolved list, got %q", got)
	}
	if len(cfg.RuntimeProxies()) != 1 {
		t.Fatal("WithRuntimeProxies must not mutate the receiver")
	}
	clone := Clone(merged)
	clone.TetherPool.Regions[0] = "india"
	if merged.TetherPool.Regions[0] != "tokyo" {
		t.Fatal("Clone must copy TetherPool.Regions")
	}
}
