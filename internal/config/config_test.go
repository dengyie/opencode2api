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
	}
	for i, doc := range bad {
		if _, err := Load(writeConfig(t, doc)); err == nil {
			t.Fatalf("case %d must be rejected", i)
		}
	}
}
