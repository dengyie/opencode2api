package gateway

import (
	"context"
	"testing"

	"opencode2api/internal/config"
)

// The v1.7.3 observability knobs are startup-only like the listeners:
// RestartStatus must report a configured-but-not-applied pprof_enabled or
// memory_limit_bytes as a pending restart field.
func TestRestartStatusObservabilityFields(t *testing.T) {
	quiet := quietLogger()
	baseline := config.Config{Listen: "127.0.0.1:8318", Performance: config.PerformanceConfig{MemoryLimitBytes: 0}}
	m := &RuntimeController{root: context.Background(), logger: quiet,
		effective: EffectiveListeners{API: baseline.Listen, PprofEnabled: false, MemoryLimitBytes: 0}}
	m.current.Store(&gatewayRuntime{config: baseline})

	if _, fields := m.RestartStatus(); len(fields) != 0 {
		t.Fatalf("no changes configured, got fields %v", fields)
	}

	changed := baseline
	changed.Performance.PprofEnabled = true
	changed.Performance.MemoryLimitBytes = 268435456
	m.current.Store(&gatewayRuntime{config: changed})
	_, fields := m.RestartStatus()
	if len(fields) != 2 || fields[0] != "performance.pprof_enabled" || fields[1] != "performance.memory_limit_bytes" {
		t.Fatalf("observability changes must be pending restart fields, got %v", fields)
	}
}
