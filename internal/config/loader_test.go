package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/speechmux/core/internal/config"
)

const testCoreYAML = `
server:
  grpc_port: 50051
  http_port: 8000
  ws_port: 8001
  max_sessions: 10
  session_timeout_sec: 60
  shutdown_drain_sec: 30

stream:
  vad_silence_sec: 0.5
  max_buffer_sec: 20

codec:
  target_sample_rate: 16000

logging:
  level: "info"
`

const testPluginsYAML = `
vad:
  socket: "/tmp/vad.sock"
  health_check_interval_sec: 10

inference:
  routing_mode: "round_robin"
  endpoints:
    - id: "ep1"
      socket: "/tmp/ep1.sock"
      priority: 1
  health_check_interval_sec: 10
`

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	f.Close()
	return f.Name()
}

func TestLoaderReadsConfig(t *testing.T) {
	path := writeTempFile(t, testCoreYAML)
	l, err := config.NewLoader(path)
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}
	cfg := l.Load()
	if cfg.Server.GRPCPort != 50051 {
		t.Errorf("grpc_port = %d, want 50051", cfg.Server.GRPCPort)
	}
	if cfg.Server.MaxSessions != 10 {
		t.Errorf("max_sessions = %d, want 10", cfg.Server.MaxSessions)
	}
	if cfg.Server.ShutdownDrainSec != 30*time.Second {
		t.Errorf("shutdown_drain_sec = %v, want 30s", cfg.Server.ShutdownDrainSec)
	}
	if cfg.Codec.TargetSampleRate != 16000 {
		t.Errorf("target_sample_rate = %d, want 16000", cfg.Codec.TargetSampleRate)
	}
}

func TestLoaderReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "core.yaml")
	if err := os.WriteFile(path, []byte(testCoreYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	l, err := config.NewLoader(path)
	if err != nil {
		t.Fatal(err)
	}

	// Write updated config.
	updated := testCoreYAML + "\n# reloaded\n"
	if err := os.WriteFile(path, []byte(updated), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := l.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	cfg := l.Load()
	if cfg == nil {
		t.Fatal("config is nil after reload")
	}
}

func TestLoaderMissingFile(t *testing.T) {
	_, err := config.NewLoader("/nonexistent/path/core.yaml")
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

func TestLoaderDefaults(t *testing.T) {
	minimal := "server:\n  grpc_port: 0\n"
	path := writeTempFile(t, minimal)
	l, err := config.NewLoader(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg := l.Load()
	if cfg.Server.GRPCPort != 50051 {
		t.Errorf("default grpc_port = %d, want 50051", cfg.Server.GRPCPort)
	}
	if cfg.Codec.TargetSampleRate != 16000 {
		t.Errorf("default sample_rate = %d, want 16000", cfg.Codec.TargetSampleRate)
	}
}

func TestLoadPlugins(t *testing.T) {
	path := writeTempFile(t, testPluginsYAML)
	p, err := config.LoadPlugins(path)
	if err != nil {
		t.Fatalf("LoadPlugins: %v", err)
	}
	if p.VAD.Socket != "/tmp/vad.sock" {
		t.Errorf("vad.socket = %q, want /tmp/vad.sock", p.VAD.Socket)
	}
	if p.Inference.RoutingMode != "round_robin" {
		t.Errorf("routing_mode = %q, want round_robin", p.Inference.RoutingMode)
	}
	if len(p.Inference.Endpoints) != 1 {
		t.Errorf("endpoints count = %d, want 1", len(p.Inference.Endpoints))
	}
}
