package ctl

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadWorkspace_Valid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "workspace.yaml")
	content := `
state_dir: /tmp/test-speechmux
processes:
  - name: vad
    command: python
    args: ["-m", "vad.main"]
    working_directory: ../plugin-vad
    restart: on-failure
    startup_delay_ms: 200
  - name: stt
    command: python
    args: ["-m", "stt.main"]
    restart: always
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write workspace: %v", err)
	}

	cfg, err := LoadWorkspace(path)
	if err != nil {
		t.Fatalf("LoadWorkspace: %v", err)
	}

	if cfg.StateDir != "/tmp/test-speechmux" {
		t.Errorf("state_dir: got %q, want /tmp/test-speechmux", cfg.StateDir)
	}
	if len(cfg.Processes) != 2 {
		t.Fatalf("processes: got %d, want 2", len(cfg.Processes))
	}

	vad := cfg.Processes[0]
	if vad.Name != "vad" {
		t.Errorf("name: got %q, want vad", vad.Name)
	}
	if vad.Restart != RestartOnFailure {
		t.Errorf("restart: got %q, want on-failure", vad.Restart)
	}
	if vad.StartupDelay() != 200*time.Millisecond {
		t.Errorf("startup_delay: got %v, want 200ms", vad.StartupDelay())
	}

	stt := cfg.Processes[1]
	if stt.Restart != RestartAlways {
		t.Errorf("restart: got %q, want always", stt.Restart)
	}
}

func TestLoadWorkspace_DefaultStateDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "workspace.yaml")
	if err := os.WriteFile(path, []byte("processes: []\n"), 0o644); err != nil {
		t.Fatalf("write workspace: %v", err)
	}

	cfg, err := LoadWorkspace(path)
	if err != nil {
		t.Fatalf("LoadWorkspace: %v", err)
	}
	if cfg.StateDir != "/tmp/speechmux" {
		t.Errorf("default state_dir: got %q, want /tmp/speechmux", cfg.StateDir)
	}
}

func TestLoadWorkspace_MissingFile(t *testing.T) {
	_, err := LoadWorkspace("/nonexistent/workspace.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadWorkspace_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "workspace.yaml")
	if err := os.WriteFile(path, []byte(":\tinvalid: yaml: [\n"), 0o644); err != nil {
		t.Fatalf("write workspace: %v", err)
	}
	_, err := LoadWorkspace(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}
