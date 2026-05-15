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

// newTestWorkspace builds a WorkspaceConfig with a two-level profiles section and slot/direct entries.
func newTestWorkspace() *WorkspaceConfig {
	return &WorkspaceConfig{
		StateDir: "/tmp/test",
		Profiles: map[string]map[string]ProfileConfig{
			"vad-plugins": {
				"silero": {
					Command:        ".venv/bin/python3",
					Args:           []string{"-m", "speechmux_plugin_vad.main"},
					Restart:        RestartOnFailure,
					StartupDelayMs: 500,
				},
			},
			"stt-plugins": {
				"sherpa-onnx": {
					Command: ".venv/bin/python3",
					Args:    []string{"-m", "speechmux_plugin_stt.main", "--config", "inference-onnx.yaml"},
					Restart: RestartOnFailure,
				},
				"mlx-whisper": {
					Command: ".venv/bin/python3",
					Args:    []string{"-m", "speechmux_plugin_stt.main", "--config", "inference-mlx.yaml"},
					Restart: RestartOnFailure,
				},
			},
		},
		Processes: []ProcessConfig{
			{Name: "vad-plugins", Profiles: []string{"silero"}},
			{Name: "stt-plugins", Profiles: []string{"sherpa-onnx", "mlx-whisper"}},
			{Name: "core", Command: "./bin/core", Restart: RestartOnFailure, StartupDelayMs: 1000},
		},
	}
}

func procNames(cfg *WorkspaceConfig) []string {
	out := make([]string, 0, len(cfg.Processes))
	for _, p := range cfg.Processes {
		out = append(out, p.Name)
	}
	return out
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestFilterByProfiles_NoActiveProfiles_ExpandsAll(t *testing.T) {
	cfg := newTestWorkspace()
	got := cfg.FilterByProfiles(nil)
	// All profiles in all slots + core.
	want := []string{"silero", "sherpa-onnx", "mlx-whisper", "core"}
	if !equalSlices(procNames(got), want) {
		t.Errorf("got %v, want %v", procNames(got), want)
	}
}

func TestFilterByProfiles_SingleVADProfile(t *testing.T) {
	cfg := newTestWorkspace()
	got := cfg.FilterByProfiles([]string{"silero"})
	want := []string{"silero", "core"}
	if !equalSlices(procNames(got), want) {
		t.Errorf("got %v, want %v", procNames(got), want)
	}
}

func TestFilterByProfiles_VADAndOneSTT(t *testing.T) {
	cfg := newTestWorkspace()
	got := cfg.FilterByProfiles([]string{"silero", "sherpa-onnx"})
	want := []string{"silero", "sherpa-onnx", "core"}
	if !equalSlices(procNames(got), want) {
		t.Errorf("got %v, want %v", procNames(got), want)
	}
}

func TestFilterByProfiles_MLXWhisper(t *testing.T) {
	cfg := newTestWorkspace()
	got := cfg.FilterByProfiles([]string{"silero", "mlx-whisper"})
	want := []string{"silero", "mlx-whisper", "core"}
	if !equalSlices(procNames(got), want) {
		t.Errorf("got %v, want %v", procNames(got), want)
	}
}

func TestFilterByProfiles_InstantiatesProfileSettings(t *testing.T) {
	cfg := newTestWorkspace()
	got := cfg.FilterByProfiles([]string{"silero"})

	var sileroProc *ProcessConfig
	for i := range got.Processes {
		if got.Processes[i].Name == "silero" {
			sileroProc = &got.Processes[i]
		}
	}
	if sileroProc == nil {
		t.Fatal("silero process not found in result")
	}
	if sileroProc.Command != ".venv/bin/python3" {
		t.Errorf("command: got %q, want .venv/bin/python3", sileroProc.Command)
	}
	if sileroProc.StartupDelay() != 500*time.Millisecond {
		t.Errorf("startup_delay: got %v, want 500ms", sileroProc.StartupDelay())
	}
}

func TestFilterByProfiles_DirectEntryAlwaysIncluded(t *testing.T) {
	cfg := newTestWorkspace()
	// Even with no matching STT/VAD profile, core must appear.
	got := cfg.FilterByProfiles([]string{"silero"})
	last := got.Processes[len(got.Processes)-1]
	if last.Name != "core" {
		t.Errorf("last process: got %q, want core", last.Name)
	}
	if last.Command != "./bin/core" {
		t.Errorf("core command: got %q, want ./bin/core", last.Command)
	}
}

func TestFilterByProfiles_UndefinedProfileSkipped(t *testing.T) {
	cfg := newTestWorkspace()
	// "faster-whisper" is in activeProfiles but not defined in profiles section.
	got := cfg.FilterByProfiles([]string{"silero", "faster-whisper"})
	for _, p := range got.Processes {
		if p.Name == "faster-whisper" {
			t.Error("undefined profile must not appear in result")
		}
	}
}

func TestFilterByProfiles_StateDir_Preserved(t *testing.T) {
	cfg := newTestWorkspace()
	cfg.StateDir = "/tmp/custom"
	got := cfg.FilterByProfiles([]string{"silero"})
	if got.StateDir != "/tmp/custom" {
		t.Errorf("state_dir: got %q, want /tmp/custom", got.StateDir)
	}
}

func TestFilterByProfiles_Profiles_Preserved(t *testing.T) {
	cfg := newTestWorkspace()
	got := cfg.FilterByProfiles([]string{"silero"})
	if len(got.Profiles) != len(cfg.Profiles) {
		t.Errorf("profiles map not preserved: got %d entries, want %d", len(got.Profiles), len(cfg.Profiles))
	}
}

func TestLoadWorkspace_ProfilesSection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "workspace.yaml")
	content := `
state_dir: /tmp/test
profiles:
  vad-plugins:
    silero:
      command: python3
      args: ["-m", "vad.main"]
      working_directory: "."
      restart: on-failure
      startup_delay_ms: 500
processes:
  - name: vad-plugins
    profiles: [silero]
  - name: core
    command: ./bin/core
    restart: on-failure
    startup_delay_ms: 1000
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write workspace: %v", err)
	}
	cfg, err := LoadWorkspace(path)
	if err != nil {
		t.Fatalf("LoadWorkspace: %v", err)
	}
	if len(cfg.Profiles) != 1 {
		t.Fatalf("profiles top-level categories: got %d, want 1", len(cfg.Profiles))
	}
	vadProfiles, ok := cfg.Profiles["vad-plugins"]
	if !ok {
		t.Fatal("vad-plugins category not found")
	}
	silero, ok := vadProfiles["silero"]
	if !ok {
		t.Fatal("silero profile not found under vad-plugins")
	}
	if silero.StartupDelayMs != 500 {
		t.Errorf("startup_delay_ms: got %d, want 500", silero.StartupDelayMs)
	}
	// Filter and verify expansion.
	expanded := cfg.FilterByProfiles([]string{"silero"})
	if !equalSlices(procNames(expanded), []string{"silero", "core"}) {
		t.Errorf("expanded: got %v", procNames(expanded))
	}
}
