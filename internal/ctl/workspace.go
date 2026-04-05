// Package ctl implements the speechmux-core ctl subcommand:
// a lightweight process manager for VAD and STT plugin processes.
//
// Usage:
//
//	speechmux-core ctl start   --workspace config/workspace.yaml
//	speechmux-core ctl status  --workspace config/workspace.yaml
//	speechmux-core ctl stop    --workspace config/workspace.yaml
package ctl

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// RestartPolicy controls when a supervised process is restarted after it exits.
type RestartPolicy string

const (
	// RestartAlways restarts the process on every exit (clean or error).
	RestartAlways RestartPolicy = "always"
	// RestartOnFailure restarts only when the process exits with a non-zero status.
	RestartOnFailure RestartPolicy = "on-failure"
	// RestartNever does not restart the process after it exits.
	RestartNever RestartPolicy = "never"
)

// WorkspaceConfig is the top-level configuration loaded from workspace.yaml.
type WorkspaceConfig struct {
	// StateDir is the directory where PID files are written at runtime.
	// Defaults to /tmp/speechmux when empty.
	StateDir  string          `yaml:"state_dir"`
	Processes []ProcessConfig `yaml:"processes"`
}

// ProcessConfig describes a single managed subprocess.
type ProcessConfig struct {
	// Name is a unique identifier for this process (used in status output and PID files).
	Name string `yaml:"name"`
	// Command is the executable to run (e.g. "python").
	Command string `yaml:"command"`
	// Args are the command-line arguments passed to Command.
	Args []string `yaml:"args"`
	// WorkingDirectory is the directory in which the process runs.
	// Relative paths are resolved from the working directory of speechmux-core.
	WorkingDirectory string `yaml:"working_directory"`
	// Restart controls the restart policy after the process exits.
	Restart RestartPolicy `yaml:"restart"`
	// StartupDelayMs is the number of milliseconds to wait after this process
	// starts before launching the next one in the list.
	StartupDelayMs int `yaml:"startup_delay_ms"`
}

// StartupDelay returns the configured startup delay as a time.Duration.
func (p *ProcessConfig) StartupDelay() time.Duration {
	return time.Duration(p.StartupDelayMs) * time.Millisecond
}

// LoadWorkspace reads and parses a workspace YAML file.
// Returns an error if the file cannot be read or parsed.
func LoadWorkspace(path string) (*WorkspaceConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ctl: read workspace %q: %w", path, err)
	}
	var cfg WorkspaceConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("ctl: parse workspace %q: %w", path, err)
	}
	if cfg.StateDir == "" {
		cfg.StateDir = "/tmp/speechmux"
	}
	return &cfg, nil
}
