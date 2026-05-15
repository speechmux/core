// Package ctl implements the speechmux-core ctl subcommand:
// a lightweight process manager for VAD and STT plugin processes.
//
// Usage:
//
//	speechmux-core ctl start   --workspace workspace.yaml
//	speechmux-core ctl status  --workspace workspace.yaml
//	speechmux-core ctl stop    --workspace workspace.yaml
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

// ProfileConfig defines the executable and runtime settings for a named plugin profile.
// Profiles are declared in the top-level profiles: section of workspace.yaml and
// instantiated by ProcessConfig slot entries via their profiles: list.
type ProfileConfig struct {
	// Command is the executable to run (e.g. ".venv/bin/python3").
	Command string `yaml:"command"`
	// Args are the command-line arguments passed to Command.
	Args []string `yaml:"args"`
	// WorkingDirectory is the directory in which the process runs.
	WorkingDirectory string `yaml:"working_directory"`
	// Restart controls the restart policy after the process exits.
	Restart RestartPolicy `yaml:"restart"`
	// StartupDelayMs is the number of milliseconds to wait after this process
	// starts before launching the next one in the list.
	StartupDelayMs int `yaml:"startup_delay_ms"`
}

// WorkspaceConfig is the top-level configuration loaded from workspace.yaml.
type WorkspaceConfig struct {
	// StateDir is the directory where PID files are written at runtime.
	// Defaults to /tmp/speechmux when empty.
	StateDir string `yaml:"state_dir"`
	// Profiles is a two-level map: category name → engine name → ProfileConfig.
	// The category name must match the Name field of the corresponding slot entry
	// in Processes. Example:
	//   profiles:
	//     vad-plugins:
	//       silero: { command: ..., args: [...] }
	//     stt-plugins:
	//       sherpa-onnx: { command: ..., args: [...] }
	//       mlx-whisper: { command: ..., args: [...] }
	Profiles map[string]map[string]ProfileConfig `yaml:"profiles"`
	// Processes is the ordered list of process groups and direct process entries.
	Processes []ProcessConfig `yaml:"processes"`
}

// ProcessConfig describes a managed subprocess.
//
// There are two modes:
//
//  1. Slot entry (Profiles non-empty): this entry is a named group of plugin profiles.
//     FilterByProfiles expands each active profile in the list into a concrete process
//     using its definition from WorkspaceConfig.Profiles. Command and Args are ignored.
//
//  2. Direct entry (Profiles empty): this entry is always included regardless of active
//     profiles and runs the Command/Args defined here (e.g. the core process).
type ProcessConfig struct {
	// Name is a unique identifier for this process or slot (used in status output and PID files).
	Name string `yaml:"name"`
	// Profiles lists the individual engine profiles that this slot can instantiate.
	// Leave empty to make this a direct always-on process entry.
	Profiles []string `yaml:"profiles"`
	// Command is the executable to run. Used only for direct entries (Profiles empty).
	Command string `yaml:"command"`
	// Args are the command-line arguments passed to Command.
	Args []string `yaml:"args"`
	// WorkingDirectory is the directory in which the process runs.
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

// FilterByProfiles returns a new WorkspaceConfig with all ProcessConfig entries
// resolved to concrete (runnable) processes.
//
// Resolution rules:
//   - Direct entries (Profiles empty) are always included unchanged.
//   - Slot entries (Profiles non-empty) are expanded: each profile name in the slot
//     that appears in activeProfiles is looked up in WorkspaceConfig.Profiles and
//     instantiated as a concrete ProcessConfig using the profile's settings.
//   - When activeProfiles is empty, all profiles in every slot are instantiated
//     (equivalent to activating every engine).
//   - Profile names not defined in WorkspaceConfig.Profiles are silently skipped.
func (c *WorkspaceConfig) FilterByProfiles(activeProfiles []string) *WorkspaceConfig {
	var active map[string]struct{}
	if len(activeProfiles) > 0 {
		active = make(map[string]struct{}, len(activeProfiles))
		for _, p := range activeProfiles {
			active[p] = struct{}{}
		}
	}
	return c.expand(active)
}

// expand builds a concrete process list. active == nil means "include all profiles".
// For slot entries, profiles are looked up via c.Profiles[proc.Name][profileName],
// so the process Name must match the category key in the profiles: section.
func (c *WorkspaceConfig) expand(active map[string]struct{}) *WorkspaceConfig {
	result := &WorkspaceConfig{StateDir: c.StateDir, Profiles: c.Profiles}
	for _, proc := range c.Processes {
		if len(proc.Profiles) == 0 {
			// Direct entry — always included.
			result.Processes = append(result.Processes, proc)
			continue
		}
		// Slot entry — look up this slot's category in the profiles map.
		category := c.Profiles[proc.Name] // map[engineName]ProfileConfig; may be nil
		for _, profileName := range proc.Profiles {
			if active != nil {
				if _, ok := active[profileName]; !ok {
					continue
				}
			}
			if category == nil {
				continue
			}
			cfg, defined := category[profileName]
			if !defined {
				continue
			}
			result.Processes = append(result.Processes, ProcessConfig{
				Name:             profileName,
				Command:          cfg.Command,
				Args:             cfg.Args,
				WorkingDirectory: cfg.WorkingDirectory,
				Restart:          cfg.Restart,
				StartupDelayMs:   cfg.StartupDelayMs,
			})
		}
	}
	return result
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
