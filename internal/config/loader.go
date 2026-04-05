package config

import (
	"fmt"
	"os"
	"sync/atomic"

	"gopkg.in/yaml.v3"
)

// Loader holds a live Config pointer and reloads it from disk on demand.
type Loader struct {
	ptr     atomic.Pointer[Config]
	cfgPath string
}

// NewLoader reads the config file once and returns a ready Loader.
func NewLoader(path string) (*Loader, error) {
	l := &Loader{cfgPath: path}
	if err := l.reload(); err != nil {
		return nil, err
	}
	return l, nil
}

// Load returns the current config snapshot. Safe for concurrent use.
func (l *Loader) Load() *Config {
	return l.ptr.Load()
}

// Reload re-reads the config file and atomically replaces the active config.
// Callers may use this in response to SIGHUP.
func (l *Loader) Reload() error {
	return l.reload()
}

func (l *Loader) reload() error {
	cfg, err := loadFile(l.cfgPath)
	if err != nil {
		return err
	}
	l.ptr.Store(cfg)
	return nil
}

// loadFile reads and parses a YAML config file into a validated Config.
func loadFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse %q: %w", path, err)
	}
	cfg.Defaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: validate %q: %w", path, err)
	}
	return &cfg, nil
}

// LoadPlugins reads and parses config/plugins.yaml.
func LoadPlugins(path string) (*PluginsConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read plugins %q: %w", path, err)
	}
	var cfg PluginsConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse plugins %q: %w", path, err)
	}
	return &cfg, nil
}
