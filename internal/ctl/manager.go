package ctl

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

// Manager orchestrates all Supervisors defined in a WorkspaceConfig.
// It is the entry point for the three ctl commands: Start, Stop, and Status.
type Manager struct {
	cfg         *WorkspaceConfig
	supervisors []*Supervisor
}

// NewManager creates a Manager from the given workspace config.
func NewManager(cfg *WorkspaceConfig) *Manager {
	supervisors := make([]*Supervisor, len(cfg.Processes))
	for i, p := range cfg.Processes {
		supervisors[i] = NewSupervisor(p, cfg.StateDir)
	}
	return &Manager{cfg: cfg, supervisors: supervisors}
}

// Start launches all processes in declaration order, writing a manager PID file
// so that Stop can find and terminate this process. It blocks until ctx is
// cancelled or a fatal error occurs.
func (m *Manager) Start(ctx context.Context) error {
	if err := os.MkdirAll(m.cfg.StateDir, 0o755); err != nil {
		return fmt.Errorf("ctl: create state_dir %q: %w", m.cfg.StateDir, err)
	}

	// Write own PID so `ctl stop` can send SIGTERM to the manager.
	if err := m.writeManagerPID(); err != nil {
		return err
	}
	defer func() { _ = m.removeManagerPID() }()

	g, gCtx := errgroup.WithContext(ctx)

	for i, sup := range m.supervisors {
		sup := sup // capture loop variable
		cfg := m.cfg.Processes[i]

		g.Go(func() error {
			return sup.Run(gCtx)
		})

		// Startup delay before launching the next process.
		if d := cfg.StartupDelay(); d > 0 {
			select {
			case <-time.After(d):
			case <-gCtx.Done():
				break
			}
		}
	}

	return g.Wait()
}

// Stop sends SIGTERM to the running manager process (the `ctl start` process)
// identified by the manager PID file. If no manager is running, it falls back
// to stopping each process individually via their PID files.
func (m *Manager) Stop() error {
	mgrPID := m.readManagerPID()
	if mgrPID > 0 && isAlive(mgrPID) {
		slog.Info("sending SIGTERM to ctl manager", "pid", mgrPID)
		proc, err := os.FindProcess(mgrPID)
		if err != nil {
			return fmt.Errorf("ctl: find manager process %d: %w", mgrPID, err)
		}
		if err := proc.Signal(syscall.SIGTERM); err != nil {
			return fmt.Errorf("ctl: signal manager process %d: %w", mgrPID, err)
		}
		// Wait up to 10 s for the manager to clean up, then stop plugins directly.
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if !isAlive(mgrPID) {
				slog.Info("ctl manager stopped")
				return nil
			}
			time.Sleep(200 * time.Millisecond)
		}
		slog.Warn("ctl manager did not stop in time; stopping plugins directly")
	}

	// Fallback: stop each plugin directly.
	return m.stopPluginsDirect()
}

// stopPluginsDirect stops each plugin process by reading its PID file and
// sending SIGTERM. Used when no manager process is running.
func (m *Manager) stopPluginsDirect() error {
	var firstErr error
	for _, sup := range m.supervisors {
		pid := sup.readPID()
		if pid <= 0 || !isAlive(pid) {
			continue
		}
		slog.Info("stopping plugin process", "name", sup.cfg.Name, "pid", pid)
		proc, err := os.FindProcess(pid)
		if err != nil {
			slog.Warn("find process failed", "name", sup.cfg.Name, "err", err)
			continue
		}
		if err := proc.Signal(syscall.SIGTERM); err != nil {
			slog.Warn("SIGTERM failed", "name", sup.cfg.Name, "err", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// Wait up to 5 s for graceful exit, then kill.
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if !isAlive(pid) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if isAlive(pid) {
			slog.Warn("process did not stop; sending SIGKILL", "name", sup.cfg.Name, "pid", pid)
			_ = proc.Signal(syscall.SIGKILL)
		}
		_ = sup.removePID()
	}
	return firstErr
}

// Status returns the current status of all configured processes by reading
// their PID files. It does not require the manager to be running.
func (m *Manager) Status() []ProcessStatus {
	statuses := make([]ProcessStatus, len(m.supervisors))
	for i, sup := range m.supervisors {
		statuses[i] = sup.StatusFromPID()
	}
	return statuses
}

// PrintStatus writes a human-readable status table to stdout.
func (m *Manager) PrintStatus() {
	statuses := m.Status()
	fmt.Printf("%-20s  %-8s  %s\n", "NAME", "PID", "STATUS")
	fmt.Printf("%-20s  %-8s  %s\n", "----", "---", "------")
	for _, s := range statuses {
		pidStr := "-"
		if s.PID > 0 {
			pidStr = strconv.Itoa(s.PID)
		}
		status := "stopped"
		if s.Running {
			status = "running"
		}
		fmt.Printf("%-20s  %-8s  %s\n", s.Name, pidStr, status)
	}
}

// ── manager PID file ──────────────────────────────────────────────────────────

func (m *Manager) managerPIDPath() string {
	return filepath.Join(m.cfg.StateDir, "ctl.pid")
}

func (m *Manager) writeManagerPID() error {
	return os.WriteFile(m.managerPIDPath(), []byte(strconv.Itoa(os.Getpid())), 0o644)
}

func (m *Manager) removeManagerPID() error {
	return os.Remove(m.managerPIDPath())
}

func (m *Manager) readManagerPID() int {
	data, err := os.ReadFile(m.managerPIDPath())
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return 0
	}
	return pid
}
