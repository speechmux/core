package ctl

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// ProcessStatus is a snapshot of a supervised process's state.
type ProcessStatus struct {
	Name    string
	PID     int
	Running bool
}

// Supervisor manages the lifecycle of a single subprocess.
// It starts the process, monitors it, and restarts it according to the
// configured RestartPolicy. A PID file is written to StateDir so that
// the status and stop commands can inspect it without the supervisor running.
type Supervisor struct {
	cfg      ProcessConfig
	stateDir string

	mu          sync.Mutex
	cmd         *exec.Cmd     // current running command; nil when idle
	lastPID     int           // PID of the most recently started process
	running     bool          // true while the process is alive
	processDone chan struct{}  // closed by Run() after cmd.Wait() returns; nil when idle
	stopCh      chan struct{}  // closed by Stop() to request shutdown
}

// NewSupervisor creates a Supervisor for the given process configuration.
func NewSupervisor(cfg ProcessConfig, stateDir string) *Supervisor {
	return &Supervisor{
		cfg:      cfg,
		stateDir: stateDir,
		stopCh:   make(chan struct{}),
	}
}

// Run starts the process and blocks until ctx is cancelled or Stop is called.
// It respects the RestartPolicy and applies exponential backoff between retries.
func (s *Supervisor) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		if s.isStopped() || ctx.Err() != nil {
			return nil
		}

		cmd := exec.Command(s.cfg.Command, s.cfg.Args...) //nolint:gosec
		if s.cfg.WorkingDirectory != "" {
			cmd.Dir = s.cfg.WorkingDirectory
		}
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		// Put the child in its own process group so SIGTERM reaches it even
		// if the parent's process group has changed.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		if err := cmd.Start(); err != nil {
			slog.Error("process start failed", "name", s.cfg.Name, "err", err)
			if s.cfg.Restart == RestartNever {
				return fmt.Errorf("ctl: %s: start: %w", s.cfg.Name, err)
			}
			if !s.sleepBackoff(ctx, &backoff) {
				return nil
			}
			continue
		}

		// processDone is closed when cmd.Wait() returns below.
		// Stop() and the watcher goroutine read it via the mutex.
		processDone := make(chan struct{})
		s.mu.Lock()
		s.cmd = cmd
		s.lastPID = cmd.Process.Pid
		s.running = true
		s.processDone = processDone
		s.mu.Unlock()

		_ = s.writePID(cmd.Process.Pid)
		slog.Info("process started", "name", s.cfg.Name, "pid", cmd.Process.Pid)

		// Watcher: terminates the process when ctx or stopCh fires.
		// It is the only goroutine that signals the process; Run() only
		// calls cmd.Wait(), so there is no double-Wait race.
		go s.watchAndKill(cmd, processDone, ctx)

		waitErr := cmd.Wait()
		close(processDone)

		s.mu.Lock()
		s.cmd = nil
		s.running = false
		s.processDone = nil
		s.mu.Unlock()

		_ = s.removePID()
		backoff = time.Second // reset after a successful start+run cycle

		if waitErr != nil {
			slog.Warn("process exited with error", "name", s.cfg.Name, "pid", cmd.Process.Pid, "err", waitErr)
		} else {
			slog.Info("process exited", "name", s.cfg.Name, "pid", cmd.Process.Pid)
		}

		if s.isStopped() || ctx.Err() != nil {
			return nil
		}

		switch s.cfg.Restart {
		case RestartNever:
			return nil
		case RestartOnFailure:
			if waitErr == nil {
				return nil // clean exit — do not restart
			}
		case RestartAlways:
			// fall through to restart
		}

		slog.Info("restarting process", "name", s.cfg.Name, "delay", backoff)
		if !s.sleepBackoff(ctx, &backoff) {
			return nil
		}
		backoff = time.Second
	}
}

// watchAndKill is the per-process goroutine that signals the process when
// ctx is cancelled or Stop() is called. It is the sole caller of
// Process.Signal / Kill for the given cmd, eliminating races with cmd.Wait().
func (s *Supervisor) watchAndKill(cmd *exec.Cmd, processDone <-chan struct{}, ctx context.Context) {
	select {
	case <-processDone:
		return // process already exited — nothing to do
	case <-ctx.Done():
	case <-s.stopCh:
	}

	// Send SIGTERM to the process group.
	pid := cmd.Process.Pid
	if pgid, err := syscall.Getpgid(pid); err == nil {
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
	} else {
		_ = cmd.Process.Signal(syscall.SIGTERM)
	}

	// Give the process a grace period, then SIGKILL.
	select {
	case <-processDone:
	case <-time.After(5 * time.Second):
		if pgid, err := syscall.Getpgid(pid); err == nil {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		} else {
			_ = cmd.Process.Kill()
		}
	}
}

// Stop closes the stop channel, which causes the watcher goroutine to
// terminate the current process, and blocks until the process has exited.
func (s *Supervisor) Stop() {
	select {
	case <-s.stopCh:
		// already closed
	default:
		close(s.stopCh)
	}

	// Wait for cmd.Wait() in Run() to return.
	s.mu.Lock()
	done := s.processDone
	s.mu.Unlock()

	if done != nil {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			// Last-resort kill if the watcher goroutine somehow stalled.
			s.mu.Lock()
			cmd := s.cmd
			s.mu.Unlock()
			if cmd != nil && cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
		}
	}
}

// StatusFromPID returns the status of this process by reading its PID file.
// It does not require the supervisor's Run loop to be active.
func (s *Supervisor) StatusFromPID() ProcessStatus {
	pid := s.readPID()
	running := pid > 0 && isAlive(pid)
	return ProcessStatus{Name: s.cfg.Name, PID: pid, Running: running}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func (s *Supervisor) isStopped() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

// sleepBackoff waits for the current backoff duration, doubles it (cap 30s),
// and returns false if ctx or stopCh fires first.
func (s *Supervisor) sleepBackoff(ctx context.Context, backoff *time.Duration) bool {
	select {
	case <-time.After(*backoff):
		*backoff = min(*backoff*2, 30*time.Second)
		return true
	case <-ctx.Done():
		return false
	case <-s.stopCh:
		return false
	}
}

func (s *Supervisor) pidFilePath() string {
	return filepath.Join(s.stateDir, s.cfg.Name+".pid")
}

func (s *Supervisor) writePID(pid int) error {
	if err := os.MkdirAll(s.stateDir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(s.pidFilePath(), []byte(strconv.Itoa(pid)), 0o644)
}

func (s *Supervisor) removePID() error {
	return os.Remove(s.pidFilePath())
}

func (s *Supervisor) readPID() int {
	data, err := os.ReadFile(s.pidFilePath())
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return 0
	}
	return pid
}

// isAlive returns true if the process with the given PID is currently running.
func isAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// Signal(0) checks process existence without delivering a real signal.
	return proc.Signal(syscall.Signal(0)) == nil
}
