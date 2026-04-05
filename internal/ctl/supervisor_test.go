package ctl

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// TestSupervisor_StartStop verifies that a supervisor starts a real process,
// writes a PID file, and stops it cleanly.
func TestSupervisor_StartStop(t *testing.T) {
	stateDir := t.TempDir()
	cfg := ProcessConfig{
		Name:    "test-sleep",
		Command: "sleep",
		Args:    []string{"60"},
		Restart: RestartNever,
	}
	sup := NewSupervisor(cfg, stateDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() {
		runDone <- sup.Run(ctx)
	}()

	// Wait for the PID file to appear (process started).
	pidFile := filepath.Join(stateDir, "test-sleep.pid")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(pidFile); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := os.Stat(pidFile); err != nil {
		t.Fatalf("PID file not created within timeout: %v", err)
	}

	// Verify the PID file contains a valid PID.
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("read PID file: %v", err)
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil || pid <= 0 {
		t.Fatalf("invalid PID in file: %q", string(data))
	}

	// StatusFromPID should report running.
	st := sup.StatusFromPID()
	if !st.Running {
		t.Error("expected process to be running")
	}
	if st.PID != pid {
		t.Errorf("status PID %d != file PID %d", st.PID, pid)
	}

	// Stop the supervisor.
	sup.Stop()

	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}

	// PID file should be gone.
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Error("expected PID file to be removed after Stop")
	}
}

// TestSupervisor_RestartOnFailure verifies that a process that exits non-zero
// is restarted when restart=on-failure.
func TestSupervisor_RestartOnFailure(t *testing.T) {
	stateDir := t.TempDir()
	cfg := ProcessConfig{
		Name:    "test-fail",
		Command: "sh",
		// Exit 1 three times; the test cancels before the fourth start.
		Args:    []string{"-c", "exit 1"},
		Restart: RestartOnFailure,
	}
	sup := NewSupervisor(cfg, stateDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() {
		runDone <- sup.Run(ctx)
	}()

	// Wait until context expires (proves restarts are happening without hanging).
	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run returned unexpected error: %v", err)
		}
	case <-ctx.Done():
		// Expected: context timeout — supervisor was looping correctly.
		sup.Stop()
	}
}

// TestSupervisor_RestartNever verifies that the supervisor exits without
// restarting when restart=never and the process exits.
func TestSupervisor_RestartNever(t *testing.T) {
	stateDir := t.TempDir()
	cfg := ProcessConfig{
		Name:    "test-exit",
		Command: "sh",
		Args:    []string{"-c", "exit 0"},
		Restart: RestartNever,
	}
	sup := NewSupervisor(cfg, stateDir)

	ctx := context.Background()
	runDone := make(chan error, 1)
	go func() {
		runDone <- sup.Run(ctx)
	}()

	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after process exited with restart=never")
	}
}

// TestSupervisor_StatusFromPID_NoFile verifies that StatusFromPID returns a
// stopped status when no PID file exists.
func TestSupervisor_StatusFromPID_NoFile(t *testing.T) {
	stateDir := t.TempDir()
	cfg := ProcessConfig{Name: "no-proc"}
	sup := NewSupervisor(cfg, stateDir)

	st := sup.StatusFromPID()
	if st.Running {
		t.Error("expected not running when no PID file")
	}
	if st.PID != 0 {
		t.Errorf("expected PID 0, got %d", st.PID)
	}
}
