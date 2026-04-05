package ctl

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testWorkspace(stateDir string) *WorkspaceConfig {
	return &WorkspaceConfig{
		StateDir: stateDir,
		Processes: []ProcessConfig{
			{
				Name:    "p1",
				Command: "sleep",
				Args:    []string{"60"},
				Restart: RestartNever,
			},
			{
				Name:    "p2",
				Command: "sleep",
				Args:    []string{"60"},
				Restart: RestartNever,
			},
		},
	}
}

// TestManager_StartStop verifies that Start launches all processes and Stop
// terminates the manager (via context cancellation), cleaning up PID files.
func TestManager_StartStop(t *testing.T) {
	stateDir := t.TempDir()
	cfg := testWorkspace(stateDir)
	mgr := NewManager(cfg)

	ctx, cancel := context.WithCancel(context.Background())

	startDone := make(chan error, 1)
	go func() {
		startDone <- mgr.Start(ctx)
	}()

	// Wait for both PID files.
	for _, name := range []string{"p1", "p2"} {
		pidFile := filepath.Join(stateDir, name+".pid")
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(pidFile); err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if _, err := os.Stat(pidFile); err != nil {
			t.Fatalf("PID file for %s not created: %v", name, err)
		}
	}

	// Status should report both running.
	statuses := mgr.Status()
	for _, s := range statuses {
		if !s.Running {
			t.Errorf("expected %s to be running", s.Name)
		}
	}

	// Cancel context to trigger graceful shutdown.
	cancel()

	select {
	case err := <-startDone:
		if err != nil {
			t.Errorf("Start returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}

// TestManager_Status_AllStopped verifies that Status reports stopped when no
// PID files exist.
func TestManager_Status_AllStopped(t *testing.T) {
	stateDir := t.TempDir()
	cfg := testWorkspace(stateDir)
	mgr := NewManager(cfg)

	statuses := mgr.Status()
	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(statuses))
	}
	for _, s := range statuses {
		if s.Running {
			t.Errorf("expected %s to be stopped", s.Name)
		}
	}
}

// TestManager_PrintStatus_NoPanic verifies PrintStatus does not panic.
func TestManager_PrintStatus_NoPanic(t *testing.T) {
	stateDir := t.TempDir()
	cfg := testWorkspace(stateDir)
	mgr := NewManager(cfg)
	mgr.PrintStatus() // must not panic
}
