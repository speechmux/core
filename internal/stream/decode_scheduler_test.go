package stream

import (
	"context"
	"errors"
	"testing"
	"time"

	sttErrors "github.com/speechmux/core/internal/errors"
	"github.com/speechmux/core/internal/plugin"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
)

// newTestRouter returns a PluginRouter with no real endpoints (all
// IsHealthy checks will fail immediately). Used to test the router-error path.
func newEmptyRouter() *plugin.PluginRouter {
	return plugin.NewPluginRouter("")
}

// ── Route error propagates as ErrAllPluginsUnavailable ─────────────────────

func TestDecodeScheduler_NoEndpoints_ReturnsAllPluginsUnavailable(t *testing.T) {
	scheduler := NewDecodeScheduler(newEmptyRouter(), 0, 0, 10.0, nil)
	ctx := context.Background()

	_, err := scheduler.Submit(ctx, "s1", "r1", []byte{0, 0}, 16000, "en",
		inferencepb.Task_TASK_TRANSCRIBE, nil, true, false)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var sttErr *sttErrors.STTError
	if !asSTTError(err, &sttErr) {
		t.Fatalf("expected STTError, got %T: %v", err, err)
	}
	if sttErr.Code() != sttErrors.ErrAllPluginsUnavailable {
		t.Errorf("want ErrAllPluginsUnavailable, got %v", sttErr.Code())
	}
}

// ── Global pending semaphore: final blocks, partial drops ──────────────────

func TestDecodeScheduler_PendingFull_PartialDropped(t *testing.T) {
	// maxPending = 0 immediately saturates.  Fill with the non-blocking path.
	scheduler := NewDecodeScheduler(newEmptyRouter(), 1, 0, 10.0, nil)
	// Fill the slot manually.
	scheduler.pending <- struct{}{}

	ctx := context.Background()
	_, err := scheduler.Submit(ctx, "s1", "r1", []byte{0, 0}, 16000, "en",
		inferencepb.Task_TASK_TRANSCRIBE, nil, false, true) // isPartial=true

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var sttErr *sttErrors.STTError
	if !asSTTError(err, &sttErr) {
		t.Fatalf("expected STTError, got %T: %v", err, err)
	}
	if sttErr.Code() != sttErrors.ErrGlobalPendingExceeded {
		t.Errorf("want ErrGlobalPendingExceeded, got %v", sttErr.Code())
	}
	// Drain the slot we filled manually.
	<-scheduler.pending
}

func TestDecodeScheduler_PendingFull_FinalTimesOut(t *testing.T) {
	scheduler := NewDecodeScheduler(newEmptyRouter(), 1, 0, 10.0, nil)
	// Shorten maxDecodeWait to avoid a slow test.
	scheduler.maxDecodeWait = 50 * time.Millisecond
	// Fill the slot manually.
	scheduler.pending <- struct{}{}

	ctx := context.Background()
	start := time.Now()
	_, err := scheduler.Submit(ctx, "s1", "r1", []byte{0, 0}, 16000, "en",
		inferencepb.Task_TASK_TRANSCRIBE, nil, true, false) // isFinal=true

	elapsed := time.Since(start)
	if elapsed < 40*time.Millisecond {
		t.Errorf("expected to block ~50ms, returned in %v", elapsed)
	}
	var sttErr *sttErrors.STTError
	if !asSTTError(err, &sttErr) {
		t.Fatalf("expected STTError, got %T: %v", err, err)
	}
	if sttErr.Code() != sttErrors.ErrGlobalPendingExceeded {
		t.Errorf("want ErrGlobalPendingExceeded, got %v", sttErr.Code())
	}
	<-scheduler.pending
}

// ── Context cancellation releases waiting goroutine ────────────────────────

func TestDecodeScheduler_CtxCancelledWhileWaiting(t *testing.T) {
	scheduler := NewDecodeScheduler(newEmptyRouter(), 1, 0, 10.0, nil)
	scheduler.pending <- struct{}{} // fill slot

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := scheduler.Submit(ctx, "s1", "r1", []byte{0, 0}, 16000, "en",
			inferencepb.Task_TASK_TRANSCRIBE, nil, true, false)
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("want context.Canceled, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("Submit did not return after context cancellation")
	}
	<-scheduler.pending
}

// ── helpers ──────────────────────────────────────────────────────────────────

func asSTTError(err error, target **sttErrors.STTError) bool {
	return errors.As(err, target)
}
