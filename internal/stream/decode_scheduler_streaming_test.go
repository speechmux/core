package stream

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestScheduler_AcquireStreamingSlot_Unlimited(t *testing.T) {
	s := NewDecodeScheduler(newEmptyRouter(), 0, 0, 10.0, nil)

	for range 100 {
		release, err := s.AcquireStreamingSlot(context.Background())
		if err != nil {
			t.Fatalf("AcquireStreamingSlot: %v", err)
		}
		release()
	}
}

func TestScheduler_AcquireStreamingSlot_BlocksWhenFull(t *testing.T) {
	s := NewDecodeScheduler(newEmptyRouter(), 0, 2, 10.0, nil)

	r1, err := s.AcquireStreamingSlot(context.Background())
	if err != nil {
		t.Fatalf("slot 1: %v", err)
	}
	r2, err := s.AcquireStreamingSlot(context.Background())
	if err != nil {
		t.Fatalf("slot 2: %v", err)
	}

	// Third acquire should block until a slot is released.
	acquired := make(chan struct{})
	go func() {
		r3, err := s.AcquireStreamingSlot(context.Background())
		if err == nil {
			r3()
		}
		close(acquired)
	}()

	// Ensure the goroutine is blocked.
	select {
	case <-acquired:
		t.Fatal("third acquire should have blocked")
	case <-time.After(30 * time.Millisecond):
	}

	// Release one slot — third acquire should unblock.
	r1()
	select {
	case <-acquired:
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("third acquire did not unblock after release")
	}

	r2()
}

func TestScheduler_AcquireStreamingSlot_CtxCancel(t *testing.T) {
	s := NewDecodeScheduler(newEmptyRouter(), 0, 1, 10.0, nil)

	r1, err := s.AcquireStreamingSlot(context.Background())
	if err != nil {
		t.Fatalf("slot 1: %v", err)
	}
	defer r1()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err = s.AcquireStreamingSlot(ctx)
	if err == nil {
		t.Fatal("expected error on cancelled context")
	}
}

func TestScheduler_AcquireStreamingSlot_ReleaseOnce(t *testing.T) {
	s := NewDecodeScheduler(newEmptyRouter(), 0, 1, 10.0, nil)

	release, err := s.AcquireStreamingSlot(context.Background())
	if err != nil {
		t.Fatalf("AcquireStreamingSlot: %v", err)
	}

	// Double release must not panic or deadlock.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); release() }()
	go func() { defer wg.Done(); release() }()
	wg.Wait()

	// Slot should be free after double release — a new acquire must succeed immediately.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	r2, err := s.AcquireStreamingSlot(ctx)
	if err != nil {
		t.Fatalf("slot should be free after double release: %v", err)
	}
	r2()
}
