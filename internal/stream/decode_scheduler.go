package stream

import (
	"context"
	"sync"

	sttErrors "github.com/speechmux/core/internal/errors"
)

// DecodeScheduler manages concurrency for streaming inference sessions.
// The batch decode path is handled by FairDecodeDispatcher; this type is
// retained solely for AcquireStreamingSlot / ReleaseStreamingSlot.
type DecodeScheduler struct {
	streamingSlots chan struct{} // streaming-session semaphore; nil = unlimited
}

// NewDecodeScheduler creates a DecodeScheduler.
//
//   - maxStreaming: max concurrent streaming sessions; 0 = unlimited.
func NewDecodeScheduler(maxStreaming int) *DecodeScheduler {
	var streamingSlots chan struct{}
	if maxStreaming > 0 {
		streamingSlots = make(chan struct{}, maxStreaming)
	}
	return &DecodeScheduler{streamingSlots: streamingSlots}
}

// AcquireStreamingSlot blocks until a streaming session slot is free or ctx is
// cancelled. Returns a release func the caller MUST invoke exactly once on
// session end. When maxStreaming is 0 (unlimited), returns immediately.
func (s *DecodeScheduler) AcquireStreamingSlot(ctx context.Context) (func(), error) {
	if s.streamingSlots == nil {
		return func() {}, nil
	}
	select {
	case s.streamingSlots <- struct{}{}:
		once := sync.Once{}
		return func() {
			once.Do(func() { <-s.streamingSlots })
		}, nil
	case <-ctx.Done():
		return nil, sttErrors.New(sttErrors.ErrGlobalPendingExceeded,
			"streaming session slot unavailable: "+ctx.Err().Error())
	}
}
