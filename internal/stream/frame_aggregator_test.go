package stream

import (
	"testing"
)

// makePCM creates n bytes of zeroed PCM S16LE data.
func makePCM(n int) []byte {
	return make([]byte, n)
}

func TestFrameAggregator_ExactFrame(t *testing.T) {
	// 30 ms at 16000 Hz = 480 samples = 960 bytes.
	agg := NewFrameAggregator(30, 16000)
	pcm := makePCM(960)

	frames := agg.Push(pcm)
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	if len(frames[0]) != 960 {
		t.Fatalf("frame size = %d, want 960", len(frames[0]))
	}
}

func TestFrameAggregator_SmallChunksAccumulate(t *testing.T) {
	// 30 ms = 960 bytes; send 3 × 320-byte chunks.
	agg := NewFrameAggregator(30, 16000)

	if got := agg.Push(makePCM(320)); len(got) != 0 {
		t.Fatalf("push 320 bytes: expected 0 frames, got %d", len(got))
	}
	if got := agg.Push(makePCM(320)); len(got) != 0 {
		t.Fatalf("push 640 bytes: expected 0 frames, got %d", len(got))
	}
	frames := agg.Push(makePCM(320))
	if len(frames) != 1 {
		t.Fatalf("push 960 bytes: expected 1 frame, got %d", len(frames))
	}
	if len(frames[0]) != 960 {
		t.Fatalf("frame size = %d, want 960", len(frames[0]))
	}
}

func TestFrameAggregator_LargeChunkProducesMultipleFrames(t *testing.T) {
	// 20 ms at 16000 Hz = 320 samples = 640 bytes per frame.
	// Send 3 × 640 bytes at once.
	agg := NewFrameAggregator(20, 16000)
	frames := agg.Push(makePCM(640 * 3))

	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}
}

func TestFrameAggregator_RemainingBytesInBuffer(t *testing.T) {
	agg := NewFrameAggregator(30, 16000) // 960 bytes per frame
	agg.Push(makePCM(500))

	if got := agg.BufferedBytes(); got != 500 {
		t.Fatalf("buffered = %d, want 500", got)
	}
}

func TestFrameAggregator_Flush(t *testing.T) {
	agg := NewFrameAggregator(30, 16000)
	agg.Push(makePCM(200))

	tail := agg.Flush()
	if len(tail) != 200 {
		t.Fatalf("flush returned %d bytes, want 200", len(tail))
	}
	if agg.BufferedBytes() != 0 {
		t.Fatal("buffer should be empty after Flush")
	}
}

func TestFrameAggregator_FlushEmptyReturnsNil(t *testing.T) {
	agg := NewFrameAggregator(30, 16000)
	if got := agg.Flush(); got != nil {
		t.Fatalf("empty Flush returned %v, want nil", got)
	}
}

func TestFrameAggregator_FramesAreIndependentCopies(t *testing.T) {
	agg := NewFrameAggregator(20, 16000) // 640-byte frames
	src := make([]byte, 640)
	src[0] = 0xAB

	frames := agg.Push(src)
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}

	// Mutate the source — frame must be unaffected.
	src[0] = 0x00
	if frames[0][0] != 0xAB {
		t.Fatal("frame is not an independent copy of the source")
	}
}
