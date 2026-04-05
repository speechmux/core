package stream

// FrameAggregator accumulates PCM S16LE bytes into fixed-size frames.
// This reduces IPC overhead when the client sends smaller chunks than the
// VAD plugin's optimal_frame_ms requires.
type FrameAggregator struct {
	targetBytes int    // bytes per aggregated frame (samples * 2)
	buf         []byte // accumulation buffer
}

// NewFrameAggregator creates a FrameAggregator targeting frames of targetMs
// milliseconds at the given PCM S16LE mono sample rate.
func NewFrameAggregator(targetMs, sampleRate int) *FrameAggregator {
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	if targetMs <= 0 {
		targetMs = 30
	}
	samples := (sampleRate * targetMs) / 1000
	return &FrameAggregator{targetBytes: samples * 2}
}

// Push appends pcm to the internal buffer and returns all complete frames
// that have been assembled. Each returned slice is an independent copy.
func (a *FrameAggregator) Push(pcm []byte) [][]byte {
	a.buf = append(a.buf, pcm...)

	var frames [][]byte
	for len(a.buf) >= a.targetBytes {
		frame := make([]byte, a.targetBytes)
		copy(frame, a.buf[:a.targetBytes])
		frames = append(frames, frame)
		a.buf = a.buf[a.targetBytes:]
	}
	return frames
}

// Flush returns any remaining buffered bytes as a final (possibly partial)
// frame and resets the internal buffer. Returns nil if the buffer is empty.
func (a *FrameAggregator) Flush() []byte {
	if len(a.buf) == 0 {
		return nil
	}
	out := make([]byte, len(a.buf))
	copy(out, a.buf)
	a.buf = a.buf[:0]
	return out
}

// BufferedBytes returns the number of bytes currently held in the buffer.
func (a *FrameAggregator) BufferedBytes() int {
	return len(a.buf)
}
