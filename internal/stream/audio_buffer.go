// Package stream implements the audio processing pipeline for SpeechMux Core.
package stream

import (
	"sync"
	"time"
)

// AudioRingBuffer stores audio frames indexed by sequence number.
// It implements watermark-based trimming to guarantee that audio not yet
// acknowledged by the VAD plugin is never discarded regardless of age.
//
// Backpressure: Append returns false when the buffer is at capacity.
// The caller must not advance until space becomes available (BATCH mode)
// or must drop the oldest entry (REALTIME mode) via DropOldest.
type AudioRingBuffer struct {
	mu                 sync.Mutex
	entries            []audioEntry
	head               int     // index of the oldest entry
	size               int     // number of occupied entries
	maxEntries         int     // total capacity; Append returns false when full
	maxSec             float64 // maximum age for confirmed entries
	confirmedWatermark uint64  // highest sequence number VAD has acknowledged
}

type audioEntry struct {
	seq       uint64
	data      []byte
	timestamp time.Time
}

// NewAudioRingBuffer creates an AudioRingBuffer.
// maxSec controls how long confirmed audio is kept before Trim removes it.
// Unconfirmed audio (seq > confirmedWatermark) is never trimmed regardless of age.
func NewAudioRingBuffer(maxSec float64) *AudioRingBuffer {
	maxEntries := int(maxSec * 100)
	if maxEntries < 200 {
		maxEntries = 200
	}
	return &AudioRingBuffer{
		entries:    make([]audioEntry, maxEntries),
		maxEntries: maxEntries,
		maxSec:     maxSec,
	}
}

// Append adds a new audio chunk with the given sequence number.
// Returns false if the buffer is at capacity (backpressure signal).
func (b *AudioRingBuffer) Append(seq uint64, data []byte) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.size >= b.maxEntries {
		return false
	}
	idx := (b.head + b.size) % b.maxEntries
	b.entries[idx] = audioEntry{seq: seq, data: data, timestamp: time.Now()}
	b.size++
	return true
}

// DropOldest removes the oldest entry unconditionally.
// Used in REALTIME mode when the buffer is full and blocking is not acceptable.
func (b *AudioRingBuffer) DropOldest() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.size > 0 {
		b.head = (b.head + 1) % b.maxEntries
		b.size--
	}
}

// AdvanceWatermark updates the confirmed watermark to seq.
// Only advances monotonically; a lower seq is silently ignored.
func (b *AudioRingBuffer) AdvanceWatermark(seq uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if seq > b.confirmedWatermark {
		b.confirmedWatermark = seq
	}
}

// ExtractRange returns the concatenated audio data for all entries with
// sequence numbers in [startSeq, endSeq], in ascending sequence order.
func (b *AudioRingBuffer) ExtractRange(startSeq, endSeq uint64) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	var totalBytes int
	for i := 0; i < b.size; i++ {
		e := &b.entries[(b.head+i)%b.maxEntries]
		if e.seq >= startSeq && e.seq <= endSeq {
			totalBytes += len(e.data)
		}
	}
	if totalBytes == 0 {
		return nil
	}

	out := make([]byte, 0, totalBytes)
	for i := 0; i < b.size; i++ {
		e := &b.entries[(b.head+i)%b.maxEntries]
		if e.seq >= startSeq && e.seq <= endSeq {
			out = append(out, e.data...)
		}
	}
	return out
}

// Trim removes entries that satisfy both conditions:
//  1. seq <= confirmedWatermark (VAD has acknowledged this frame).
//  2. timestamp is older than maxSec.
//
// Entries beyond the confirmed watermark are never removed regardless of age.
func (b *AudioRingBuffer) Trim() {
	b.mu.Lock()
	defer b.mu.Unlock()

	cutoff := time.Now().Add(-time.Duration(float64(time.Second) * b.maxSec))
	for b.size > 0 {
		e := &b.entries[b.head]
		if e.seq <= b.confirmedWatermark && e.timestamp.Before(cutoff) {
			b.head = (b.head + 1) % b.maxEntries
			b.size--
		} else {
			break
		}
	}
}

// Size returns the current number of buffered entries.
func (b *AudioRingBuffer) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}

// ConfirmedWatermark returns the current confirmed watermark value.
func (b *AudioRingBuffer) ConfirmedWatermark() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.confirmedWatermark
}

// LatestSequence returns the sequence number of the most recently appended entry.
// Returns 0 if the buffer is empty.
func (b *AudioRingBuffer) LatestSequence() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.size == 0 {
		return 0
	}
	idx := (b.head + b.size - 1) % b.maxEntries
	return b.entries[idx].seq
}
