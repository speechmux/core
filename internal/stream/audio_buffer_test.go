package stream

import (
	"testing"
	"time"
)

func TestAudioRingBuffer_AppendAndSize(t *testing.T) {
	buf := NewAudioRingBuffer(10)
	if got := buf.Size(); got != 0 {
		t.Fatalf("initial size = %d, want 0", got)
	}

	if !buf.Append(1, []byte("audio1")) {
		t.Fatal("first Append returned false")
	}
	if !buf.Append(2, []byte("audio2")) {
		t.Fatal("second Append returned false")
	}
	if got := buf.Size(); got != 2 {
		t.Fatalf("size = %d, want 2", got)
	}
}

func TestAudioRingBuffer_BackpressureWhenFull(t *testing.T) {
	// Use small maxSec so maxEntries is capped at 200.
	buf := &AudioRingBuffer{
		entries:    make([]audioEntry, 3),
		maxEntries: 3,
		maxSec:     1,
	}

	if !buf.Append(1, []byte("a")) {
		t.Fatal("Append 1 failed")
	}
	if !buf.Append(2, []byte("b")) {
		t.Fatal("Append 2 failed")
	}
	if !buf.Append(3, []byte("c")) {
		t.Fatal("Append 3 failed")
	}
	// Buffer is full — next Append must return false.
	if buf.Append(4, []byte("d")) {
		t.Fatal("expected Append to return false when full")
	}
}

func TestAudioRingBuffer_DropOldest(t *testing.T) {
	buf := &AudioRingBuffer{
		entries:    make([]audioEntry, 2),
		maxEntries: 2,
		maxSec:     1,
	}
	buf.Append(1, []byte("a"))
	buf.Append(2, []byte("b"))

	buf.DropOldest()

	if got := buf.Size(); got != 1 {
		t.Fatalf("size after DropOldest = %d, want 1", got)
	}
	// After dropping seq=1, seq=2 should still be available.
	got := buf.ExtractRange(2, 2)
	if string(got) != "b" {
		t.Fatalf("ExtractRange after DropOldest = %q, want %q", got, "b")
	}
}

func TestAudioRingBuffer_AdvanceWatermarkMonotonic(t *testing.T) {
	buf := NewAudioRingBuffer(10)
	buf.AdvanceWatermark(10)
	buf.AdvanceWatermark(5) // lower — must be ignored
	if got := buf.ConfirmedWatermark(); got != 10 {
		t.Fatalf("watermark = %d, want 10", got)
	}
}

func TestAudioRingBuffer_ExtractRange(t *testing.T) {
	buf := NewAudioRingBuffer(10)
	buf.Append(1, []byte("one"))
	buf.Append(2, []byte("two"))
	buf.Append(3, []byte("three"))
	buf.Append(4, []byte("four"))

	got := buf.ExtractRange(2, 3)
	want := "twothree"
	if string(got) != want {
		t.Fatalf("ExtractRange(2,3) = %q, want %q", got, want)
	}
}

func TestAudioRingBuffer_ExtractRangeEmpty(t *testing.T) {
	buf := NewAudioRingBuffer(10)
	buf.Append(1, []byte("x"))
	got := buf.ExtractRange(5, 10) // no entries in range
	if got != nil {
		t.Fatalf("ExtractRange out-of-range = %v, want nil", got)
	}
}

func TestAudioRingBuffer_TrimConfirmedAndAged(t *testing.T) {
	buf := &AudioRingBuffer{
		entries:    make([]audioEntry, 10),
		maxEntries: 10,
		maxSec:     0.001, // 1 ms — entries age out almost instantly
	}

	buf.Append(1, []byte("a"))
	buf.Append(2, []byte("b"))
	buf.Append(3, []byte("c"))

	buf.AdvanceWatermark(2)
	time.Sleep(5 * time.Millisecond) // let seq 1 and 2 age past 1 ms
	buf.Trim()

	// seq 1 and 2 are confirmed and aged: trimmed.
	// seq 3 is not confirmed: preserved.
	if got := buf.Size(); got != 1 {
		t.Fatalf("size after Trim = %d, want 1 (only seq 3 remains)", got)
	}
	got := buf.ExtractRange(3, 3)
	if string(got) != "c" {
		t.Fatalf("seq 3 after Trim = %q, want %q", got, "c")
	}
}

func TestAudioRingBuffer_TrimPreservesUnconfirmed(t *testing.T) {
	buf := &AudioRingBuffer{
		entries:    make([]audioEntry, 10),
		maxEntries: 10,
		maxSec:     0.001,
	}

	buf.Append(1, []byte("a"))
	buf.Append(2, []byte("b"))
	// No AdvanceWatermark — confirmedWatermark stays 0.
	time.Sleep(5 * time.Millisecond)
	buf.Trim()

	// Both entries are unconfirmed: must not be trimmed.
	if got := buf.Size(); got != 2 {
		t.Fatalf("size after Trim (no watermark) = %d, want 2", got)
	}
}

func TestAudioRingBuffer_WrapAround(t *testing.T) {
	buf := &AudioRingBuffer{
		entries:    make([]audioEntry, 4),
		maxEntries: 4,
		maxSec:     10,
	}

	// Fill, then drop oldest, fill again to force wrap-around.
	buf.Append(1, []byte("1"))
	buf.Append(2, []byte("2"))
	buf.Append(3, []byte("3"))
	buf.Append(4, []byte("4"))
	buf.DropOldest() // remove seq 1
	buf.Append(5, []byte("5"))

	got := buf.ExtractRange(2, 5)
	if string(got) != "2345" {
		t.Fatalf("wrap-around extract = %q, want %q", got, "2345")
	}
}

func TestAudioRingBuffer_LatestSequence(t *testing.T) {
	buf := NewAudioRingBuffer(10)

	if got := buf.LatestSequence(); got != 0 {
		t.Fatalf("empty buffer: LatestSequence = %d, want 0", got)
	}

	buf.Append(1, []byte("a"))
	if got := buf.LatestSequence(); got != 1 {
		t.Fatalf("LatestSequence = %d, want 1", got)
	}

	buf.Append(5, []byte("b"))
	buf.Append(9, []byte("c"))
	if got := buf.LatestSequence(); got != 9 {
		t.Fatalf("LatestSequence = %d, want 9", got)
	}

	// After DropOldest the latest should still be 9.
	buf.DropOldest()
	if got := buf.LatestSequence(); got != 9 {
		t.Fatalf("after DropOldest: LatestSequence = %d, want 9", got)
	}
}

func TestAudioRingBuffer_TrimByAge(t *testing.T) {
	buf := NewAudioRingBuffer(10)

	// Append 3 entries; they all have fresh timestamps.
	buf.Append(1, []byte("a"))
	buf.Append(2, []byte("b"))
	buf.Append(3, []byte("c"))

	// TrimByAge with a large maxSec should leave all entries intact.
	buf.TrimByAge(60)
	if got := buf.Size(); got != 3 {
		t.Fatalf("after large-maxSec TrimByAge: size = %d, want 3", got)
	}

	// TrimByAge ignores confirmedWatermark — even un-confirmed entries are removed.
	// Use maxSec=0 to trim everything that is older than "now".
	// Since entries were just appended they have timestamp ≈ now; use negative
	// maxSec to force them all into the past.
	buf.mu.Lock()
	for i := 0; i < buf.size; i++ {
		idx := (buf.head + i) % buf.maxEntries
		buf.entries[idx].timestamp = buf.entries[idx].timestamp.Add(-10 * time.Second)
	}
	buf.mu.Unlock()

	buf.TrimByAge(5) // entries are 10 s old; cutoff is 5 s
	if got := buf.Size(); got != 0 {
		t.Fatalf("after TrimByAge cutoff: size = %d, want 0", got)
	}

	// Watermark independence: append 2 un-confirmed entries, age them, TrimByAge removes them.
	buf2 := NewAudioRingBuffer(10)
	buf2.Append(10, []byte("x"))
	buf2.Append(11, []byte("y"))
	buf2.mu.Lock()
	for i := 0; i < buf2.size; i++ {
		idx := (buf2.head + i) % buf2.maxEntries
		buf2.entries[idx].timestamp = buf2.entries[idx].timestamp.Add(-10 * time.Second)
	}
	buf2.mu.Unlock()
	// No Watermark advance: confirmedWatermark is still 0.
	buf2.TrimByAge(5)
	if got := buf2.Size(); got != 0 {
		t.Fatalf("TrimByAge must trim un-confirmed entries; size = %d, want 0", got)
	}
}
