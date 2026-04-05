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
