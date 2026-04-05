package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/speechmux/core/internal/config"
)

func newTestStorage(t *testing.T) (*AudioStorage, string) {
	t.Helper()
	dir := t.TempDir()
	enabled := true
	s := NewAudioStorage(config.StorageConfig{
		Enabled:   enabled,
		Directory: dir,
	})
	if s == nil {
		t.Fatal("NewAudioStorage returned nil for enabled storage")
	}
	return s, dir
}

// TestRecorder_WriteAndClose verifies that chunks written before Close are
// fully flushed to disk.
func TestRecorder_WriteAndClose(t *testing.T) {
	s, dir := newTestStorage(t)

	rec := s.NewRecorder("test-sess-001")
	if rec == nil {
		t.Fatal("NewRecorder returned nil")
	}

	data := []byte{0x01, 0x02, 0x03, 0x04}
	rec.Write(data)
	rec.Close()

	// Find the written file.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 file, got %d", len(entries))
	}

	content, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != string(data) {
		t.Errorf("file content mismatch: got %v want %v", content, data)
	}
}

// TestRecorder_MultipleWrites verifies that multiple chunks are concatenated.
func TestRecorder_MultipleWrites(t *testing.T) {
	s, dir := newTestStorage(t)
	rec := s.NewRecorder("multi-001")

	var expected []byte
	for i := byte(0); i < 10; i++ {
		chunk := []byte{i, i + 1}
		rec.Write(chunk)
		expected = append(expected, chunk...)
	}
	rec.Close()

	entries, _ := os.ReadDir(dir)
	if len(entries) == 0 {
		t.Fatal("no file written")
	}
	content, _ := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if string(content) != string(expected) {
		t.Errorf("content mismatch: got %d bytes, want %d bytes", len(content), len(expected))
	}
}

// TestRecorder_CloseIdempotent ensures Close() can be called multiple times safely.
func TestRecorder_CloseIdempotent(t *testing.T) {
	s, _ := newTestStorage(t)
	rec := s.NewRecorder("idempotent-001")
	rec.Close()
	rec.Close() // should not panic
}

// TestRecorder_NilSafe verifies that a nil recorder is safe to call.
func TestRecorder_NilSafe(t *testing.T) {
	var rec *SessionRecorder
	rec.Write([]byte{1, 2, 3}) // must not panic
	rec.Close()                 // must not panic
	if rec.DroppedChunks() != 0 {
		t.Error("nil recorder DroppedChunks should return 0")
	}
}

// TestRecorder_Drop verifies that chunks are dropped and counted when the
// write channel is full (simulates I/O delay).
func TestRecorder_Drop(t *testing.T) {
	s, dir := newTestStorage(t)
	rec := s.NewRecorder("drop-test-001")

	// Block the writer goroutine by filling the channel without reading.
	// writerChanSize = 256; write 256+50 chunks — at least 50 should be dropped.
	chunk := make([]byte, 2)
	for i := 0; i < writerChanSize+50; i++ {
		chunk[0] = byte(i)
		rec.Write(chunk)
	}

	// Drain; file is created regardless.
	rec.Close()

	if rec.DroppedChunks() == 0 {
		t.Log("no drops observed — writer was fast enough; test is non-deterministic")
	}
	// File must exist regardless of drops.
	entries, _ := os.ReadDir(dir)
	if len(entries) == 0 {
		t.Error("expected output file even with drops")
	}
}

// TestStorage_Disabled verifies that a disabled storage returns nil recorder.
func TestStorage_Disabled(t *testing.T) {
	s := NewAudioStorage(config.StorageConfig{Enabled: false})
	if s != nil {
		t.Fatal("expected nil for disabled storage")
	}
	// nil.NewRecorder should be safe via the nil-check in the method.
	var nilStorage *AudioStorage
	rec := nilStorage.NewRecorder("x")
	if rec != nil {
		t.Fatal("expected nil recorder from nil storage")
	}
}

// TestCleanup_MaxAgeDays evicts files older than the limit.
func TestCleanup_MaxAgeDays(t *testing.T) {
	dir := t.TempDir()
	maxAge := 7
	s := &AudioStorage{
		cfg: config.StorageConfig{
			Enabled:    true,
			Directory:  dir,
			MaxAgeDays: &maxAge,
		},
	}

	// Write an "old" file by backdating its mtime.
	oldFile := filepath.Join(dir, "old_file.pcm")
	if err := os.WriteFile(oldFile, []byte("audio"), 0644); err != nil {
		t.Fatal(err)
	}
	oldTime := time.Now().Add(-8 * 24 * time.Hour) // 8 days ago
	if err := os.Chtimes(oldFile, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	// Write a fresh file.
	freshFile := filepath.Join(dir, "fresh_file.pcm")
	if err := os.WriteFile(freshFile, []byte("audio"), 0644); err != nil {
		t.Fatal(err)
	}

	s.cleanup()

	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Error("old file should have been removed")
	}
	if _, err := os.Stat(freshFile); os.IsNotExist(err) {
		t.Error("fresh file should still exist")
	}
}

// TestCleanup_MaxFiles evicts the oldest file when the limit is exceeded.
func TestCleanup_MaxFiles(t *testing.T) {
	dir := t.TempDir()
	maxFiles := 2
	s := &AudioStorage{
		cfg: config.StorageConfig{
			Enabled:   true,
			Directory: dir,
			MaxFiles:  &maxFiles,
		},
	}

	// Write 3 files with distinct mtimes.
	for i, name := range []string{"a.pcm", "b.pcm", "c.pcm"} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
		mtime := time.Now().Add(time.Duration(i) * time.Second)
		_ = os.Chtimes(path, mtime, mtime)
	}

	s.cleanup()

	entries, _ := os.ReadDir(dir)
	if len(entries) != 2 {
		t.Errorf("expected 2 files after cleanup, got %d", len(entries))
	}
	// Oldest file (a.pcm) should be removed.
	if _, err := os.Stat(filepath.Join(dir, "a.pcm")); !os.IsNotExist(err) {
		t.Error("oldest file (a.pcm) should have been removed")
	}
}

// TestCleanup_MaxBytes evicts files until total size is within the limit.
func TestCleanup_MaxBytes(t *testing.T) {
	dir := t.TempDir()
	maxBytes := int64(10)
	s := &AudioStorage{
		cfg: config.StorageConfig{
			Enabled:  true,
			Directory: dir,
			MaxBytes: &maxBytes,
		},
	}

	// Write 3 files, each 6 bytes, with distinct mtimes.
	for i, name := range []string{"a.pcm", "b.pcm", "c.pcm"} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("aaaaaa"), 0644); err != nil {
			t.Fatal(err)
		}
		mtime := time.Now().Add(time.Duration(i) * time.Second)
		_ = os.Chtimes(path, mtime, mtime)
	}
	// Total = 18 bytes; limit = 10 bytes → must remove at least one 6-byte file.

	s.cleanup()

	entries, _ := os.ReadDir(dir)
	// Remaining total must be ≤ 10 bytes.
	var total int64
	for _, e := range entries {
		info, _ := e.Info()
		total += info.Size()
	}
	if total > maxBytes {
		t.Errorf("total size %d exceeds max_bytes %d after cleanup", total, maxBytes)
	}
}
