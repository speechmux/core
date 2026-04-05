// Package storage provides async, non-blocking audio recording for sessions.
// Writes are best-effort: I/O delays never block the streaming pipeline.
package storage

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/speechmux/core/internal/config"
)

const (
	// writerChanSize is the capacity of a SessionRecorder's async write channel.
	// At 16 kHz mono S16LE, 256 × 20 ms chunks = ~5 s of buffered audio.
	writerChanSize = 256
)

// AudioStorage manages the directory where recorded session audio is stored
// and enforces capacity limits (max_bytes, max_files, max_age_days).
//
// A nil *AudioStorage is valid and silently disables all recording.
type AudioStorage struct {
	cfg config.StorageConfig
}

// NewAudioStorage creates an AudioStorage backed by cfg.
// Returns nil when cfg.Enabled is false (recording disabled).
func NewAudioStorage(cfg config.StorageConfig) *AudioStorage {
	if !cfg.Enabled {
		return nil
	}
	return &AudioStorage{cfg: cfg}
}

// NewRecorder creates a SessionRecorder that writes converted PCM data to a
// per-session file under the storage directory.
//
// Returns nil when the AudioStorage itself is nil (recording disabled).
// The caller must call Close() when the session ends to flush pending writes.
func (s *AudioStorage) NewRecorder(sessionID string) *SessionRecorder {
	if s == nil {
		return nil
	}

	ts := time.Now().UTC().Format("20060102-150405")
	fileName := fmt.Sprintf("%s_%s.pcm", ts, sessionID)
	filePath := filepath.Join(s.cfg.Directory, fileName)

	r := &SessionRecorder{
		writeCh:  make(chan []byte, writerChanSize),
		doneCh:   make(chan struct{}),
		filePath: filePath,
	}
	go r.run()
	return r
}

// RunJanitor starts a background goroutine that enforces storage capacity limits
// every 5 minutes. It runs until ctx is cancelled.
//
// A nil *AudioStorage is a no-op.
func (s *AudioStorage) RunJanitor(ctx context.Context) {
	if s == nil {
		return
	}
	go func() {
		s.cleanup()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.cleanup()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// cleanup removes files that violate age, count, or byte-size limits.
// Oldest files are evicted first.
func (s *AudioStorage) cleanup() {
	dir := s.cfg.Directory
	entries, err := os.ReadDir(dir)
	if err != nil {
		slog.Warn("audio storage: cleanup: cannot read dir", "dir", dir, "err", err)
		return
	}

	type entry struct {
		name    string
		modTime time.Time
		size    int64
	}

	var files []entry
	var totalBytes int64
	now := time.Now()

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}

		// Delete files older than max_age_days immediately.
		if s.cfg.MaxAgeDays != nil {
			age := now.Sub(info.ModTime())
			if age > time.Duration(*s.cfg.MaxAgeDays)*24*time.Hour {
				path := filepath.Join(dir, e.Name())
				if rmErr := os.Remove(path); rmErr == nil {
					slog.Debug("audio storage: removed aged file",
						"file", e.Name(), "age_days", int(age.Hours()/24))
				}
				continue
			}
		}

		files = append(files, entry{
			name:    e.Name(),
			modTime: info.ModTime(),
			size:    info.Size(),
		})
		totalBytes += info.Size()
	}

	// Sort oldest-first for eviction ordering.
	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.Before(files[j].modTime)
	})

	// Evict oldest files when max_files is exceeded.
	if s.cfg.MaxFiles != nil {
		for len(files) > *s.cfg.MaxFiles {
			path := filepath.Join(dir, files[0].name)
			if rmErr := os.Remove(path); rmErr == nil {
				slog.Debug("audio storage: removed file (max_files limit)", "file", files[0].name)
				totalBytes -= files[0].size
			}
			files = files[1:]
		}
	}

	// Evict oldest files when max_bytes is exceeded.
	if s.cfg.MaxBytes != nil {
		for totalBytes > *s.cfg.MaxBytes && len(files) > 0 {
			path := filepath.Join(dir, files[0].name)
			if rmErr := os.Remove(path); rmErr == nil {
				slog.Debug("audio storage: removed file (max_bytes limit)", "file", files[0].name)
				totalBytes -= files[0].size
			}
			files = files[1:]
		}
	}
}

// ── SessionRecorder ───────────────────────────────────────────────────────────

// SessionRecorder buffers and writes audio chunks for a single session.
// It is safe for concurrent use from a single goroutine (the recvLoop).
type SessionRecorder struct {
	writeCh  chan []byte
	doneCh   chan struct{}
	dropped  atomic.Int64
	filePath string
	once     sync.Once
}

// Write enqueues a copy of chunk for async writing.
// Non-blocking: if the write channel is full the chunk is silently dropped
// and the drop counter is incremented (design doc §10.11).
func (r *SessionRecorder) Write(chunk []byte) {
	if r == nil {
		return
	}
	// Copy the slice so the caller can reuse/free the original buffer.
	cp := make([]byte, len(chunk))
	copy(cp, chunk)
	select {
	case r.writeCh <- cp:
	default:
		drops := r.dropped.Add(1)
		slog.Debug("audio storage: chunk dropped",
			"file", r.filePath, "total_drops", drops)
	}
}

// Close signals the writer goroutine to flush and close the file.
// Blocks until all queued chunks are written. Safe to call multiple times.
func (r *SessionRecorder) Close() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		close(r.writeCh)
		<-r.doneCh
	})
}

// DroppedChunks returns the number of chunks dropped due to a full write channel.
func (r *SessionRecorder) DroppedChunks() int64 {
	if r == nil {
		return 0
	}
	return r.dropped.Load()
}

// run is the background writer goroutine. It owns the file handle exclusively.
func (r *SessionRecorder) run() {
	defer close(r.doneCh)

	f, err := os.OpenFile(r.filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		slog.Error("audio storage: failed to open file",
			"file", r.filePath, "err", err)
		// Drain the channel so Write() callers can eventually close it.
		for range r.writeCh {
		}
		return
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			slog.Warn("audio storage: file close error",
				"file", r.filePath, "err", closeErr)
		}
	}()

	for chunk := range r.writeCh {
		if _, err := f.Write(chunk); err != nil {
			slog.Warn("audio storage: write error",
				"file", r.filePath, "err", err)
		}
	}
}
