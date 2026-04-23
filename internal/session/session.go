// Package session manages the lifecycle of client streaming sessions.
package session

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clientpb "github.com/speechmux/proto/gen/go/client/v1"
)

// SessionInfo holds the resolved, negotiated settings for a session.
// It is immutable once set at session creation.
type SessionInfo struct {
	ClientIP      string
	APIKey        string
	Language      string
	Task          string
	DecodeProfile string
	VADSilence    float64
	VADThreshold  float64
	StreamMode    clientpb.StreamMode
	Attributes    map[string]string

	// EngineHint is the client-requested engine endpoint ID (from engine_hint in
	// RecognitionConfig). Empty string means "let the router decide".
	EngineHint string

	// Audio codec settings resolved from AudioConfig at session creation.
	// Used by the transport layer to convert incoming audio to PCM S16LE.
	Encoding         clientpb.AudioEncoding
	SourceSampleRate int32
}

// Session represents a single active client streaming session.
// It holds per-session state, channels, and a context for lifetime management.
type Session struct {
	// ID is the client-provided (or server-generated) session identifier.
	ID string

	// Info holds the negotiated settings resolved at session creation.
	Info *SessionInfo

	// CreatedAt is the wall-clock time the session was registered.
	CreatedAt time.Time

	// lastActivity is updated on every audio frame received.
	// Use TouchActivity / LastActivity to access it safely.
	lastActivity atomic.Value // holds time.Time

	ctx    context.Context
	cancel context.CancelFunc

	// done is closed when the session goroutine has fully stopped.
	// DrainAll waits on this channel.
	done chan struct{}

	// audioEndOnce ensures AudioInCh is closed exactly once.
	audioEndOnce sync.Once
	// processingDone is closed when ProcessSession completes.
	processingDone chan struct{}

	// AudioInCh receives codec-converted PCM audio frames.
	AudioInCh chan []byte

	// ResultCh receives recognition results from the decode pipeline.
	// The gRPC handler reads from this channel and streams results to the client.
	ResultCh chan *clientpb.RecognitionResult

	// resumeToken is a secret used to authenticate WebSocket reconnects.
	// Generated once at session creation; never changes.
	resumeToken string

	// Parking state: set when the WebSocket connection drops unexpectedly.
	// The session remains alive (context not cancelled) until the park timer fires.
	parked    atomic.Bool
	parkMu    sync.Mutex  // guards parkTimer
	parkTimer *time.Timer // fires onTimeout when the resume window expires
}

// newSession creates a Session and starts its background lifecycle goroutine.
func newSession(ctx context.Context, id string, info *SessionInfo) *Session {
	sCtx, cancel := context.WithCancel(ctx)
	s := &Session{
		ID:             id,
		Info:           info,
		CreatedAt:      time.Now(),
		ctx:            sCtx,
		cancel:         cancel,
		done:           make(chan struct{}),
		processingDone: make(chan struct{}),
		AudioInCh:      make(chan []byte, 64),
		ResultCh:       make(chan *clientpb.RecognitionResult, 16),
		resumeToken:    generateResumeToken(),
	}
	s.lastActivity.Store(time.Now())

	go func() {
		defer close(s.done)
		<-sCtx.Done()
	}()

	return s
}

// TouchActivity records the current time as the last audio activity.
func (s *Session) TouchActivity() {
	s.lastActivity.Store(time.Now())
}

// LastActivity returns the time of the most recent audio frame.
func (s *Session) LastActivity() time.Time {
	v, _ := s.lastActivity.Load().(time.Time)
	return v
}

// Close cancels the session context and signals the session goroutine to stop.
func (s *Session) Close() {
	s.cancel()
}

// Done returns a channel that is closed when the session has fully stopped.
func (s *Session) Done() <-chan struct{} {
	return s.done
}

// Context returns the session's context.
func (s *Session) Context() context.Context {
	return s.ctx
}

// SignalAudioEnd closes AudioInCh exactly once, signalling end-of-audio to
// the stream processor. Safe to call from multiple goroutines.
func (s *Session) SignalAudioEnd() {
	s.audioEndOnce.Do(func() { close(s.AudioInCh) })
}

// MarkProcessingDone closes processingDone, indicating the stream pipeline has
// completed. Must be called exactly once (from ProcessSession defer).
func (s *Session) MarkProcessingDone() {
	select {
	case <-s.processingDone:
	default:
		close(s.processingDone)
	}
}

// ProcessingDone returns a channel that is closed when the stream pipeline is
// complete and ResultCh will receive no more values.
func (s *Session) ProcessingDone() <-chan struct{} { return s.processingDone }

// ResumeToken returns the session's resume token, issued at creation.
// The client must present this token to authenticate a WebSocket reconnect.
func (s *Session) ResumeToken() string { return s.resumeToken }

// IsParked reports whether the session is currently parked (WS connection dropped,
// waiting for reconnect within the resume window).
func (s *Session) IsParked() bool { return s.parked.Load() }

// Park marks the session as parked and starts a timer that calls onTimeout when
// the resume window expires. onTimeout is called from a separate goroutine and
// should close the session. Park is idempotent: calling it again replaces the
// previous timer.
func (s *Session) Park(timeout time.Duration, onTimeout func()) {
	s.parked.Store(true)
	s.parkMu.Lock()
	defer s.parkMu.Unlock()
	if s.parkTimer != nil {
		s.parkTimer.Stop()
	}
	s.parkTimer = time.AfterFunc(timeout, func() {
		s.parked.Store(false)
		onTimeout()
	})
}

// Unpark cancels the park timer and clears the parked flag. Returns true if the
// session was parked and has been successfully claimed for reconnect; false if it
// was not parked (e.g. timeout already fired or session was never parked).
func (s *Session) Unpark() bool {
	s.parkMu.Lock()
	defer s.parkMu.Unlock()
	if s.parkTimer != nil {
		s.parkTimer.Stop()
		s.parkTimer = nil
	}
	return s.parked.CompareAndSwap(true, false)
}

// generateResumeToken returns a cryptographically random base64url-encoded token.
func generateResumeToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand.Read: %v", err))
	}
	return base64.URLEncoding.EncodeToString(b)
}
