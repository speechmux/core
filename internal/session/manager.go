package session

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/speechmux/core/internal/config"
	sttErrors "github.com/speechmux/core/internal/errors"
	"github.com/speechmux/core/internal/metrics"
	"github.com/speechmux/core/internal/ratelimit"
	clientpb "github.com/speechmux/proto/gen/go/client/v1"
)

// Manager manages the lifecycle of all active sessions.
// It is the Go equivalent of Python's SessionRegistry + CreateSessionHandler.
type Manager struct {
	sessions sync.Map // session_id → *Session
	count    atomic.Int64

	// per-IP and per-key active session counters (IP/key → *atomic.Int64)
	perIPCount  sync.Map
	perKeyCount sync.Map

	limiter *ratelimit.KeyedLimiter // per-key CreateSession rate limiter (nil = disabled)

	cfg       config.ServerConfig
	streamCfg config.StreamConfig
	authCfg   config.AuthConfig
	rateCfg   config.RateLimitConfig
	accepting atomic.Bool // set to false during graceful shutdown

	obs metrics.MetricsObserver
}

// NewManager creates a Manager with the given config and metrics observer.
// Call StopAccepting before DrainAll on graceful shutdown.
func NewManager(cfg *config.Config, obs metrics.MetricsObserver) *Manager {
	if obs == nil {
		obs = metrics.NopMetrics{}
	}
	m := &Manager{
		cfg:       cfg.Server,
		streamCfg: cfg.Stream,
		authCfg:   cfg.Auth,
		rateCfg:   cfg.RateLimit,
		obs:       obs,
	}
	m.accepting.Store(true)

	if cfg.RateLimit.CreateSessionRPS > 0 {
		m.limiter = ratelimit.NewKeyedLimiter(
			cfg.RateLimit.CreateSessionRPS,
			int(cfg.RateLimit.CreateSessionBurst),
		)
	}
	return m
}

// CreateSession validates the request, creates a Session, and registers it.
// Returns an *errors.STTError on any validation or capacity failure.
func (m *Manager) CreateSession(
	ctx context.Context,
	req *clientpb.SessionConfig,
	peerIP string,
	grpcMD map[string]string,
) (*Session, error) {
	if !m.accepting.Load() {
		return nil, sttErrors.New(sttErrors.ErrServerShuttingDown, "").ToGRPC()
	}

	// 1. session_id is required.
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, sttErrors.New(sttErrors.ErrSessionIDMissing, "").ToGRPC()
	}

	// 2. Auth.
	apiKey := strings.TrimSpace(req.GetApiKey())
	if err := Authenticate(m.authCfg, sessionID, apiKey, grpcMD); err != nil {
		return nil, err.(*sttErrors.STTError).ToGRPC()
	}

	// 3. Rate limit (per API key or IP).
	if m.limiter != nil {
		key := apiKey
		if key == "" {
			key = peerIP
		}
		if key == "" {
			key = "anonymous"
		}
		if !m.limiter.Allow(key) {
			return nil, sttErrors.New(sttErrors.ErrCreateSessionRateLimit, "").ToGRPC()
		}
	}

	// 4. Global max sessions.
	if max := m.cfg.MaxSessions; max > 0 && int(m.count.Load()) >= max {
		return nil, sttErrors.New(sttErrors.ErrMaxSessionsExceeded, "").ToGRPC()
	}

	// 5. Per-IP limit.
	if limit := m.rateCfg.MaxSessionsPerIP; limit > 0 && peerIP != "" {
		if m.countByKey(&m.perIPCount, peerIP) >= limit {
			return nil, sttErrors.New(sttErrors.ErrMaxSessionsExceeded,
				fmt.Sprintf("max sessions per IP exceeded: %s", peerIP)).ToGRPC()
		}
	}

	// 6. Per-API-key limit.
	if limit := m.rateCfg.MaxSessionsPerKey; limit > 0 && apiKey != "" {
		if m.countByKey(&m.perKeyCount, apiKey) >= limit {
			return nil, sttErrors.New(sttErrors.ErrMaxSessionsExceeded,
				fmt.Sprintf("max sessions per API key exceeded (limit: %d)", limit)).ToGRPC()
		}
	}

	// 7. Validate VAD config.
	if vad := req.GetVadConfig(); vad != nil {
		if vad.GetThreshold() < 0 {
			return nil, sttErrors.New(sttErrors.ErrVADConfigInvalid, "threshold must be >= 0").ToGRPC()
		}
	}

	// 8. Resolve session settings from request + server defaults.
	info := m.resolveInfo(req, peerIP, apiKey)

	// 9. Register the session (duplicate check is atomic via LoadOrStore).
	sess := newSession(ctx, sessionID, info)
	if _, loaded := m.sessions.LoadOrStore(sessionID, sess); loaded {
		sess.Close()
		return nil, sttErrors.New(sttErrors.ErrSessionIDDuplicate, sessionID).ToGRPC()
	}

	m.count.Add(1)
	m.incKey(&m.perIPCount, peerIP)
	m.incKey(&m.perKeyCount, apiKey)

	m.obs.IncActiveSessions()
	slog.Info("session created",
		"session_id", sessionID,
		"peer_ip", peerIP,
		"language", info.Language,
		"vad_silence", info.VADSilence,
		"vad_threshold", info.VADThreshold,
	)
	return sess, nil
}

// CloseSession removes the session and cancels its context.
func (m *Manager) CloseSession(sessionID string) {
	v, ok := m.sessions.LoadAndDelete(sessionID)
	if !ok {
		return
	}
	sess := v.(*Session)
	m.count.Add(-1)
	m.decKey(&m.perIPCount, sess.Info.ClientIP)
	m.decKey(&m.perKeyCount, sess.Info.APIKey)
	sess.Close()
	m.obs.DecActiveSessions()
	slog.Info("session closed", "session_id", sessionID)
}

// Get returns the session for the given ID, or nil if not found.
func (m *Manager) Get(sessionID string) *Session {
	v, ok := m.sessions.Load(sessionID)
	if !ok {
		return nil
	}
	return v.(*Session)
}

// ParkSession marks a session as parked (WebSocket connection dropped unexpectedly).
// After timeout the session is automatically closed via CloseSession.
// Does nothing if the session does not exist or resumable sessions are disabled
// (timeout == 0); in those cases the session is closed immediately.
func (m *Manager) ParkSession(sessionID string, timeout time.Duration) {
	if timeout == 0 {
		m.CloseSession(sessionID)
		return
	}
	v, ok := m.sessions.Load(sessionID)
	if !ok {
		return
	}
	sess := v.(*Session)
	sess.Park(timeout, func() {
		slog.Info("parked session resume window expired", "session_id", sessionID)
		m.CloseSession(sessionID)
	})
	slog.Info("session parked", "session_id", sessionID, "timeout", timeout)
}

// ResumeSession validates the resume token and unparks the session.
// Returns the session on success, or an *errors.STTError wrapped as gRPC error on failure.
func (m *Manager) ResumeSession(sessionID, token string) (*Session, error) {
	v, ok := m.sessions.Load(sessionID)
	if !ok {
		return nil, sttErrors.New(sttErrors.ErrSessionNotFound, sessionID).ToGRPC()
	}
	sess := v.(*Session)
	if sess.ResumeToken() != token {
		return nil, sttErrors.New(sttErrors.ErrResumeTokenInvalid, "").ToGRPC()
	}
	if !sess.Unpark() {
		// Session was not parked — either already resumed or park timer already fired.
		return nil, sttErrors.New(sttErrors.ErrResumeTokenInvalid, "session is not parked").ToGRPC()
	}
	slog.Info("session resumed", "session_id", sessionID)
	return sess, nil
}

// ActiveCount returns the number of currently active sessions.
func (m *Manager) ActiveCount() int {
	return int(m.count.Load())
}

// RunIdleChecker starts a background goroutine that periodically closes sessions
// that have not received audio within the configured SessionTimeoutSec window.
// Parked sessions are excluded — they are managed by their park timer.
// Returns immediately; the goroutine stops when ctx is cancelled.
// If SessionTimeoutSec is 0, this is a no-op.
func (m *Manager) RunIdleChecker(ctx context.Context) {
	timeout := m.cfg.SessionTimeoutSec
	if timeout == 0 {
		return
	}
	// Check at half the timeout interval so sessions are reaped within 1.5× the timeout.
	// Floor at 1 s to avoid spinning on very short timeouts.
	interval := timeout / 2
	if interval < time.Second {
		interval = time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.closeIdleSessions(timeout)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// closeIdleSessions iterates all active sessions and closes those idle longer than timeout.
func (m *Manager) closeIdleSessions(timeout time.Duration) {
	now := time.Now()
	m.sessions.Range(func(_, value any) bool {
		sess := value.(*Session)
		if sess.IsParked() {
			return true // park timer manages this session's lifetime
		}
		idle := now.Sub(sess.LastActivity())
		if idle > timeout {
			slog.Info("closing idle session", "session_id", sess.ID, "idle", idle.Round(time.Second))
			m.CloseSession(sess.ID)
		}
		return true
	})
}

// StopAccepting prevents new sessions from being created.
// Called in Phase 1 of graceful shutdown.
func (m *Manager) StopAccepting() {
	m.accepting.Store(false)
}

// DrainAll waits for all active sessions to finish, or until ctx is cancelled.
// Called in Phase 2 of graceful shutdown.
func (m *Manager) DrainAll(ctx context.Context) {
	// Collect done channels of all active sessions.
	var doneChs []<-chan struct{}
	m.sessions.Range(func(_, v any) bool {
		doneChs = append(doneChs, v.(*Session).Done())
		return true
	})

	if len(doneChs) == 0 {
		return
	}

	// Wait for all sessions or context cancellation.
	done := make(chan struct{})
	go func() {
		for _, ch := range doneChs {
			select {
			case <-ch:
			case <-ctx.Done():
				close(done)
				return
			}
		}
		close(done)
	}()

	select {
	case <-done:
		slog.Info("all sessions drained")
	case <-ctx.Done():
		slog.Warn("drain timeout — forcing shutdown", "remaining", m.count.Load())
	}
}

// resolveInfo builds a SessionInfo from the proto request and server defaults.
func (m *Manager) resolveInfo(req *clientpb.SessionConfig, peerIP, apiKey string) *SessionInfo {
	info := &SessionInfo{
		ClientIP:   peerIP,
		APIKey:     apiKey,
		StreamMode: req.GetStreamMode(),
		Attributes: make(map[string]string),
	}

	for k, v := range req.GetAttributes() {
		// Strip auth-related keys so they are not echoed back to the client.
		if k != "api_key" && k != "api-key" && k != "auth_sig" && k != "auth_ts" {
			info.Attributes[k] = v
		}
	}

	// Language and task from RecognitionConfig.
	if rc := req.GetRecognitionConfig(); rc != nil {
		info.Language = rc.GetLanguageCode()
		switch rc.GetTask() {
		case clientpb.Task_TASK_TRANSLATE:
			info.Task = "translate"
		default:
			info.Task = "transcribe"
		}
		switch rc.GetDecodeProfile() {
		case clientpb.DecodeProfile_DECODE_PROFILE_ACCURATE:
			info.DecodeProfile = "accurate"
		default:
			info.DecodeProfile = "realtime"
		}
	} else {
		info.Task = "transcribe"
		info.DecodeProfile = "realtime"
	}

	// Audio codec settings from AudioConfig.
	if ac := req.GetAudioConfig(); ac != nil {
		info.Encoding = ac.GetEncoding()
		info.SourceSampleRate = ac.GetSampleRate()
	}

	// VAD settings — apply server defaults then override with per-session values.
	info.VADSilence = m.streamCfg.VADSilenceSec
	info.VADThreshold = m.streamCfg.VADThreshold

	if vad := req.GetVadConfig(); vad != nil {
		if vad.GetSilenceDuration() > 0 {
			info.VADSilence = vad.GetSilenceDuration()
		}
		if vad.ThresholdOverride != nil {
			info.VADThreshold = vad.GetThresholdOverride()
		} else if vad.GetThreshold() > 0 {
			info.VADThreshold = vad.GetThreshold()
		}
	}

	return info
}

// countByKey returns the current session count for the given map key.
func (m *Manager) countByKey(sm *sync.Map, key string) int {
	if key == "" {
		return 0
	}
	v, ok := sm.Load(key)
	if !ok {
		return 0
	}
	return int(v.(*atomic.Int64).Load())
}

// incKey increments the per-key counter.
func (m *Manager) incKey(sm *sync.Map, key string) {
	if key == "" {
		return
	}
	actual, _ := sm.LoadOrStore(key, &atomic.Int64{})
	actual.(*atomic.Int64).Add(1)
}

// decKey decrements the per-key counter (floor 0).
// Uses a CAS loop to avoid a check-then-act race where two concurrent
// decrements on a counter of 1 would both observe > 0 and both subtract,
// driving the value to -1.
func (m *Manager) decKey(sm *sync.Map, key string) {
	if key == "" {
		return
	}
	v, ok := sm.Load(key)
	if !ok {
		return
	}
	ctr := v.(*atomic.Int64)
	for {
		old := ctr.Load()
		if old <= 0 {
			return
		}
		if ctr.CompareAndSwap(old, old-1) {
			return
		}
	}
}
