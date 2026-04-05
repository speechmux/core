package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/speechmux/core/internal/config"
	"github.com/speechmux/core/internal/session"
	clientpb "github.com/speechmux/proto/gen/go/client/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func makeConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Defaults()
	_ = cfg.Validate()
	cfg.Server.MaxSessions = 5
	cfg.RateLimit.MaxSessionsPerIP = 3
	cfg.RateLimit.MaxSessionsPerKey = 2
	cfg.Stream.VADSilenceSec = 0.5
	cfg.Stream.VADThreshold = 0.5
	return cfg
}

func makeReq(id string) *clientpb.SessionConfig {
	return &clientpb.SessionConfig{
		SessionId: id,
		RecognitionConfig: &clientpb.RecognitionConfig{
			LanguageCode: "en",
		},
	}
}

func TestCreateSession_Success(t *testing.T) {
	m := session.NewManager(makeConfig(), nil)
	sess, err := m.CreateSession(context.Background(), makeReq("s1"), "1.2.3.4", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sess.ID != "s1" {
		t.Errorf("ID = %q, want s1", sess.ID)
	}
	if m.ActiveCount() != 1 {
		t.Errorf("ActiveCount = %d, want 1", m.ActiveCount())
	}
}

func TestCreateSession_MissingID(t *testing.T) {
	m := session.NewManager(makeConfig(), nil)
	_, err := m.CreateSession(context.Background(), makeReq(""), "1.2.3.4", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}

func TestCreateSession_Duplicate(t *testing.T) {
	m := session.NewManager(makeConfig(), nil)
	_, err := m.CreateSession(context.Background(), makeReq("s1"), "1.2.3.4", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.CreateSession(context.Background(), makeReq("s1"), "1.2.3.5", nil)
	if err == nil {
		t.Fatal("expected duplicate error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.AlreadyExists {
		t.Errorf("code = %v, want AlreadyExists", st.Code())
	}
}

func TestCreateSession_MaxSessionsExceeded(t *testing.T) {
	cfg := makeConfig()
	cfg.Server.MaxSessions = 2
	m := session.NewManager(cfg, nil)
	for i := range 2 {
		id := "s" + string(rune('1'+i))
		if _, err := m.CreateSession(context.Background(), makeReq(id), "1.2.3.4", nil); err != nil {
			t.Fatalf("session %s: %v", id, err)
		}
	}
	_, err := m.CreateSession(context.Background(), makeReq("s3"), "1.2.3.4", nil)
	if err == nil {
		t.Fatal("expected capacity error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("code = %v, want ResourceExhausted", st.Code())
	}
}

func TestCreateSession_PerIPLimit(t *testing.T) {
	cfg := makeConfig()
	cfg.RateLimit.MaxSessionsPerIP = 2
	m := session.NewManager(cfg, nil)
	for _, id := range []string{"s1", "s2"} {
		if _, err := m.CreateSession(context.Background(), makeReq(id), "1.2.3.4", nil); err != nil {
			t.Fatal(err)
		}
	}
	_, err := m.CreateSession(context.Background(), makeReq("s3"), "1.2.3.4", nil)
	if err == nil {
		t.Fatal("expected per-IP limit error")
	}
}

func TestCloseSession(t *testing.T) {
	m := session.NewManager(makeConfig(), nil)
	sess, err := m.CreateSession(context.Background(), makeReq("s1"), "1.2.3.4", nil)
	if err != nil {
		t.Fatal(err)
	}
	m.CloseSession(sess.ID)
	if m.ActiveCount() != 0 {
		t.Errorf("ActiveCount = %d after close, want 0", m.ActiveCount())
	}
}

func TestShuttingDown_RejectsNewSessions(t *testing.T) {
	m := session.NewManager(makeConfig(), nil)
	m.StopAccepting()
	_, err := m.CreateSession(context.Background(), makeReq("s1"), "1.2.3.4", nil)
	if err == nil {
		t.Fatal("expected shutdown error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable {
		t.Errorf("code = %v, want Unavailable", st.Code())
	}
}

func TestDrainAll_CompletesWhenSessionsClose(t *testing.T) {
	m := session.NewManager(makeConfig(), nil)
	sess, err := m.CreateSession(context.Background(), makeReq("s1"), "1.2.3.4", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Close the session asynchronously after a short delay.
	go func() {
		time.Sleep(50 * time.Millisecond)
		m.CloseSession(sess.ID)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	m.DrainAll(ctx)

	if ctx.Err() != nil {
		t.Error("DrainAll timed out unexpectedly")
	}
}

func TestDrainAll_TimesOutWhenSessionStuck(t *testing.T) {
	m := session.NewManager(makeConfig(), nil)
	_, err := m.CreateSession(context.Background(), makeReq("stuck"), "1.2.3.4", nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	m.DrainAll(ctx) // should return after timeout without panic
}

func TestVADConfigOverride(t *testing.T) {
	m := session.NewManager(makeConfig(), nil)
	thresh := 0.7
	req := &clientpb.SessionConfig{
		SessionId: "vad-test",
		VadConfig: &clientpb.VADConfig{
			SilenceDuration:  1.5,
			ThresholdOverride: &thresh,
		},
	}
	sess, err := m.CreateSession(context.Background(), req, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sess.Info.VADSilence != 1.5 {
		t.Errorf("VADSilence = %v, want 1.5", sess.Info.VADSilence)
	}
	if sess.Info.VADThreshold != 0.7 {
		t.Errorf("VADThreshold = %v, want 0.7", sess.Info.VADThreshold)
	}
}

// ── idle checker ─────────────────────────────────────────────────────────────

// idleConfig returns a config with SessionTimeoutSec set to a short value
// for use in idle-checker tests.
func idleConfig(timeoutSec int) *config.Config {
	cfg := makeConfig()
	cfg.Server.SessionTimeoutRaw = timeoutSec
	_ = cfg.Validate()
	return cfg
}

// TestRunIdleChecker_ClosesIdleSession verifies that a session with no recent
// audio activity is closed by the idle checker.
func TestRunIdleChecker_ClosesIdleSession(t *testing.T) {
	cfg := idleConfig(1) // 1 s timeout → checker fires every 500 ms
	m := session.NewManager(cfg, nil)

	sess, err := m.CreateSession(context.Background(), makeReq("idle-1"), "", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Back-date LastActivity so the session appears idle.
	// TouchActivity sets now; we need it to be > 1 s ago.
	// Simulate by waiting past the timeout window instead (simpler, no test-only
	// accessor needed).
	_ = sess

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	m.RunIdleChecker(ctx)

	// Wait for the idle checker to reap the session (timeout 1 s, check every 500 ms).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if m.ActiveCount() == 0 {
			return // closed as expected
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Errorf("session was not closed by idle checker within 3 s; ActiveCount = %d", m.ActiveCount())
}

// TestRunIdleChecker_SkipsParkedSession verifies that a parked session is NOT
// reaped by the idle checker — the park timer manages its lifetime.
func TestRunIdleChecker_SkipsParkedSession(t *testing.T) {
	cfg := idleConfig(1) // 1 s idle timeout
	cfg.Server.ResumableSessionTimeoutRaw = 10
	_ = cfg.Validate()
	m := session.NewManager(cfg, nil)

	sess, err := m.CreateSession(context.Background(), makeReq("parked-1"), "", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Park the session with a long timeout so the park timer won't fire during the test.
	m.ParkSession(sess.ID, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	m.RunIdleChecker(ctx)

	// Wait past the idle timeout; the parked session should survive.
	time.Sleep(2 * time.Second)

	if m.ActiveCount() != 1 {
		t.Errorf("parked session was closed by idle checker; ActiveCount = %d, want 1", m.ActiveCount())
	}

	// Clean up: cancel the session manually before test exits.
	m.CloseSession(sess.ID)
}

// TestRunIdleChecker_ClosesAfterActivityStops verifies that a session receiving
// audio is NOT reaped while active, but IS reaped after audio stops.
func TestRunIdleChecker_ClosesAfterActivityStops(t *testing.T) {
	cfg := idleConfig(1) // 1 s idle timeout
	m := session.NewManager(cfg, nil)

	sess, err := m.CreateSession(context.Background(), makeReq("active-then-idle"), "", nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.RunIdleChecker(ctx)

	// Simulate audio arriving every 100 ms for 1.5 s — session must stay alive.
	start := time.Now()
	for time.Since(start) < 1500*time.Millisecond {
		sess.TouchActivity()
		time.Sleep(100 * time.Millisecond)
		if m.ActiveCount() == 0 {
			t.Fatal("session was closed while audio was still arriving")
		}
	}

	// Audio has stopped. Session should be reaped within ~2 s.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if m.ActiveCount() == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Errorf("session was not closed after activity stopped; ActiveCount = %d", m.ActiveCount())
}

// TestRunIdleChecker_Disabled verifies that RunIdleChecker is a no-op when
// SessionTimeoutSec is 0.
func TestRunIdleChecker_Disabled(t *testing.T) {
	cfg := makeConfig()
	cfg.Server.SessionTimeoutRaw = 0
	_ = cfg.Validate()
	m := session.NewManager(cfg, nil)

	_, err := m.CreateSession(context.Background(), makeReq("no-idle"), "", nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.RunIdleChecker(ctx) // should return immediately (no-op)
	cancel()

	// Session should still be present since no idle checker ran.
	if m.ActiveCount() != 1 {
		t.Errorf("ActiveCount = %d, want 1", m.ActiveCount())
	}
}
