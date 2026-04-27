package transport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"nhooyr.io/websocket"

	"github.com/speechmux/core/internal/config"
	sttErrors "github.com/speechmux/core/internal/errors"
	"github.com/speechmux/core/internal/session"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// newTestSessionManager returns a session.Manager with default test settings.
// Shared by grpc_server_test.go, tls_test.go, and pipeline_integration_test.go.
func newTestSessionManager() *session.Manager {
	cfg := &config.Config{}
	cfg.Defaults()
	_ = cfg.Validate()
	cfg.Server.MaxSessions = 100
	return session.NewManager(cfg, nil)
}

// wsTestConfig returns a minimal config for session.Manager.
func wsTestConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Defaults()
	_ = cfg.Validate()
	cfg.Server.MaxSessions = 10
	return cfg
}

// wsTestServer wraps a WebSocketHandler in an httptest.Server.
// The caller must close the returned server.
func wsTestServer(h *WebSocketHandler) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", h.serveWS)
	return httptest.NewServer(mux)
}

// dialWS connects a WebSocket client to the test server and returns the conn.
// The caller must close the connection.
func dialWS(t *testing.T, srv *httptest.Server) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	conn, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

// readJSON reads a single text frame and unmarshals it into v.
func readJSON(t *testing.T, conn *websocket.Conn, v any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mt, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("conn.Read: %v", err)
	}
	if mt != websocket.MessageText {
		t.Fatalf("expected text frame, got %v", mt)
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("unmarshal: %v (raw: %s)", err, data)
	}
}

// writeJSON sends v as a JSON text frame.
func writeJSON(t *testing.T, conn *websocket.Conn, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := conn.Write(context.Background(), websocket.MessageText, b); err != nil {
		t.Fatalf("conn.Write: %v", err)
	}
}

// startMsg builds a minimal start message for use in tests.
func startMsg(sessionID string) wsStartMessage {
	return wsStartMessage{
		Type:         "start",
		SessionID:    sessionID,
		LanguageCode: "en",
		SampleRate:   16000,
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestWebSocket_BinaryFirstFrame verifies that a binary first frame is rejected.
func TestWebSocket_BinaryFirstFrame(t *testing.T) {
	sm := session.NewManager(wsTestConfig(), nil)
	h := NewWebSocketHandler(0, sm, nil, nil, nil, nil, 0, nil)
	srv := wsTestServer(h)
	defer srv.Close()

	conn := dialWS(t, srv)
	defer conn.CloseNow()

	// Send binary frame as first message.
	if err := conn.Write(context.Background(), websocket.MessageBinary, []byte{0x01, 0x02}); err != nil {
		t.Fatalf("write binary: %v", err)
	}

	var errMsg wsErrorMessage
	readJSON(t, conn, &errMsg)
	if errMsg.Type != "error" {
		t.Errorf("type = %q, want error", errMsg.Type)
	}
}

// TestWebSocket_InvalidJSON verifies that a malformed JSON first frame is rejected.
func TestWebSocket_InvalidJSON(t *testing.T) {
	sm := session.NewManager(wsTestConfig(), nil)
	h := NewWebSocketHandler(0, sm, nil, nil, nil, nil, 0, nil)
	srv := wsTestServer(h)
	defer srv.Close()

	conn := dialWS(t, srv)
	defer conn.CloseNow()

	if err := conn.Write(context.Background(), websocket.MessageText, []byte(`not json`)); err != nil {
		t.Fatalf("write: %v", err)
	}

	var errMsg wsErrorMessage
	readJSON(t, conn, &errMsg)
	if errMsg.Type != "error" {
		t.Errorf("type = %q, want error", errMsg.Type)
	}
}

// TestWebSocket_UnknownType verifies that an unknown type field is rejected.
func TestWebSocket_UnknownType(t *testing.T) {
	sm := session.NewManager(wsTestConfig(), nil)
	h := NewWebSocketHandler(0, sm, nil, nil, nil, nil, 0, nil)
	srv := wsTestServer(h)
	defer srv.Close()

	conn := dialWS(t, srv)
	defer conn.CloseNow()

	writeJSON(t, conn, map[string]string{"type": "unknown"})

	var errMsg wsErrorMessage
	readJSON(t, conn, &errMsg)
	if errMsg.Type != "error" {
		t.Errorf("type = %q, want error", errMsg.Type)
	}
}

// TestWebSocket_Start_SessionCreated verifies that a valid start message creates
// a session and returns a session confirmed frame.
func TestWebSocket_Start_SessionCreated(t *testing.T) {
	sm := session.NewManager(wsTestConfig(), nil)
	h := NewWebSocketHandler(0, sm, nil, nil, nil, nil, 0, nil)
	srv := wsTestServer(h)
	defer srv.Close()

	conn := dialWS(t, srv)
	defer conn.CloseNow()

	writeJSON(t, conn, startMsg("sess-start"))

	var sessMsg wsSessionMessage
	readJSON(t, conn, &sessMsg)
	if sessMsg.Type != "session" {
		t.Errorf("type = %q, want session", sessMsg.Type)
	}
	if sessMsg.SessionID != "sess-start" {
		t.Errorf("session_id = %q, want sess-start", sessMsg.SessionID)
	}
	if sm.ActiveCount() != 1 {
		t.Errorf("ActiveCount = %d, want 1", sm.ActiveCount())
	}
}

// TestWebSocket_Start_ResumeTokenPresent verifies that when resumeTimeout > 0
// the session confirmed frame includes a non-empty resume_token.
func TestWebSocket_Start_ResumeTokenPresent(t *testing.T) {
	sm := session.NewManager(wsTestConfig(), nil)
	h := NewWebSocketHandler(0, sm, nil, nil, nil, nil, 30*time.Second, nil)
	srv := wsTestServer(h)
	defer srv.Close()

	conn := dialWS(t, srv)
	defer conn.CloseNow()

	writeJSON(t, conn, startMsg("sess-token"))

	var sessMsg wsSessionMessage
	readJSON(t, conn, &sessMsg)
	if sessMsg.ResumeToken == "" {
		t.Error("resume_token is empty; expected a non-empty token when resumeTimeout > 0")
	}
}

// TestWebSocket_Resume_Disabled verifies that a resume message is rejected when
// resumeTimeout is 0.
func TestWebSocket_Resume_Disabled(t *testing.T) {
	sm := session.NewManager(wsTestConfig(), nil)
	h := NewWebSocketHandler(0, sm, nil, nil, nil, nil, 0, nil)
	srv := wsTestServer(h)
	defer srv.Close()

	conn := dialWS(t, srv)
	defer conn.CloseNow()

	writeJSON(t, conn, wsResumeMessage{
		Type:        "resume",
		SessionID:   "any",
		ResumeToken: "any",
	})

	var errMsg wsErrorMessage
	readJSON(t, conn, &errMsg)
	if errMsg.Type != "error" {
		t.Errorf("type = %q, want error", errMsg.Type)
	}
}

// TestWebSocket_Resume_InvalidToken verifies that a resume with a wrong token
// is rejected with an error frame.
func TestWebSocket_Resume_InvalidToken(t *testing.T) {
	sm := session.NewManager(wsTestConfig(), nil)
	timeout := 30 * time.Second
	h := NewWebSocketHandler(0, sm, nil, nil, nil, nil, timeout, nil)
	srv := wsTestServer(h)
	defer srv.Close()

	// Create and park a session by starting, then disconnecting.
	cfg := wsTestConfig()
	cfg.Server.ResumableSessionTimeoutSec = 30
	_ = cfg.Validate()
	sm2 := session.NewManager(cfg, nil)
	h2 := NewWebSocketHandler(0, sm2, nil, nil, nil, nil, timeout, nil)
	srv2 := wsTestServer(h2)
	defer srv2.Close()

	// Start a session on srv2 to get a real session ID.
	conn1 := dialWS(t, srv2)
	writeJSON(t, conn1, startMsg("sess-resume"))
	var sessMsg wsSessionMessage
	readJSON(t, conn1, &sessMsg)
	// Abruptly close (simulates unexpected disconnect) → session parks.
	conn1.CloseNow()
	time.Sleep(50 * time.Millisecond) // give server time to park

	// Reconnect with wrong token.
	conn2 := dialWS(t, srv2)
	defer conn2.CloseNow()
	writeJSON(t, conn2, wsResumeMessage{
		Type:        "resume",
		SessionID:   "sess-resume",
		ResumeToken: "wrong-token",
	})

	var errMsg wsErrorMessage
	readJSON(t, conn2, &errMsg)
	if errMsg.Type != "error" {
		t.Errorf("type = %q, want error", errMsg.Type)
	}
	if errMsg.Code != "ERR1019" {
		t.Errorf("code = %q, want ERR1019", errMsg.Code)
	}
}

// wsErrProcessor is a SessionProcessor that returns a fixed error.
type wsErrProcessor struct{ err error }

func (p *wsErrProcessor) ProcessSession(_ context.Context, sess *session.Session) error {
	for range sess.AudioInCh {
	}
	sess.SignalPipelineExit(p.err)
	close(sess.ResultCh)
	sess.MarkProcessingDone()
	return p.err
}

// TestWebSocket_PipelineError_ErrorFrame verifies that when ProcessSession returns
// an *STTError the WebSocket client receives {"type":"error","code":"ERR2001"}.
func TestWebSocket_PipelineError_ErrorFrame(t *testing.T) {
	sm := session.NewManager(wsTestConfig(), nil)
	proc := &wsErrProcessor{
		err: sttErrors.New(sttErrors.ErrDecodeTimeout, "test timeout"),
	}
	h := NewWebSocketHandler(0, sm, proc, nil, nil, nil, 0, nil)
	srv := wsTestServer(h)
	defer srv.Close()

	conn := dialWS(t, srv)
	defer conn.CloseNow()

	writeJSON(t, conn, startMsg("sess-pipe-err-ws"))

	// session confirmed
	var sessMsg wsSessionMessage
	readJSON(t, conn, &sessMsg)
	if sessMsg.Type != "session" {
		t.Fatalf("expected session, got %q", sessMsg.Type)
	}

	// Signal end-of-audio so the processor returns its error.
	writeJSON(t, conn, wsControlMessage{Type: "end"})

	// Next frame must be {"type":"error","code":"ERR2001"}.
	var errMsg wsErrorMessage
	readJSON(t, conn, &errMsg)
	if errMsg.Type != "error" {
		t.Errorf("type = %q, want error", errMsg.Type)
	}
	if errMsg.Code != string(sttErrors.ErrDecodeTimeout) {
		t.Errorf("code = %q, want %q", errMsg.Code, sttErrors.ErrDecodeTimeout)
	}
}

// TestWsStartToSessionConfig_EngineHint verifies that engine_hint in the JSON
// start message is propagated to RecognitionConfig.EngineHint.
func TestWsStartToSessionConfig_EngineHint(t *testing.T) {
	m := &wsStartMessage{
		Type:         "start",
		LanguageCode: "ko",
		EngineHint:   "whisper-mlx",
	}
	cfg := wsStartToSessionConfig(m)
	got := cfg.GetRecognitionConfig().GetEngineHint()
	if got != "whisper-mlx" {
		t.Errorf("EngineHint = %q, want %q", got, "whisper-mlx")
	}
}

// TestWsStartToSessionConfig_EngineHint_Empty verifies that omitting engine_hint
// results in an empty string (no default substitution).
func TestWsStartToSessionConfig_EngineHint_Empty(t *testing.T) {
	m := &wsStartMessage{Type: "start", LanguageCode: "en"}
	cfg := wsStartToSessionConfig(m)
	if got := cfg.GetRecognitionConfig().GetEngineHint(); got != "" {
		t.Errorf("EngineHint = %q, want empty", got)
	}
}

// TestWebSocket_PipelineExit_RecvLoopExits verifies that after the pipeline signals
// an error, the server-side recvLoop exits and the connection is closed within
// 500ms, preventing a goroutine leak.
func TestWebSocket_PipelineExit_RecvLoopExits(t *testing.T) {
	sm := session.NewManager(wsTestConfig(), nil)
	proc := &wsErrProcessor{err: sttErrors.New(sttErrors.ErrDecodeTimeout, "test timeout")}
	h := NewWebSocketHandler(0, sm, proc, nil, nil, nil, 0, nil)
	srv := wsTestServer(h)
	defer srv.Close()

	conn := dialWS(t, srv)
	defer conn.CloseNow()

	writeJSON(t, conn, startMsg("sess-leak-ws"))

	var sessMsg wsSessionMessage
	readJSON(t, conn, &sessMsg)
	if sessMsg.Type != "session" {
		t.Fatalf("expected session, got %q", sessMsg.Type)
	}

	// Signal end-of-audio: recvLoop calls SignalAudioEnd and returns nil.
	// The processor drains AudioInCh and signals PipelineExitCh with an error.
	writeJSON(t, conn, wsControlMessage{Type: "end"})

	// sendLoop receives PipelineExitCh, sends the error frame, and returns
	// non-nil — cancelling gCtx so that recvLoop (if still running) exits too.
	var errMsg wsErrorMessage
	readJSON(t, conn, &errMsg)
	if errMsg.Type != "error" {
		t.Fatalf("expected error frame, got %q", errMsg.Type)
	}

	// sendLoop returned non-nil → errgroup returned → handler returned.
	// nhooyr.io/websocket sends a close frame when the handler returns.
	// The next conn.Read() must return an error (close or disconnect) within 500ms.
	connClosed := make(chan struct{})
	go func() {
		defer close(connClosed)
		readCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _, _ = conn.Read(readCtx) // any error (close frame / EOF) satisfies the check
	}()

	select {
	case <-connClosed:
		// connection closed by server as expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("server did not close connection within 500ms after pipeline error")
	}
}

// TestWebSocket_Resume_Success verifies that a valid resume reconnects to the
// parked session and receives a session confirmed frame with the same token.
func TestWebSocket_Resume_Success(t *testing.T) {
	cfg := wsTestConfig()
	cfg.Server.ResumableSessionTimeoutSec = 30
	_ = cfg.Validate()
	sm := session.NewManager(cfg, nil)
	timeout := 30 * time.Second
	h := NewWebSocketHandler(0, sm, nil, nil, nil, nil, timeout, nil)
	srv := wsTestServer(h)
	defer srv.Close()

	// Open session, grab resume token, then abruptly close.
	conn1 := dialWS(t, srv)
	writeJSON(t, conn1, startMsg("sess-res-ok"))
	var sessMsg1 wsSessionMessage
	readJSON(t, conn1, &sessMsg1)
	token := sessMsg1.ResumeToken
	if token == "" {
		t.Fatal("resume_token is empty; cannot resume")
	}
	conn1.CloseNow()
	time.Sleep(50 * time.Millisecond) // give server time to park

	// Reconnect with valid token.
	conn2 := dialWS(t, srv)
	defer conn2.CloseNow()
	writeJSON(t, conn2, wsResumeMessage{
		Type:        "resume",
		SessionID:   "sess-res-ok",
		ResumeToken: token,
	})

	var sessMsg2 wsSessionMessage
	readJSON(t, conn2, &sessMsg2)
	if sessMsg2.Type != "session" {
		t.Errorf("type = %q, want session", sessMsg2.Type)
	}
	if sessMsg2.SessionID != "sess-res-ok" {
		t.Errorf("session_id = %q, want sess-res-ok", sessMsg2.SessionID)
	}
	if sessMsg2.ResumeToken != token {
		t.Errorf("resume_token changed after resume: got %q, want %q", sessMsg2.ResumeToken, token)
	}
}
