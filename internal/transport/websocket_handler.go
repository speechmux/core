// Package transport contains network transport implementations for SpeechMux Core.
package transport

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/status"
	"nhooyr.io/websocket"

	"github.com/speechmux/core/internal/codec"
	"github.com/speechmux/core/internal/session"
	"github.com/speechmux/core/internal/storage"
	clientpb "github.com/speechmux/proto/gen/go/client/v1"
)

// ── JSON message types ────────────────────────────────────────────────────────

// wsStartMessage is the first JSON frame the client sends to open a session.
// Compatible with the existing web_mobile client.
type wsStartMessage struct {
	Type          string            `json:"type"`
	SessionID     string            `json:"session_id"`
	SampleRate    int32             `json:"sample_rate"`
	Encoding      string            `json:"encoding,omitempty"` // "pcm_s16le"|"alaw"|"mulaw"|"wav" (default: pcm_s16le)
	Task          string            `json:"task"`
	LanguageCode  string            `json:"language_code"`
	DecodeProfile string            `json:"decode_profile"`
	VADSilence    float64           `json:"vad_silence"`
	VADThreshold  float64           `json:"vad_threshold"`
	APIKey        string            `json:"api_key"`
	Attributes    map[string]string `json:"attributes"`
}

// wsResumeMessage is the first JSON frame for resuming a parked session after
// an unexpected WebSocket disconnect.
type wsResumeMessage struct {
	Type        string `json:"type"`
	SessionID   string `json:"session_id"`
	ResumeToken string `json:"resume_token"`
}

// wsSessionMessage is sent back to the client after a session is created or resumed.
// ResumeToken is included when the server has session resume enabled (resumeTimeout > 0).
type wsSessionMessage struct {
	Type          string  `json:"type"`
	SessionID     string  `json:"session_id"`
	LanguageCode  string  `json:"language_code"`
	Task          string  `json:"task"`
	DecodeProfile string  `json:"decode_profile"`
	VADSilence    float64 `json:"vad_silence"`
	VADThreshold  float64 `json:"vad_threshold"`
	ResumeToken   string  `json:"resume_token,omitempty"` // non-empty when server resume is enabled
}

// wsResultMessage carries a single recognition result to the client.
type wsResultMessage struct {
	Type          string  `json:"type"`
	IsFinal       bool    `json:"is_final"`
	Text          string  `json:"text"`           // combined transcript for backward compat
	CommittedText string  `json:"committed_text"` // stable prefix
	UnstableText  string  `json:"unstable_text"`  // in-flight suffix
	LanguageCode  string  `json:"language_code,omitempty"`
	StartSec      float64 `json:"start_sec"` // utterance start (relative to session; 0 until tracking is added)
	EndSec        float64 `json:"end_sec"`   // utterance end ≈ audio_duration for this result
}

// wsErrorMessage is sent when an error prevents the session from continuing.
type wsErrorMessage struct {
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"` // ERR#### if available
	Message string `json:"message"`
}

// wsDoneMessage is the final server-to-client frame on clean session end.
type wsDoneMessage struct {
	Type string `json:"type"`
}

// wsControlMessage is a generic client-to-server text frame (e.g. {"type":"end"}).
type wsControlMessage struct {
	Type string `json:"type"`
}

// ── WebSocketHandler ──────────────────────────────────────────────────────────

// WebSocketHandler bridges browser WebSocket clients to the Core stream processor.
//
// Protocol (compatible with the existing web_mobile client at /ws/stream):
//
// New session:
//
//	Client → Server (first): JSON {"type":"start", "session_id":"...", ...}
//	Server → Client:          JSON {"type":"session", "session_id":"...", "resume_token":"...", ...}
//	Client → Server:          binary PCM S16LE frames
//	Client → Server (final):  JSON {"type":"end"}
//	Server → Client:          JSON {"type":"result", ...} (streaming)
//	Server → Client (final):  JSON {"type":"done"} or {"type":"error",...}
//
// Resume (when server resumeTimeout > 0 and client reconnects after unexpected disconnect):
//
//	Client → Server (first): JSON {"type":"resume", "session_id":"...", "resume_token":"..."}
//	Server → Client:          JSON {"type":"session", ...} (same resume_token)
//	  ... then same audio/result flow continues ...
type WebSocketHandler struct {
	port           int
	server         *http.Server
	sessions       *session.Manager
	processor      SessionProcessor      // nil when VAD plugin is not configured
	converter      *codec.CodecConverter
	storage        *storage.AudioStorage // nil when storage is disabled
	resumeTimeout  time.Duration         // 0 means resume is disabled
	allowedOrigins []string              // empty = allow all (InsecureSkipVerify)
}

// NewWebSocketHandler creates a WebSocketHandler listening on the given port.
// sm and processor are wired from Application; processor may be nil.
// converter and stor may be nil (passthrough / recording disabled).
// tlsCfg may be nil for plaintext; when non-nil the server requires TLS (wss://).
// resumeTimeout controls the park-and-resume window; 0 disables session resume.
func NewWebSocketHandler(
	port int,
	sm *session.Manager,
	processor SessionProcessor,
	converter *codec.CodecConverter,
	stor *storage.AudioStorage,
	tlsCfg *tls.Config,
	resumeTimeout time.Duration,
	allowedOrigins []string,
) *WebSocketHandler {
	h := &WebSocketHandler{
		port:           port,
		sessions:       sm,
		processor:      processor,
		converter:      converter,
		storage:        stor,
		resumeTimeout:  resumeTimeout,
		allowedOrigins: allowedOrigins,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", h.serveWS)
	mux.HandleFunc("/ws/stream", h.serveWS) // path used by the existing web_mobile client
	h.server = &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		TLSConfig:         tlsCfg, // non-nil enables wss:// (TLS WebSocket)
		ReadHeaderTimeout: 10 * time.Second,
	}
	return h
}

// Serve starts the WebSocket HTTP server and blocks until ctx is cancelled.
// When TLSConfig is set the TCP listener is wrapped in a TLS listener (wss://).
func (h *WebSocketHandler) Serve(ctx context.Context) error {
	rawLis, err := net.Listen("tcp", h.server.Addr)
	if err != nil {
		return fmt.Errorf("websocket: listen %s: %w", h.server.Addr, err)
	}

	var lis net.Listener = rawLis
	if h.server.TLSConfig != nil {
		lis = tls.NewListener(rawLis, h.server.TLSConfig)
	}
	slog.Info("WebSocket server listening", "addr", h.server.Addr, "tls", h.server.TLSConfig != nil)

	errCh := make(chan error, 1)
	go func() {
		if serveErr := h.server.Serve(lis); serveErr != nil && serveErr != http.ErrServerClosed {
			errCh <- serveErr
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return h.server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// serveWS dispatches an incoming WebSocket connection to serveWSStart or
// serveWSResume based on the type field of the first JSON frame.
func (h *WebSocketHandler) serveWS(w http.ResponseWriter, r *http.Request) {
	acceptOpts := &websocket.AcceptOptions{}
	if len(h.allowedOrigins) > 0 {
		acceptOpts.OriginPatterns = h.allowedOrigins
	} else {
		acceptOpts.InsecureSkipVerify = true // allow all origins when no list is configured
	}
	conn, err := websocket.Accept(w, r, acceptOpts)
	if err != nil {
		slog.Warn("websocket accept failed", "err", err)
		return
	}
	defer conn.CloseNow()

	// Read first message — must be a JSON text frame.
	msgType, data, err := conn.Read(r.Context())
	if err != nil {
		return // client disconnected before sending first frame
	}
	if msgType != websocket.MessageText {
		_ = writeWSError(r.Context(), conn, "", "first message must be JSON")
		conn.Close(websocket.StatusUnsupportedData, "expected JSON start message")
		return
	}

	var envelope struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &envelope) != nil {
		_ = writeWSError(r.Context(), conn, "", "invalid JSON")
		conn.Close(websocket.StatusUnsupportedData, "invalid JSON")
		return
	}

	switch envelope.Type {
	case "start":
		var start wsStartMessage
		if json.Unmarshal(data, &start) != nil {
			_ = writeWSError(r.Context(), conn, "", `invalid start message`)
			conn.Close(websocket.StatusUnsupportedData, "invalid start message")
			return
		}
		h.serveWSStart(r, conn, &start)

	case "resume":
		if h.resumeTimeout == 0 {
			_ = writeWSError(r.Context(), conn, "ERR1018", "session resume is disabled on this server")
			conn.Close(websocket.StatusNormalClosure, "resume disabled")
			return
		}
		var resume wsResumeMessage
		if json.Unmarshal(data, &resume) != nil {
			_ = writeWSError(r.Context(), conn, "", "invalid resume message")
			conn.Close(websocket.StatusUnsupportedData, "invalid resume message")
			return
		}
		h.serveWSResume(r, conn, &resume)

	default:
		_ = writeWSError(r.Context(), conn, "", `first message must have type "start" or "resume"`)
		conn.Close(websocket.StatusUnsupportedData, "invalid message type")
	}
}

// serveWSStart creates a new session and runs the audio loop.
func (h *WebSocketHandler) serveWSStart(r *http.Request, conn *websocket.Conn, start *wsStartMessage) {
	peerIP := wsRemoteIP(r)
	sessionCfg := wsStartToSessionConfig(start)
	grpcMD := wsMetadata(r)

	sess, err := h.sessions.CreateSession(r.Context(), sessionCfg, peerIP, grpcMD)
	if err != nil {
		_ = writeWSError(r.Context(), conn, wsErrorCode(err), err.Error())
		conn.Close(websocket.StatusNormalClosure, "session creation failed")
		return
	}

	rec := h.storage.NewRecorder(sess.ID)
	defer rec.Close()

	slog.Info("websocket session started", "session_id", sess.ID, "peer", peerIP)
	h.runSessionLoop(r.Context(), conn, sess, rec, true)
}

// serveWSResume validates the resume token, unparks the session, and runs the
// audio loop on the existing session without restarting the stream processor.
func (h *WebSocketHandler) serveWSResume(r *http.Request, conn *websocket.Conn, resume *wsResumeMessage) {
	sess, err := h.sessions.ResumeSession(resume.SessionID, resume.ResumeToken)
	if err != nil {
		_ = writeWSError(r.Context(), conn, wsErrorCode(err), err.Error())
		conn.Close(websocket.StatusNormalClosure, "session resume failed")
		return
	}

	rec := h.storage.NewRecorder(sess.ID)
	defer rec.Close()

	slog.Info("websocket session resumed", "session_id", sess.ID, "peer", wsRemoteIP(r))
	h.runSessionLoop(r.Context(), conn, sess, rec, false)
}

// runSessionLoop sends the session confirmed frame, optionally starts the stream
// processor, then runs recvLoop and sendLoop concurrently until the connection ends.
//
// startProcessor must be true for new sessions and false for resumed sessions
// (the processor goroutine is already running during park).
//
// On a clean end (client sent {"type":"end"}), the session is closed normally.
// On an unexpected disconnect, the session is parked if resumeTimeout > 0.
func (h *WebSocketHandler) runSessionLoop(
	ctx context.Context,
	conn *websocket.Conn,
	sess *session.Session,
	rec *storage.SessionRecorder,
	startProcessor bool,
) {
	// Send session confirmed.
	resumeToken := ""
	if h.resumeTimeout > 0 {
		resumeToken = sess.ResumeToken()
	}
	if sendErr := writeWSJSON(ctx, conn, wsSessionMessage{
		Type:          "session",
		SessionID:     sess.ID,
		LanguageCode:  sess.Info.Language,
		Task:          sess.Info.Task,
		DecodeProfile: sess.Info.DecodeProfile,
		VADSilence:    sess.Info.VADSilence,
		VADThreshold:  sess.Info.VADThreshold,
		ResumeToken:   resumeToken,
	}); sendErr != nil {
		h.sessions.CloseSession(sess.ID)
		return
	}

	// Start stream processor for new sessions only (resumed sessions already
	// have a running ProcessSession goroutine that survived the park window).
	if startProcessor && h.processor != nil {
		go func() {
			if procErr := h.processor.ProcessSession(sess.Context(), sess); procErr != nil {
				slog.Warn("websocket stream processor error",
					"session_id", sess.ID, "err", procErr)
			}
		}()
	}

	// Run recv + send concurrently.
	//
	// Base context is the HTTP request context (not sess.Context()) so that
	// when recvLoop returns nil (client sent "end"), errgroup does NOT cancel
	// gCtx, allowing sendLoop to drain remaining results before exiting.
	//
	// Clean end flow:
	//   recvLoop → SignalAudioEnd() → returns nil (cleanEnd=true)
	//   pipeline drains → ProcessSession closes ResultCh
	//   sendLoop → ResultCh closed → sends "done" → returns nil
	//   g.Wait() → nil → CloseSession
	//
	// Unexpected disconnect flow:
	//   recvLoop → conn.Read error → returns err (cleanEnd=false)
	//   gCtx cancelled → sendLoop exits without "done"
	//   g.Wait() → non-nil → ParkSession (if resumeTimeout > 0)
	cleanEnd := false
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		err := h.recvLoop(gCtx, conn, sess, rec)
		if err == nil {
			cleanEnd = true
		}
		return err
	})
	g.Go(func() error { return h.sendLoop(gCtx, conn, sess) })

	if waitErr := g.Wait(); waitErr != nil {
		slog.Debug("websocket session ended with error", "session_id", sess.ID, "err", waitErr)
	}

	if cleanEnd || h.resumeTimeout == 0 {
		h.sessions.CloseSession(sess.ID)
	} else {
		// Unexpected disconnect: park the session and start the resume timer.
		h.sessions.ParkSession(sess.ID, h.resumeTimeout)
	}
}

// recvLoop reads binary audio and JSON control messages from the WebSocket client.
//
// Audio frames are codec-converted (if needed) before being forwarded to
// sess.AudioInCh. Converted audio is also written to rec if non-nil.
//
// Returns nil on a clean end (client sent {"type":"end"}).
// Returns a connection error on disconnect or protocol violation.
func (h *WebSocketHandler) recvLoop(
	ctx context.Context,
	conn *websocket.Conn,
	sess *session.Session,
	rec *storage.SessionRecorder,
) error {
	for {
		msgType, data, err := conn.Read(ctx)
		if err != nil {
			return err // websocket.CloseError, io.EOF, or ctx cancel
		}

		switch msgType {
		case websocket.MessageBinary:
			sess.TouchActivity()

			// Convert audio to PCM S16LE if needed.
			pcm := data
			if h.converter != nil {
				converted, convErr := h.converter.Convert(
					data,
					sess.Info.Encoding,
					sess.Info.SourceSampleRate,
				)
				if convErr != nil {
					slog.Warn("codec conversion failed",
						"session_id", sess.ID, "err", convErr)
					// Drop the unconvertible frame; do not terminate the session.
					continue
				}
				pcm = converted
			}

			// Best-effort async storage write.
			rec.Write(pcm)

			select {
			case sess.AudioInCh <- pcm:
			case <-ctx.Done():
				return ctx.Err()
			case <-sess.Context().Done():
				return nil
			}

		case websocket.MessageText:
			var ctrl wsControlMessage
			if json.Unmarshal(data, &ctrl) != nil {
				continue // ignore malformed control messages
			}
			if ctrl.Type == "end" {
				// Signal the pipeline to flush; ProcessSession will close
				// ResultCh when done, causing sendLoop to send "done".
				sess.SignalAudioEnd()
				if h.processor == nil {
					// No pipeline: cancel the session context so sendLoop exits.
					h.sessions.CloseSession(sess.ID)
				}
				return nil
			}
		}
	}
}

// sendLoop reads RecognitionResults from the session and streams them as JSON.
//
// Exits when:
//   - ResultCh is closed (ProcessSession flushed all results) → sends "done"
//   - sess.Context() is cancelled (abnormal shutdown / processor nil) →
//     drains buffered results, sends "done"
//   - ctx is cancelled (recv error / HTTP disconnect) → exits without "done"
func (h *WebSocketHandler) sendLoop(
	ctx context.Context,
	conn *websocket.Conn,
	sess *session.Session,
) error {
	for {
		select {
		case result, ok := <-sess.ResultCh:
			if !ok {
				// Channel closed: all results flushed.
				return writeWSJSON(ctx, conn, wsDoneMessage{Type: "done"})
			}
			if err := writeWSJSON(ctx, conn, wsResult(result)); err != nil {
				return fmt.Errorf("send result: %w", err)
			}

		case <-ctx.Done():
			// WebSocket error or HTTP disconnect — no "done" frame sent.
			return ctx.Err()

		case <-sess.Context().Done():
			// Session closed (processor finished or client sent "end").
			// Drain any results already buffered in the channel.
			for {
				select {
				case result, ok := <-sess.ResultCh:
					if !ok || result == nil {
						_ = writeWSJSON(ctx, conn, wsDoneMessage{Type: "done"})
						return nil
					}
					if err := writeWSJSON(ctx, conn, wsResult(result)); err != nil {
						return nil // connection closing; swallow error
					}
				default:
					_ = writeWSJSON(ctx, conn, wsDoneMessage{Type: "done"})
					return nil
				}
			}
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// wsResult converts a RecognitionResult proto to a wsResultMessage.
func wsResult(r *clientpb.RecognitionResult) wsResultMessage {
	text := r.GetText()
	if text == "" {
		// Build combined text for backward compatibility with clients that
		// display the "text" field directly.
		text = strings.TrimSpace(r.GetCommittedText() + " " + r.GetUnstableText())
	}
	return wsResultMessage{
		Type:          "result",
		IsFinal:       r.GetIsFinal(),
		Text:          text,
		CommittedText: r.GetCommittedText(),
		UnstableText:  r.GetUnstableText(),
		LanguageCode:  r.GetLanguageCode(),
		StartSec:      r.GetStartSec(),
		EndSec:        r.GetEndSec(),
	}
}

// wsStartToSessionConfig builds a SessionConfig proto from the JSON start message.
func wsStartToSessionConfig(m *wsStartMessage) *clientpb.SessionConfig {
	task := clientpb.Task_TASK_TRANSCRIBE
	if strings.EqualFold(m.Task, "translate") {
		task = clientpb.Task_TASK_TRANSLATE
	}

	decodeProfile := clientpb.DecodeProfile_DECODE_PROFILE_REALTIME
	if strings.EqualFold(m.DecodeProfile, "accurate") {
		decodeProfile = clientpb.DecodeProfile_DECODE_PROFILE_ACCURATE
	}

	cfg := &clientpb.SessionConfig{
		SessionId: m.SessionID,
		ApiKey:    m.APIKey,
		RecognitionConfig: &clientpb.RecognitionConfig{
			LanguageCode:  m.LanguageCode,
			Task:          task,
			DecodeProfile: decodeProfile,
		},
		Attributes: m.Attributes,
	}

	if m.VADSilence > 0 || m.VADThreshold > 0 {
		cfg.VadConfig = &clientpb.VADConfig{
			SilenceDuration: m.VADSilence,
			Threshold:       m.VADThreshold,
		}
	}

	if m.SampleRate > 0 || m.Encoding != "" {
		cfg.AudioConfig = &clientpb.AudioConfig{
			SampleRate: m.SampleRate,
			Encoding:   wsParseEncoding(m.Encoding),
		}
	}

	return cfg
}

// writeWSJSON serialises v to JSON and sends it as a WebSocket text frame.
func writeWSJSON(ctx context.Context, conn *websocket.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return conn.Write(ctx, websocket.MessageText, b)
}

// writeWSError sends a JSON error frame to the client.
func writeWSError(ctx context.Context, conn *websocket.Conn, code, message string) error {
	return writeWSJSON(ctx, conn, wsErrorMessage{
		Type:    "error",
		Code:    code,
		Message: message,
	})
}

// wsRemoteIP extracts the client IP from the HTTP request.
// Respects X-Forwarded-For when present (first entry = client IP).
func wsRemoteIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		if idx := strings.IndexByte(fwd, ','); idx >= 0 {
			return strings.TrimSpace(fwd[:idx])
		}
		return strings.TrimSpace(fwd)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// wsMetadata converts HTTP request headers to a lowercase string map,
// matching the format expected by session.Manager.CreateSession.
func wsMetadata(r *http.Request) map[string]string {
	md := make(map[string]string, len(r.Header))
	for k, vs := range r.Header {
		if len(vs) > 0 {
			md[strings.ToLower(k)] = vs[0]
		}
	}
	return md
}

// wsParseEncoding maps a JSON encoding string to the AudioEncoding proto enum.
// Unknown values default to PCM_S16LE for backward compatibility.
func wsParseEncoding(enc string) clientpb.AudioEncoding {
	switch strings.ToLower(enc) {
	case "alaw":
		return clientpb.AudioEncoding_AUDIO_ENCODING_ALAW
	case "mulaw", "ulaw":
		return clientpb.AudioEncoding_AUDIO_ENCODING_MULAW
	case "wav":
		return clientpb.AudioEncoding_AUDIO_ENCODING_WAV
	case "ogg_opus", "ogg-opus", "opus":
		return clientpb.AudioEncoding_AUDIO_ENCODING_OGG_OPUS
	default:
		return clientpb.AudioEncoding_AUDIO_ENCODING_PCM_S16LE
	}
}

// wsErrorCode extracts the ERR#### code prefix from a gRPC status error message.
// The format is "ERR#### message: detail". Returns "" if not recognisable.
func wsErrorCode(err error) string {
	s, ok := status.FromError(err)
	if !ok {
		return ""
	}
	msg := s.Message()
	// ERR#### is exactly 7 chars: "ERR" + 4 digits.
	if idx := strings.IndexByte(msg, ' '); idx == 7 && strings.HasPrefix(msg, "ERR") {
		return msg[:idx]
	}
	return ""
}
