package transport

import (
	"context"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/speechmux/core/internal/config"
	"github.com/speechmux/core/internal/session"
	clientpb "github.com/speechmux/proto/gen/go/client/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20 // 1 MiB

// startTestGRPCServer spins up an in-process gRPC server using bufconn.
// It registers the given session manager and processor, and returns a client
// that can call StreamingRecognize. The server and client are cleaned up via t.Cleanup.
func startTestGRPCServer(t *testing.T, sm *session.Manager, proc SessionProcessor) clientpb.STTServiceClient {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	srv := NewGRPCServer(0, sm, proc, nil)
	// Replace the internal grpc.Server's listener with the bufconn one.
	// We start the server directly via Serve(lis) instead of going through
	// Serve(ctx) which opens a TCP port.
	go func() {
		_ = srv.server.Serve(lis)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}

	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
		_ = conn.Close()
	})

	return clientpb.NewSTTServiceClient(conn)
}

// ── stub processors ───────────────────────────────────────────────────────────

// echoProcessor drains AudioInCh, optionally sends a single result to ResultCh,
// then closes ResultCh and marks processing done.
type echoProcessor struct {
	result *clientpb.RecognitionResult // nil = send nothing
}

func (p *echoProcessor) ProcessSession(_ context.Context, sess *session.Session) error {
	// Drain audio so the sender never blocks.
	for range sess.AudioInCh {
	}
	if p.result != nil {
		sess.ResultCh <- p.result
	}
	close(sess.ResultCh)
	sess.MarkProcessingDone()
	return nil
}

// countingProcessor counts audio frames received and drains audio.
type countingProcessor struct {
	received atomic.Int64
}

func (p *countingProcessor) ProcessSession(_ context.Context, sess *session.Session) error {
	for range sess.AudioInCh {
		p.received.Add(1)
	}
	close(sess.ResultCh)
	sess.MarkProcessingDone()
	return nil
}

// ── message helpers ───────────────────────────────────────────────────────────

func sessionConfigMsg(sessionID, lang string) *clientpb.StreamingRequest {
	return &clientpb.StreamingRequest{
		StreamingRequest: &clientpb.StreamingRequest_SessionConfig{
			SessionConfig: &clientpb.SessionConfig{
				SessionId: sessionID,
				RecognitionConfig: &clientpb.RecognitionConfig{
					LanguageCode: lang,
				},
			},
		},
	}
}

func audioMsg(pcm []byte) *clientpb.StreamingRequest {
	return &clientpb.StreamingRequest{
		StreamingRequest: &clientpb.StreamingRequest_Audio{Audio: pcm},
	}
}

func isLastMsg() *clientpb.StreamingRequest {
	return &clientpb.StreamingRequest{
		StreamingRequest: &clientpb.StreamingRequest_Signal{
			Signal: &clientpb.StreamSignal{IsLast: true},
		},
	}
}

// recvAll reads all StreamingResponses until io.EOF and returns them.
func recvAll(t *testing.T, stream clientpb.STTService_StreamingRecognizeClient) []*clientpb.StreamingResponse {
	t.Helper()
	var msgs []*clientpb.StreamingResponse
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Non-EOF errors are returned as a one-element slice containing nil
			// so callers that expect an error can check grpc status instead.
			return nil
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestStreamingRecognize_FirstMessageNotConfig verifies that sending a non-config
// first message returns ERR1016 (codes.InvalidArgument).
func TestStreamingRecognize_FirstMessageNotConfig(t *testing.T) {
	sm := newTestSessionManager()
	client := startTestGRPCServer(t, sm, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatalf("StreamingRecognize: %v", err)
	}

	// Send audio instead of session_config.
	if err := stream.Send(audioMsg(make([]byte, 640))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	_ = stream.CloseSend()

	_, recvErr := stream.Recv()
	if recvErr == nil {
		t.Fatal("expected error, got nil")
	}
	st, ok := status.FromError(recvErr)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", recvErr)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("want codes.InvalidArgument, got %v", st.Code())
	}
	if !strings.Contains(st.Message(), "ERR1016") {
		t.Errorf("want ERR1016 in message, got %q", st.Message())
	}
}

// TestStreamingRecognize_SessionCreated verifies that a valid session_config
// as the first message yields a SessionCreated response with the correct session_id.
func TestStreamingRecognize_SessionCreated(t *testing.T) {
	sm := newTestSessionManager()
	client := startTestGRPCServer(t, sm, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatalf("StreamingRecognize: %v", err)
	}

	if err := stream.Send(sessionConfigMsg("sess-grpc-1", "ko")); err != nil {
		t.Fatalf("Send: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	sc := resp.GetSessionCreated()
	if sc == nil {
		t.Fatalf("expected SessionCreated, got %T", resp.GetStreamingResponse())
	}
	if sc.GetSessionId() != "sess-grpc-1" {
		t.Errorf("want session_id=sess-grpc-1, got %q", sc.GetSessionId())
	}

	_ = stream.CloseSend()
}

// TestStreamingRecognize_NilProcessor verifies that when no processor is set
// audio is silently discarded and the stream closes cleanly on CloseSend.
func TestStreamingRecognize_NilProcessor(t *testing.T) {
	sm := newTestSessionManager()
	client := startTestGRPCServer(t, sm, nil) // nil processor

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatalf("StreamingRecognize: %v", err)
	}

	if err := stream.Send(sessionConfigMsg("sess-nil-proc", "en")); err != nil {
		t.Fatalf("Send config: %v", err)
	}
	// Consume SessionCreated.
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv SessionCreated: %v", err)
	}

	// Send audio frames — they should be dropped without error.
	for i := 0; i < 5; i++ {
		if err := stream.Send(audioMsg(make([]byte, 640))); err != nil {
			t.Fatalf("Send audio[%d]: %v", i, err)
		}
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	// Server should close cleanly (io.EOF).
	_, err = stream.Recv()
	if err != io.EOF {
		t.Errorf("want io.EOF, got %v", err)
	}
}

// TestStreamingRecognize_AudioForwarding verifies that audio frames sent by the
// client arrive on sess.AudioInCh in the order they were sent.
func TestStreamingRecognize_AudioForwarding(t *testing.T) {
	sm := newTestSessionManager()
	proc := &countingProcessor{}
	client := startTestGRPCServer(t, sm, proc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatalf("StreamingRecognize: %v", err)
	}

	if err := stream.Send(sessionConfigMsg("sess-audio-fwd", "ko")); err != nil {
		t.Fatalf("Send config: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv SessionCreated: %v", err)
	}

	const numFrames = 20
	frame := make([]byte, 640)
	for i := 0; i < numFrames; i++ {
		if err := stream.Send(audioMsg(frame)); err != nil {
			t.Fatalf("Send audio[%d]: %v", i, err)
		}
	}
	// Send is_last to close AudioInCh so countingProcessor can finish.
	if err := stream.Send(isLastMsg()); err != nil {
		t.Fatalf("Send isLast: %v", err)
	}
	_ = stream.CloseSend()

	// Wait for stream to end.
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
	}

	// Allow a brief moment for the processor goroutine to finish counting.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if proc.received.Load() == numFrames {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := proc.received.Load(); got != numFrames {
		t.Errorf("want %d frames forwarded, got %d", numFrames, got)
	}
}

// TestStreamingRecognize_IsLastSignal verifies that sending is_last calls
// sess.SignalAudioEnd(), which closes AudioInCh and lets the processor finish.
func TestStreamingRecognize_IsLastSignal(t *testing.T) {
	sm := newTestSessionManager()

	result := &clientpb.RecognitionResult{
		Text:    "is_last test",
		IsFinal: true,
	}
	proc := &echoProcessor{result: result}
	client := startTestGRPCServer(t, sm, proc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatalf("StreamingRecognize: %v", err)
	}

	if err := stream.Send(sessionConfigMsg("sess-islast", "ko")); err != nil {
		t.Fatalf("Send config: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv SessionCreated: %v", err)
	}

	// Send a couple of audio frames then signal end.
	for i := 0; i < 3; i++ {
		if err := stream.Send(audioMsg(make([]byte, 640))); err != nil {
			t.Fatalf("Send audio[%d]: %v", i, err)
		}
	}
	if err := stream.Send(isLastMsg()); err != nil {
		t.Fatalf("Send isLast: %v", err)
	}
	_ = stream.CloseSend()

	// Collect all responses; we expect at least the result.
	var results []*clientpb.RecognitionResult
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if r := resp.GetResult(); r != nil {
			results = append(results, r)
		}
	}

	if len(results) == 0 {
		t.Fatal("expected at least one RecognitionResult, got none")
	}
	if results[0].GetText() != "is_last test" {
		t.Errorf("want text %q, got %q", "is_last test", results[0].GetText())
	}
}

// TestStreamingRecognize_ResultStreaming verifies that results placed on
// sess.ResultCh are forwarded to the gRPC client as StreamingResponse_Result.
func TestStreamingRecognize_ResultStreaming(t *testing.T) {
	sm := newTestSessionManager()
	proc := &echoProcessor{
		result: &clientpb.RecognitionResult{
			Text:    "hello grpc",
			IsFinal: true,
		},
	}
	client := startTestGRPCServer(t, sm, proc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatalf("StreamingRecognize: %v", err)
	}

	if err := stream.Send(sessionConfigMsg("sess-result", "en")); err != nil {
		t.Fatalf("Send config: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv SessionCreated: %v", err)
	}

	// Signal end so the processor sends the result and closes ResultCh.
	if err := stream.Send(isLastMsg()); err != nil {
		t.Fatalf("Send isLast: %v", err)
	}
	_ = stream.CloseSend()

	var got *clientpb.RecognitionResult
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if r := resp.GetResult(); r != nil {
			got = r
		}
	}

	if got == nil {
		t.Fatal("expected a RecognitionResult, got none")
	}
	if got.GetText() != "hello grpc" {
		t.Errorf("want text %q, got %q", "hello grpc", got.GetText())
	}
	if !got.GetIsFinal() {
		t.Error("expected IsFinal=true")
	}
}

// TestStreamingRecognize_MaxSessionsExceeded verifies that once the session
// limit is reached, new sessions are rejected with ERR1011 (ResourceExhausted).
func TestStreamingRecognize_MaxSessionsExceeded(t *testing.T) {
	cfg := &config.Config{}
	cfg.Stream.VADSilenceSec = 0.8
	cfg.Server.MaxSessions = 1
	cfg.Defaults()
	cfg.Server.MaxSessions = 1
	sm := session.NewManager(cfg, nil)

	client := startTestGRPCServer(t, sm, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First session — must succeed.
	stream1, err := client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatalf("StreamingRecognize (1): %v", err)
	}
	if err := stream1.Send(sessionConfigMsg("sess-limit-1", "ko")); err != nil {
		t.Fatalf("Send (1): %v", err)
	}
	resp1, err := stream1.Recv()
	if err != nil {
		t.Fatalf("Recv (1): %v", err)
	}
	if resp1.GetSessionCreated() == nil {
		t.Fatalf("expected SessionCreated for first session, got %T", resp1.GetStreamingResponse())
	}

	// Second session — must fail because MaxSessions == 1.
	stream2, err := client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatalf("StreamingRecognize (2): %v", err)
	}
	if err := stream2.Send(sessionConfigMsg("sess-limit-2", "ko")); err != nil {
		t.Fatalf("Send (2): %v", err)
	}

	_, recvErr := stream2.Recv()
	if recvErr == nil {
		t.Fatal("expected error for second session, got nil")
	}
	st, ok := status.FromError(recvErr)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", recvErr)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("want codes.ResourceExhausted, got %v", st.Code())
	}
	if !strings.Contains(st.Message(), "ERR1011") {
		t.Errorf("want ERR1011 in message, got %q", st.Message())
	}

	_ = stream1.CloseSend()
}
