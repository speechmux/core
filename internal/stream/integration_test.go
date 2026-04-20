package stream_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/speechmux/core/internal/config"
	"github.com/speechmux/core/internal/plugin"
	"github.com/speechmux/core/internal/session"
	"github.com/speechmux/core/internal/stream"
	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
	vadpb "github.com/speechmux/proto/gen/go/vad/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// tempSockDir creates a short-path temp directory suitable for Unix sockets.
// macOS limits socket paths to 104 bytes, so we use /tmp directly.
func tempSockDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "smux")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func startVADServer(t *testing.T, srv vadpb.VADPluginServer) *plugin.Endpoint {
	t.Helper()
	sock := filepath.Join(tempSockDir(t), "v.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	vadpb.RegisterVADPluginServer(gs, srv)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(func() { gs.Stop() })
	ep, err := plugin.NewEndpoint("test-vad", sock, plugin.EndpointCircuitBreaker{})
	if err != nil {
		t.Fatalf("new endpoint: %v", err)
	}
	t.Cleanup(func() { _ = ep.Close() })
	return ep
}

func startInferenceServer(t *testing.T, srv inferencepb.InferencePluginServer) *plugin.Endpoint {
	t.Helper()
	return startInferenceServerWithID(t, "test-stt", srv)
}

func startInferenceServerWithID(t *testing.T, id string, srv inferencepb.InferencePluginServer) *plugin.Endpoint {
	t.Helper()
	sock := filepath.Join(tempSockDir(t), "s.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	inferencepb.RegisterInferencePluginServer(gs, srv)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(func() { gs.Stop() })
	ep, err := plugin.NewEndpoint(id, sock, plugin.EndpointCircuitBreaker{})
	if err != nil {
		t.Fatalf("new endpoint: %v", err)
	}
	t.Cleanup(func() { _ = ep.Close() })
	return ep
}

func newTestConfig(vadSilenceSec, decodeTimeoutSec float64) *atomic.Pointer[config.Config] {
	cfg := &config.Config{}
	cfg.Stream.VADSilenceSec = vadSilenceSec
	cfg.Stream.MaxBufferSec = 30
	cfg.Stream.DecodeTimeoutSec = decodeTimeoutSec
	cfg.Codec.TargetSampleRate = 16000
	cfg.Defaults()
	// Override after Defaults so our values take effect.
	cfg.Stream.VADSilenceSec = vadSilenceSec
	cfg.Stream.DecodeTimeoutSec = decodeTimeoutSec

	var ptr atomic.Pointer[config.Config]
	ptr.Store(cfg)
	return &ptr
}

// ── stub VAD server: speech for seq 1–25, silence for seq 26+ ─────────────────

type epdVADServer struct {
	vadpb.UnimplementedVADPluginServer
}

func (s *epdVADServer) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *epdVADServer) GetCapabilities(_ context.Context, _ *emptypb.Empty) (*vadpb.VADCapabilities, error) {
	return &vadpb.VADCapabilities{OptimalFrameMs: 30}, nil
}

func (s *epdVADServer) StreamVAD(stream vadpb.VADPlugin_StreamVADServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		// Skip the SessionStart signal message — it has no sequence number.
		if req.GetSessionStart() != nil || req.GetSessionEnd() != nil {
			continue
		}
		seq := req.GetSequenceNumber()
		isSpeech := seq >= 1 && seq <= 25
		if err := stream.Send(&vadpb.VADResponse{
			IsSpeech:          isSpeech,
			SpeechProbability: map[bool]float32{true: 0.95, false: 0.05}[isSpeech],
			SequenceNumber:    seq,
		}); err != nil {
			return nil
		}
	}
}

// ── stub STT server ────────────────────────────────────────────────────────────

type helloSTTServer struct {
	inferencepb.UnimplementedInferencePluginServer
}

func (s *helloSTTServer) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *helloSTTServer) Transcribe(_ context.Context, req *inferencepb.TranscribeRequest) (*inferencepb.TranscribeResponse, error) {
	return &inferencepb.TranscribeResponse{
		RequestId: req.RequestId,
		SessionId: req.SessionId,
		Text:      "hello test",
	}, nil
}

// ── Test 1: EPD trigger ────────────────────────────────────────────────────────

func TestStreamProcessor_EPDTrigger(t *testing.T) {
	vadEP := startVADServer(t, &epdVADServer{})
	sttEP := startInferenceServer(t, &helloSTTServer{})

	router := plugin.NewPluginRouter("")
	if err := router.Add(sttEP.ID(), sttEP.Socket(), 0); err != nil {
		t.Fatalf("router.Add: %v", err)
	}
	scheduler := stream.NewDecodeScheduler(router, 4, 0, 5.0, nil)

	cfgPtr := newTestConfig(0.15, 5.0)
	proc := stream.NewStreamProcessor(cfgPtr, []*plugin.Endpoint{vadEP}, scheduler, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess := session.NewTestSession(ctx, "epd-test")
	sess.Info.VADSilence = 0.15

	errCh := make(chan error, 1)
	go func() {
		errCh <- proc.ProcessSession(ctx, sess)
	}()

	// Send 35 frames of 640-byte silence (20 ms @ 16 kHz S16LE each).
	frame := make([]byte, 640)
	for i := 0; i < 35; i++ {
		select {
		case sess.AudioInCh <- frame:
		case <-ctx.Done():
			t.Error("context expired while sending audio frames")
			return
		}
	}

	// Wait for a result (up to 3 s).
	var result interface{ GetText() string }
	select {
	case r := <-sess.ResultCh:
		result = r
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for recognition result")
	}

	if result == nil {
		t.Fatal("received nil result")
	}
	text := result.GetText()
	if !strings.Contains(text, "hello test") {
		t.Errorf("expected text to contain %q, got %q", "hello test", text)
	}

	// Cancel context and wait for ProcessSession to return.
	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			// Allow gRPC-wrapped context errors too.
			t.Logf("ProcessSession returned: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("ProcessSession did not return after context cancel")
	}
}

// ── stub VAD that fails after N frames ────────────────────────────────────────

type failAfterNVADServer struct {
	vadpb.UnimplementedVADPluginServer
	maxFrames int
}

func (s *failAfterNVADServer) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *failAfterNVADServer) GetCapabilities(_ context.Context, _ *emptypb.Empty) (*vadpb.VADCapabilities, error) {
	return &vadpb.VADCapabilities{OptimalFrameMs: 30}, nil
}

func (s *failAfterNVADServer) StreamVAD(stream vadpb.VADPlugin_StreamVADServer) error {
	count := 0
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		if req.GetSessionStart() != nil || req.GetSessionEnd() != nil {
			continue
		}
		count++
		if count > s.maxFrames {
			return fmt.Errorf("stub vad error")
		}
		seq := req.GetSequenceNumber()
		if err := stream.Send(&vadpb.VADResponse{
			IsSpeech:       false,
			SequenceNumber: seq,
		}); err != nil {
			return nil
		}
	}
}

// ── Test 2: VAD plugin failure ─────────────────────────────────────────────────

func TestStreamProcessor_VADPluginFailure(t *testing.T) {
	vadEP := startVADServer(t, &failAfterNVADServer{maxFrames: 5})

	cfgPtr := newTestConfig(0.5, 5.0)
	// No scheduler — decode never fires.
	proc := stream.NewStreamProcessor(cfgPtr, []*plugin.Endpoint{vadEP}, nil, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(cancel)

	sess := session.NewTestSession(ctx, "vad-fail-test")

	errCh := make(chan error, 1)
	go func() {
		errCh <- proc.ProcessSession(ctx, sess)
	}()

	frame := make([]byte, 640)
	for i := 0; i < 10; i++ {
		select {
		case sess.AudioInCh <- frame:
		case <-ctx.Done():
			return
		}
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected non-nil error when VAD plugin fails, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Error("ProcessSession did not return within 5 s after VAD failure")
	}
}

// ── stub VAD: echoes sequence numbers, returns is_speech=false ────────────────

type echoVADServer struct {
	vadpb.UnimplementedVADPluginServer
}

func (s *echoVADServer) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *echoVADServer) GetCapabilities(_ context.Context, _ *emptypb.Empty) (*vadpb.VADCapabilities, error) {
	return &vadpb.VADCapabilities{OptimalFrameMs: 30}, nil
}

func (s *echoVADServer) StreamVAD(stream vadpb.VADPlugin_StreamVADServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		if req.GetSessionStart() != nil || req.GetSessionEnd() != nil {
			continue
		}
		if err := stream.Send(&vadpb.VADResponse{
			IsSpeech:       false,
			SequenceNumber: req.GetSequenceNumber(),
		}); err != nil {
			return nil
		}
	}
}

// ── Test 3: multiple concurrent VAD streams ────────────────────────────────────

func TestVADClient_MultipleConcurrentStreams(t *testing.T) {
	vadEP := startVADServer(t, &echoVADServer{})

	const numSessions = 5
	const framesPerSession = 10

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	type result struct {
		seqs []uint64
		err  error
	}
	results := make([]result, numSessions)

	for i := 0; i < numSessions; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sessionID := fmt.Sprintf("concurrent-sess-%d", idx)
			vc, err := plugin.NewVADClient(ctx, vadEP, sessionID, 0.5, 16000)
			if err != nil {
				results[idx] = result{err: fmt.Errorf("NewVADClient: %w", err)}
				return
			}
			defer vc.Close()

			frame := make([]byte, 640)
			for j := 0; j < framesPerSession; j++ {
				if _, err := vc.Send(frame, 16000); err != nil {
					results[idx] = result{err: fmt.Errorf("Send: %w", err)}
					return
				}
			}

			var seqs []uint64
			for j := 0; j < framesPerSession; j++ {
				resp, err := vc.Recv()
				if err != nil {
					results[idx] = result{err: fmt.Errorf("Recv: %w", err)}
					return
				}
				seqs = append(seqs, resp.SequenceNumber)
			}
			results[idx] = result{seqs: seqs}
		}(i)
	}

	wg.Wait()

	for i, r := range results {
		if r.err != nil {
			t.Errorf("session %d error: %v", i, r.err)
			continue
		}
		if len(r.seqs) != framesPerSession {
			t.Errorf("session %d: expected %d sequence numbers, got %d", i, framesPerSession, len(r.seqs))
			continue
		}
		// Verify that sequence numbers are 1..framesPerSession (monotonically increasing).
		for j, seq := range r.seqs {
			expected := uint64(j + 1)
			if seq != expected {
				t.Errorf("session %d: frame %d: expected seq %d, got %d", i, j, expected, seq)
			}
		}
	}
}
