package transport

// pipeline_integration_test.go — Full-stack integration tests that exercise
// the complete Go pipeline from a gRPC client through GRPCServer →
// StreamProcessor → VAD stub → STT stub → recognition result returned to the
// client.
//
// These tests complement the unit-level tests in grpc_server_test.go (which use
// stub processors) and the stream-level tests in stream/integration_test.go
// (which bypass the gRPC transport layer).

import (
	"context"
	"io"
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
	"github.com/speechmux/core/internal/stream"
	clientpb "github.com/speechmux/proto/gen/go/client/v1"
	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
	vadpb "github.com/speechmux/proto/gen/go/vad/v1"
	"google.golang.org/grpc"
)

// ── pipeline builder ──────────────────────────────────────────────────────────

// pipelineConfig holds the parameters for buildPipeline.
type pipelineConfig struct {
	vadSilenceSec    float64
	decodeTimeoutSec float64
	maxSessions      int
}

// pipelineComponents holds the assembled pipeline for use in a test.
type pipelineComponents struct {
	client clientpb.STTServiceClient
}

// buildPipeline starts stub VAD and STT gRPC servers, wires them through
// StreamProcessor and GRPCServer, and returns a gRPC client connected via
// bufconn. All resources are cleaned up via t.Cleanup.
func buildPipeline(
	t *testing.T,
	vadSrv vadpb.VADPluginServer,
	sttSrv inferencepb.InferencePluginServer,
	pcfg pipelineConfig,
) *pipelineComponents {
	t.Helper()

	// ── 1. Start stub plugin servers on Unix sockets ──────────────────────
	vadEP := startPipelineVADServer(t, vadSrv)
	sttEP := startPipelineSTTServer(t, sttSrv)

	// ── 2. Build Core routing layer ───────────────────────────────────────
	router := plugin.NewPluginRouter("")
	if err := router.Add(sttEP.ID(), sttEP.Socket(), 0); err != nil {
		t.Fatalf("router.Add stt: %v", err)
	}
	scheduler := stream.NewDecodeScheduler(router, 8, pcfg.decodeTimeoutSec, nil)

	// ── 3. Build StreamProcessor ──────────────────────────────────────────
	cfg := &config.Config{}
	cfg.Stream.VADSilenceSec = pcfg.vadSilenceSec
	cfg.Stream.MaxBufferSec = 30
	cfg.Stream.DecodeTimeoutSec = pcfg.decodeTimeoutSec
	cfg.Codec.TargetSampleRate = 16000
	cfg.Defaults()
	cfg.Stream.VADSilenceSec = pcfg.vadSilenceSec
	cfg.Stream.DecodeTimeoutSec = pcfg.decodeTimeoutSec

	var cfgPtr atomic.Pointer[config.Config]
	cfgPtr.Store(cfg)
	proc := stream.NewStreamProcessor(&cfgPtr, []*plugin.Endpoint{vadEP}, scheduler, nil)

	// ── 4. Build GRPCServer ───────────────────────────────────────────────
	smCfg := &config.Config{}
	smCfg.Stream.VADSilenceSec = 0.8
	smCfg.Server.MaxSessions = pcfg.maxSessions
	smCfg.Defaults()
	smCfg.Server.MaxSessions = pcfg.maxSessions
	sm := newTestSessionManager()
	if pcfg.maxSessions > 0 {
		smCfg.Server.MaxSessions = pcfg.maxSessions
	}
	_ = smCfg
	grpcClient := startTestGRPCServer(t, sm, proc)

	return &pipelineComponents{client: grpcClient}
}

// ── Unix socket helpers ───────────────────────────────────────────────────────

func pipelineTempSockDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "smux-pipe")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func startPipelineVADServer(t *testing.T, srv vadpb.VADPluginServer) *plugin.Endpoint {
	t.Helper()
	sock := filepath.Join(pipelineTempSockDir(t), "vad.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen vad: %v", err)
	}
	gs := grpc.NewServer()
	vadpb.RegisterVADPluginServer(gs, srv)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(func() { gs.Stop() })
	ep, err := plugin.NewEndpoint("pipe-vad", sock, plugin.EndpointCircuitBreaker{})
	if err != nil {
		t.Fatalf("NewEndpoint vad: %v", err)
	}
	t.Cleanup(func() { _ = ep.Close() })
	return ep
}

func startPipelineSTTServer(t *testing.T, srv inferencepb.InferencePluginServer) *plugin.Endpoint {
	t.Helper()
	sock := filepath.Join(pipelineTempSockDir(t), "stt.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen stt: %v", err)
	}
	gs := grpc.NewServer()
	inferencepb.RegisterInferencePluginServer(gs, srv)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(func() { gs.Stop() })
	ep, err := plugin.NewEndpoint("pipe-stt", sock, plugin.EndpointCircuitBreaker{})
	if err != nil {
		t.Fatalf("NewEndpoint stt: %v", err)
	}
	t.Cleanup(func() { _ = ep.Close() })
	return ep
}

// ── stub plugin servers ───────────────────────────────────────────────────────

// pipelineVADServer marks frames 1–25 as speech and the rest as silence.
// This triggers EPD after the silence window expires.
type pipelineVADServer struct {
	vadpb.UnimplementedVADPluginServer
}

func (s *pipelineVADServer) HealthCheck(_ context.Context, _ *vadpb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *pipelineVADServer) GetCapabilities(_ context.Context, _ *vadpb.Empty) (*vadpb.VADCapabilities, error) {
	return &vadpb.VADCapabilities{OptimalFrameMs: 30}, nil
}

func (s *pipelineVADServer) StreamVAD(strm vadpb.VADPlugin_StreamVADServer) error {
	for {
		req, err := strm.Recv()
		if err != nil {
			return nil
		}
		if req.GetSessionStart() != nil || req.GetSessionEnd() != nil {
			continue
		}
		seq := req.GetSequenceNumber()
		isSpeech := seq >= 1 && seq <= 25
		if err := strm.Send(&vadpb.VADResponse{
			IsSpeech:          isSpeech,
			SpeechProbability: map[bool]float32{true: 0.95, false: 0.05}[isSpeech],
			SequenceNumber:    seq,
		}); err != nil {
			return nil
		}
	}
}

// pipelineSTTServer echoes a fixed text and records call count.
type pipelineSTTServer struct {
	inferencepb.UnimplementedInferencePluginServer
	text  string
	calls atomic.Int64
}

func (s *pipelineSTTServer) HealthCheck(_ context.Context, _ *inferencepb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *pipelineSTTServer) Transcribe(_ context.Context, req *inferencepb.TranscribeRequest) (*inferencepb.TranscribeResponse, error) {
	s.calls.Add(1)
	return &inferencepb.TranscribeResponse{
		RequestId: req.RequestId,
		SessionId: req.SessionId,
		Text:      s.text,
	}, nil
}

// ── Test 1: Full pipeline EPD trigger ─────────────────────────────────────────

// TestFullPipeline_EPDTriggerToGRPCClient exercises the complete path:
//
//	gRPC client → GRPCServer → StreamProcessor → VAD stub (EPD)
//	→ DecodeScheduler → STT stub → ResultCh → gRPC client receives result
func TestFullPipeline_EPDTriggerToGRPCClient(t *testing.T) {
	sttSrv := &pipelineSTTServer{text: "pipeline hello"}
	p := buildPipeline(t, &pipelineVADServer{}, sttSrv, pipelineConfig{
		vadSilenceSec:    0.15,
		decodeTimeoutSec: 5.0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	stream, err := p.client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatalf("StreamingRecognize: %v", err)
	}

	// Session setup.
	if err := stream.Send(sessionConfigMsg("pipe-sess-1", "ko")); err != nil {
		t.Fatalf("Send config: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv SessionCreated: %v", err)
	}
	if resp.GetSessionCreated() == nil {
		t.Fatalf("expected SessionCreated, got %T", resp.GetStreamingResponse())
	}

	// Send 35 frames — first 25 are "speech", then silence triggers EPD.
	frame := make([]byte, 640) // 20 ms @ 16 kHz S16LE
	for i := 0; i < 35; i++ {
		if err := stream.Send(audioMsg(frame)); err != nil {
			t.Fatalf("Send audio[%d]: %v", i, err)
		}
	}

	// Wait for the recognition result.
	var result *clientpb.RecognitionResult
	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Context deadline from outer timeout triggers here too; tolerate it.
			if waitCtx.Err() != nil {
				break
			}
			t.Logf("Recv error: %v", err)
			break
		}
		if r := resp.GetResult(); r != nil {
			result = r
			break
		}
	}

	if result == nil {
		t.Fatal("expected a RecognitionResult, got none")
	}
	if !strings.Contains(result.GetText(), "pipeline hello") {
		t.Errorf("want text to contain %q, got %q", "pipeline hello", result.GetText())
	}
	if sttSrv.calls.Load() == 0 {
		t.Error("STT stub should have been called at least once")
	}

	cancel()
}

// ── Test 2: Concurrent sessions with full pipeline ───────────────────────────

// TestFullPipeline_ConcurrentSessions opens N simultaneous gRPC
// StreamingRecognize streams and verifies that each session:
//  1. Receives a SessionCreated response with its own session_id.
//  2. Receives a recognition result from the STT stub.
//  3. Sessions are isolated — no cross-contamination.
func TestFullPipeline_ConcurrentSessions(t *testing.T) {
	sttSrv := &pipelineSTTServer{text: "concurrent ok"}
	p := buildPipeline(t, &pipelineVADServer{}, sttSrv, pipelineConfig{
		vadSilenceSec:    0.15,
		decodeTimeoutSec: 5.0,
	})

	const numSessions = 3

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	type sessionResult struct {
		sessionID string
		text      string
		err       error
	}
	results := make([]sessionResult, numSessions)
	var wg sync.WaitGroup

	for i := 0; i < numSessions; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			sessID := "concurrent-pipe-" + string(rune('a'+idx))
			strm, err := p.client.StreamingRecognize(ctx)
			if err != nil {
				results[idx] = sessionResult{err: err}
				return
			}

			if err := strm.Send(sessionConfigMsg(sessID, "ko")); err != nil {
				results[idx] = sessionResult{err: err}
				return
			}
			resp, err := strm.Recv()
			if err != nil {
				results[idx] = sessionResult{err: err}
				return
			}
			sc := resp.GetSessionCreated()
			if sc == nil || sc.GetSessionId() != sessID {
				results[idx] = sessionResult{err: nil, sessionID: sessID}
				return
			}

			frame := make([]byte, 640)
			for j := 0; j < 35; j++ {
				if err := strm.Send(audioMsg(frame)); err != nil {
					results[idx] = sessionResult{sessionID: sessID, err: err}
					return
				}
			}

			var text string
			waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
			defer waitCancel()
			for {
				resp, err := strm.Recv()
				if err != nil {
					break
				}
				if r := resp.GetResult(); r != nil {
					text = r.GetText()
					break
				}
				if waitCtx.Err() != nil {
					break
				}
			}
			results[idx] = sessionResult{sessionID: sessID, text: text}
		}(i)
	}

	wg.Wait()

	for i, r := range results {
		if r.err != nil {
			t.Errorf("session %d error: %v", i, r.err)
			continue
		}
		if !strings.Contains(r.text, "concurrent ok") {
			t.Errorf("session %d (%s): want text containing %q, got %q",
				i, r.sessionID, "concurrent ok", r.text)
		}
	}

	if sttSrv.calls.Load() == 0 {
		t.Error("STT stub should have been called at least once across all sessions")
	}
}

// ── Test 3: is_last triggers final decode ────────────────────────────────────

// TestFullPipeline_IsLastFlushesResult verifies that sending is_last when
// there is buffered speech (but the EPD silence window has not yet elapsed)
// still triggers a final decode and delivers the result to the client.
func TestFullPipeline_IsLastFlushesResult(t *testing.T) {
	// Use a very long silence window so EPD would not fire on its own within
	// the test timeout.  The is_last signal must flush the in-progress utterance.
	sttSrv := &pipelineSTTServer{text: "flushed hello"}
	p := buildPipeline(t, &pipelineVADServer{}, sttSrv, pipelineConfig{
		vadSilenceSec:    10.0, // intentionally long — is_last must bypass this
		decodeTimeoutSec: 5.0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	strm, err := p.client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatalf("StreamingRecognize: %v", err)
	}

	if err := strm.Send(sessionConfigMsg("islast-pipe", "ko")); err != nil {
		t.Fatalf("Send config: %v", err)
	}
	if _, err := strm.Recv(); err != nil {
		t.Fatalf("Recv SessionCreated: %v", err)
	}

	// Send 20 speech frames (seq 1–20 → marked as speech by pipelineVADServer).
	frame := make([]byte, 640)
	for i := 0; i < 20; i++ {
		if err := strm.Send(audioMsg(frame)); err != nil {
			t.Fatalf("Send audio[%d]: %v", i, err)
		}
	}

	// Signal end-of-audio — EPDController must flush the in-progress utterance.
	if err := strm.Send(isLastMsg()); err != nil {
		t.Fatalf("Send isLast: %v", err)
	}
	if err := strm.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	var result *clientpb.RecognitionResult
	for {
		resp, err := strm.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Logf("Recv: %v", err)
			break
		}
		if r := resp.GetResult(); r != nil {
			result = r
		}
	}

	if result == nil {
		t.Fatal("expected a RecognitionResult from is_last flush, got none")
	}
	if !strings.Contains(result.GetText(), "flushed hello") {
		t.Errorf("want %q in result text, got %q", "flushed hello", result.GetText())
	}
}
