package transport

// pipeline_integration_test.go — Full-stack integration tests that exercise
// the complete Go pipeline from a gRPC client through GRPCServer →
// StreamProcessor → VAD stub → STT stub → recognition result returned to the
// gRPC client.

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
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

// ── pipeline test config ──────────────────────────────────────────────────────

type pipelineCfg struct {
	vadSilenceSec    float64
	decodeTimeoutSec float64
}

func defaultPipelineCfg() pipelineCfg {
	return pipelineCfg{vadSilenceSec: 0.15, decodeTimeoutSec: 5.0}
}

func buildPipelineConfig(pcfg pipelineCfg) *atomic.Pointer[config.Config] {
	cfg := &config.Config{}
	cfg.Stream.VADSilenceSec = pcfg.vadSilenceSec
	cfg.Stream.MaxBufferSec = 30
	cfg.Stream.DecodeTimeoutSec = pcfg.decodeTimeoutSec
	cfg.Codec.TargetSampleRate = 16000
	cfg.Defaults()
	cfg.Stream.VADSilenceSec = pcfg.vadSilenceSec
	cfg.Stream.DecodeTimeoutSec = pcfg.decodeTimeoutSec

	var ptr atomic.Pointer[config.Config]
	ptr.Store(cfg)
	return &ptr
}

// ── socket helpers ────────────────────────────────────────────────────────────

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
	sock := filepath.Join(pipelineTempSockDir(t), "v.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen VAD: %v", err)
	}
	gs := grpc.NewServer()
	vadpb.RegisterVADPluginServer(gs, srv)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(func() { gs.Stop() })
	ep, err := plugin.NewEndpoint("pipe-vad", sock, plugin.EndpointCircuitBreaker{})
	if err != nil {
		t.Fatalf("NewEndpoint VAD: %v", err)
	}
	t.Cleanup(func() { _ = ep.Close() })
	return ep
}

func startPipelineSTTServer(t *testing.T, srv inferencepb.InferencePluginServer) *plugin.Endpoint {
	t.Helper()
	sock := filepath.Join(pipelineTempSockDir(t), "s.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen STT: %v", err)
	}
	gs := grpc.NewServer()
	inferencepb.RegisterInferencePluginServer(gs, srv)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(func() { gs.Stop() })
	ep, err := plugin.NewEndpoint("pipe-stt", sock, plugin.EndpointCircuitBreaker{})
	if err != nil {
		t.Fatalf("NewEndpoint STT: %v", err)
	}
	t.Cleanup(func() { _ = ep.Close() })
	return ep
}

// ── stub servers ──────────────────────────────────────────────────────────────

// pipelineVADServer reports speech for frames 1–25, silence thereafter.
type pipelineVADServer struct {
	vadpb.UnimplementedVADPluginServer
}

func (s *pipelineVADServer) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *pipelineVADServer) GetCapabilities(_ context.Context, _ *emptypb.Empty) (*vadpb.VADCapabilities, error) {
	return &vadpb.VADCapabilities{OptimalFrameMs: 30}, nil
}

func (s *pipelineVADServer) StreamVAD(srv vadpb.VADPlugin_StreamVADServer) error {
	for {
		req, err := srv.Recv()
		if err != nil {
			return nil
		}
		if req.GetSessionStart() != nil || req.GetSessionEnd() != nil {
			continue
		}
		seq := req.GetSequenceNumber()
		isSpeech := seq >= 1 && seq <= 25
		if err := srv.Send(&vadpb.VADResponse{
			IsSpeech:          isSpeech,
			SpeechProbability: map[bool]float32{true: 0.95, false: 0.05}[isSpeech],
			SequenceNumber:    seq,
		}); err != nil {
			return nil
		}
	}
}

// pipelineSTTServer returns a fixed transcript for every Transcribe call.
type pipelineSTTServer struct {
	inferencepb.UnimplementedInferencePluginServer
	text string
}

func (s *pipelineSTTServer) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *pipelineSTTServer) GetCapabilities(_ context.Context, _ *emptypb.Empty) (*inferencepb.InferenceCapabilities, error) {
	return &inferencepb.InferenceCapabilities{EngineName: "pipe-stub", MaxConcurrentRequests: 8}, nil
}

func (s *pipelineSTTServer) Transcribe(_ context.Context, req *inferencepb.TranscribeRequest) (*inferencepb.TranscribeResponse, error) {
	return &inferencepb.TranscribeResponse{
		RequestId: req.GetRequestId(),
		SessionId: req.GetSessionId(),
		Text:      s.text,
	}, nil
}

// ── pipeline builder ──────────────────────────────────────────────────────────

func buildPipeline(t *testing.T, pcfg pipelineCfg, text string) (clientpb_ interface {
	StreamingRecognize(ctx context.Context, opts ...grpc.CallOption) (interface{}, error)
}, cancel func()) {
	t.Helper()

	vadEP := startPipelineVADServer(t, &pipelineVADServer{})
	sttEP := startPipelineSTTServer(t, &pipelineSTTServer{text: text})

	router := plugin.NewPluginRouter("")
	if err := router.Add(sttEP.ID(), sttEP.Socket(), 0); err != nil {
		t.Fatalf("router.Add: %v", err)
	}
	scheduler := stream.NewDecodeScheduler(router, 8, 0, pcfg.decodeTimeoutSec, nil)

	cfgPtr := buildPipelineConfig(pcfg)
	proc := stream.NewStreamProcessor(cfgPtr, []*plugin.Endpoint{vadEP}, scheduler, nil)

	cfg := &config.Config{}
	cfg.Defaults()
	sm := session.NewManager(cfg, nil)

	_ = proc
	_ = sm
	return nil, func() {}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestFullPipeline_IsLastFlushesResult verifies that sending is_last triggers a
// final EPD and the STT result flows back to the gRPC client.
func TestFullPipeline_IsLastFlushesResult(t *testing.T) {
	pcfg := defaultPipelineCfg()

	vadEP := startPipelineVADServer(t, &pipelineVADServer{})
	sttEP := startPipelineSTTServer(t, &pipelineSTTServer{text: "pipeline result"})

	router := plugin.NewPluginRouter("")
	if err := router.Add(sttEP.ID(), sttEP.Socket(), 0); err != nil {
		t.Fatalf("router.Add: %v", err)
	}
	scheduler := stream.NewDecodeScheduler(router, 8, 0, pcfg.decodeTimeoutSec, nil)

	cfgPtr := buildPipelineConfig(pcfg)
	proc := stream.NewStreamProcessor(cfgPtr, []*plugin.Endpoint{vadEP}, scheduler, nil)

	cfg := &config.Config{}
	cfg.Defaults()
	sm := session.NewManager(cfg, nil)

	client := startTestGRPCServer(t, sm, proc)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rpcStream, err := client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatalf("StreamingRecognize: %v", err)
	}

	if err := rpcStream.Send(sessionConfigMsg("pipe-islast", "ko")); err != nil {
		t.Fatalf("Send config: %v", err)
	}
	if _, err := rpcStream.Recv(); err != nil {
		t.Fatalf("Recv SessionCreated: %v", err)
	}

	// Send 35 frames: 25 speech + 10 silence to trigger EPD.
	frame := make([]byte, 960) // 30 ms @ 16 kHz S16LE
	for i := 0; i < 35; i++ {
		if err := rpcStream.Send(audioMsg(frame)); err != nil {
			t.Fatalf("Send audio[%d]: %v", i, err)
		}
	}

	// is_last signals end of audio → final EPD → final decode.
	if err := rpcStream.Send(isLastMsg()); err != nil {
		t.Fatalf("Send isLast: %v", err)
	}
	if err := rpcStream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	// Collect all results until EOF.
	var resultCount int
	for {
		resp, err := rpcStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Logf("Recv error (may be normal on clean shutdown): %v", err)
			break
		}
		if resp.GetResult() != nil {
			resultCount++
		}
	}

	if resultCount == 0 {
		t.Fatal("expected a RecognitionResult from is_last flush, got none")
	}
}

// TestFullPipeline_AudioEndSignal verifies that the pipeline exits cleanly
// after AudioInCh is closed by the is_last signal.
func TestFullPipeline_AudioEndSignal(t *testing.T) {
	pcfg := defaultPipelineCfg()

	vadEP := startPipelineVADServer(t, &pipelineVADServer{})

	cfgPtr := buildPipelineConfig(pcfg)
	proc := stream.NewStreamProcessor(cfgPtr, []*plugin.Endpoint{vadEP}, nil, nil)

	cfg := &config.Config{}
	cfg.Defaults()
	sm := session.NewManager(cfg, nil)

	client := startTestGRPCServer(t, sm, proc)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rpcStream, err := client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatalf("StreamingRecognize: %v", err)
	}

	if err := rpcStream.Send(sessionConfigMsg("pipe-end", "ko")); err != nil {
		t.Fatalf("Send config: %v", err)
	}
	if _, err := rpcStream.Recv(); err != nil {
		t.Fatalf("Recv SessionCreated: %v", err)
	}

	// Send a few frames then is_last.
	frame := make([]byte, 960)
	for i := 0; i < 5; i++ {
		if err := rpcStream.Send(audioMsg(frame)); err != nil {
			t.Fatalf("Send audio[%d]: %v", i, err)
		}
	}
	if err := rpcStream.Send(isLastMsg()); err != nil {
		t.Fatalf("Send isLast: %v", err)
	}
	if err := rpcStream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	// Stream must close within 5 s.
	done := make(chan struct{})
	go func() {
		for {
			_, err := rpcStream.Recv()
			if err != nil {
				close(done)
				return
			}
		}
	}()
	select {
	case <-done:
		// clean exit
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not exit cleanly within 5 s after is_last")
	}
}
