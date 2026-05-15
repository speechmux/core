package stream_test

// scheduler_integration_test.go — integration tests for fair decode dispatch:
// plugin failure re-routing and engine name metrics flow.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/speechmux/core/internal/plugin"
	"github.com/speechmux/core/internal/stream"
	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ── spy MetricsObserver ───────────────────────────────────────────────────────

type spyMetrics struct {
	mu                  sync.Mutex
	decodeLatencyEngine []string
	decodeResultEngine  []string
}

func (s *spyMetrics) IncActiveSessions()  {}
func (s *spyMetrics) DecActiveSessions()  {}
func (s *spyMetrics) RecordVADTrigger()   {}
func (s *spyMetrics) RecordVADWatermarkLag() {}
func (s *spyMetrics) RecordStreamingSessionOpen(_ string)                {}
func (s *spyMetrics) RecordStreamingSessionClose(_ string, _ string)     {}
func (s *spyMetrics) RecordStreamingPartialLatency(_ float64, _ string)  {}
func (s *spyMetrics) RecordStreamingFinalizeLatency(_ float64, _ string) {}
func (s *spyMetrics) RecordEngineResponseTimeout(_ string)               {}
func (s *spyMetrics) RecordDecodeLatency(_ float64, _ bool, engineName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.decodeLatencyEngine = append(s.decodeLatencyEngine, engineName)
}
func (s *spyMetrics) RecordDecodeResult(_ bool, _ bool, engineName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.decodeResultEngine = append(s.decodeResultEngine, engineName)
}
func (s *spyMetrics) RecordFairDispatchQueueDepth(_ string, _ int) {}
func (s *spyMetrics) RecordFairDispatchPartialCancelled(_ string)  {}
func (s *spyMetrics) RecordFairDispatchWaitSec(_ float64, _ bool)  {}

// ── erroring STT stub: always returns a gRPC error ────────────────────────────

type errorSTTServer struct {
	inferencepb.UnimplementedInferencePluginServer
	callCount atomic.Int64
}

func (s *errorSTTServer) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *errorSTTServer) GetCapabilities(_ context.Context, _ *emptypb.Empty) (*inferencepb.InferenceCapabilities, error) {
	return &inferencepb.InferenceCapabilities{
		EngineName:    "error-stub",
		StreamingMode: inferencepb.StreamingMode_STREAMING_MODE_BATCH_ONLY,
	}, nil
}

func (s *errorSTTServer) Transcribe(_ context.Context, _ *inferencepb.TranscribeRequest) (*inferencepb.TranscribeResponse, error) {
	s.callCount.Add(1)
	return nil, grpcStatus.Error(codes.Internal, "stub error")
}

// ── Test: plugin failure re-routing ──────────────────────────────────────────

// TestDecodeScheduler_PluginFailureRerouting verifies that once an inference
// endpoint's circuit breaker opens (after failureThreshold=5 failures), the
// router skips it and routes subsequent requests to a healthy endpoint.
func TestDecodeScheduler_PluginFailureRerouting(t *testing.T) {
	// Endpoint A: always errors → circuit will open after 5 failures.
	errSrv := &errorSTTServer{}
	errEP := startInferenceServerWithID(t, "test-stt-err", errSrv)
	// Endpoint B: always succeeds.
	okEP := startInferenceServerWithID(t, "test-stt-ok", &helloSTTServer{})

	router := plugin.NewPluginRouter("")
	if err := router.Add(errEP.ID(), errEP.Socket(), "", 0); err != nil {
		t.Fatalf("router.Add errEP: %v", err)
	}
	if err := router.Add(okEP.ID(), okEP.Socket(), "", 0); err != nil {
		t.Fatalf("router.Add okEP: %v", err)
	}

	d := stream.NewFairDecodeDispatcher(router, nil, 0, 0, 5.0)
	t.Cleanup(func() { d.Shutdown() })

	ctx := context.Background()
	audio := make([]byte, 640)

	// Submit 10 requests sequentially (each waits for the previous result).
	// Round-robin alternates between errEP and okEP; after 5 failures errEP's
	// circuit opens and all remaining requests route to okEP.
	successes := 0
	failures := 0
	for i := 0; i < 10; i++ {
		ch := d.Enqueue(&stream.BatchTask{
			Ctx:          ctx,
			SessionID:    "s",
			RequestID:    "r",
			AudioData:    audio,
			SampleRate:   16000,
			LanguageCode: "ko",
			Task:         inferencepb.Task_TASK_TRANSCRIBE,
			IsFinal:      true,
		})
		result := <-ch
		if result.Err == nil && result.Resp != nil {
			successes++
		} else {
			failures++
		}
	}

	// After the error threshold is exceeded, some requests must succeed via okEP.
	if successes == 0 {
		t.Errorf("expected some successful decodes after re-routing to healthy endpoint, got 0 (failures=%d)", failures)
	}
	// errEP must have received at least failureThreshold (5) calls.
	if errSrv.callCount.Load() < 5 {
		t.Errorf("expected errEP to receive ≥5 calls, got %d", errSrv.callCount.Load())
	}
}

// ── capable STT stub: reports engine identity via GetCapabilities ─────────────

type capableSTTServer struct {
	inferencepb.UnimplementedInferencePluginServer
}

func (s *capableSTTServer) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *capableSTTServer) GetCapabilities(_ context.Context, _ *emptypb.Empty) (*inferencepb.InferenceCapabilities, error) {
	return &inferencepb.InferenceCapabilities{
		EngineName:    "capable-engine",
		ModelSize:     "large-v3",
		Device:        "mps",
		StreamingMode: inferencepb.StreamingMode_STREAMING_MODE_BATCH_ONLY,
	}, nil
}

func (s *capableSTTServer) Transcribe(_ context.Context, req *inferencepb.TranscribeRequest) (*inferencepb.TranscribeResponse, error) {
	return &inferencepb.TranscribeResponse{
		RequestId: req.GetRequestId(),
		SessionId: req.GetSessionId(),
		Text:      "ok",
	}, nil
}

// ── Test: engine name flows from GetCapabilities to metrics ──────────────────

// TestDecodeScheduler_EngineNamePassedToMetrics verifies that the engine name
// fetched from GetCapabilities flows through FairDecodeDispatcher.Enqueue()
// to both RecordDecodeLatency and RecordDecodeResult on the observer.
func TestDecodeScheduler_EngineNamePassedToMetrics(t *testing.T) {
	ep := startInferenceServerWithID(t, "capable-stt", &capableSTTServer{})

	router := plugin.NewPluginRouter("")
	if err := router.Add(ep.ID(), ep.Socket(), "", 0); err != nil {
		t.Fatalf("router.Add: %v", err)
	}

	spy := &spyMetrics{}
	d := stream.NewFairDecodeDispatcher(router, spy, 0, 0, 5.0)
	t.Cleanup(func() { d.Shutdown() })

	ch := d.Enqueue(&stream.BatchTask{
		Ctx:          context.Background(),
		SessionID:    "s",
		RequestID:    "r",
		AudioData:    make([]byte, 640),
		SampleRate:   16000,
		LanguageCode: "en",
		Task:         inferencepb.Task_TASK_TRANSCRIBE,
		IsFinal:      true,
	})
	result := <-ch
	if result.Err != nil {
		t.Fatalf("Enqueue: %v", result.Err)
	}

	spy.mu.Lock()
	latencyEngines := append([]string(nil), spy.decodeLatencyEngine...)
	resultEngines := append([]string(nil), spy.decodeResultEngine...)
	spy.mu.Unlock()

	if len(latencyEngines) == 0 {
		t.Fatal("RecordDecodeLatency was not called")
	}
	if latencyEngines[0] != "capable-engine" {
		t.Errorf("RecordDecodeLatency engine = %q, want %q", latencyEngines[0], "capable-engine")
	}
	if len(resultEngines) == 0 {
		t.Fatal("RecordDecodeResult was not called")
	}
	if resultEngines[0] != "capable-engine" {
		t.Errorf("RecordDecodeResult engine = %q, want %q", resultEngines[0], "capable-engine")
	}
}
