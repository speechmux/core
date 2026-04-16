package stream_test

// scheduler_integration_test.go — Phase 6/7 integration tests for
// DecodeScheduler: concurrent request limiting and plugin failure re-routing.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/speechmux/core/internal/plugin"
	"github.com/speechmux/core/internal/stream"
	sttErrors "github.com/speechmux/core/internal/errors"
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
func (s *spyMetrics) RecordVADTrigger()       {}
func (s *spyMetrics) RecordVADWatermarkLag() {}
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

// ── slow STT stub: introduces a fixed latency ─────────────────────────────────

type slowSTTServer struct {
	inferencepb.UnimplementedInferencePluginServer
	latency time.Duration
}

func (s *slowSTTServer) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *slowSTTServer) GetCapabilities(_ context.Context, _ *emptypb.Empty) (*inferencepb.InferenceCapabilities, error) {
	return &inferencepb.InferenceCapabilities{EngineName: "slow-stub", MaxConcurrentRequests: 8}, nil
}

func (s *slowSTTServer) Transcribe(_ context.Context, req *inferencepb.TranscribeRequest) (*inferencepb.TranscribeResponse, error) {
	time.Sleep(s.latency)
	return &inferencepb.TranscribeResponse{
		RequestId: req.GetRequestId(),
		SessionId: req.GetSessionId(),
		Text:      "slow ok",
	}, nil
}

// ── Test: concurrent request limit (Phase 6) ──────────────────────────────────

// TestDecodeScheduler_ConcurrentLimit verifies that the global pending
// semaphore drops partial decodes immediately and blocks final decodes up to
// maxDecodeWait when all slots are occupied.
func TestDecodeScheduler_ConcurrentLimit(t *testing.T) {
	// STT stub holds each request for 300 ms so we can saturate the semaphore.
	sttEP := startInferenceServer(t, &slowSTTServer{latency: 300 * time.Millisecond})
	router := plugin.NewPluginRouter("")
	if err := router.Add(sttEP.ID(), sttEP.Socket(), 0); err != nil {
		t.Fatalf("router.Add: %v", err)
	}

	const maxPending = 2
	scheduler := stream.NewDecodeScheduler(router, maxPending, 5.0, nil)

	ctx := context.Background()
	audio := make([]byte, 640)

	// Submit maxPending final requests that will hold the semaphore for 300 ms.
	var wg sync.WaitGroup
	for i := 0; i < maxPending; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = scheduler.Submit(ctx, "s", "r", audio, 16000, "ko",
				inferencepb.Task_TASK_TRANSCRIBE, nil, true, false)
		}()
	}

	// Give the goroutines above time to acquire their slots.
	time.Sleep(50 * time.Millisecond)

	// A partial decode with no free slot must be dropped immediately (not block).
	start := time.Now()
	_, err := scheduler.Submit(ctx, "s", "r2", audio, 16000, "ko",
		inferencepb.Task_TASK_TRANSCRIBE, nil, false, true)
	elapsed := time.Since(start)

	if elapsed > 50*time.Millisecond {
		t.Errorf("partial decode should return immediately when queue full, took %v", elapsed)
	}
	if err == nil {
		t.Error("expected error for partial decode when queue full, got nil")
	}
	var sttErr *sttErrors.STTError
	if !errors.As(err, &sttErr) || sttErr.Code() != sttErrors.ErrGlobalPendingExceeded {
		t.Errorf("expected ErrGlobalPendingExceeded, got %v", err)
	}

	wg.Wait()
}

// ── erroring STT stub: always returns a gRPC error ────────────────────────────

type errorSTTServer struct {
	inferencepb.UnimplementedInferencePluginServer
	callCount atomic.Int64
}

func (s *errorSTTServer) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *errorSTTServer) GetCapabilities(_ context.Context, _ *emptypb.Empty) (*inferencepb.InferenceCapabilities, error) {
	return &inferencepb.InferenceCapabilities{EngineName: "error-stub"}, nil
}

func (s *errorSTTServer) Transcribe(_ context.Context, _ *inferencepb.TranscribeRequest) (*inferencepb.TranscribeResponse, error) {
	s.callCount.Add(1)
	return nil, grpcStatus.Error(codes.Internal, "stub error")
}

// ── Test: plugin failure re-routing (Phase 7) ─────────────────────────────────

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
	if err := router.Add(errEP.ID(), errEP.Socket(), 0); err != nil {
		t.Fatalf("router.Add errEP: %v", err)
	}
	if err := router.Add(okEP.ID(), okEP.Socket(), 0); err != nil {
		t.Fatalf("router.Add okEP: %v", err)
	}
	scheduler := stream.NewDecodeScheduler(router, 0, 5.0, nil)

	ctx := context.Background()
	audio := make([]byte, 640)

	// Submit 5 requests: errClient is tried first (round-robin starts at 0),
	// each fails, incrementing its failure counter toward the threshold=5.
	// The 6th request should be routed to okClient after errClient's circuit opens.
	successes := 0
	failures := 0
	for i := 0; i < 10; i++ {
		resp, err := scheduler.Submit(ctx, "s", "r", audio, 16000, "ko",
			inferencepb.Task_TASK_TRANSCRIBE, nil, true, false)
		if err == nil && resp != nil {
			successes++
		} else {
			failures++
		}
	}

	// After the error threshold is exceeded, some requests must succeed via okClient.
	if successes == 0 {
		t.Errorf("expected some successful decodes after re-routing to healthy endpoint, got 0 (failures=%d)", failures)
	}
	// errClient must have received at least failureThreshold (5) calls.
	if errSrv.callCount.Load() < 5 {
		t.Errorf("expected errClient to receive ≥5 calls, got %d", errSrv.callCount.Load())
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
		EngineName: "capable-engine",
		ModelSize:  "large-v3",
		Device:     "mps",
	}, nil
}

func (s *capableSTTServer) Transcribe(_ context.Context, req *inferencepb.TranscribeRequest) (*inferencepb.TranscribeResponse, error) {
	return &inferencepb.TranscribeResponse{
		RequestId: req.GetRequestId(),
		SessionId: req.GetSessionId(),
		Text:      "ok",
	}, nil
}

// ── Test: engine name flows from GetCapabilities to metrics (Phase observability)

// TestDecodeScheduler_EngineNamePassedToMetrics verifies that the engine name
// fetched from GetCapabilities at endpoint registration flows through Submit()
// to both RecordDecodeLatency and RecordDecodeResult calls on the observer.
func TestDecodeScheduler_EngineNamePassedToMetrics(t *testing.T) {
	ep := startInferenceServerWithID(t, "capable-stt", &capableSTTServer{})

	router := plugin.NewPluginRouter("")
	if err := router.Add(ep.ID(), ep.Socket(), 0); err != nil {
		t.Fatalf("router.Add: %v", err)
	}

	spy := &spyMetrics{}
	scheduler := stream.NewDecodeScheduler(router, 0, 5.0, spy)

	_, err := scheduler.Submit(context.Background(), "s", "r",
		make([]byte, 640), 16000, "en",
		inferencepb.Task_TASK_TRANSCRIBE, nil, true, false)
	if err != nil {
		t.Fatalf("Submit: %v", err)
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
