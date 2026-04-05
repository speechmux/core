package plugin

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
	"google.golang.org/grpc"
)

// ── mock InferencePluginClient ────────────────────────────────────────────────

// mockInferencePlugin is a hand-rolled mock of inferencepb.InferencePluginClient.
// Each method delegates to a corresponding function field so individual tests
// can inject behaviour without needing a real gRPC server.
type mockInferencePlugin struct {
	transcribeFn    func(ctx context.Context, req *inferencepb.TranscribeRequest, opts ...grpc.CallOption) (*inferencepb.TranscribeResponse, error)
	capabilitiesFn  func(ctx context.Context, req *inferencepb.Empty, opts ...grpc.CallOption) (*inferencepb.InferenceCapabilities, error)
	healthCheckFn   func(ctx context.Context, req *inferencepb.Empty, opts ...grpc.CallOption) (*commonpb.PluginHealthStatus, error)
}

func (m *mockInferencePlugin) Transcribe(ctx context.Context, req *inferencepb.TranscribeRequest, opts ...grpc.CallOption) (*inferencepb.TranscribeResponse, error) {
	if m.transcribeFn != nil {
		return m.transcribeFn(ctx, req, opts...)
	}
	return nil, errors.New("transcribeFn not set")
}

func (m *mockInferencePlugin) GetCapabilities(ctx context.Context, req *inferencepb.Empty, opts ...grpc.CallOption) (*inferencepb.InferenceCapabilities, error) {
	if m.capabilitiesFn != nil {
		return m.capabilitiesFn(ctx, req, opts...)
	}
	return nil, errors.New("capabilitiesFn not set")
}

func (m *mockInferencePlugin) HealthCheck(ctx context.Context, req *inferencepb.Empty, opts ...grpc.CallOption) (*commonpb.PluginHealthStatus, error) {
	if m.healthCheckFn != nil {
		return m.healthCheckFn(ctx, req, opts...)
	}
	return nil, errors.New("healthCheckFn not set")
}

// ── helper ────────────────────────────────────────────────────────────────────

// newInferenceClientWithMock returns an InferenceClient whose gRPC stub is
// replaced by mock, backed by an Endpoint with the given failure threshold and
// half-open timeout. This allows testing without a real UDS socket.
func newInferenceClientWithMock(mock inferencepb.InferencePluginClient, threshold int64, halfOpen time.Duration) *InferenceClient {
	ep := &Endpoint{
		id:               "infer-mock",
		failureThreshold: threshold,
		halfOpenTimeout:  halfOpen,
	}
	return &InferenceClient{endpoint: ep, stub: mock}
}

// ── Transcribe: success path ──────────────────────────────────────────────────

func TestInferenceClient_Transcribe_Success(t *testing.T) {
	wantText := "hello world"
	mock := &mockInferencePlugin{
		transcribeFn: func(_ context.Context, _ *inferencepb.TranscribeRequest, _ ...grpc.CallOption) (*inferencepb.TranscribeResponse, error) {
			return &inferencepb.TranscribeResponse{Text: wantText}, nil
		},
	}
	c := newInferenceClientWithMock(mock, 5, 30*time.Second)

	req := &inferencepb.TranscribeRequest{RequestId: "req-1", SessionId: "sess-1"}
	resp, err := c.Transcribe(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetText() != wantText {
		t.Fatalf("text = %q, want %q", resp.GetText(), wantText)
	}
}

func TestInferenceClient_Transcribe_Success_RecordsSuccessOnEndpoint(t *testing.T) {
	mock := &mockInferencePlugin{
		transcribeFn: func(_ context.Context, _ *inferencepb.TranscribeRequest, _ ...grpc.CallOption) (*inferencepb.TranscribeResponse, error) {
			return &inferencepb.TranscribeResponse{}, nil
		},
	}
	c := newInferenceClientWithMock(mock, 5, 30*time.Second)

	if _, err := c.Transcribe(context.Background(), &inferencepb.TranscribeRequest{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.endpoint.IsHealthy() {
		t.Fatal("endpoint should be healthy after successful Transcribe")
	}
	if c.endpoint.failureCount.Load() != 0 {
		t.Fatal("failure counter should be 0 after RecordSuccess")
	}
}

// ── Transcribe: circuit breaker open ─────────────────────────────────────────

func TestInferenceClient_Transcribe_CircuitOpen_ReturnsError(t *testing.T) {
	called := false
	mock := &mockInferencePlugin{
		transcribeFn: func(_ context.Context, _ *inferencepb.TranscribeRequest, _ ...grpc.CallOption) (*inferencepb.TranscribeResponse, error) {
			called = true
			return &inferencepb.TranscribeResponse{}, nil
		},
	}
	// threshold=1, halfOpen=forever → one failure opens the circuit permanently.
	c := newInferenceClientWithMock(mock, 1, 1*time.Hour)
	c.endpoint.RecordFailure() // → OPEN

	_, err := c.Transcribe(context.Background(), &inferencepb.TranscribeRequest{})
	if err == nil {
		t.Fatal("expected error when circuit is open, got nil")
	}
	if called {
		t.Fatal("Transcribe RPC should not be forwarded when circuit is open")
	}
}

func TestInferenceClient_Transcribe_CircuitOpen_ErrorMentionsEndpointID(t *testing.T) {
	mock := &mockInferencePlugin{}
	c := newInferenceClientWithMock(mock, 1, 1*time.Hour)
	c.endpoint.RecordFailure()

	_, err := c.Transcribe(context.Background(), &inferencepb.TranscribeRequest{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "infer-mock") {
		t.Fatalf("error %q does not mention endpoint ID %q", err.Error(), "infer-mock")
	}
}

// ── Transcribe: RPC failure ───────────────────────────────────────────────────

func TestInferenceClient_Transcribe_RPCError_RecordsFailure(t *testing.T) {
	sentinel := errors.New("plugin OOM")
	mock := &mockInferencePlugin{
		transcribeFn: func(_ context.Context, _ *inferencepb.TranscribeRequest, _ ...grpc.CallOption) (*inferencepb.TranscribeResponse, error) {
			return nil, sentinel
		},
	}
	c := newInferenceClientWithMock(mock, 10, 30*time.Second)

	_, err := c.Transcribe(context.Background(), &inferencepb.TranscribeRequest{})
	if err == nil {
		t.Fatal("expected error when RPC fails, got nil")
	}
	if c.endpoint.failureCount.Load() == 0 {
		t.Fatal("failure counter should be incremented after RPC error")
	}
}

func TestInferenceClient_Transcribe_RPCError_WrapsOriginalError(t *testing.T) {
	sentinel := errors.New("connection reset")
	mock := &mockInferencePlugin{
		transcribeFn: func(_ context.Context, _ *inferencepb.TranscribeRequest, _ ...grpc.CallOption) (*inferencepb.TranscribeResponse, error) {
			return nil, sentinel
		},
	}
	c := newInferenceClientWithMock(mock, 10, 30*time.Second)

	_, err := c.Transcribe(context.Background(), &inferencepb.TranscribeRequest{})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped sentinel error, got %v", err)
	}
}

// ── HealthCheck ───────────────────────────────────────────────────────────────

func TestInferenceClient_HealthCheck_Success(t *testing.T) {
	want := commonpb.PluginState_PLUGIN_STATE_READY
	mock := &mockInferencePlugin{
		healthCheckFn: func(_ context.Context, _ *inferencepb.Empty, _ ...grpc.CallOption) (*commonpb.PluginHealthStatus, error) {
			return &commonpb.PluginHealthStatus{State: want}, nil
		},
	}
	c := newInferenceClientWithMock(mock, 5, 30*time.Second)

	status, err := c.HealthCheck(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status.GetState() != want {
		t.Fatalf("state = %v, want %v", status.GetState(), want)
	}
}

func TestInferenceClient_HealthCheck_Error_RecordsFailure(t *testing.T) {
	sentinel := errors.New("health check failed")
	mock := &mockInferencePlugin{
		healthCheckFn: func(_ context.Context, _ *inferencepb.Empty, _ ...grpc.CallOption) (*commonpb.PluginHealthStatus, error) {
			return nil, sentinel
		},
	}
	c := newInferenceClientWithMock(mock, 10, 30*time.Second)

	_, err := c.HealthCheck(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if c.endpoint.failureCount.Load() == 0 {
		t.Fatal("failure counter should be incremented after HealthCheck error")
	}
}

func TestInferenceClient_HealthCheck_Error_WrapsOriginalError(t *testing.T) {
	sentinel := errors.New("dial timeout")
	mock := &mockInferencePlugin{
		healthCheckFn: func(_ context.Context, _ *inferencepb.Empty, _ ...grpc.CallOption) (*commonpb.PluginHealthStatus, error) {
			return nil, sentinel
		},
	}
	c := newInferenceClientWithMock(mock, 10, 30*time.Second)

	_, err := c.HealthCheck(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped sentinel error, got %v", err)
	}
}

// ── HealthCheckProbe ──────────────────────────────────────────────────────────

func TestInferenceClient_HealthCheckProbe_Success(t *testing.T) {
	want := commonpb.PluginState_PLUGIN_STATE_READY
	mock := &mockInferencePlugin{
		healthCheckFn: func(_ context.Context, _ *inferencepb.Empty, _ ...grpc.CallOption) (*commonpb.PluginHealthStatus, error) {
			return &commonpb.PluginHealthStatus{State: want}, nil
		},
	}
	c := newInferenceClientWithMock(mock, 5, 30*time.Second)

	id, state, cbHealthy, err := c.HealthCheckProbe(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "infer-mock" {
		t.Fatalf("id = %q, want %q", id, "infer-mock")
	}
	if state != want {
		t.Fatalf("state = %v, want %v", state, want)
	}
	if !cbHealthy {
		t.Fatal("cbHealthy should be true for a healthy CLOSED endpoint")
	}
}

func TestInferenceClient_HealthCheckProbe_Error_ReturnsUnknownState(t *testing.T) {
	sentinel := errors.New("plugin gone")
	mock := &mockInferencePlugin{
		healthCheckFn: func(_ context.Context, _ *inferencepb.Empty, _ ...grpc.CallOption) (*commonpb.PluginHealthStatus, error) {
			return nil, sentinel
		},
	}
	c := newInferenceClientWithMock(mock, 10, 30*time.Second)

	id, state, _, err := c.HealthCheckProbe(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if id != "infer-mock" {
		t.Fatalf("id = %q, want %q", id, "infer-mock")
	}
	if state != commonpb.PluginState_PLUGIN_STATE_UNKNOWN {
		t.Fatalf("state = %v on error, want PLUGIN_STATE_UNKNOWN", state)
	}
}

func TestInferenceClient_HealthCheckProbe_ReflectsCircuitBreakerHealth(t *testing.T) {
	// Open the circuit before probing; cbHealthy must reflect the open state.
	mock := &mockInferencePlugin{
		healthCheckFn: func(_ context.Context, _ *inferencepb.Empty, _ ...grpc.CallOption) (*commonpb.PluginHealthStatus, error) {
			return nil, errors.New("error")
		},
	}
	// threshold=1, halfOpen=forever → one failure opens circuit permanently.
	c := newInferenceClientWithMock(mock, 1, 1*time.Hour)

	// The HealthCheck inside HealthCheckProbe will fail and call RecordFailure.
	_, _, cbHealthy, err := c.HealthCheckProbe(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// After RecordFailure with threshold=1, circuit is open → not healthy.
	if cbHealthy {
		t.Fatal("cbHealthy should be false when circuit is OPEN")
	}
}

// ── FetchCapabilities ─────────────────────────────────────────────────────────

func TestInferenceClient_FetchCapabilities_Success(t *testing.T) {
	mock := &mockInferencePlugin{
		capabilitiesFn: func(_ context.Context, _ *inferencepb.Empty, _ ...grpc.CallOption) (*inferencepb.InferenceCapabilities, error) {
			return &inferencepb.InferenceCapabilities{
				EngineName: "mlx_whisper",
				ModelSize:  "large-v3-turbo",
				Device:     "mps",
			}, nil
		},
	}
	c := newInferenceClientWithMock(mock, 5, 30*time.Second)

	if err := c.FetchCapabilities(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.EngineName() != "mlx_whisper" {
		t.Errorf("EngineName = %q, want %q", c.EngineName(), "mlx_whisper")
	}
	if c.ModelSize() != "large-v3-turbo" {
		t.Errorf("ModelSize = %q, want %q", c.ModelSize(), "large-v3-turbo")
	}
	if c.Device() != "mps" {
		t.Errorf("Device = %q, want %q", c.Device(), "mps")
	}
}

func TestInferenceClient_FetchCapabilities_Error_ReturnsError(t *testing.T) {
	sentinel := errors.New("plugin not ready")
	mock := &mockInferencePlugin{
		capabilitiesFn: func(_ context.Context, _ *inferencepb.Empty, _ ...grpc.CallOption) (*inferencepb.InferenceCapabilities, error) {
			return nil, sentinel
		},
	}
	c := newInferenceClientWithMock(mock, 5, 30*time.Second)

	err := c.FetchCapabilities(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped sentinel error, got %v", err)
	}
	// Fields must remain empty on error.
	if c.EngineName() != "" || c.ModelSize() != "" || c.Device() != "" {
		t.Error("engine fields must be empty when FetchCapabilities fails")
	}
}

// ── Endpoint accessor ─────────────────────────────────────────────────────────

func TestInferenceClient_Endpoint_ReturnsSameEndpoint(t *testing.T) {
	mock := &mockInferencePlugin{}
	c := newInferenceClientWithMock(mock, 5, 30*time.Second)

	if c.Endpoint() != c.endpoint {
		t.Fatal("Endpoint() should return the underlying Endpoint")
	}
}

// ── table-driven: Transcribe state transitions ────────────────────────────────

func TestInferenceClient_Transcribe_TableDriven(t *testing.T) {
	tests := []struct {
		name        string
		rpcErr      error
		circuitOpen bool
		wantErr     bool
	}{
		{"success", nil, false, false},
		{"rpc error", errors.New("oops"), false, true},
		{"circuit open", nil, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockInferencePlugin{
				transcribeFn: func(_ context.Context, _ *inferencepb.TranscribeRequest, _ ...grpc.CallOption) (*inferencepb.TranscribeResponse, error) {
					if tc.rpcErr != nil {
						return nil, tc.rpcErr
					}
					return &inferencepb.TranscribeResponse{Text: "ok"}, nil
				},
			}
			c := newInferenceClientWithMock(mock, 1, 1*time.Hour)
			if tc.circuitOpen {
				c.endpoint.RecordFailure() // open the circuit (threshold=1)
			}

			_, err := c.Transcribe(context.Background(), &inferencepb.TranscribeRequest{})
			gotErr := err != nil
			if gotErr != tc.wantErr {
				t.Fatalf("wantErr=%v, got err=%v (%v)", tc.wantErr, gotErr, err)
			}
		})
	}
}

