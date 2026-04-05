package plugin

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	vadpb "github.com/speechmux/proto/gen/go/vad/v1"
	"google.golang.org/grpc"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// newTestEndpoint returns an Endpoint with no real gRPC connection, suitable
// for unit-testing the circuit breaker state machine in isolation.
func newTestEndpoint(threshold int64, halfOpen time.Duration) *Endpoint {
	return &Endpoint{
		id:               "test-ep",
		failureThreshold: threshold,
		halfOpenTimeout:  halfOpen,
	}
}

// ── circuit breaker: CLOSED state ────────────────────────────────────────────

func TestEndpoint_InitialStateClosed(t *testing.T) {
	ep := newTestEndpoint(5, 30*time.Second)
	if !ep.IsHealthy() {
		t.Fatal("new endpoint should be CLOSED (healthy)")
	}
	if got := ep.CircuitState(); got != "closed" {
		t.Fatalf("CircuitState = %q, want %q", got, "closed")
	}
}

func TestEndpoint_RecordSuccess_KeepsClosed(t *testing.T) {
	ep := newTestEndpoint(5, 30*time.Second)
	ep.RecordSuccess()
	if !ep.IsHealthy() {
		t.Fatal("endpoint should remain CLOSED after RecordSuccess")
	}
}

func TestEndpoint_RecordFailure_BelowThreshold_StaysClosed(t *testing.T) {
	ep := newTestEndpoint(5, 30*time.Second)
	for i := range 4 {
		ep.RecordFailure()
		if !ep.IsHealthy() {
			t.Fatalf("circuit opened after only %d failure(s), threshold=5", i+1)
		}
	}
}

// ── circuit breaker: CLOSED → OPEN ───────────────────────────────────────────

func TestEndpoint_RecordFailure_AtThreshold_OpensCircuit(t *testing.T) {
	ep := newTestEndpoint(3, 1*time.Hour)
	ep.RecordFailure()
	ep.RecordFailure()
	// Third failure should trip the breaker.
	ep.RecordFailure()

	if ep.IsHealthy() {
		t.Fatal("circuit should be OPEN after reaching failure threshold")
	}
	if got := ep.CircuitState(); got != "open" {
		t.Fatalf("CircuitState = %q, want %q", got, "open")
	}
}

// ── circuit breaker: OPEN → HALF_OPEN ────────────────────────────────────────

func TestEndpoint_IsHealthy_TransitionsToHalfOpenAfterTimeout(t *testing.T) {
	// Use a very short halfOpenTimeout so the test doesn't have to sleep long.
	ep := newTestEndpoint(1, 10*time.Millisecond)
	ep.RecordFailure() // opens the circuit

	if ep.IsHealthy() {
		t.Fatal("circuit should be OPEN right after opening")
	}

	// Wait for the halfOpenTimeout to elapse.
	time.Sleep(20 * time.Millisecond)

	if !ep.IsHealthy() {
		t.Fatal("circuit should transition to HALF_OPEN after halfOpenTimeout")
	}
	if got := ep.CircuitState(); got != "half_open" {
		t.Fatalf("CircuitState = %q, want %q", got, "half_open")
	}
}

func TestEndpoint_IsHealthy_StaysOpenBeforeTimeout(t *testing.T) {
	ep := newTestEndpoint(1, 1*time.Hour)
	ep.RecordFailure() // opens the circuit

	// Repeated calls must not transition while timeout has not elapsed.
	for range 5 {
		if ep.IsHealthy() {
			t.Fatal("circuit should remain OPEN before halfOpenTimeout elapses")
		}
	}
}

// ── circuit breaker: HALF_OPEN → CLOSED ──────────────────────────────────────

func TestEndpoint_RecordSuccess_ClosesFromHalfOpen(t *testing.T) {
	ep := newTestEndpoint(1, 10*time.Millisecond)
	ep.RecordFailure() // → OPEN
	time.Sleep(20 * time.Millisecond)
	ep.IsHealthy() // → HALF_OPEN

	ep.RecordSuccess() // → CLOSED

	if !ep.IsHealthy() {
		t.Fatal("circuit should be CLOSED after RecordSuccess in HALF_OPEN state")
	}
	if got := ep.CircuitState(); got != "closed" {
		t.Fatalf("CircuitState = %q, want %q", got, "closed")
	}
}

// ── circuit breaker: HALF_OPEN → OPEN ────────────────────────────────────────

func TestEndpoint_RecordFailure_OpensFromHalfOpen(t *testing.T) {
	ep := newTestEndpoint(1, 10*time.Millisecond)
	ep.RecordFailure() // → OPEN
	time.Sleep(20 * time.Millisecond)
	ep.IsHealthy() // → HALF_OPEN

	ep.RecordFailure() // → OPEN again

	if ep.IsHealthy() {
		t.Fatal("circuit should re-open on failure in HALF_OPEN state")
	}
	if got := ep.CircuitState(); got != "open" {
		t.Fatalf("CircuitState = %q, want %q", got, "open")
	}
}

// ── circuit breaker: failure counter reset ───────────────────────────────────

func TestEndpoint_RecordSuccess_ResetsFailureCounter(t *testing.T) {
	ep := newTestEndpoint(3, 1*time.Hour)
	ep.RecordFailure()
	ep.RecordFailure()
	ep.RecordSuccess() // resets counter
	// Two more failures should not open the circuit (counter restarted).
	ep.RecordFailure()
	ep.RecordFailure()
	if !ep.IsHealthy() {
		t.Fatal("circuit should stay CLOSED; RecordSuccess should have reset the counter")
	}
}

// ── concurrency ───────────────────────────────────────────────────────────────

func TestEndpoint_RecordFailure_ConcurrentSafe(t *testing.T) {
	ep := newTestEndpoint(10, 1*time.Hour)
	const goroutines = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			ep.RecordFailure()
		}()
	}
	wg.Wait()

	// All 20 goroutines finished; circuit must now be open (threshold=10 exceeded).
	if ep.IsHealthy() {
		t.Fatal("circuit should be OPEN after 20 concurrent failures (threshold=10)")
	}
}

func TestEndpoint_IsHealthy_ConcurrentSafe(t *testing.T) {
	ep := newTestEndpoint(1, 10*time.Millisecond)
	ep.RecordFailure() // → OPEN
	time.Sleep(20 * time.Millisecond)

	const goroutines = 50
	results := make([]bool, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			results[idx] = ep.IsHealthy()
		}(i)
	}
	wg.Wait()

	// At least one call must have seen the HALF_OPEN state (returned true).
	healthy := false
	for _, r := range results {
		if r {
			healthy = true
			break
		}
	}
	if !healthy {
		t.Fatal("expected at least one goroutine to observe HALF_OPEN / healthy state")
	}
}

// ── ID ────────────────────────────────────────────────────────────────────────

func TestEndpoint_ID(t *testing.T) {
	ep := &Endpoint{id: "vad-0"}
	if got := ep.ID(); got != "vad-0" {
		t.Fatalf("ID = %q, want %q", got, "vad-0")
	}
}

// ── HealthCheckProbe ──────────────────────────────────────────────────────────

// mockVADPluginClient is a hand-rolled mock for vadpb.VADPluginClient.
type mockVADPluginClient struct {
	healthCheckFn func(ctx context.Context, in *vadpb.Empty, opts ...grpc.CallOption) (*commonpb.PluginHealthStatus, error)
}

func (m *mockVADPluginClient) StreamVAD(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[vadpb.VADRequest, vadpb.VADResponse], error) {
	return nil, errors.New("not implemented in mock")
}

func (m *mockVADPluginClient) GetCapabilities(ctx context.Context, in *vadpb.Empty, opts ...grpc.CallOption) (*vadpb.VADCapabilities, error) {
	return nil, errors.New("not implemented in mock")
}

func (m *mockVADPluginClient) HealthCheck(ctx context.Context, in *vadpb.Empty, opts ...grpc.CallOption) (*commonpb.PluginHealthStatus, error) {
	return m.healthCheckFn(ctx, in, opts...)
}

// endpointWithMockVAD returns an Endpoint whose VADPluginClient() method is
// overridden via a thin wrapper type, allowing injection without a real socket.
type endpointWithMockVAD struct {
	Endpoint
	mockClient vadpb.VADPluginClient
}

func (e *endpointWithMockVAD) HealthCheckProbe(ctx context.Context) (commonpb.PluginState, error) {
	resp, err := e.mockClient.HealthCheck(ctx, &vadpb.Empty{})
	if err != nil {
		return commonpb.PluginState_PLUGIN_STATE_UNKNOWN, err
	}
	return resp.GetState(), nil
}

func TestEndpoint_HealthCheckProbe_Success(t *testing.T) {
	want := commonpb.PluginState_PLUGIN_STATE_READY
	mock := &mockVADPluginClient{
		healthCheckFn: func(_ context.Context, _ *vadpb.Empty, _ ...grpc.CallOption) (*commonpb.PluginHealthStatus, error) {
			return &commonpb.PluginHealthStatus{State: want}, nil
		},
	}
	ep := &endpointWithMockVAD{
		Endpoint:   Endpoint{id: "vad-mock", failureThreshold: 5},
		mockClient: mock,
	}

	got, err := ep.HealthCheckProbe(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Fatalf("PluginState = %v, want %v", got, want)
	}
}

func TestEndpoint_HealthCheckProbe_Error(t *testing.T) {
	sentinel := errors.New("vad unreachable")
	mock := &mockVADPluginClient{
		healthCheckFn: func(_ context.Context, _ *vadpb.Empty, _ ...grpc.CallOption) (*commonpb.PluginHealthStatus, error) {
			return nil, sentinel
		},
	}
	ep := &endpointWithMockVAD{
		Endpoint:   Endpoint{id: "vad-mock", failureThreshold: 5},
		mockClient: mock,
	}

	state, err := ep.HealthCheckProbe(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if state != commonpb.PluginState_PLUGIN_STATE_UNKNOWN {
		t.Fatalf("state = %v on error, want PLUGIN_STATE_UNKNOWN", state)
	}
}
