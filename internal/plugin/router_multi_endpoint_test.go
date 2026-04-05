package plugin

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
	"google.golang.org/grpc"
)

// ── stub InferencePlugin servers ──────────────────────────────────────────────

// countingInferenceServer is a minimal InferencePluginServer that counts how
// many Transcribe calls it receives.
type countingInferenceServer struct {
	inferencepb.UnimplementedInferencePluginServer
	calls atomic.Int64
	text  string // response text
}

func (s *countingInferenceServer) HealthCheck(
	_ context.Context, _ *inferencepb.Empty,
) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *countingInferenceServer) Transcribe(
	_ context.Context, req *inferencepb.TranscribeRequest,
) (*inferencepb.TranscribeResponse, error) {
	s.calls.Add(1)
	return &inferencepb.TranscribeResponse{
		RequestId: req.RequestId,
		SessionId: req.SessionId,
		Text:      s.text,
	}, nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

func tempSockPath(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "smux-plugin")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, name)
}

// startCountingServer starts an InferencePlugin gRPC server on a Unix socket
// and returns the server + endpoint. The server is registered for cleanup.
func startCountingServer(t *testing.T, id, text string) (*countingInferenceServer, *grpc.Server, *Endpoint) {
	t.Helper()
	sock := tempSockPath(t, id+".sock")

	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen %s: %v", sock, err)
	}
	gs := grpc.NewServer()
	srv := &countingInferenceServer{text: text}
	inferencepb.RegisterInferencePluginServer(gs, srv)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(func() { gs.Stop() })

	ep, err := NewEndpoint(id, sock, EndpointCircuitBreaker{})
	if err != nil {
		t.Fatalf("NewEndpoint %s: %v", id, err)
	}
	t.Cleanup(func() { _ = ep.Close() })
	return srv, gs, ep
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestPluginRouter_MultiEndpoint_RoundRobinDistribution verifies that requests
// are distributed approximately evenly across two healthy inference endpoints.
func TestPluginRouter_MultiEndpoint_RoundRobinDistribution(t *testing.T) {
	srv1, _, ep1 := startCountingServer(t, "stt-1", "engine-one")
	srv2, _, ep2 := startCountingServer(t, "stt-2", "engine-two")

	c1 := NewInferenceClient(ep1)
	c2 := NewInferenceClient(ep2)
	router := NewPluginRouter(RoutingRoundRobin)
	router.addEntry(c1, 0)
	router.addEntry(c2, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const numRequests = 10
	for i := range numRequests {
		client, err := router.Route()
		if err != nil {
			t.Fatalf("Route[%d]: %v", i, err)
		}
		req := &inferencepb.TranscribeRequest{
			RequestId: "req",
			SessionId: "sess",
		}
		if _, err := client.Transcribe(ctx, req); err != nil {
			t.Fatalf("Transcribe[%d]: %v", i, err)
		}
	}

	calls1 := srv1.calls.Load()
	calls2 := srv2.calls.Load()

	if calls1+calls2 != numRequests {
		t.Errorf("total calls = %d, want %d", calls1+calls2, numRequests)
	}
	// With strict round-robin and an even count, each server must get exactly half.
	if calls1 != numRequests/2 || calls2 != numRequests/2 {
		t.Errorf("expected %d calls each, got stt-1=%d stt-2=%d", numRequests/2, calls1, calls2)
	}
}

// TestPluginRouter_MultiEndpoint_FailoverWhenOneDown verifies that when one
// endpoint is stopped mid-run, subsequent requests are routed exclusively to
// the healthy endpoint.
func TestPluginRouter_MultiEndpoint_FailoverWhenOneDown(t *testing.T) {
	_, gs1, ep1 := startCountingServer(t, "stt-a", "engine-a")
	srv2, _, ep2 := startCountingServer(t, "stt-b", "engine-b")

	// Lower failure threshold so the circuit opens quickly.
	ep1.failureThreshold = 2

	c1 := NewInferenceClient(ep1)
	c2 := NewInferenceClient(ep2)
	router := NewPluginRouter(RoutingRoundRobin)
	router.addEntry(c1, 0)
	router.addEntry(c2, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Phase 1: both endpoints healthy — send a few requests.
	for i := range 4 {
		client, err := router.Route()
		if err != nil {
			t.Fatalf("Phase1 Route[%d]: %v", i, err)
		}
		if _, err := client.Transcribe(ctx, &inferencepb.TranscribeRequest{RequestId: "r", SessionId: "s"}); err != nil {
			t.Fatalf("Phase1 Transcribe[%d]: %v", i, err)
		}
	}

	// Phase 2: stop stt-a and exhaust its circuit breaker.
	gs1.Stop() // kills the gRPC server
	// Drain the circuit by triggering enough failures.
	for range int(ep1.failureThreshold) + 1 {
		c, _ := router.Route()
		if c == c1 {
			_, _ = c1.Transcribe(ctx, &inferencepb.TranscribeRequest{})
		}
	}

	// Phase 3: now stt-a's circuit should be open; all requests must go to stt-b.
	calls2Before := srv2.calls.Load()
	const phase3Requests = 6
	for i := range phase3Requests {
		client, err := router.Route()
		if err != nil {
			t.Fatalf("Phase3 Route[%d]: %v", i, err)
		}
		if client != c2 {
			t.Errorf("Phase3[%d]: expected all requests to route to stt-b", i)
		}
		if _, err := client.Transcribe(ctx, &inferencepb.TranscribeRequest{RequestId: "r", SessionId: "s"}); err != nil {
			t.Fatalf("Phase3 Transcribe[%d]: %v", i, err)
		}
	}

	calls2After := srv2.calls.Load()
	if calls2After-calls2Before != phase3Requests {
		t.Errorf("stt-b: want %d new calls in phase3, got %d", phase3Requests, calls2After-calls2Before)
	}
}

// TestPluginRouter_MultiEndpoint_CircuitBreakerRecovery verifies that an
// endpoint that was marked OPEN transitions to HALF_OPEN after halfOpenTimeout
// and is used again once it recovers.
func TestPluginRouter_MultiEndpoint_CircuitBreakerRecovery(t *testing.T) {
	// ep1 has a very short half-open timeout so recovery is fast in tests.
	ep1 := &Endpoint{
		id:               "stt-recover",
		failureThreshold: 1,
		halfOpenTimeout:  20 * time.Millisecond,
	}
	ep2 := healthyEndpoint() // unit-test helper — no real socket needed

	// Build clients purely from circuit-breaker state, not real RPCs.
	// We only test that PluginRouter selects ep1 again after recovery.
	c1 := &InferenceClient{endpoint: ep1}
	c2 := &InferenceClient{endpoint: ep2}
	router := NewPluginRouter(RoutingRoundRobin)
	router.addEntry(c1, 0)
	router.addEntry(c2, 0)

	// Open ep1's circuit.
	ep1.RecordFailure() // threshold=1 → OPEN
	if ep1.IsHealthy() {
		t.Fatal("ep1 should be OPEN after RecordFailure")
	}

	// Immediately after opening, Route should never pick ep1.
	for range 5 {
		got, err := router.Route()
		if err != nil {
			t.Fatalf("Route: %v", err)
		}
		if got == c1 {
			t.Error("c1 should not be selected while its circuit is OPEN")
		}
	}

	// Wait for halfOpenTimeout to elapse — ep1 transitions to HALF_OPEN.
	time.Sleep(40 * time.Millisecond)

	// In HALF_OPEN state IsHealthy() returns true (one probe allowed).
	if !ep1.IsHealthy() {
		t.Fatal("ep1 should be HALF_OPEN (healthy) after timeout")
	}

	// Simulate a successful probe — ep1 transitions back to CLOSED.
	ep1.RecordSuccess()
	if !ep1.IsHealthy() {
		t.Fatal("ep1 should be CLOSED after RecordSuccess")
	}
	if ep1.CircuitState() != "closed" {
		t.Fatalf("ep1 CircuitState = %q, want %q", ep1.CircuitState(), "closed")
	}

	// Route should now distribute across both clients again.
	got1, _ := router.Route()
	got2, _ := router.Route()
	if got1 == got2 {
		t.Error("after recovery, Route should distribute requests to both endpoints")
	}
}
