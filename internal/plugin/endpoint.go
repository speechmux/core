// Package plugin manages gRPC connections to sidecar plugin processes.
package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
	vadpb "github.com/speechmux/proto/gen/go/vad/v1"
)

// circuitState represents the state of the circuit breaker.
type circuitState int32

const (
	circuitClosed   circuitState = 0
	circuitOpen     circuitState = 1
	circuitHalfOpen circuitState = 2
)

// Endpoint manages a gRPC connection to a sidecar plugin process and tracks
// plugin health via a circuit breaker.
//
// Circuit breaker transitions:
//
//	CLOSED   → normal operation; failures are counted.
//	OPEN     → failure threshold exceeded; requests are rejected until
//	           halfOpenTimeout elapses.
//	HALF_OPEN → one probe request is allowed through to test recovery.
type Endpoint struct {
	id               string
	socket           string
	conn             *grpc.ClientConn
	state            atomic.Int32 // holds circuitState
	failureCount     atomic.Int64
	openedAtNano     atomic.Int64 // UnixNano when the circuit transitioned to OPEN
	failureThreshold int64
	halfOpenTimeout  time.Duration
}

// EndpointCircuitBreaker carries circuit-breaker tuning for a single endpoint.
// Zero values fall back to defaults (failure_threshold: 5, half_open_timeout: 30s).
type EndpointCircuitBreaker struct {
	FailureThreshold int
	HalfOpenTimeout  time.Duration
}

// NewEndpoint dials the given Unix domain socket and returns a ready Endpoint.
// Zero values in cb fall back to the defaults (5 failures, 30 s half-open).
func NewEndpoint(id, socket string, cb EndpointCircuitBreaker) (*Endpoint, error) {
	if cb.FailureThreshold == 0 {
		cb.FailureThreshold = 5
	}
	if cb.HalfOpenTimeout == 0 {
		cb.HalfOpenTimeout = 30 * time.Second
	}
	conn, err := grpc.NewClient(
		"unix://"+socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("endpoint %s: dial unix://%s: %w", id, socket, err)
	}
	return &Endpoint{
		id:               id,
		socket:           socket,
		conn:             conn,
		failureThreshold: int64(cb.FailureThreshold),
		halfOpenTimeout:  cb.HalfOpenTimeout,
	}, nil
}

// VADPluginClient returns a VAD plugin gRPC stub backed by this endpoint's connection.
func (e *Endpoint) VADPluginClient() vadpb.VADPluginClient {
	return vadpb.NewVADPluginClient(e.conn)
}

// InferencePluginClient returns an inference plugin gRPC stub backed by this endpoint's connection.
func (e *Endpoint) InferencePluginClient() inferencepb.InferencePluginClient {
	return inferencepb.NewInferencePluginClient(e.conn)
}

// IsHealthy returns true when the circuit is CLOSED or HALF_OPEN.
// When OPEN, it checks whether halfOpenTimeout has elapsed and, if so,
// transitions to HALF_OPEN before returning true.
func (e *Endpoint) IsHealthy() bool {
	switch circuitState(e.state.Load()) {
	case circuitClosed:
		return true
	case circuitOpen:
		opened := time.Unix(0, e.openedAtNano.Load())
		if time.Since(opened) < e.halfOpenTimeout {
			return false
		}
		// Only the goroutine that wins the CAS transitions to HALF_OPEN and
		// is allowed to probe. Concurrent goroutines whose CAS loses see the
		// circuit as still OPEN and return false — on their next call they
		// reach the HALF_OPEN default branch once the probe result is known.
		return e.state.CompareAndSwap(int32(circuitOpen), int32(circuitHalfOpen))
	default: // circuitHalfOpen
		return true
	}
}

// RecordSuccess resets the failure counter and closes the circuit.
func (e *Endpoint) RecordSuccess() {
	e.failureCount.Store(0)
	e.state.Store(int32(circuitClosed))
}

// RecordFailure increments the failure counter and opens the circuit when the
// failure threshold is exceeded, or immediately when the state is HALF_OPEN.
func (e *Endpoint) RecordFailure() {
	n := e.failureCount.Add(1)
	cur := circuitState(e.state.Load())
	if cur == circuitHalfOpen || (cur == circuitClosed && n >= e.failureThreshold) {
		e.openedAtNano.Store(time.Now().UnixNano())
		e.state.Store(int32(circuitOpen))
		slog.Warn("circuit breaker opened", "endpoint_id", e.id, "failures", n)
	}
}

// ID returns the endpoint identifier.
func (e *Endpoint) ID() string { return e.id }

// Socket returns the Unix domain socket path for this endpoint.
func (e *Endpoint) Socket() string { return e.socket }

// CircuitState returns a human-readable circuit breaker state string.
func (e *Endpoint) CircuitState() string {
	switch circuitState(e.state.Load()) {
	case circuitOpen:
		return "open"
	case circuitHalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// HealthCheckProbe calls the VAD plugin's HealthCheck RPC and returns the
// plugin state. Implements runtime.vadHealthProber.
func (e *Endpoint) HealthCheckProbe(ctx context.Context) (commonpb.PluginState, error) {
	resp, err := e.VADPluginClient().HealthCheck(ctx, &emptypb.Empty{})
	if err != nil {
		return commonpb.PluginState_PLUGIN_STATE_UNSPECIFIED, fmt.Errorf("VAD health check: %w", err)
	}
	return resp.GetState(), nil
}

// Close shuts down the underlying gRPC connection.
// Safe to call on an Endpoint without a connection (e.g., in tests).
func (e *Endpoint) Close() error {
	if e.conn == nil {
		return nil
	}
	return e.conn.Close()
}
