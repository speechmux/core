package plugin

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	commonpb "github.com/speechmux/proto/gen/go/common/v1"
)

// Routing mode constants matching the plugins.yaml routing_mode field.
const (
	RoutingRoundRobin    = "round_robin"
	RoutingLeastConn     = "least_connections"
	RoutingActiveStandby = "active_standby"
)

// routerEntry pairs an InferenceClient with its configured priority.
// Priority is used by active_standby routing: higher value = more preferred.
type routerEntry struct {
	client   *InferenceClient
	priority int
}

// PluginRouter routes inference requests to a pool of InferenceClients.
//
// Three routing modes are supported (set at construction via NewPluginRouter):
//
//   - round_robin (default): cycles through healthy endpoints in order.
//   - least_connections: picks the healthy endpoint with fewest in-flight RPCs.
//   - active_standby: picks the highest-priority healthy endpoint; falls back to
//     lower-priority endpoints when the primary is unavailable.
//
// Circuit-breaker recovery is driven by a background health probe loop:
//   - OPEN endpoints transition to HALF_OPEN after halfOpenTimeout.
//   - A probe HealthCheck on a HALF_OPEN endpoint closes or re-opens the circuit.
type PluginRouter struct {
	mu           sync.RWMutex
	entries      []routerEntry
	mode         string
	counter      atomic.Int64           // used by round_robin
	probeTimeout time.Duration          // per-probe HealthCheck timeout; 0 defaults to 5 s in probeAll
	cb           EndpointCircuitBreaker // applied to every endpoint added via Add(); zero = use defaults
}

// EndpointSummary is a point-in-time snapshot of a registered endpoint's state.
type EndpointSummary struct {
	ID             string `json:"id"`
	Socket         string `json:"socket"`
	CircuitBreaker string `json:"circuit_breaker"`
	EngineName     string `json:"engine_name"`  // from GetCapabilities; empty if unavailable
	ModelSize      string `json:"model_size"`   // from GetCapabilities; empty if unavailable
	Device         string `json:"device"`       // from GetCapabilities; empty if unavailable
}

// NewPluginRouter creates a PluginRouter using the given routing mode.
// mode must be one of: "round_robin", "least_connections", "active_standby".
// An empty or unrecognised mode defaults to round_robin.
func NewPluginRouter(mode string) *PluginRouter {
	switch mode {
	case RoutingLeastConn, RoutingActiveStandby:
		// valid; use as-is
	default:
		if mode != "" && mode != RoutingRoundRobin {
			slog.Warn("unknown inference routing_mode; defaulting to round_robin", "mode", mode)
		}
		mode = RoutingRoundRobin
	}
	return &PluginRouter{
		entries: make([]routerEntry, 0),
		mode:    mode,
	}
}

// Add dials the given socket, wraps it in an InferenceClient, and appends it
// to the pool with the given priority (used only by active_standby routing).
// Returns an error if id is already registered or dial fails.
func (r *PluginRouter) Add(id, socket string, priority int) error {
	// Pre-check under read lock for a clear early error.
	r.mu.RLock()
	for _, e := range r.entries {
		if e.client.endpoint.ID() == id {
			r.mu.RUnlock()
			return fmt.Errorf("inference endpoint %q already registered", id)
		}
	}
	r.mu.RUnlock()

	ep, err := NewEndpoint(id, socket, r.cb)
	if err != nil {
		return fmt.Errorf("add inference endpoint %q: %w", id, err)
	}
	client := NewInferenceClient(ep)

	// FetchCapabilities before acquiring the write lock: the RPC may block
	// and holding the router lock during a network call would stall all routing.
	capCtx, capCancel := context.WithTimeout(context.Background(), 5*time.Second)
	capErr := client.FetchCapabilities(capCtx)
	capCancel()
	if capErr != nil {
		slog.Warn("GetCapabilities failed; engine info unavailable", "id", id, "error", capErr)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// Re-check under write lock to close the TOCTOU window.
	for _, e := range r.entries {
		if e.client.endpoint.ID() == id {
			_ = ep.Close()
			return fmt.Errorf("inference endpoint %q already registered", id)
		}
	}
	r.entries = append(r.entries, routerEntry{client: client, priority: priority})
	slog.Info("inference endpoint registered",
		"id", id, "socket", socket, "priority", priority,
		"engine", client.EngineName(), "model", client.ModelSize(), "device", client.Device(),
	)
	return nil
}

// Remove finds the endpoint with the given id, removes it from the pool, and
// closes its gRPC connection. Returns an error if the id is not found.
func (r *PluginRouter) Remove(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i, e := range r.entries {
		if e.client.endpoint.ID() == id {
			r.entries = append(r.entries[:i], r.entries[i+1:]...)
			go func() {
				if err := e.client.endpoint.Close(); err != nil {
					slog.Warn("inference endpoint close error", "id", id, "err", err)
				}
			}()
			slog.Info("inference endpoint removed", "id", id)
			return nil
		}
	}
	return fmt.Errorf("inference endpoint %q not found", id)
}

// InferenceProber can report the health of an inference endpoint.
// Satisfied by *InferenceClient; defined here so callers outside the plugin
// package can probe without importing InferenceClient directly.
type InferenceProber interface {
	HealthCheckProbe(ctx context.Context) (string, commonpb.PluginState, bool, error)
}

// InferenceProbers returns a snapshot of all current inference clients as
// InferenceProber values.
func (r *PluginRouter) InferenceProbers() []InferenceProber {
	r.mu.RLock()
	defer r.mu.RUnlock()
	probers := make([]InferenceProber, len(r.entries))
	for i, e := range r.entries {
		probers[i] = e.client
	}
	return probers
}

// List returns a snapshot of all registered endpoints and their circuit-breaker states.
func (r *PluginRouter) List() []EndpointSummary {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]EndpointSummary, len(r.entries))
	for i, e := range r.entries {
		result[i] = EndpointSummary{
			ID:             e.client.endpoint.ID(),
			Socket:         e.client.endpoint.Socket(),
			CircuitBreaker: e.client.endpoint.CircuitState(),
			EngineName:     e.client.EngineName(),
			ModelSize:      e.client.ModelSize(),
			Device:         e.client.Device(),
		}
	}
	return result
}

// Route returns a healthy InferenceClient according to the configured routing mode.
// Returns an error if no healthy client is available.
func (r *PluginRouter) Route() (*InferenceClient, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.entries) == 0 {
		return nil, fmt.Errorf("no inference endpoints configured")
	}

	switch r.mode {
	case RoutingLeastConn:
		return r.routeLeastConn()
	case RoutingActiveStandby:
		return r.routeActiveStandby()
	default:
		return r.routeRoundRobin()
	}
}

// routeRoundRobin cycles through healthy endpoints in registration order.
func (r *PluginRouter) routeRoundRobin() (*InferenceClient, error) {
	n := len(r.entries)
	start := int(r.counter.Add(1) - 1)
	for i := range n {
		e := r.entries[(start+i)%n]
		if e.client.endpoint.IsHealthy() {
			return e.client, nil
		}
	}
	return nil, fmt.Errorf("no healthy inference endpoint available")
}

// routeLeastConn picks the healthy endpoint with the fewest in-flight Transcribe RPCs.
// Ties are broken by the round-robin counter.
func (r *PluginRouter) routeLeastConn() (*InferenceClient, error) {
	var best *InferenceClient
	var bestInflight int64 = -1

	n := len(r.entries)
	start := int(r.counter.Add(1) - 1)
	for i := range n {
		e := r.entries[(start+i)%n]
		if !e.client.endpoint.IsHealthy() {
			continue
		}
		inflight := e.client.Inflight()
		if best == nil || inflight < bestInflight {
			best = e.client
			bestInflight = inflight
		}
	}
	if best == nil {
		return nil, fmt.Errorf("no healthy inference endpoint available")
	}
	return best, nil
}

// routeActiveStandby picks the healthy endpoint with the highest priority.
// When multiple endpoints share the same priority, round-robin is used among them.
func (r *PluginRouter) routeActiveStandby() (*InferenceClient, error) {
	var best *InferenceClient
	bestPriority := -1 << 31 // min int

	n := len(r.entries)
	start := int(r.counter.Add(1) - 1)
	for i := range n {
		e := r.entries[(start+i)%n]
		if !e.client.endpoint.IsHealthy() {
			continue
		}
		if e.priority > bestPriority {
			best = e.client
			bestPriority = e.priority
		}
	}
	if best == nil {
		return nil, fmt.Errorf("no healthy inference endpoint available")
	}
	return best, nil
}

// SetCircuitBreaker sets the circuit-breaker parameters applied to every
// endpoint added via Add. Must be called before Add.
func (r *PluginRouter) SetCircuitBreaker(cb EndpointCircuitBreaker) {
	r.cb = cb
}

// StartHealthProbe launches a background goroutine that periodically calls
// HealthCheck on all endpoints to drive circuit-breaker state transitions.
// probeTimeout is the per-call deadline; zero defaults to 5 s.
func (r *PluginRouter) StartHealthProbe(ctx context.Context, interval, probeTimeout time.Duration) {
	if probeTimeout > 0 {
		r.probeTimeout = probeTimeout
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.probeAll(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// UnbindSession is a no-op stub reserved for future session-affinity support.
func (r *PluginRouter) UnbindSession(_ string) {}

// addEntry directly appends a pre-built client entry to the router.
// Used only by intra-package tests that need control over InferenceClient
// instances without going through a real socket dial.
func (r *PluginRouter) addEntry(client *InferenceClient, priority int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, routerEntry{client: client, priority: priority})
}

func (r *PluginRouter) probeAll(ctx context.Context) {
	r.mu.RLock()
	entries := make([]routerEntry, len(r.entries))
	copy(entries, r.entries)
	r.mu.RUnlock()

	probeTimeout := r.probeTimeout
	if probeTimeout == 0 {
		probeTimeout = 5 * time.Second
	}
	for _, e := range entries {
		probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
		status, err := e.client.HealthCheck(probeCtx)
		cancel()
		if err != nil {
			slog.Warn("inference health probe failed", "endpoint_id", e.client.endpoint.ID(), "error", err)
			continue
		}
		if status.State == commonpb.PluginState_PLUGIN_STATE_READY {
			e.client.endpoint.RecordSuccess()
		} else {
			e.client.endpoint.RecordFailure()
			slog.Warn("inference health probe unhealthy", "endpoint_id", e.client.endpoint.ID(), "state", status.State)
		}
	}
}
