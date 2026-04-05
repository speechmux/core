package plugin

import (
	"testing"
)

// newRouterWith is a test helper that builds a PluginRouter pre-loaded with
// the given InferenceClients (all at priority 0, round-robin mode).
func newRouterWith(clients ...*InferenceClient) *PluginRouter {
	r := NewPluginRouter(RoutingRoundRobin)
	for _, c := range clients {
		r.addEntry(c, 0)
	}
	return r
}

// ── PluginRouter.Route ────────────────────────────────────────────────────────

func TestPluginRouter_NoClients_ReturnsError(t *testing.T) {
	r := NewPluginRouter("")
	_, err := r.Route()
	if err == nil {
		t.Fatal("expected error with no clients, got nil")
	}
}

func TestPluginRouter_AllUnhealthy_ReturnsError(t *testing.T) {
	ep := unhealthyEndpoint()
	c := &InferenceClient{endpoint: ep}
	r := newRouterWith(c)

	_, err := r.Route()
	if err == nil {
		t.Fatal("expected error when all endpoints are unhealthy, got nil")
	}
}

func TestPluginRouter_HealthyEndpointReturned(t *testing.T) {
	ep := healthyEndpoint()
	c := &InferenceClient{endpoint: ep}
	r := newRouterWith(c)

	got, err := r.Route()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != c {
		t.Error("expected the only healthy client to be returned")
	}
}

func TestPluginRouter_SkipsUnhealthyPicksHealthy(t *testing.T) {
	bad := &InferenceClient{endpoint: unhealthyEndpoint()}
	good := &InferenceClient{endpoint: healthyEndpoint()}
	r := newRouterWith(bad, good)

	got, err := r.Route()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != good {
		t.Error("expected the healthy client to be returned, got the unhealthy one")
	}
}

func TestPluginRouter_RoundRobin(t *testing.T) {
	c1 := &InferenceClient{endpoint: healthyEndpoint()}
	c2 := &InferenceClient{endpoint: healthyEndpoint()}
	r := newRouterWith(c1, c2)

	got1, _ := r.Route()
	got2, _ := r.Route()
	got3, _ := r.Route()

	if got1 == got2 {
		t.Error("expected different clients on consecutive calls")
	}
	if got1 != got3 {
		t.Error("expected round-robin to wrap around to c1")
	}
}

// ── PluginRouter.Add / Remove / List ─────────────────────────────────────────

func TestPluginRouter_List_Empty(t *testing.T) {
	r := NewPluginRouter("")
	if got := r.List(); len(got) != 0 {
		t.Fatalf("expected empty list, got %d entries", len(got))
	}
}

func TestPluginRouter_List_ReflectsClients(t *testing.T) {
	ep1 := &Endpoint{id: "ep1", socket: "/tmp/ep1.sock", failureThreshold: 5}
	ep2 := &Endpoint{id: "ep2", socket: "/tmp/ep2.sock", failureThreshold: 5}
	r := newRouterWith(&InferenceClient{endpoint: ep1}, &InferenceClient{endpoint: ep2})

	list := r.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(list))
	}
	if list[0].ID != "ep1" || list[1].ID != "ep2" {
		t.Errorf("unexpected IDs: %v", list)
	}
	if list[0].Socket != "/tmp/ep1.sock" {
		t.Errorf("unexpected socket: %s", list[0].Socket)
	}
	if list[0].CircuitBreaker != "closed" {
		t.Errorf("expected closed circuit, got %s", list[0].CircuitBreaker)
	}
}

func TestPluginRouter_List_ReflectsEngineFields(t *testing.T) {
	ep := &Endpoint{id: "ep1", socket: "/tmp/ep1.sock", failureThreshold: 5}
	c := &InferenceClient{
		endpoint:   ep,
		engineName: "mlx_whisper",
		modelSize:  "large-v3-turbo",
		device:     "mps",
	}
	r := newRouterWith(c)

	list := r.List()
	if len(list) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(list))
	}
	s := list[0]
	if s.EngineName != "mlx_whisper" {
		t.Errorf("EngineName = %q, want %q", s.EngineName, "mlx_whisper")
	}
	if s.ModelSize != "large-v3-turbo" {
		t.Errorf("ModelSize = %q, want %q", s.ModelSize, "large-v3-turbo")
	}
	if s.Device != "mps" {
		t.Errorf("Device = %q, want %q", s.Device, "mps")
	}
}

func TestPluginRouter_Remove_Found(t *testing.T) {
	ep := &Endpoint{id: "ep1", socket: "/tmp/ep1.sock", failureThreshold: 5}
	r := newRouterWith(&InferenceClient{endpoint: ep})

	if err := r.Remove("ep1"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if len(r.entries) != 0 {
		t.Error("expected entry list to be empty after remove")
	}
}

func TestPluginRouter_Remove_NotFound(t *testing.T) {
	r := NewPluginRouter("")
	if err := r.Remove("nonexistent"); err == nil {
		t.Fatal("expected error when removing nonexistent id")
	}
}

func TestPluginRouter_Add_DuplicateID(t *testing.T) {
	ep := &Endpoint{id: "ep1", socket: "/tmp/ep1.sock", failureThreshold: 5}
	r := newRouterWith(&InferenceClient{endpoint: ep})

	// Adding the same id without a real socket should fail on the duplicate check,
	// not on the dial (which would fail anyway without a real socket).
	err := r.Add("ep1", "/tmp/ep1.sock", 0)
	if err == nil {
		t.Fatal("expected duplicate-id error")
	}
}

func TestPluginRouter_InferenceProbers_Snapshot(t *testing.T) {
	ep1 := &Endpoint{id: "p1", failureThreshold: 5}
	ep2 := &Endpoint{id: "p2", failureThreshold: 5}
	r := newRouterWith(&InferenceClient{endpoint: ep1}, &InferenceClient{endpoint: ep2})

	probers := r.InferenceProbers()
	if len(probers) != 2 {
		t.Fatalf("expected 2 probers, got %d", len(probers))
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

// healthyEndpoint returns an Endpoint in CLOSED (healthy) state.
func healthyEndpoint() *Endpoint {
	ep := &Endpoint{failureThreshold: 5}
	// state defaults to circuitClosed (0) — healthy.
	return ep
}

// unhealthyEndpoint returns an Endpoint in OPEN (unhealthy) state with a very
// long halfOpenTimeout so it won't self-heal during the test.
func unhealthyEndpoint() *Endpoint {
	ep := &Endpoint{failureThreshold: 1, halfOpenTimeout: 1e12} // ~31 years
	ep.RecordFailure() // opens the circuit
	return ep
}
