package plugin

import (
	"errors"
	"testing"
	"time"
)

// ── Pin / Pinned / Unpin ───────────────────────────────────────────────────────

func TestRouter_Pin_ReturnsSameClient(t *testing.T) {
	ep := newTestEndpoint(5, 0)
	client := NewInferenceClient(ep)
	r := NewPluginRouter("")
	r.addEntry(client, 0)

	pinned, err := r.Pin("sess-1")
	if err != nil {
		t.Fatalf("Pin: %v", err)
	}

	got, err := r.Pinned("sess-1")
	if err != nil {
		t.Fatalf("Pinned: %v", err)
	}
	if got != pinned {
		t.Fatal("Pinned returned a different client than Pin")
	}
}

func TestRouter_Pin_UnpinReleases(t *testing.T) {
	ep := newTestEndpoint(5, 0)
	r := NewPluginRouter("")
	r.addEntry(NewInferenceClient(ep), 0)

	if _, err := r.Pin("sess-2"); err != nil {
		t.Fatalf("Pin: %v", err)
	}
	r.Unpin("sess-2")

	_, err := r.Pinned("sess-2")
	if err == nil {
		t.Fatal("Pinned should error after Unpin")
	}
}

func TestRouter_Pin_UnhealthyEndpoint(t *testing.T) {
	// halfOpenTimeout=1h so the circuit stays OPEN during the test.
	ep := newTestEndpoint(1, time.Hour)
	r := NewPluginRouter("")
	r.addEntry(NewInferenceClient(ep), 0)

	// Pin while healthy.
	if _, err := r.Pin("sess-3"); err != nil {
		t.Fatalf("Pin: %v", err)
	}

	// Open the circuit.
	ep.RecordFailure()

	_, err := r.Pinned("sess-3")
	if !errors.Is(err, ErrPinnedEndpointLost) {
		t.Fatalf("want ErrPinnedEndpointLost, got %v", err)
	}
}

func TestRouter_Pin_NoHealthyEndpoint(t *testing.T) {
	// Endpoint with threshold=1 and long halfOpen so it stays OPEN after one failure.
	ep := newTestEndpoint(1, time.Hour)
	ep.RecordFailure() // open the circuit before Pin
	r := NewPluginRouter("")
	r.addEntry(NewInferenceClient(ep), 0)

	_, err := r.Pin("sess-4")
	if err == nil {
		t.Fatal("Pin should fail when no healthy endpoint is available")
	}
}

func TestRouter_Unpin_Idempotent(t *testing.T) {
	ep := newTestEndpoint(5, 0)
	r := NewPluginRouter("")
	r.addEntry(NewInferenceClient(ep), 0)

	if _, err := r.Pin("sess-5"); err != nil {
		t.Fatalf("Pin: %v", err)
	}
	r.Unpin("sess-5")
	r.Unpin("sess-5") // second call must not panic
}
