// Package health defines shared types for the /health endpoint.
// It is imported by both the transport (HTTP handler) and runtime (checker)
// packages to avoid an import cycle.
package health

import "context"

// PluginStatus summarises the observable state of one plugin endpoint.
type PluginStatus struct {
	ID             string `json:"id"`
	PluginState    string `json:"plugin_state"`    // READY | LOADING | ERROR | UNKNOWN
	CircuitBreaker string `json:"circuit_breaker"` // closed | open | half_open
}

// Status is the JSON body returned by the /health endpoint.
type Status struct {
	Status   string         `json:"status"`  // ok | degraded | draining | error
	Draining bool           `json:"draining,omitempty"`
	Plugins  []PluginStatus `json:"plugins,omitempty"`
}

// Checker aggregates plugin health into a Status.
type Checker interface {
	Check(ctx context.Context) Status
}
