package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/speechmux/core/internal/health"
	"github.com/speechmux/core/internal/plugin"
	commonpb "github.com/speechmux/proto/gen/go/common/v1"
)

// vadHealthProber is satisfied by *plugin.Endpoint.
type vadHealthProber interface {
	HealthCheckProbe(ctx context.Context) (commonpb.PluginState, error)
}

// inferenceProberSource provides a fresh snapshot of inference probers on each
// call. Satisfied by *plugin.PluginRouter so that dynamically added endpoints
// are visible at health-check time without restarting Core.
type inferenceProberSource interface {
	InferenceProbers() []plugin.InferenceProber
}

// healthChecker gathers live status from plugin endpoints and implements health.Checker.
type healthChecker struct {
	vadProbers  []vadHealthProber
	inferProbers inferenceProberSource // queried fresh on every Check call
	draining    func() bool            // returns true during graceful shutdown
}

// Check queries all configured plugins and aggregates into a health.Status.
func (h *healthChecker) Check(ctx context.Context) health.Status {
	status := health.Status{Status: "ok"}
	if h.draining != nil && h.draining() {
		status.Draining = true
		status.Status = "draining"
	}

	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var anyReady, anyUnhealthy bool

	for i, p := range h.vadProbers {
		id := "vad"
		if len(h.vadProbers) > 1 {
			id = fmt.Sprintf("vad-%d", i)
		}
		state, err := p.HealthCheckProbe(probeCtx)
		ps := health.PluginStatus{ID: id, CircuitBreaker: "closed"}
		if err != nil {
			ps.PluginState = "UNKNOWN"
			anyUnhealthy = true
		} else {
			ps.PluginState = pluginStateName(state)
			if state == commonpb.PluginState_PLUGIN_STATE_READY {
				anyReady = true
			} else {
				anyUnhealthy = true
			}
		}
		status.Plugins = append(status.Plugins, ps)
	}

	for _, p := range h.inferProbers.InferenceProbers() {
		id, state, cbHealthy, err := p.HealthCheckProbe(probeCtx)
		ps := health.PluginStatus{ID: id, CircuitBreaker: "closed"}
		if !cbHealthy {
			ps.CircuitBreaker = "open"
		}
		if err != nil {
			ps.PluginState = "UNKNOWN"
			anyUnhealthy = true
		} else {
			ps.PluginState = pluginStateName(state)
			if state == commonpb.PluginState_PLUGIN_STATE_READY {
				anyReady = true
			} else {
				anyUnhealthy = true
			}
		}
		status.Plugins = append(status.Plugins, ps)
	}

	// Aggregate: any plugin unhealthy but at least one ready → degraded.
	if anyUnhealthy && status.Status == "ok" {
		if anyReady {
			status.Status = "degraded"
		} else if len(status.Plugins) > 0 {
			status.Status = "error"
		}
	}
	return status
}

func pluginStateName(s commonpb.PluginState) string {
	switch s {
	case commonpb.PluginState_PLUGIN_STATE_READY:
		return "READY"
	case commonpb.PluginState_PLUGIN_STATE_LOADING:
		return "LOADING"
	case commonpb.PluginState_PLUGIN_STATE_ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}
