package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/speechmux/core/internal/plugin"
	commonpb "github.com/speechmux/proto/gen/go/common/v1"
)

// ── stub implementations ──────────────────────────────────────────────────────

// stubVADProber is a test double for vadHealthProber.
type stubVADProber struct {
	state commonpb.PluginState
	err   error
}

func (s *stubVADProber) HealthCheckProbe(_ context.Context) (commonpb.PluginState, error) {
	return s.state, s.err
}

// stubInferenceProber is a test double for plugin.InferenceProber.
type stubInferenceProber struct {
	id        string
	state     commonpb.PluginState
	cbHealthy bool
	err       error
}

func (s *stubInferenceProber) HealthCheckProbe(_ context.Context) (string, commonpb.PluginState, bool, error) {
	return s.id, s.state, s.cbHealthy, s.err
}

// stubInferProberSource wraps a fixed slice of probers and satisfies inferenceProberSource.
type stubInferProberSource struct {
	probers []*stubInferenceProber
}

func (s *stubInferProberSource) InferenceProbers() []plugin.InferenceProber {
	result := make([]plugin.InferenceProber, len(s.probers))
	for i, p := range s.probers {
		result[i] = p
	}
	return result
}

// ── helpers ───────────────────────────────────────────────────────────────────

// newChecker is a convenience constructor for tests.
// vad may be nil (no VAD prober). infer is the list of stub inference probers.
func newChecker(vad vadHealthProber, infer []*stubInferenceProber, draining func() bool) *healthChecker {
	var vadProbers []vadHealthProber
	if vad != nil {
		vadProbers = []vadHealthProber{vad}
	}
	return &healthChecker{
		vadProbers:   vadProbers,
		inferProbers: &stubInferProberSource{probers: infer},
		draining:     draining,
	}
}

// ── pluginStateName tests ─────────────────────────────────────────────────────

// TestPluginStateName_AllEnumValues verifies that every known PluginState enum
// value is mapped to the expected human-readable string.
func TestPluginStateName_AllEnumValues(t *testing.T) {
	cases := []struct {
		input commonpb.PluginState
		want  string
	}{
		{commonpb.PluginState_PLUGIN_STATE_READY, "READY"},
		{commonpb.PluginState_PLUGIN_STATE_LOADING, "LOADING"},
		{commonpb.PluginState_PLUGIN_STATE_ERROR, "ERROR"},
		// DRAINING and UNKNOWN fall through to the default branch.
		{commonpb.PluginState_PLUGIN_STATE_DRAINING, "UNKNOWN"},
		{commonpb.PluginState_PLUGIN_STATE_UNKNOWN, "UNKNOWN"},
		{commonpb.PluginState(999), "UNKNOWN"},
	}

	for _, tc := range cases {
		got := pluginStateName(tc.input)
		if got != tc.want {
			t.Errorf("pluginStateName(%v) = %q; want %q", tc.input, got, tc.want)
		}
	}
}

// ── healthChecker.Check tests ─────────────────────────────────────────────────

// TestHealthChecker_AllPluginsReady verifies that when both the VAD and an
// inference endpoint report READY, the aggregate status is "ok".
func TestHealthChecker_AllPluginsReady(t *testing.T) {
	vad := &stubVADProber{state: commonpb.PluginState_PLUGIN_STATE_READY}
	infer := &stubInferenceProber{id: "stt-0", state: commonpb.PluginState_PLUGIN_STATE_READY, cbHealthy: true}

	hc := newChecker(vad, []*stubInferenceProber{infer}, nil)
	status := hc.Check(context.Background())

	if status.Status != "ok" {
		t.Errorf("status.Status = %q; want %q", status.Status, "ok")
	}
	if status.Draining {
		t.Error("status.Draining should be false when draining func is nil")
	}
	if len(status.Plugins) != 2 {
		t.Fatalf("len(status.Plugins) = %d; want 2", len(status.Plugins))
	}
	for _, ps := range status.Plugins {
		if ps.PluginState != "READY" {
			t.Errorf("plugin %s: PluginState = %q; want READY", ps.ID, ps.PluginState)
		}
		if ps.CircuitBreaker != "closed" {
			t.Errorf("plugin %s: CircuitBreaker = %q; want closed", ps.ID, ps.CircuitBreaker)
		}
	}
}

// TestHealthChecker_DrainingFlag verifies that when the draining function
// returns true, the aggregate status field is "draining" and Draining is true,
// regardless of plugin health.
func TestHealthChecker_DrainingFlag(t *testing.T) {
	vad := &stubVADProber{state: commonpb.PluginState_PLUGIN_STATE_READY}

	drainingTrue := func() bool { return true }
	hc := newChecker(vad, nil, drainingTrue)
	status := hc.Check(context.Background())

	if status.Status != "draining" {
		t.Errorf("status.Status = %q; want %q", status.Status, "draining")
	}
	if !status.Draining {
		t.Error("status.Draining should be true when draining func returns true")
	}
}

// TestHealthChecker_OnePluginError verifies that when all plugins report ERROR
// and none reports READY, the aggregate status is "error".
func TestHealthChecker_OnePluginError(t *testing.T) {
	vad := &stubVADProber{state: commonpb.PluginState_PLUGIN_STATE_ERROR}

	hc := newChecker(vad, nil, nil)
	status := hc.Check(context.Background())

	if status.Status != "error" {
		t.Errorf("status.Status = %q; want %q", status.Status, "error")
	}
	if len(status.Plugins) != 1 {
		t.Fatalf("len(status.Plugins) = %d; want 1", len(status.Plugins))
	}
	if status.Plugins[0].PluginState != "ERROR" {
		t.Errorf("plugin PluginState = %q; want ERROR", status.Plugins[0].PluginState)
	}
}

// TestHealthChecker_MixedReadyAndError verifies that when one plugin is READY
// and another is ERROR, the aggregate status is "degraded".
func TestHealthChecker_MixedReadyAndError(t *testing.T) {
	vad := &stubVADProber{state: commonpb.PluginState_PLUGIN_STATE_READY}
	inferErr := &stubInferenceProber{
		id:        "stt-0",
		state:     commonpb.PluginState_PLUGIN_STATE_ERROR,
		cbHealthy: true,
	}

	hc := newChecker(vad, []*stubInferenceProber{inferErr}, nil)
	status := hc.Check(context.Background())

	if status.Status != "degraded" {
		t.Errorf("status.Status = %q; want %q", status.Status, "degraded")
	}
}

// TestHealthChecker_NoPluginsConfigured verifies that when neither a VAD prober
// nor any inference probers are configured, the aggregate status is "ok" and
// the Plugins slice is empty.
func TestHealthChecker_NoPluginsConfigured(t *testing.T) {
	hc := newChecker(nil, nil, nil)
	status := hc.Check(context.Background())

	if status.Status != "ok" {
		t.Errorf("status.Status = %q; want %q", status.Status, "ok")
	}
	if len(status.Plugins) != 0 {
		t.Errorf("len(status.Plugins) = %d; want 0", len(status.Plugins))
	}
}

// TestHealthChecker_VADProbeError verifies that when the VAD probe returns an
// error, its PluginState is "UNKNOWN" and the aggregate status is "error" (no
// plugin reported READY).
func TestHealthChecker_VADProbeError(t *testing.T) {
	vad := &stubVADProber{err: errors.New("connection refused")}

	hc := newChecker(vad, nil, nil)
	status := hc.Check(context.Background())

	if status.Status != "error" {
		t.Errorf("status.Status = %q; want %q", status.Status, "error")
	}
	if len(status.Plugins) != 1 {
		t.Fatalf("len(status.Plugins) = %d; want 1", len(status.Plugins))
	}
	if status.Plugins[0].PluginState != "UNKNOWN" {
		t.Errorf("PluginState = %q; want UNKNOWN", status.Plugins[0].PluginState)
	}
}

// TestHealthChecker_InferenceCircuitOpen verifies that when an inference prober
// reports cbHealthy=false, the CircuitBreaker field is "open".
func TestHealthChecker_InferenceCircuitOpen(t *testing.T) {
	infer := &stubInferenceProber{
		id:        "stt-0",
		state:     commonpb.PluginState_PLUGIN_STATE_READY,
		cbHealthy: false,
	}

	hc := newChecker(nil, []*stubInferenceProber{infer}, nil)
	status := hc.Check(context.Background())

	if len(status.Plugins) != 1 {
		t.Fatalf("len(status.Plugins) = %d; want 1", len(status.Plugins))
	}
	if status.Plugins[0].CircuitBreaker != "open" {
		t.Errorf("CircuitBreaker = %q; want open", status.Plugins[0].CircuitBreaker)
	}
}

// TestHealthChecker_LoadingPlugin verifies that a LOADING plugin is treated as
// unhealthy (not READY), so the aggregate status is "error" when it is the only
// configured plugin.
func TestHealthChecker_LoadingPlugin(t *testing.T) {
	vad := &stubVADProber{state: commonpb.PluginState_PLUGIN_STATE_LOADING}

	hc := newChecker(vad, nil, nil)
	status := hc.Check(context.Background())

	if status.Status != "error" {
		t.Errorf("status.Status = %q; want %q", status.Status, "error")
	}
	if len(status.Plugins) != 1 {
		t.Fatalf("len(status.Plugins) = %d; want 1", len(status.Plugins))
	}
	if status.Plugins[0].PluginState != "LOADING" {
		t.Errorf("PluginState = %q; want LOADING", status.Plugins[0].PluginState)
	}
}

// TestHealthChecker_DrainingDoesNotOverrideError verifies that even when the
// draining flag is set, a plugin ERROR does not change the "draining" status
// into "error" — the draining flag wins on the status field.
func TestHealthChecker_DrainingDoesNotOverrideError(t *testing.T) {
	vad := &stubVADProber{state: commonpb.PluginState_PLUGIN_STATE_ERROR}

	hc := newChecker(vad, nil, func() bool { return true })
	status := hc.Check(context.Background())

	// When draining=true the status starts as "draining"; the unhealthy plugin
	// does not override it because the anyUnhealthy branch only fires when
	// status.Status == "ok".
	if status.Status != "draining" {
		t.Errorf("status.Status = %q; want %q", status.Status, "draining")
	}
	if !status.Draining {
		t.Error("status.Draining should be true")
	}
}
