// Package metrics defines the MetricsObserver interface and its Prometheus
// implementation. Lower-level packages (session, stream) depend only on the
// interface, keeping the prometheus import isolated to this package.
package metrics

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsObserver is the interface that business-logic packages use to record
// operational metrics. It replaces the Python Hooks pattern with a Go interface.
type MetricsObserver interface {
	// Session lifecycle.
	IncActiveSessions()
	DecActiveSessions()

	// Decode pipeline.
	RecordDecodeLatency(secs float64, isFinal bool, engineName string)
	RecordDecodeResult(isFinal bool, ok bool, engineName string)

	// EPD / VAD.
	RecordVADTrigger()
	// RecordVADWatermarkLag records a watermark lag threshold-exceeded event.
	RecordVADWatermarkLag()

	// Streaming STT session lifecycle and latency.
	RecordStreamingSessionOpen(engineName string)
	RecordStreamingSessionClose(engineName string, reason string)
	RecordStreamingPartialLatency(latencySec float64, engineName string)
	RecordStreamingFinalizeLatency(latencySec float64, engineName string)
	RecordEngineResponseTimeout(engineName string)
}

// ── Prometheus implementation ─────────────────────────────────────────────────

// decodeLatencyBuckets are the shared histogram bucket boundaries used for all
// latency histograms.
var decodeLatencyBuckets = []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30}

// PrometheusMetrics records metrics to a Prometheus registry.
type PrometheusMetrics struct {
	registry             *prometheus.Registry
	activeSessions       prometheus.Gauge
	sessionsTotal        prometheus.Counter
	decodeLatency        *prometheus.HistogramVec
	decodeTotal          *prometheus.CounterVec
	vadTriggers          prometheus.Counter
	vadWatermarkLagTotal prometheus.Counter

	// Streaming STT observability.
	streamingActiveSessions  *prometheus.GaugeVec
	streamingPartialLatency  *prometheus.HistogramVec
	streamingFinalizeLatency *prometheus.HistogramVec
	streamingTerminations    *prometheus.CounterVec
	engineResponseTimeouts   *prometheus.CounterVec
}

// NewPrometheusMetrics creates and registers all SpeechMux metrics.
// Pass the returned *PrometheusMetrics to NewHandler() for the HTTP endpoint.
func NewPrometheusMetrics() *PrometheusMetrics {
	reg := prometheus.NewRegistry()
	m := &PrometheusMetrics{
		registry: reg,
		activeSessions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "speechmux_active_sessions",
			Help: "Number of currently active streaming sessions.",
		}),
		sessionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "speechmux_sessions_total",
			Help: "Total number of sessions created since process start.",
		}),
		decodeLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "speechmux_decode_latency_seconds",
			Help:    "Inference decode latency in seconds.",
			Buckets: decodeLatencyBuckets,
		}, []string{"type", "engine"}), // type = "final" | "partial"; engine = engine_name
		decodeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "speechmux_decode_requests_total",
			Help: "Total decode requests by type and outcome.",
		}, []string{"type", "status", "engine"}), // status = "ok" | "error"; engine = engine_name
		vadTriggers: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "speechmux_vad_triggers_total",
			Help: "Total number of EPD speech-start events (utterance starts).",
		}),
		vadWatermarkLagTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "speechmux_vad_watermark_lag_total",
			Help: "Total number of times the VAD watermark lag threshold was exceeded.",
		}),
		streamingActiveSessions: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "speechmux_streaming_sessions_active",
			Help: "Number of currently active streaming STT sessions.",
		}, []string{"engine_name"}),
		streamingPartialLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "speechmux_streaming_partial_latency_seconds",
			Help:    "Latency from audio chunk send time to partial hypothesis receipt.",
			Buckets: decodeLatencyBuckets,
		}, []string{"engine_name"}),
		streamingFinalizeLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "speechmux_streaming_finalize_latency_seconds",
			Help:    "Latency from FINALIZE_UTTERANCE send to is_final hypothesis receipt.",
			Buckets: decodeLatencyBuckets,
		}, []string{"engine_name"}),
		streamingTerminations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "speechmux_streaming_session_terminations_total",
			Help: "Total streaming session terminations by engine and reason.",
		}, []string{"engine_name", "reason"}),
		engineResponseTimeouts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "speechmux_engine_response_timeout_total",
			Help: "Total engine response lag timeout events.",
		}, []string{"engine_name"}),
	}

	reg.MustRegister(
		m.activeSessions,
		m.sessionsTotal,
		m.decodeLatency,
		m.decodeTotal,
		m.vadTriggers,
		m.vadWatermarkLagTotal,
		m.streamingActiveSessions,
		m.streamingPartialLatency,
		m.streamingFinalizeLatency,
		m.streamingTerminations,
		m.engineResponseTimeouts,
		// Standard Go runtime metrics.
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	return m
}

func (m *PrometheusMetrics) IncActiveSessions() {
	m.activeSessions.Inc()
	m.sessionsTotal.Inc()
}
func (m *PrometheusMetrics) DecActiveSessions() { m.activeSessions.Dec() }

func (m *PrometheusMetrics) RecordDecodeLatency(secs float64, isFinal bool, engineName string) {
	t := decodeType(isFinal)
	m.decodeLatency.WithLabelValues(t, engineName).Observe(secs)
}

func (m *PrometheusMetrics) RecordDecodeResult(isFinal bool, ok bool, engineName string) {
	t := decodeType(isFinal)
	status := "ok"
	if !ok {
		status = "error"
	}
	m.decodeTotal.WithLabelValues(t, status, engineName).Inc()
}

func (m *PrometheusMetrics) RecordVADTrigger()      { m.vadTriggers.Inc() }
func (m *PrometheusMetrics) RecordVADWatermarkLag() { m.vadWatermarkLagTotal.Inc() }

func (m *PrometheusMetrics) RecordStreamingSessionOpen(engineName string) {
	m.streamingActiveSessions.WithLabelValues(engineName).Inc()
}

func (m *PrometheusMetrics) RecordStreamingSessionClose(engineName string, reason string) {
	m.streamingActiveSessions.WithLabelValues(engineName).Dec()
	m.streamingTerminations.WithLabelValues(engineName, reason).Inc()
}

func (m *PrometheusMetrics) RecordStreamingPartialLatency(latencySec float64, engineName string) {
	m.streamingPartialLatency.WithLabelValues(engineName).Observe(latencySec)
}

func (m *PrometheusMetrics) RecordStreamingFinalizeLatency(latencySec float64, engineName string) {
	m.streamingFinalizeLatency.WithLabelValues(engineName).Observe(latencySec)
}

func (m *PrometheusMetrics) RecordEngineResponseTimeout(engineName string) {
	m.engineResponseTimeouts.WithLabelValues(engineName).Inc()
}

// TextHandler returns an http.Handler that serves /metrics in Prometheus text format.
func (m *PrometheusMetrics) TextHandler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// JSONHandler returns an http.Handler that serves /metrics.json as a flat JSON map.
func (m *PrometheusMetrics) JSONHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mfs, err := m.registry.Gather()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result := make(map[string]any, len(mfs))
		for _, mf := range mfs {
			name := mf.GetName()
			metrics := mf.GetMetric()
			if len(metrics) == 1 && len(metrics[0].GetLabel()) == 0 {
				// Single value: expose as scalar.
				switch {
				case metrics[0].GetGauge() != nil:
					result[name] = metrics[0].GetGauge().GetValue()
				case metrics[0].GetCounter() != nil:
					result[name] = metrics[0].GetCounter().GetValue()
				default:
					result[name] = metrics[0].String()
				}
			} else {
				// Multiple label combinations: expose as list.
				result[name] = metrics
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})
}

func decodeType(isFinal bool) string {
	if isFinal {
		return "final"
	}
	return "partial"
}

// ── NopMetrics ────────────────────────────────────────────────────────────────

// NopMetrics is a no-op MetricsObserver for use in tests and when no metrics
// backend is configured.
type NopMetrics struct{}

func (NopMetrics) IncActiveSessions()                              {}
func (NopMetrics) DecActiveSessions()                              {}
func (NopMetrics) RecordDecodeLatency(_ float64, _ bool, _ string) {}
func (NopMetrics) RecordDecodeResult(_ bool, _ bool, _ string)     {}
func (NopMetrics) RecordVADTrigger()                               {}
func (NopMetrics) RecordVADWatermarkLag()                          {}

func (NopMetrics) RecordStreamingSessionOpen(_ string)                    {}
func (NopMetrics) RecordStreamingSessionClose(_ string, _ string)         {}
func (NopMetrics) RecordStreamingPartialLatency(_ float64, _ string)      {}
func (NopMetrics) RecordStreamingFinalizeLatency(_ float64, _ string)     {}
func (NopMetrics) RecordEngineResponseTimeout(_ string)                   {}
