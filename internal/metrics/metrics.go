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
}

// ── Prometheus implementation ─────────────────────────────────────────────────

// PrometheusMetrics records metrics to a Prometheus registry.
type PrometheusMetrics struct {
	registry      *prometheus.Registry
	activeSessions prometheus.Gauge
	sessionsTotal  prometheus.Counter
	decodeLatency  *prometheus.HistogramVec
	decodeTotal    *prometheus.CounterVec
	vadTriggers    prometheus.Counter
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
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
		}, []string{"type", "engine"}), // type = "final" | "partial"; engine = engine_name
		decodeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "speechmux_decode_requests_total",
			Help: "Total decode requests by type and outcome.",
		}, []string{"type", "status", "engine"}), // status = "ok" | "error"; engine = engine_name
		vadTriggers: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "speechmux_vad_triggers_total",
			Help: "Total number of EPD speech-start events (utterance starts).",
		}),
	}

	reg.MustRegister(
		m.activeSessions,
		m.sessionsTotal,
		m.decodeLatency,
		m.decodeTotal,
		m.vadTriggers,
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

func (m *PrometheusMetrics) RecordVADTrigger() { m.vadTriggers.Inc() }

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

func (NopMetrics) IncActiveSessions()                       {}
func (NopMetrics) DecActiveSessions()                       {}
func (NopMetrics) RecordDecodeLatency(_ float64, _ bool, _ string) {}
func (NopMetrics) RecordDecodeResult(_ bool, _ bool, _ string)     {}
func (NopMetrics) RecordVADTrigger()                        {}
