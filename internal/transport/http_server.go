package transport

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/speechmux/core/internal/health"
	"github.com/speechmux/core/internal/metrics"
)

// HTTPTimeouts configures the connection timeouts for an HTTPServer.
// Zero values fall back to sensible defaults (read/write: 10s, idle: 60s, shutdown: 5s).
type HTTPTimeouts struct {
	Read     time.Duration // Maximum duration to read an entire request, including the body.
	Write    time.Duration // Maximum duration to write a response, including handler execution time.
	Idle     time.Duration // Keep-alive idle timeout between requests on a persistent connection.
	Shutdown time.Duration // Graceful-shutdown drain timeout before connections are forcibly closed.
}

// HTTPServer serves the operations HTTP endpoints:
//
//	GET    /health                           — liveness + plugin state
//	GET    /metrics                          — Prometheus text exposition format
//	GET    /metrics.json                     — JSON metrics snapshot
//	POST   /admin/reload                     — config hot-reload (admin token required)
//	GET    /admin/plugins                    — list registered inference endpoints
//	POST   /admin/plugins/inference          — register a new inference endpoint
//	DELETE /admin/plugins/inference/{id}     — deregister an inference endpoint
type HTTPServer struct {
	server          *http.Server
	shutdownTimeout time.Duration
	metricsObs      *metrics.PrometheusMetrics // nil when metrics disabled
	healthChecker   health.Checker
	reloadFunc      func() error   // nil when reload is not configured
	adminToken      string         // "" = no auth required on admin endpoints
	registry        PluginRegistry // nil when dynamic plugin management is disabled
}

// NewHTTPServer constructs an HTTPServer on the given port.
// prom, checker, reloadFunc, and registry may be nil (those features are disabled).
// tlsCfg may be nil for plaintext; when non-nil the server requires TLS for all connections.
// Zero values in timeouts fall back to defaults (read/write: 10s, idle: 60s, shutdown: 5s).
func NewHTTPServer(
	port int,
	prom *metrics.PrometheusMetrics,
	checker health.Checker,
	reloadFunc func() error,
	adminToken string,
	tlsCfg *tls.Config,
	registry PluginRegistry,
	timeouts HTTPTimeouts,
) *HTTPServer {
	if timeouts.Read == 0 {
		timeouts.Read = 10 * time.Second
	}
	if timeouts.Write == 0 {
		timeouts.Write = 10 * time.Second
	}
	if timeouts.Idle == 0 {
		timeouts.Idle = 60 * time.Second
	}
	if timeouts.Shutdown == 0 {
		timeouts.Shutdown = 5 * time.Second
	}
	hs := &HTTPServer{
		metricsObs:      prom,
		healthChecker:   checker,
		reloadFunc:      reloadFunc,
		adminToken:      adminToken,
		registry:        registry,
		shutdownTimeout: timeouts.Shutdown,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", hs.handleHealth)

	if prom != nil {
		mux.Handle("/metrics", prom.TextHandler())
		mux.Handle("/metrics.json", prom.JSONHandler())
	}

	mux.HandleFunc("/admin/reload", hs.handleAdminReload)
	mux.HandleFunc("/admin/plugins", hs.handleAdminPluginsList)
	mux.HandleFunc("/admin/plugins/inference", hs.handleAdminPluginsInference)
	mux.HandleFunc("/admin/plugins/inference/{id}", hs.handleAdminPluginsInference)

	hs.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		TLSConfig:    tlsCfg, // non-nil enables HTTP/2 negotiation via ALPN
		ReadTimeout:  timeouts.Read,
		WriteTimeout: timeouts.Write,
		IdleTimeout:  timeouts.Idle,
	}
	return hs
}

// Serve starts the HTTP server and blocks until ctx is cancelled or the server errors.
// When TLSConfig is set the TCP listener is wrapped in a TLS listener before serving.
func (h *HTTPServer) Serve(ctx context.Context) error {
	rawLis, err := net.Listen("tcp", h.server.Addr)
	if err != nil {
		return err
	}

	var lis net.Listener = rawLis
	if h.server.TLSConfig != nil {
		lis = tls.NewListener(rawLis, h.server.TLSConfig)
	}
	slog.Info("HTTP server listening", "addr", h.server.Addr, "tls", h.server.TLSConfig != nil)

	errCh := make(chan error, 1)
	go func() {
		if err := h.server.Serve(lis); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), h.shutdownTimeout)
		defer cancel()
		return h.server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// handleHealth returns a JSON health.Status.
// Reports "ok", "degraded", "draining", or "error" based on plugin states.
func (h *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	var status health.Status
	if h.healthChecker != nil {
		status = h.healthChecker.Check(r.Context())
	} else {
		status = health.Status{Status: "ok"}
	}

	httpCode := http.StatusOK
	if status.Status == "error" {
		httpCode = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpCode)
	_ = json.NewEncoder(w).Encode(status)
}

// handleAdminReload triggers a config hot-reload.
// Requires the admin token in the Authorization header when adminToken is set.
func (h *HTTPServer) handleAdminReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.reloadFunc == nil {
		http.Error(w, `{"code":"ERR4001","message":"admin API disabled"}`,
			http.StatusNotImplemented)
		return
	}
	if !h.authorizeAdmin(w, r) {
		return
	}
	if err := h.reloadFunc(); err != nil {
		// Log the full error internally but never expose it to the caller:
		// the error may contain file paths, config keys, or other internal
		// details that should not be visible outside the admin endpoint.
		slog.Warn("config reload failed", "err", err)
		http.Error(w, `{"code":"ERR5001","message":"config reload failed"}`,
			http.StatusInternalServerError)
		return
	}
	slog.Info("config reloaded via admin API")
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"reloaded"}`))
}
