// Package runtime assembles and runs the SpeechMux Core application.
package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/speechmux/core/internal/codec"
	"github.com/speechmux/core/internal/config"
	"github.com/speechmux/core/internal/metrics"
	"github.com/speechmux/core/internal/plugin"
	"github.com/speechmux/core/internal/session"
	"github.com/speechmux/core/internal/storage"
	"github.com/speechmux/core/internal/stream"
	"github.com/speechmux/core/internal/tracing"
	"github.com/speechmux/core/internal/transport"
)

// Application is the root component that wires together all subsystems and
// manages the server lifecycle with a 3-phase graceful shutdown.
type Application struct {
	cfgLoader           *config.Loader
	cfg                 *atomic.Pointer[config.Config]
	reloadMu            sync.Mutex // serialises concurrent /admin/reload requests
	sessionManager      *session.Manager
	grpc                *transport.GRPCServer
	http                *transport.HTTPServer
	websocket           *transport.WebSocketHandler
	stor                *storage.AudioStorage        // nil when storage is disabled
	vadEndpoints        []*plugin.Endpoint           // empty when VAD plugin is not configured
	inferRouter         *plugin.PluginRouter         // nil when no inference endpoints are configured
	healthProbeInterval time.Duration                // 0 means no health probe
	healthProbeTimeout  time.Duration                // per-probe HealthCheck deadline
	draining            atomic.Bool                  // set during graceful shutdown (for health endpoint)
	tracingShutdown     func(context.Context) error  // flushes and stops the OTel exporter; never nil
}

// New assembles the dependency graph from the provided configs.
func New(cfgLoader *config.Loader, pluginsCfg *config.PluginsConfig) (*Application, error) {
	cfg := cfgLoader.Load()

	var ptr atomic.Pointer[config.Config]
	ptr.Store(cfg)

	// Metrics — Prometheus registry.
	prom := metrics.NewPrometheusMetrics()

	sm := session.NewManager(cfg, prom)

	// Wire VAD endpoints from the pool (supports multiple instances).
	var vadEndpoints []*plugin.Endpoint
	if pluginsCfg != nil {
		config.ValidatePlugins(pluginsCfg)
		vadCB := plugin.EndpointCircuitBreaker{
			FailureThreshold: pluginsCfg.VAD.CircuitBreaker.FailureThreshold,
			HalfOpenTimeout:  pluginsCfg.VAD.CircuitBreaker.HalfOpenTimeout,
		}
		for _, epCfg := range pluginsCfg.VAD.AllEndpoints() {
			ep, err := plugin.NewEndpoint(epCfg.ID, epCfg.Socket, vadCB)
			if err != nil {
				// Log but do not fail startup — the circuit breaker will handle recovery.
				slog.Warn("VAD endpoint dial failed; plugin unavailable", "id", epCfg.ID, "error", err)
				continue
			}
			vadEndpoints = append(vadEndpoints, ep)
		}
	}

	// Always create the inference router and scheduler so that inference endpoints
	// can be added dynamically via the Admin API without restarting Core.
	routingMode := ""
	if pluginsCfg != nil {
		routingMode = pluginsCfg.Inference.RoutingMode
	}
	inferRouter := plugin.NewPluginRouter(routingMode)
	if pluginsCfg != nil {
		inferRouter.SetCircuitBreaker(plugin.EndpointCircuitBreaker{
			FailureThreshold: pluginsCfg.Inference.CircuitBreaker.FailureThreshold,
			HalfOpenTimeout:  pluginsCfg.Inference.CircuitBreaker.HalfOpenTimeout,
		})
	}
	scheduler := stream.NewDecodeScheduler(inferRouter, 0, cfg.Stream.DecodeTimeoutSec, prom)

	// Populate the router from the static configuration (if any).
	if pluginsCfg != nil {
		for _, epCfg := range pluginsCfg.Inference.Endpoints {
			if err := inferRouter.Add(epCfg.ID, epCfg.Socket, epCfg.Priority); err != nil {
				slog.Warn("inference endpoint init failed", "id", epCfg.ID, "err", err)
			}
		}
	}

	// Wire the stream processor with both VAD and inference.
	// Use the SessionProcessor interface type so that a nil concrete pointer
	// is not wrapped in a non-nil interface (Go nil-interface pitfall).
	var proc transport.SessionProcessor
	if len(vadEndpoints) > 0 {
		proc = stream.NewStreamProcessor(&ptr, vadEndpoints, scheduler, prom)
	}

	// Build codec converter targeting the configured sample rate.
	converter := codec.NewCodecConverter(cfg.Codec.TargetSampleRate)

	// Build audio storage (nil when disabled).
	stor := storage.NewAudioStorage(cfg.Storage)

	// Health probe interval and per-probe timeout: use configured values when available.
	healthProbeInterval := 30 * time.Second
	healthProbeTimeout := 5 * time.Second
	if pluginsCfg != nil && pluginsCfg.Inference.HealthCheckIntervalSec > 0 {
		healthProbeInterval = time.Duration(pluginsCfg.Inference.HealthCheckIntervalSec) * time.Second
	}
	if pluginsCfg != nil && pluginsCfg.Inference.HealthProbeTimeoutSec > 0 {
		healthProbeTimeout = time.Duration(pluginsCfg.Inference.HealthProbeTimeoutSec) * time.Second
	}

	// Initialise OpenTelemetry tracing. When endpoint is empty, a no-op provider
	// is installed so all otel.Tracer calls become zero-cost.
	tracingShutdown, err := tracing.Init(
		context.Background(),
		cfg.OTel.ServiceName,
		cfg.OTel.Endpoint,
		cfg.OTel.SampleRate,
	)
	if err != nil {
		return nil, fmt.Errorf("application: %w", err)
	}

	app := &Application{
		cfgLoader:           cfgLoader,
		cfg:                 &ptr,
		sessionManager:      sm,
		stor:                stor,
		vadEndpoints:        vadEndpoints,
		inferRouter:         inferRouter,
		healthProbeInterval: healthProbeInterval,
		healthProbeTimeout:  healthProbeTimeout,
		tracingShutdown:     tracingShutdown,
	}

	// Build HealthChecker.
	// Inference probers are read from the router at each Check call so that
	// dynamically added endpoints are reflected without restarting.
	vadProbers := make([]vadHealthProber, len(vadEndpoints))
	for i, ep := range vadEndpoints {
		vadProbers[i] = ep
	}
	hc := &healthChecker{
		vadProbers:   vadProbers,
		inferProbers: inferRouter,
		draining:     func() bool { return app.draining.Load() },
	}

	// Admin token for /admin/reload (use auth_secret as a simple admin token for now).
	adminToken := cfg.Auth.AuthSecret

	// Build TLS config once at startup (cert/key loaded from disk).
	// TLS config changes require a restart — hot-reload does not re-read certs.
	tlsCfg, err := config.BuildTLSConfig(cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("application: TLS setup: %w", err)
	}

	app.grpc = transport.NewGRPCServer(cfg.Server.GRPCPort, sm, proc, tlsCfg)
	app.http = transport.NewHTTPServer(
		cfg.Server.HTTPPort, prom, hc,
		func() error { return app.reloadConfig() },
		adminToken,
		tlsCfg,
		app,
		transport.HTTPTimeouts{
			Read:     cfg.Server.HTTPReadTimeout,
			Write:    cfg.Server.HTTPWriteTimeout,
			Idle:     cfg.Server.HTTPIdleTimeout,
			Shutdown: cfg.Server.HTTPShutdownTimeout,
		},
	)
	app.websocket = transport.NewWebSocketHandler(cfg.Server.WSPort, sm, proc, converter, stor, tlsCfg, cfg.Server.ResumableSessionTimeoutSec, cfg.Server.AllowedOrigins)

	return app, nil
}

// AddInferenceEndpoint implements transport.PluginRegistry.
// Endpoints added via the Admin API receive priority 0 (lowest).
func (a *Application) AddInferenceEndpoint(id, socket string) error {
	return a.inferRouter.Add(id, socket, 0)
}

// RemoveInferenceEndpoint implements transport.PluginRegistry.
func (a *Application) RemoveInferenceEndpoint(id string) error {
	return a.inferRouter.Remove(id)
}

// ListInferenceEndpoints implements transport.PluginRegistry.
func (a *Application) ListInferenceEndpoints() []plugin.EndpointSummary {
	return a.inferRouter.List()
}

// reloadConfig re-reads the config file and updates the atomic pointer.
// Fields with "immediate" reload semantics take effect on the next processor tick;
// fields with "new session" semantics take effect on the next CreateSession call.
// Fields requiring restart are logged as warnings.
//
// reloadMu serialises concurrent /admin/reload calls to avoid interleaving
// Reload+Load pairs that could store a stale or partially-updated config.
func (a *Application) reloadConfig() error {
	a.reloadMu.Lock()
	defer a.reloadMu.Unlock()
	if err := a.cfgLoader.Reload(); err != nil {
		return err
	}
	newCfg := a.cfgLoader.Load()
	a.cfg.Store(newCfg)
	slog.Info("config reloaded",
		"vad_silence_sec", newCfg.Stream.VADSilenceSec,
		"decode_timeout_sec", newCfg.Stream.DecodeTimeoutSec,
		"max_sessions", newCfg.Server.MaxSessions,
	)
	return nil
}

// Run starts all servers and blocks until shutdown is complete.
// It performs a 3-phase graceful shutdown on SIGTERM or SIGINT:
//
//	Phase 1 — stop accepting new gRPC streams (GracefulStop).
//	Phase 2 — drain existing sessions (DrainAll with timeout).
//	Phase 3 — force-stop all remaining connections.
func (a *Application) Run(ctx context.Context) error {
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error { return a.grpc.Serve(gCtx) })
	g.Go(func() error { return a.http.Serve(gCtx) })
	g.Go(func() error { return a.websocket.Serve(gCtx) })
	g.Go(func() error { return a.gracefulShutdown(gCtx) })

	// Start inference health probe with gCtx so the goroutine stops cleanly
	// during graceful shutdown.  context.Background() was previously used here
	// which left the goroutine running after all servers had stopped.
	if a.inferRouter != nil && a.healthProbeInterval > 0 {
		a.inferRouter.StartHealthProbe(gCtx, a.healthProbeInterval, a.healthProbeTimeout)
	}

	// SIGHUP → config hot-reload.
	go a.sighupLoop(gCtx)

	// Storage janitor stops automatically when gCtx is cancelled.
	a.stor.RunJanitor(gCtx)

	// Idle session reaper: closes sessions that have not sent audio within
	// session_timeout_sec. Parked sessions are excluded.
	a.sessionManager.RunIdleChecker(gCtx)

	if err := g.Wait(); err != nil {
		return fmt.Errorf("application: %w", err)
	}

	// Flush any buffered spans before process exit.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.tracingShutdown(shutdownCtx); err != nil {
		slog.Warn("tracing shutdown error", "err", err)
	}
	return nil
}

// sighupLoop listens for SIGHUP and reloads config.
func (a *Application) sighupLoop(ctx context.Context) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	defer signal.Stop(ch)
	for {
		select {
		case <-ch:
			slog.Info("SIGHUP received — reloading config")
			if err := a.reloadConfig(); err != nil {
				slog.Warn("config reload on SIGHUP failed", "err", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// gracefulShutdown waits for a termination signal, then executes the 3-phase shutdown.
func (a *Application) gracefulShutdown(ctx context.Context) error {
	sig := waitForSignal(ctx, syscall.SIGTERM, syscall.SIGINT)
	if sig == nil {
		// Context cancelled before a signal arrived.
		return nil
	}
	slog.Info("shutdown initiated", "signal", sig)

	// Signal health endpoint to return "draining".
	a.draining.Store(true)

	// Phase 1: Stop accepting new gRPC streams; existing streams continue.
	a.sessionManager.StopAccepting()
	a.grpc.GracefulStop()
	slog.Info("gRPC graceful stop complete")

	// Phase 2: Drain existing sessions.
	drainTimeout := a.cfg.Load().Server.ShutdownDrainSec
	slog.Info("draining sessions", "timeout", drainTimeout)
	drainCtx, drainCancel := context.WithTimeout(context.Background(), drainTimeout)
	defer drainCancel()
	a.sessionManager.DrainAll(drainCtx)

	// Phase 3: Force-stop any remaining connections.
	a.grpc.Stop()
	slog.Info("shutdown complete")
	return nil
}

// waitForSignal blocks until one of the given OS signals arrives or ctx is cancelled.
// Returns the received signal, or nil if ctx was cancelled first.
func waitForSignal(ctx context.Context, sigs ...os.Signal) os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, sigs...)
	defer signal.Stop(ch)

	select {
	case sig := <-ch:
		return sig
	case <-ctx.Done():
		return nil
	}
}
