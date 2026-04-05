// Package tracing configures the global OpenTelemetry TracerProvider for
// SpeechMux Core. All packages obtain their tracer via otel.Tracer(name) after
// Init has been called.
package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace/noop"
)

// Init configures the global OpenTelemetry TracerProvider.
//
// When endpoint is empty, a no-op provider is installed — all otel.Tracer calls
// return a tracer whose spans are discarded immediately with zero allocation.
//
// When endpoint is non-empty, an OTLP/gRPC exporter is created pointing to that
// address (e.g. "localhost:4317" for a local Jaeger or OpenTelemetry Collector).
// The connection is plaintext; add a TLS-terminating sidecar or collector if
// the endpoint is remote.
//
// Returns a shutdown function that flushes and stops the exporter. The caller
// must invoke it before process exit to avoid losing buffered spans.
func Init(ctx context.Context, serviceName, endpoint string, sampleRate float64) (func(context.Context) error, error) {
	if endpoint == "" {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(), // plaintext to collector/Jaeger; add TLS at proxy
		otlptracegrpc.WithEndpoint(endpoint),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: create OTLP exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		if shutdownErr := exp.Shutdown(ctx); shutdownErr != nil {
			return nil, fmt.Errorf("tracing: build resource: %w; exporter shutdown: %w", err, shutdownErr)
		}
		return nil, fmt.Errorf("tracing: build resource: %w", err)
	}

	sampler := resolveSampler(sampleRate)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// resolveSampler returns a Sampler for the given rate (0.0–1.0).
func resolveSampler(rate float64) sdktrace.Sampler {
	switch {
	case rate >= 1.0:
		return sdktrace.AlwaysSample()
	case rate <= 0.0:
		return sdktrace.NeverSample()
	default:
		// TraceIDRatioBased samples a consistent fraction without Collector coordination.
		return sdktrace.TraceIDRatioBased(rate)
	}
}
