package plugin

import (
	"context"
	"fmt"
	"sync/atomic"

	"google.golang.org/protobuf/types/known/emptypb"

	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
)


// InferenceClient wraps an Endpoint and calls the InferencePlugin Transcribe RPC.
// Circuit-breaker state is managed by the underlying Endpoint: each RPC error
// increments the failure counter; success resets it.
type InferenceClient struct {
	endpoint   *Endpoint
	stub       inferencepb.InferencePluginClient
	inflight   atomic.Int64 // number of in-flight Transcribe RPCs; used by least_connections routing

	// Engine metadata populated by FetchCapabilities at startup.
	// Empty strings/zero values when GetCapabilities fails or has not been called.
	engineName            string                          // e.g. "mlx_whisper", "faster_whisper"
	modelSize             string                          // e.g. "large-v3-turbo"
	device                string                          // e.g. "mps", "cuda", "cpu"
	streamingMode         inferencepb.StreamingMode       // BATCH_ONLY or NATIVE
	endpointingCapability inferencepb.EndpointingCapability // NONE, DETECTION, or AUTO_FINALIZE
}

// NewInferenceClient creates an InferenceClient backed by the given endpoint.
func NewInferenceClient(ep *Endpoint) *InferenceClient {
	return &InferenceClient{
		endpoint: ep,
		stub:     ep.InferencePluginClient(),
	}
}

// FetchCapabilities calls GetCapabilities on the plugin and caches engine metadata.
// Called once after endpoint registration; errors are non-fatal — the endpoint
// remains usable but engine info fields will be empty strings.
func (c *InferenceClient) FetchCapabilities(ctx context.Context) error {
	resp, err := c.stub.GetCapabilities(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("inference endpoint %s: GetCapabilities: %w", c.endpoint.ID(), err)
	}
	c.engineName = resp.GetEngineName()
	c.modelSize = resp.GetModelSize()
	c.device = resp.GetDevice()
	c.streamingMode = resp.GetStreamingMode()
	c.endpointingCapability = resp.GetEndpointingCapability()
	return nil
}

// EngineName returns the engine name reported by GetCapabilities (e.g. "mlx_whisper").
// Empty when FetchCapabilities has not been called or failed.
func (c *InferenceClient) EngineName() string { return c.engineName }

// ModelSize returns the model size reported by GetCapabilities (e.g. "large-v3-turbo").
// Empty when FetchCapabilities has not been called or failed.
func (c *InferenceClient) ModelSize() string { return c.modelSize }

// Device returns the compute device reported by GetCapabilities (e.g. "mps", "cuda", "cpu").
// Empty when FetchCapabilities has not been called or failed.
func (c *InferenceClient) Device() string { return c.device }

// StreamingMode returns the streaming mode reported by GetCapabilities.
// Returns STREAMING_MODE_UNSPECIFIED when FetchCapabilities has not been called or failed.
func (c *InferenceClient) StreamingMode() inferencepb.StreamingMode { return c.streamingMode }

// EndpointingCapability returns the endpointing capability reported by GetCapabilities.
// Returns ENDPOINTING_CAPABILITY_UNSPECIFIED when FetchCapabilities has not been called or failed.
func (c *InferenceClient) EndpointingCapability() inferencepb.EndpointingCapability {
	return c.endpointingCapability
}

// Capabilities returns the full InferenceCapabilities struct populated by FetchCapabilities.
// Returns a zero-value struct if FetchCapabilities has not been called or failed.
func (c *InferenceClient) Capabilities() *inferencepb.InferenceCapabilities {
	return &inferencepb.InferenceCapabilities{
		EngineName:             c.engineName,
		ModelSize:              c.modelSize,
		Device:                 c.device,
		StreamingMode:          c.streamingMode,
		EndpointingCapability:  c.endpointingCapability,
	}
}

// Inflight returns the number of Transcribe RPCs currently in progress on this client.
// Used by the least_connections routing mode.
func (c *InferenceClient) Inflight() int64 { return c.inflight.Load() }

// Transcribe sends an audio segment to the inference plugin for decoding.
// It returns an error if the endpoint is unhealthy or the RPC fails.
func (c *InferenceClient) Transcribe(
	ctx context.Context,
	req *inferencepb.TranscribeRequest,
) (*inferencepb.TranscribeResponse, error) {
	if !c.endpoint.IsHealthy() {
		return nil, fmt.Errorf("inference endpoint %s: circuit open", c.endpoint.ID())
	}
	c.inflight.Add(1)
	defer c.inflight.Add(-1)
	resp, err := c.stub.Transcribe(ctx, req)
	if err != nil {
		c.endpoint.RecordFailure()
		return nil, fmt.Errorf("inference endpoint %s: %w", c.endpoint.ID(), err)
	}
	c.endpoint.RecordSuccess()
	return resp, nil
}

// HealthCheck probes the plugin's current state.
func (c *InferenceClient) HealthCheck(ctx context.Context) (*commonpb.PluginHealthStatus, error) {
	status, err := c.stub.HealthCheck(ctx, &emptypb.Empty{})
	if err != nil {
		c.endpoint.RecordFailure()
		return nil, fmt.Errorf("inference endpoint %s: health check: %w", c.endpoint.ID(), err)
	}
	return status, nil
}

// Endpoint returns the underlying Endpoint for inspection.
func (c *InferenceClient) Endpoint() *Endpoint { return c.endpoint }

// HealthCheckProbe calls the HealthCheck RPC and returns (id, state, cbHealthy, err).
// Implements runtime.inferenceHealthProber.
func (c *InferenceClient) HealthCheckProbe(
	ctx context.Context,
) (string, commonpb.PluginState, bool, error) {
	status, err := c.HealthCheck(ctx)
	if err != nil {
		return c.endpoint.ID(), commonpb.PluginState_PLUGIN_STATE_UNSPECIFIED,
			c.endpoint.IsHealthy(), err
	}
	return c.endpoint.ID(), status.GetState(), c.endpoint.IsHealthy(), nil
}
