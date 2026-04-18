package plugin

import (
	"context"
	"fmt"
	"sync/atomic"

	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
	"google.golang.org/grpc"
)

// InferenceStreamClient manages a single per-session TranscribeStream bidi.
// Core opens one InferenceStreamClient per streaming session; the client owns
// the stream for the session's entire lifetime.
type InferenceStreamClient struct {
	endpoint *Endpoint
	stream   grpc.BidiStreamingClient[inferencepb.StreamRequest, inferencepb.StreamResponse]
	ctx      context.Context
	cancel   context.CancelFunc
	seqNum   atomic.Uint64
}

// NewInferenceStreamClient opens a TranscribeStream on ep and sends cfg as the
// first message. On failure: cancel() + ep.RecordFailure() + wrapped error.
func NewInferenceStreamClient(
	ctx context.Context,
	ep *Endpoint,
	cfg *inferencepb.StreamStartConfig,
) (*InferenceStreamClient, error) {
	sCtx, cancel := context.WithCancel(ctx)

	stream, err := ep.InferencePluginClient().TranscribeStream(sCtx)
	if err != nil {
		cancel()
		ep.RecordFailure()
		return nil, fmt.Errorf("open TranscribeStream: %w", err)
	}

	if err := stream.Send(&inferencepb.StreamRequest{
		Payload: &inferencepb.StreamRequest_Start{Start: cfg},
	}); err != nil {
		cancel()
		ep.RecordFailure()
		return nil, fmt.Errorf("send StreamStartConfig: %w", err)
	}

	ep.RecordSuccess()
	return &InferenceStreamClient{
		endpoint: ep,
		stream:   stream,
		ctx:      sCtx,
		cancel:   cancel,
	}, nil
}

// SendAudio atomically increments the sequence counter and sends an AudioChunk.
func (c *InferenceStreamClient) SendAudio(pcm []byte) (uint64, error) {
	seq := c.seqNum.Add(1)
	err := c.stream.Send(&inferencepb.StreamRequest{
		Payload: &inferencepb.StreamRequest_Audio{
			Audio: &inferencepb.AudioChunk{
				SequenceNumber: seq,
				AudioData:      pcm,
			},
		},
	})
	if err != nil {
		c.endpoint.RecordFailure()
		return 0, fmt.Errorf("stream send audio seq=%d: %w", seq, err)
	}
	return seq, nil
}

// SendFinalize signals the engine to emit a final hypothesis for the current utterance.
func (c *InferenceStreamClient) SendFinalize() error {
	return c.sendControl(inferencepb.StreamControl_KIND_FINALIZE_UTTERANCE)
}

// SendCancel aborts the current decoding context on the engine side.
func (c *InferenceStreamClient) SendCancel() error {
	return c.sendControl(inferencepb.StreamControl_KIND_CANCEL)
}

func (c *InferenceStreamClient) sendControl(kind inferencepb.StreamControl_Kind) error {
	err := c.stream.Send(&inferencepb.StreamRequest{
		Payload: &inferencepb.StreamRequest_Control{
			Control: &inferencepb.StreamControl{Kind: kind},
		},
	})
	if err != nil {
		c.endpoint.RecordFailure()
		return fmt.Errorf("stream send control %v: %w", kind, err)
	}
	return nil
}

// Recv reads the next StreamResponse. Records endpoint health on each call.
func (c *InferenceStreamClient) Recv() (*inferencepb.StreamResponse, error) {
	resp, err := c.stream.Recv()
	if err != nil {
		c.endpoint.RecordFailure()
		return nil, err
	}
	c.endpoint.RecordSuccess()
	return resp, nil
}

// Close half-closes the send side of the stream. Does NOT cancel ctx — context
// cancellation is owned by the parent session so recv can drain in-flight responses.
func (c *InferenceStreamClient) Close() {
	_ = c.stream.CloseSend()
}
