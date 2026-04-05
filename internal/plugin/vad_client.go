package plugin

import (
	"context"
	"fmt"
	"sync/atomic"

	vadpb "github.com/speechmux/proto/gen/go/vad/v1"
)

// VADClient manages a single per-session StreamVAD bidi-stream.
// Core opens one VADClient per session; the client owns the stream for the
// session's entire lifetime.
type VADClient struct {
	endpoint *Endpoint
	stream   vadpb.VADPlugin_StreamVADClient
	ctx      context.Context
	cancel   context.CancelFunc
	seqNum   atomic.Uint64
}

// NewVADClient opens a StreamVAD stream on ep and sends the SessionStart message.
// sampleRate should match the PCM data that will be sent via Send.
func NewVADClient(
	ctx context.Context,
	ep *Endpoint,
	sessionID string,
	threshold float64,
	sampleRate int32,
) (*VADClient, error) {
	sCtx, cancel := context.WithCancel(ctx)

	stream, err := ep.VADPluginClient().StreamVAD(sCtx)
	if err != nil {
		cancel()
		ep.RecordFailure()
		return nil, fmt.Errorf("open StreamVAD: %w", err)
	}

	if err := stream.Send(&vadpb.VADRequest{
		Signal: &vadpb.VADRequest_SessionStart{
			SessionStart: &vadpb.SessionStart{
				SessionId:  sessionID,
				Threshold:  threshold,
				SampleRate: sampleRate,
			},
		},
	}); err != nil {
		cancel()
		ep.RecordFailure()
		return nil, fmt.Errorf("send SessionStart: %w", err)
	}

	ep.RecordSuccess()
	return &VADClient{
		endpoint: ep,
		stream:   stream,
		ctx:      sCtx,
		cancel:   cancel,
	}, nil
}

// Send transmits a PCM audio frame to the VAD plugin.
// Returns the sequence number assigned to this frame.
func (v *VADClient) Send(pcm []byte, sampleRate int32) (uint64, error) {
	seq := v.seqNum.Add(1)
	err := v.stream.Send(&vadpb.VADRequest{
		PcmData:        pcm,
		SampleRate:     sampleRate,
		SequenceNumber: seq,
	})
	if err != nil {
		v.endpoint.RecordFailure()
		return 0, fmt.Errorf("VAD send seq=%d: %w", seq, err)
	}
	return seq, nil
}

// Recv reads the next VAD response. Blocks until a response is available or
// the stream ends.
func (v *VADClient) Recv() (*vadpb.VADResponse, error) {
	resp, err := v.stream.Recv()
	if err != nil {
		v.endpoint.RecordFailure()
		return nil, err
	}
	v.endpoint.RecordSuccess()
	return resp, nil
}

// Close sends SessionEnd and half-closes the send side of the stream.
// It does NOT cancel the stream context so that vadRecvLoop can drain any
// responses already in flight from the VAD plugin. The context is cancelled
// by the parent session context when the session ends.
func (v *VADClient) Close() {
	_ = v.stream.Send(&vadpb.VADRequest{
		Signal: &vadpb.VADRequest_SessionEnd{
			SessionEnd: &vadpb.SessionEnd{},
		},
	})
	_ = v.stream.CloseSend()
}

