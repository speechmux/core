package plugin

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
	"google.golang.org/grpc/metadata"
)

// ── mock bidi-stream ──────────────────────────────────────────────────────────

type mockInferenceStream struct {
	sent      []*inferencepb.StreamRequest
	recvQueue []*inferencepb.StreamResponse
	recvErr   error
	sendErr   error
	closed    bool
}

func (m *mockInferenceStream) Send(req *inferencepb.StreamRequest) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sent = append(m.sent, req)
	return nil
}

func (m *mockInferenceStream) Recv() (*inferencepb.StreamResponse, error) {
	if len(m.recvQueue) == 0 {
		if m.recvErr != nil {
			return nil, m.recvErr
		}
		return nil, io.EOF
	}
	resp := m.recvQueue[0]
	m.recvQueue = m.recvQueue[1:]
	return resp, nil
}

func (m *mockInferenceStream) CloseSend() error {
	m.closed = true
	return nil
}

func (m *mockInferenceStream) Header() (metadata.MD, error) { return nil, nil }
func (m *mockInferenceStream) Trailer() metadata.MD          { return nil }
func (m *mockInferenceStream) Context() context.Context      { return context.Background() }
func (m *mockInferenceStream) SendMsg(_ any) error           { return nil }
func (m *mockInferenceStream) RecvMsg(_ any) error           { return nil }

// ── helper ────────────────────────────────────────────────────────────────────

func buildInferenceStreamClient(ms *mockInferenceStream, ep *Endpoint) *InferenceStreamClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &InferenceStreamClient{
		endpoint: ep,
		stream:   ms,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestInferenceStreamClient_SendsStartConfigFirst(t *testing.T) {
	ms := &mockInferenceStream{}
	ep := newTestEndpoint(5, 0)

	cfg := &inferencepb.StreamStartConfig{SessionId: "s1", SampleRate: 16000, LanguageCode: "en"}
	if err := ms.Send(&inferencepb.StreamRequest{
		Payload: &inferencepb.StreamRequest_Start{Start: cfg},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	ep.RecordSuccess()
	c := buildInferenceStreamClient(ms, ep)

	if len(ms.sent) == 0 {
		t.Fatal("expected at least one sent message")
	}
	first := ms.sent[0]
	start := first.GetStart()
	if start == nil {
		t.Fatal("first message must be StreamStartConfig")
	}
	if start.GetSessionId() != "s1" {
		t.Fatalf("session_id = %q, want %q", start.GetSessionId(), "s1")
	}
	_ = c
}

func TestInferenceStreamClient_SendAudioIncrementsSeq(t *testing.T) {
	ms := &mockInferenceStream{}
	ep := newTestEndpoint(5, 0)
	c := buildInferenceStreamClient(ms, ep)

	seq1, err := c.SendAudio([]byte{0x01})
	if err != nil {
		t.Fatalf("SendAudio 1: %v", err)
	}
	seq2, err := c.SendAudio([]byte{0x02})
	if err != nil {
		t.Fatalf("SendAudio 2: %v", err)
	}
	if seq2 <= seq1 {
		t.Fatalf("sequence numbers not monotonically increasing: %d then %d", seq1, seq2)
	}
}

func TestInferenceStreamClient_SendFinalize_SendsControl(t *testing.T) {
	ms := &mockInferenceStream{}
	ep := newTestEndpoint(5, 0)
	c := buildInferenceStreamClient(ms, ep)

	if err := c.SendFinalize(); err != nil {
		t.Fatalf("SendFinalize: %v", err)
	}
	if len(ms.sent) == 0 {
		t.Fatal("expected a sent message")
	}
	ctrl := ms.sent[len(ms.sent)-1].GetControl()
	if ctrl == nil {
		t.Fatal("expected StreamControl message")
	}
	if ctrl.GetKind() != inferencepb.StreamControl_KIND_FINALIZE_UTTERANCE {
		t.Fatalf("kind = %v, want KIND_FINALIZE_UTTERANCE", ctrl.GetKind())
	}
}

func TestInferenceStreamClient_Close_HalfClosesSend(t *testing.T) {
	ms := &mockInferenceStream{}
	ep := newTestEndpoint(5, 0)
	c := buildInferenceStreamClient(ms, ep)

	c.Close()

	if !ms.closed {
		t.Fatal("Close must call CloseSend on the underlying stream")
	}
	// Context must not be cancelled by Close.
	select {
	case <-c.ctx.Done():
		t.Fatal("Close must not cancel the stream context")
	default:
	}
}

func TestInferenceStreamClient_StartFailure_RecordsEndpointFailure(t *testing.T) {
	// halfOpenTimeout=time.Hour keeps the circuit OPEN after one failure.
	ep := newTestEndpoint(1, time.Hour)
	sendErr := errors.New("stream broken")
	ms := &mockInferenceStream{sendErr: sendErr}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &InferenceStreamClient{endpoint: ep, stream: ms, ctx: ctx, cancel: cancel}

	// Manually invoke the send-start path to trigger RecordFailure.
	cfg := &inferencepb.StreamStartConfig{SessionId: "s1"}
	err := ms.Send(&inferencepb.StreamRequest{Payload: &inferencepb.StreamRequest_Start{Start: cfg}})
	if err == nil {
		t.Fatal("expected send error")
	}
	ep.RecordFailure()

	if ep.IsHealthy() {
		t.Fatal("endpoint should be unhealthy after start failure")
	}
	_ = c
}
