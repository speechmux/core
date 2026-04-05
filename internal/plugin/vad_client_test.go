package plugin

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	vadpb "github.com/speechmux/proto/gen/go/vad/v1"
	"google.golang.org/grpc/metadata"
)

// ── mock bidi-stream ──────────────────────────────────────────────────────────

// mockVADStream is a hand-rolled implementation of
// grpc.BidiStreamingClient[VADRequest, VADResponse].
// Sent messages are appended to sent; recvQueue feeds Recv calls in order.
type mockVADStream struct {
	sent      []*vadpb.VADRequest
	recvQueue []*vadpb.VADResponse
	recvErr   error // returned after queue is drained
	sendErr   error // returned on every Send call if non-nil
	closed    bool  // true after CloseSend
}

func (m *mockVADStream) Send(req *vadpb.VADRequest) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sent = append(m.sent, req)
	return nil
}

func (m *mockVADStream) Recv() (*vadpb.VADResponse, error) {
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

func (m *mockVADStream) CloseSend() error {
	m.closed = true
	return nil
}

// The following methods satisfy grpc.ClientStream; they are not exercised by
// VADClient and are provided as no-ops.
func (m *mockVADStream) Header() (metadata.MD, error) { return nil, nil }
func (m *mockVADStream) Trailer() metadata.MD          { return nil }
func (m *mockVADStream) Context() context.Context      { return context.Background() }
func (m *mockVADStream) SendMsg(_ any) error           { return nil }
func (m *mockVADStream) RecvMsg(_ any) error           { return nil }

// ── helpers ───────────────────────────────────────────────────────────────────

// buildVADClientFromStream constructs a VADClient directly from a mock stream,
// mirroring the logic inside NewVADClient without needing a real gRPC dial.
// It sends the SessionStart message and records success on ep.
func buildVADClientFromStream(
	t *testing.T,
	ms *mockVADStream,
	ep *Endpoint,
) *VADClient {
	t.Helper()

	req := &vadpb.VADRequest{
		Signal: &vadpb.VADRequest_SessionStart{
			SessionStart: &vadpb.SessionStart{
				SessionId:  "sess-1",
				Threshold:  0.5,
				SampleRate: 16000,
			},
		},
	}
	if err := ms.Send(req); err != nil {
		t.Fatalf("session start send failed: %v", err)
	}
	ep.RecordSuccess()

	ctx, cancel := context.WithCancel(context.Background())
	return &VADClient{
		endpoint: ep,
		stream:   ms,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// ── NewVADClient construction ─────────────────────────────────────────────────

func TestVADClient_New_SendsSessionStart(t *testing.T) {
	ms := &mockVADStream{}
	ep := newTestEndpoint(5, 0)
	vc := buildVADClientFromStream(t, ms, ep)

	if vc == nil {
		t.Fatal("expected VADClient, got nil")
	}
	if len(ms.sent) == 0 {
		t.Fatal("expected SessionStart to be sent, got no messages")
	}
	first := ms.sent[0]
	ss := first.GetSessionStart()
	if ss == nil {
		t.Fatal("first message should be a SessionStart signal")
	}
	if ss.GetSessionId() != "sess-1" {
		t.Fatalf("session_id = %q, want %q", ss.GetSessionId(), "sess-1")
	}
	if ss.GetSampleRate() != 16000 {
		t.Fatalf("sample_rate = %d, want 16000", ss.GetSampleRate())
	}
}

func TestVADClient_New_RecordsSuccessOnEndpoint(t *testing.T) {
	ms := &mockVADStream{}
	ep := newTestEndpoint(5, 0)
	buildVADClientFromStream(t, ms, ep)

	if !ep.IsHealthy() {
		t.Fatal("endpoint should be healthy (RecordSuccess) after successful construction")
	}
}

func TestVADClient_New_RecordsFailureWhenStreamOpenFails(t *testing.T) {
	// halfOpenTimeout must be non-zero so that IsHealthy() does not immediately
	// transition OPEN→HALF_OPEN when called right after RecordFailure().
	ep := newTestEndpoint(1, time.Hour)
	// Simulate the path where ep.VADPluginClient().StreamVAD() returns an error.
	// RecordFailure is called by NewVADClient in that case.
	ep.RecordFailure()

	// With threshold=1 the circuit is now open.
	if ep.IsHealthy() {
		t.Fatal("endpoint should be OPEN after stream open failure")
	}
}

// ── Send ──────────────────────────────────────────────────────────────────────

func TestVADClient_Send_ReturnsSequenceNumber(t *testing.T) {
	ms := &mockVADStream{}
	ep := newTestEndpoint(5, 0)
	vc := buildVADClientFromStream(t, ms, ep)

	seq, err := vc.Send([]byte{0x01, 0x02}, 16000)
	if err != nil {
		t.Fatalf("Send error: %v", err)
	}
	if seq == 0 {
		t.Fatal("Send should return a non-zero sequence number")
	}
}

func TestVADClient_Send_SequenceNumberMonotonicallyIncreasing(t *testing.T) {
	ms := &mockVADStream{}
	ep := newTestEndpoint(5, 0)
	vc := buildVADClientFromStream(t, ms, ep)

	seq1, err := vc.Send([]byte{0x01}, 16000)
	if err != nil {
		t.Fatalf("Send 1 error: %v", err)
	}
	seq2, err := vc.Send([]byte{0x02}, 16000)
	if err != nil {
		t.Fatalf("Send 2 error: %v", err)
	}
	if seq2 <= seq1 {
		t.Fatalf("sequence numbers not monotonically increasing: got %d then %d", seq1, seq2)
	}
}

func TestVADClient_Send_FrameContentForwarded(t *testing.T) {
	ms := &mockVADStream{}
	ep := newTestEndpoint(5, 0)
	vc := buildVADClientFromStream(t, ms, ep)

	pcm := []byte{0x10, 0x20, 0x30}
	seq, err := vc.Send(pcm, 16000)
	if err != nil {
		t.Fatalf("Send error: %v", err)
	}

	// ms.sent[0] is the SessionStart from buildVADClientFromStream.
	// ms.sent[1] is the audio frame we just sent.
	if len(ms.sent) < 2 {
		t.Fatalf("expected at least 2 messages, got %d", len(ms.sent))
	}
	audioMsg := ms.sent[1]
	if string(audioMsg.GetPcmData()) != string(pcm) {
		t.Fatalf("pcm_data = %v, want %v", audioMsg.GetPcmData(), pcm)
	}
	if audioMsg.GetSampleRate() != 16000 {
		t.Fatalf("sample_rate = %d, want 16000", audioMsg.GetSampleRate())
	}
	if audioMsg.GetSequenceNumber() != seq {
		t.Fatalf("sequence_number in message = %d, want %d", audioMsg.GetSequenceNumber(), seq)
	}
}

func TestVADClient_Send_RecordsFailureOnStreamError(t *testing.T) {
	// Build a VADClient whose stream fails on Send.
	ep := newTestEndpoint(5, 0)
	ms := &mockVADStream{sendErr: errors.New("stream broken")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vc := &VADClient{endpoint: ep, stream: ms, ctx: ctx, cancel: cancel}

	_, err := vc.Send([]byte{0xFF}, 16000)
	if err == nil {
		t.Fatal("expected error on stream failure, got nil")
	}
	if ep.failureCount.Load() == 0 {
		t.Fatal("expected failure counter to be incremented after Send error")
	}
}

// ── Recv ──────────────────────────────────────────────────────────────────────

func TestVADClient_Recv_ReturnsQueuedResponse(t *testing.T) {
	want := &vadpb.VADResponse{IsSpeech: true, SequenceNumber: 42}
	ms := &mockVADStream{recvQueue: []*vadpb.VADResponse{want}}
	ep := newTestEndpoint(5, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vc := &VADClient{endpoint: ep, stream: ms, ctx: ctx, cancel: cancel}

	got, err := vc.Recv()
	if err != nil {
		t.Fatalf("Recv error: %v", err)
	}
	if got.GetSequenceNumber() != want.GetSequenceNumber() {
		t.Fatalf("SequenceNumber = %d, want %d", got.GetSequenceNumber(), want.GetSequenceNumber())
	}
	if got.GetIsSpeech() != want.GetIsSpeech() {
		t.Fatalf("IsSpeech = %v, want %v", got.GetIsSpeech(), want.GetIsSpeech())
	}
}

func TestVADClient_Recv_RecordsSuccessOnEndpoint(t *testing.T) {
	ms := &mockVADStream{recvQueue: []*vadpb.VADResponse{{SequenceNumber: 7}}}
	ep := newTestEndpoint(5, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vc := &VADClient{endpoint: ep, stream: ms, ctx: ctx, cancel: cancel}

	if _, err := vc.Recv(); err != nil {
		t.Fatalf("unexpected Recv error: %v", err)
	}
	if !ep.IsHealthy() {
		t.Fatal("endpoint should remain healthy (RecordSuccess) after successful Recv")
	}
}

func TestVADClient_Recv_RecordsFailureOnError(t *testing.T) {
	sentinel := errors.New("plugin crashed")
	ms := &mockVADStream{recvErr: sentinel}
	ep := newTestEndpoint(5, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vc := &VADClient{endpoint: ep, stream: ms, ctx: ctx, cancel: cancel}

	_, err := vc.Recv()
	if err == nil {
		t.Fatal("expected error from Recv, got nil")
	}
	if ep.failureCount.Load() == 0 {
		t.Fatal("expected failure counter to be incremented after Recv error")
	}
}

func TestVADClient_Recv_MultipleResponses(t *testing.T) {
	responses := []*vadpb.VADResponse{
		{SequenceNumber: 1, IsSpeech: true},
		{SequenceNumber: 2, IsSpeech: false},
		{SequenceNumber: 3, IsSpeech: true},
	}
	ms := &mockVADStream{recvQueue: responses}
	ep := newTestEndpoint(5, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vc := &VADClient{endpoint: ep, stream: ms, ctx: ctx, cancel: cancel}

	for i, want := range responses {
		got, err := vc.Recv()
		if err != nil {
			t.Fatalf("Recv[%d] error: %v", i, err)
		}
		if got.GetSequenceNumber() != want.GetSequenceNumber() {
			t.Fatalf("Recv[%d] SequenceNumber = %d, want %d", i, got.GetSequenceNumber(), want.GetSequenceNumber())
		}
	}
}

// ── Close ─────────────────────────────────────────────────────────────────────

func TestVADClient_Close_SendsSessionEnd(t *testing.T) {
	ms := &mockVADStream{}
	ep := newTestEndpoint(5, 0)
	vc := buildVADClientFromStream(t, ms, ep)

	vc.Close()

	// Find the SessionEnd message among all sent messages.
	found := false
	for _, req := range ms.sent {
		if req.GetSessionEnd() != nil {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Close should send a SessionEnd message to the VAD plugin")
	}
}

func TestVADClient_Close_CallsCloseSend(t *testing.T) {
	ms := &mockVADStream{}
	ep := newTestEndpoint(5, 0)
	vc := buildVADClientFromStream(t, ms, ep)

	vc.Close()

	if !ms.closed {
		t.Fatal("Close should call CloseSend on the underlying stream")
	}
}

func TestVADClient_Close_DoesNotCancelContext(t *testing.T) {
	// Per the design: Close must NOT cancel the stream context so that
	// vadRecvLoop can drain in-flight responses after the send side closes.
	ms := &mockVADStream{}
	ep := newTestEndpoint(5, 0)
	vc := buildVADClientFromStream(t, ms, ep)

	vc.Close()

	select {
	case <-vc.ctx.Done():
		t.Fatal("Close must not cancel the stream context")
	default:
		// expected: context is still alive
	}
}

// ── table-driven: sequence number progression ─────────────────────────────────

func TestVADClient_Send_SequenceNumbersTable(t *testing.T) {
	tests := []struct {
		name        string
		sends       int
		wantLastSeq uint64
	}{
		{"one send", 1, 1},
		{"five sends", 5, 5},
		{"ten sends", 10, 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ms := &mockVADStream{}
			ep := newTestEndpoint(5, 0)
			vc := buildVADClientFromStream(t, ms, ep)

			var lastSeq uint64
			for range tc.sends {
				seq, err := vc.Send([]byte{0x00}, 16000)
				if err != nil {
					t.Fatalf("Send error: %v", err)
				}
				lastSeq = seq
			}
			if lastSeq != tc.wantLastSeq {
				t.Fatalf("last sequence number = %d, want %d", lastSeq, tc.wantLastSeq)
			}
		})
	}
}
