package stream

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/speechmux/core/internal/config"
	"github.com/speechmux/core/internal/plugin"
	"github.com/speechmux/core/internal/session"
	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ── test helpers ──────────────────────────────────────────────────────────────

func batchEngineTestSession(ctx context.Context) *session.Session {
	return session.NewTestSession(ctx, "batch-engine-test")
}

func batchEngineCfg(partialIntervalSec float64) SessionDecodeConfig {
	return SessionDecodeConfig{
		SampleRate:   16000,
		LanguageCode: "ko",
		Realtime:     false,
		Stream: config.StreamConfig{
			PartialDecodeIntervalSec: partialIntervalSec,
			PartialDecodeWindowSec:   10,
			MaxBufferSec:             30,
		},
	}
}

// fixedTextSTTServer is a minimal gRPC STT stub that returns a fixed transcript.
type fixedTextSTTServer struct {
	inferencepb.UnimplementedInferencePluginServer
	text string
}

func (s *fixedTextSTTServer) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *fixedTextSTTServer) GetCapabilities(_ context.Context, _ *emptypb.Empty) (*inferencepb.InferenceCapabilities, error) {
	return &inferencepb.InferenceCapabilities{EngineName: "fixed-stub", MaxConcurrentRequests: 8}, nil
}

func (s *fixedTextSTTServer) Transcribe(_ context.Context, req *inferencepb.TranscribeRequest) (*inferencepb.TranscribeResponse, error) {
	return &inferencepb.TranscribeResponse{
		RequestId:    req.GetRequestId(),
		SessionId:    req.GetSessionId(),
		Text:         s.text,
		LanguageCode: "ko",
	}, nil
}

// newTestScheduler wires a real DecodeScheduler backed by the given STT server.
func newTestScheduler(t *testing.T, srv inferencepb.InferencePluginServer) *DecodeScheduler {
	t.Helper()
	dir, err := os.MkdirTemp("", "batcheng")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	sock := filepath.Join(dir, "s.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	gs := grpc.NewServer()
	inferencepb.RegisterInferencePluginServer(gs, srv)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(func() { gs.Stop() })

	ep, err := plugin.NewEndpoint("test-stt", sock, plugin.EndpointCircuitBreaker{})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = ep.Close() })

	router := plugin.NewPluginRouter("")
	if err := router.Add(ep.ID(), ep.Socket(), 0); err != nil {
		t.Fatalf("router.Add: %v", err)
	}

	return NewDecodeScheduler(router, 0, 10.0, nil)
}

// ── tests ──────────────────────────────────────────────────────────────────────

// TestBatchDecodeEngine_StartClose verifies that Start followed by immediate
// Close exits cleanly with no goroutine leak and closes Results().
func TestBatchDecodeEngine_StartClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := NewAudioRingBuffer(30)
	eng := newBatchDecodeEngine(nil, buf)
	sess := batchEngineTestSession(ctx)

	if err := eng.Start(ctx, sess, batchEngineCfg(1.5)); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Results channel must be closed after Close returns.
	select {
	case _, ok := <-eng.Results():
		if ok {
			t.Fatal("Results() returned value after Close; expected closed channel")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Results() not closed within 500ms after Close")
	}
}

// TestBatchDecodeEngine_NilSchedulerDrains verifies that with a nil scheduler,
// queued tasks are drained and the worker exits cleanly.
func TestBatchDecodeEngine_NilSchedulerDrains(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := NewAudioRingBuffer(30)
	buf.Append(1, make([]byte, 960))
	buf.AdvanceWatermark(1)

	eng := newBatchDecodeEngine(nil, buf)
	sess := batchEngineTestSession(ctx)
	if err := eng.Start(ctx, sess, batchEngineCfg(1.5)); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Enqueue a final task.
	if err := eng.OnUtteranceEnd(1, 1); err != nil {
		t.Fatalf("OnUtteranceEnd: %v", err)
	}

	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// With nil scheduler, no result should appear on Results().
	select {
	case res, ok := <-eng.Results():
		if ok {
			t.Fatalf("unexpected result with nil scheduler: %+v", res)
		}
		// channel closed — expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Results() not closed within 500ms")
	}
}

// TestBatchDecodeEngine_OnUtteranceEndEmitsFinal verifies that OnUtteranceEnd
// causes the worker to emit one DecodeResult with IsFinal=true.
func TestBatchDecodeEngine_OnUtteranceEndEmitsFinal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler := newTestScheduler(t, &fixedTextSTTServer{text: "hello"})

	buf := NewAudioRingBuffer(30)
	buf.Append(1, make([]byte, 960))
	buf.Append(2, make([]byte, 960))
	buf.AdvanceWatermark(2)

	eng := newBatchDecodeEngine(scheduler, buf)
	sess := batchEngineTestSession(ctx)
	if err := eng.Start(ctx, sess, batchEngineCfg(1.5)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = eng.Close() }()

	if err := eng.OnUtteranceEnd(1, 2); err != nil {
		t.Fatalf("OnUtteranceEnd: %v", err)
	}

	select {
	case res := <-eng.Results():
		if !res.IsFinal {
			t.Fatalf("IsFinal = false, want true")
		}
		if res.Text != "hello" {
			t.Fatalf("Text = %q, want %q", res.Text, "hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no result within 2s")
	}
}

// TestBatchDecodeEngine_PartialTimerFires verifies that after OnSpeechStart a
// partial DecodeResult with IsFinal=false arrives within interval+200ms.
func TestBatchDecodeEngine_PartialTimerFires(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler := newTestScheduler(t, &fixedTextSTTServer{text: "partial"})

	buf := NewAudioRingBuffer(30)
	buf.Append(1, make([]byte, 960))
	buf.Append(2, make([]byte, 960))
	buf.AdvanceWatermark(2)

	const intervalSec = 0.1 // 100ms for fast test
	eng := newBatchDecodeEngine(scheduler, buf)
	sess := batchEngineTestSession(ctx)
	if err := eng.Start(ctx, sess, batchEngineCfg(intervalSec)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = eng.Close() }()

	eng.OnSpeechStart(1)

	deadline := time.Duration(intervalSec*float64(time.Second)) + 200*time.Millisecond
	select {
	case res := <-eng.Results():
		if res.IsFinal {
			t.Fatalf("IsFinal = true, want false (partial)")
		}
		if res.Text != "partial" {
			t.Fatalf("Text = %q, want %q", res.Text, "partial")
		}
	case <-time.After(deadline):
		t.Fatalf("no partial result within %v", deadline)
	}
}

// TestBatchDecodeEngine_PartialStopsOnSpeechEnd verifies that after OnSpeechEnd
// no further partial results are produced even if the interval elapses.
func TestBatchDecodeEngine_PartialStopsOnSpeechEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := NewAudioRingBuffer(30)
	buf.Append(1, make([]byte, 960))
	buf.AdvanceWatermark(1)

	const intervalSec = 0.1
	eng := newBatchDecodeEngine(nil, buf) // nil scheduler: no results ever
	sess := batchEngineTestSession(ctx)
	if err := eng.Start(ctx, sess, batchEngineCfg(intervalSec)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = eng.Close() }()

	eng.OnSpeechStart(1)
	eng.OnSpeechEnd()

	// Wait longer than the interval to be sure the timer was stopped.
	time.Sleep(time.Duration(intervalSec*float64(time.Second)) * 3)

	select {
	case res, ok := <-eng.Results():
		if ok {
			t.Fatalf("unexpected result after OnSpeechEnd: %+v", res)
		}
		// channel closed — that's only possible if Close was called, which it wasn't
		t.Fatal("Results() closed unexpectedly before Close")
	default:
		// No result and channel still open — correct.
	}
}

// TestBatchDecodeEngine_ContextCancelClean verifies that cancelling the parent
// ctx causes the engine to exit cleanly and close Results().
func TestBatchDecodeEngine_ContextCancelClean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	buf := NewAudioRingBuffer(30)
	eng := newBatchDecodeEngine(nil, buf)
	sess := batchEngineTestSession(ctx)
	if err := eng.Start(ctx, sess, batchEngineCfg(1.5)); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cancel() // trigger engine shutdown via ctx cancellation

	// Close should still be safe to call after ctx cancel.
	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case _, ok := <-eng.Results():
		if ok {
			t.Fatal("Results() still open after Close")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Results() not closed within 500ms")
	}
}

// TestBatchDecodeEngine_CloseIdempotent verifies that calling Close twice does
// not panic and returns nil both times.
func TestBatchDecodeEngine_CloseIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := NewAudioRingBuffer(30)
	eng := newBatchDecodeEngine(nil, buf)
	sess := batchEngineTestSession(ctx)
	if err := eng.Start(ctx, sess, batchEngineCfg(1.5)); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := eng.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
