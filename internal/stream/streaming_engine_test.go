package stream

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/speechmux/core/internal/config"
	sttErrors "github.com/speechmux/core/internal/errors"
	"github.com/speechmux/core/internal/plugin"
	"github.com/speechmux/core/internal/session"
	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ── stub server helpers ───────────────────────────────────────────────────────

func streamingTempSockDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "seng")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// startStubServer starts a gRPC server with the given servicer and returns an Endpoint.
func startStubServer(t *testing.T, srv inferencepb.InferencePluginServer) *plugin.Endpoint {
	t.Helper()
	sock := filepath.Join(streamingTempSockDir(t), "se.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	inferencepb.RegisterInferencePluginServer(gs, srv)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(func() { gs.Stop() })
	ep, err := plugin.NewEndpoint("stub", sock, plugin.EndpointCircuitBreaker{})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = ep.Close() })
	return ep
}

// buildEngine creates a streamingDecodeEngine backed by a real stub gRPC server.
func buildEngine(t *testing.T, ep *plugin.Endpoint, src endpointingSource, cfg config.StreamConfig) *streamingDecodeEngine {
	t.Helper()
	ctx := context.Background()
	startCfg := &inferencepb.StreamStartConfig{SessionId: "test-sess", SampleRate: 16000}
	client, err := plugin.NewInferenceStreamClient(ctx, ep, startCfg)
	if err != nil {
		t.Fatalf("NewInferenceStreamClient: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return newStreamingDecodeEngine(client, src, cfg)
}

func startEngine(t *testing.T, e *streamingDecodeEngine) {
	t.Helper()
	ctx := context.Background()
	sess := session.NewTestSession(ctx, "test-sess")
	cfg := SessionDecodeConfig{SampleRate: 16000, LanguageCode: "ko", Stream: e.cfg}
	if err := e.Start(ctx, sess, cfg); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

// ── stub server implementations ───────────────────────────────────────────────

// closeImmediatelyStub closes the stream as soon as it receives any message.
type closeImmediatelyStub struct {
	inferencepb.UnimplementedInferencePluginServer
}

func (s *closeImmediatelyStub) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}
func (s *closeImmediatelyStub) TranscribeStream(srv inferencepb.InferencePlugin_TranscribeStreamServer) error {
	// Drain all requests silently, then return (server closes stream).
	for {
		_, err := srv.Recv()
		if err != nil {
			return nil
		}
	}
}

// echoAudioStub records received audio chunks then closes.
type echoAudioStub struct {
	inferencepb.UnimplementedInferencePluginServer
	received chan []byte
}

func (s *echoAudioStub) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}
func (s *echoAudioStub) TranscribeStream(srv inferencepb.InferencePlugin_TranscribeStreamServer) error {
	for {
		req, err := srv.Recv()
		if err != nil {
			return nil
		}
		if a := req.GetAudio(); a != nil {
			s.received <- a.GetAudioData()
		}
	}
}

// hypothesisStub sends one hypothesis after receiving audio then closes.
type hypothesisStub struct {
	inferencepb.UnimplementedInferencePluginServer
	text    string
	isFinal bool
}

func (s *hypothesisStub) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}
func (s *hypothesisStub) TranscribeStream(srv inferencepb.InferencePlugin_TranscribeStreamServer) error {
	for {
		req, err := srv.Recv()
		if err != nil {
			return nil
		}
		if req.GetAudio() != nil {
			_ = srv.Send(&inferencepb.StreamResponse{
				Payload: &inferencepb.StreamResponse_Hypothesis{
					Hypothesis: &inferencepb.StreamHypothesis{
						Text:    s.text,
						IsFinal: s.isFinal,
					},
				},
			})
			return nil // close after one response
		}
	}
}

// twoUtteranceStub sends partial text for utterance 1, is_final, then partial for utterance 2.
type twoUtteranceStub struct {
	inferencepb.UnimplementedInferencePluginServer
}

func (s *twoUtteranceStub) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}
func (s *twoUtteranceStub) TranscribeStream(srv inferencepb.InferencePlugin_TranscribeStreamServer) error {
	send := func(text string, isFinal bool) error {
		return srv.Send(&inferencepb.StreamResponse{
			Payload: &inferencepb.StreamResponse_Hypothesis{
				Hypothesis: &inferencepb.StreamHypothesis{Text: text, IsFinal: isFinal},
			},
		})
	}
	// Utterance 1: partial then final.
	_ = send("hello", false)
	_ = send("hello world", true)
	// Utterance 2: partial (committed should have grown).
	_ = send("foo", false)
	// Drain requests until client closes.
	for {
		_, err := srv.Recv()
		if err != nil {
			return nil
		}
	}
}

// errorResponseStub sends a StreamError response immediately.
type errorResponseStub struct {
	inferencepb.UnimplementedInferencePluginServer
}

func (s *errorResponseStub) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}
func (s *errorResponseStub) TranscribeStream(srv inferencepb.InferencePlugin_TranscribeStreamServer) error {
	_, _ = srv.Recv() // consume start config
	_ = srv.Send(&inferencepb.StreamResponse{
		Payload: &inferencepb.StreamResponse_Error{
			Error: &inferencepb.StreamError{Code: 500, Message: "engine boom"},
		},
	})
	// Keep stream open so recvLoop sees the error response, not EOF.
	for {
		_, err := srv.Recv()
		if err != nil {
			return nil
		}
	}
}

// hangingStub reads the start message then stops responding (for watchdog tests).
type hangingStub struct {
	inferencepb.UnimplementedInferencePluginServer
}

func (s *hangingStub) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}
func (s *hangingStub) TranscribeStream(srv inferencepb.InferencePlugin_TranscribeStreamServer) error {
	// Drain requests silently, never send any response.
	for {
		_, err := srv.Recv()
		if err != nil {
			return nil
		}
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestStreamingEngine_StartClose(t *testing.T) {
	ep := startStubServer(t, &closeImmediatelyStub{})
	e := buildEngine(t, ep, endpointingCore, config.StreamConfig{})
	startEngine(t, e)

	done := make(chan struct{})
	go func() {
		_ = e.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return within 3 s")
	}
}

func TestStreamingEngine_FeedFrameSendsAudio(t *testing.T) {
	received := make(chan []byte, 8)
	ep := startStubServer(t, &echoAudioStub{received: received})
	e := buildEngine(t, ep, endpointingCore, config.StreamConfig{})
	startEngine(t, e)
	defer func() { _ = e.Close() }()

	pcm := []byte{0x01, 0x02, 0x03, 0x04}
	if err := e.FeedFrame(1, pcm); err != nil {
		t.Fatalf("FeedFrame: %v", err)
	}

	select {
	case got := <-received:
		if len(got) != len(pcm) {
			t.Fatalf("received %d bytes, want %d", len(got), len(pcm))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("plugin did not receive audio within 3 s")
	}
}

func TestStreamingEngine_RecvLoopEmitsResult(t *testing.T) {
	ep := startStubServer(t, &hypothesisStub{text: "hello", isFinal: false})
	e := buildEngine(t, ep, endpointingCore, config.StreamConfig{})
	startEngine(t, e)
	defer func() { _ = e.Close() }()

	// Trigger audio so stub sends the hypothesis.
	_ = e.FeedFrame(1, []byte{0x00, 0x00})

	select {
	case res, ok := <-e.Results():
		if !ok {
			t.Fatal("Results channel closed unexpectedly")
		}
		if res.Text != "hello" {
			t.Fatalf("text = %q, want %q", res.Text, "hello")
		}
		if res.IsFinal {
			t.Fatal("expected partial result")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no result received within 3 s")
	}
}

func TestStreamingEngine_IsFinalResetsAssembler(t *testing.T) {
	ep := startStubServer(t, &twoUtteranceStub{})
	e := buildEngine(t, ep, endpointingCore, config.StreamConfig{})
	startEngine(t, e)
	defer func() { _ = e.Close() }()

	var prevCommitted string
	for i := 0; i < 3; i++ {
		select {
		case res, ok := <-e.Results():
			if !ok {
				return
			}
			// committed must never shrink.
			if len(res.CommittedText) < len(prevCommitted) {
				t.Fatalf("committed shrank: %q → %q", prevCommitted, res.CommittedText)
			}
			prevCommitted = res.CommittedText
		case <-time.After(3 * time.Second):
			t.Fatal("timeout waiting for result")
		}
	}
}

func TestStreamingEngine_ErrorResponseCancels(t *testing.T) {
	ep := startStubServer(t, &errorResponseStub{})
	e := buildEngine(t, ep, endpointingCore, config.StreamConfig{})
	startEngine(t, e)

	// resultsCh should close after the error response causes recvLoop to cancel.
	timeout := time.After(3 * time.Second)
	for {
		select {
		case _, ok := <-e.Results():
			if !ok {
				return // channel closed as expected
			}
		case <-timeout:
			t.Fatal("Results channel not closed within 3 s after StreamError response")
		}
	}
}

func TestStreamingEngine_LagWatchdog(t *testing.T) {
	ep := startStubServer(t, &hangingStub{})
	// Very short lag watchdog to keep the test fast.
	cfg := config.StreamConfig{EngineResponseTimeoutSec: 0.1}
	e := buildEngine(t, ep, endpointingEngine, cfg)
	startEngine(t, e)

	// Engine sends no hypotheses; watchdog should cancel and close resultsCh.
	timeout := time.After(3 * time.Second)
	for {
		select {
		case _, ok := <-e.Results():
			if !ok {
				return // watchdog fired, channel closed
			}
		case <-timeout:
			t.Fatal("lag watchdog did not fire within 3 s")
		}
	}
}

func TestStreamingEngine_OnUtteranceEndSendsFinalize(t *testing.T) {
	// Stub that records whether it received FINALIZE_UTTERANCE.
	finalized := make(chan struct{}, 1)
	srv := &finalizeRecorderStub{finalized: finalized}
	ep := startStubServer(t, srv)
	e := buildEngine(t, ep, endpointingCore, config.StreamConfig{})
	startEngine(t, e)
	defer func() { _ = e.Close() }()

	if err := e.OnUtteranceEnd(1, 10); err != nil {
		t.Fatalf("OnUtteranceEnd: %v", err)
	}
	select {
	case <-finalized:
	case <-time.After(3 * time.Second):
		t.Fatal("plugin did not receive FINALIZE_UTTERANCE within 3 s")
	}
}

// neverFinalStub sends partial hypotheses at 20 ms intervals but never sends is_final,
// even after receiving KIND_FINALIZE_UTTERANCE. Used to test finalize timeout.
type neverFinalStub struct {
	inferencepb.UnimplementedInferencePluginServer
}

func (s *neverFinalStub) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}
func (s *neverFinalStub) TranscribeStream(srv inferencepb.InferencePlugin_TranscribeStreamServer) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	reqCh := make(chan struct{}, 4)
	go func() {
		for {
			if _, err := srv.Recv(); err != nil {
				return
			}
			select {
			case reqCh <- struct{}{}:
			default:
			}
		}
	}()
	for {
		select {
		case <-ticker.C:
			_ = srv.Send(&inferencepb.StreamResponse{
				Payload: &inferencepb.StreamResponse_Hypothesis{
					Hypothesis: &inferencepb.StreamHypothesis{Text: "hello", IsFinal: false},
				},
			})
		case <-reqCh:
			// Intentionally ignore FINALIZE_UTTERANCE — never send is_final.
		case <-srv.Context().Done():
			return nil
		}
	}
}

func TestStreamingEngine_FinalizeTimeout(t *testing.T) {
	ep := startStubServer(t, &neverFinalStub{})
	cfg := config.StreamConfig{StreamingFinalizeTimeoutSec: 0.1}
	e := buildEngine(t, ep, endpointingCore, cfg)
	startEngine(t, e)

	// Trigger FINALIZE_UTTERANCE; stub will never respond with is_final.
	if err := e.OnUtteranceEnd(1, 10); err != nil {
		t.Fatalf("OnUtteranceEnd: %v", err)
	}

	// Drain results until channel closes (finalize timeout fires).
	timeout := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-e.Results():
			if !ok {
				if terr := e.TerminalErr(); terr == nil {
					t.Fatal("TerminalErr is nil after finalize timeout")
				}
				return
			}
		case <-timeout:
			t.Fatal("finalize timeout did not fire within 5 s")
		}
	}
}

// finalizeRecorderStub signals when it receives KIND_FINALIZE_UTTERANCE.
type finalizeRecorderStub struct {
	inferencepb.UnimplementedInferencePluginServer
	finalized chan struct{}
}

func (s *finalizeRecorderStub) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}
func (s *finalizeRecorderStub) TranscribeStream(srv inferencepb.InferencePlugin_TranscribeStreamServer) error {
	for {
		req, err := srv.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return nil
		}
		if ctrl := req.GetControl(); ctrl != nil {
			if ctrl.GetKind() == inferencepb.StreamControl_KIND_FINALIZE_UTTERANCE {
				select {
				case s.finalized <- struct{}{}:
				default:
				}
			}
		}
	}
}

// ── resolveEndpointingSource unit tests ──────────────────────────────────────

func TestResolveEndpointingSource_CoreMode(t *testing.T) {
	caps := &inferencepb.InferenceCapabilities{
		StreamingMode:         inferencepb.StreamingMode_STREAMING_MODE_BATCH_ONLY,
		EndpointingCapability: inferencepb.EndpointingCapability_ENDPOINTING_CAPABILITY_NONE,
	}
	src, err := resolveEndpointingSource("core", caps)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if src != endpointingCore {
		t.Fatalf("src = %v, want endpointingCore", src)
	}
}

func TestResolveEndpointingSource_EngineMode_Valid(t *testing.T) {
	caps := &inferencepb.InferenceCapabilities{
		StreamingMode:         inferencepb.StreamingMode_STREAMING_MODE_NATIVE,
		EndpointingCapability: inferencepb.EndpointingCapability_ENDPOINTING_CAPABILITY_AUTO_FINALIZE,
	}
	src, err := resolveEndpointingSource("engine", caps)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if src != endpointingEngine {
		t.Fatalf("src = %v, want endpointingEngine", src)
	}
}

func TestResolveEndpointingSource_InvalidCombo_BatchEngine(t *testing.T) {
	caps := &inferencepb.InferenceCapabilities{
		StreamingMode:         inferencepb.StreamingMode_STREAMING_MODE_BATCH_ONLY,
		EndpointingCapability: inferencepb.EndpointingCapability_ENDPOINTING_CAPABILITY_NONE,
	}
	_, err := resolveEndpointingSource("engine", caps)
	if err == nil {
		t.Fatal("expected error for engine endpointing on batch-only engine, got nil")
	}
	var sttErr *sttErrors.STTError
	if !errors.As(err, &sttErr) {
		t.Fatalf("expected STTError, got %T: %v", err, err)
	}
	if sttErr.Code() != sttErrors.ErrEngineEndpointingUnsupported {
		t.Fatalf("code = %v, want ErrEngineEndpointingUnsupported", sttErr.Code())
	}
}

func TestResolveEndpointingSource_InvalidCombo_NativeNoAutoFinalize(t *testing.T) {
	caps := &inferencepb.InferenceCapabilities{
		StreamingMode:         inferencepb.StreamingMode_STREAMING_MODE_NATIVE,
		EndpointingCapability: inferencepb.EndpointingCapability_ENDPOINTING_CAPABILITY_NONE,
	}
	_, err := resolveEndpointingSource("engine", caps)
	if err == nil {
		t.Fatal("expected error for engine endpointing without AUTO_FINALIZE capability, got nil")
	}
	var sttErr *sttErrors.STTError
	if !errors.As(err, &sttErr) {
		t.Fatalf("expected STTError, got %T: %v", err, err)
	}
	if sttErr.Code() != sttErrors.ErrEngineEndpointingUnsupported {
		t.Fatalf("code = %v, want ErrEngineEndpointingUnsupported", sttErr.Code())
	}
}

func TestStreamingEngine_LagWatchdogSetsTerminalErr(t *testing.T) {
	ep := startStubServer(t, &hangingStub{})
	cfg := config.StreamConfig{EngineResponseTimeoutSec: 0.1}
	e := buildEngine(t, ep, endpointingEngine, cfg)
	startEngine(t, e)

	timeout := time.After(3 * time.Second)
	for {
		select {
		case _, ok := <-e.Results():
			if !ok {
				terr := e.TerminalErr()
				if terr == nil {
					t.Fatal("TerminalErr is nil after lag watchdog fired")
				}
				var sttErr *sttErrors.STTError
				if !errors.As(terr, &sttErr) {
					t.Fatalf("TerminalErr is not STTError: %T %v", terr, terr)
				}
				if sttErr.Code() != sttErrors.ErrEngineResponseTimeout {
					t.Fatalf("code = %v, want ErrEngineResponseTimeout", sttErr.Code())
				}
				return
			}
		case <-timeout:
			t.Fatal("lag watchdog did not fire within 3 s")
		}
	}
}
