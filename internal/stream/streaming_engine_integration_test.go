package stream_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/speechmux/core/internal/config"
	"github.com/speechmux/core/internal/plugin"
	"github.com/speechmux/core/internal/session"
	"github.com/speechmux/core/internal/stream"
	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ── streaming engine stubs ────────────────────────────────────────────────────

// nativeStreamingSTT advertises STREAMING_MODE_NATIVE + ENDPOINTING_CAPABILITY_NONE.
// It emits a partial hypothesis for every received audio chunk and a final
// hypothesis when it receives KIND_FINALIZE_UTTERANCE.
type nativeStreamingSTT struct {
	inferencepb.UnimplementedInferencePluginServer
}

func (s *nativeStreamingSTT) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *nativeStreamingSTT) GetCapabilities(_ context.Context, _ *emptypb.Empty) (*inferencepb.InferenceCapabilities, error) {
	return &inferencepb.InferenceCapabilities{
		EngineName:            "native-stub",
		StreamingMode:         inferencepb.StreamingMode_STREAMING_MODE_NATIVE,
		EndpointingCapability: inferencepb.EndpointingCapability_ENDPOINTING_CAPABILITY_NONE,
	}, nil
}

func (s *nativeStreamingSTT) TranscribeStream(srv inferencepb.InferencePlugin_TranscribeStreamServer) error {
	audioCount := 0
	for {
		req, err := srv.Recv()
		if err != nil {
			return nil
		}
		if req.GetAudio() != nil {
			audioCount++
			_ = srv.Send(&inferencepb.StreamResponse{
				Payload: &inferencepb.StreamResponse_Hypothesis{
					Hypothesis: &inferencepb.StreamHypothesis{
						Text:    "hello world",
						IsFinal: false,
					},
				},
			})
		}
		if ctrl := req.GetControl(); ctrl != nil {
			if ctrl.GetKind() == inferencepb.StreamControl_KIND_FINALIZE_UTTERANCE {
				_ = srv.Send(&inferencepb.StreamResponse{
					Payload: &inferencepb.StreamResponse_Hypothesis{
						Hypothesis: &inferencepb.StreamHypothesis{
							Text:    "hello world",
							IsFinal: true,
						},
					},
				})
			}
		}
	}
}

// autoFinalizeSTT advertises STREAMING_MODE_NATIVE + ENDPOINTING_CAPABILITY_AUTO_FINALIZE.
// It emits partials and auto-finalizes after 3 audio chunks.
type autoFinalizeSTT struct {
	inferencepb.UnimplementedInferencePluginServer
}

func (s *autoFinalizeSTT) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *autoFinalizeSTT) GetCapabilities(_ context.Context, _ *emptypb.Empty) (*inferencepb.InferenceCapabilities, error) {
	return &inferencepb.InferenceCapabilities{
		EngineName:            "auto-finalize-stub",
		StreamingMode:         inferencepb.StreamingMode_STREAMING_MODE_NATIVE,
		EndpointingCapability: inferencepb.EndpointingCapability_ENDPOINTING_CAPABILITY_AUTO_FINALIZE,
	}, nil
}

func (s *autoFinalizeSTT) TranscribeStream(srv inferencepb.InferencePlugin_TranscribeStreamServer) error {
	audioCount := 0
	for {
		req, err := srv.Recv()
		if err != nil {
			return nil
		}
		if req.GetAudio() != nil {
			audioCount++
			_ = srv.Send(&inferencepb.StreamResponse{
				Payload: &inferencepb.StreamResponse_Hypothesis{
					Hypothesis: &inferencepb.StreamHypothesis{
						Text:    "auto partial",
						IsFinal: false,
					},
				},
			})
			if audioCount >= 3 {
				_ = srv.Send(&inferencepb.StreamResponse{
					Payload: &inferencepb.StreamResponse_Hypothesis{
						Hypothesis: &inferencepb.StreamHypothesis{
							Text:    "auto final",
							IsFinal: true,
						},
					},
				})
				audioCount = 0
			}
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newStreamingTestConfig(vadSilenceSec float64) *atomic.Pointer[config.Config] {
	cfg := &config.Config{}
	cfg.Stream.VADSilenceSec = vadSilenceSec
	cfg.Stream.MaxBufferSec = 30
	cfg.Stream.DecodeTimeoutSec = 5.0
	cfg.Stream.EndpointingSource = "core"
	cfg.Codec.TargetSampleRate = 16000
	cfg.Defaults()
	cfg.Stream.VADSilenceSec = vadSilenceSec
	cfg.Stream.EndpointingSource = "core"

	var ptr atomic.Pointer[config.Config]
	ptr.Store(cfg)
	return &ptr
}

// ── integration tests ─────────────────────────────────────────────────────────

// TestStreamingEngine_ProcessSession_PartialsAndFinal verifies that ProcessSession
// with a NATIVE streaming engine delivers partial results followed by a final result
// when the VAD EPD triggers FINALIZE_UTTERANCE.
func TestStreamingEngine_ProcessSession_PartialsAndFinal(t *testing.T) {
	vadEP := startVADServer(t, &speechThenSilenceVAD{})
	sttEP := startInferenceServer(t, &nativeStreamingSTT{})

	router := plugin.NewPluginRouter("")
	if err := router.Add(sttEP.ID(), sttEP.Socket(), 0); err != nil {
		t.Fatalf("router.Add: %v", err)
	}
	// FetchCapabilities so the router knows this is a NATIVE engine.
	scheduler := stream.NewDecodeScheduler(router, 4, 0, 5.0, nil)
	cfgPtr := newStreamingTestConfig(0.15)
	proc := stream.NewStreamProcessor(cfgPtr, []*plugin.Endpoint{vadEP}, scheduler, router, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess := session.NewTestSession(ctx, "streaming-partial-final")
	sess.Info.VADSilence = 0.15

	errCh := make(chan error, 1)
	go func() {
		errCh <- proc.ProcessSession(ctx, sess)
	}()

	// Send enough frames to push VAD seq past 25 so the VAD stub returns silence
	// for seq 26+. 50 PCM frames at 20 ms each → ~33 aggregated 30 ms VAD frames.
	// VAD seq 26–33 come back as silence, which exceeds the 0.15 s EPD threshold
	// (5 frames × 30 ms = 150 ms) and triggers utterance-end → FINALIZE_UTTERANCE.
	sendAudioFrames(ctx, t, sess, 50)

	// Expect at least one final result.
	var sawFinal bool
	deadline := time.After(10 * time.Second)
	for !sawFinal {
		select {
		case result, ok := <-sess.ResultCh:
			if !ok {
				goto done
			}
			if result.GetIsFinal() {
				sawFinal = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for final result from streaming engine")
		}
	}
done:
	if !sawFinal {
		t.Error("no final result received from streaming engine")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			t.Logf("ProcessSession returned: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("ProcessSession did not return after cancel")
	}
}

// TestStreamingEngine_ProcessSession_CommittedMonotonic verifies that CommittedText
// never shrinks across successive results during a streaming engine session.
func TestStreamingEngine_ProcessSession_CommittedMonotonic(t *testing.T) {
	vadEP := startVADServer(t, &speechThenSilenceVAD{})
	sttEP := startInferenceServer(t, &nativeStreamingSTT{})

	router := plugin.NewPluginRouter("")
	if err := router.Add(sttEP.ID(), sttEP.Socket(), 0); err != nil {
		t.Fatalf("router.Add: %v", err)
	}
	scheduler := stream.NewDecodeScheduler(router, 4, 0, 5.0, nil)
	cfgPtr := newStreamingTestConfig(0.15)
	proc := stream.NewStreamProcessor(cfgPtr, []*plugin.Endpoint{vadEP}, scheduler, router, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess := session.NewTestSession(ctx, "streaming-committed-monotonic")
	sess.Info.VADSilence = 0.15

	errCh := make(chan error, 1)
	go func() {
		errCh <- proc.ProcessSession(ctx, sess)
	}()

	sendAudioFrames(ctx, t, sess, 50)

	var prevCommitted string
	resultCount := 0
	deadline := time.After(10 * time.Second)
loop:
	for resultCount < 5 {
		select {
		case result, ok := <-sess.ResultCh:
			if !ok {
				break loop
			}
			cur := result.GetCommittedText()
			if len(cur) < len(prevCommitted) {
				t.Errorf("CommittedText shrank: %q → %q", prevCommitted, cur)
			}
			prevCommitted = cur
			resultCount++
		case <-deadline:
			break loop
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			t.Logf("ProcessSession returned: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("ProcessSession did not return after cancel")
	}
}

// TestStreamingEngine_ProcessSession_AudioEndClean verifies that SignalAudioEnd
// causes ProcessSession to return cleanly (no goroutine leak) on the streaming path.
func TestStreamingEngine_ProcessSession_AudioEndClean(t *testing.T) {
	vadEP := startVADServer(t, &alwaysSilenceVAD{})
	sttEP := startInferenceServer(t, &nativeStreamingSTT{})

	router := plugin.NewPluginRouter("")
	if err := router.Add(sttEP.ID(), sttEP.Socket(), 0); err != nil {
		t.Fatalf("router.Add: %v", err)
	}
	scheduler := stream.NewDecodeScheduler(router, 4, 0, 5.0, nil)
	cfgPtr := newStreamingTestConfig(0.5)
	proc := stream.NewStreamProcessor(cfgPtr, []*plugin.Endpoint{vadEP}, scheduler, router, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	sess := session.NewTestSession(ctx, "streaming-audioend-clean")

	errCh := make(chan error, 1)
	go func() {
		errCh <- proc.ProcessSession(ctx, sess)
	}()

	sendAudioFrames(ctx, t, sess, 5)
	sess.SignalAudioEnd()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("ProcessSession returned unexpected error after AudioEnd: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Error("ProcessSession did not return within 4 s after SignalAudioEnd (streaming path)")
	}

	select {
	case <-sess.ProcessingDone():
	case <-time.After(time.Second):
		t.Error("sess.ProcessingDone() not closed after ProcessSession returned")
	}
}

// TestStreamingEngine_ProcessSession_AutoFinalizeEngine verifies that when
// endpointing_source=engine and the engine has AUTO_FINALIZE capability, the
// VAD goroutines are skipped and the engine auto-finalizes.
func TestStreamingEngine_ProcessSession_AutoFinalizeEngine(t *testing.T) {
	sttEP := startInferenceServer(t, &autoFinalizeSTT{})

	router := plugin.NewPluginRouter("")
	if err := router.Add(sttEP.ID(), sttEP.Socket(), 0); err != nil {
		t.Fatalf("router.Add: %v", err)
	}
	scheduler := stream.NewDecodeScheduler(router, 4, 0, 5.0, nil)

	// Use endpointing_source=engine: no VAD endpoints needed.
	cfg := &config.Config{}
	cfg.Stream.EndpointingSource = "engine"
	cfg.Stream.MaxBufferSec = 30
	cfg.Stream.DecodeTimeoutSec = 5.0
	cfg.Codec.TargetSampleRate = 16000
	cfg.Defaults()
	cfg.Stream.EndpointingSource = "engine"
	var cfgPtr atomic.Pointer[config.Config]
	cfgPtr.Store(cfg)

	proc := stream.NewStreamProcessor(&cfgPtr, nil, scheduler, router, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess := session.NewTestSession(ctx, "streaming-autofin-engine")

	errCh := make(chan error, 1)
	go func() {
		errCh <- proc.ProcessSession(ctx, sess)
	}()

	// Send enough audio to trigger 3 auto-finalize cycles.
	sendAudioFrames(ctx, t, sess, 12)

	var sawFinal bool
	deadline := time.After(5 * time.Second)
	for !sawFinal {
		select {
		case result, ok := <-sess.ResultCh:
			if !ok {
				goto autoDone
			}
			if result.GetIsFinal() {
				sawFinal = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for auto-finalize result")
		}
	}
autoDone:
	if !sawFinal {
		t.Error("no final result received from auto-finalize engine")
	}

	sess.SignalAudioEnd()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			t.Logf("ProcessSession returned: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("ProcessSession did not return after audio end")
	}
}
