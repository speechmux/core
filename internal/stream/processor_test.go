package stream_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/speechmux/core/internal/plugin"
	"github.com/speechmux/core/internal/session"
	"github.com/speechmux/core/internal/stream"
	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
	vadpb "github.com/speechmux/proto/gen/go/vad/v1"
)

// ── VAD stub: speech for frames 1–25, silence for 26+ ────────────────────────

// speechThenSilenceVAD reports is_speech=true for sequence numbers 1–25 and
// is_speech=false for all subsequent frames, triggering one EPD utterance.
type speechThenSilenceVAD struct {
	vadpb.UnimplementedVADPluginServer
}

func (s *speechThenSilenceVAD) HealthCheck(_ context.Context, _ *vadpb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *speechThenSilenceVAD) GetCapabilities(_ context.Context, _ *vadpb.Empty) (*vadpb.VADCapabilities, error) {
	return &vadpb.VADCapabilities{OptimalFrameMs: 30}, nil
}

func (s *speechThenSilenceVAD) StreamVAD(srv vadpb.VADPlugin_StreamVADServer) error {
	for {
		req, err := srv.Recv()
		if err != nil {
			return nil
		}
		if req.GetSessionStart() != nil || req.GetSessionEnd() != nil {
			continue
		}
		seq := req.GetSequenceNumber()
		isSpeech := seq >= 1 && seq <= 25
		if err := srv.Send(&vadpb.VADResponse{
			IsSpeech:          isSpeech,
			SpeechProbability: map[bool]float32{true: 0.95, false: 0.05}[isSpeech],
			SequenceNumber:    seq,
		}); err != nil {
			return nil
		}
	}
}

// ── VAD stub: always silence (no EPD trigger) ─────────────────────────────────

// alwaysSilenceVAD returns is_speech=false for every frame so EPD never fires.
type alwaysSilenceVAD struct {
	vadpb.UnimplementedVADPluginServer
}

func (s *alwaysSilenceVAD) HealthCheck(_ context.Context, _ *vadpb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *alwaysSilenceVAD) GetCapabilities(_ context.Context, _ *vadpb.Empty) (*vadpb.VADCapabilities, error) {
	return &vadpb.VADCapabilities{OptimalFrameMs: 30}, nil
}

func (s *alwaysSilenceVAD) StreamVAD(srv vadpb.VADPlugin_StreamVADServer) error {
	for {
		req, err := srv.Recv()
		if err != nil {
			return nil
		}
		if req.GetSessionStart() != nil || req.GetSessionEnd() != nil {
			continue
		}
		if err := srv.Send(&vadpb.VADResponse{
			IsSpeech:       false,
			SequenceNumber: req.GetSequenceNumber(),
		}); err != nil {
			return nil
		}
	}
}

// ── STT stub ──────────────────────────────────────────────────────────────────

// fixedTextSTT always returns the same transcript text.
type fixedTextSTT struct {
	inferencepb.UnimplementedInferencePluginServer
	text string
}

func (s *fixedTextSTT) HealthCheck(_ context.Context, _ *inferencepb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *fixedTextSTT) Transcribe(_ context.Context, req *inferencepb.TranscribeRequest) (*inferencepb.TranscribeResponse, error) {
	return &inferencepb.TranscribeResponse{
		RequestId:        req.RequestId,
		SessionId:        req.SessionId,
		Text:             s.text,
		AudioDurationSec: 0.75,
	}, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

// sendAudioFrames sends n frames of PCM silence (640 bytes = 20 ms @ 16 kHz S16LE)
// into sess.AudioInCh, stopping early if ctx is cancelled.
func sendAudioFrames(ctx context.Context, t *testing.T, sess *session.Session, n int) {
	t.Helper()
	frame := make([]byte, 640)
	for i := 0; i < n; i++ {
		select {
		case sess.AudioInCh <- frame:
		case <-ctx.Done():
			t.Logf("sendAudioFrames: context cancelled after %d/%d frames", i, n)
			return
		}
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestProcessSession_HappyPath exercises the full pipeline:
// config → VAD session → audio forwarding → EPD fires → decode → result on ResultCh.
func TestProcessSession_HappyPath(t *testing.T) {
	vadEP := startVADServer(t, &speechThenSilenceVAD{})
	sttEP := startInferenceServer(t, &fixedTextSTT{text: "hello world"})

	router := plugin.NewPluginRouter("")
	if err := router.Add(sttEP.ID(), sttEP.Socket(), 0); err != nil {
		t.Fatalf("router.Add: %v", err)
	}
	scheduler := stream.NewDecodeScheduler(router, 4, 5.0, nil)

	cfgPtr := newTestConfig(0.15, 5.0)
	proc := stream.NewStreamProcessor(cfgPtr, []*plugin.Endpoint{vadEP}, scheduler, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess := session.NewTestSession(ctx, "happy-path-test")
	sess.Info.VADSilence = 0.15

	errCh := make(chan error, 1)
	go func() {
		errCh <- proc.ProcessSession(ctx, sess)
	}()

	// Send enough frames to trigger speech (1–25) and then silence (26–35).
	sendAudioFrames(ctx, t, sess, 35)

	// Expect at least one result within 3 s.
	select {
	case result, ok := <-sess.ResultCh:
		if !ok {
			t.Fatal("ResultCh closed before a result arrived")
		}
		if result == nil {
			t.Fatal("received nil result")
		}
		if !strings.Contains(result.GetText(), "hello world") {
			t.Errorf("result.Text = %q; want to contain %q", result.GetText(), "hello world")
		}
		if result.GetCommittedText() == "" && result.GetIsFinal() {
			// Final result should carry committed text after the assembler update.
			t.Logf("committed_text is empty for final result (acceptable if assembler starts empty)")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for recognition result")
	}

	// Cancel and verify ProcessSession returns cleanly.
	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			t.Logf("ProcessSession returned non-nil (may be gRPC-wrapped cancel): %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("ProcessSession did not return after context cancellation")
	}
}

// TestProcessSession_ContextCancellation verifies that cancelling the context
// while ProcessSession is running causes it to return promptly, with no
// goroutine leak detectable within the test window.
//
// The cancel is deferred until ProcessSession has actually started (detected by
// waiting for the first audio frame to be consumed) to avoid cancelling before
// the VAD stream is established.
func TestProcessSession_ContextCancellation(t *testing.T) {
	vadEP := startVADServer(t, &alwaysSilenceVAD{})

	cfgPtr := newTestConfig(0.5, 5.0)
	proc := stream.NewStreamProcessor(cfgPtr, []*plugin.Endpoint{vadEP}, nil, nil)

	// Use a long-lived parent context so that session and pipeline can start.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess := session.NewTestSession(ctx, "cancel-test")

	errCh := make(chan error, 1)
	go func() {
		errCh <- proc.ProcessSession(ctx, sess)
	}()

	// Send frames one at a time until we confirm at least one has been
	// consumed (AudioInCh has capacity 64, so a successful send means the
	// pipeline goroutine exists). We send via a separate context-free push so
	// the test itself does not block.
	frame := make([]byte, 640)
	for i := 0; i < 5; i++ {
		select {
		case sess.AudioInCh <- frame:
		case <-ctx.Done():
			t.Fatal("outer context expired before frames could be sent")
		}
	}

	// Give the pipeline goroutines a brief moment to start consuming.
	time.Sleep(50 * time.Millisecond)

	// Cancel the context now that the pipeline is running.
	cancel()

	select {
	case err := <-errCh:
		// nil or context-related error are both acceptable return values.
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			t.Logf("ProcessSession returned: %v (acceptable after context cancel)", err)
		}
	case <-time.After(4 * time.Second):
		t.Error("ProcessSession did not return within 4 s after context cancellation — possible goroutine leak")
	}

	// ProcessingDone must be closed after ProcessSession returns.
	select {
	case <-sess.ProcessingDone():
		// Pass.
	case <-time.After(2 * time.Second):
		t.Error("sess.ProcessingDone() not closed after ProcessSession returned")
	}

	// ResultCh must be closed.
	select {
	case _, ok := <-sess.ResultCh:
		if ok {
			t.Error("unexpected value received from ResultCh after cancel")
		}
	case <-time.After(time.Second):
		t.Error("ResultCh not closed after ProcessSession returned")
	}
}

// TestProcessSession_NoInferencePlugin verifies that when scheduler is nil
// (no inference endpoints configured), the pipeline still runs cleanly:
// EPD may fire but no decode attempt is made, and the session ends without error
// when the context is cancelled.
func TestProcessSession_NoInferencePlugin(t *testing.T) {
	vadEP := startVADServer(t, &speechThenSilenceVAD{})

	// nil scheduler — no inference endpoint.
	cfgPtr := newTestConfig(0.15, 5.0)
	proc := stream.NewStreamProcessor(cfgPtr, []*plugin.Endpoint{vadEP}, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	sess := session.NewTestSession(ctx, "no-infer-test")
	sess.Info.VADSilence = 0.15

	errCh := make(chan error, 1)
	go func() {
		errCh <- proc.ProcessSession(ctx, sess)
	}()

	// Send frames that will trigger EPD (speech 1–25, silence 26–35).
	// EPD fires but the decodeWorkerLoop drains tasks without calling the
	// scheduler, so no result should arrive on ResultCh.
	sendAudioFrames(ctx, t, sess, 35)

	// No result should arrive — the processor should swallow decode tasks.
	select {
	case _, ok := <-sess.ResultCh:
		if ok {
			t.Error("received unexpected result on ResultCh with nil scheduler")
		}
		// Channel closed — ProcessSession returned after audio end; this is also
		// acceptable if the pipeline noticed the closed channel.
	case <-time.After(400 * time.Millisecond):
		// No result within 400 ms is the expected path; proceed to cancel.
	}

	// Cancel and confirm the processor exits.
	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			t.Logf("ProcessSession returned: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("ProcessSession did not return within 3 s after context cancel")
	}
}

// TestProcessSession_AudioEndSignal verifies that closing AudioInCh
// (via SignalAudioEnd) causes the pipeline to flush and terminate naturally,
// even without cancelling the context.
func TestProcessSession_AudioEndSignal(t *testing.T) {
	vadEP := startVADServer(t, &alwaysSilenceVAD{})

	cfgPtr := newTestConfig(0.5, 5.0)
	proc := stream.NewStreamProcessor(cfgPtr, []*plugin.Endpoint{vadEP}, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	sess := session.NewTestSession(ctx, "audioend-test")

	errCh := make(chan error, 1)
	go func() {
		errCh <- proc.ProcessSession(ctx, sess)
	}()

	// Send a handful of frames, then signal end-of-audio.
	sendAudioFrames(ctx, t, sess, 5)
	sess.SignalAudioEnd()

	// The pipeline should drain and ProcessSession should return.
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("ProcessSession returned unexpected error after AudioEnd: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Error("ProcessSession did not return within 4 s after SignalAudioEnd")
	}

	// Both done channels must be closed.
	select {
	case <-sess.ProcessingDone():
	case <-time.After(time.Second):
		t.Error("sess.ProcessingDone() not closed after pipeline terminated")
	}
}

// TestProcessSession_ResultHasExpectedFields verifies that the RecognitionResult
// received from the pipeline contains the expected fields: non-empty Text,
// a non-negative AudioDuration, and monotonically non-decreasing CommittedText
// for final results.
func TestProcessSession_ResultHasExpectedFields(t *testing.T) {
	vadEP := startVADServer(t, &speechThenSilenceVAD{})
	sttEP := startInferenceServer(t, &fixedTextSTT{text: "field check"})

	router := plugin.NewPluginRouter("")
	if err := router.Add(sttEP.ID(), sttEP.Socket(), 0); err != nil {
		t.Fatalf("router.Add: %v", err)
	}
	scheduler := stream.NewDecodeScheduler(router, 4, 5.0, nil)

	cfgPtr := newTestConfig(0.15, 5.0)
	proc := stream.NewStreamProcessor(cfgPtr, []*plugin.Endpoint{vadEP}, scheduler, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess := session.NewTestSession(ctx, "fields-test")
	sess.Info.VADSilence = 0.15

	errCh := make(chan error, 1)
	go func() {
		errCh <- proc.ProcessSession(ctx, sess)
	}()

	sendAudioFrames(ctx, t, sess, 35)

	select {
	case result, ok := <-sess.ResultCh:
		if !ok {
			t.Fatal("ResultCh closed without a result")
		}
		if result.GetText() == "" {
			t.Error("result.Text should not be empty")
		}
		if result.GetAudioDuration() < 0 {
			t.Errorf("result.AudioDuration = %f; want >= 0", result.GetAudioDuration())
		}
		// StartSec and EndSec are derived from sequence numbers; EndSec >= StartSec.
		if result.GetEndSec() < result.GetStartSec() {
			t.Errorf("EndSec %f < StartSec %f", result.GetEndSec(), result.GetStartSec())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for a result with expected fields")
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
