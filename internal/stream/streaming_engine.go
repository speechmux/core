package stream

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/speechmux/core/internal/config"
	sttErrors "github.com/speechmux/core/internal/errors"
	"github.com/speechmux/core/internal/plugin"
	"github.com/speechmux/core/internal/session"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
)

// endpointingSource mirrors config.StreamConfig.EndpointingSource as a typed constant.
type endpointingSource int

const (
	endpointingCore   endpointingSource = iota // Core VAD + EPDController drive finalization.
	endpointingEngine                          // Engine auto-finalizes; VAD goroutines are skipped.
)

// parseEndpointingSource converts the config string to the typed constant.
// "hybrid" is rejected at config load; any unknown value falls back to core.
func parseEndpointingSource(s string) endpointingSource {
	if s == "engine" {
		return endpointingEngine
	}
	return endpointingCore
}

// resolveEndpointingSource validates the config EndpointingSource against the
// engine's advertised capabilities and returns the resolved endpointingSource.
// Returns ERR1020 when the config requests engine endpointing but the engine
// does not support it.
func resolveEndpointingSource(cfgSrc string, caps *inferencepb.InferenceCapabilities) (endpointingSource, error) {
	src := parseEndpointingSource(cfgSrc)
	if src == endpointingEngine {
		if caps.GetStreamingMode() != inferencepb.StreamingMode_STREAMING_MODE_NATIVE {
			return 0, sttErrors.New(sttErrors.ErrEngineEndpointingUnsupported,
				"endpointing_source=engine requires STREAMING_MODE_NATIVE engine")
		}
		if caps.GetEndpointingCapability() != inferencepb.EndpointingCapability_ENDPOINTING_CAPABILITY_AUTO_FINALIZE {
			return 0, sttErrors.New(sttErrors.ErrEngineEndpointingUnsupported,
				"endpointing_source=engine requires ENDPOINTING_CAPABILITY_AUTO_FINALIZE")
		}
	}
	return src, nil
}

// streamingDecodeEngine implements DecodeEngine for STREAMING_MODE_NATIVE engines.
// It holds a single per-session TranscribeStream bidi stream for the session lifetime.
//
// Goroutines (all started in Start, all exit before Close returns):
//   - recvLoop:       reads StreamResponse from plugin; emits DecodeResult; owns resultsCh close.
//   - engineWatchdog: (src=engine only) lag + max-utterance safeguard timers.
type streamingDecodeEngine struct {
	client    *plugin.InferenceStreamClient
	src       endpointingSource
	assembler *ResultAssembler
	cfg       config.StreamConfig
	sessID    string

	resultsCh chan DecodeResult
	closeOnce sync.Once
	cancel    context.CancelFunc // cancels internal goroutines

	recvDone chan struct{} // closed when recvLoop exits

	// terminalErr carries the error that caused abnormal shutdown (e.g. lag watchdog,
	// finalize timeout). Stored before e.cancel() fires; read by the forwarder goroutine
	// after resultsCh closes so the error propagates to ProcessSession as ERR3007.
	terminalErr atomic.Pointer[error]

	// Shared state updated by recvLoop, read by engineWatchdog (atomic access).
	lastHypAt   atomic.Int64 // unix nano of last received hypothesis
	lastFinalAt atomic.Int64 // unix nano of last received is_final hypothesis
	finalizeAt  atomic.Int64 // unix nano when last FINALIZE_UTTERANCE was sent (0 = not pending)
}

// newStreamingDecodeEngine creates a streamingDecodeEngine. Call Start before use.
func newStreamingDecodeEngine(
	client *plugin.InferenceStreamClient,
	src endpointingSource,
	cfg config.StreamConfig,
) *streamingDecodeEngine {
	return &streamingDecodeEngine{
		client:    client,
		src:       src,
		assembler: NewResultAssembler(),
		cfg:       cfg,
		resultsCh: make(chan DecodeResult, 32),
		recvDone:  make(chan struct{}),
	}
}

// Start initialises internal state and launches background goroutines.
func (e *streamingDecodeEngine) Start(ctx context.Context, sess *session.Session, cfg SessionDecodeConfig) error {
	e.sessID = sess.ID
	e.cfg = cfg.Stream
	now := time.Now().UnixNano()
	e.lastHypAt.Store(now)
	e.lastFinalAt.Store(now)

	iCtx, rawCancel := context.WithCancel(ctx)
	// Wrap rawCancel so that cancelling the engine context also cancels the
	// underlying gRPC stream, unblocking any in-flight Recv calls.
	e.cancel = func() {
		rawCancel()
		e.client.CancelStream()
	}

	go e.recvLoop(iCtx)
	if e.src == endpointingEngine {
		go e.engineWatchdog(iCtx)
	}
	return nil
}

// FeedFrame sends a PCM chunk to the engine over the bidi stream.
// For streaming engines this is the primary audio delivery path; called by
// vadSendLoop for each aggregated frame. Backpressure propagates via gRPC send.
func (e *streamingDecodeEngine) FeedFrame(_ uint64, pcm []byte) error {
	if _, err := e.client.SendAudio(pcm); err != nil {
		return fmt.Errorf("streaming engine FeedFrame: %w", err)
	}
	return nil
}

// OnSpeechStart is a no-op for streaming engines; they emit partials at their own cadence.
func (e *streamingDecodeEngine) OnSpeechStart(_ uint64) {}

// OnSpeechEnd is a no-op for streaming engines.
func (e *streamingDecodeEngine) OnSpeechEnd() {}

// OnUtteranceEnd is called by EPDController when endpointing_source=core.
// Sends FINALIZE_UTTERANCE to the engine and records the finalize timestamp
// so recvLoop can enforce streaming_finalize_timeout_sec.
// NOT called when endpointing_source=engine (EPD goroutine does not run).
func (e *streamingDecodeEngine) OnUtteranceEnd(_, _ uint64) error {
	e.finalizeAt.Store(time.Now().UnixNano())
	return e.client.SendFinalize()
}

// Results returns the channel that carries decode outputs.
func (e *streamingDecodeEngine) Results() <-chan DecodeResult { return e.resultsCh }

// Close shuts down the engine. Shutdown order:
//  1. client.Close() — half-closes the send side so the plugin flushes remaining responses.
//  2. cancel()       — unblocks recvLoop and engineWatchdog if they are stuck.
//  3. <-recvDone    — waits for recvLoop to drain plugin responses and close resultsCh.
//
// Close is idempotent.
func (e *streamingDecodeEngine) Close() error {
	e.closeOnce.Do(func() {
		e.client.Close() // half-close send: signals plugin we are done
		e.cancel()       // unblock any stuck Recv / watchdog select
		<-e.recvDone     // wait for recvLoop to finish draining
	})
	return nil
}

// recvLoop reads StreamResponse messages from the plugin, assembles them into
// DecodeResult values, and pushes them onto resultsCh. It owns resultsCh close.
func (e *streamingDecodeEngine) recvLoop(ctx context.Context) {
	defer close(e.recvDone)
	defer close(e.resultsCh)

	for {
		resp, err := e.client.Recv()
		if err != nil {
			if !isNormalShutdown(err) && err != io.EOF && !isStreamEnd(err) {
				slog.Warn("streaming engine recv error",
					"session_id", e.sessID, "error", err)
			}
			return
		}

		now := time.Now().UnixNano()
		e.lastHypAt.Store(now)

		switch p := resp.Payload.(type) {
		case *inferencepb.StreamResponse_Hypothesis:
			h := p.Hypothesis
			if h.GetIsFinal() {
				e.lastFinalAt.Store(now)
				e.finalizeAt.Store(0) // finalize fulfilled; clear pending sentinel
			}

			// Check finalize timeout (endpointing_source=core path).
			// Fired when a hypothesis arrives while FINALIZE_UTTERANCE is pending but
			// is_final has not yet come within StreamingFinalizeTimeoutSec.
			if e.cfg.StreamingFinalizeTimeoutSec > 0 {
				if sentAt := e.finalizeAt.Load(); sentAt != 0 {
					elapsed := time.Duration(now-sentAt) * time.Nanosecond
					if elapsed > time.Duration(float64(time.Second)*e.cfg.StreamingFinalizeTimeoutSec) {
						slog.Error("streaming finalize timeout",
							"session_id", e.sessID,
							"elapsed_sec", elapsed.Seconds(),
							"timeout_sec", e.cfg.StreamingFinalizeTimeoutSec)
						e.storeTerminalErr(sttErrors.New(sttErrors.ErrEngineResponseTimeout,
							"streaming finalize timeout: engine did not emit is_final"))
						e.cancel()
						return
					}
				}
			}

			var committed, unstable string
			if h.GetCommittedText() != "" || h.GetUnstableText() != "" {
				committed, unstable = e.assembler.UpdateRaw(
					h.GetCommittedText(), h.GetUnstableText(), h.GetIsFinal())
			} else {
				committed, unstable = e.assembler.Update(h.GetText(), h.GetIsFinal())
			}

			result := DecodeResult{
				Text:          h.GetText(),
				CommittedText: committed,
				UnstableText:  unstable,
				IsFinal:       h.GetIsFinal(),
				StartSec:      h.GetStartSec(),
				EndSec:        h.GetEndSec(),
				LanguageCode:  h.GetLanguageCode(),
			}
			select {
			case e.resultsCh <- result:
			case <-ctx.Done():
				return
			}

		case *inferencepb.StreamResponse_Error:
			slog.Error("streaming engine error response",
				"session_id", e.sessID,
				"code", p.Error.GetCode(),
				"message", p.Error.GetMessage())
			e.cancel()
			return
		}
	}
}

// engineWatchdog runs only when endpointing_source=engine. It enforces two policies:
//
//  1. Lag watchdog: if engine emits no hypothesis for EngineResponseTimeoutSec,
//     cancel the session (surfaces as ERR3007 in the caller).
//
//  2. Max-utterance safeguard: if engine emits no is_final for MaxUtteranceSec,
//     force FINALIZE_UTTERANCE. Protects against engines that never auto-finalize.
func (e *streamingDecodeEngine) engineWatchdog(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()

			if e.cfg.EngineResponseTimeoutSec > 0 {
				lastHyp := time.Unix(0, e.lastHypAt.Load())
				if now.Sub(lastHyp) > time.Duration(float64(time.Second)*e.cfg.EngineResponseTimeoutSec) {
					slog.Error("streaming engine response timeout",
						"session_id", e.sessID,
						"lag_sec", now.Sub(lastHyp).Seconds())
					e.storeTerminalErr(sttErrors.New(sttErrors.ErrEngineResponseTimeout,
						"streaming engine response timeout: no hypothesis received"))
					e.cancel()
					return
				}
			}

			if e.cfg.MaxUtteranceSec > 0 {
				lastFinal := time.Unix(0, e.lastFinalAt.Load())
				if now.Sub(lastFinal) > time.Duration(float64(time.Second)*e.cfg.MaxUtteranceSec) {
					slog.Warn("max utterance duration exceeded; forcing FINALIZE_UTTERANCE",
						"session_id", e.sessID,
						"elapsed_sec", now.Sub(lastFinal).Seconds())
					// Reset before sending to prevent rapid re-firing (D6).
					e.lastFinalAt.Store(now.UnixNano())
					if err := e.client.SendFinalize(); err != nil {
						slog.Error("SendFinalize after max_utterance_sec failed",
							"session_id", e.sessID, "error", err)
						e.cancel()
						return
					}
				}
			}
		}
	}
}

// storeTerminalErr records the error that caused abnormal shutdown.
// Only the first call takes effect; subsequent calls are no-ops.
func (e *streamingDecodeEngine) storeTerminalErr(err error) {
	e.terminalErr.CompareAndSwap(nil, &err)
}

// TerminalErr returns the abnormal shutdown error set by the watchdog or
// recvLoop, or nil on clean shutdown. Called by the forwarder goroutine
// in ProcessSession after resultsCh closes.
func (e *streamingDecodeEngine) TerminalErr() error {
	if ep := e.terminalErr.Load(); ep != nil {
		return *ep
	}
	return nil
}

// Ensure compile-time implementation check.
var _ DecodeEngine = (*streamingDecodeEngine)(nil)
