package stream

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/speechmux/core/internal/config"
	sttErrors "github.com/speechmux/core/internal/errors"
	"github.com/speechmux/core/internal/metrics"
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

// audioChunkRecord stores the cumulative audio position and the wall-clock time
// a PCM chunk was forwarded to the engine. Used for partial-latency calculation.
type audioChunkRecord struct {
	audioEndSec float64   // cumulative audio position (seconds) at send time
	sentAt      time.Time
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

	// Observability.
	obs        metrics.MetricsObserver
	engineName string
	logger     *slog.Logger
	span       trace.Span      // long-lived session span; ended in Close()
	sessionCtx context.Context // parent context for utterance child spans

	// closeReason stores the termination reason string (enum-style, closed set).
	// Set at each exit path before cancel(); read in Close() for RecordStreamingSessionClose.
	closeReason atomic.Value

	// Per-utterance OTel tracking. Guarded by utteranceMu because OnUtteranceEnd
	// (EPD goroutine) and recvLoop (recv goroutine) access these fields concurrently.
	utteranceMu   sync.Mutex
	utteranceSpan trace.Span
	utteranceIdx  int

	// Partial-latency ring. FeedFrame is called from a single goroutine (vadSendLoop),
	// so these fields need no mutex.
	audioRing       [512]audioChunkRecord
	audioRingIdx    int
	audioElapsedSec float64
	sampleRate      int // set in Start() from int(sess.Info.SourceSampleRate)
}

// newStreamingDecodeEngine creates a streamingDecodeEngine. Call Start before use.
func newStreamingDecodeEngine(
	client *plugin.InferenceStreamClient,
	src endpointingSource,
	cfg config.StreamConfig,
	obs metrics.MetricsObserver,
	engineName string,
) *streamingDecodeEngine {
	return &streamingDecodeEngine{
		client:     client,
		src:        src,
		assembler:  NewResultAssembler(),
		cfg:        cfg,
		obs:        obs,
		engineName: engineName,
		resultsCh:  make(chan DecodeResult, 32),
		recvDone:   make(chan struct{}),
	}
}

// Start initialises internal state and launches background goroutines.
func (e *streamingDecodeEngine) Start(ctx context.Context, sess *session.Session, cfg SessionDecodeConfig) error {
	e.sessID = sess.ID
	e.cfg = cfg.Stream
	e.sampleRate = int(sess.Info.SourceSampleRate)

	e.logger = slog.With(
		"component", "streaming_decode_engine",
		"session_id", sess.ID,
		"engine_name", e.engineName,
	)

	cfgSrc := "core"
	if e.src == endpointingEngine {
		cfgSrc = "engine"
	}
	tracer := otel.Tracer("speechmux/streaming")
	sCtx, span := tracer.Start(ctx, "stt.stream_session",
		trace.WithAttributes(
			attribute.String("session.id", sess.ID),
			attribute.String("engine.name", e.engineName),
			attribute.String("endpointing.source", cfgSrc),
		))
	e.span = span
	e.sessionCtx = sCtx

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

	e.obs.RecordStreamingSessionOpen(e.engineName)

	go e.recvLoop(iCtx)
	if e.src == endpointingEngine {
		go e.engineWatchdog(iCtx)
	}
	return nil
}

// FeedFrame sends a PCM chunk to the engine over the bidi stream and records
// the cumulative audio position for partial-latency tracking.
// For streaming engines this is the primary audio delivery path; called by
// vadSendLoop for each aggregated frame. Backpressure propagates via gRPC send.
func (e *streamingDecodeEngine) FeedFrame(_ uint64, pcm []byte) error {
	if _, err := e.client.SendAudio(pcm); err != nil {
		return fmt.Errorf("streaming engine FeedFrame: %w", err)
	}
	if e.sampleRate > 0 {
		chunkSec := float64(len(pcm)) / float64(e.sampleRate*2) // 16-bit PCM
		e.audioElapsedSec += chunkSec
		e.audioRing[e.audioRingIdx%512] = audioChunkRecord{
			audioEndSec: e.audioElapsedSec,
			sentAt:      time.Now(),
		}
		e.audioRingIdx++
	}
	return nil
}

// OnSpeechStart is a no-op for streaming engines; they emit partials at their own cadence.
func (e *streamingDecodeEngine) OnSpeechStart(_ uint64) {}

// OnSpeechEnd is a no-op for streaming engines.
func (e *streamingDecodeEngine) OnSpeechEnd() {}

// OnUtteranceEnd is called by EPDController when endpointing_source=core.
// Sends FINALIZE_UTTERANCE to the engine, records the finalize timestamp,
// and opens a per-utterance OTel child span.
// NOT called when endpointing_source=engine (EPD goroutine does not run).
func (e *streamingDecodeEngine) OnUtteranceEnd(_, _ uint64) error {
	tracer := otel.Tracer("speechmux/streaming")
	e.utteranceMu.Lock()
	_, uSpan := tracer.Start(e.sessionCtx, "stt.stream_utterance",
		trace.WithAttributes(attribute.Int("utterance.index", e.utteranceIdx)))
	e.utteranceSpan = uSpan
	e.utteranceIdx++
	e.utteranceMu.Unlock()

	e.finalizeAt.Store(time.Now().UnixNano())
	return e.client.SendFinalize()
}

// Results returns the channel that carries decode outputs.
func (e *streamingDecodeEngine) Results() <-chan DecodeResult { return e.resultsCh }

// Close shuts down the engine. Shutdown order:
//  1. client.Close() — half-closes the send side so the plugin flushes remaining responses.
//  2. <-recvDone    — waits for recvLoop to drain plugin responses and close resultsCh.
//  3. cancel()       — stops the watchdog goroutine after recvLoop has finished.
//
// The watchdog (endpointing_source=engine only) acts as a safety timeout: if the
// plugin does not send responses within EngineResponseTimeoutSec, the watchdog
// calls cancel() itself, which unblocks Recv and causes recvLoop to exit early.
//
// Close is idempotent.
func (e *streamingDecodeEngine) Close() error {
	e.closeOnce.Do(func() {
		e.client.Close() // half-close send: signals plugin we are done
		<-e.recvDone     // wait for recvLoop to drain plugin responses
		e.cancel()       // stop the watchdog goroutine

		if e.span != nil {
			e.span.End()
		}

		reason, _ := e.closeReason.Load().(string)
		if reason == "" {
			reason = "client_disconnect"
		}
		e.obs.RecordStreamingSessionClose(e.engineName, reason)
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
				e.logger.Warn("streaming engine recv error", "error", err)
				e.storeTerminalErr(sttErrors.New(sttErrors.ErrStreamingEndpointLost,
					fmt.Sprintf("streaming plugin recv error: %s", err)))
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
				// Record finalize latency before clearing the sentinel.
				if sentAt := e.finalizeAt.Load(); sentAt != 0 {
					latSec := float64(now-sentAt) / float64(time.Second)
					e.obs.RecordStreamingFinalizeLatency(latSec, e.engineName)
				}
				e.finalizeAt.Store(0) // finalize fulfilled; clear pending sentinel

				// Close the per-utterance span opened in OnUtteranceEnd.
				e.utteranceMu.Lock()
				uSpan := e.utteranceSpan
				e.utteranceSpan = nil
				e.utteranceMu.Unlock()
				if uSpan != nil {
					uSpan.SetAttributes(attribute.Float64("audio.duration_sec",
						float64(h.GetEndSec()-h.GetStartSec())))
					uSpan.End()
				}
			} else {
				// Non-final hypothesis: record partial latency via ring lookup.
				if latSec, ok := e.partialLatencySec(h.GetEndSec(), time.Unix(0, now)); ok {
					e.obs.RecordStreamingPartialLatency(latSec, e.engineName)
				}
			}

			// Check finalize timeout (endpointing_source=core path).
			// Fired when a hypothesis arrives while FINALIZE_UTTERANCE is pending but
			// is_final has not yet come within StreamingFinalizeTimeoutSec.
			if e.cfg.StreamingFinalizeTimeoutSec > 0 {
				if sentAt := e.finalizeAt.Load(); sentAt != 0 {
					elapsed := time.Duration(now-sentAt) * time.Nanosecond
					if elapsed > time.Duration(float64(time.Second)*e.cfg.StreamingFinalizeTimeoutSec) {
						e.logger.Error("streaming finalize timeout",
							"elapsed_sec", elapsed.Seconds(),
							"timeout_sec", e.cfg.StreamingFinalizeTimeoutSec)
						e.closeReason.Store("finalize_timeout")
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
			e.logger.Error("streaming engine error response",
				"code", p.Error.GetCode(),
				"message", p.Error.GetMessage())
			e.closeReason.Store("engine_error")
			e.storeTerminalErr(sttErrors.New(sttErrors.ErrDecodeTaskFailed,
				fmt.Sprintf("engine error response: %s", p.Error.GetMessage())))
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
					e.logger.Error("streaming engine response timeout",
						"lag_sec", now.Sub(lastHyp).Seconds())
					e.obs.RecordEngineResponseTimeout(e.engineName)
					e.closeReason.Store("engine_response_timeout")
					e.storeTerminalErr(sttErrors.New(sttErrors.ErrEngineResponseTimeout,
						"streaming engine response timeout: no hypothesis received"))
					e.cancel()
					return
				}
			}

			if e.cfg.MaxUtteranceSec > 0 {
				lastFinal := time.Unix(0, e.lastFinalAt.Load())
				if now.Sub(lastFinal) > time.Duration(float64(time.Second)*e.cfg.MaxUtteranceSec) {
					e.logger.Warn("max utterance duration exceeded; forcing FINALIZE_UTTERANCE",
						"elapsed_sec", now.Sub(lastFinal).Seconds())
					// Reset before sending to prevent rapid re-firing.
					e.lastFinalAt.Store(now.UnixNano())
					if err := e.client.SendFinalize(); err != nil {
						e.logger.Error("SendFinalize after max_utterance_sec failed", "error", err)
						e.closeReason.Store("engine_error")
						e.storeTerminalErr(sttErrors.New(sttErrors.ErrStreamingEndpointLost,
							fmt.Sprintf("SendFinalize failed: %s", err)))
						e.cancel()
						return
					}
				}
			}
		}
	}
}

// partialLatencySec searches the ring buffer backward for the most-recent entry
// whose audioEndSec ≤ endSec and returns now.Sub(entry.sentAt).Seconds().
// Returns (0, false) when the ring is empty or endSec is zero.
// FeedFrame is single-goroutine, so no lock is needed.
func (e *streamingDecodeEngine) partialLatencySec(endSec float64, now time.Time) (float64, bool) {
	if e.audioRingIdx == 0 || endSec == 0 {
		return 0, false
	}
	count := e.audioRingIdx
	if count > 512 {
		count = 512
	}
	for i := 0; i < count; i++ {
		idx := (e.audioRingIdx - 1 - i) % 512
		entry := e.audioRing[idx]
		if entry.audioEndSec <= endSec {
			return now.Sub(entry.sentAt).Seconds(), true
		}
	}
	return 0, false
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
