package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/speechmux/core/internal/config"
	sttErrors "github.com/speechmux/core/internal/errors"
	"github.com/speechmux/core/internal/metrics"
	"github.com/speechmux/core/internal/plugin"
	"github.com/speechmux/core/internal/session"
	clientpb "github.com/speechmux/proto/gen/go/client/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
)

// StreamProcessor orchestrates the per-session audio pipeline:
//
//	AudioInCh → frameAggregator → VAD plugin (vadSendLoop, src=core only)
//	AudioInCh → AudioRingBuffer
//	AudioInCh → engine.FeedFrame (streaming engines, via vadSendLoop fanout)
//	VAD plugin responses → vadResultCh (vadRecvLoop, src=core only)
//	vadResultCh → EPDController → engine (epdLoop, src=core only)
//	engine.Results() → sess.ResultCh (forwarder goroutine)
type StreamProcessor struct {
	cfg          *atomic.Pointer[config.Config]
	vadEndpoints []*plugin.Endpoint
	vadRR        atomic.Uint64    // round-robin counter for VAD endpoint selection
	scheduler    *DecodeScheduler // nil when no inference endpoints are configured
	router       *plugin.PluginRouter // nil when no inference endpoints are configured
	obs          metrics.MetricsObserver
}

// NewStreamProcessor creates a StreamProcessor backed by the given VAD endpoint
// pool, optional decode scheduler, and optional plugin router. Each ProcessSession
// call picks the next VAD endpoint in round-robin order.
func NewStreamProcessor(
	cfg *atomic.Pointer[config.Config],
	vadEndpoints []*plugin.Endpoint,
	scheduler *DecodeScheduler,
	router *plugin.PluginRouter,
	obs metrics.MetricsObserver,
) *StreamProcessor {
	if obs == nil {
		obs = metrics.NopMetrics{}
	}
	return &StreamProcessor{
		cfg:          cfg,
		vadEndpoints: vadEndpoints,
		scheduler:    scheduler,
		router:       router,
		obs:          obs,
	}
}

// selectVADEndpoint returns the next VAD endpoint in round-robin order.
// Returns nil when no endpoints are configured.
func (p *StreamProcessor) selectVADEndpoint() *plugin.Endpoint {
	n := len(p.vadEndpoints)
	if n == 0 {
		return nil
	}
	idx := int(p.vadRR.Add(1)-1) % n
	return p.vadEndpoints[idx]
}

// ProcessSession runs the full audio pipeline for a session until ctx is
// cancelled or a fatal error occurs.
func (p *StreamProcessor) ProcessSession(ctx context.Context, sess *session.Session) error {
	ctx, span := otel.Tracer("speechmux/core/stream").Start(ctx, "session.pipeline")
	span.SetAttributes(
		attribute.String("session.id", sess.ID),
		attribute.String("session.language", sess.Info.Language),
		attribute.String("session.task", sess.Info.Task),
	)
	defer span.End()

	cfg := p.cfg.Load()
	sampleRate := int32(cfg.Codec.TargetSampleRate)
	if sampleRate == 0 {
		sampleRate = 16000
	}
	realtime := sess.Info.StreamMode == clientpb.StreamMode_STREAM_MODE_REALTIME

	buf := NewAudioRingBuffer(cfg.Stream.MaxBufferSec)

	// ── Capability dispatch ────────────────────────────────────────────────────
	// Determine engine type (batch vs streaming) from plugin capabilities.
	// For streaming: pin endpoint, acquire slot, open TranscribeStream.
	var (
		engine DecodeEngine
		src    = endpointingCore // default for batch path
	)

	if p.router != nil {
		routedClient, routeErr := p.router.Route()
		if routeErr == nil {
			caps := routedClient.Capabilities()

			// Validate endpointing_source vs engine capability upfront.
			var capErr error
			src, capErr = resolveEndpointingSource(cfg.Stream.EndpointingSource, caps)
			if capErr != nil {
				return capErr.(*sttErrors.STTError).ToGRPC()
			}

			if caps.GetStreamingMode() == inferencepb.StreamingMode_STREAMING_MODE_NATIVE {
				// Pin the endpoint for the session lifetime. We already have caps from
				// Route so we accept the small race — capability mismatch is a deployment
				// error, not a runtime one.
				pinnedClient, pinErr := p.router.PinByHint(sess.ID, sess.Info.EngineHint)
				if pinErr != nil {
					return sttErrors.New(sttErrors.ErrAllPluginsUnavailable, pinErr.Error()).ToGRPC()
				}
				defer p.router.Unpin(sess.ID)

				// Acquire a streaming session slot (blocks if at capacity).
				release, slotErr := p.scheduler.AcquireStreamingSlot(ctx)
				if slotErr != nil {
					return slotErr.(*sttErrors.STTError).ToGRPC()
				}
				defer release()

				streamCfg := &inferencepb.StreamStartConfig{
					SessionId:    sess.ID,
					SampleRate:   sampleRate,
					LanguageCode: sess.Info.Language,
				}
				streamClient, err := plugin.NewInferenceStreamClient(ctx, pinnedClient.Endpoint(), streamCfg)
				if err != nil {
					return sttErrors.New(sttErrors.ErrStreamingEndpointLost, err.Error()).ToGRPC()
				}
				// streamClient is closed via engine.Close() in the defer below.

				engineName := pinnedClient.Capabilities().GetEngineName()
				if engineName == "" {
					engineName = cfg.Stream.EndpointingSource
				}
				engine = newStreamingDecodeEngine(streamClient, src, cfg.Stream, p.obs, engineName)
			}
		}
	}

	if engine == nil {
		// Batch path (no router, no healthy endpoint, or BATCH_ONLY engine).
		engine = newBatchDecodeEngine(p.scheduler, buf)
		src = endpointingCore
	}

	// ── VAD initialization (src=core only) ────────────────────────────────────
	var vadClient *plugin.VADClient
	var vadResultCh chan VADFrame

	if src == endpointingCore {
		var err error
		vadClient, err = plugin.NewVADClient(ctx, p.selectVADEndpoint(), sess.ID, sess.Info.VADThreshold, sampleRate)
		if err != nil {
			slog.Error("VAD stream open failed", "session_id", sess.ID, "error", err)
			return sttErrors.New(sttErrors.ErrVADStreamConnectFailed, err.Error()).ToGRPC()
		}
		defer vadClient.Close()
		vadResultCh = make(chan VADFrame, 64)
	}

	engineCfg := SessionDecodeConfig{
		SampleRate:   sampleRate,
		LanguageCode: sess.Info.Language,
		Realtime:     realtime,
		Stream:       cfg.Stream,
	}
	if err := engine.Start(ctx, sess, engineCfg); err != nil {
		return fmt.Errorf("engine start: %w", err)
	}

	g, gCtx := errgroup.WithContext(ctx)

	// Close order: engine.Close is deferred BEFORE the ResultCh close so that
	// the engine fully drains and closes Results() before the forwarder exits.
	// The forwarder is in the errgroup, so g.Wait() returns only after the
	// forwarder has seen Results() close. After g.Wait(), engine.Close runs
	// (idempotent; already closed on the normal path via EPD goroutine or
	// vadSendLoop end-of-stream for src=engine).
	defer func() {
		_ = engine.Close()
		close(sess.ResultCh)
		sess.MarkProcessingDone()
	}()

	// Goroutine 1: VAD send loop — forwards audio to VAD (src=core) and/or
	// engine.FeedFrame (streaming engines). For src=engine, also signals
	// engine.Close() when audio ends to unblock the results forwarder.
	g.Go(func() error {
		return p.vadSendLoop(gCtx, sess, buf, vadClient, engine, src, cfg, sampleRate)
	})

	// Goroutines 2 + 3: VAD recv + EPD — only when src=core.
	if src == endpointingCore {
		g.Go(func() error {
			defer close(vadResultCh)
			return p.vadRecvLoop(gCtx, vadClient, vadResultCh)
		})

		g.Go(func() error {
			defer func() { _ = engine.Close() }()
			optFrameMs := 30 // matches vadSendLoop default
			epd := NewEPDController(
				sess.Info.VADSilence,
				cfg.Stream.SpeechRMSThreshold,
				cfg.Stream.VADFrameTimeoutSec,
				cfg.Stream.EPDHeartbeatIntervalSec,
				cfg.Stream.VADWatermarkLagThresholdSec,
				float64(optFrameMs),
				!realtime, // terminateOnLag: true for BATCH, false for REALTIME
			)
			epd.SetSpeechStartCallback(func(startSeq uint64) {
				p.obs.RecordVADTrigger()
				engine.OnSpeechStart(startSeq)
			})
			epd.SetSpeechEndCallback(func() {
				engine.OnSpeechEnd()
			})
			epd.SetWatermarkLagCallback(func(lagSec float64) {
				slog.Warn("VAD watermark lag exceeded threshold",
					"session_id", sess.ID,
					"lag_sec", lagSec,
					"threshold_sec", cfg.Stream.VADWatermarkLagThresholdSec,
					"realtime", realtime,
				)
				p.obs.RecordVADWatermarkLag()
			})
			return epd.Run(gCtx, vadResultCh, buf, func(audio []byte, startSeq, endSeq uint64) {
				if err := engine.OnUtteranceEnd(startSeq, endSeq); err != nil {
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						slog.Warn("engine OnUtteranceEnd failed",
							"session_id", sess.ID, "error", err)
					}
				}
			})
		})
	}

	// Goroutine 4: results forwarder — engine.Results() → sess.ResultCh.
	g.Go(func() error {
		for {
			select {
			case res, ok := <-engine.Results():
				if !ok {
					// Surface terminal error from streaming engine (lag watchdog / finalize timeout).
					if sde, isSDE := engine.(*streamingDecodeEngine); isSDE {
						if terr := sde.TerminalErr(); terr != nil {
							return terr
						}
					}
					return nil
				}
				out := &clientpb.RecognitionResult{
					IsFinal:       res.IsFinal,
					Text:          res.Text,
					CommittedText: res.CommittedText,
					UnstableText:  res.UnstableText,
					AudioDuration: res.AudioDuration,
					LanguageCode:  res.LanguageCode,
					StartSec:      res.StartSec,
					EndSec:        res.EndSec,
				}
				select {
				case sess.ResultCh <- out:
				case <-gCtx.Done():
					return gCtx.Err()
				}
			case <-gCtx.Done():
				return gCtx.Err()
			}
		}
	})

	// Trim ticker: use TrimByAge when VAD is not running (watermark never advances).
	trimCtx, cancelTrim := context.WithCancel(ctx)
	defer cancelTrim()
	trimFn := buf.Trim
	if src == endpointingEngine {
		maxSec := cfg.Stream.MaxBufferSec
		trimFn = func() { buf.TrimByAge(maxSec) }
	}
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				trimFn()
			case <-trimCtx.Done():
				return
			}
		}
	}()

	if err := g.Wait(); err != nil {
		if isNormalShutdown(err) {
			return nil
		}
		if errors.Is(err, ErrVADFrameTimeout) || errors.Is(err, ErrVADWatermarkLag) {
			return sttErrors.New(sttErrors.ErrVADStreamConnectFailed, err.Error()).ToGRPC()
		}
		var sttErr *sttErrors.STTError
		if errors.As(err, &sttErr) {
			return sttErr.ToGRPC()
		}
		return fmt.Errorf("stream processor: %w", err)
	}
	return nil
}

// vadSendLoop reads PCM from sess.AudioInCh, appends to the ring buffer, and
// sends aggregated frames to the VAD plugin (src=core) and/or engine (streaming).
//
// Backpressure:
//   - BATCH mode: spins (with ctx) when the ring buffer is full.
//   - REALTIME mode: drops the oldest entry to make room instead of blocking.
//
// End-of-stream:
//   - src=core: calls vadClient.Close(), which triggers vadRecvLoop → EPD → engine.Close().
//   - src=engine: calls engine.Close() directly so recvLoop drains and resultsCh closes,
//     which unblocks the results forwarder goroutine.
func (p *StreamProcessor) vadSendLoop(
	ctx context.Context,
	sess *session.Session,
	buf *AudioRingBuffer,
	vadClient *plugin.VADClient, // nil when src=engine
	engine DecodeEngine,
	src endpointingSource,
	cfg *config.Config,
	sampleRate int32,
) error {
	optFrameMs := 30 // default; updated by GetCapabilities in a future phase
	agg := NewFrameAggregator(optFrameMs, int(sampleRate))
	var seq uint64
	realtime := sess.Info.StreamMode == clientpb.StreamMode_STREAM_MODE_REALTIME

	sendFrame := func(frame []byte) error {
		seq++
		for !buf.Append(seq, frame) {
			if realtime {
				buf.DropOldest()
				continue
			}
			// BATCH: wait briefly for Trim to free a slot, then retry.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Millisecond):
			}
		}

		// Send to VAD plugin only when src=core.
		if src == endpointingCore && vadClient != nil {
			if _, err := vadClient.Send(frame, sampleRate); err != nil {
				return fmt.Errorf("VAD send: %w", err)
			}
		}

		// Feed streaming engine (no-op for batchDecodeEngine.FeedFrame).
		if err := engine.FeedFrame(seq, frame); err != nil {
			if realtime {
				slog.Warn("streaming engine FeedFrame failed (realtime drop)",
					"session_id", sess.ID, "error", err)
				return nil
			}
			return fmt.Errorf("engine FeedFrame: %w", err)
		}
		return nil
	}

	for {
		select {
		case pcm, ok := <-sess.AudioInCh:
			if !ok {
				// Channel closed — flush any remaining partial frame then signal
				// end-of-audio to the appropriate downstream.
				if tail := agg.Flush(); tail != nil {
					if err := sendFrame(tail); err != nil && !isNormalShutdown(err) {
						return err
					}
				}
				if src == endpointingCore && vadClient != nil {
					vadClient.Close()
				} else if src == endpointingEngine {
					// Half-close the stream; recvLoop drains and closes resultsCh.
					_ = engine.Close()
				}
				return nil
			}
			for _, frame := range agg.Push(pcm) {
				if err := sendFrame(frame); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// vadRecvLoop reads VAD responses from the plugin and forwards them to vadResultCh.
func (p *StreamProcessor) vadRecvLoop(
	ctx context.Context,
	vadClient *plugin.VADClient,
	vadResultCh chan<- VADFrame,
) error {
	for {
		resp, err := vadClient.Recv()
		if err != nil {
			if isNormalShutdown(err) || isStreamEnd(err) {
				return nil
			}
			return fmt.Errorf("VAD recv: %w", err)
		}
		select {
		case vadResultCh <- VADFrame{
			SequenceNumber:    resp.SequenceNumber,
			IsSpeech:          resp.IsSpeech,
			SpeechProbability: resp.SpeechProbability,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// isNormalShutdown returns true for errors that indicate a clean context
// cancellation or deadline expiry.
func isNormalShutdown(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// isStreamEnd returns true for errors that signal the remote side closed
// the stream (io.EOF) or the gRPC layer cancelled the RPC.
func isStreamEnd(err error) bool {
	if errors.Is(err, io.EOF) {
		return true
	}
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.Canceled, codes.DeadlineExceeded:
			return true
		}
	}
	return false
}
