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
)

// StreamProcessor orchestrates the per-session audio pipeline:
//
//	AudioInCh → frameAggregator → VAD plugin (vadSendLoop)
//	AudioInCh → AudioRingBuffer
//	VAD plugin responses → vadResultCh (vadRecvLoop)
//	vadResultCh → EPDController → engine (epdLoop)
//	engine.Results() → sess.ResultCh (forwarder goroutine)
type StreamProcessor struct {
	cfg          *atomic.Pointer[config.Config]
	vadEndpoints []*plugin.Endpoint
	vadRR        atomic.Uint64    // round-robin counter for VAD endpoint selection
	scheduler    *DecodeScheduler // nil when no inference endpoints are configured
	obs          metrics.MetricsObserver
}

// NewStreamProcessor creates a StreamProcessor backed by the given VAD endpoint
// pool and optional decode scheduler. Each ProcessSession call picks the next
// endpoint in round-robin order.
func NewStreamProcessor(
	cfg *atomic.Pointer[config.Config],
	vadEndpoints []*plugin.Endpoint,
	scheduler *DecodeScheduler,
	obs metrics.MetricsObserver,
) *StreamProcessor {
	if obs == nil {
		obs = metrics.NopMetrics{}
	}
	return &StreamProcessor{cfg: cfg, vadEndpoints: vadEndpoints, scheduler: scheduler, obs: obs}
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

	vadClient, err := plugin.NewVADClient(ctx, p.selectVADEndpoint(), sess.ID, sess.Info.VADThreshold, sampleRate)
	if err != nil {
		slog.Error("VAD stream open failed", "session_id", sess.ID, "error", err)
		return sttErrors.New(sttErrors.ErrVADStreamConnectFailed, err.Error()).ToGRPC()
	}
	defer vadClient.Close()

	buf := NewAudioRingBuffer(cfg.Stream.MaxBufferSec)
	vadResultCh := make(chan VADFrame, 64)

	// Engine construction. Phase A introduces only batchDecodeEngine.
	realtime := sess.Info.StreamMode == clientpb.StreamMode_STREAM_MODE_REALTIME
	engine := newBatchDecodeEngine(p.scheduler, buf)
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
	// (idempotent; already closed on the normal path).
	defer func() {
		_ = engine.Close()
		close(sess.ResultCh)
		sess.MarkProcessingDone()
	}()

	// Goroutine 1: VAD send loop. Unchanged.
	g.Go(func() error {
		return p.vadSendLoop(gCtx, sess, buf, vadClient, cfg, sampleRate)
	})

	// Goroutine 2: VAD recv loop. Unchanged.
	g.Go(func() error {
		defer close(vadResultCh)
		return p.vadRecvLoop(gCtx, vadClient, vadResultCh)
	})

	// Goroutine 3: EPD controller. Callbacks now call engine methods directly
	// instead of writing to channels. Defers engine.Close() so that on any
	// exit (clean or error) the results channel is closed, which unblocks
	// the forwarder goroutine and allows g.Wait() to return.
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
			// EPD guarantees its own goroutine serialization. Ignore errors
			// from OnUtteranceEnd other than ctx cancellation (the engine
			// already logs failures internally).
			if err := engine.OnUtteranceEnd(startSeq, endSeq); err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					slog.Warn("engine OnUtteranceEnd failed",
						"session_id", sess.ID, "error", err)
				}
			}
		})
	})

	// Goroutine 4: results forwarder — engine.Results() → sess.ResultCh.
	// Replaces the old goroutine-5 direct write from decodeWorkerLoop.
	g.Go(func() error {
		for {
			select {
			case res, ok := <-engine.Results():
				if !ok {
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

	// Trim ticker (formerly goroutine 6). Unchanged.
	trimCtx, cancelTrim := context.WithCancel(ctx)
	defer cancelTrim()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				buf.Trim()
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
		return fmt.Errorf("stream processor: %w", err)
	}
	return nil
}

// vadSendLoop reads PCM from sess.AudioInCh, appends to the ring buffer, and
// sends aggregated frames to the VAD plugin.
//
// Backpressure:
//   - BATCH mode: spins (with ctx) when the ring buffer is full. AudioInCh
//     will fill up, blocking stream.Recv() in the gRPC handler and propagating
//     HTTP/2 flow control back to the client.
//   - REALTIME mode: drops the oldest entry to make room instead of blocking.
func (p *StreamProcessor) vadSendLoop(
	ctx context.Context,
	sess *session.Session,
	buf *AudioRingBuffer,
	vadClient *plugin.VADClient,
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
				// Drop the oldest entry to make room, then retry Append on the
				// next iteration.  Without the retry the current frame would be
				// sent to VAD (for probability) but never stored in the ring
				// buffer, creating a gap that EPD cannot fill during extraction.
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
		if _, err := vadClient.Send(frame, sampleRate); err != nil {
			return fmt.Errorf("VAD send: %w", err)
		}
		return nil
	}

	for {
		select {
		case pcm, ok := <-sess.AudioInCh:
			if !ok {
				// Channel closed — flush any remaining partial frame then signal
				// the VAD plugin that this session's audio stream has ended.
				if tail := agg.Flush(); tail != nil {
					if err := sendFrame(tail); err != nil && !isNormalShutdown(err) {
						return err
					}
				}
				vadClient.Close()
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
