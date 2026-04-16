package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	sttErrors "github.com/speechmux/core/internal/errors"
	"github.com/speechmux/core/internal/metrics"
	"github.com/speechmux/core/internal/plugin"
	"github.com/speechmux/core/internal/session"
	"github.com/speechmux/core/internal/config"
	clientpb "github.com/speechmux/proto/gen/go/client/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
)

// decodeTask is an audio segment queued for inference.
type decodeTask struct {
	audio     []byte
	isFinal   bool
	isPartial bool
	// Session-relative timestamps derived from ring-buffer sequence numbers.
	startSec float64
	endSec   float64
}

// StreamProcessor orchestrates the per-session audio pipeline:
//
//	AudioInCh → frameAggregator → VAD plugin (vadSendLoop)
//	AudioInCh → AudioRingBuffer
//	VAD plugin responses → vadResultCh (vadRecvLoop)
//	vadResultCh → EPDController → decodeQueueCh (epdLoop)
//	partialDecodeTimerLoop → decodeQueueCh (adaptive intervals during speech)
//	decodeQueueCh → decodeWorkerLoop → sess.ResultCh
type StreamProcessor struct {
	cfg          *atomic.Pointer[config.Config]
	vadEndpoints []*plugin.Endpoint
	vadRR        atomic.Uint64 // round-robin counter for VAD endpoint selection
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
	// Root span for the entire session pipeline. All stt.decode child spans are
	// parented here via context propagation through the errgroup ctx.
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
	decodeQueueCh := make(chan decodeTask, 4)
	speechStartCh := make(chan uint64, 1)
	speechEndCh := make(chan struct{}, 1)

	g, gCtx := errgroup.WithContext(ctx)

	// Close ResultCh and signal processing-done once all pipeline goroutines exit.
	// This runs after g.Wait() completes because defers fire at function return.
	defer func() {
		close(sess.ResultCh)
		sess.MarkProcessingDone()
	}()

	// Goroutine 1: receive audio, buffer it, aggregate and forward to VAD.
	g.Go(func() error {
		return p.vadSendLoop(gCtx, sess, buf, vadClient, cfg, sampleRate)
	})

	// Goroutine 2: receive VAD responses and forward to EPD controller.
	g.Go(func() error {
		defer close(vadResultCh)
		return p.vadRecvLoop(gCtx, vadClient, vadResultCh)
	})

	// Goroutine 3: EPD — silence detection and utterance extraction.
	g.Go(func() error {
		defer close(decodeQueueCh)
		defer close(speechEndCh)
		// Each sequence number = one VAD frame = optFrameMs milliseconds.
		optFrameMs := 30 // matches vadSendLoop default
		realtime := sess.Info.StreamMode == clientpb.StreamMode_STREAM_MODE_REALTIME
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
			select {
			case speechStartCh <- startSeq:
			default: // overwrite with newer seq; previous partial timer will pick it up
			}
		})
		epd.SetSpeechEndCallback(func() {
			select {
			case speechEndCh <- struct{}{}:
			default:
			}
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
		seqToSec := func(seq uint64) float64 {
			return float64(seq) * float64(optFrameMs) / 1000.0
		}
		return epd.Run(gCtx, vadResultCh, buf, func(audio []byte, startSeq, endSeq uint64) {
			// Submit final decode (non-blocking enqueue; context guards the select).
			select {
			case decodeQueueCh <- decodeTask{
				audio:    audio,
				isFinal:  true,
				startSec: seqToSec(startSeq),
				endSec:   seqToSec(endSeq),
			}:
			case <-gCtx.Done():
			}
		})
	})

	// Goroutine 4: adaptive partial decode timer (fires during speech).
	g.Go(func() error {
		return p.partialDecodeTimerLoop(gCtx, buf, speechStartCh, speechEndCh, decodeQueueCh, cfg, int(sampleRate))
	})

	// Goroutine 5: decode worker — runs inference and sends results to client.
	g.Go(func() error {
		return p.decodeWorkerLoop(gCtx, sess, decodeQueueCh, sampleRate, cfg)
	})

	// Goroutine 6: periodic Trim of confirmed, aged-out ring buffer entries.
	// Runs outside the errgroup so that it does not block g.Wait() after the
	// pipeline chain (goroutines 1–5) has terminated naturally.
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

// partialDecodeTimerLoop fires partial decode requests at adaptive intervals
// while speech is active. It enqueues audio into decodeQueueCh without blocking
// (drops if queue is full, which is correct for partials).
//
// Adaptive interval (design doc §10.12):
//
//	audio < 5 s  → partial_decode_interval_sec (default 1.5 s)
//	audio 5–10 s → 3 s
//	audio > 10 s → 5 s
func (p *StreamProcessor) partialDecodeTimerLoop(
	ctx context.Context,
	buf *AudioRingBuffer,
	speechStartCh <-chan uint64,
	speechEndCh <-chan struct{},
	decodeQueueCh chan<- decodeTask,
	cfg *config.Config,
	sampleRate int,
) error {
	var speechStartSeq uint64
	var inSpeech bool
	var partialTimer *time.Timer
	var partialTimerC <-chan time.Time // nil channel when no timer

	stopT := func() {
		if partialTimer != nil {
			if !partialTimer.Stop() {
				select {
				case <-partialTimer.C:
				default:
				}
			}
			partialTimer = nil
			partialTimerC = nil
		}
	}

	initialInterval := func() time.Duration {
		d := cfg.Stream.PartialDecodeIntervalSec
		if d <= 0 {
			d = 1.5
		}
		return time.Duration(d * float64(time.Second))
	}

	adaptiveInterval := func(audioDurSec float64) time.Duration {
		switch {
		case audioDurSec < 5:
			return initialInterval()
		case audioDurSec < 10:
			return 3 * time.Second
		default:
			return 5 * time.Second
		}
	}

	for {
		select {
		case startSeq, ok := <-speechStartCh:
			if !ok {
				return nil
			}
			speechStartSeq = startSeq
			inSpeech = true
			stopT()
			partialTimer = time.NewTimer(initialInterval())
			partialTimerC = partialTimer.C

		case _, ok := <-speechEndCh:
			if !ok {
				return nil
			}
			inSpeech = false
			stopT()

		case <-partialTimerC:
			if !inSpeech {
				partialTimerC = nil
				break
			}
			curSeq := buf.ConfirmedWatermark()
			audio := buf.ExtractRange(speechStartSeq, curSeq)

			// Cap audio to max_partial_audio_sec (design doc §10.12).
			maxPartialSec := cfg.Stream.PartialDecodeWindowSec
			if maxPartialSec <= 0 {
				maxPartialSec = 10
			}
			maxBytes := int(maxPartialSec*float64(sampleRate)) * 2 // S16LE
			if maxBytes > 0 && len(audio) > maxBytes {
				audio = audio[len(audio)-maxBytes:]
			}

			if len(audio) > 0 {
				// Each sequence = one VAD frame = 30ms.
				seqToSec := func(seq uint64) float64 {
					return float64(seq) * 30.0 / 1000.0
				}
				select {
				case decodeQueueCh <- decodeTask{
					audio:    audio,
					isFinal:  false,
					isPartial: true,
					startSec: seqToSec(speechStartSeq),
					endSec:   seqToSec(curSeq),
				}:
				default:
					// Drop partial if queue is full — correct behaviour.
				}
			}

			audioDurSec := float64(len(audio)/2) / float64(sampleRate)
			partialTimer = time.NewTimer(adaptiveInterval(audioDurSec))
			partialTimerC = partialTimer.C

		case <-ctx.Done():
			stopT()
			return ctx.Err()
		}
	}
}

// decodeWorkerLoop reads decode tasks from decodeQueueCh, calls the inference
// plugin via DecodeScheduler, updates the ResultAssembler, and writes
// RecognitionResult messages to sess.ResultCh.
//
// The assembler is owned exclusively by this goroutine; no mutex is needed.
func (p *StreamProcessor) decodeWorkerLoop(
	ctx context.Context,
	sess *session.Session,
	decodeQueueCh <-chan decodeTask,
	sampleRate int32,
	cfg *config.Config,
) error {
	if p.scheduler == nil {
		// No inference endpoints configured — drain the queue and log.
		for range decodeQueueCh {
			slog.Debug("decode worker: no scheduler, dropping task", "session_id", sess.ID)
		}
		return nil
	}

	assembler := NewResultAssembler()
	var reqSeq uint64

	for task := range decodeQueueCh {
		reqSeq++
		reqID := fmt.Sprintf("%s-r%d", sess.ID, reqSeq)

		resp, err := p.scheduler.Submit(
			ctx,
			sess.ID,
			reqID,
			task.audio,
			sampleRate,
			sess.Info.Language,
			inferencepb.Task_TASK_TRANSCRIBE,
			nil, // TODO: DecodeProfile → DecodeOptions in a future phase
			task.isFinal,
			task.isPartial,
		)
		if err != nil {
			if isNormalShutdown(err) {
				return nil
			}
			var sttErr *sttErrors.STTError
			if errors.As(err, &sttErr) {
				// ErrGlobalPendingExceeded on a partial is a normal drop — not fatal.
				if sttErr.Code() == sttErrors.ErrGlobalPendingExceeded && !task.isFinal {
					slog.Debug("partial decode dropped (queue full)", "session_id", sess.ID)
					continue
				}
			}
			slog.Warn("decode failed", "session_id", sess.ID, "is_final", task.isFinal, "error", err)
			if task.isFinal {
				// Non-fatal: reset assembler and continue processing the session.
				assembler.Reset()
			}
			continue
		}

		committed, unstable := assembler.Update(resp.Text, task.isFinal)
		if task.isFinal {
			assembler.Reset()
		}

		result := &clientpb.RecognitionResult{
			IsFinal:       task.isFinal,
			Text:          resp.Text,
			CommittedText: committed,
			UnstableText:  unstable,
			AudioDuration: resp.AudioDurationSec,
			LanguageCode:  resp.LanguageCode,
			StartSec:      task.startSec,
			EndSec:        task.endSec,
		}

		select {
		case sess.ResultCh <- result:
		case <-ctx.Done():
			return ctx.Err()
		}
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
