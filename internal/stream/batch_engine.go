package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	sttErrors "github.com/speechmux/core/internal/errors"
	"github.com/speechmux/core/internal/session"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
)

// decodeTask is an audio segment queued for inference.
// Moved from processor.go (was lines 27-35 there).
type decodeTask struct {
	audio     []byte
	isFinal   bool
	isPartial bool
	// Session-relative timestamps derived from ring-buffer sequence numbers.
	startSec float64
	endSec   float64
}

// batchFrameMs is the VAD frame aggregation period used by vadSendLoop. It is
// also the sequence-to-seconds conversion factor for the ring buffer sequence
// numbers that reach this engine via OnUtteranceEnd.
// Must match vadSendLoop's optFrameMs (processor.go).
const batchFrameMs = 30

// batchDecodeEngine implements DecodeEngine for Whisper-family unary plugins.
// It owns an internal decode queue, a partial decode timer goroutine, a decode
// worker goroutine, and a ResultAssembler for committed/unstable tracking.
type batchDecodeEngine struct {
	scheduler *DecodeScheduler
	buf       *AudioRingBuffer

	// Populated at Start.
	sess *session.Session
	cfg  SessionDecodeConfig

	// Lifetime.
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once

	// Internal channels — created in Start, owned by this engine.
	decodeQueueCh chan decodeTask
	speechStartCh chan uint64
	speechEndCh   chan struct{}
	resultsCh     chan DecodeResult

	// timerStopCh is closed to stop the partial timer without cancelling e.ctx.
	// This allows in-flight Submit calls in the worker to complete normally when
	// the engine is closed gracefully (e.g. after the VAD stream ends).
	timerStopCh chan struct{}

	// Goroutine completion signals.
	timerDone  chan struct{}
	workerDone chan struct{}

	// Monotonic request-id counter (only accessed by runDecodeWorker).
	reqSeq uint64
}

// newBatchDecodeEngine constructs an unstarted batchDecodeEngine.
// The scheduler may be nil, in which case the worker drains its queue and
// logs, matching the pre-refactor behaviour (processor.go:359-365).
func newBatchDecodeEngine(scheduler *DecodeScheduler, buf *AudioRingBuffer) *batchDecodeEngine {
	return &batchDecodeEngine{scheduler: scheduler, buf: buf}
}

// Start implements DecodeEngine.
func (e *batchDecodeEngine) Start(ctx context.Context, sess *session.Session, cfg SessionDecodeConfig) error {
	e.sess = sess
	e.cfg = cfg
	e.ctx, e.cancel = context.WithCancel(ctx)

	// Channel buffer sizes preserved from processor.go (lines 107-109).
	e.decodeQueueCh = make(chan decodeTask, 4)
	e.speechStartCh = make(chan uint64, 1)
	e.speechEndCh = make(chan struct{}, 1)
	e.resultsCh = make(chan DecodeResult, 8)
	e.timerStopCh = make(chan struct{})

	e.timerDone = make(chan struct{})
	e.workerDone = make(chan struct{})

	go func() {
		defer close(e.timerDone)
		e.runPartialTimer()
	}()

	go func() {
		defer close(e.workerDone)
		e.runDecodeWorker()
	}()

	return nil
}

// FeedFrame implements DecodeEngine. Batch engines do not consume audio per
// frame; the ring buffer (populated by the caller) is the source of truth.
func (e *batchDecodeEngine) FeedFrame(_ uint64, _ []byte) error {
	return nil
}

// OnSpeechStart implements DecodeEngine. Non-blocking; if the previous start
// has not yet been consumed, the new seq replaces it (preserves processor.go
// line 149-152 behaviour).
func (e *batchDecodeEngine) OnSpeechStart(startSeq uint64) {
	select {
	case e.speechStartCh <- startSeq:
	default:
		// Drain the stale value and try once more.
		select {
		case <-e.speechStartCh:
		default:
		}
		select {
		case e.speechStartCh <- startSeq:
		default:
		}
	}
}

// OnSpeechEnd implements DecodeEngine. Non-blocking.
// Preserves processor.go lines 154-158 behaviour.
func (e *batchDecodeEngine) OnSpeechEnd() {
	select {
	case e.speechEndCh <- struct{}{}:
	default:
	}
}

// OnUtteranceEnd implements DecodeEngine. Extracts [startSeq, endSeq] audio
// from the ring buffer and enqueues a final decode task.
func (e *batchDecodeEngine) OnUtteranceEnd(startSeq, endSeq uint64) error {
	audio := e.buf.ExtractRange(startSeq, endSeq)
	task := decodeTask{
		audio:    audio,
		isFinal:  true,
		startSec: seqToSec(startSeq),
		endSec:   seqToSec(endSeq),
	}
	select {
	case e.decodeQueueCh <- task:
		return nil
	case <-e.ctx.Done():
		return e.ctx.Err()
	}
}

// Results implements DecodeEngine.
func (e *batchDecodeEngine) Results() <-chan DecodeResult { return e.resultsCh }

// Close implements DecodeEngine. Shutdown order is critical:
//  1. Close timerStopCh — partial timer exits without cancelling e.ctx so that
//     the worker's in-flight Submit calls can still complete normally.
//  2. Wait for the partial timer to exit — no more writes to decodeQueueCh.
//  3. Close decodeQueueCh — the worker's range loop drains remaining items
//     (including any final task enqueued just before Close) and exits.
//  4. Wait for the worker to exit.
//  5. Close resultsCh — signals the forwarder goroutine that all results are done.
//  6. Call e.cancel() to release the derived context.
//
// This ordering is required to avoid "send on closed channel" panics and to
// ensure that a final decode enqueued by OnUtteranceEnd is not lost.
func (e *batchDecodeEngine) Close() error {
	e.closeOnce.Do(func() {
		close(e.timerStopCh)
		<-e.timerDone
		close(e.decodeQueueCh)
		<-e.workerDone
		close(e.resultsCh)
		e.cancel()
	})
	return nil
}

// seqToSec converts a ring buffer sequence number to session-relative seconds.
func seqToSec(seq uint64) float64 {
	return float64(seq) * float64(batchFrameMs) / 1000.0
}

// ── runPartialTimer ──────────────────────────────────────────────────────────
// Adaptive partial decode timer. Moved verbatim from
// StreamProcessor.partialDecodeTimerLoop (processor.go:235-345).
// The only substitutions are:
//
//	ctx            → e.ctx
//	buf            → e.buf
//	speechStartCh  → e.speechStartCh
//	speechEndCh    → e.speechEndCh
//	decodeQueueCh  → e.decodeQueueCh
//	cfg            → &e.cfg.Stream    (pointer to embedded StreamConfig)
//	sampleRate     → int(e.cfg.SampleRate)
//
// Adaptive interval (design doc §10.12):
//
//	audio < 5 s  → partial_decode_interval_sec (default 1.5 s)
//	audio 5–10 s → 3 s
//	audio > 10 s → 5 s
func (e *batchDecodeEngine) runPartialTimer() {
	var speechStartSeq uint64
	var inSpeech bool
	var partialTimer *time.Timer
	var partialTimerC <-chan time.Time

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
		d := e.cfg.Stream.PartialDecodeIntervalSec
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

	sampleRate := int(e.cfg.SampleRate)

	for {
		select {
		case startSeq, ok := <-e.speechStartCh:
			if !ok {
				return
			}
			speechStartSeq = startSeq
			inSpeech = true
			stopT()
			partialTimer = time.NewTimer(initialInterval())
			partialTimerC = partialTimer.C

		case _, ok := <-e.speechEndCh:
			if !ok {
				return
			}
			inSpeech = false
			stopT()

		case <-partialTimerC:
			if !inSpeech {
				partialTimerC = nil
				break
			}
			curSeq := e.buf.ConfirmedWatermark()
			audio := e.buf.ExtractRange(speechStartSeq, curSeq)

			maxPartialSec := e.cfg.Stream.PartialDecodeWindowSec
			if maxPartialSec <= 0 {
				maxPartialSec = 10
			}
			maxBytes := int(maxPartialSec*float64(sampleRate)) * 2 // S16LE
			if maxBytes > 0 && len(audio) > maxBytes {
				audio = audio[len(audio)-maxBytes:]
			}

			if len(audio) > 0 {
				select {
				case e.decodeQueueCh <- decodeTask{
					audio:     audio,
					isFinal:   false,
					isPartial: true,
					startSec:  seqToSec(speechStartSeq),
					endSec:    seqToSec(curSeq),
				}:
				default:
					// Drop partial if queue is full — correct behaviour.
				}
			}

			audioDurSec := float64(len(audio)/2) / float64(sampleRate)
			partialTimer = time.NewTimer(adaptiveInterval(audioDurSec))
			partialTimerC = partialTimer.C

		case <-e.timerStopCh:
			stopT()
			return

		case <-e.ctx.Done():
			stopT()
			return
		}
	}
}

// ── runDecodeWorker ──────────────────────────────────────────────────────────
// Drains decodeQueueCh, calls the scheduler, runs the ResultAssembler, and
// emits DecodeResult on resultsCh. Moved from StreamProcessor.decodeWorkerLoop
// (processor.go:347-429). The key difference: instead of building a
// clientpb.RecognitionResult and writing to sess.ResultCh, this writes a
// DecodeResult to e.resultsCh. The caller (StreamProcessor) converts.
func (e *batchDecodeEngine) runDecodeWorker() {
	if e.scheduler == nil {
		for range e.decodeQueueCh {
			slog.Debug("decode worker: no scheduler, dropping task", "session_id", e.sess.ID)
		}
		return
	}

	assembler := NewResultAssembler()

	for task := range e.decodeQueueCh {
		e.reqSeq++
		reqID := fmt.Sprintf("%s-r%d", e.sess.ID, e.reqSeq)

		resp, err := e.scheduler.Submit(
			e.ctx,
			e.sess.ID,
			reqID,
			task.audio,
			e.cfg.SampleRate,
			e.cfg.LanguageCode,
			inferencepb.Task_TASK_TRANSCRIBE,
			nil, // DecodeProfile → DecodeOptions handled in a future phase
			task.isFinal,
			task.isPartial,
		)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			var sttErr *sttErrors.STTError
			if errors.As(err, &sttErr) &&
				sttErr.Code() == sttErrors.ErrGlobalPendingExceeded && !task.isFinal {
				slog.Debug("partial decode dropped (queue full)", "session_id", e.sess.ID)
				continue
			}
			slog.Warn("decode failed",
				"session_id", e.sess.ID, "is_final", task.isFinal, "error", err)
			if task.isFinal {
				assembler.Reset()
			}
			continue
		}

		committed, unstable := assembler.Update(resp.Text, task.isFinal)
		if task.isFinal {
			assembler.Reset()
		}

		result := DecodeResult{
			Text:          resp.Text,
			CommittedText: committed,
			UnstableText:  unstable,
			IsFinal:       task.isFinal,
			StartSec:      task.startSec,
			EndSec:        task.endSec,
			AudioDuration: resp.AudioDurationSec,
			LanguageCode:  resp.LanguageCode,
		}

		select {
		case e.resultsCh <- result:
		case <-e.ctx.Done():
			return
		}
	}
}
