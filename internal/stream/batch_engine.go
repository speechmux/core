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

// submittedTask pairs the original decodeTask metadata with the result channel
// returned by FairDecodeDispatcher.Enqueue(). runResultCollector drains these
// in submission order to preserve ResultAssembler invariants.
type submittedTask struct {
	meta     decodeTask
	resultCh <-chan BatchResult
}

// batchFrameMs is the VAD frame aggregation period used by vadSendLoop. It is
// also the sequence-to-seconds conversion factor for the ring buffer sequence
// numbers that reach this engine via OnUtteranceEnd.
// Must match vadSendLoop's optFrameMs (processor.go).
const batchFrameMs = 30

// batchDecodeEngine implements DecodeEngine for Whisper-family unary plugins.
// It owns an internal decode queue, a partial decode timer goroutine, a
// submitter goroutine (runSubmitter), a result collector goroutine
// (runResultCollector), and a ResultAssembler for committed/unstable tracking.
type batchDecodeEngine struct {
	dispatcher BatchScheduler
	buf        *AudioRingBuffer

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
	// This allows in-flight Transcribe RPCs to complete normally when the engine
	// is closed gracefully (e.g. after the VAD stream ends).
	timerStopCh chan struct{}

	// Goroutine completion signals.
	timerDone     chan struct{}
	workerDone    chan struct{} // closed when runSubmitter exits
	collectorDone chan struct{} // closed when runResultCollector exits

	// Monotonic request-id counter (only accessed by runSubmitter).
	reqSeq uint64
}

// newBatchDecodeEngine constructs an unstarted batchDecodeEngine.
// dispatcher may be nil, in which case tasks are drained and logged.
func newBatchDecodeEngine(dispatcher BatchScheduler, buf *AudioRingBuffer) *batchDecodeEngine {
	return &batchDecodeEngine{dispatcher: dispatcher, buf: buf}
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
	e.collectorDone = make(chan struct{})

	go func() {
		defer close(e.timerDone)
		e.runPartialTimer()
	}()

	pendingCh := make(chan submittedTask, 8)

	go func() {
		defer close(e.workerDone)
		e.runSubmitter(pendingCh)
	}()

	go func() {
		defer close(e.collectorDone)
		e.runResultCollector(pendingCh)
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

// Close implements DecodeEngine. Shutdown order:
//  1. dispatcher.CancelSession — cancel queued (not-yet-dispatched) tasks.
//  2. close(timerStopCh) — partial timer exits; no more writes to decodeQueueCh.
//  3. <-timerDone
//  4. close(decodeQueueCh) — runSubmitter drains remaining items and exits.
//  5. <-workerDone — runSubmitter done; pendingCh closed by its defer.
//  6. <-collectorDone — runResultCollector drained pendingCh and exited.
//     MUST wait here before closing resultsCh — otherwise "send on closed channel".
//  7. close(resultsCh)
//  8. e.cancel()
func (e *batchDecodeEngine) Close() error {
	e.closeOnce.Do(func() {
		if e.dispatcher != nil {
			e.dispatcher.CancelSession(e.sess.ID)
		}
		close(e.timerStopCh)
		<-e.timerDone
		close(e.decodeQueueCh)
		<-e.workerDone
		<-e.collectorDone
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

// ── runSubmitter ─────────────────────────────────────────────────────────────
// Drains decodeQueueCh, enqueues each task to the FairDecodeDispatcher, and
// sends the (meta, resultCh) pair to pendingCh for runResultCollector.
// Closes pendingCh when decodeQueueCh is closed (engine is done submitting).
func (e *batchDecodeEngine) runSubmitter(pendingCh chan<- submittedTask) {
	defer close(pendingCh)
	if e.dispatcher == nil {
		for range e.decodeQueueCh {
			slog.Debug("decode worker: no dispatcher, dropping task", "session_id", e.sess.ID)
		}
		return
	}
	for task := range e.decodeQueueCh {
		e.reqSeq++
		reqID := fmt.Sprintf("%s-r%d", e.sess.ID, e.reqSeq)
		resultCh := e.dispatcher.Enqueue(&BatchTask{
			Ctx:          e.ctx,
			SessionID:    e.sess.ID,
			RequestID:    reqID,
			AudioData:    task.audio,
			SampleRate:   e.cfg.SampleRate,
			LanguageCode: e.cfg.LanguageCode,
			Task:         inferencepb.Task_TASK_TRANSCRIBE,
			DecodeOptions: nil, // TODO: forward from negotiated session config
			IsFinal:      task.isFinal,
			IsPartial:    task.isPartial,
			StartSec:     task.startSec,
			EndSec:       task.endSec,
		})
		select {
		case pendingCh <- submittedTask{meta: task, resultCh: resultCh}:
		case <-e.ctx.Done():
			return
		}
	}
}

// ── runResultCollector ───────────────────────────────────────────────────────
// Drains pendingCh in submission order, waits for each BatchResult, runs the
// ResultAssembler, and emits DecodeResult on resultsCh.
// Submission-order processing preserves ResultAssembler's committed/unstable
// invariants even though the dispatcher may complete tasks out of order.
func (e *batchDecodeEngine) runResultCollector(pendingCh <-chan submittedTask) {
	assembler := NewResultAssembler()

	for st := range pendingCh {
		var fr BatchResult
		select {
		case fr = <-st.resultCh:
		case <-e.ctx.Done():
			return
		}

		if errors.Is(fr.Err, errPartialCancelled) ||
			errors.Is(fr.Err, errSessionCancelled) ||
			errors.Is(fr.Err, ErrShutdown) {
			continue // stale partial, session cancelled, or dispatcher shutdown
		}
		if fr.Err != nil {
			if errors.Is(fr.Err, context.Canceled) || errors.Is(fr.Err, context.DeadlineExceeded) {
				return
			}
			var sttErr *sttErrors.STTError
			if errors.As(fr.Err, &sttErr) &&
				sttErr.Code() == sttErrors.ErrGlobalPendingExceeded && !st.meta.isFinal {
				slog.Debug("partial decode dropped (dispatcher full)", "session_id", e.sess.ID)
				continue
			}
			slog.Warn("decode failed",
				"session_id", e.sess.ID, "is_final", st.meta.isFinal, "error", fr.Err)
			if st.meta.isFinal {
				assembler.Reset()
			}
			continue
		}

		// When the engine returns fine-grained segments, emit one DecodeResult
		// per segment with accurate per-sentence timestamps.  Segment offsets
		// are clip-relative; add st.meta.startSec to make them session-relative.
		if st.meta.isFinal && len(fr.Resp.GetSegments()) > 0 {
			assembler.Reset()
			for _, seg := range fr.Resp.GetSegments() {
				if seg.GetText() == "" {
					continue
				}
				result := DecodeResult{
					Text:          seg.GetText(),
					CommittedText: seg.GetText(),
					IsFinal:       true,
					StartSec:      st.meta.startSec + float64(seg.GetStartSec()),
					EndSec:        st.meta.startSec + float64(seg.GetEndSec()),
					AudioDuration: fr.Resp.AudioDurationSec,
					LanguageCode:  fr.Resp.LanguageCode,
				}
				select {
				case e.resultsCh <- result:
				case <-e.ctx.Done():
					return
				}
			}
			continue
		}

		committed, unstable := assembler.Update(fr.Resp.Text, st.meta.isFinal)
		if st.meta.isFinal {
			assembler.Reset()
		}

		result := DecodeResult{
			Text:          fr.Resp.Text,
			CommittedText: committed,
			UnstableText:  unstable,
			IsFinal:       st.meta.isFinal,
			StartSec:      st.meta.startSec,
			EndSec:        st.meta.endSec,
			AudioDuration: fr.Resp.AudioDurationSec,
			LanguageCode:  fr.Resp.LanguageCode,
		}

		select {
		case e.resultsCh <- result:
		case <-e.ctx.Done():
			return
		}
	}
}
