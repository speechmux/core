package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"

	sttErrors "github.com/speechmux/core/internal/errors"
	"github.com/speechmux/core/internal/metrics"
	"github.com/speechmux/core/internal/plugin"
	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
)

// ErrShutdown is returned to callers whose queued tasks were not dispatched
// because the dispatcher was shut down before they could run.
var ErrShutdown = errors.New("fair dispatcher shut down")

// errPartialCancelled is sent to a resultCh when a partial task is cancelled
// before dispatch (stale partial or queue cap). Silent-drop in runResultCollector.
var errPartialCancelled = errors.New("partial task cancelled")

// errSessionCancelled is sent when CancelSession() discards a queued task.
// Also silent-dropped in runResultCollector.
var errSessionCancelled = errors.New("session task cancelled")

// BatchTask carries everything FairDecodeDispatcher needs to dispatch a single
// Transcribe RPC.
type BatchTask struct {
	Ctx          context.Context
	SessionID    string
	RequestID    string
	AudioData    []byte
	SampleRate   int32
	LanguageCode string
	Task         inferencepb.Task
	DecodeOptions *inferencepb.DecodeOptions
	IsFinal      bool
	IsPartial    bool
	StartSec     float64
	EndSec       float64
}

// BatchResult is the outcome of a dispatched BatchTask.
type BatchResult struct {
	Resp *inferencepb.TranscribeResponse
	Err  error
}

// BatchScheduler is the scheduling interface used by batchDecodeEngine.
type BatchScheduler interface {
	// Enqueue adds task to the per-session queue. Returns a channel that
	// delivers exactly one BatchResult. The caller must always receive from it.
	Enqueue(task *BatchTask) <-chan BatchResult

	// CancelSession cancels all queued (not-yet-dispatched) tasks for sessionID.
	// Called by batchDecodeEngine.Close() before draining decodeQueueCh.
	CancelSession(sessionID string)
}

// sessionSlot holds the per-session queue and in-flight flag.
type sessionSlot struct {
	queue           []*pendingBatchTask
	inFlight        bool
	inFlightIsFinal bool // true when the current in-flight task is the final decode
	inOrder         bool // true when sessionID is present in d.order
	// finalQueued is set when a final task is enqueued (via cancelStalePartials)
	// and cleared when the final completes (in releaseInFlight). While true,
	// any incoming partial is immediately cancelled — preventing the race where
	// runPartialTimer fires after OnSpeechEnd, sending an extra partial that
	// arrives at the dispatcher after the final and reverts committed text to
	// unstable in runResultCollector.
	finalQueued bool
}

// pendingBatchTask is a BatchTask with its result delivery channel and timing info.
type pendingBatchTask struct {
	task       *BatchTask
	resultCh   chan BatchResult // buffered capacity 1; receives exactly once
	cancelled  bool            // set when stale-partial cancelled or session cancelled
	enqueuedAt time.Time       // for fair_wait_sec metric
}

// FairDecodeDispatcher implements cross-session fair queueing for batch decode.
// It is shared across all sessions for the server lifetime.
type FairDecodeDispatcher struct {
	router  *plugin.PluginRouter
	obs     metrics.MetricsObserver
	timeout time.Duration

	maxConcurrent int
	slots         chan struct{} // semaphore; nil when maxConcurrent=0 (unlimited)

	mu       sync.Mutex
	sessions map[string]*sessionSlot
	order    []string // round-robin queue of session IDs

	maxPartialPerSession int

	notifyCh chan struct{} // non-blocking capacity-1 wakeup channel

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewFairDecodeDispatcher creates and starts a FairDecodeDispatcher.
//
//   - router: routes Transcribe RPCs to healthy endpoints.
//   - obs: metrics observer; nil becomes NopMetrics.
//   - maxConcurrent: max parallel Transcribe RPCs across all sessions; 0 = unlimited.
//   - maxPartialPerSession: per-session partial queue cap; 0 = unlimited.
//   - decodeTimeoutSec: per-RPC timeout in seconds; 0 = no timeout.
func NewFairDecodeDispatcher(
	router *plugin.PluginRouter,
	obs metrics.MetricsObserver,
	maxConcurrent int,
	maxPartialPerSession int,
	decodeTimeoutSec float64,
) *FairDecodeDispatcher {
	if obs == nil {
		obs = metrics.NopMetrics{}
	}
	var slots chan struct{}
	if maxConcurrent > 0 {
		slots = make(chan struct{}, maxConcurrent)
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &FairDecodeDispatcher{
		router:               router,
		obs:                  obs,
		timeout:              time.Duration(decodeTimeoutSec * float64(time.Second)),
		maxConcurrent:        maxConcurrent,
		slots:                slots,
		sessions:             make(map[string]*sessionSlot),
		notifyCh:             make(chan struct{}, 1),
		maxPartialPerSession: maxPartialPerSession,
		ctx:                  ctx,
		cancel:               cancel,
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.dispatchLoop()
	}()
	return d
}

// Enqueue adds task to the per-session sub-queue and returns a channel that
// delivers exactly one BatchResult. The caller must eventually receive from it.
//
// If the task is final, all queued partials for the same session are cancelled
// before the final is appended (stale partial cancellation).
// If the partial queue is full, the oldest partial is cancelled to make room.
func (d *FairDecodeDispatcher) Enqueue(task *BatchTask) <-chan BatchResult {
	resultCh := make(chan BatchResult, 1)
	pt := &pendingBatchTask{task: task, resultCh: resultCh, enqueuedAt: time.Now()}

	d.mu.Lock()
	slot, ok := d.sessions[task.SessionID]
	if !ok {
		slot = &sessionSlot{inOrder: true}
		d.sessions[task.SessionID] = slot
		d.order = append(d.order, task.SessionID)
	}

	if task.IsFinal {
		d.cancelStalePartials(slot) // also sets slot.finalQueued = true
	} else if slot.finalQueued {
		// A final for this session is already queued or in-flight. This partial
		// arrived due to the partial-timer / OnSpeechEnd race in runPartialTimer.
		// Cancel it immediately so it does not revert committed text to unstable.
		pt.cancelled = true
		resultCh <- BatchResult{Err: errPartialCancelled}
		d.mu.Unlock()
		d.obs.RecordFairDispatchPartialCancelled("post_final")
		return resultCh
	} else if d.maxPartialPerSession > 0 {
		d.trimPartialQueue(slot)
	}

	slot.queue = append(slot.queue, pt)
	depth := len(slot.queue)
	d.mu.Unlock()

	d.obs.RecordFairDispatchQueueDepth(task.SessionID, depth)
	d.notify()
	return resultCh
}

// CancelSession cancels all queued (not-yet-dispatched) tasks for sessionID.
// In-flight tasks complete normally; their resultCh writes are silently dropped
// by runResultCollector after batchDecodeEngine exits.
func (d *FairDecodeDispatcher) CancelSession(sessionID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	slot := d.sessions[sessionID]
	if slot == nil {
		return
	}
	for _, pt := range slot.queue {
		if !pt.cancelled {
			pt.cancelled = true
			pt.resultCh <- BatchResult{Err: errSessionCancelled}
		}
	}
	slot.queue = slot.queue[:0]
	if !slot.inFlight {
		delete(d.sessions, sessionID)
		d.removeFromOrder(sessionID)
	}
}

// Shutdown cancels all queued tasks with ErrShutdown, waits for in-flight
// Transcribe RPCs to complete (bounded by timeout), then returns.
func (d *FairDecodeDispatcher) Shutdown() {
	d.cancel()
	d.wg.Wait()
}

// cancelStalePartials cancels all queued partial (IsPartial=true) tasks for slot
// and sets slot.finalQueued so subsequent partials are also rejected until the
// final completes. Must be called with d.mu held.
func (d *FairDecodeDispatcher) cancelStalePartials(slot *sessionSlot) {
	remaining := slot.queue[:0]
	for _, pt := range slot.queue {
		if !pt.task.IsPartial {
			remaining = append(remaining, pt)
		} else if !pt.cancelled {
			pt.cancelled = true
			pt.resultCh <- BatchResult{Err: errPartialCancelled}
			d.obs.RecordFairDispatchPartialCancelled("stale_final")
		}
	}
	slot.queue = remaining
	slot.finalQueued = true
}

// trimPartialQueue cancels the oldest non-cancelled partial (IsPartial=true) when
// the slot's partial queue is at capacity. Must be called with d.mu held.
func (d *FairDecodeDispatcher) trimPartialQueue(slot *sessionSlot) {
	count := 0
	for _, pt := range slot.queue {
		if pt.task.IsPartial && !pt.cancelled {
			count++
		}
	}
	if count < d.maxPartialPerSession {
		return
	}
	for i, pt := range slot.queue {
		if pt.task.IsPartial && !pt.cancelled {
			pt.cancelled = true
			pt.resultCh <- BatchResult{Err: errPartialCancelled}
			d.obs.RecordFairDispatchPartialCancelled("queue_full")
			slot.queue = append(slot.queue[:i], slot.queue[i+1:]...)
			return
		}
	}
}

// removeFromOrder removes sessionID from d.order (linear scan).
// Must be called with d.mu held.
func (d *FairDecodeDispatcher) removeFromOrder(sessionID string) {
	for i, s := range d.order {
		if s == sessionID {
			d.order = append(d.order[:i], d.order[i+1:]...)
			return
		}
	}
}

// notify wakes the dispatcher goroutine without blocking.
func (d *FairDecodeDispatcher) notify() {
	select {
	case d.notifyCh <- struct{}{}:
	default:
	}
}

// tryAcquireSlot non-blockingly tries to occupy one concurrency slot.
// When slots is nil (maxConcurrent=0), always returns acquired=true.
func tryAcquireSlot(slots chan struct{}) (acquired bool, release func()) {
	if slots == nil {
		return true, func() {}
	}
	select {
	case slots <- struct{}{}:
		return true, func() { <-slots }
	default:
		return false, nil
	}
}

// dispatchLoop is the single long-lived dispatcher goroutine.
func (d *FairDecodeDispatcher) dispatchLoop() {
	for {
		select {
		case <-d.ctx.Done():
			// Drain all remaining queued tasks with ErrShutdown.
			d.mu.Lock()
			for _, slot := range d.sessions {
				for _, pt := range slot.queue {
					if !pt.cancelled {
						pt.resultCh <- BatchResult{Err: ErrShutdown}
					}
				}
			}
			d.sessions = make(map[string]*sessionSlot)
			d.order = d.order[:0]
			d.mu.Unlock()
			return

		case <-d.notifyCh:
			for {
				acquired, slotRelease := tryAcquireSlot(d.slots)
				if !acquired {
					break
				}
				d.mu.Lock()
				pt := d.popNext()
				d.mu.Unlock()
				if pt == nil {
					slotRelease()
					break
				}
				d.wg.Add(1)
				go d.runDispatch(pt, slotRelease)
			}
		}
	}
}

// popNext selects the next dispatchable task using round-robin across sessions.
// Must be called with d.mu held. Returns nil when nothing is ready.
func (d *FairDecodeDispatcher) popNext() *pendingBatchTask {
	n := len(d.order)
	for i := 0; i < n; i++ {
		if len(d.order) == 0 {
			break
		}

		sessionID := d.order[0]
		d.order = d.order[1:] // unconditional pop — decide push-back below

		slot := d.sessions[sessionID]
		if slot != nil {
			slot.inOrder = false
		}

		if slot == nil || (len(slot.queue) == 0 && !slot.inFlight) {
			delete(d.sessions, sessionID)
			continue
		}

		if slot.inFlight {
			// Session busy: push back for the next round.
			d.order = append(d.order, sessionID)
			slot.inOrder = true
			continue
		}

		// Session not in-flight: find the first non-cancelled task.
		for len(slot.queue) > 0 {
			pt := slot.queue[0]
			slot.queue = slot.queue[1:]
			if pt.cancelled {
				continue
			}
			slot.inFlight = true
			slot.inFlightIsFinal = pt.task.IsFinal
			if len(slot.queue) > 0 {
				// More tasks remain: push back so other sessions get a turn.
				d.order = append(d.order, sessionID)
				slot.inOrder = true
			}
			// else: leave slot in d.sessions with inFlight=true, inOrder=false.
			// A concurrent Enqueue will find it and append without re-adding to order.
			// releaseInFlight re-adds to order if queue is non-empty after RPC completes.
			return pt
		}

		// All entries cancelled and not in-flight: remove.
		delete(d.sessions, sessionID)
	}
	return nil
}

// runDispatch executes a single Transcribe RPC for the given task.
// Ordering is critical (see §8 Invariants in design doc):
//  1. Write result to pt.resultCh.
//  2. slotRelease() — free the concurrency slot BEFORE notifying.
//  3. releaseInFlight() — clear inFlight and call notify() internally.
//
// Deferring slotRelease would run it after releaseInFlight's notify(), causing
// the dispatcher to wake, find the slot still occupied, and go back to sleep —
// leaving queued tasks unserved until the next Enqueue.
func (d *FairDecodeDispatcher) runDispatch(pt *pendingBatchTask, slotRelease func()) {
	defer d.wg.Done()

	resp, err := d.callTranscribe(pt)
	pt.resultCh <- BatchResult{Resp: resp, Err: err}

	// Must call slotRelease before releaseInFlight to avoid the liveness bug
	// described in the function doc above.
	slotRelease()
	d.releaseInFlight(pt.task.SessionID)
}

// releaseInFlight clears the in-flight flag for sessionID and re-adds to order
// if the session has queued tasks that arrived while the RPC was running.
func (d *FairDecodeDispatcher) releaseInFlight(sessionID string) {
	d.mu.Lock()
	slot := d.sessions[sessionID]
	if slot != nil {
		if slot.inFlightIsFinal {
			// The final for this utterance completed. Allow partials for the next
			// utterance by clearing finalQueued.
			slot.finalQueued = false
			slot.inFlightIsFinal = false
		}
		slot.inFlight = false
		if len(slot.queue) == 0 {
			// No tasks remain: remove. slot.inOrder==false when queue was empty at
			// dispatch time, so the session is not in d.order.
			delete(d.sessions, sessionID)
		} else if !slot.inOrder {
			// Tasks arrived while in-flight (Enqueue ran after popNext removed from
			// d.order). Re-add so the dispatcher sees them.
			d.order = append(d.order, sessionID)
			slot.inOrder = true
		}
		// else: slot.inOrder==true — popNext already pushed it back (more tasks
		// were queued at dispatch time); already in d.order, nothing to do.
	}
	d.mu.Unlock()
	// Notify AFTER releasing the lock and clearing inFlight so the dispatcher
	// wakes up with an accurate view.
	d.notify()
}

// callTranscribe executes the actual Transcribe gRPC call, mirroring
// DecodeScheduler.Submit steps 2–5 (without the semaphore step).
func (d *FairDecodeDispatcher) callTranscribe(pt *pendingBatchTask) (*inferencepb.TranscribeResponse, error) {
	task := pt.task
	waitSec := time.Since(pt.enqueuedAt).Seconds()
	d.obs.RecordFairDispatchWaitSec(waitSec, task.IsFinal)

	ctx, span := otel.Tracer("speechmux/core/stream").Start(task.Ctx, "stt.decode")
	span.SetAttributes(
		attribute.String("session.id", task.SessionID),
		attribute.Bool("decode.is_final", task.IsFinal),
		attribute.Bool("decode.is_partial", task.IsPartial),
		attribute.Float64("decode.audio_sec", float64(len(task.AudioData))/float64(task.SampleRate*2)),
		attribute.Float64("decode.fair_wait_sec", waitSec),
	)
	defer span.End()
	_ = ctx // span context propagated via otel; RPC uses decodeCtx below

	client, err := d.router.RouteBatch()
	if err != nil {
		routeErr := sttErrors.New(sttErrors.ErrAllPluginsUnavailable, err.Error())
		span.RecordError(routeErr)
		span.SetStatus(otelcodes.Error, routeErr.Error())
		return nil, routeErr
	}
	engineName := client.EngineName()
	span.SetAttributes(
		attribute.String("endpoint.id", client.Endpoint().ID()),
		attribute.String("engine.name", engineName),
		attribute.String("engine.model", client.ModelSize()),
		attribute.String("engine.device", client.Device()),
	)

	// Decouple from the session context: a session being closed must not abort
	// an in-flight decode that was already dispatched (same rationale as Submit).
	decodeCtx := context.Background()
	var cancelFn context.CancelFunc
	if d.timeout > 0 {
		decodeCtx, cancelFn = context.WithTimeout(context.Background(), d.timeout)
		defer cancelFn()
	}

	decodeStart := time.Now()
	req := &inferencepb.TranscribeRequest{
		RequestId:     task.RequestID,
		SessionId:     task.SessionID,
		AudioData:     task.AudioData,
		SampleRate:    task.SampleRate,
		LanguageCode:  task.LanguageCode,
		Task:          task.Task,
		DecodeOptions: task.DecodeOptions,
		IsFinal:       task.IsFinal,
		IsPartial:     task.IsPartial,
	}
	resp, err := client.Transcribe(decodeCtx, req)
	latency := time.Since(decodeStart).Seconds()
	if err != nil {
		d.obs.RecordDecodeLatency(latency, task.IsFinal, engineName)
		d.obs.RecordDecodeResult(task.IsFinal, false, engineName)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		if decodeCtx.Err() == context.DeadlineExceeded {
			return nil, sttErrors.New(sttErrors.ErrDecodeTimeout, fmt.Sprintf("decode timed out after %.1fs", d.timeout.Seconds()))
		}
		return nil, fmt.Errorf("transcribe: %w", err)
	}

	if resp.ErrorCode != commonpb.PluginErrorCode_PLUGIN_ERROR_UNSPECIFIED {
		d.obs.RecordDecodeLatency(latency, task.IsFinal, engineName)
		d.obs.RecordDecodeResult(task.IsFinal, false, engineName)
		coreErr := sttErrors.FromPluginError(resp.ErrorCode)
		slog.Warn("inference plugin returned error",
			"session_id", task.SessionID, "plugin_error", resp.ErrorCode, "core_error", coreErr)
		pluginErr := sttErrors.New(coreErr, fmt.Sprintf("plugin error: %s", resp.ErrorCode))
		span.RecordError(pluginErr)
		span.SetStatus(otelcodes.Error, pluginErr.Error())
		return nil, pluginErr
	}

	d.obs.RecordDecodeLatency(latency, task.IsFinal, engineName)
	d.obs.RecordDecodeResult(task.IsFinal, true, engineName)
	return resp, nil
}
