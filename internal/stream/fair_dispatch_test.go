package stream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/speechmux/core/internal/metrics"
	"github.com/speechmux/core/internal/plugin"
	commonpb "github.com/speechmux/proto/gen/go/common/v1"
	inferencepb "github.com/speechmux/proto/gen/go/inference/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ── gated STT server ──────────────────────────────────────────────────────────

// gatedSTTServer blocks each Transcribe call until the test releases it via
// release(). Incoming requests are recorded on reqCh for inspection.
type gatedSTTServer struct {
	inferencepb.UnimplementedInferencePluginServer
	reqCh chan *inferencepb.TranscribeRequest
	relCh chan string
}

func newGatedSTTServer() *gatedSTTServer {
	return &gatedSTTServer{
		reqCh: make(chan *inferencepb.TranscribeRequest, 32),
		relCh: make(chan string, 32),
	}
}

func (s *gatedSTTServer) HealthCheck(_ context.Context, _ *emptypb.Empty) (*commonpb.PluginHealthStatus, error) {
	return &commonpb.PluginHealthStatus{State: commonpb.PluginState_PLUGIN_STATE_READY}, nil
}

func (s *gatedSTTServer) GetCapabilities(_ context.Context, _ *emptypb.Empty) (*inferencepb.InferenceCapabilities, error) {
	return &inferencepb.InferenceCapabilities{
		EngineName:            "gated-stub",
		MaxConcurrentRequests: 8,
		StreamingMode:         inferencepb.StreamingMode_STREAMING_MODE_BATCH_ONLY,
	}, nil
}

func (s *gatedSTTServer) Transcribe(_ context.Context, req *inferencepb.TranscribeRequest) (*inferencepb.TranscribeResponse, error) {
	s.reqCh <- req
	text := <-s.relCh
	return &inferencepb.TranscribeResponse{
		RequestId:    req.GetRequestId(),
		SessionId:    req.GetSessionId(),
		Text:         text,
		LanguageCode: "en",
	}, nil
}

func (s *gatedSTTServer) release(text string) { s.relCh <- text }

func (s *gatedSTTServer) waitRequest(t *testing.T) *inferencepb.TranscribeRequest {
	t.Helper()
	select {
	case req := <-s.reqCh:
		return req
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for RPC request")
		return nil
	}
}

// ── dispatcher test helpers ───────────────────────────────────────────────────

func newTestFairDispatcher(t *testing.T, srv inferencepb.InferencePluginServer, maxConcurrent, maxPartial int) *FairDecodeDispatcher {
	t.Helper()
	dir, err := os.MkdirTemp("", "fairdispatch")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	sock := filepath.Join(dir, "s.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	inferencepb.RegisterInferencePluginServer(gs, srv)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(func() { gs.Stop() })

	ep, err := plugin.NewEndpoint("test-stt", sock, "", plugin.EndpointCircuitBreaker{})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = ep.Close() })

	router := plugin.NewPluginRouter("")
	if err := router.Add(ep.ID(), ep.Socket(), "", 0); err != nil {
		t.Fatalf("router.Add: %v", err)
	}

	d := NewFairDecodeDispatcher(router, metrics.NopMetrics{}, maxConcurrent, maxPartial, 10.0)
	t.Cleanup(func() { d.Shutdown() })
	return d
}

func makeTestTask(sessionID string, isFinal bool) *BatchTask {
	return &BatchTask{
		Ctx:          context.Background(),
		SessionID:    sessionID,
		RequestID:    sessionID + "-req",
		AudioData:    []byte{0, 0, 0, 0},
		SampleRate:   16000,
		LanguageCode: "en",
		Task:         inferencepb.Task_TASK_TRANSCRIBE,
		IsFinal:      isFinal,
		IsPartial:    false, // not a partial decode; use makePartialTask for partial timer tasks
	}
}

// makePartialTask creates a partial-decode task (IsPartial=true). These are
// subject to stale-partial cancellation (cancelStalePartials) and the
// per-session queue cap (trimPartialQueue).
func makePartialTask(sessionID string) *BatchTask {
	return &BatchTask{
		Ctx:          context.Background(),
		SessionID:    sessionID,
		RequestID:    sessionID + "-partial",
		AudioData:    []byte{0, 0, 0, 0},
		SampleRate:   16000,
		LanguageCode: "en",
		Task:         inferencepb.Task_TASK_TRANSCRIBE,
		IsFinal:      false,
		IsPartial:    true,
	}
}

func mustResult(t *testing.T, ch <-chan BatchResult) BatchResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for BatchResult")
		return BatchResult{}
	}
}

// ── TestFairDispatch_RoundRobin ────────────────────────────────────────────────

// TestFairDispatch_RoundRobin verifies that with 3 sessions (A, B, C) each
// having tasks queued while A1 is blocking, the dispatch order is B→C→A→B→C→A.
func TestFairDispatch_RoundRobin(t *testing.T) {
	srv := newGatedSTTServer()
	d := newTestFairDispatcher(t, srv, 1, 0) // maxConcurrent=1

	// A1 dispatches immediately; wait for it to block at the server.
	chA1 := d.Enqueue(makeTestTask("A", false))
	srv.waitRequest(t) // A1 arrived; slot occupied

	// Enqueue the remaining tasks while A1 is blocking.
	chB1 := d.Enqueue(makeTestTask("B", false))
	chC1 := d.Enqueue(makeTestTask("C", false))
	chA2 := d.Enqueue(makeTestTask("A", false))
	chB2 := d.Enqueue(makeTestTask("B", false))
	chC2 := d.Enqueue(makeTestTask("C", false))
	chA3 := d.Enqueue(makeTestTask("A", true))
	chB3 := d.Enqueue(makeTestTask("B", true))
	chC3 := d.Enqueue(makeTestTask("C", true))

	// Release A1; expect B→C→A→B→C→A→B→C order.
	srv.release("A1")
	wantOrder := []string{"B", "C", "A", "B", "C", "A", "B", "C"}
	for i, want := range wantOrder {
		req := srv.waitRequest(t)
		if req.SessionId != want {
			t.Errorf("dispatch %d: want session %q, got %q", i+1, want, req.SessionId)
		}
		srv.release(want + "-text")
	}

	// All results must arrive without error.
	for _, ch := range []<-chan BatchResult{chA1, chB1, chC1, chA2, chB2, chC2, chA3, chB3, chC3} {
		r := mustResult(t, ch)
		if r.Err != nil {
			t.Errorf("unexpected error: %v", r.Err)
		}
	}
}

// ── TestFairDispatch_InFlightGate ─────────────────────────────────────────────

// TestFairDispatch_InFlightGate verifies that session A's second task is not
// dispatched while A's first task is still in-flight.
func TestFairDispatch_InFlightGate(t *testing.T) {
	srv := newGatedSTTServer()
	d := newTestFairDispatcher(t, srv, 0, 0) // unlimited concurrency; gate is per-session

	chA1 := d.Enqueue(makeTestTask("A", false))
	srv.waitRequest(t) // A1 at server

	chA2 := d.Enqueue(makeTestTask("A", true))

	// A2 must not have been dispatched yet (A1 still in-flight).
	time.Sleep(30 * time.Millisecond)
	if len(srv.reqCh) > 0 {
		t.Fatal("A2 dispatched before A1 completed: in-flight gate violated")
	}

	// Release A1; A2 should dispatch next.
	srv.release("A1 text")
	req := srv.waitRequest(t)
	if req.SessionId != "A" {
		t.Errorf("expected A2 dispatch, got session %q", req.SessionId)
	}
	srv.release("A2 text")

	r1 := mustResult(t, chA1)
	if r1.Err != nil {
		t.Errorf("A1: %v", r1.Err)
	}
	r2 := mustResult(t, chA2)
	if r2.Err != nil {
		t.Errorf("A2: %v", r2.Err)
	}
}

// ── TestFairDispatch_StalePartialCancellation ─────────────────────────────────

// TestFairDispatch_StalePartialCancellation verifies that when a final is
// enqueued, queued partials for the same session are cancelled immediately
// (before any dispatch), and the final is dispatched after the in-flight
// partial completes.
func TestFairDispatch_StalePartialCancellation(t *testing.T) {
	srv := newGatedSTTServer()
	d := newTestFairDispatcher(t, srv, 1, 0) // maxConcurrent=1

	// P1 dispatches immediately and blocks.
	chP1 := d.Enqueue(makePartialTask("A"))
	srv.waitRequest(t) // P1 at server

	// P2 queued (A in-flight).
	chP2 := d.Enqueue(makePartialTask("A"))

	// Enqueue final → P2 should be cancelled immediately (cancelStalePartials).
	chFinal := d.Enqueue(makeTestTask("A", true))

	select {
	case r := <-chP2:
		if !errors.Is(r.Err, errPartialCancelled) {
			t.Errorf("P2: want errPartialCancelled, got %v", r.Err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("P2 not cancelled within 100ms after final enqueue")
	}

	// Release P1 (it was already dispatched; completes normally).
	srv.release("P1 text")

	// Final should dispatch next (only one RPC, not P2 which was cancelled).
	req := srv.waitRequest(t)
	if req.SessionId != "A" {
		t.Errorf("expected final for A, got session %q", req.SessionId)
	}
	srv.release("final text")

	r1 := mustResult(t, chP1)
	if r1.Err != nil {
		t.Errorf("P1: %v", r1.Err)
	}
	rFinal := mustResult(t, chFinal)
	if rFinal.Err != nil {
		t.Errorf("final: %v", rFinal.Err)
	}
	if rFinal.Resp == nil || rFinal.Resp.Text != "final text" {
		t.Errorf("final text: got %v, want %q", rFinal.Resp, "final text")
	}
}

// ── TestFairDispatch_PartialQueueCap ──────────────────────────────────────────

// TestFairDispatch_PartialQueueCap verifies that with maxPartialPerSession=3,
// enqueuing 10 more partials (after P1 dispatches and blocks) results in
// exactly 6 cancelled (oldest dropped on each new enqueue over the cap).
func TestFairDispatch_PartialQueueCap(t *testing.T) {
	srv := newGatedSTTServer()
	d := newTestFairDispatcher(t, srv, 1, 3) // maxConcurrent=1, maxPartial=3

	// P1 dispatches immediately and blocks; slot occupied.
	d.Enqueue(makePartialTask("A"))
	srv.waitRequest(t)

	// Enqueue P2-P10 (9 more partials; P5-P10 should each cancel the oldest).
	chs := make([]<-chan BatchResult, 9)
	for i := range chs {
		chs[i] = d.Enqueue(makePartialTask("A"))
	}

	// Cancellations happen synchronously inside Enqueue (under the mutex),
	// so the cancelled resultChs are already written by the time Enqueue returns.
	cancelledCount := 0
	pendingCount := 0
	for _, ch := range chs {
		select {
		case r := <-ch:
			if errors.Is(r.Err, errPartialCancelled) {
				cancelledCount++
			}
		default:
			pendingCount++
		}
	}

	// P2, P3, P4 → no cancel (queue stays ≤ 3). P5 cancels P2, P6 cancels P3, ... P10 cancels P7.
	// Net: P2-P7 cancelled = 6; P8, P9, P10 remain queued = 3.
	if cancelledCount != 6 {
		t.Errorf("want 6 cancelled, got %d (pending=%d)", cancelledCount, pendingCount)
	}
	if pendingCount != 3 {
		t.Errorf("want 3 pending, got %d", pendingCount)
	}

	// Release P1 and drain remaining.
	srv.release("P1")
	for range 3 {
		req := srv.waitRequest(t)
		srv.release(req.SessionId)
	}
}

// ── TestFairDispatch_HeavySessionDoesNotStarve ────────────────────────────────

// TestFairDispatch_HeavySessionDoesNotStarve verifies that a light session (B,
// one task) is dispatched before the heavy session (A, 20+ tasks) gets its
// second turn, even when A's first task was dispatched first.
func TestFairDispatch_HeavySessionDoesNotStarve(t *testing.T) {
	srv := newGatedSTTServer()
	d := newTestFairDispatcher(t, srv, 1, 0)

	// A1 dispatches first; wait for it to arrive.
	d.Enqueue(makeTestTask("A", false))
	srv.waitRequest(t) // A1 at server, slot occupied

	// Enqueue 4 more A tasks (A2–A5, last is final) while A1 is blocking.
	for i := range 4 {
		d.Enqueue(makeTestTask("A", i == 3))
	}

	// Enqueue B1 (only task for B).
	chB1 := d.Enqueue(makeTestTask("B", true))

	// Release A1 → A is re-added to order=[B,A] (B was added after A1 dispatched).
	srv.release("A1")

	// B1 must be dispatched before A2 (round-robin: order had B before A).
	req := srv.waitRequest(t)
	if req.SessionId != "B" {
		t.Errorf("B1 should be dispatched next after A1; got session %q", req.SessionId)
	}
	srv.release("B1")

	// Drain A2–A5.
	for range 4 {
		req := srv.waitRequest(t)
		if req.SessionId != "A" {
			t.Errorf("expected A task, got session %q", req.SessionId)
		}
		srv.release("A-text")
	}

	// B1 result must be clean.
	r := mustResult(t, chB1)
	if r.Err != nil {
		t.Errorf("B1: %v", r.Err)
	}
}

// ── TestFairDispatch_Shutdown ─────────────────────────────────────────────────

// TestFairDispatch_Shutdown verifies that:
//   - Queued (not-yet-dispatched) tasks receive ErrShutdown.
//   - The in-flight task completes normally (its RPC runs to completion).
func TestFairDispatch_Shutdown(t *testing.T) {
	srv := newGatedSTTServer()
	d := newTestFairDispatcher(t, srv, 1, 0)

	// A dispatches immediately; B and C queue.
	chA := d.Enqueue(makeTestTask("A", true))
	srv.waitRequest(t) // A at server, slot occupied

	chB := d.Enqueue(makeTestTask("B", true))
	chC := d.Enqueue(makeTestTask("C", true))

	// Start Shutdown in background; release server after a short delay so the
	// dispatcher loop has time to drain B and C before A's RPC unblocks.
	shutdownDone := make(chan struct{})
	go func() {
		d.Shutdown()
		close(shutdownDone)
	}()

	time.Sleep(20 * time.Millisecond)
	srv.release("A text") // let A complete so wg.Wait() unblocks

	<-shutdownDone

	rB := mustResult(t, chB)
	if !errors.Is(rB.Err, ErrShutdown) {
		t.Errorf("B: want ErrShutdown, got %v", rB.Err)
	}
	rC := mustResult(t, chC)
	if !errors.Is(rC.Err, ErrShutdown) {
		t.Errorf("C: want ErrShutdown, got %v", rC.Err)
	}
	rA := mustResult(t, chA)
	if rA.Err != nil {
		t.Errorf("A (in-flight): unexpected error %v", rA.Err)
	}
}

// ── TestFairDispatch_ConcurrentEnqueue ───────────────────────────────────────

// TestFairDispatch_ConcurrentEnqueue verifies that 100 goroutines enqueuing
// tasks for 10 sessions concurrently produce no panics and all results are
// delivered. Run with -race.
func TestFairDispatch_ConcurrentEnqueue(t *testing.T) {
	d := newTestFairDispatcher(t, &fixedTextSTTServer{text: "ok"}, 0, 0) // unlimited

	const numSessions = 10
	const tasksPerSession = 10

	type entry struct {
		ch  <-chan BatchResult
		idx int
	}

	allChs := make([]entry, 0, numSessions*tasksPerSession)
	var mu sync.Mutex

	var wg sync.WaitGroup
	for s := range numSessions {
		wg.Add(1)
		go func(sIdx int) {
			defer wg.Done()
			sessID := fmt.Sprintf("sess-%d", sIdx)
			for taskIdx := range tasksPerSession {
				ch := d.Enqueue(&BatchTask{
					Ctx:          context.Background(),
					SessionID:    sessID,
					RequestID:    fmt.Sprintf("%s-t%d", sessID, taskIdx),
					AudioData:    make([]byte, 32),
					SampleRate:   16000,
					LanguageCode: "en",
					Task:         inferencepb.Task_TASK_TRANSCRIBE,
					IsFinal:      taskIdx == tasksPerSession-1,
					IsPartial:    taskIdx != tasksPerSession-1,
				})
				mu.Lock()
				allChs = append(allChs, entry{ch: ch, idx: sIdx*tasksPerSession + taskIdx})
				mu.Unlock()
			}
		}(s)
	}
	wg.Wait()

	// All results must be delivered.
	for _, e := range allChs {
		select {
		case <-e.ch:
			// result received (may be cancelled partial or real result — both ok)
		case <-time.After(10 * time.Second):
			t.Fatalf("result %d not received within 10s", e.idx)
		}
	}
}
