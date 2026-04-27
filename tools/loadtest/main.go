// Command loadtest runs concurrent gRPC StreamingRecognize sessions against
// SpeechMux Core to measure throughput and latency under load.
//
// It is designed to run against Dummy VAD + Dummy STT plugins so that Core's
// orchestration overhead can be measured in isolation from inference latency.
//
// Usage:
//
//	go run ./tools/loadtest/ --sessions 100 --duration 60s --scenario steady
//
// Scenarios:
//
//	steady    — all sessions start at once, each runs for --session-sec seconds
//	rampup    — sessions are started evenly over --ramp-up seconds, then all run
//	spike     — sessions/2 start immediately, sessions/2 more start at duration/3
//	streaming — same launch pattern as steady; additionally tracks per-partial
//	            inter-arrival cadence and finalize latency in the JSON report
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	clientpb "github.com/speechmux/proto/gen/go/client/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// runConfig holds all parameters for a load test run.
type runConfig struct {
	addr        string
	sessions    int
	sessionSec  int
	duration    time.Duration // overrides sessionSec when non-zero
	scenario    string
	rampUpSec   int
	chunkMs     int
	lang        string
	engineHint  string
	jsonOut     bool
}

// effectiveSessionSec returns the session duration in seconds, preferring
// --duration over --session-sec when both are specified.
func (c *runConfig) effectiveSessionSec() int {
	if c.duration > 0 {
		return int(c.duration.Seconds())
	}
	return c.sessionSec
}

// sessionResult captures the outcome of one test session.
type sessionResult struct {
	openLatency    time.Duration // SessionConfig sent → session_created received
	firstResultLat time.Duration // first audio sent → first RecognitionResult received; -1 = no result
	resultCount    int
	errCode        string        // empty = success
	duration       time.Duration // total wall time for this session
	// Streaming-specific fields (populated only when scenario=streaming).
	partialInterMs []float64 // inter-arrival times between successive partial results (ms)
	finalizeLatMs  []float64 // time from first-audio to each is_final result (ms)
}

// latencyStats holds aggregated percentile latency values in milliseconds.
type latencyStats struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

// Report is the final output structure written as JSON or printed as text.
type Report struct {
	Scenario           string         `json:"scenario"`
	Sessions           int            `json:"sessions"`
	SessionSec         int            `json:"session_sec"`
	SessionsAttempted  int            `json:"sessions_attempted"`
	SessionsSucceeded  int            `json:"sessions_succeeded"`
	SessionsFailed     int            `json:"sessions_failed"`
	ErrorsByCode       map[string]int `json:"errors_by_code,omitempty"`
	OpenLatency        latencyStats   `json:"session_open_latency"`
	FirstResultLatency latencyStats   `json:"first_result_latency"`
	TotalResults       int            `json:"total_results"`
	GoVersion          string         `json:"go_version"`
	NumCPU             int            `json:"num_cpu"`
	// Streaming-specific fields (populated only for scenario=streaming; omitempty).
	StreamingPartialCadenceP50  float64 `json:"streaming_partial_cadence_p50_ms,omitempty"`
	StreamingPartialCadenceP95  float64 `json:"streaming_partial_cadence_p95_ms,omitempty"`
	StreamingFinalizeLatencyP50 float64 `json:"streaming_finalize_latency_p50_ms,omitempty"`
	StreamingFinalizeLatencyP95 float64 `json:"streaming_finalize_latency_p95_ms,omitempty"`
}

func main() {
	cfg := &runConfig{}
	flag.StringVar(&cfg.addr, "addr", "localhost:50051", "Core gRPC address")
	flag.IntVar(&cfg.sessions, "sessions", 10, "number of concurrent sessions")
	flag.IntVar(&cfg.sessionSec, "session-sec", 10, "audio duration per session (seconds); overridden by --duration")
	flag.DurationVar(&cfg.duration, "duration", 0, "audio duration per session (e.g. 5m, 30s); overrides --session-sec")
	flag.StringVar(&cfg.scenario, "scenario", "steady", "test scenario: steady | rampup | spike | streaming")
	flag.IntVar(&cfg.rampUpSec, "ramp-up", 5, "ramp-up period in seconds (scenario=rampup only)")
	flag.IntVar(&cfg.chunkMs, "chunk-ms", 20, "audio chunk size in milliseconds")
	flag.StringVar(&cfg.lang, "lang", "ko", "BCP-47 language code")
	flag.StringVar(&cfg.engineHint, "engine-hint", "",
		"endpoint ID hint for all sessions (e.g. 'sherpa-onnx'); use with --scenario streaming")
	flag.BoolVar(&cfg.jsonOut, "json", false, "output JSON report only (no progress)")
	flag.Parse()

	if !cfg.jsonOut {
		fmt.Printf("SpeechMux Core Load Test\n")
		fmt.Printf("  addr=%s  sessions=%d  session-sec=%d  scenario=%s\n\n",
			cfg.addr, cfg.sessions, cfg.effectiveSessionSec(), cfg.scenario)
	}

	conn, err := grpc.NewClient(cfg.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", cfg.addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	results := runScenario(cfg, conn)
	r := buildReport(cfg, results)

	if cfg.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(r)
		return
	}
	printReport(r)
}

// runScenario launches sessions according to the selected scenario and waits
// for all of them to finish, then returns the collected results.
func runScenario(cfg *runConfig, conn *grpc.ClientConn) []sessionResult {
	var (
		mu      sync.Mutex
		results []sessionResult
		wg      sync.WaitGroup
		idSeq   atomic.Int64
	)

	launch := func(delay time.Duration) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if delay > 0 {
				time.Sleep(delay)
			}
			id := fmt.Sprintf("lt-%d", idSeq.Add(1))
			r := runSession(cfg, conn, id)
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}()
	}

	switch cfg.scenario {
	case "rampup":
		// Spread sessions evenly over rampUpSec.
		rampDur := time.Duration(cfg.rampUpSec) * time.Second
		interval := rampDur / time.Duration(cfg.sessions)
		for i := 0; i < cfg.sessions; i++ {
			launch(time.Duration(i) * interval)
		}

	case "spike":
		// Half the sessions start immediately; the other half start after
		// one-third of the session duration has elapsed.
		half := cfg.sessions / 2
		spike := cfg.sessions - half
		spikedelay := time.Duration(cfg.effectiveSessionSec()) * time.Second / 3
		for i := 0; i < half; i++ {
			launch(0)
		}
		for i := 0; i < spike; i++ {
			launch(spikedelay)
		}

	case "streaming":
		// Same launch pattern as steady; per-session streaming metrics are
		// collected in runSession and aggregated in buildReport.
		if cfg.engineHint == "" && !cfg.jsonOut {
			fmt.Fprintln(os.Stderr,
				"WARNING: --scenario streaming without --engine-hint may hit batch endpoints")
		}
		for i := 0; i < cfg.sessions; i++ {
			launch(0)
		}

	default: // steady
		for i := 0; i < cfg.sessions; i++ {
			launch(0)
		}
	}

	wg.Wait()
	return results
}

// runSession executes a single StreamingRecognize session and returns metrics.
func runSession(cfg *runConfig, conn *grpc.ClientConn, id string) sessionResult {
	res := sessionResult{firstResultLat: -1}
	sessionStart := time.Now()

	// Give the session enough timeout to complete its audio plus a generous buffer.
	timeout := time.Duration(cfg.effectiveSessionSec())*time.Second + 60*time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client := clientpb.NewSTTServiceClient(conn)
	stream, err := client.StreamingRecognize(ctx)
	if err != nil {
		res.errCode = grpcErrCode(err)
		res.duration = time.Since(sessionStart)
		return res
	}

	// Send SessionConfig (first message).
	t0 := time.Now()
	if err := stream.Send(&clientpb.StreamingRecognizeRequest{
		StreamingRequest: &clientpb.StreamingRecognizeRequest_SessionConfig{
			SessionConfig: &clientpb.SessionConfig{
				SessionId: id,
				RecognitionConfig: &clientpb.RecognitionConfig{
					LanguageCode: cfg.lang,
					EngineHint:   cfg.engineHint,
				},
			},
		},
	}); err != nil {
		res.errCode = grpcErrCode(err)
		res.duration = time.Since(sessionStart)
		return res
	}

	// Read session_created (or error).
	resp, err := stream.Recv()
	if err != nil {
		res.errCode = grpcErrCode(err)
		res.duration = time.Since(sessionStart)
		return res
	}
	if errPb := resp.GetError(); errPb != nil {
		res.errCode = errPb.GetErrorCode()
		res.duration = time.Since(sessionStart)
		return res
	}
	res.openLatency = time.Since(t0)

	// Audio send goroutine: streams synthetic PCM silence at realtime speed,
	// then sends the is_last signal.
	audioStart := time.Now()
	sendDone := make(chan error, 1)
	go func() {
		bytesPerChunk := 16000 * cfg.chunkMs / 1000 * 2 // 16kHz S16LE
		silence := make([]byte, bytesPerChunk)
		deadline := time.Now().Add(time.Duration(cfg.effectiveSessionSec()) * time.Second)
		chunkDur := time.Duration(cfg.chunkMs) * time.Millisecond

		for time.Now().Before(deadline) {
			if err := stream.Send(&clientpb.StreamingRecognizeRequest{
				StreamingRequest: &clientpb.StreamingRecognizeRequest_Audio{Audio: silence},
			}); err != nil {
				sendDone <- err
				return
			}
			time.Sleep(chunkDur)
		}
		// Signal end-of-audio.
		_ = stream.Send(&clientpb.StreamingRecognizeRequest{
			StreamingRequest: &clientpb.StreamingRecognizeRequest_Signal{
				Signal: &clientpb.StreamSignal{IsLast: true},
			},
		})
		_ = stream.CloseSend()
		sendDone <- nil
	}()

	// Receive results until the stream closes or an error arrives.
	// For scenario=streaming, track partial inter-arrival cadence and finalize latency.
	var lastPartialAt time.Time
	var partialInterMs, finalizeLatMs []float64

	for {
		msg, err := stream.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) && res.errCode == "" {
				res.errCode = grpcErrCode(err)
			}
			break
		}
		if errPb := msg.GetError(); errPb != nil {
			if res.errCode == "" {
				res.errCode = errPb.GetErrorCode()
			}
			break
		}
		if result := msg.GetResult(); result != nil {
			now := time.Now()
			res.resultCount++
			if res.firstResultLat < 0 {
				res.firstResultLat = now.Sub(audioStart)
			}
			if cfg.scenario == "streaming" {
				if result.GetIsFinal() {
					finalizeLatMs = append(finalizeLatMs,
						float64(now.Sub(audioStart))/float64(time.Millisecond))
					lastPartialAt = time.Time{} // reset cadence tracking after final
				} else {
					if !lastPartialAt.IsZero() {
						partialInterMs = append(partialInterMs,
							float64(now.Sub(lastPartialAt))/float64(time.Millisecond))
					}
					lastPartialAt = now
				}
			}
		}
	}
	res.partialInterMs = partialInterMs
	res.finalizeLatMs = finalizeLatMs

	<-sendDone
	res.duration = time.Since(sessionStart)
	return res
}

// grpcErrCode extracts an ERR#### code from a gRPC status error, or falls back
// to the gRPC status code string.
func grpcErrCode(err error) string {
	if err == nil {
		return ""
	}
	if s, ok := status.FromError(err); ok {
		msg := s.Message()
		// gRPC error messages from Core are formatted as "ERR#### <description>"
		if len(msg) >= 7 && msg[:3] == "ERR" {
			return msg[:7]
		}
		return s.Code().String()
	}
	return err.Error()
}

// buildReport aggregates session results into a Report.
func buildReport(cfg *runConfig, results []sessionResult) *Report {
	r := &Report{
		Scenario:          cfg.scenario,
		Sessions:          cfg.sessions,
		SessionSec:        cfg.effectiveSessionSec(),
		SessionsAttempted: len(results),
		ErrorsByCode:      make(map[string]int),
		GoVersion:         runtime.Version(),
		NumCPU:            runtime.NumCPU(),
	}

	var openMs, firstMs []float64
	for _, res := range results {
		if res.errCode == "" {
			r.SessionsSucceeded++
		} else {
			r.SessionsFailed++
			r.ErrorsByCode[res.errCode]++
		}
		r.TotalResults += res.resultCount
		if res.openLatency > 0 {
			openMs = append(openMs, float64(res.openLatency)/float64(time.Millisecond))
		}
		if res.firstResultLat >= 0 {
			firstMs = append(firstMs, float64(res.firstResultLat)/float64(time.Millisecond))
		}
	}

	if len(r.ErrorsByCode) == 0 {
		r.ErrorsByCode = nil
	}

	r.OpenLatency = computeStats(openMs)
	r.FirstResultLatency = computeStats(firstMs)

	if cfg.scenario == "streaming" {
		var allPartialInterMs, allFinalizeLatMs []float64
		for _, res := range results {
			allPartialInterMs = append(allPartialInterMs, res.partialInterMs...)
			allFinalizeLatMs = append(allFinalizeLatMs, res.finalizeLatMs...)
		}
		sort.Float64s(allPartialInterMs)
		sort.Float64s(allFinalizeLatMs)
		if len(allPartialInterMs) > 0 {
			r.StreamingPartialCadenceP50 = percentile(allPartialInterMs, 50)
			r.StreamingPartialCadenceP95 = percentile(allPartialInterMs, 95)
		}
		if len(allFinalizeLatMs) > 0 {
			r.StreamingFinalizeLatencyP50 = percentile(allFinalizeLatMs, 50)
			r.StreamingFinalizeLatencyP95 = percentile(allFinalizeLatMs, 95)
		}
	}

	return r
}

// computeStats calculates percentile latency statistics from a slice of
// millisecond values.
func computeStats(ms []float64) latencyStats {
	if len(ms) == 0 {
		return latencyStats{}
	}
	sorted := make([]float64, len(ms))
	copy(sorted, ms)
	sort.Float64s(sorted)
	return latencyStats{
		Count: len(sorted),
		P50:   percentile(sorted, 50),
		P95:   percentile(sorted, 95),
		P99:   percentile(sorted, 99),
		Max:   sorted[len(sorted)-1],
	}
}

// percentile returns the p-th percentile value from a sorted slice.
func percentile(sorted []float64, p int) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	idx := int(float64(p) / 100.0 * float64(n-1) + 0.5)
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

// printReport writes a human-readable summary to stdout.
func printReport(r *Report) {
	fmt.Printf("=== Load Test Report ===\n\n")
	fmt.Printf("Scenario:     %s\n", r.Scenario)
	fmt.Printf("Sessions:     %d\n", r.Sessions)
	fmt.Printf("Session dur:  %ds\n", r.SessionSec)
	fmt.Printf("Runtime:      %s  (NumCPU=%d)\n\n", r.GoVersion, r.NumCPU)

	fmt.Printf("--- Session Results ---\n")
	fmt.Printf("Attempted:    %d\n", r.SessionsAttempted)
	fmt.Printf("Succeeded:    %d\n", r.SessionsSucceeded)
	fmt.Printf("Failed:       %d\n", r.SessionsFailed)
	if len(r.ErrorsByCode) > 0 {
		fmt.Printf("Errors by code:\n")
		for code, count := range r.ErrorsByCode {
			fmt.Printf("  %-10s %d\n", code, count)
		}
	}

	fmt.Printf("\n--- Latency (ms) ---\n")
	fmt.Printf("Session open:   p50=%6.1f  p95=%6.1f  p99=%6.1f  max=%6.1f  n=%d\n",
		r.OpenLatency.P50, r.OpenLatency.P95, r.OpenLatency.P99, r.OpenLatency.Max, r.OpenLatency.Count)
	fmt.Printf("First result:   p50=%6.1f  p95=%6.1f  p99=%6.1f  max=%6.1f  n=%d\n",
		r.FirstResultLatency.P50, r.FirstResultLatency.P95, r.FirstResultLatency.P99, r.FirstResultLatency.Max, r.FirstResultLatency.Count)

	fmt.Printf("\n--- Throughput ---\n")
	fmt.Printf("Total results received: %d\n", r.TotalResults)

	if r.StreamingPartialCadenceP50 > 0 || r.StreamingFinalizeLatencyP50 > 0 {
		fmt.Printf("\n--- Streaming Metrics (ms) ---\n")
		fmt.Printf("Partial cadence:   p50=%6.1f  p95=%6.1f\n",
			r.StreamingPartialCadenceP50, r.StreamingPartialCadenceP95)
		fmt.Printf("Finalize latency:  p50=%6.1f  p95=%6.1f\n",
			r.StreamingFinalizeLatencyP50, r.StreamingFinalizeLatencyP95)
	}
}
