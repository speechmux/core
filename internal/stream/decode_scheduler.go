package stream

import (
	"context"
	"fmt"
	"log/slog"
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

// defaultMaxDecodeWait is the maximum time a final-decode request will block
// waiting for a global pending slot. Prevents HOL blocking (design doc §10.6).
const defaultMaxDecodeWait = 5 * time.Second

// DecodeScheduler manages global concurrency for inference requests and routes
// them to healthy inference endpoints via the PluginRouter.
//
// Global pending semaphore (design doc §10.6):
//   - For final decodes, Submit blocks up to maxDecodeWait for a slot.
//   - For partial decodes, Submit returns ErrGlobalPendingExceeded immediately
//     when no slot is available (non-blocking drop semantics).
type DecodeScheduler struct {
	router        *plugin.PluginRouter
	pending       chan struct{} // bounded global semaphore; nil = unlimited
	maxDecodeWait time.Duration
	decodeTimeout time.Duration
	obs           metrics.MetricsObserver
}

// NewDecodeScheduler creates a DecodeScheduler.
//
//   - router: routes requests to healthy inference endpoints.
//   - maxPending: maximum number of in-flight decode requests across all sessions.
//     Use 0 for unlimited.
//   - decodeTimeoutSec: per-request timeout in seconds (ERR2001 on expiry).
func NewDecodeScheduler(
	router *plugin.PluginRouter,
	maxPending int,
	decodeTimeoutSec float64,
	obs metrics.MetricsObserver,
) *DecodeScheduler {
	if obs == nil {
		obs = metrics.NopMetrics{}
	}
	var pending chan struct{}
	if maxPending > 0 {
		pending = make(chan struct{}, maxPending)
	}
	return &DecodeScheduler{
		router:        router,
		pending:       pending,
		maxDecodeWait: defaultMaxDecodeWait,
		decodeTimeout: time.Duration(decodeTimeoutSec * float64(time.Second)),
		obs:           obs,
	}
}

// Submit sends an audio segment to an inference endpoint for decoding.
//
// For final decodes (isFinal=true), it waits up to maxDecodeWait for a global
// pending slot (ERR2008 on timeout). For partial decodes, it drops immediately
// when no slot is available.
//
// The returned *TranscribeResponse is non-nil only on success; plugin error
// codes are translated to Core ERR#### errors.
func (s *DecodeScheduler) Submit(
	ctx context.Context,
	sessionID string,
	requestID string,
	audioData []byte,
	sampleRate int32,
	languageCode string,
	task inferencepb.Task,
	decodeOptions *inferencepb.DecodeOptions,
	isFinal bool,
	isPartial bool,
) (*inferencepb.TranscribeResponse, error) {
	// 0. Start a child span under the session.pipeline span.
	ctx, span := otel.Tracer("speechmux/core/stream").Start(ctx, "stt.decode")
	span.SetAttributes(
		attribute.String("session.id", sessionID),
		attribute.Bool("decode.is_final", isFinal),
		attribute.Bool("decode.is_partial", isPartial),
		attribute.Float64("decode.audio_sec", float64(len(audioData))/float64(sampleRate*2)),
	)
	defer span.End()

	// 1. Acquire global pending slot.
	if s.pending != nil {
		if isFinal {
			select {
			case s.pending <- struct{}{}:
			case <-time.After(s.maxDecodeWait):
				err := sttErrors.New(sttErrors.ErrGlobalPendingExceeded, "decode queue full")
				span.RecordError(err)
				span.SetStatus(otelcodes.Error, err.Error())
				return nil, err
			case <-ctx.Done():
				span.RecordError(ctx.Err())
				span.SetStatus(otelcodes.Error, ctx.Err().Error())
				return nil, ctx.Err()
			}
		} else {
			// Non-blocking: drop partial if no slot is available.
			select {
			case s.pending <- struct{}{}:
			default:
				err := sttErrors.New(sttErrors.ErrGlobalPendingExceeded, "partial decode dropped: queue full")
				span.RecordError(err)
				span.SetStatus(otelcodes.Error, err.Error())
				return nil, err
			}
		}
		defer func() { <-s.pending }()
	}

	// 2. Find a healthy endpoint.
	client, err := s.router.Route()
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

	// 3. Apply per-request decode timeout.
	// Intentionally decouple from the caller's context: a session being closed
	// must not abort an in-flight decode that was already dispatched. Once the
	// semaphore slot is acquired and a healthy endpoint is selected, the RPC
	// runs to completion (bounded by decodeTimeout). The session context is
	// still checked at the semaphore stage above — it only prevents *new*
	// decodes from starting after the session is cancelled.
	decodeCtx := context.Background()
	var cancel context.CancelFunc
	if s.decodeTimeout > 0 {
		decodeCtx, cancel = context.WithTimeout(context.Background(), s.decodeTimeout)
		defer cancel()
	}

	// 4. Call Transcribe.
	decodeStart := time.Now()
	req := &inferencepb.TranscribeRequest{
		RequestId:     requestID,
		SessionId:     sessionID,
		AudioData:     audioData,
		SampleRate:    sampleRate,
		LanguageCode:  languageCode,
		Task:          task,
		DecodeOptions: decodeOptions,
		IsFinal:       isFinal,
		IsPartial:     isPartial,
	}
	resp, err := client.Transcribe(decodeCtx, req)
	latency := time.Since(decodeStart).Seconds()
	if err != nil {
		s.obs.RecordDecodeLatency(latency, isFinal, engineName)
		s.obs.RecordDecodeResult(isFinal, false, engineName)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		if decodeCtx.Err() == context.DeadlineExceeded {
			return nil, sttErrors.New(sttErrors.ErrDecodeTimeout, fmt.Sprintf("decode timed out after %.1fs", s.decodeTimeout.Seconds()))
		}
		return nil, fmt.Errorf("transcribe: %w", err)
	}

	// 5. Translate plugin error codes to Core ERR#### codes.
	if resp.ErrorCode != commonpb.PluginErrorCode_PLUGIN_ERROR_UNSPECIFIED {
		s.obs.RecordDecodeLatency(latency, isFinal, engineName)
		s.obs.RecordDecodeResult(isFinal, false, engineName)
		coreErr := sttErrors.FromPluginError(resp.ErrorCode)
		slog.Warn("inference plugin returned error", "session_id", sessionID, "plugin_error", resp.ErrorCode, "core_error", coreErr)
		pluginErr := sttErrors.New(coreErr, fmt.Sprintf("plugin error: %s", resp.ErrorCode))
		span.RecordError(pluginErr)
		span.SetStatus(otelcodes.Error, pluginErr.Error())
		return nil, pluginErr
	}

	s.obs.RecordDecodeLatency(latency, isFinal, engineName)
	s.obs.RecordDecodeResult(isFinal, true, engineName)
	return resp, nil
}
