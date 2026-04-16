package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// ErrVADFrameTimeout is returned by EPDController.Run when the VAD plugin
// stops producing responses for longer than vadFrameTimeout (silent hang).
// The caller should close the session with ERR3004.
var ErrVADFrameTimeout = errors.New("VAD frame timeout: silent hang detected")

// ErrVADWatermarkLag is returned by EPDController.Run (terminateOnLag=true, BATCH
// mode) when the gap between the latest buffered audio and the VAD confirmed
// watermark exceeds watermarkLagThreshold. Indicates a slow-but-responding VAD
// plugin. The caller should close the session with ERR3004.
var ErrVADWatermarkLag = errors.New("VAD watermark lag: slow-but-responding VAD detected")

// lagWarnCooldown is the minimum interval between consecutive onWatermarkLag calls
// in non-terminating (REALTIME) mode.
const lagWarnCooldown = 30 * time.Second

// VADFrame carries a single VAD result echoed back from the plugin.
type VADFrame struct {
	SequenceNumber    uint64
	IsSpeech          bool
	SpeechProbability float32
}

// EPDController drives end-point detection by consuming VAD results and
// triggering an utterance-end callback when silence_duration elapses after speech.
//
// Silent hang defence: if the VAD plugin stops responding for longer than
// vadFrameTimeout, Run returns ErrVADFrameTimeout so the caller can terminate
// the session with ERR3004.
//
// Watermark lag defence: if the gap between the latest buffered audio and the VAD
// confirmed watermark exceeds watermarkLagThreshold, Run either returns
// ErrVADWatermarkLag (terminateOnLag=true, BATCH) or invokes onWatermarkLag with
// a cooldown (terminateOnLag=false, REALTIME).
type EPDController struct {
	silenceDuration       time.Duration
	vadFrameTimeout       time.Duration
	rmsThreshold          float64
	heartbeatInterval     time.Duration // 0 = disabled
	watermarkLagThreshold time.Duration // 0 = disabled
	lagFrameDurationMs    float64       // milliseconds per sequence number unit
	terminateOnLag        bool          // true: return error (BATCH); false: warn only (REALTIME)

	// onSpeechStart is called with the ring-buffer sequence number of the first
	// speech frame in a new utterance. May be nil.
	onSpeechStart func(startSeq uint64)
	// onSpeechEnd is called immediately after onUtteranceEnd fires. May be nil.
	onSpeechEnd func()
	// onWatermarkLag is called when the watermark lag exceeds the threshold.
	// lagSec is the measured lag in seconds. May be nil.
	onWatermarkLag func(lagSec float64)
}

// NewEPDController returns an EPDController with the given configuration.
// A zero vadFrameTimeoutSec falls back to 3 s.
// A zero heartbeatIntervalSec disables the periodic heartbeat log.
// A zero watermarkLagThresholdSec disables watermark lag detection.
// lagFrameDurationMs is the duration of one sequence unit in milliseconds
// (matches the VAD plugin's optimal_frame_ms; typically 30).
// terminateOnLag=true returns ErrVADWatermarkLag on threshold breach (BATCH);
// false logs a warning via onWatermarkLag with a 30 s cooldown (REALTIME).
func NewEPDController(
	silenceSec, rmsThreshold, vadFrameTimeoutSec, heartbeatIntervalSec,
	watermarkLagThresholdSec, lagFrameDurationMs float64,
	terminateOnLag bool,
) *EPDController {
	if vadFrameTimeoutSec == 0 {
		vadFrameTimeoutSec = 3.0
	}
	return &EPDController{
		silenceDuration:       time.Duration(silenceSec * float64(time.Second)),
		vadFrameTimeout:       time.Duration(vadFrameTimeoutSec * float64(time.Second)),
		rmsThreshold:          rmsThreshold,
		heartbeatInterval:     time.Duration(heartbeatIntervalSec * float64(time.Second)),
		watermarkLagThreshold: time.Duration(watermarkLagThresholdSec * float64(time.Second)),
		lagFrameDurationMs:    lagFrameDurationMs,
		terminateOnLag:        terminateOnLag,
	}
}

// SetSpeechStartCallback registers a function that is called with the ring-buffer
// sequence number of the first speech frame in each new utterance.
func (e *EPDController) SetSpeechStartCallback(fn func(startSeq uint64)) {
	e.onSpeechStart = fn
}

// SetSpeechEndCallback registers a function that is called immediately after
// the onUtteranceEnd callback fires (i.e. after each utterance boundary).
func (e *EPDController) SetSpeechEndCallback(fn func()) {
	e.onSpeechEnd = fn
}

// SetWatermarkLagCallback registers a function called when the VAD watermark lag
// exceeds the configured threshold. lagSec is the measured lag in seconds.
// In non-termination mode (REALTIME), the callback is invoked at most once per
// lagWarnCooldown (30 s) to suppress log spam.
func (e *EPDController) SetWatermarkLagCallback(fn func(lagSec float64)) {
	e.onWatermarkLag = fn
}

// Run processes VAD frames and calls onUtteranceEnd with the concatenated
// speech audio when silence_duration elapses after an active speech segment.
// It blocks until ctx is cancelled, vadResultCh is closed,
// ErrVADFrameTimeout fires, or (in BATCH mode) ErrVADWatermarkLag fires.
func (e *EPDController) Run(
	ctx context.Context,
	vadResultCh <-chan VADFrame,
	buf *AudioRingBuffer,
	onUtteranceEnd func(audio []byte, startSeq, endSeq uint64),
) error {
	silenceTimer := time.NewTimer(e.silenceDuration)
	silenceTimer.Stop()
	vadTimeout := time.NewTimer(e.vadFrameTimeout)
	defer silenceTimer.Stop()
	defer vadTimeout.Stop()

	// Optional periodic heartbeat ticker (disabled when interval == 0).
	var heartbeatCh <-chan time.Time
	if e.heartbeatInterval > 0 {
		hbTicker := time.NewTicker(e.heartbeatInterval)
		defer hbTicker.Stop()
		heartbeatCh = hbTicker.C
	}

	var speechActive bool
	var silenceTimerRunning bool // true when silence timer is counting down
	var speechStartSeq uint64
	var lastSeq uint64
	var frameCount uint64
	var epdCount int
	var lastLagWarn time.Time // last time onWatermarkLag was invoked (REALTIME debounce)

	stopTimer := func(t *time.Timer) {
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
	}

	for {
		select {
		case vad, ok := <-vadResultCh:
			if !ok {
				// VAD stream ended — flush any in-progress utterance as a final decode.
				if speechActive {
					audio := buf.ExtractRange(speechStartSeq, lastSeq)
					if len(audio) > 0 {
						onUtteranceEnd(audio, speechStartSeq, lastSeq)
					}
					if e.onSpeechEnd != nil {
						e.onSpeechEnd()
					}
				}
				return nil
			}
			stopTimer(vadTimeout)
			vadTimeout.Reset(e.vadFrameTimeout)

			buf.AdvanceWatermark(vad.SequenceNumber)
			lastSeq = vad.SequenceNumber
			frameCount++

			// Watermark lag check: detect slow-but-responding VAD.
			if e.watermarkLagThreshold > 0 && e.lagFrameDurationMs > 0 {
				if latestSeq := buf.LatestSequence(); latestSeq > vad.SequenceNumber {
					lagSeqs := latestSeq - vad.SequenceNumber
					lagDur := time.Duration(float64(lagSeqs) * e.lagFrameDurationMs * float64(time.Millisecond))
					if lagDur > e.watermarkLagThreshold {
						if e.terminateOnLag {
							if e.onWatermarkLag != nil {
								e.onWatermarkLag(lagDur.Seconds())
							}
							return fmt.Errorf("%w (lag=%v, threshold=%v)", ErrVADWatermarkLag, lagDur.Round(time.Millisecond), e.watermarkLagThreshold)
						}
						// REALTIME: warn at most once per lagWarnCooldown.
						if e.onWatermarkLag != nil && (lastLagWarn.IsZero() || time.Since(lastLagWarn) >= lagWarnCooldown) {
							e.onWatermarkLag(lagDur.Seconds())
							lastLagWarn = time.Now()
						}
					}
				}
			}

			// Per-frame detail is DEBUG-only; use the heartbeat ticker for
			// periodic INFO-level activity confirmation.
			slog.Debug("EPD vad sample",
				"seq", vad.SequenceNumber,
				"is_speech", vad.IsSpeech,
				"prob", vad.SpeechProbability,
				"speech_active", speechActive,
				"epd_count", epdCount,
			)

			if vad.IsSpeech {
				if silenceTimerRunning {
					stopTimer(silenceTimer)
					silenceTimerRunning = false
				}
				if !speechActive {
					speechStartSeq = vad.SequenceNumber
					speechActive = true
					slog.Info("EPD speech start",
						"seq", speechStartSeq,
						"prob", vad.SpeechProbability,
						"epd_count", epdCount,
					)
					if e.onSpeechStart != nil {
						e.onSpeechStart(speechStartSeq)
					}
				}
			} else if speechActive && !silenceTimerRunning {
				// Start the silence timer on the FIRST non-speech frame
				// after speech. Do NOT reset on subsequent non-speech frames;
				// otherwise the timer is perpetually pushed forward and
				// never fires (frames arrive every ~30ms but silence
				// duration is typically 0.8s).
				silenceTimer.Reset(e.silenceDuration)
				silenceTimerRunning = true
			}

		case <-silenceTimer.C:
			silenceTimerRunning = false
			if speechActive {
				audio := buf.ExtractRange(speechStartSeq, lastSeq)
				epdCount++
				slog.Info("EPD utterance end",
					"epd_count", epdCount,
					"start_seq", speechStartSeq,
					"end_seq", lastSeq,
					"audio_bytes", len(audio),
				)
				if len(audio) > 0 {
					onUtteranceEnd(audio, speechStartSeq, lastSeq)
				}
				if e.onSpeechEnd != nil {
					e.onSpeechEnd()
				}
			}
			speechActive = false

		case <-heartbeatCh:
			slog.Info("EPD heartbeat",
				"frames", frameCount,
				"epd_count", epdCount,
				"speech_active", speechActive,
				"last_seq", lastSeq,
			)

		case <-vadTimeout.C:
			return fmt.Errorf("%w (timeout=%v)", ErrVADFrameTimeout, e.vadFrameTimeout)

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
