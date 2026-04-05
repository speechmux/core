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
type EPDController struct {
	silenceDuration   time.Duration
	vadFrameTimeout   time.Duration
	rmsThreshold      float64
	heartbeatInterval time.Duration // 0 = disabled

	// onSpeechStart is called with the ring-buffer sequence number of the first
	// speech frame in a new utterance. May be nil.
	onSpeechStart func(startSeq uint64)
	// onSpeechEnd is called immediately after onUtteranceEnd fires. May be nil.
	onSpeechEnd func()
}

// NewEPDController returns an EPDController with the given silence duration,
// RMS threshold, VAD-frame timeout, and heartbeat log interval.
// A zero vadFrameTimeoutSec falls back to 3 s.
// A zero heartbeatIntervalSec disables the periodic heartbeat log.
func NewEPDController(silenceSec, rmsThreshold, vadFrameTimeoutSec, heartbeatIntervalSec float64) *EPDController {
	if vadFrameTimeoutSec == 0 {
		vadFrameTimeoutSec = 3.0
	}
	return &EPDController{
		silenceDuration:   time.Duration(silenceSec * float64(time.Second)),
		vadFrameTimeout:   time.Duration(vadFrameTimeoutSec * float64(time.Second)),
		rmsThreshold:      rmsThreshold,
		heartbeatInterval: time.Duration(heartbeatIntervalSec * float64(time.Second)),
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

// Run processes VAD frames and calls onUtteranceEnd with the concatenated
// speech audio when silence_duration elapses after an active speech segment.
// It blocks until ctx is cancelled, vadResultCh is closed, or
// ErrVADFrameTimeout fires.
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
