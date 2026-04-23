// Package stream implements the audio processing pipeline for SpeechMux Core.
package stream

import (
	"context"

	"github.com/speechmux/core/internal/config"
	"github.com/speechmux/core/internal/session"
)

// SessionDecodeConfig is the per-session configuration passed to a DecodeEngine
// at Start. It contains only values the engine needs; full config and session
// state are available via the Session pointer also passed to Start.
type SessionDecodeConfig struct {
	// SampleRate is the PCM sample rate in Hz (e.g. 16000).
	SampleRate int32
	// LanguageCode is the BCP-47 language hint; empty for auto-detect.
	LanguageCode string
	// Realtime indicates STREAM_MODE_REALTIME when true; BATCH otherwise.
	Realtime bool
	// Stream is a snapshot of the stream-related config at session start.
	// Engines read tuning values (partial intervals, window sizes, timeouts)
	// from here; any later config reloads are not visible to an already-running
	// engine instance.
	Stream config.StreamConfig
}

// DecodeResult is the engine-agnostic decode output. Callers convert this to
// clientpb.RecognitionResult when forwarding to the client.
//
// Fields match clientpb.RecognitionResult exactly (minus client-only fields).
// Adding fields here requires a matching update in the result-forwarding
// goroutine in StreamProcessor.ProcessSession.
type DecodeResult struct {
	// Text is the full decoded text for this result (raw engine output).
	Text string
	// CommittedText is the stable prefix — never shrinks across successive
	// partial results for the same utterance.
	CommittedText string
	// UnstableText is the remainder beyond CommittedText; may change on the
	// next partial.
	UnstableText string
	// IsFinal is true iff this result finalises the current utterance.
	IsFinal bool
	// StartSec is the utterance-relative start time in seconds.
	StartSec float64
	// EndSec is the utterance-relative end time in seconds.
	EndSec float64
	// AudioDuration is the duration of audio this result was decoded from.
	AudioDuration float64
	// LanguageCode is the detected BCP-47 code (may differ from the hint).
	LanguageCode string
}

// DecodeEngine abstracts how audio is decoded into transcription results.
//
// Two implementations exist:
//   - batchDecodeEngine: unary Transcribe calls on per-utterance audio plus
//     adaptive partial decodes. Appropriate for Whisper-family engines.
//   - streamingDecodeEngine: persistent bidi stream.
//     Appropriate for engines with native streaming support.
//
// Lifecycle:
//  1. Start is called exactly once before any other method.
//  2. FeedFrame, OnSpeechStart, OnSpeechEnd, OnUtteranceEnd may be called any
//     number of times in any order after Start returns without error.
//  3. Close is called exactly once. After Close returns, the Results channel
//     is closed and no further methods may be called.
//
// Concurrency: all methods are safe to call from any goroutine. Implementations
// must serialize internally.
type DecodeEngine interface {
	// Start initialises engine state and launches any internal goroutines.
	// The ctx is the session-scoped context; engine goroutines observe it
	// for cancellation. The Session pointer provides access to the ID and
	// SessionInfo; it must not be mutated by the engine.
	Start(ctx context.Context, sess *session.Session, cfg SessionDecodeConfig) error

	// FeedFrame notifies the engine of a newly appended audio frame.
	// Batch engines may no-op (they read from the ring buffer on demand at
	// OnUtteranceEnd time). Streaming engines forward the PCM to the plugin.
	// The seq parameter matches the AudioRingBuffer sequence number for the
	// same frame. Returning an error aborts the session.
	FeedFrame(seq uint64, pcm []byte) error

	// OnSpeechStart is called by the EPD driver when the speech-start event
	// fires. Batch engines use this to seed the partial decode timer.
	// Streaming engines may no-op or log.
	OnSpeechStart(startSeq uint64)

	// OnSpeechEnd is called by the EPD driver when speech transitions to
	// silence (not yet a finalized utterance end). Batch engines use this to
	// stop the partial decode timer. Streaming engines may no-op.
	OnSpeechEnd()

	// OnUtteranceEnd is called by the EPD driver when the utterance is
	// finalized (silence has persisted past vad_silence_sec).
	// startSeq and endSeq delimit the utterance range in the ring buffer
	// sequence space.
	//
	// Batch engines: extract audio from [startSeq, endSeq] via the ring
	// buffer and submit a final decode.
	// Streaming engines: send StreamControl{FINALIZE_UTTERANCE}.
	//
	// The method returns quickly; the final DecodeResult is delivered
	// asynchronously on Results(). Returning an error aborts the session.
	OnUtteranceEnd(startSeq, endSeq uint64) error

	// Results returns the channel that carries decode outputs. The channel is
	// buffered; producers drop on full only for explicitly non-critical cases
	// (implementation-defined). The channel is closed by Close after all
	// in-flight decodes have been drained.
	Results() <-chan DecodeResult

	// Close initiates shutdown. It must be idempotent. It returns after all
	// internal goroutines have exited and the Results channel has been closed.
	// The engine must not produce further results after Close returns.
	Close() error
}
