package stream

import (
	"context"
	"errors"
	"testing"
	"time"
)

// sendVAD pushes a single VADFrame into the channel without blocking.
func sendVAD(ch chan<- VADFrame, seq uint64, isSpeech bool, prob float32) {
	ch <- VADFrame{SequenceNumber: seq, IsSpeech: isSpeech, SpeechProbability: prob}
}

func TestEPDController_UtteranceEndOnSilence(t *testing.T) {
	buf := NewAudioRingBuffer(10)
	buf.Append(1, []byte("audio1"))
	buf.Append(2, []byte("audio2"))
	buf.Append(3, []byte("audio3"))

	vadCh := make(chan VADFrame, 8)
	got := make(chan []byte, 1)

	// Short silence: 50 ms
	epd := &EPDController{
		silenceDuration: 50 * time.Millisecond,
		vadFrameTimeout: 3 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		_ = epd.Run(ctx, vadCh, buf, func(audio []byte, _, _ uint64) {
			got <- audio
		})
	}()

	// Speech frames, then silence.
	sendVAD(vadCh, 1, true, 0.9)
	sendVAD(vadCh, 2, true, 0.9)
	sendVAD(vadCh, 3, false, 0.1) // triggers silence timer

	select {
	case audio := <-got:
		if len(audio) == 0 {
			t.Fatal("onUtteranceEnd called with empty audio")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("onUtteranceEnd not called within timeout")
	}
}

func TestEPDController_WatermarkAdvanced(t *testing.T) {
	buf := NewAudioRingBuffer(10)
	buf.Append(1, []byte("x"))

	vadCh := make(chan VADFrame, 4)
	epd := &EPDController{
		silenceDuration: 100 * time.Millisecond,
		vadFrameTimeout: 3 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = epd.Run(ctx, vadCh, buf, func(_ []byte, _, _ uint64) {})
	}()

	sendVAD(vadCh, 1, false, 0.1)
	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)

	if got := buf.ConfirmedWatermark(); got != 1 {
		t.Fatalf("watermark = %d, want 1", got)
	}
}

func TestEPDController_VADFrameTimeout(t *testing.T) {
	buf := NewAudioRingBuffer(10)
	vadCh := make(chan VADFrame) // nothing will be sent

	epd := &EPDController{
		silenceDuration: 500 * time.Millisecond,
		vadFrameTimeout: 50 * time.Millisecond, // very short for test speed
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := epd.Run(ctx, vadCh, buf, func(_ []byte, _, _ uint64) {})
	if !errors.Is(err, ErrVADFrameTimeout) {
		t.Fatalf("Run returned %v, want ErrVADFrameTimeout", err)
	}
}

func TestEPDController_ContextCancelledClean(t *testing.T) {
	buf := NewAudioRingBuffer(10)
	vadCh := make(chan VADFrame, 1)

	epd := &EPDController{
		silenceDuration: 500 * time.Millisecond,
		vadFrameTimeout: 3 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- epd.Run(ctx, vadCh, buf, func(_ []byte, _, _ uint64) {})
	}()

	cancel()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected error on ctx cancel: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not exit after ctx cancel")
	}
}

func TestEPDController_ClosedChannelExitsClean(t *testing.T) {
	buf := NewAudioRingBuffer(10)
	vadCh := make(chan VADFrame, 1)

	epd := &EPDController{
		silenceDuration: 500 * time.Millisecond,
		vadFrameTimeout: 3 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	close(vadCh) // signal stream ended

	err := epd.Run(ctx, vadCh, buf, func(_ []byte, _, _ uint64) {})
	if err != nil {
		t.Fatalf("closed channel: unexpected error %v", err)
	}
}

func TestEPDController_NoUtteranceWhenNeverSpeech(t *testing.T) {
	buf := NewAudioRingBuffer(10)
	buf.Append(1, []byte("audio"))

	vadCh := make(chan VADFrame, 4)
	called := false

	epd := &EPDController{
		silenceDuration: 30 * time.Millisecond,
		vadFrameTimeout: 3 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_ = epd.Run(ctx, vadCh, buf, func(_ []byte, _, _ uint64) { called = true })
	}()

	sendVAD(vadCh, 1, false, 0.1)
	sendVAD(vadCh, 2, false, 0.1)
	time.Sleep(100 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	if called {
		t.Fatal("onUtteranceEnd should not be called when speech never active")
	}
}
