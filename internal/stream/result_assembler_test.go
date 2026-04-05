package stream

import (
	"testing"
)

// ── English (whitespace boundary) ────────────────────────────────────────────

func TestResultAssembler_EnglishProgression(t *testing.T) {
	a := NewResultAssembler()

	committed, unstable := a.Update("hello wor", false)
	if committed != "" {
		t.Errorf("want committed='', got %q", committed)
	}
	if unstable != "hello wor" {
		t.Errorf("want unstable='hello wor', got %q", unstable)
	}

	committed, unstable = a.Update("hello world again", false)
	if committed != "hello" {
		t.Errorf("want committed='hello', got %q", committed)
	}
	if unstable != "world again" {
		t.Errorf("want unstable='world again', got %q", unstable)
	}

	committed, unstable = a.Update("hello world again please", false)
	if committed != "hello world" {
		t.Errorf("want committed='hello world', got %q", committed)
	}
	if unstable != "again please" {
		t.Errorf("want unstable='again please', got %q", unstable)
	}

	committed, unstable = a.Update("hello world again please", true)
	if committed != "hello world again please" {
		t.Errorf("want committed='hello world again please', got %q", committed)
	}
	if unstable != "" {
		t.Errorf("want unstable='', got %q", unstable)
	}
}

// ── Korean (CJK, no spaces — LCP fallback) ───────────────────────────────────

func TestResultAssembler_KoreanCJKNoSpaces(t *testing.T) {
	a := NewResultAssembler()

	first := "안녕하세" // 안녕하세
	second := "안녕하세요" // 안녕하세요

	committed, unstable := a.Update(first, false)
	if committed != "" {
		t.Errorf("want committed='', got %q", committed)
	}
	if unstable != first {
		t.Errorf("want unstable=%q, got %q", first, unstable)
	}

	committed, unstable = a.Update(second, false)
	if committed != first {
		t.Errorf("want committed=%q, got %q", first, committed)
	}
	wantUnstable := "요"
	if unstable != wantUnstable {
		t.Errorf("want unstable=%q, got %q", wantUnstable, unstable)
	}
}

// ── Chinese (punctuation boundary) ───────────────────────────────────────────

func TestResultAssembler_ChinesePunctuationBoundary(t *testing.T) {
	a := NewResultAssembler()

	first := "你好世界。"       // 你好世界。
	second := "你好世界。今天" // 你好世界。今天

	committed, unstable := a.Update(first, false)
	if committed != "" {
		t.Errorf("want committed='', got %q", committed)
	}
	if unstable != first {
		t.Errorf("want unstable=%q, got %q", first, unstable)
	}

	committed, unstable = a.Update(second, false)
	if committed != first {
		t.Errorf("want committed=%q, got %q", first, committed)
	}
	wantUnstable := "今天"
	if unstable != wantUnstable {
		t.Errorf("want unstable=%q, got %q", wantUnstable, unstable)
	}
}

// ── Monotonic committed guarantee ────────────────────────────────────────────

func TestResultAssembler_CommittedNeverShrinks(t *testing.T) {
	a := NewResultAssembler()

	a.Update("hello world today", false)
	committed1, _ := a.Update("hello world tomorrow", false)

	// Simulate a regression in the decoded text — committed must not shrink.
	committed2, _ := a.Update("hello", false)
	if runeLen(committed2) < runeLen(committed1) {
		t.Errorf("committed shrank from %q to %q", committed1, committed2)
	}
}

// ── Final decode resets state ─────────────────────────────────────────────────

func TestResultAssembler_FinalResetsState(t *testing.T) {
	a := NewResultAssembler()

	a.Update("hello wor", false)
	committed, unstable := a.Update("hello world", true)
	if committed != "hello world" {
		t.Errorf("want committed='hello world', got %q", committed)
	}
	if unstable != "" {
		t.Errorf("want unstable='', got %q", unstable)
	}

	// After Reset, the next partial starts fresh.
	a.Reset()
	committed, unstable = a.Update("new utterance", false)
	if committed != "" {
		t.Errorf("after Reset: want committed='', got %q", committed)
	}
	if unstable != "new utterance" {
		t.Errorf("after Reset: want unstable='new utterance', got %q", unstable)
	}
}

// ── Empty decode text ─────────────────────────────────────────────────────────

func TestResultAssembler_EmptyText(t *testing.T) {
	a := NewResultAssembler()

	committed, unstable := a.Update("", false)
	if committed != "" || unstable != "" {
		t.Errorf("empty partial: want ('',''), got (%q,%q)", committed, unstable)
	}

	committed, unstable = a.Update("", true)
	if committed != "" || unstable != "" {
		t.Errorf("empty final: want ('',''), got (%q,%q)", committed, unstable)
	}
}

// ── mergeTranscript helper ────────────────────────────────────────────────────

func TestMergeTranscript_PrefixAlreadyPresent(t *testing.T) {
	result := mergeTranscript("hello", "hello world")
	if result != "hello world" {
		t.Errorf("want 'hello world', got %q", result)
	}
}

func TestMergeTranscript_NewTextDoesNotStartWithPrefix(t *testing.T) {
	result := mergeTranscript("hello", "world")
	if result != "hello world" {
		t.Errorf("want 'hello world', got %q", result)
	}
}

// ── LCP window (long-session GC protection) ──────────────────────────────────

func TestWindowRunes_CapAt200(t *testing.T) {
	long := make([]rune, 300)
	for i := range long {
		long[i] = 'a'
	}
	s := string(long)
	window := windowRunes(s)
	if len(window) != lcpWindowRunes {
		t.Errorf("want window len=%d, got %d", lcpWindowRunes, len(window))
	}
}

func TestWindowRunes_ShortStringUnchanged(t *testing.T) {
	s := "hello"
	window := windowRunes(s)
	if string(window) != s {
		t.Errorf("want %q, got %q", s, string(window))
	}
}
