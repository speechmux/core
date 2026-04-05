package stream

import "strings"

// punctBoundaries lists characters that serve as stable commit boundaries in
// languages that lack inter-word spaces (e.g. Chinese, Japanese).
const punctBoundaries = ".,?!\u3001\u3002\uff0c\uff01\uff1f\u2026"

// lcpWindowRunes is the maximum number of runes used when computing the
// longest-common-prefix against committed_so_far in long sessions.
// Once committed_so_far exceeds this length, only its tail is compared to
// avoid O(n) rune conversions on every partial decode (design doc §10.8).
const lcpWindowRunes = 200

// ResultAssembler tracks committed_text / unstable_text across successive
// partial and final decode results for a single session.
//
// Rules (design doc §8):
//   - committed_so_far is monotonically non-decreasing (rune count).
//   - On is_final, the full decoded text is committed; state resets for the next utterance.
//   - LCP comparison uses rune (Unicode code point) granularity, not bytes.
//
// ResultAssembler is not safe for concurrent use; callers must serialise access.
type ResultAssembler struct {
	committedSoFar string // confirmed text; never shrinks
	previousText   string // the full merged text from the previous partial
}

// NewResultAssembler returns a zeroed ResultAssembler.
func NewResultAssembler() *ResultAssembler { return &ResultAssembler{} }

// Reset clears all internal state. Call between utterances if the session
// continues after a final result.
func (a *ResultAssembler) Reset() {
	a.committedSoFar = ""
	a.previousText = ""
}

// Update advances the commit state given the latest decoded text and returns
// (committedText, unstableText).
//
//   - If isFinal is true, currentText becomes the full committed text and the
//     unstable portion is empty.
//   - If isFinal is false, the LCP of previousText and currentText (merged with
//     committed prefix) is used to advance the commit boundary.
func (a *ResultAssembler) Update(currentText string, isFinal bool) (committed, unstable string) {
	current := strings.TrimSpace(currentText)

	if isFinal {
		if current == "" {
			// Final decode with empty result — keep whatever was committed.
			current = a.committedSoFar
		}
		// Merge so the final never regresses behind what was committed.
		merged := mergeTranscript(a.committedSoFar, current)
		a.committedSoFar = merged
		a.previousText = ""
		return a.committedSoFar, ""
	}

	// --- Partial decode path ---

	if a.previousText == "" {
		// First partial: nothing is committed yet.
		a.previousText = current
		return a.committedSoFar, current
	}

	merged := mergeTranscript(a.committedSoFar, current)

	next := commitFromPartials(a.committedSoFar, a.previousText, merged)
	if runeLen(next) > runeLen(a.committedSoFar) {
		a.committedSoFar = next
	}
	a.previousText = merged

	if a.committedSoFar == "" {
		return "", merged
	}

	// Extract unstable portion (rune-safe).
	committedRunes := []rune(a.committedSoFar)
	mergedRunes := []rune(merged)
	if len(mergedRunes) <= len(committedRunes) {
		return a.committedSoFar, ""
	}
	unstable = strings.TrimLeft(string(mergedRunes[len(committedRunes):]), " \t\n")
	return a.committedSoFar, unstable
}

// --- helpers ----------------------------------------------------------------

// mergeTranscript prepends the committed prefix to the decoded text unless
// decoded already starts with the committed prefix.
func mergeTranscript(committed, decoded string) string {
	committed = strings.TrimSpace(committed)
	decoded = strings.TrimSpace(decoded)
	if committed == "" {
		return decoded
	}
	if decoded == "" {
		return committed
	}
	if strings.HasPrefix(decoded, committed) {
		return decoded
	}
	return committed + " " + decoded
}

// commitFromPartials returns the longest stable commit boundary in the LCP of
// previous and current, provided it exceeds the existing committed length.
//
//  1. Whitespace boundary (space / tab / newline) — preferred for Latin.
//  2. Punctuation boundary — CJK support.
//  3. Full LCP fallback (character-boundary commit for non-space CJK text).
//
// All index arithmetic uses rune granularity, not bytes.
func commitFromPartials(committed, previous, current string) string {
	if previous == "" || current == "" {
		return committed
	}

	prevRunes := windowRunes(previous)
	curRunes := windowRunes(current)

	lcpLen := longestCommonPrefixRunes(prevRunes, curRunes)
	committedLen := runeLen(committed)

	if lcpLen <= committedLen {
		return committed
	}

	// Work entirely on rune slices to avoid byte-index confusion with CJK text.
	candidate := curRunes[:lcpLen]

	// 1. Whitespace boundary (prefer for Latin text).
	wb := lastWhitespaceBoundaryRunes(candidate)
	if wb > committedLen {
		return strings.TrimSpace(string(candidate[:wb]))
	}

	// 2. Punctuation boundary (CJK: 。、，！？…).
	pb := lastPunctuationBoundaryRunes(candidate)
	if pb >= committedLen {
		return strings.TrimSpace(string(candidate[:pb+1]))
	}

	// 3. Full LCP fallback (no word boundaries — commit at the rune level).
	fallback := strings.TrimSpace(string(candidate))
	if runeLen(fallback) > committedLen {
		return fallback
	}
	return committed
}

// windowRunes returns the rune slice of s, but only the last lcpWindowRunes
// runes if s is longer than lcpWindowRunes. This bounds the cost of LCP
// comparison in long sessions (design doc §10.8).
func windowRunes(s string) []rune {
	r := []rune(s)
	if len(r) > lcpWindowRunes {
		return r[len(r)-lcpWindowRunes:]
	}
	return r
}

// longestCommonPrefixRunes returns the length of the longest common prefix of
// a and b, measured in runes.
func longestCommonPrefixRunes(a, b []rune) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// runeLen returns the rune count of s.
func runeLen(s string) int { return len([]rune(s)) }

// lastWhitespaceBoundaryRunes returns the rune index of the last whitespace
// character (space, tab, newline) in candidate, or -1 if none.
// The caller should slice candidate[:result] to get the text before the space.
func lastWhitespaceBoundaryRunes(candidate []rune) int {
	for i := len(candidate) - 1; i >= 0; i-- {
		if candidate[i] == ' ' || candidate[i] == '\t' || candidate[i] == '\n' {
			return i
		}
	}
	return -1
}

// lastPunctuationBoundaryRunes returns the rune index of the last punctuation
// boundary character in candidate, or -1 if none.
// The caller should slice candidate[:result+1] to include the punctuation char.
func lastPunctuationBoundaryRunes(candidate []rune) int {
	punc := []rune(punctBoundaries)
	for i := len(candidate) - 1; i >= 0; i-- {
		for _, p := range punc {
			if candidate[i] == p {
				return i
			}
		}
	}
	return -1
}
