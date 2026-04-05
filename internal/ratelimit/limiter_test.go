package ratelimit_test

import (
	"testing"

	"github.com/speechmux/core/internal/ratelimit"
)

func TestKeyedLimiter_AllowsUpToBurst(t *testing.T) {
	l := ratelimit.NewKeyedLimiter(1.0, 3)
	for i := range 3 {
		if !l.Allow("key") {
			t.Errorf("call %d: expected Allow=true", i)
		}
	}
	if l.Allow("key") {
		t.Error("4th call: expected Allow=false (burst exhausted)")
	}
}

func TestKeyedLimiter_IndependentKeys(t *testing.T) {
	l := ratelimit.NewKeyedLimiter(1.0, 1)
	if !l.Allow("a") {
		t.Error("first call for 'a' should be allowed")
	}
	if !l.Allow("b") {
		t.Error("first call for 'b' should be allowed (independent bucket)")
	}
	if l.Allow("a") {
		t.Error("second call for 'a' should be denied")
	}
}

func TestKeyedLimiter_ZeroRateAlwaysAllows(t *testing.T) {
	l := ratelimit.NewKeyedLimiter(0, 0)
	for range 10 {
		if !l.Allow("any") {
			t.Error("zero-rate limiter should always allow")
		}
	}
}
