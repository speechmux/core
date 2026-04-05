// Package ratelimit provides a per-key token-bucket rate limiter.
package ratelimit

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	defaultTTL           = 5 * time.Minute
	defaultPruneInterval = time.Minute
)

type entry struct {
	lim      *rate.Limiter
	lastSeen time.Time
}

// KeyedLimiter applies an independent token-bucket to each string key.
// Stale keys are pruned automatically to cap memory usage.
type KeyedLimiter struct {
	mu       sync.Mutex
	entries  map[string]*entry
	rps      rate.Limit
	burst    int
	ttl      time.Duration
	nextPrune time.Time
}

// NewKeyedLimiter creates a KeyedLimiter with the given per-key rate (events/sec)
// and burst size. Pass burst=0 to default to ceil(rps).
func NewKeyedLimiter(rps float64, burst int) *KeyedLimiter {
	b := burst
	if b <= 0 {
		b = max(1, int(rps))
	}
	return &KeyedLimiter{
		entries:   make(map[string]*entry),
		rps:       rate.Limit(rps),
		burst:     b,
		ttl:       defaultTTL,
		nextPrune: time.Now().Add(defaultPruneInterval),
	}
}

// Allow returns true if the key has budget remaining. Thread-safe.
// If the limiter was created with rate=0, it always allows.
func (l *KeyedLimiter) Allow(key string) bool {
	if l.rps <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	l.pruneIfDue(now)

	e, ok := l.entries[key]
	if !ok {
		e = &entry{lim: rate.NewLimiter(l.rps, l.burst)}
		l.entries[key] = e
	}
	e.lastSeen = now
	return e.lim.Allow()
}

func (l *KeyedLimiter) pruneIfDue(now time.Time) {
	if now.Before(l.nextPrune) {
		return
	}
	cutoff := now.Add(-l.ttl)
	for k, e := range l.entries {
		if e.lastSeen.Before(cutoff) {
			delete(l.entries, k)
		}
	}
	l.nextPrune = now.Add(defaultPruneInterval)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
