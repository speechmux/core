//go:build !production

package session

import "context"

// NewTestSession creates a minimal Session for use in tests outside this package.
// The session is live (ctx is cancellable) and has buffered AudioInCh/ResultCh.
// This function is excluded from production builds via the "production" build tag.
func NewTestSession(ctx context.Context, id string) *Session {
	return newSession(ctx, id, &SessionInfo{
		VADSilence:   0.5,
		VADThreshold: 0.5,
		Language:     "ko",
	})
}
