package stream

// Submit() and the batch pending semaphore were removed from DecodeScheduler
// when FairDecodeDispatcher took over the batch path. Tests for those
// behaviours now live in fair_dispatch_test.go.
//
// The asSTTError helper is kept here for reuse by fair_dispatch_test.go.

import (
	"errors"

	sttErrors "github.com/speechmux/core/internal/errors"
)

func asSTTError(err error, target **sttErrors.STTError) bool {
	return errors.As(err, target)
}
