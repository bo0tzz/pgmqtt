package engine

import (
	"context"
	"time"
)

// EndOrphanedSessionsForTest runs one pass of the orphaned-session sweep.
func (e *Engine) EndOrphanedSessionsForTest(ctx context.Context) error {
	return e.endOrphanedSessions(ctx)
}

// SetWriteTimeoutForTest shortens the per-packet write timeout. Not safe
// alongside parallel tests: call it from a test that does not use
// t.Parallel, and restore before returning.
func SetWriteTimeoutForTest(d time.Duration) (restore func()) {
	prev := writeTimeout
	writeTimeout = d
	return func() { writeTimeout = prev }
}
