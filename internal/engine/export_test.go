package engine

import "context"

// EndOrphanedSessionsForTest runs one pass of the orphaned-session sweep.
func (e *Engine) EndOrphanedSessionsForTest(ctx context.Context) error {
	return e.endOrphanedSessions(ctx)
}
