package janitor

import (
	"context"

	"github.com/google/uuid"
)

// FireDueWillsForTest exposes fireDueWills for concurrency tests in the
// janitor_test package. Production callers use Tick().
func (j *Janitor) FireDueWillsForTest(ctx context.Context) error {
	return j.fireDueWills(ctx)
}

// HandleDeadBrokerForTest exposes handleDeadBroker for concurrency tests.
// Returns (claimed, err) — claimed is true iff this caller won the
// per-broker advisory lock and performed the takeover.
func (j *Janitor) HandleDeadBrokerForTest(ctx context.Context, brokerID uuid.UUID) (bool, error) {
	return j.handleDeadBroker(ctx, brokerID)
}
