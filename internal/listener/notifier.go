package listener

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bo0tzz/pgmqtt/internal/engine"
)

// PgTakeoverNotifier emits the takeover signal on pgmqtt_takeover_<broker_id>.
type PgTakeoverNotifier struct {
	pool *pgxpool.Pool
}

// NewTakeoverNotifier returns a TakeoverNotifier emitting pg_notify on
// pgmqtt_takeover_<broker_id> with the client_id as payload.
func NewTakeoverNotifier(pool *pgxpool.Pool) engine.TakeoverNotifier {
	return &PgTakeoverNotifier{pool: pool}
}

func (n *PgTakeoverNotifier) NotifyTakeover(ctx context.Context, brokerID uuid.UUID, clientID string) error {
	_, err := n.pool.Exec(ctx,
		`SELECT pg_notify($1, $2)`, "pgmqtt_takeover_"+brokerID.String(), clientID)
	return err
}

// PgQuotaNotifier signals "this client overflowed its per-conn delivery cap;
// disconnect them" on pgmqtt_quota_<broker_id>.
type PgQuotaNotifier struct {
	pool *pgxpool.Pool
}

// NewQuotaNotifier returns a QuotaNotifier emitting pg_notify on
// pgmqtt_quota_<broker_id> with the client_id as payload.
func NewQuotaNotifier(pool *pgxpool.Pool) engine.QuotaNotifier {
	return &PgQuotaNotifier{pool: pool}
}

func (n *PgQuotaNotifier) NotifyQuota(ctx context.Context, brokerID uuid.UUID, clientID string) error {
	_, err := n.pool.Exec(ctx,
		`SELECT pg_notify($1, $2)`, "pgmqtt_quota_"+brokerID.String(), clientID)
	return err
}
