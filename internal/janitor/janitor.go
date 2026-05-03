// Package janitor runs the dead-Pod scan, will fan-out, and orphan-message
// sweep. Exactly one Pod runs the janitor at a time, gated by leader.Leader.
package janitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bo0tzz/pgmqtt/internal/engine"
	"github.com/bo0tzz/pgmqtt/internal/metrics"
)

// Run loops until ctx is cancelled. While leader.IsLeader is true (gated by
// the Acquired/Lost channels) it runs Tick every interval.
type Janitor struct {
	pool     *pgxpool.Pool
	eng      *engine.Engine
	logger   *slog.Logger
	interval time.Duration

	// orphanGrace is the minimum age of a message with zero deliveries before
	// the sweep deletes it. Keeps very-recent publishes safe from races.
	orphanGrace time.Duration

	// inboundQoS2Grace is the minimum age of an inbound_qos2 row before the
	// sweep evicts it for sessions that are currently disconnected. Protects
	// against pathological clients that send PUBLISH but never PUBREL.
	inboundQoS2Grace time.Duration

	metrics *metrics.Metrics
}

// New constructs a Janitor.
func New(pool *pgxpool.Pool, eng *engine.Engine, logger *slog.Logger) *Janitor {
	return &Janitor{
		pool:   pool,
		eng:    eng,
		logger: logger,
		// 1s default. Time-bound operations (will-delay, session-expire,
		// retained-expire) all key off this. The dead-broker advisory-lock
		// scan rides along; it's a handful of point queries against an
		// already-indexed `broker_id` column, so 1s is fine.
		interval:         1 * time.Second,
		orphanGrace:      10 * time.Minute,
		inboundQoS2Grace: 1 * time.Hour,
	}
}

// SetInterval overrides the default tick interval. Useful for tests.
func (j *Janitor) SetInterval(d time.Duration) { j.interval = d }

// SetOrphanGrace overrides the orphan-message grace period.
func (j *Janitor) SetOrphanGrace(d time.Duration) { j.orphanGrace = d }

// SetInboundQoS2Grace overrides the inbound_qos2 grace period.
func (j *Janitor) SetInboundQoS2Grace(d time.Duration) { j.inboundQoS2Grace = d }

// SetMetrics installs a Metrics instance for janitor counters. Optional.
func (j *Janitor) SetMetrics(m *metrics.Metrics) { j.metrics = m }

// Leader is the subset of leader.Leader the janitor depends on.
type Leader interface {
	Acquired() <-chan struct{}
	Lost() <-chan struct{}
	IsLeader() bool
}

// RunWith starts the loop, gated on leader.
func (j *Janitor) RunWith(ctx context.Context, leader Leader) {
	defer func() {
		if r := recover(); r != nil {
			j.logger.Error("janitor goroutine panic",
				"panic", r, "stack", string(debug.Stack()))
		}
	}()

	select {
	case <-ctx.Done():
		return
	case <-leader.Acquired():
	case <-leader.Lost():
		return
	}

	// runCtx cancels when leadership is lost so an in-flight tick's PG
	// queries don't keep running on an ex-leader pod. Combined with the
	// leader-write fence (task #84), this closes the double-fire window
	// for wills/expiries when leadership transitions mid-tick.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go func() {
		select {
		case <-leader.Lost():
			cancelRun()
		case <-runCtx.Done():
		}
	}()

	t := time.NewTicker(j.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-leader.Lost():
			return
		case <-t.C:
			if err := j.tickWithRecover(runCtx); err != nil &&
				!errors.Is(err, context.Canceled) {
				j.logger.Warn("janitor tick", "err", err)
			}
		}
	}
}

// tickWithRecover wraps a single Tick so a panic in any sub-job logs and
// returns an error instead of taking the whole pod down.
func (j *Janitor) tickWithRecover(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			j.logger.Error("janitor tick panic",
				"panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("janitor tick panic: %v", r)
		}
	}()
	return j.Tick(ctx)
}

// Tick runs one full scan cycle: dead-Pod detection + sweep.
func (j *Janitor) Tick(ctx context.Context) error {
	dead, err := j.findDeadBrokers(ctx)
	if err != nil {
		return err
	}
	for _, id := range dead {
		if err := j.handleDeadBroker(ctx, id); err != nil {
			j.logger.Warn("dead broker handling", "broker", id, "err", err)
			continue
		}
		if j.metrics != nil {
			j.metrics.DeadBrokersTotal.Inc()
		}
	}
	if err := j.fireDueWills(ctx); err != nil {
		j.logger.Warn("fire due wills", "err", err)
	}
	if err := j.expireSessions(ctx); err != nil {
		j.logger.Warn("expire sessions", "err", err)
	}
	if err := j.expireRetained(ctx); err != nil {
		j.logger.Warn("expire retained", "err", err)
	}
	if err := j.sweepInboundQoS2(ctx); err != nil {
		j.logger.Warn("sweep inbound qos2", "err", err)
	}
	if err := j.sweepOrphanDeliveries(ctx); err != nil {
		j.logger.Warn("sweep orphan deliveries", "err", err)
	}
	if err := j.refreshDeliveriesGauge(ctx); err != nil {
		j.logger.Warn("refresh deliveries gauge", "err", err)
	}
	return j.sweepOrphanMessages(ctx)
}

// refreshDeliveriesGauge re-populates pgmqtt_deliveries_inflight{state} once
// per tick. Cheap (one indexed COUNT GROUP BY query) and gives operators a
// continuous view of broker queue depth without touching the broker hot
// path.
func (j *Janitor) refreshDeliveriesGauge(ctx context.Context) error {
	if j.metrics == nil {
		return nil
	}
	rows, err := j.pool.Query(ctx, `SELECT state, count(*) FROM deliveries GROUP BY state`)
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := map[int]int{0: 0, 1: 0, 2: 0}
	for rows.Next() {
		var state int
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return err
		}
		seen[state] = n
	}
	for state, n := range seen {
		j.metrics.DeliveriesInflight.WithLabelValues(stateLabel(state)).Set(float64(n))
	}
	return rows.Err()
}

func stateLabel(state int) string {
	switch state {
	case 0:
		return "queued"
	case 1:
		return "inflight"
	case 2:
		return "awaiting_pubcomp"
	default:
		return "unknown"
	}
}

// fireDueWills publishes wills whose scheduled fire-time has arrived. We use
// a CTE that captures the OLD values via a self-join (UPDATE...RETURNING in
// Postgres returns the post-UPDATE row) and SELECT FOR UPDATE SKIP LOCKED so
// concurrent janitors don't double-fire.
func (j *Janitor) fireDueWills(ctx context.Context) error {
	tx, err := j.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT client_id, will_topic, will_payload, will_qos, will_retain, will_properties
		  FROM sessions
		 WHERE will_fire_at IS NOT NULL
		   AND will_fire_at <= now()
		   AND will_topic IS NOT NULL
		 FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		return err
	}
	type w struct {
		client  string
		topic   string
		payload []byte
		qos     int
		retain  bool
		props   []byte
	}
	var wills []w
	for rows.Next() {
		var x w
		if err := rows.Scan(&x.client, &x.topic, &x.payload, &x.qos, &x.retain, &x.props); err != nil {
			rows.Close()
			return err
		}
		wills = append(wills, x)
	}
	rows.Close()

	if len(wills) == 0 {
		return tx.Commit(ctx)
	}

	clientIDs := make([]string, len(wills))
	for i, w := range wills {
		clientIDs[i] = w.client
	}

	// Publish FIRST, clear will_* SECOND. Previously the order was
	// reversed: clear committed before PublishWill ran, so a crash
	// between them silently dropped the will. Inverting the order means
	// a crash between publish and clear surfaces as a duplicate-will on
	// the next janitor tick (the SKIP LOCKED query re-finds the row,
	// re-fires) — duplicate-will is the better failure than lost-will.
	//
	// We hold the FOR UPDATE SKIP LOCKED row locks across PublishWill
	// so concurrent janitors can't fire the same will twice within a
	// single tick. PublishWill opens its own publishCore tx on a
	// different conn — the outer lock just gates "who owns these rows
	// for this round."
	for _, w := range wills {
		if err := j.eng.PublishWill(ctx, w.topic, w.payload, byte(w.qos), w.retain, w.props); err != nil {
			j.logger.Warn("fire delayed will", "client", w.client, "err", err)
			continue
		}
		if j.metrics != nil {
			j.metrics.WillsFiredTotal.Inc()
		}
	}

	if _, err := tx.Exec(ctx, `
		UPDATE sessions SET will_fire_at=NULL,
		    will_topic=NULL, will_payload=NULL, will_qos=NULL,
		    will_retain=NULL, will_delay=NULL, will_properties=NULL
		 WHERE client_id = ANY($1)
	`, clientIDs); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// expireSessions deletes session rows whose v5 SessionExpiryInterval has
// elapsed. Subscriptions still cascade via FK; deliveries cascade was
// dropped in migration 0006 (MultiXact SLRU thrash), so we delete those
// explicitly in the same tx.
func (j *Janitor) expireSessions(ctx context.Context) error {
	tx, err := j.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT client_id FROM sessions
		 WHERE connected = false
		   AND session_expires_at IS NOT NULL
		   AND session_expires_at <= now()
		 FOR UPDATE
	`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) == 0 {
		return tx.Commit(ctx)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM deliveries WHERE client_id = ANY($1)`, ids); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM sessions WHERE client_id = ANY($1)`, ids); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if j.metrics != nil {
		j.metrics.SessionsExpired.Add(float64(len(ids)))
	}
	return nil
}

// sweepOrphanDeliveries deletes deliveries rows whose client_id no
// longer has a sessions row. Orphans accumulate when sessions are
// removed by an out-of-band path (operator psql, restore from backup);
// the in-broker delete paths clean explicitly. Rare in steady state.
func (j *Janitor) sweepOrphanDeliveries(ctx context.Context) error {
	_, err := j.pool.Exec(ctx, `
		DELETE FROM deliveries
		 WHERE NOT EXISTS (
		    SELECT 1 FROM sessions s WHERE s.client_id = deliveries.client_id
		 )
	`)
	return err
}

// expireRetained drops retained rows past their MessageExpiryInterval.
func (j *Janitor) expireRetained(ctx context.Context) error {
	_, err := j.pool.Exec(ctx, `
		DELETE FROM retained
		 WHERE expires_at IS NOT NULL AND expires_at <= now()
	`)
	return err
}

// sweepInboundQoS2 deletes inbound_qos2 rows that are older than the grace
// period AND belong to a currently-disconnected session. A v5 client that
// sends QoS-2 PUBLISH but never sends PUBREL would otherwise leave its
// (client_id, packet_id) tombstones forever. Connected sessions are left
// alone — those rows are still in-flight from the broker's perspective.
func (j *Janitor) sweepInboundQoS2(ctx context.Context) error {
	cutoff := time.Now().Add(-j.inboundQoS2Grace)
	ct, err := j.pool.Exec(ctx, `
		DELETE FROM inbound_qos2 q
		 WHERE q.received_at < $1
		   AND EXISTS (
		         SELECT 1 FROM sessions s
		          WHERE s.client_id = q.client_id
		            AND s.connected = false
		       )
	`, cutoff)
	if err != nil {
		return err
	}
	if ct.RowsAffected() > 0 {
		j.logger.Debug("inbound_qos2 sweep", "deleted", ct.RowsAffected())
	}
	return nil
}

// findDeadBrokers selects every distinct broker_id currently referenced in
// sessions and tries pg_try_advisory_lock per id. A successful try means the
// owning Pod is gone.
func (j *Janitor) findDeadBrokers(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := j.pool.Query(ctx,
		`SELECT DISTINCT broker_id FROM sessions WHERE broker_id IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		candidates = append(candidates, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var dead []uuid.UUID
	for _, id := range candidates {
		var got bool
		err := j.pool.QueryRow(ctx,
			`SELECT pg_try_advisory_lock(hashtextextended($1, 0))`,
			"pgmqtt:broker:"+id.String()).Scan(&got)
		if err != nil {
			j.logger.Warn("try lock", "broker", id, "err", err)
			continue
		}
		if got {
			dead = append(dead, id)
		}
	}
	return dead, nil
}

// handleDeadBroker fires wills for sessions owned by a dead broker, clears
// ownership, and releases the temporarily-held advisory lock.
func (j *Janitor) handleDeadBroker(ctx context.Context, brokerID uuid.UUID) error {
	defer func() {
		_, _ = j.pool.Exec(ctx,
			`SELECT pg_advisory_unlock(hashtextextended($1, 0))`,
			"pgmqtt:broker:"+brokerID.String())
	}()

	rows, err := j.pool.Query(ctx, `
		SELECT client_id, will_topic, will_payload, will_qos, will_retain, will_properties
		  FROM sessions
		 WHERE broker_id = $1
		   AND will_topic IS NOT NULL
	`, brokerID)
	if err != nil {
		return err
	}
	defer rows.Close()
	type will struct {
		client  string
		topic   string
		payload []byte
		qos     int
		retain  bool
		props   []byte
	}
	var wills []will
	for rows.Next() {
		var w will
		if err := rows.Scan(&w.client, &w.topic, &w.payload, &w.qos, &w.retain, &w.props); err != nil {
			return err
		}
		wills = append(wills, w)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()

	for _, w := range wills {
		if err := j.eng.PublishWill(ctx, w.topic, w.payload, byte(w.qos), w.retain, w.props); err != nil {
			j.logger.Warn("fire will from dead broker", "client", w.client, "err", err)
		}
	}

	_, err = j.pool.Exec(ctx,
		`UPDATE sessions SET connected=false, broker_id=NULL,
		    will_topic=NULL, will_payload=NULL, will_qos=NULL,
		    will_retain=NULL, will_delay=NULL, will_properties=NULL
		 WHERE broker_id=$1`, brokerID)
	return err
}

// sweepOrphanMessages deletes messages with no referencing deliveries that are
// older than the grace period. (Deliveries cascade-delete on session cleanup,
// so abandoned messages accumulate without a sweep.)
func (j *Janitor) sweepOrphanMessages(ctx context.Context) error {
	tx, err := j.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	cutoff := time.Now().Add(-j.orphanGrace)
	ct, err := tx.Exec(ctx, `
		DELETE FROM messages m
		 WHERE created_at < $1
		   AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.message_id = m.id)
	`, cutoff)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if ct.RowsAffected() > 0 {
		j.logger.Debug("orphan sweep", "deleted", ct.RowsAffected())
	}
	return nil
}

// Compile-time guard to keep handler errors from being unused.
var _ = errors.New
