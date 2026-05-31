// Package janitor runs the dead-Pod scan, will fan-out, and orphan-message
// sweep. Every Pod runs an independent Tick loop. The work is concurrency-
// safe by construction:
//
//   - findDeadBrokerCandidates + handleDeadBroker uses pg_try_advisory_lock
//     per broker_id, so only one Pod claims any given dead broker at a time.
//   - fireDueWills uses SELECT … FOR UPDATE SKIP LOCKED so concurrent Pods
//     don't double-fire the same will.
//   - expireSessions uses SELECT … FOR UPDATE; the row locks serialise
//     concurrent janitors and the DELETE is idempotent.
//   - expireRetained / sweepInboundQoS2 / sweepOrphanDeliveries /
//     sweepOrphanMessages are pure idempotent DELETEs — each row is
//     deleted at most once across Pods.
//   - refreshDeliveriesGauge / refreshStateGauges are read-only. Each Pod
//     observes the same state, sets its own Prometheus gauge; aggregation
//     across Pods (Prom max()) gives the right cluster-wide value.
//
// # Stratified intervals
//
// The base ticker fires every j.interval (default 1s, the GCD of all
// per-job intervals). Each sub-job carries its own minimum interval and
// a lastRun timestamp; on each base tick the loop visits every job and
// fires it iff (now - lastRun) >= job.interval. This drops idle DB churn
// ~5× vs the naive "every-job-every-tick" model without losing
// time-bound spec precision:
//
//	fire_due_wills          1s   Paho test_will_delay asserts within 1s
//	expire_sessions         5s   SessionExpiryInterval is minutes-hours
//	expire_retained         5s   MessageExpiryInterval is seconds-grade
//	find_dead_brokers       5s   broker keepalive is 30s anyway
//	handle_dead_broker      —    fired in the same tick as find_dead_brokers
//	refresh_deliveries_gauge 10s sub-minute observability gauge
//	refresh_state_gauges    10s  sub-minute observability gauge
//	sweep_inbound_qos2      30s  grace 1h default
//	sweep_orphan_deliveries 30s  grace 10min default
//	sweep_orphan_messages   30s  grace 10min default
//
// At idle on a 3-pod cluster this is ~3.4 PG queries/sec averaged
// (1Hz fire_due_wills × 3 pods + 0.2Hz × 3 jobs × 3 pods + 0.1Hz × 2
// × 3 + 0.033Hz × 3 × 3) instead of 33/sec.
//
// Tests can collapse stratification with SetJobIntervals (set every job
// to 0 → "fire on every tick"); SetInterval still overrides the base
// cadence.
//
// Cost of running on every Pod: even with stratification, all jobs are
// idempotent across Pods (advisory locks, SKIP LOCKED, pure DELETEs)
// so the work is correct on every Pod. If a deployment scales past
// hundreds of Pods, an opt-in pg_try_advisory_lock(TICK_KEY) prefix
// on Tick would rate-limit cluster-wide.
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

// Job names used as the per-job metric label and as the key for
// SetJobIntervals. Keep in sync with the table in the package doc.
const (
	JobFindDeadBrokers       = "find_dead_brokers"
	JobHandleDeadBroker      = "handle_dead_broker"
	JobFireDueWills          = "fire_due_wills"
	JobExpireSessions        = "expire_sessions"
	JobExpireRetained        = "expire_retained"
	JobSweepInboundQoS2      = "sweep_inbound_qos2"
	JobSweepOrphanDeliveries = "sweep_orphan_deliveries"
	JobRefreshDeliveriesGauge = "refresh_deliveries_gauge"
	JobRefreshStateGauges    = "refresh_state_gauges"
	JobSweepOrphanMessages   = "sweep_orphan_messages"
	JobRefreshNotifyQueue    = "refresh_notify_queue"
)

// defaultJobIntervals defines the per-job cadence. The base ticker fires
// at the GCD (1s by default); a job whose interval is 0 fires on every
// tick (used by tests via SetJobIntervals).
var defaultJobIntervals = map[string]time.Duration{
	JobFindDeadBrokers:        5 * time.Second,
	JobFireDueWills:           1 * time.Second,
	// Paho test_session_expiry sleeps 6 s after a disconnect with
	// SessionExpiryInterval=5 and asserts the session is gone on
	// reconnect. With expire_sessions at 5 s, the next tick after
	// expiry can arrive >5 s late depending on phase, blowing the
	// 1 s slack the test gives. Tighten to 1 s so expiry is honored
	// within the spec's <2× tolerance and the test's window.
	JobExpireSessions:         1 * time.Second,
	JobExpireRetained:         5 * time.Second,
	JobSweepInboundQoS2:       30 * time.Second,
	JobSweepOrphanDeliveries:  30 * time.Second,
	JobRefreshDeliveriesGauge: 10 * time.Second,
	JobRefreshStateGauges:     10 * time.Second,
	JobSweepOrphanMessages:    30 * time.Second,
	JobRefreshNotifyQueue:     10 * time.Second,
}

// Run loops until ctx is cancelled, running Tick every interval on this Pod.
type Janitor struct {
	pool     *pgxpool.Pool
	eng      *engine.Engine
	logger   *slog.Logger
	interval time.Duration

	// jobIntervals maps job name → minimum cadence. A job fires on a
	// given Tick iff (now - lastRun[job]) >= jobIntervals[job]. Initialised
	// from defaultJobIntervals; tests override via SetJobIntervals.
	jobIntervals map[string]time.Duration
	lastRun      map[string]time.Time

	// nowFn returns the current time. Defaults to time.Now; tests can
	// substitute a fake clock to exercise stratified intervals without
	// real sleeps.
	nowFn func() time.Time

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
	intervals := make(map[string]time.Duration, len(defaultJobIntervals))
	for k, v := range defaultJobIntervals {
		intervals[k] = v
	}
	return &Janitor{
		pool:   pool,
		eng:    eng,
		logger: logger,
		// 1s base tick = GCD of stratified per-job intervals. The shortest
		// per-job cadence is fire_due_wills @ 1s (Paho v5 test_will_delay
		// asserts within 1s of WillDelayInterval); cleanup jobs run on
		// 5s/10s/30s cadences via the per-job stratification below, so
		// idle DB churn is ~5× lower than naive "every-job-every-tick".
		// Override via PGMQTT_JANITOR_INTERVAL_MS or Janitor.SetInterval.
		interval:         1 * time.Second,
		jobIntervals:     intervals,
		lastRun:          make(map[string]time.Time),
		nowFn:            time.Now,
		orphanGrace:      10 * time.Minute,
		inboundQoS2Grace: 1 * time.Hour,
	}
}

// SetInterval overrides the default base tick interval. Useful for tests.
func (j *Janitor) SetInterval(d time.Duration) { j.interval = d }

// beginTxTimed wraps pool.BeginTx and observes pgmqtt_pgx_acquire_seconds.
// Counterpart to engine.beginTxTimed — same purpose, different pool owner.
func (j *Janitor) beginTxTimed(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	start := time.Now()
	tx, err := j.pool.BeginTx(ctx, opts)
	j.metrics.ObservePgxAcquire(time.Since(start))
	return tx, err
}

// acquireTimed wraps pool.Acquire and observes pgmqtt_pgx_acquire_seconds.
// handleDeadBroker holds a dedicated conn for the lock+unlock pair so it
// can't go through BeginTx; this is the one Acquire site in the broker.
func (j *Janitor) acquireTimed(ctx context.Context) (*pgxpool.Conn, error) {
	start := time.Now()
	conn, err := j.pool.Acquire(ctx)
	j.metrics.ObservePgxAcquire(time.Since(start))
	return conn, err
}

// SetJobIntervals overrides per-job intervals. Unspecified jobs keep their
// current value. A job with interval 0 fires on every base tick — handy
// for tests that want one Tick to exercise every sub-job. Useful for
// tests that want to collapse stratification.
func (j *Janitor) SetJobIntervals(m map[string]time.Duration) {
	for k, v := range m {
		j.jobIntervals[k] = v
	}
}

// SetOrphanGrace overrides the orphan-message grace period.
func (j *Janitor) SetOrphanGrace(d time.Duration) { j.orphanGrace = d }

// SetInboundQoS2Grace overrides the inbound_qos2 grace period.
func (j *Janitor) SetInboundQoS2Grace(d time.Duration) { j.inboundQoS2Grace = d }

// SetMetrics installs a Metrics instance for janitor counters. Optional.
func (j *Janitor) SetMetrics(m *metrics.Metrics) { j.metrics = m }

// Run starts the tick loop on this Pod. Returns when ctx is cancelled.
// Every Pod runs its own Run; coordination is per-row in PG, not per-Pod
// (see the package doc).
func (j *Janitor) Run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			j.logger.Error("janitor goroutine panic",
				"panic", r, "stack", string(debug.Stack()))
		}
	}()

	j.logger.Info("janitor starting", "interval", j.interval)

	t := time.NewTicker(j.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := j.tickWithRecover(ctx); err != nil &&
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

// shouldRun reports whether (now - j.lastRun[job]) >= j.jobIntervals[job].
// The first call for any job always fires (zero-value lastRun).
func (j *Janitor) shouldRun(job string, now time.Time) bool {
	interval := j.jobIntervals[job]
	if interval <= 0 {
		// 0 or unset = fire every tick (test collapse mode, or a job
		// whose interval was deleted).
		return true
	}
	last := j.lastRun[job]
	if last.IsZero() {
		return true
	}
	return now.Sub(last) >= interval
}

// markRun records the time job last fired.
func (j *Janitor) markRun(job string, now time.Time) {
	j.lastRun[job] = now
}

// Tick runs one full scan cycle, with each sub-job gated by its per-job
// stratified interval. A job fires iff (now - lastRun[job]) >= interval[job].
// Each sub-job is timed and any error is counted on the per-job metrics, so
// an operator can alert on a specific job degrading without parsing logs.
func (j *Janitor) Tick(ctx context.Context) error {
	now := j.nowFn()

	// find_dead_brokers + handle_dead_broker run together: a positive
	// candidate list is meaningless without the per-broker takeover. Gate
	// on find_dead_brokers' interval.
	if j.shouldRun(JobFindDeadBrokers, now) {
		var candidates []uuid.UUID
		if err := j.timed(JobFindDeadBrokers, func() error {
			var err error
			candidates, err = j.findDeadBrokerCandidates(ctx)
			return err
		}); err != nil {
			j.markRun(JobFindDeadBrokers, now)
			return err
		}
		j.markRun(JobFindDeadBrokers, now)
		for _, id := range candidates {
			var claimed bool
			if err := j.timed(JobHandleDeadBroker, func() error {
				var err error
				claimed, err = j.handleDeadBroker(ctx, id)
				return err
			}); err != nil {
				j.logger.Warn("dead broker handling", "broker", id, "err", err)
				continue
			}
			if claimed && j.metrics != nil {
				j.metrics.DeadBrokersTotal.Inc()
			}
		}
	}

	// Run the rest of the per-job suite in deterministic order. Order
	// matters only when one job's effects feed another within the same
	// tick: expire_sessions before sweep_orphan_deliveries (cleared
	// session rows produce orphans), sweep_orphan_deliveries before
	// sweep_orphan_messages (a deletion can leave a message with no
	// deliveries). The 1s/5s stratified cadence preserves that ordering
	// across ticks too — a 30s sweep_orphan_messages tick will fire
	// after at least 6 expire_sessions ticks.
	type job struct {
		name string
		run  func(context.Context) error
	}
	jobs := []job{
		{JobFireDueWills, j.fireDueWills},
		{JobExpireSessions, j.expireSessions},
		{JobExpireRetained, j.expireRetained},
		{JobSweepInboundQoS2, j.sweepInboundQoS2},
		{JobSweepOrphanDeliveries, j.sweepOrphanDeliveries},
		{JobRefreshDeliveriesGauge, j.refreshDeliveriesGauge},
		{JobRefreshStateGauges, j.refreshStateGauges},
		{JobSweepOrphanMessages, j.sweepOrphanMessages},
		{JobRefreshNotifyQueue, j.refreshNotifyQueue},
	}
	var firstErr error
	for _, jb := range jobs {
		if !j.shouldRun(jb.name, now) {
			continue
		}
		err := j.timed(jb.name, func() error { return jb.run(ctx) })
		j.markRun(jb.name, now)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// timed runs f and records its duration + error on the per-job metrics.
// A nil j.metrics is fine — pre-metrics test cases still work.
func (j *Janitor) timed(job string, f func() error) error {
	start := time.Now()
	err := f()
	if j.metrics != nil {
		j.metrics.JanitorTickSeconds.WithLabelValues(job).Observe(time.Since(start).Seconds())
		if err != nil {
			j.metrics.JanitorErrorsTotal.WithLabelValues(job).Inc()
			j.logger.Warn("janitor sub-job", "job", job, "err", err)
		}
	} else if err != nil {
		j.logger.Warn("janitor sub-job", "job", job, "err", err)
	}
	return err
}

// refreshNotifyQueue samples pg_notification_queue_usage() and writes
// it to the gauge. PG's NOTIFY queue is shared-memory and capped; once
// it fills (one wedged listener can do that under sustained publish
// load), every committing transaction in the cluster errors at COMMIT
// with SQLSTATE 54000. Most likely cause of "lights are slow tonight"
// in a multi-pod deploy; previously invisible until the failure cliff.
func (j *Janitor) refreshNotifyQueue(ctx context.Context) error {
	if j.metrics == nil {
		return nil
	}
	var v float64
	if err := j.pool.QueryRow(ctx, `SELECT pg_notification_queue_usage()`).Scan(&v); err != nil {
		return err
	}
	j.metrics.NotifyQueueUsageRatio.Set(v)
	return nil
}

// refreshStateGauges populates the per-table cardinality gauges
// (subscriptions, sessions, retained, inbound_qos2). Cheap (one
// indexed COUNT each) and gives operators a continuous view of
// session/topic accumulation without touching the broker hot path.
//
// Note: at the homelab/HA-Z2M scale these tables are O(100-1000)
// rows, so count(*) is microseconds. Switching to
// pg_stat_user_tables.n_live_tup would be free but stale (relies on
// autovacuum/ANALYZE to be useful) — only worth doing if these
// tables ever grow into the millions.
func (j *Janitor) refreshStateGauges(ctx context.Context) error {
	if j.metrics == nil {
		return nil
	}
	queries := []struct {
		sql   string
		gauge interface{ Set(float64) }
	}{
		{`SELECT count(*) FROM subscriptions`, j.metrics.Subscriptions},
		{`SELECT count(*) FROM sessions`, j.metrics.Sessions},
		{`SELECT count(*) FROM retained`, j.metrics.RetainedCount},
		{`SELECT count(*) FROM inbound_qos2`, j.metrics.InboundQoS2Pending},
		// messages_count compounded the v0.1.15 throughput-cliff investigation:
		// orphan-messages sweep was falling behind the publish-side inflow,
		// and there was no glance-able way to see the table growing.
		{`SELECT count(*) FROM messages`, j.metrics.MessagesCount},
	}
	for _, q := range queries {
		var n int64
		if err := j.pool.QueryRow(ctx, q.sql).Scan(&n); err != nil {
			return err
		}
		q.gauge.Set(float64(n))
	}
	return nil
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
	tx, err := j.beginTxTimed(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT client_id, will_topic, will_payload, will_qos, will_retain, will_properties, will_fire_at
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
		client    string
		topic     string
		payload   []byte
		qos       int
		retain    bool
		props     []byte
		fireAt    time.Time
	}
	var wills []w
	for rows.Next() {
		var x w
		if err := rows.Scan(&x.client, &x.topic, &x.payload, &x.qos, &x.retain, &x.props, &x.fireAt); err != nil {
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
			// Lateness = (now - will_fire_at). Negative would indicate
			// firing-too-early which the WHERE clause forbids; clamp to
			// 0 just in case clocks skew or the SELECT/UPDATE window
			// lets `now()` regress behind the row's will_fire_at value.
			lateness := time.Since(w.fireAt).Seconds()
			if lateness < 0 {
				lateness = 0
			}
			j.metrics.WillFireLatenessSeconds.Observe(lateness)
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
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if j.metrics != nil {
		j.metrics.JanitorSweptRowsTotal.WithLabelValues(JobFireDueWills).
			Add(float64(len(wills)))
	}
	return nil
}

// expireSessions deletes session rows whose v5 SessionExpiryInterval has
// elapsed. Subscriptions still cascade via FK; deliveries cascade was
// dropped in migration 0006 (MultiXact SLRU thrash), so we delete those
// explicitly in the same tx.
func (j *Janitor) expireSessions(ctx context.Context) error {
	tx, err := j.beginTxTimed(ctx, pgx.TxOptions{})
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
		j.metrics.JanitorSweptRowsTotal.WithLabelValues(JobExpireSessions).
			Add(float64(len(ids)))
	}
	return nil
}

// sweepOrphanDeliveries deletes deliveries rows whose client_id no
// longer has a sessions row. Orphans accumulate when sessions are
// removed by an out-of-band path (operator psql, restore from backup);
// the in-broker delete paths clean explicitly. Rare in steady state.
func (j *Janitor) sweepOrphanDeliveries(ctx context.Context) error {
	// Two cases both yield deliveries that no live client will ever drain:
	//
	//   (a) The session row is gone (graceful clean_start=true disconnect,
	//       or an out-of-band DELETE). Pre-existing case.
	//   (b) The session row is still there but the client disconnected as
	//       clean_start=true a while ago. v3 doesn't model session expiry,
	//       and ungraceful disconnects don't always run the session-DELETE
	//       cleanup path, so these can sit indefinitely with their
	//       deliveries (real-world: 11.7k stranded rows from a 3-day-old
	//       `auto-aa1256f3` session). Wait `orphanGrace` past last_seen
	//       before deleting so a momentary reconnect-flap doesn't lose
	//       in-flight QoS-1 rows.
	cutoff := time.Now().Add(-j.orphanGrace)
	ct, err := j.pool.Exec(ctx, `
		DELETE FROM deliveries
		 WHERE NOT EXISTS (
		    SELECT 1 FROM sessions s WHERE s.client_id = deliveries.client_id
		 )
		    OR client_id IN (
		    SELECT s.client_id FROM sessions s
		     WHERE s.client_id = deliveries.client_id
		       AND s.connected = false
		       AND s.clean_start = true
		       AND s.last_seen < $1
		 )
	`, cutoff)
	if err == nil && j.metrics != nil {
		j.metrics.JanitorSweptRowsTotal.WithLabelValues(JobSweepOrphanDeliveries).
			Add(float64(ct.RowsAffected()))
	}
	return err
}

// expireRetained drops retained rows past their MessageExpiryInterval.
func (j *Janitor) expireRetained(ctx context.Context) error {
	ct, err := j.pool.Exec(ctx, `
		DELETE FROM retained
		 WHERE expires_at IS NOT NULL AND expires_at <= now()
	`)
	if err == nil && j.metrics != nil {
		j.metrics.JanitorSweptRowsTotal.WithLabelValues(JobExpireRetained).
			Add(float64(ct.RowsAffected()))
	}
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
	if j.metrics != nil {
		j.metrics.JanitorSweptRowsTotal.WithLabelValues(JobSweepInboundQoS2).
			Add(float64(ct.RowsAffected()))
	}
	if ct.RowsAffected() > 0 {
		j.logger.Debug("inbound_qos2 sweep", "deleted", ct.RowsAffected())
	}
	return nil
}

// findDeadBrokerCandidates returns every distinct broker_id currently
// referenced in sessions. The caller filters to actually-dead ones via
// pg_try_advisory_lock on a single acquired conn (so lock+unlock land on
// the same Postgres session).
func (j *Janitor) findDeadBrokerCandidates(ctx context.Context) ([]uuid.UUID, error) {
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
	return candidates, rows.Err()
}

// handleDeadBroker tries to claim a per-broker advisory lock and, if
// successful, fires wills + clears ownership for sessions owned by that
// broker. Lock acquisition + work + unlock all run on a single acquired
// conn so the unlock lands on the same Postgres session that holds the
// lock — going through pool.Exec for both lets pgxpool route them to
// different conns and the unlock becomes a no-op.
//
// Returns true if we claimed the broker (caller increments the metric);
// false if another pod beat us to it.
func (j *Janitor) handleDeadBroker(ctx context.Context, brokerID uuid.UUID) (claimed bool, err error) {
	conn, err := j.acquireTimed(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Release()

	lockKey := "pgmqtt:broker:" + brokerID.String()
	var got bool
	if err := conn.QueryRow(ctx,
		`SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, lockKey).Scan(&got); err != nil {
		return false, err
	}
	if !got {
		return false, nil
	}
	defer func() {
		_, _ = conn.Exec(ctx,
			`SELECT pg_advisory_unlock(hashtextextended($1, 0))`, lockKey)
	}()

	rows, err := conn.Query(ctx, `
		SELECT client_id, will_topic, will_payload, will_qos, will_retain,
		       will_properties, will_delay
		  FROM sessions
		 WHERE broker_id = $1
		   AND will_topic IS NOT NULL
	`, brokerID)
	if err != nil {
		return true, err
	}
	type will struct {
		client  string
		topic   string
		payload []byte
		qos     int
		retain  bool
		props   []byte
		delay   *int // nil or 0 = fire immediately
	}
	var wills []will
	for rows.Next() {
		var w will
		if err := rows.Scan(&w.client, &w.topic, &w.payload, &w.qos, &w.retain, &w.props, &w.delay); err != nil {
			rows.Close()
			return true, err
		}
		wills = append(wills, w)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return true, err
	}
	rows.Close()

	// Per [MQTT-3.1.3.2.2] (Will Delay Interval): the server MUST NOT
	// publish the will until the delay has elapsed. The previous code
	// fired ALL wills for the dead broker immediately, which broke v5
	// will-delay clients (Z2M restart → instant "device went offline"
	// instead of the configured 30 s settling window). Split the rows:
	//   - delay nil or 0  → fire now, clear will_*
	//   - delay > 0       → schedule will_fire_at, leave will_* set
	//                       so fireDueWills picks them up at the right
	//                       time. Clamp by session_expires_at if set.
	for _, w := range wills {
		if w.delay == nil || *w.delay == 0 {
			if err := j.eng.PublishWill(ctx, w.topic, w.payload, byte(w.qos), w.retain, w.props); err != nil {
				j.logger.Warn("fire will from dead broker", "client", w.client, "err", err)
			}
		}
	}

	// Step 1 — schedule delayed wills: clear broker_id, set will_fire_at
	// (clamped against session_expires_at) but keep will_* so fireDueWills
	// will pick the row up when its delay elapses.
	if _, err := conn.Exec(ctx, `
		UPDATE sessions SET
		    connected=false,
		    broker_id=NULL,
		    last_seen=now(),
		    will_fire_at = LEAST(
		        now() + (will_delay * interval '1 second'),
		        COALESCE(session_expires_at, 'infinity'::timestamptz)
		    )
		 WHERE broker_id = $1
		   AND will_topic IS NOT NULL
		   AND COALESCE(will_delay, 0) > 0
	`, brokerID); err != nil {
		return true, err
	}

	// Step 2 — every remaining session under the dead broker (no will,
	// or will already fired immediately above): clear broker_id AND all
	// will_* columns so a future fireDueWills tick can't double-fire.
	//
	// RowsAffected gates `claimed`. Holding the advisory lock alone isn't
	// enough — a second janitor that grabs the lock *after* the first
	// already NULLed broker_id finds nothing to clean up and shouldn't
	// inflate pgmqtt_dead_brokers_handled_total. This matches the
	// docstring ("false if another pod beat us to it").
	ct, err := conn.Exec(ctx,
		`UPDATE sessions SET connected=false, broker_id=NULL, last_seen=now(),
		    will_topic=NULL, will_payload=NULL, will_qos=NULL,
		    will_retain=NULL, will_delay=NULL, will_properties=NULL
		 WHERE broker_id=$1`, brokerID)
	if err != nil {
		return true, err
	}
	return ct.RowsAffected() > 0, nil
}

// sweepOrphanMessages deletes messages with no referencing deliveries that are
// older than the grace period. (Deliveries cascade-delete on session cleanup,
// so abandoned messages accumulate without a sweep.)
func (j *Janitor) sweepOrphanMessages(ctx context.Context) error {
	tx, err := j.beginTxTimed(ctx, pgx.TxOptions{})
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
	if j.metrics != nil {
		j.metrics.JanitorSweptRowsTotal.WithLabelValues(JobSweepOrphanMessages).
			Add(float64(ct.RowsAffected()))
	}
	if ct.RowsAffected() > 0 {
		j.logger.Debug("orphan sweep", "deleted", ct.RowsAffected())
	}
	return nil
}

// Compile-time guard to keep handler errors from being unused.
var _ = errors.New
