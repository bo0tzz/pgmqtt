// Package janitor runs the dead-Pod scan, will fan-out, and orphan-message
// sweep. Every Pod runs an independent Tick loop. The work is concurrency-
// safe by construction:
//
//   - reapBroker takes pg_try_advisory_lock per broker_id, so only one Pod
//     claims any given dead broker at a time.
//     A free lock alone is not proof of death — see DeadBrokerGrace.
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
//	expire_sessions         1s   Paho test_session_expiry allows 1s of slack
//	expire_retained         5s   MessageExpiryInterval is seconds-grade
//	expire_deliveries       5s   same, for messages queued to a client
//	find_dead_brokers       5s   a dead broker is reaped after DeadBrokerGrace
//	handle_dead_broker      —    fired in the same tick as find_dead_brokers
//	refresh_deliveries_gauge 10s sub-minute observability gauge
//	refresh_state_gauges    10s  sub-minute observability gauge
//	refresh_notify_queue    10s  sub-minute observability gauge
//	repair_sessions         30s  rows no current code path leaves behind
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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bo0tzz/pgmqtt/internal/engine"
	"github.com/bo0tzz/pgmqtt/internal/listener"
	"github.com/bo0tzz/pgmqtt/internal/metrics"
)

// DeadBrokerGrace is how long a broker's advisory lock must stay free,
// on every check, before the janitor treats that broker as dead.
//
// A free lock means "this broker's listener session is gone", which is
// also what a perfectly healthy broker looks like for a few seconds after
// Postgres restarts or fails over: every listener session dies at once,
// and whichever broker's pool reconnects first sees every lock free,
// its own included. Reaping on that evidence fires the wills of clients
// that are still connected (2026-06-29, 2026-11-17).
//
// The other half of the contract is listener.LockReacquireDeadline: a
// broker that cannot get its lock back inside that deadline exits. So a
// lock that has been free for longer than the deadline belongs to a
// broker that is no longer serving anyone. The margin on top covers the
// gap between Postgres releasing the lock and the owner noticing.
//
// "Free for the whole grace" is sampled, not watched, so on its own it
// could be fooled by Postgres bouncing more than once: free at one
// sample, re-held and lost again before the next. The janitor therefore
// only counts time during which its own broker's lock was held without
// interruption (see ownLockUnbroken). Anything that wipes every lock
// wipes ours too, and that restarts every clock.
//
// The samples themselves have to be real: each is timestamped when its
// probe returns, a probe that errors restarts the clock, so does a gap
// between probes longer than maxProbeGap (a stalled tick saw nothing),
// and a reap needs minDeadBrokerSamples of them however long the janitor
// interval is.
const DeadBrokerGrace = listener.LockReacquireDeadline + 10*time.Second

const minDeadBrokerSamples = 3

const brokerLockPrefix = "pgmqtt:broker:"

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
	JobExpireDeliveries      = "expire_deliveries"
	JobRepairSessions        = "repair_sessions"
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
	JobExpireDeliveries:       5 * time.Second,
	JobRepairSessions:         30 * time.Second,
}

// suspect is one unbroken run of probes that found a broker's lock free.
type suspect struct {
	since   time.Time
	last    time.Time
	samples int
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

	// deadBrokerGrace: see DeadBrokerGrace. suspects tracks each broker
	// whose lock was found free; an entry is dropped the moment the lock
	// is seen held again, so the grace always measures one unbroken run
	// of free probes.
	deadBrokerGrace time.Duration
	suspectsMu      sync.Mutex
	suspects        map[uuid.UUID]suspect

	// lockEpoch reports the listener's count of lock losses; suspectsEpoch
	// is its value when the current suspects were recorded. nil (tests
	// without a listener) reads as a constant.
	lockEpoch     func() uint64
	suspectsEpoch uint64

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
		deadBrokerGrace:  DeadBrokerGrace,
		suspects:         make(map[uuid.UUID]suspect),
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
// reapBroker holds a dedicated conn for the lock+unlock pair so it
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

// SetDeadBrokerGrace overrides DeadBrokerGrace. For tests: any value at or
// below listener.LockReacquireDeadline lets a live broker be reaped, and
// zero reaps on the first free probe.
func (j *Janitor) SetDeadBrokerGrace(d time.Duration) { j.deadBrokerGrace = d }

// SetLockEpoch installs the listener's LockEpoch, which lets the janitor
// tell that this broker's lock was lost and re-taken between two ticks.
func (j *Janitor) SetLockEpoch(f func() uint64) { j.lockEpoch = f }

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

// shouldRun reports whether job is due: whether its interval has passed
// since it last ran, give or take half a base tick. The first call for any
// job always fires (zero-value lastRun).
//
// Without the slack, a job whose interval is a whole number of ticks
// skipped a tick whenever the ticker fired a hair early, which it does:
// fire_due_wills, nominally every second, ran 41 times a minute, and a
// will could go out up to a second late.
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
	return now.Sub(last) >= interval-j.interval/2
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
		j.forgetSuspectsExcept(candidates)
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
		{JobExpireDeliveries, j.expireDeliveries},
		{JobRepairSessions, j.repairSessions},
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

// fireDueWills publishes wills whose scheduled fire-time has arrived.
func (j *Janitor) fireDueWills(ctx context.Context) error {
	fired, err := j.eng.FireDueWills(ctx)
	if j.metrics != nil && fired > 0 {
		j.metrics.JanitorSweptRowsTotal.WithLabelValues(JobFireDueWills).Add(float64(fired))
	}
	return err
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

	// A session ending is one of the two things that make its will due
	// [MQTT-3.1.2-8], so an expired session that still holds one is not
	// deleted: its will is made due, and the row goes on a later tick,
	// once the will is out. Deleting it here would lose the will whenever
	// publishing it had failed or it was scheduled past the expiry.
	if _, err := tx.Exec(ctx, `
		UPDATE sessions SET will_fire_at = now()
		 WHERE connected = false
		   AND session_expires_at <= now()
		   AND will_topic IS NOT NULL
		   AND (will_fire_at IS NULL OR will_fire_at > now())
	`); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
		SELECT client_id FROM sessions
		 WHERE connected = false
		   AND session_expires_at IS NOT NULL
		   AND session_expires_at <= now()
		   AND will_topic IS NULL
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
	// deliveries has no foreign key to sessions (0006), so rows can
	// outlive theirs. A session that still exists is never touched here,
	// connected or not: whether its queue is kept is decided by its
	// Session Expiry when the session ends, not by clean_start, which a
	// v5 client sets on a perfectly persistent session.
	cutoff := time.Now().Add(-j.orphanGrace)
	ct, err := j.pool.Exec(ctx, `
		DELETE FROM deliveries d
		 WHERE d.created_at < $1
		   AND NOT EXISTS (SELECT 1 FROM sessions s WHERE s.client_id = d.client_id)
	`, cutoff)
	if err == nil && j.metrics != nil {
		j.metrics.JanitorSweptRowsTotal.WithLabelValues(JobSweepOrphanDeliveries).
			Add(float64(ct.RowsAffected()))
	}
	return err
}

// expireDeliveries deletes queued deliveries whose message has expired
// before delivery started [MQTT-3.3.2-5]. A connected client's sender
// drops these as it meets them; this is for clients that are away, whose
// expired rows would otherwise sit there counting against their queue
// cap until they came back.
func (j *Janitor) expireDeliveries(ctx context.Context) error {
	ct, err := j.pool.Exec(ctx, `
		DELETE FROM deliveries d
		 USING messages m
		 WHERE m.id = d.message_id
		   AND d.state = 0
		   AND m.expires_at <= now()
	`)
	if err == nil && j.metrics != nil {
		j.metrics.JanitorSweptRowsTotal.WithLabelValues(JobExpireDeliveries).
			Add(float64(ct.RowsAffected()))
	}
	return err
}

// repairSessions puts right three kinds of session row that Engine.endSession
// never leaves behind, but that brokers before 0.3.0 did, on every
// shutdown and every reap, and still do while any are running during an
// upgrade. Each is a disconnected, unowned row that is missing the one
// thing that would ever move it on:
//
//   - a will with no fire time: it could never be published. It is made
//     due now (late, but the client has not come back);
//   - a finite Session Expiry Interval with no expiry time: the session
//     would never expire. Its clock starts now;
//   - a session that was meant to end with its connection, still here.
//     It is deleted, once any will it has is out.
func (j *Janitor) repairSessions(ctx context.Context) error {
	const stranded = `NOT connected AND broker_id IS NULL`
	var repaired int64
	for _, q := range []string{
		`UPDATE sessions SET will_fire_at = now()
		  WHERE ` + stranded + ` AND will_topic IS NOT NULL AND will_fire_at IS NULL`,
		`UPDATE sessions SET session_expires_at = now() + expiry_interval * interval '1 second'
		  WHERE ` + stranded + ` AND session_expires_at IS NULL
		    AND expiry_interval > 0 AND expiry_interval < 2147483647`,
		`DELETE FROM sessions
		  WHERE ` + stranded + ` AND expiry_interval = 0 AND will_topic IS NULL`,
	} {
		ct, err := j.pool.Exec(ctx, q)
		if err != nil {
			return err
		}
		repaired += ct.RowsAffected()
	}
	if repaired > 0 {
		j.logger.Info("repaired session rows left by an older broker", "rows", repaired)
		if j.metrics != nil {
			j.metrics.JanitorSweptRowsTotal.WithLabelValues(JobRepairSessions).Add(float64(repaired))
		}
	}
	return nil
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
// referenced in sessions, this broker's own included. handleDeadBroker
// filters to the actually-dead ones. Returns nothing while this broker's
// own lock is not held: see ownLockUnbroken.
func (j *Janitor) findDeadBrokerCandidates(ctx context.Context) ([]uuid.UUID, error) {
	held, err := j.ownLockUnbroken(ctx)
	if err != nil || !held {
		return nil, err
	}
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

// ownLockUnbroken reports whether this broker's own advisory lock is held
// right now, and drops every suspect if it is not, or if it was lost and
// re-taken since they were recorded.
//
// Our lock lives on the same Postgres as every peer's. A restart,
// failover or crash recovery releases all of them together, so while ours
// is missing a peer's free lock says nothing about that peer, and a clock
// that started before ours was lost may span a moment when the peer held
// its lock again. Only time observed under one unbroken hold of our own
// lock counts towards a peer's grace.
//
// The lock is checked in Postgres rather than by asking the listener,
// because the pool can be back and probing before the listener goroutine
// has noticed its session died. The epoch is read after that check: a new
// listener session cannot exist without the epoch having moved first.
func (j *Janitor) ownLockUnbroken(ctx context.Context) (bool, error) {
	free, err := j.brokerLockFree(ctx, j.eng.BrokerID())
	if err != nil {
		// We can't vouch for our own lock, so nothing observed so far
		// can be carried across this tick.
		j.suspectsMu.Lock()
		clear(j.suspects)
		j.suspectsMu.Unlock()
		return false, err
	}
	held := !free

	var epoch uint64
	if j.lockEpoch != nil {
		epoch = j.lockEpoch()
	}
	j.suspectsMu.Lock()
	dropped := 0
	if !held || epoch != j.suspectsEpoch {
		dropped = len(j.suspects)
		clear(j.suspects)
		j.suspectsEpoch = epoch
	}
	j.suspectsMu.Unlock()
	if dropped > 0 {
		j.logger.Info("own broker lock was interrupted; restarting dead-broker clocks",
			"suspects", dropped, "held_now", held)
	}
	return held, nil
}

// brokerLockFree reports whether nobody holds brokerID's advisory lock.
//
// The probe is a shared, transaction-scoped try-lock in one statement.
// Postgres drops it when the statement ends, so a probe can never leave
// a live broker's lock stranded on a pooled connection; and being shared,
// janitors probing the same broker don't read each other as the owner.
// It still conflicts with the owner's exclusive lock, which is the point.
func (j *Janitor) brokerLockFree(ctx context.Context, brokerID uuid.UUID) (bool, error) {
	var free bool
	err := j.pool.QueryRow(ctx,
		`SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1, 0))`,
		brokerLockPrefix+brokerID.String()).Scan(&free)
	return free, err
}

// releaseBrokerLock gives back a lock taken with pg_try_advisory_lock on
// conn. If the unlock cannot be confirmed the connection is closed, which
// releases the lock with the session: a lock left behind on a pooled
// connection would keep its real owner from ever taking it back.
func releaseBrokerLock(ctx context.Context, conn *pgxpool.Conn, lockKey string) {
	uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	var released bool
	err := conn.QueryRow(uctx,
		`SELECT pg_advisory_unlock(hashtextextended($1, 0))`, lockKey).Scan(&released)
	if err != nil || !released {
		_ = conn.Conn().Close(uctx)
	}
}

// handleDeadBroker decides whether brokerID is dead and, if so, reaps it.
// A broker is dead when its advisory lock has been free on every check
// for j.deadBrokerGrace (see DeadBrokerGrace); until then a free lock
// only starts or continues the clock.
//
// Returns true if we claimed the broker (caller increments the metric);
// false if it is alive, still inside its grace, or another pod beat us
// to it.
func (j *Janitor) handleDeadBroker(ctx context.Context, brokerID uuid.UUID) (claimed bool, err error) {
	// This process is running, so its broker is not dead, whatever the
	// lock says.
	if brokerID == j.eng.BrokerID() {
		return false, nil
	}
	free, err := j.brokerLockFree(ctx, brokerID)
	if err != nil {
		j.clearSuspect(brokerID)
		return false, err
	}
	if !free {
		j.clearSuspect(brokerID)
		return false, nil
	}
	// The clock reads after the probe: the tick may have waited a long
	// time for a pool connection, and none of that wait was observation.
	freeFor, samples := j.observeFree(brokerID, j.nowFn())
	if freeFor < j.deadBrokerGrace {
		return false, nil
	}
	if j.deadBrokerGrace > 0 && samples < minDeadBrokerSamples {
		return false, nil
	}
	return j.reapBroker(ctx, brokerID, freeFor)
}

// maxProbeGap is the longest two consecutive probes of a suspect may be
// apart and still count as one run. Scales with the probe cadence so a
// long janitor interval slows detection down instead of preventing it.
func (j *Janitor) maxProbeGap() time.Duration {
	cadence := max(j.interval, j.jobIntervals[JobFindDeadBrokers])
	return max(j.deadBrokerGrace/2, 2*cadence)
}

// reapBroker claims brokerID's advisory lock and, if it gets it, ends
// every session that broker owned (Engine.EndSessionsOfBroker: wills,
// session expiry, ownership). The lock is what keeps two janitors from
// reaping the same broker at once.
//
// Lock acquisition + work + unlock all run on a single acquired conn so
// the unlock lands on the same Postgres session that holds the lock —
// going through pool.Exec for both lets pgxpool route them to different
// conns and the unlock becomes a no-op.
func (j *Janitor) reapBroker(ctx context.Context, brokerID uuid.UUID, freeFor time.Duration) (claimed bool, err error) {
	conn, err := j.acquireTimed(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Release()

	lockKey := brokerLockPrefix + brokerID.String()
	var got bool
	if err := conn.QueryRow(ctx,
		`SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, lockKey).Scan(&got); err != nil {
		return false, err
	}
	if !got {
		return false, nil
	}
	defer releaseBrokerLock(ctx, conn, lockKey)
	j.logger.Warn("reaping dead broker", "broker", brokerID, "lock_free_for", freeFor)

	// Holding the lock alone isn't enough to count as a claim: a second
	// janitor that takes it after the first has finished finds nothing
	// left to end and shouldn't inflate pgmqtt_dead_brokers_handled_total.
	ended, err := j.eng.EndSessionsOfBroker(ctx, brokerID)
	return ended > 0, err
}

// observeFree records a probe at time at that found brokerID's lock free,
// and returns how long the current unbroken run of such probes spans and
// how many probes are in it.
func (j *Janitor) observeFree(brokerID uuid.UUID, at time.Time) (time.Duration, int) {
	maxGap := j.maxProbeGap()
	j.suspectsMu.Lock()
	s, ok := j.suspects[brokerID]
	restarted := ok && at.Sub(s.last) > maxGap
	if !ok || restarted {
		s = suspect{since: at}
	}
	s.last = at
	s.samples++
	j.suspects[brokerID] = s
	j.suspectsMu.Unlock()
	if j.deadBrokerGrace > 0 {
		switch {
		case restarted:
			j.logger.Info("gap between dead-broker probes too long; restarting its clock",
				"broker", brokerID, "max_gap", maxGap)
		case !ok:
			j.logger.Info("broker lock is free; will reap if it stays free",
				"broker", brokerID, "grace", j.deadBrokerGrace)
		}
	}
	return at.Sub(s.since), s.samples
}

// clearSuspect ends brokerID's run of free probes: its lock was found
// held, or the probe could not be answered.
func (j *Janitor) clearSuspect(brokerID uuid.UUID) {
	j.suspectsMu.Lock()
	_, ok := j.suspects[brokerID]
	delete(j.suspects, brokerID)
	j.suspectsMu.Unlock()
	if ok {
		j.logger.Info("broker is no longer a reap candidate", "broker", brokerID)
	}
}

// forgetSuspectsExcept drops suspects that no longer own any session
// (reaped by a peer, or every client moved elsewhere).
func (j *Janitor) forgetSuspectsExcept(candidates []uuid.UUID) {
	j.suspectsMu.Lock()
	defer j.suspectsMu.Unlock()
	if len(j.suspects) == 0 {
		return
	}
	keep := make(map[uuid.UUID]struct{}, len(candidates))
	for _, id := range candidates {
		keep[id] = struct{}{}
	}
	for id := range j.suspects {
		if _, ok := keep[id]; !ok {
			delete(j.suspects, id)
		}
	}
}

// sweepOrphanMessages deletes messages with no referencing deliveries that are
// older than the grace period. A message's deliveries go one by one, as
// each is acknowledged or its session ends, and nothing else notices when
// the last one has.
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
