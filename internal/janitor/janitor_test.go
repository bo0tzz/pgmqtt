package janitor_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/google/uuid"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	"github.com/bo0tzz/pgmqtt/internal/janitor"
	"github.com/bo0tzz/pgmqtt/internal/listener"
	"github.com/bo0tzz/pgmqtt/internal/metrics"
)

func warnLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// TestJanitorFiresWillFromDeadBroker simulates a Pod death by inserting a
// session row pointing at a fabricated broker UUID that never held a lock,
// then runs the janitor's Tick and asserts the will is delivered to a
// subscribed observer.
func TestJanitorFiresWillFromDeadBroker(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]

	// Wire a real listener so the surviving pod fans out the will.
	l, err := listener.Start(context.Background(), mh.URL, pod.Engine, warnLogger())
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	t.Cleanup(l.Stop)
	pod.Engine.SetBrokerID(l.BrokerID())
	pod.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
	pod.BrokerID = l.BrokerID()

	observer := pod.Connect(t, "obs-jan")
	defer observer.Close()
	observer.Subscribe(t, "lwt/+", 1)

	// Insert a fake dead-broker session (broker_id has never been locked).
	deadBroker := uuid.New()
	_, err = mh.Pool.Exec(context.Background(), `
		INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start,
		    will_topic, will_payload, will_qos, will_retain)
		VALUES ($1, $2, true, 5, false, 'lwt/dead', $3, 1, false)
	`, "ghost", deadBroker, []byte("ghost-died"))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	jt := janitor.New(mh.Pool, pod.Engine, warnLogger())
	if err := jt.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	pk := observer.Read(t, 3*time.Second)
	if pk.FixedHeader.Type != packets.Publish {
		t.Fatalf("expected publish, got %d", pk.FixedHeader.Type)
	}
	if pk.TopicName != "lwt/dead" || string(pk.Payload) != "ghost-died" {
		t.Errorf("got %q/%q", pk.TopicName, pk.Payload)
	}

	// Sessions row must have broker_id cleared.
	var brokerID *uuid.UUID
	if err := mh.Pool.QueryRow(context.Background(),
		`SELECT broker_id FROM sessions WHERE client_id='ghost'`).Scan(&brokerID); err != nil {
		t.Fatalf("query: %v", err)
	}
	if brokerID != nil {
		t.Errorf("broker_id not cleared: %v", brokerID)
	}
}

// TestJanitorWillDelayRespectedAcrossDeadBroker simulates an ungraceful
// death of a v5 client whose CONNECT carried WillDelayInterval=30s. The
// previous handleDeadBroker fired ALL wills for the dead broker
// immediately, so a Z2M restart surfaced as instant "device went offline"
// on HA dashboards. After the fix, handleDeadBroker schedules the will
// via will_fire_at = now() + will_delay, leaving fireDueWills to publish
// it once the delay elapses.
func TestJanitorWillDelayRespectedAcrossDeadBroker(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]

	l, err := listener.Start(context.Background(), mh.URL, pod.Engine, warnLogger())
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	t.Cleanup(l.Stop)
	pod.Engine.SetBrokerID(l.BrokerID())
	pod.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
	pod.BrokerID = l.BrokerID()

	observer := pod.Connect(t, "obs-delayed")
	defer observer.Close()
	observer.Subscribe(t, "lwt/+", 1)

	deadBroker := uuid.New()
	_, err = mh.Pool.Exec(context.Background(), `
		INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start,
		    will_topic, will_payload, will_qos, will_retain, will_delay)
		VALUES ($1, $2, true, 5, false, 'lwt/delayed', $3, 1, false, 30)
	`, "delayed-ghost", deadBroker, []byte("delayed-died"))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	jt := janitor.New(mh.Pool, pod.Engine, warnLogger())
	if err := jt.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	// Will MUST NOT have fired yet — read with a short deadline; expect
	// timeout.
	if pk := observer.TryRead(300 * time.Millisecond); pk != nil && pk.FixedHeader.Type == packets.Publish {
		t.Fatalf("will fired prematurely: topic=%q payload=%q", pk.TopicName, pk.Payload)
	}

	// will_fire_at must be ~30s ahead and broker_id cleared.
	var brokerID *uuid.UUID
	var willFireAt *time.Time
	if err := mh.Pool.QueryRow(context.Background(),
		`SELECT broker_id, will_fire_at FROM sessions WHERE client_id='delayed-ghost'`).
		Scan(&brokerID, &willFireAt); err != nil {
		t.Fatalf("query: %v", err)
	}
	if brokerID != nil {
		t.Errorf("broker_id not cleared: %v", brokerID)
	}
	if willFireAt == nil {
		t.Fatal("will_fire_at must be scheduled")
	}
	delta := time.Until(*willFireAt)
	if delta < 25*time.Second || delta > 35*time.Second {
		t.Errorf("will_fire_at delta = %v; want ~30s", delta)
	}
}

// TestJanitorExpireSessionsDeletesPastExpiry pins the contract Paho's
// test_session_expiry depends on: a session with session_expires_at in
// the past and connected=false MUST be DELETEd on the next janitor
// tick. The test seeds the row directly (no live conn) so we exercise
// the SQL path without timing flakes.
func TestJanitorExpireSessionsDeletesPastExpiry(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]

	ctx := context.Background()
	if _, err := mh.Pool.Exec(ctx, `
		INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start,
		                     session_expires_at)
		VALUES ('expired-1', NULL, false, 5, false, now() - interval '1 second')
	`); err != nil {
		t.Fatalf("seed expired: %v", err)
	}
	if _, err := mh.Pool.Exec(ctx, `
		INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start,
		                     session_expires_at)
		VALUES ('alive-1', NULL, false, 5, false, now() + interval '60 seconds')
	`); err != nil {
		t.Fatalf("seed alive: %v", err)
	}

	jt := janitor.New(mh.Pool, pod.Engine, warnLogger())
	if err := jt.Tick(ctx); err != nil {
		t.Fatalf("tick: %v", err)
	}

	var n int
	if err := mh.Pool.QueryRow(ctx,
		`SELECT count(*) FROM sessions WHERE client_id='expired-1'`).Scan(&n); err != nil {
		t.Fatalf("query expired: %v", err)
	}
	if n != 0 {
		t.Errorf("expired session not deleted: count=%d", n)
	}
	if err := mh.Pool.QueryRow(ctx,
		`SELECT count(*) FROM sessions WHERE client_id='alive-1'`).Scan(&n); err != nil {
		t.Fatalf("query alive: %v", err)
	}
	if n != 1 {
		t.Errorf("alive session deleted prematurely: count=%d", n)
	}
}

func TestJanitorOrphanSweep(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]
	l, err := listener.Start(context.Background(), mh.URL, pod.Engine, warnLogger())
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	t.Cleanup(l.Stop)
	pod.Engine.SetBrokerID(l.BrokerID())

	// Insert an orphan message older than the grace.
	_, err = mh.Pool.Exec(context.Background(), `
		INSERT INTO messages(topic, payload, qos, retain, created_at)
		VALUES ('orphan', $1, 0, false, now() - interval '1 hour')
	`, []byte{})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	// And a recent orphan (must survive).
	_, err = mh.Pool.Exec(context.Background(), `
		INSERT INTO messages(topic, payload, qos, retain, created_at)
		VALUES ('fresh', $1, 0, false, now())
	`, []byte{})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	jt := janitor.New(mh.Pool, pod.Engine, warnLogger())
	jt.SetOrphanGrace(10 * time.Minute)
	if err := jt.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	var topics []string
	rows, err := mh.Pool.Query(context.Background(),
		`SELECT topic FROM messages ORDER BY topic`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		topics = append(topics, s)
	}
	if len(topics) != 1 || topics[0] != "fresh" {
		t.Errorf("topics after sweep: %v", topics)
	}
}

// TestJanitorInboundQoS2Sweep seeds inbound_qos2 rows for a disconnected
// client and a connected client, both older than grace, and asserts the
// sweep only evicts the disconnected one's tombstones.
func TestJanitorInboundQoS2Sweep(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]
	l, err := listener.Start(context.Background(), mh.URL, pod.Engine, warnLogger())
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	t.Cleanup(l.Stop)
	pod.Engine.SetBrokerID(l.BrokerID())

	ctx := context.Background()
	// Disconnected session — its tombstone should go.
	if _, err := mh.Pool.Exec(ctx, `
		INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start)
		VALUES ('q2-dead', NULL, false, 5, false)`); err != nil {
		t.Fatalf("seed dead session: %v", err)
	}
	// Connected session — its tombstone must survive.
	if _, err := mh.Pool.Exec(ctx, `
		INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start)
		VALUES ('q2-live', $1, true, 5, false)`, l.BrokerID()); err != nil {
		t.Fatalf("seed live session: %v", err)
	}
	// Old tombstones (older than 1h grace).
	if _, err := mh.Pool.Exec(ctx, `
		INSERT INTO inbound_qos2(client_id, packet_id, received_at) VALUES
		('q2-dead', 1, now() - interval '2 hours'),
		('q2-live', 1, now() - interval '2 hours')`); err != nil {
		t.Fatalf("seed tombstones: %v", err)
	}
	// Recent tombstone for dead session — must survive (within grace).
	if _, err := mh.Pool.Exec(ctx, `
		INSERT INTO inbound_qos2(client_id, packet_id, received_at) VALUES
		('q2-dead', 2, now())`); err != nil {
		t.Fatalf("seed recent tombstone: %v", err)
	}

	jt := janitor.New(mh.Pool, pod.Engine, warnLogger())
	jt.SetInboundQoS2Grace(1 * time.Hour)
	if err := jt.Tick(ctx); err != nil {
		t.Fatalf("tick: %v", err)
	}

	type tomb struct {
		client string
		packet int
	}
	var got []tomb
	rows, err := mh.Pool.Query(ctx,
		`SELECT client_id, packet_id FROM inbound_qos2 ORDER BY client_id, packet_id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	for rows.Next() {
		var v tomb
		if err := rows.Scan(&v.client, &v.packet); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, v)
	}
	want := []tomb{{"q2-dead", 2}, {"q2-live", 1}}
	if len(got) != len(want) {
		t.Fatalf("rows after sweep: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d: got %v want %v", i, got[i], want[i])
		}
	}
}

// TestJanitorConcurrentFireDueWillsExactlyOnce seeds a single session whose
// will_fire_at is in the past, then calls fireDueWills from N goroutines
// simultaneously. The SELECT … FOR UPDATE SKIP LOCKED gate plus the
// publish-then-clear-will-columns sequence inside one tx must serialise
// such that exactly one publish is emitted and the WillsFiredTotal counter
// increments exactly once.
func TestJanitorConcurrentFireDueWillsExactlyOnce(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]

	l, err := listener.Start(context.Background(), mh.URL, pod.Engine, warnLogger())
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	t.Cleanup(l.Stop)
	pod.Engine.SetBrokerID(l.BrokerID())
	pod.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
	pod.BrokerID = l.BrokerID()

	// Subscribe so we can count delivered will messages on the wire.
	observer := pod.Connect(t, "obs-conc-will")
	defer observer.Close()
	observer.Subscribe(t, "lwt/conc/+", 1)

	// Seed a session row with a will whose fire-at is already in the past.
	// connected=false so the session isn't expected to be alive on this Pod;
	// fireDueWills uses SKIP LOCKED on the will_fire_at predicate, not on
	// the connected flag.
	if _, err := mh.Pool.Exec(context.Background(), `
		INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start,
		    will_topic, will_payload, will_qos, will_retain,
		    will_fire_at)
		VALUES ($1, NULL, false, 5, false, 'lwt/conc/x', $2, 1, false,
		    now() - interval '1 second')
	`, "conc-willer", []byte("payload-conc")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	mtx := metrics.New()
	jt := janitor.New(mh.Pool, pod.Engine, warnLogger())
	jt.SetMetrics(mtx)

	// Fire fireDueWills from N goroutines concurrently. Use a start barrier
	// so the threads release together, maximising the contention window.
	const n = 4
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			if err := jt.FireDueWillsForTest(context.Background()); err != nil {
				t.Errorf("fireDueWills: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	// Assert: messages table holds exactly one row for this topic.
	var msgCount int
	if err := mh.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM messages WHERE topic='lwt/conc/x'`).Scan(&msgCount); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if msgCount != 1 {
		t.Errorf("messages for will: got %d, want 1", msgCount)
	}

	// Assert: the WillsFiredTotal counter incremented exactly once.
	var pb dto.Metric
	if err := mtx.WillsFiredTotal.Write(&pb); err != nil {
		t.Fatalf("read WillsFiredTotal: %v", err)
	}
	if got := pb.GetCounter().GetValue(); got != 1 {
		t.Errorf("WillsFiredTotal: got %g, want 1", got)
	}

	// Assert: the session row's will_* columns are cleared.
	var willTopic *string
	if err := mh.Pool.QueryRow(context.Background(),
		`SELECT will_topic FROM sessions WHERE client_id='conc-willer'`).Scan(&willTopic); err != nil {
		t.Fatalf("query session: %v", err)
	}
	if willTopic != nil {
		t.Errorf("will_topic not cleared: %v", *willTopic)
	}

	// Drain the observer (one PUBLISH expected) so the engine doesn't
	// stall on per-conn outbound flow at test shutdown.
	pk := observer.Read(t, 3*time.Second)
	if pk.FixedHeader.Type != packets.Publish {
		t.Fatalf("expected publish, got %d", pk.FixedHeader.Type)
	}
	if pk.TopicName != "lwt/conc/x" || string(pk.Payload) != "payload-conc" {
		t.Errorf("got %q/%q", pk.TopicName, pk.Payload)
	}
}

// TestJanitorConcurrentHandleDeadBrokerLockExclusive seeds a sessions row
// pointing at a fabricated broker UUID and calls handleDeadBroker from N
// goroutines. The pg_try_advisory_lock(per-broker key) must serialise such
// that exactly one caller observes claimed=true; concurrent callers see
// claimed=false. The metric DeadBrokersTotal increments exactly once.
func TestJanitorConcurrentHandleDeadBrokerLockExclusive(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]

	l, err := listener.Start(context.Background(), mh.URL, pod.Engine, warnLogger())
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	t.Cleanup(l.Stop)
	pod.Engine.SetBrokerID(l.BrokerID())
	pod.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
	pod.BrokerID = l.BrokerID()

	// Insert a session pointing at a fabricated broker UUID — no real
	// listener holds the per-broker advisory lock for this UUID, so
	// pg_try_advisory_lock will succeed for whichever caller wins the
	// race.
	deadBroker := uuid.New()
	if _, err := mh.Pool.Exec(context.Background(), `
		INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start,
		    will_topic, will_payload, will_qos, will_retain)
		VALUES ($1, $2, true, 5, false, 'lwt/dead/conc', $3, 1, false)
	`, "ghost-conc", deadBroker, []byte("ghost-conc-died")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	mtx := metrics.New()
	jt := janitor.New(mh.Pool, pod.Engine, warnLogger())
	jt.SetMetrics(mtx)

	const n = 4
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	var (
		mu       sync.Mutex
		claimedN int
		errN     int
	)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			claimed, err := jt.HandleDeadBrokerForTest(context.Background(), deadBroker)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errN++
				t.Errorf("handleDeadBroker: %v", err)
				return
			}
			if claimed {
				claimedN++
				// Caller-side metric increment is what Tick() does after
				// a successful claim; mirror it here so we can assert on
				// the metric end-to-end.
				mtx.DeadBrokersTotal.Inc()
			}
		}()
	}
	close(start)
	wg.Wait()

	if errN != 0 {
		t.Fatalf("got %d handleDeadBroker errors", errN)
	}
	if claimedN != 1 {
		t.Errorf("claimed count: got %d, want 1", claimedN)
	}

	var pb dto.Metric
	if err := mtx.DeadBrokersTotal.Write(&pb); err != nil {
		t.Fatalf("read DeadBrokersTotal: %v", err)
	}
	if got := pb.GetCounter().GetValue(); got != 1 {
		t.Errorf("DeadBrokersTotal: got %g, want 1", got)
	}

	// Sessions row's broker_id must be NULL (cleared by the winning
	// claimant exactly once).
	var brokerID *uuid.UUID
	if err := mh.Pool.QueryRow(context.Background(),
		`SELECT broker_id FROM sessions WHERE client_id='ghost-conc'`).Scan(&brokerID); err != nil {
		t.Fatalf("query session: %v", err)
	}
	if brokerID != nil {
		t.Errorf("broker_id not cleared: %v", brokerID)
	}
}

// TestJanitorStratifiedIntervals pins the per-job stratification contract:
// each sub-job fires only when (now - lastRun) >= its configured interval,
// not on every base tick. We use a fake clock that advances 1s per Tick
// and assert that, over a 12s window, fast jobs fire 12× while slow jobs
// fire fewer times in proportion to their interval.
func TestJanitorStratifiedIntervals(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]
	l, err := listener.Start(context.Background(), mh.URL, pod.Engine, warnLogger())
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	t.Cleanup(l.Stop)
	pod.Engine.SetBrokerID(l.BrokerID())

	mtx := metrics.New()
	jt := janitor.New(mh.Pool, pod.Engine, warnLogger())
	jt.SetMetrics(mtx)

	// Configure non-default intervals to make the contract obvious:
	// fast=1s, mid=3s, slow=6s. We'll tick 12× at 1s steps and assert
	// fire counts of 12, 4, 2 respectively.
	jt.SetJobIntervals(map[string]time.Duration{
		janitor.JobFireDueWills:           1 * time.Second,
		janitor.JobFindDeadBrokers:        3 * time.Second,
		janitor.JobExpireSessions:         3 * time.Second,
		janitor.JobExpireRetained:         3 * time.Second,
		janitor.JobRefreshDeliveriesGauge: 3 * time.Second,
		janitor.JobRefreshStateGauges:     3 * time.Second,
		janitor.JobSweepInboundQoS2:       6 * time.Second,
		janitor.JobSweepOrphanDeliveries:  6 * time.Second,
		janitor.JobSweepOrphanMessages:    6 * time.Second,
	})

	// Fake clock starts at a fixed point and advances 1s each Tick.
	base := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	var step int64
	jt.SetNowForTest(func() time.Time {
		// Caller advances `step` between ticks; we read whatever it's set to.
		return base.Add(time.Duration(step) * time.Second)
	})

	const ticks = 12
	for i := 0; i < ticks; i++ {
		step = int64(i)
		if err := jt.Tick(context.Background()); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
	}

	// Read per-job fire counts off pgmqtt_janitor_tick_seconds{job}'s
	// SampleCount. Each timed() call records one observation, so the
	// histogram count == number of fires.
	type expectation struct {
		job  string
		want uint64
	}
	want := []expectation{
		{janitor.JobFireDueWills, 12},          // every tick (1s, ticks 0,1,2,...,11 → 12)
		{janitor.JobFindDeadBrokers, 4},        // 3s interval → ticks 0,3,6,9 → 4
		{janitor.JobExpireSessions, 4},         // 3s interval
		{janitor.JobExpireRetained, 4},         // 3s interval
		{janitor.JobRefreshDeliveriesGauge, 4}, // 3s interval
		{janitor.JobRefreshStateGauges, 4},     // 3s interval
		{janitor.JobSweepInboundQoS2, 2},       // 6s interval → ticks 0,6 → 2
		{janitor.JobSweepOrphanDeliveries, 2},  // 6s interval
		{janitor.JobSweepOrphanMessages, 2},    // 6s interval
	}
	for _, w := range want {
		h, err := mtx.JanitorTickSeconds.GetMetricWithLabelValues(w.job)
		if err != nil {
			t.Fatalf("%s: %v", w.job, err)
		}
		var pb dto.Metric
		// prometheus.Histogram implements prometheus.Metric (Write).
		if err := h.(prometheus.Metric).Write(&pb); err != nil {
			t.Fatalf("%s: %v", w.job, err)
		}
		if got := pb.GetHistogram().GetSampleCount(); got != w.want {
			t.Errorf("%s fired %d times, want %d", w.job, got, w.want)
		}
	}
}

// TestJanitorRefreshStateGauges seeds rows in sessions / subscriptions /
// retained / inbound_qos2 and asserts the per-table cardinality gauges
// reflect the counts after one Tick. Guards against the metric set
// silently regressing if a future schema change breaks the COUNT(*) query.
func TestJanitorRefreshStateGauges(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 1, nil)
	pod := mh.Pods[0]
	l, err := listener.Start(context.Background(), mh.URL, pod.Engine, warnLogger())
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	t.Cleanup(l.Stop)
	pod.Engine.SetBrokerID(l.BrokerID())

	ctx := context.Background()
	// Seed: 3 sessions (2 connected, 1 not), 5 subscriptions across them,
	// 2 retained messages, 4 inbound_qos2 tombstones.
	if _, err := mh.Pool.Exec(ctx, `
		INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start)
		VALUES
		  ('c1', $1, true, 5, false),
		  ('c2', $1, true, 5, false),
		  ('c3', NULL, false, 5, false)
	`, l.BrokerID()); err != nil {
		t.Fatalf("seed sessions: %v", err)
	}
	if _, err := mh.Pool.Exec(ctx, `
		INSERT INTO subscriptions(client_id, topic_filter, qos)
		VALUES ('c1', 'a/+', 1), ('c1', 'b/+', 1),
		       ('c2', 'a/+', 0), ('c2', 'c/#', 2),
		       ('c3', '+/+', 1)
	`); err != nil {
		t.Fatalf("seed subscriptions: %v", err)
	}
	if _, err := mh.Pool.Exec(ctx, `
		INSERT INTO retained(topic, payload, qos)
		VALUES ('r/1', 'p1', 0), ('r/2', 'p2', 1)
	`); err != nil {
		t.Fatalf("seed retained: %v", err)
	}
	if _, err := mh.Pool.Exec(ctx, `
		INSERT INTO inbound_qos2(client_id, packet_id, received_at)
		VALUES ('c1', 1, now()), ('c1', 2, now()),
		       ('c2', 1, now()), ('c3', 1, now())
	`); err != nil {
		t.Fatalf("seed inbound_qos2: %v", err)
	}

	mtx := metrics.New()
	jt := janitor.New(mh.Pool, pod.Engine, warnLogger())
	jt.SetMetrics(mtx)
	if err := jt.Tick(ctx); err != nil {
		t.Fatalf("tick: %v", err)
	}

	checks := []struct {
		name string
		g    interface{ Write(*dto.Metric) error }
		want float64
	}{
		{"pgmqtt_sessions", mtx.Sessions, 3},
		{"pgmqtt_subscriptions", mtx.Subscriptions, 5},
		{"pgmqtt_retained_count", mtx.RetainedCount, 2},
		{"pgmqtt_inbound_qos2_pending", mtx.InboundQoS2Pending, 4},
	}
	for _, c := range checks {
		var pb dto.Metric
		if err := c.g.Write(&pb); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got := pb.GetGauge().GetValue(); got != c.want {
			t.Errorf("%s: got %g, want %g", c.name, got, c.want)
		}
	}
}
