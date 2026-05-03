package janitor_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	"github.com/bo0tzz/pgmqtt/internal/janitor"
	"github.com/bo0tzz/pgmqtt/internal/listener"
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
	pod.Engine.SetNotifier(listener.NewNotifier(mh.Pool))
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
	pod.Engine.SetNotifier(listener.NewNotifier(mh.Pool))

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
