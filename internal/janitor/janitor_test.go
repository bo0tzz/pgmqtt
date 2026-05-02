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
