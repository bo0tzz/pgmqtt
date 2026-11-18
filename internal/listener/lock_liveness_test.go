package listener_test

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	"github.com/bo0tzz/pgmqtt/internal/janitor"
	"github.com/bo0tzz/pgmqtt/internal/listener"
)

// terminateListenerBackends kills every listener session and returns once
// those backends have exited, i.e. once their advisory locks are free.
func terminateListenerBackends(t *testing.T, mh *enginetest.MultiHarness) {
	t.Helper()
	ctx := context.Background()
	rows, err := mh.Pool.Query(ctx, `
		SELECT pid FROM pg_stat_activity
		 WHERE application_name = 'pgmqttd-listener'
		   AND datname = current_database()
	`)
	if err != nil {
		t.Fatalf("list listener backends: %v", err)
	}
	pids, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		t.Fatalf("list listener backends: %v", err)
	}
	if _, err := mh.Pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM unnest($1::int[]) AS pid`, pids); err != nil {
		t.Fatalf("pg_terminate_backend: %v", err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		var alive int
		if err := mh.Pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_stat_activity WHERE pid = ANY($1::int[])`, pids).Scan(&alive); err != nil {
			t.Fatalf("poll listener backends: %v", err)
		}
		if alive == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d terminated listener backends never exited", alive)
		}
	}
}

func waitListenerBackends(t *testing.T, mh *enginetest.MultiHarness, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var got int
	for time.Now().Before(deadline) {
		if err := mh.Pool.QueryRow(context.Background(), `
			SELECT count(*) FROM pg_stat_activity
			 WHERE application_name = 'pgmqttd-listener'
			   AND datname = current_database()
		`).Scan(&got); err != nil {
			t.Fatalf("count listener backends: %v", err)
		}
		if got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("listener backends: got %d, want %d", got, want)
}

// TestListenerFlapDoesNotReapLiveBroker replays the 2026-11-17 incident:
// Postgres restarts, every listener session dies, and for a few seconds
// one broker has its pool back but not its listener. Its advisory lock is
// free for that stretch while it is still serving clients.
//
// Before the fix the janitor took a free lock as proof of death. The
// broker's own janitor reaped it (its pool was back before its listener)
// and so could any peer whose listener had already returned: the client's
// retained will was published while the client was connected, and the
// will columns were wiped so no later disconnect could fire it.
//
// The broker under test is held in reconnect by pointing its listener at
// an unreachable address, so the window is wide enough to tick both
// janitors inside it.
func TestListenerFlapDoesNotReapLiveBroker(t *testing.T) {
	var exits atomic.Int32
	t.Cleanup(listener.SetOSExitForTest(func(int) { exits.Add(1) }))

	mh := enginetest.NewMultiHarness(t, 2, nil)
	listeners := make([]*listener.Listener, len(mh.Pods))
	janitors := make([]*janitor.Janitor, len(mh.Pods))
	for i, p := range mh.Pods {
		l, err := listener.Start(context.Background(), mh.URL, p.Engine, newPodLogger())
		if err != nil {
			t.Fatalf("listener[%d]: %v", i, err)
		}
		listeners[i] = l
		t.Cleanup(l.Stop)
		p.Engine.SetBrokerID(l.BrokerID())
		p.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
		p.BrokerID = l.BrokerID()

		janitors[i] = janitor.New(mh.Pool, p.Engine, newPodLogger())
		janitors[i].SetJobIntervals(map[string]time.Duration{janitor.JobFindDeadBrokers: 0})
		janitors[i].SetLockEpoch(l.LockEpoch)
	}
	tickAll := func() {
		t.Helper()
		for i, j := range janitors {
			if err := j.Tick(context.Background()); err != nil {
				t.Fatalf("janitor[%d] tick: %v", i, err)
			}
		}
	}

	const willTopic = "flap/bridge/state"
	observer := mh.Pods[1].Connect(t, "flap-observer")
	defer observer.Close()
	observer.Subscribe(t, willTopic, 1)

	const willerID = "flap-willer"
	willer := mh.Pods[0].Connect(t, willerID, func(p *packets.Packet) {
		p.Connect.WillFlag = true
		p.Connect.WillTopic = willTopic
		p.Connect.WillPayload = []byte("offline")
		p.Connect.WillQos = 1
		p.Connect.WillRetain = true
	})
	defer willer.Close()

	assertSessionIntact := func(when string) {
		t.Helper()
		var brokerID *uuid.UUID
		var connected bool
		var gotWill *string
		if err := mh.Pool.QueryRow(context.Background(),
			`SELECT broker_id, connected, will_topic FROM sessions WHERE client_id=$1`,
			willerID).Scan(&brokerID, &connected, &gotWill); err != nil {
			t.Fatalf("%s: read session: %v", when, err)
		}
		if brokerID == nil || *brokerID != mh.Pods[0].BrokerID || !connected {
			t.Errorf("%s: session disowned: broker_id=%v connected=%v", when, brokerID, connected)
		}
		if gotWill == nil || *gotWill != willTopic {
			t.Errorf("%s: will wiped from session: will_topic=%v", when, gotWill)
		}
		var retained int
		if err := mh.Pool.QueryRow(context.Background(),
			`SELECT count(*) FROM retained WHERE topic=$1`, willTopic).Scan(&retained); err != nil {
			t.Fatalf("%s: read retained: %v", when, err)
		}
		if retained != 0 {
			t.Errorf("%s: will was published for a connected client", when)
		}
	}

	listeners[0].SetURLForTest("postgres://nobody:nobody@127.0.0.1:1/nodb?sslmode=disable")
	terminateListenerBackends(t, mh)
	// Pod 1's listener is back; pod 0's is not, so pod 0's lock is free.
	waitListenerBackends(t, mh, 1)

	for i := 0; i < 3; i++ {
		tickAll()
	}
	if pk := observer.TryRead(300 * time.Millisecond); pk != nil {
		t.Errorf("observer got a packet while the willer was connected: type=%d topic=%q payload=%q",
			pk.FixedHeader.Type, pk.TopicName, pk.Payload)
	}
	assertSessionIntact("listener down")

	listeners[0].SetURLForTest(mh.URL)
	waitListenerBackends(t, mh, 2)
	tickAll()
	assertSessionIntact("listener back")

	// The will must still be armed: a real death after the flap fires it.
	willer.Kill()
	pk := observer.Read(t, 10*time.Second)
	if pk.FixedHeader.Type != packets.Publish || pk.TopicName != willTopic || string(pk.Payload) != "offline" {
		t.Fatalf("after real disconnect: type=%d topic=%q payload=%q",
			pk.FixedHeader.Type, pk.TopicName, pk.Payload)
	}

	if n := exits.Load(); n != 0 {
		t.Errorf("listener gave up %d times; it should have reconnected", n)
	}
}

// TestReconnectGivesUpAtDeadlineWhenDialHangs pins the broker's side of
// the liveness contract: without its lock it must exit by
// LockReacquireDeadline, even when the reconnect is stuck rather than
// failing. The server here accepts the TCP connection and then never
// answers the startup message, which previously hung the first attempt
// for as long as the process lived.
func TestReconnectGivesUpAtDeadlineWhenDialHangs(t *testing.T) {
	hang, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer hang.Close()
	go func() {
		for {
			c, err := hang.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()

	exited := make(chan int, 1)
	t.Cleanup(listener.SetOSExitForTest(func(code int) {
		select {
		case exited <- code:
		default:
		}
	}))
	t.Cleanup(listener.SetReconnectDeadlineForTest(500 * time.Millisecond))

	mh := enginetest.NewMultiHarness(t, 1, nil)
	l, err := listener.Start(context.Background(), mh.URL, mh.Pods[0].Engine, newPodLogger())
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	t.Cleanup(l.Stop)

	l.SetURLForTest(fmt.Sprintf("postgres://nobody:nobody@%s/nodb?sslmode=disable", hang.Addr()))
	terminateListenerBackends(t, mh)

	select {
	case code := <-exited:
		if code != 1 {
			t.Errorf("exit code: got %d, want 1", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("listener still reconnecting long after its deadline")
	}
}
