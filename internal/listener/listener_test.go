package listener_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	"github.com/bo0tzz/pgmqtt/internal/listener"
)

func newPodLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func TestCrossPodFanout(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 2, nil)
	for _, p := range mh.Pods {
		l, err := listener.Start(context.Background(), mh.URL, p.Engine, newPodLogger())
		if err != nil {
			t.Fatalf("listener: %v", err)
		}
		t.Cleanup(l.Stop)
		p.Engine.SetBrokerID(l.BrokerID())
		p.Engine.SetNotifier(listener.NewNotifier(mh.Pool))
		p.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
		p.BrokerID = l.BrokerID()
	}

	subClient := mh.Pods[0].Connect(t, "sub-cross")
	pubClient := mh.Pods[1].Connect(t, "pub-cross")
	defer subClient.Close()
	defer pubClient.Close()

	subClient.Subscribe(t, "x/#", 1)

	pubClient.Publish(t, "x/y", []byte("hi"), 1, false)

	pk := subClient.Read(t, 3*time.Second)
	if pk.FixedHeader.Type != packets.Publish {
		t.Fatalf("expected publish, got %d", pk.FixedHeader.Type)
	}
	if string(pk.Payload) != "hi" {
		t.Errorf("payload = %q", pk.Payload)
	}
}

func TestSessionMigratesOnPodLoss(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 2, nil)
	listeners := make(map[int]*listener.Listener)
	for i, p := range mh.Pods {
		l, err := listener.Start(context.Background(), mh.URL, p.Engine, newPodLogger())
		if err != nil {
			t.Fatalf("listener: %v", err)
		}
		listeners[i] = l
		t.Cleanup(l.Stop)
		p.Engine.SetBrokerID(l.BrokerID())
		p.Engine.SetNotifier(listener.NewNotifier(mh.Pool))
		p.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
		p.BrokerID = l.BrokerID()
	}

	// Persistent client on pod 0 with a subscription, then disconnect cleanly.
	notClean := func(p *packets.Packet) { p.Connect.Clean = false }
	c1 := mh.Pods[0].Connect(t, "migrant", notClean)
	c1.Subscribe(t, "mig/#", 1)
	c1.Close()

	// "Kill" pod 0 by stopping its listener — releases its advisory lock.
	// Any session rows still pointing at broker 0 would now be reclaimable.
	listeners[0].Stop()

	// Reconnect same client_id on pod 1 — it should succeed (the dead pod's
	// takeover NOTIFY is unobserved but irrelevant) and the subscription must
	// still be intact.
	c2 := mh.Pods[1].Connect(t, "migrant", notClean)
	defer c2.Close()

	pub := mh.Pods[1].Connect(t, "mig-pub")
	defer pub.Close()
	pub.Publish(t, "mig/x", []byte("alive"), 1, false)

	pk := c2.Read(t, 3*time.Second)
	if pk.FixedHeader.Type != packets.Publish || string(pk.Payload) != "alive" {
		t.Fatalf("post-migration delivery failed: type=%d payload=%q", pk.FixedHeader.Type, pk.Payload)
	}
}

func TestTakeoverClosesPriorPodSocket(t *testing.T) {
	t.Parallel()
	mh := enginetest.NewMultiHarness(t, 2, nil)
	for _, p := range mh.Pods {
		l, err := listener.Start(context.Background(), mh.URL, p.Engine, newPodLogger())
		if err != nil {
			t.Fatalf("listener: %v", err)
		}
		t.Cleanup(l.Stop)
		p.Engine.SetBrokerID(l.BrokerID())
		p.Engine.SetNotifier(listener.NewNotifier(mh.Pool))
		p.Engine.SetTakeoverNotifier(listener.NewTakeoverNotifier(mh.Pool))
		p.BrokerID = l.BrokerID()
	}

	first := mh.Pods[0].Connect(t, "client-A")
	defer first.Close()

	// Same client_id reconnects to a different pod.
	second := mh.Pods[1].Connect(t, "client-A")
	defer second.Close()

	// First connection should be closed by the takeover signal.
	if err := first.Conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	_, err := first.NextRaw()
	if err == nil {
		t.Fatalf("expected first conn to be closed; read succeeded")
	}
}
