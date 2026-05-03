// soak runs a publishers + subscribers traffic generator against any
// MQTT broker reachable on TCP. It tracks per-message sequence numbers
// to detect loss (QoS-1+) and duplicates (QoS-2). At end-of-run it prints
// a JSON summary suitable for piping into jq.
//
// Pair it with an external chaos loop (e.g. a kubectl-delete-pod ticker)
// to validate broker behaviour under restarts. Example:
//
//	# Terminal A:
//	./soak -broker 127.0.0.1:1883 -duration 10m -rate 1000 -qos 1 -subs 5
//
//	# Terminal B (chaos):
//	while true; do kubectl -n mqtt delete pod -l app=pgmqtt --field-selector ... ; sleep 30; done
//
// Output (last line is JSON):
//
//	{"published":600000,"received":[600000,600000,600000,600000,600000],
//	 "lost":0,"dups":0,"qos":1,"duration":"10m0s"}
//
// Exit code 0 when no QoS≥1 loss and no dups; 1 otherwise.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"

	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

// subStats accumulates per-subscriber receive counts. Loss is counted at
// end-of-run via gap analysis on the seen-sequence map.
type subStats struct {
	received atomic.Int64
	dups     atomic.Int64
	lost     atomic.Int64
	seen     map[int64]int
	mu       sync.Mutex
}

func main() {
	broker := flag.String("broker", "127.0.0.1:1883", "broker host:port")
	user := flag.String("user", "test", "username")
	pass := flag.String("pass", "test", "password")
	dur := flag.Duration("duration", 1*time.Minute, "how long to run")
	rate := flag.Int("rate", 100, "messages per second (publisher)")
	qos := flag.Int("qos", 1, "QoS for publishes (0/1/2)")
	subs := flag.Int("subs", 1, "number of subscribers")
	topic := flag.String("topic", "soak/x", "topic to publish on")
	flag.Parse()

	if *qos < 0 || *qos > 2 {
		log.Fatalf("qos must be 0/1/2")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	endAt := time.Now().Add(*dur)

	subStatsList := make([]*subStats, *subs)
	var subWG sync.WaitGroup
	for i := 0; i < *subs; i++ {
		s := &subStats{seen: map[int64]int{}}
		subStatsList[i] = s
		subWG.Add(1)
		go func(idx int, st *subStats) {
			defer subWG.Done()
			runSubscriber(ctx, *broker, fmt.Sprintf("soak-sub-%d", idx), *user, *pass,
				*topic, byte(*qos), st)
		}(i, s)
	}

	// Give subscribers a moment to land their SUBSCRIBE.
	time.Sleep(500 * time.Millisecond)

	// Publisher.
	var published atomic.Int64
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		runPublisher(ctx, *broker, "soak-pub", *user, *pass, *topic, byte(*qos), *rate, endAt, &published)
	}()

	<-pubDone
	// Drain time — wait for any inflight messages to land.
	time.Sleep(2 * time.Second)
	cancel()
	subWG.Wait()

	// Compute loss via gap analysis on each sub's received set. A sequence
	// is "lost" if it's in [0, max_observed] but missing.
	type subReport struct {
		Received int64 `json:"received"`
		Dups     int64 `json:"dups"`
		Lost     int64 `json:"lost"`
	}
	reports := make([]subReport, len(subStatsList))
	totalLost := int64(0)
	totalDups := int64(0)
	for i, s := range subStatsList {
		s.mu.Lock()
		var maxSeq int64 = -1
		for k := range s.seen {
			if k > maxSeq {
				maxSeq = k
			}
		}
		var lost int64
		if maxSeq >= 0 {
			for j := int64(0); j <= maxSeq; j++ {
				if _, ok := s.seen[j]; !ok {
					lost++
				}
			}
		}
		dups := s.dups.Load()
		s.mu.Unlock()
		reports[i] = subReport{Received: s.received.Load(), Dups: dups, Lost: lost}
		totalLost += lost
		totalDups += dups
	}

	out := map[string]any{
		"broker":       *broker,
		"qos":          *qos,
		"duration":     dur.String(),
		"rate":         *rate,
		"subs":         *subs,
		"published":    published.Load(),
		"sub_reports":  reports,
		"total_lost":   totalLost,
		"total_dups":   totalDups,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)

	if *qos >= 1 && totalLost > 0 {
		log.Printf("FAIL: QoS %d had %d lost messages across %d subscribers", *qos, totalLost, *subs)
		os.Exit(1)
	}
	if *qos == 2 && totalDups > 0 {
		log.Printf("FAIL: QoS 2 had %d duplicates across %d subscribers", totalDups, *subs)
		os.Exit(1)
	}
}

// runPublisher publishes seq=0..N at the configured rate until ctx ends or
// endAt elapses. Each payload is the BE-uint64 sequence number.
func runPublisher(ctx context.Context, broker, clientID, user, pass, topic string,
	qos byte, rate int, endAt time.Time, published *atomic.Int64) {
	c, err := dial(broker)
	if err != nil {
		log.Printf("publisher dial: %v", err)
		return
	}
	defer c.Close()
	r := mqttwire.NewReader(c)
	if err := connect(c, r, clientID, user, pass); err != nil {
		log.Printf("publisher connect: %v", err)
		return
	}

	tick := time.NewTicker(time.Second / time.Duration(rate))
	defer tick.Stop()

	var seq int64
	pid := uint16(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if time.Now().After(endAt) {
			return
		}
		payload := make([]byte, 8)
		binary.BigEndian.PutUint64(payload, uint64(seq))
		seq++
		pk := &packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: qos},
			ProtocolVersion: mqttwire.ProtocolMQTT5,
			TopicName:       topic,
			Payload:         payload,
		}
		if qos > 0 {
			pid++
			if pid == 0 {
				pid = 1
			}
			pk.PacketID = pid
		}
		if err := mqttwire.Write(c, pk); err != nil {
			log.Printf("publish write: %v", err)
			return
		}
		published.Add(1)
		switch qos {
		case 1:
			if _, err := r.Read(); err != nil {
				log.Printf("publish ack read: %v", err)
				return
			}
		case 2:
			rec, err := r.Read()
			if err != nil {
				log.Printf("publish pubrec read: %v", err)
				return
			}
			if rec.FixedHeader.Type != packets.Pubrec {
				log.Printf("expected PUBREC, got %d", rec.FixedHeader.Type)
				return
			}
			if err := mqttwire.Write(c, &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Pubrel, Qos: 1},
				ProtocolVersion: mqttwire.ProtocolMQTT5,
				PacketID:        rec.PacketID,
			}); err != nil {
				log.Printf("publish pubrel: %v", err)
				return
			}
			if _, err := r.Read(); err != nil {
				log.Printf("publish pubcomp read: %v", err)
				return
			}
		}
	}
}

// runSubscriber subscribes and accumulates per-sequence counts. Loss and
// duplicate tally happens at end-of-run.
func runSubscriber(ctx context.Context, broker, clientID, user, pass, topic string,
	qos byte, stats *subStats) {
	c, err := dial(broker)
	if err != nil {
		log.Printf("subscriber dial: %v", err)
		return
	}
	defer c.Close()
	r := mqttwire.NewReader(c)
	if err := connect(c, r, clientID, user, pass); err != nil {
		log.Printf("subscriber connect: %v", err)
		return
	}
	sub := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		PacketID:        1,
		Filters:         packets.Subscriptions{{Filter: topic, Qos: qos}},
	}
	if err := mqttwire.Write(c, sub); err != nil {
		log.Printf("subscriber sub: %v", err)
		return
	}
	if _, err := r.Read(); err != nil {
		log.Printf("subscriber suback: %v", err)
		return
	}
	for {
		if ctx.Err() != nil {
			return
		}
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		pk, err := r.Read()
		if err != nil {
			if errIsTimeout(err) || ctx.Err() != nil {
				continue
			}
			if err == io.EOF {
				return
			}
			log.Printf("subscriber read: %v", err)
			return
		}
		if pk.FixedHeader.Type != packets.Publish {
			continue
		}
		if len(pk.Payload) < 8 {
			continue
		}
		seq := int64(binary.BigEndian.Uint64(pk.Payload[:8]))
		stats.mu.Lock()
		stats.seen[seq]++
		if stats.seen[seq] > 1 {
			stats.dups.Add(1)
		}
		stats.mu.Unlock()
		stats.received.Add(1)
		if pk.FixedHeader.Qos == 1 {
			_ = mqttwire.Write(c, &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Puback},
				ProtocolVersion: mqttwire.ProtocolMQTT5,
				PacketID:        pk.PacketID,
			})
		} else if pk.FixedHeader.Qos == 2 {
			_ = mqttwire.Write(c, &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Pubrec},
				ProtocolVersion: mqttwire.ProtocolMQTT5,
				PacketID:        pk.PacketID,
			})
			pubrel, err := r.Read()
			if err != nil {
				continue
			}
			if pubrel.FixedHeader.Type == packets.Pubrel {
				_ = mqttwire.Write(c, &packets.Packet{
					FixedHeader:     packets.FixedHeader{Type: packets.Pubcomp},
					ProtocolVersion: mqttwire.ProtocolMQTT5,
					PacketID:        pubrel.PacketID,
				})
			}
		}
	}
}

func dial(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, 5*time.Second)
}

func connect(c net.Conn, r *mqttwire.Reader, clientID, user, pass string) error {
	cp := packets.ConnectParams{
		ProtocolName:     []byte("MQTT"),
		Clean:            true,
		Keepalive:        60,
		ClientIdentifier: clientID,
	}
	if user != "" {
		cp.Username = []byte(user)
		cp.UsernameFlag = true
	}
	if pass != "" {
		cp.Password = []byte(pass)
		cp.PasswordFlag = true
	}
	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		Connect:         cp,
	}
	if err := mqttwire.Write(c, pk); err != nil {
		return err
	}
	r.ProtocolVersion = mqttwire.ProtocolMQTT5
	cack, err := r.Read()
	if err != nil {
		return err
	}
	if cack.FixedHeader.Type != packets.Connack || cack.ReasonCode != 0 {
		return fmt.Errorf("connack reason=%d", cack.ReasonCode)
	}
	return nil
}

func errIsTimeout(err error) bool {
	if err == nil {
		return false
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return true
	}
	return false
}
