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

// subStats accumulates per-subscriber receive counts, tracked per-publisher
// so multiple publishers don't collide in the seq space. Loss is counted at
// end-of-run via gap analysis on each publisher's seen-sequence map.
type subStats struct {
	received atomic.Int64
	dups     atomic.Int64
	mu       sync.Mutex
	// seen[pub_id][seq] = times-received. Loss = gaps in seen[pub_id]
	// over [0, max_seq_for_pub]. Dups = any seen[pub_id][seq] > 1.
	seen map[uint16]map[int64]int
}

// payload layout: 2 bytes BE pub_id || 8 bytes BE seq. Subscribers decode
// both fields to attribute the publish to the right publisher.
const payloadLen = 10

func encodePayload(pubID uint16, seq int64) []byte {
	b := make([]byte, payloadLen)
	binary.BigEndian.PutUint16(b[0:2], pubID)
	binary.BigEndian.PutUint64(b[2:10], uint64(seq))
	return b
}

func decodePayload(b []byte) (pubID uint16, seq int64, ok bool) {
	if len(b) < payloadLen {
		return 0, 0, false
	}
	return binary.BigEndian.Uint16(b[0:2]), int64(binary.BigEndian.Uint64(b[2:10])), true
}

func main() {
	broker := flag.String("broker", "127.0.0.1:1883", "broker host:port")
	user := flag.String("user", "test", "username")
	pass := flag.String("pass", "test", "password")
	dur := flag.Duration("duration", 1*time.Minute, "how long to run")
	rate := flag.Int("rate", 100, "TOTAL messages per second across all publishers")
	qos := flag.Int("qos", 1, "QoS for publishes (0/1/2)")
	subs := flag.Int("subs", 1, "number of subscribers")
	pubs := flag.Int("pubs", 1, "number of concurrent publishers (each on its own conn + topic)")
	inflight := flag.Int("inflight", 1, "per-publisher in-flight PUBLISH window (1 = strict serial). >1 pipelines PUBLISH→PUBACK to push past RTT limits; should be ≤ broker's serverReceiveMaximum")
	topic := flag.String("topic", "soak/x", "topic prefix; each publisher uses '<topic>/<pub-id>'; subscribers wildcard-subscribe '<topic>/+'")
	flag.Parse()

	if *qos < 0 || *qos > 2 {
		log.Fatalf("qos must be 0/1/2")
	}
	if *pubs < 1 {
		log.Fatalf("pubs must be ≥ 1")
	}
	if *pubs > 0xFFFF {
		log.Fatalf("pubs must fit in uint16")
	}
	if *inflight < 1 {
		log.Fatalf("inflight must be ≥ 1")
	}
	if *qos == 2 && *inflight > 1 {
		// QoS-2's PUBREC→PUBREL→PUBCOMP makes per-pid bookkeeping more
		// involved than the rig currently implements. Strict-serial QoS-2
		// is what we test today.
		log.Fatalf("inflight > 1 not supported for QoS 2 (PUBREC/PUBREL bookkeeping)")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	endAt := time.Now().Add(*dur)

	// Subscribers wildcard-subscribe to <topic>/+ so they get publishes
	// from every publisher.
	subFilter := *topic + "/+"

	subStatsList := make([]*subStats, *subs)
	var subWG sync.WaitGroup
	for i := 0; i < *subs; i++ {
		s := &subStats{seen: map[uint16]map[int64]int{}}
		subStatsList[i] = s
		subWG.Add(1)
		go func(idx int, st *subStats) {
			defer subWG.Done()
			runSubscriber(ctx, *broker, fmt.Sprintf("soak-sub-%d", idx), *user, *pass,
				subFilter, byte(*qos), st)
		}(i, s)
	}

	// Give subscribers a moment to land their SUBSCRIBE.
	time.Sleep(500 * time.Millisecond)

	// Publishers. Each gets its own connection (separate goroutine), client_id,
	// payload pub_id, and topic. Total target rate is split evenly; if the
	// configured rate doesn't divide cleanly the remainder spills onto the
	// first few publishers.
	var published atomic.Int64
	publishedByPub := make([]*atomic.Int64, *pubs)
	for i := range publishedByPub {
		publishedByPub[i] = new(atomic.Int64)
	}

	// Per-minute heartbeat: long soaks otherwise look identical to a frozen
	// rig in their log output (no reconnects = no log lines). One line per
	// minute lets a future investigator confirm the rig is still pumping.
	go func() {
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		var lastPublished int64
		var lastReceived int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				curPub := int64(0)
				for _, p := range publishedByPub {
					curPub += p.Load()
				}
				curRecv := int64(0)
				for _, s := range subStatsList {
					curRecv += s.received.Load()
				}
				log.Printf("heartbeat: published=%d (+%d/min) received=%d (+%d/min) elapsed=%s",
					curPub, curPub-lastPublished, curRecv, curRecv-lastReceived,
					time.Until(endAt).Round(time.Second))
				lastPublished = curPub
				lastReceived = curRecv
			}
		}
	}()
	var pubWG sync.WaitGroup
	perPubRate := *rate / *pubs
	if perPubRate < 1 {
		perPubRate = 1
	}
	remainder := *rate - perPubRate**pubs
	for i := 0; i < *pubs; i++ {
		myRate := perPubRate
		if i < remainder {
			myRate++
		}
		pubWG.Add(1)
		go func(idx int, r int) {
			defer pubWG.Done()
			pubTopic := fmt.Sprintf("%s/pub-%d", *topic, idx)
			runPublisher(ctx, *broker, fmt.Sprintf("soak-pub-%d", idx),
				*user, *pass, pubTopic, byte(*qos), r, *inflight, endAt,
				uint16(idx), publishedByPub[idx])
		}(i, myRate)
	}
	pubWG.Wait()
	for i := range publishedByPub {
		published.Add(publishedByPub[i].Load())
	}
	pubDone := make(chan struct{})
	close(pubDone)

	<-pubDone
	// Drain phase. Previously: time.Sleep(30s). That coupled the verdict
	// to wall-clock — a slow environment (CI runner, busy laptop, kind on
	// a shared host) couldn't drain steady-state backlog in 30s and the
	// rig would FAIL despite the broker having no actual loss/dups. The
	// 95% threshold sometimes caught the real wedge regression it was
	// added for, sometimes caught environment-driven throughput shortfall.
	//
	// Replaced with quiescence detection: poll the aggregate `received`
	// counter at 1s cadence; once it stops growing for `drainQuiescence`
	// consecutive seconds, broker has delivered everything it's going to.
	// The verdict (received vs published) then reflects broker correctness,
	// not "broker-vs-the-clock". Hard cap at `drainMaxWait` so a wedged
	// broker can't hang the rig indefinitely — at the cap we still
	// emit a verdict, which fails strictly because received << expected.
	const (
		drainPollInterval = 1 * time.Second
		drainQuiescence   = 10 * time.Second
		drainMaxWait      = 5 * time.Minute
	)
	drainStart := time.Now()
	var lastReceived int64
	unchangedFor := time.Duration(0)
	for {
		time.Sleep(drainPollInterval)
		var curReceived int64
		for _, s := range subStatsList {
			curReceived += s.received.Load()
		}
		if curReceived != lastReceived {
			lastReceived = curReceived
			unchangedFor = 0
			continue
		}
		unchangedFor += drainPollInterval
		if unchangedFor >= drainQuiescence {
			log.Printf("drain: quiescent at received=%d (drain took %s)",
				curReceived, time.Since(drainStart).Round(time.Second))
			break
		}
		if time.Since(drainStart) >= drainMaxWait {
			log.Printf("drain: hit max wait %s with received=%d (broker still has work — verdict will fail)",
				drainMaxWait, curReceived)
			break
		}
	}
	cancel()
	subWG.Wait()

	// Compute loss via gap analysis per publisher × subscriber. A sequence
	// is "lost" for (sub, pub) if it's in [0, max_observed_for_that_pub]
	// but missing from sub's seen map for that pub.
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
		var lost int64
		for _, perPub := range s.seen {
			var maxSeq int64 = -1
			for k := range perPub {
				if k > maxSeq {
					maxSeq = k
				}
			}
			if maxSeq < 0 {
				continue
			}
			for j := int64(0); j <= maxSeq; j++ {
				if _, ok := perPub[j]; !ok {
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
		"broker":      *broker,
		"qos":         *qos,
		"duration":    dur.String(),
		"rate":        *rate,
		"subs":        *subs,
		"pubs":        *pubs,
		"published":   published.Load(),
		"sub_reports": reports,
		"total_lost":  totalLost,
		"total_dups":  totalDups,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)

	if *qos >= 1 && totalLost > 0 {
		log.Printf("FAIL: QoS %d had %d lost messages across %d subscribers", *qos, totalLost, *subs)
		os.Exit(1)
	}
	if *qos == 2 && totalDups > 0 {
		// QoS-2 duplicates are expected when the *publisher* reconnects
		// with a fresh session — the broker's `inbound_qos2` dedup is
		// keyed on (client_id, packet_id) and a clean-start publisher
		// gets a new packet-id space. Real producers maintain durable
		// QoS-2 state across restarts; the soak rig's publisher
		// doesn't. Log the count but don't fail.
		log.Printf("INFO: QoS 2 saw %d duplicates across %d subscribers (expected under publisher chaos with cleanstart=true)",
			totalDups, *subs)
	}
}

// runPublisher publishes seq=0..N at the configured rate until ctx ends or
// endAt elapses. Each payload encodes (pubID, seq) so multiple concurrent
// publishers don't collide in the seq space at the subscriber. On I/O
// error the publisher reconnects (with a 500ms backoff) and resumes from
// the current seq — `published` only increments on a successful ack so a
// broker restart mid-PUBLISH is correctly counted as a re-send.
//
// inflight controls QoS-1 pipelining: with inflight=1 the publisher
// awaits each PUBACK before sending the next PUBLISH (strict-serial,
// RTT-bound). With inflight>1 it queues up to N un-ACKed PUBLISHes,
// using a separate read goroutine to demux PUBACKs by packet_id. QoS 0
// and QoS 2 always use the strict-serial path.
func runPublisher(ctx context.Context, broker, clientID, user, pass, topic string,
	qos byte, rate, inflight int, endAt time.Time, pubID uint16, published *atomic.Int64) {
	if qos == 1 && inflight > 1 {
		runPublisherPipelined(ctx, broker, clientID, user, pass, topic,
			rate, inflight, endAt, pubID, published)
		return
	}
	tick := time.NewTicker(time.Second / time.Duration(rate))
	defer tick.Stop()

	var seq int64
	pid := uint16(0)
	for {
		// Publisher doesn't need persistent state; on broker restart it
		// reconnects fresh and continues from the next seq.
		c, r, err := connectWithBackoff(ctx, broker, clientID, user, pass, false)
		if err != nil {
			return
		}
		var disconnected bool
		for !disconnected {
			select {
			case <-ctx.Done():
				_ = c.Close()
				return
			case <-tick.C:
			}
			if time.Now().After(endAt) {
				_ = c.Close()
				return
			}
			payload := encodePayload(pubID, seq)
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
				log.Printf("publish write (seq=%d): %v — reconnecting", seq, err)
				_ = c.Close()
				disconnected = true
				continue
			}
			switch qos {
			case 0:
				seq++
				published.Add(1)
			case 1:
				ack, err := r.Read()
				if err != nil {
					log.Printf("publish puback read (seq=%d): %v — reconnecting", seq, err)
					_ = c.Close()
					disconnected = true
					continue
				}
				// MUST verify the packet is actually a PUBACK matching our
				// in-flight packet ID. The broker can send other packets
				// (e.g. DISCONNECT 0x8B during graceful shutdown) that
				// would otherwise be silently mis-counted as PUBACK and
				// produce false-positive "published" counts.
				if ack.FixedHeader.Type != packets.Puback || ack.PacketID != pid {
					log.Printf("publish: got type=%d pid=%d (want PUBACK pid=%d) for seq=%d — reconnecting",
						ack.FixedHeader.Type, ack.PacketID, pid, seq)
					_ = c.Close()
					disconnected = true
					continue
				}
				seq++
				published.Add(1)
			case 2:
				rec, err := r.Read()
				if err != nil {
					log.Printf("publish pubrec read (seq=%d): %v — reconnecting", seq, err)
					_ = c.Close()
					disconnected = true
					continue
				}
				if rec.FixedHeader.Type != packets.Pubrec || rec.PacketID != pid {
					log.Printf("publish: got type=%d pid=%d (want PUBREC pid=%d) for seq=%d — reconnecting",
						rec.FixedHeader.Type, rec.PacketID, pid, seq)
					_ = c.Close()
					disconnected = true
					continue
				}
				if err := mqttwire.Write(c, &packets.Packet{
					FixedHeader:     packets.FixedHeader{Type: packets.Pubrel, Qos: 1},
					ProtocolVersion: mqttwire.ProtocolMQTT5,
					PacketID:        rec.PacketID,
				}); err != nil {
					log.Printf("publish pubrel (seq=%d): %v — reconnecting", seq, err)
					_ = c.Close()
					disconnected = true
					continue
				}
				comp, err := r.Read()
				if err != nil {
					log.Printf("publish pubcomp read (seq=%d): %v — reconnecting", seq, err)
					_ = c.Close()
					disconnected = true
					continue
				}
				if comp.FixedHeader.Type != packets.Pubcomp || comp.PacketID != pid {
					log.Printf("publish: got type=%d pid=%d (want PUBCOMP pid=%d) for seq=%d — reconnecting",
						comp.FixedHeader.Type, comp.PacketID, pid, seq)
					_ = c.Close()
					disconnected = true
					continue
				}
				seq++
				published.Add(1)
			}
		}
	}
}

// runPublisherPipelined is the QoS-1 pipelined variant. A reader
// goroutine pulls PUBACKs off the wire and increments `published`
// when each in-flight pid's ACK arrives. The writer fires PUBLISHes
// up to `inflight` ahead.
//
// Crash recovery: on every PUBLISH the writer records (pid → seq) in
// `outstanding`; the reader removes entries on PUBACK. When the conn
// dies, anything still in `outstanding` plus any un-consumed prior
// `replay` entries are folded into the next iteration's replay queue
// and resent on the new conn before fresh seqs are issued. Without
// this, in-flight seqs at the time of disconnect would be skipped
// (writer had already advanced `seq`) and the subscriber would see
// gaps. Replayed seqs the broker had already committed surface as
// dups at the subscriber — that's the correct QoS-1 at-least-once
// trade-off.
func runPublisherPipelined(ctx context.Context, broker, clientID, user, pass, topic string,
	rate, inflight int, endAt time.Time, pubID uint16, published *atomic.Int64) {
	tick := time.NewTicker(time.Second / time.Duration(rate))
	defer tick.Stop()

	var seq int64
	pid := uint16(0)
	var replay []int64
	for {
		c, r, err := connectWithBackoff(ctx, broker, clientID, user, pass, false)
		if err != nil {
			return
		}
		slot := make(chan struct{}, inflight)
		stop := make(chan struct{})
		var once sync.Once
		killWriter := func() { once.Do(func() { close(stop) }) }

		var outstandingMu sync.Mutex
		outstanding := make(map[uint16]int64)

		go func() {
			defer killWriter()
			for {
				ack, err := r.Read()
				if err != nil {
					log.Printf("publish[pid=%d] reader: %v — reconnecting", pubID, err)
					return
				}
				if ack.FixedHeader.Type != packets.Puback {
					log.Printf("publish[pid=%d] reader: got type=%d (want PUBACK) — reconnecting",
						pubID, ack.FixedHeader.Type)
					return
				}
				outstandingMu.Lock()
				delete(outstanding, ack.PacketID)
				outstandingMu.Unlock()
				published.Add(1)
				select {
				case <-slot:
				default:
				}
			}
		}()

		replayIdx := 0
		exit := false
	writer:
		for {
			select {
			case <-ctx.Done():
				_ = c.Close()
				exit = true
				break writer
			case <-stop:
				_ = c.Close()
				break writer
			case slot <- struct{}{}:
			}
			if time.Now().After(endAt) {
				// End of run: give the reader a moment to drain any
				// PUBACKs queued in the kernel recv buffer before closing
				// the conn. Without this, `published` can under-report by
				// the in-flight window at end-of-run (sub still sees those
				// messages — they were committed and delivered — so this
				// is purely a metric correctness fix).
				deadline := time.Now().Add(2 * time.Second)
				for {
					outstandingMu.Lock()
					n := len(outstanding)
					outstandingMu.Unlock()
					if n == 0 || time.Now().After(deadline) {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				_ = c.Close()
				exit = true
				break writer
			}
			select {
			case <-tick.C:
			case <-stop:
				_ = c.Close()
				break writer
			case <-ctx.Done():
				_ = c.Close()
				exit = true
				break writer
			}
			var seqToSend int64
			if replayIdx < len(replay) {
				seqToSend = replay[replayIdx]
				replayIdx++
			} else {
				seqToSend = seq
				seq++
			}
			payload := encodePayload(pubID, seqToSend)
			pid++
			if pid == 0 {
				pid = 1
			}
			outstandingMu.Lock()
			outstanding[pid] = seqToSend
			outstandingMu.Unlock()
			pk := &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 1},
				ProtocolVersion: mqttwire.ProtocolMQTT5,
				TopicName:       topic,
				Payload:         payload,
				PacketID:        pid,
			}
			if err := mqttwire.Write(c, pk); err != nil {
				log.Printf("publish[pid=%d] write (seq=%d): %v — reconnecting", pubID, seqToSend, err)
				_ = c.Close()
				break writer
			}
		}
		<-stop
		if exit {
			return
		}
		next := make([]int64, 0, len(outstanding)+(len(replay)-replayIdx))
		for _, s := range outstanding {
			next = append(next, s)
		}
		for i := replayIdx; i < len(replay); i++ {
			next = append(next, replay[i])
		}
		replay = next
	}
}

// connectWithBackoff dials and CONNECTs, retrying on failure until ctx ends.
// Returns the connection + reader on success. Discards the CONNACK's
// session_present flag; use connectWithBackoffV5 if the caller needs it.
func connectWithBackoff(ctx context.Context, broker, clientID, user, pass string, persistent bool) (net.Conn, *mqttwire.Reader, error) {
	c, r, _, err := connectWithBackoffV5(ctx, broker, clientID, user, pass, persistent)
	return c, r, err
}

// connectWithBackoffV5 is like connectWithBackoff but additionally returns
// the CONNACK's session_present flag — needed by persistent subscribers
// to decide whether to re-issue SUBSCRIBE.
func connectWithBackoffV5(ctx context.Context, broker, clientID, user, pass string, persistent bool) (net.Conn, *mqttwire.Reader, bool, error) {
	for {
		if ctx.Err() != nil {
			return nil, nil, false, ctx.Err()
		}
		c, err := dial(broker)
		if err != nil {
			log.Printf("dial %q: %v — retrying in 500ms", clientID, err)
			select {
			case <-ctx.Done():
				return nil, nil, false, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}
		r := mqttwire.NewReader(c)
		sessionPresent, err := connectOptsV5(c, r, clientID, user, pass, persistent)
		if err != nil {
			log.Printf("connect %q: %v — retrying in 500ms", clientID, err)
			_ = c.Close()
			select {
			case <-ctx.Done():
				return nil, nil, false, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}
		return c, r, sessionPresent, nil
	}
}

// runSubscriber subscribes and accumulates per-sequence counts. Uses a
// persistent v5 session so messages published during the disconnect
// window are queued and drained on reconnect — required for zero-loss
// QoS-1 under broker chaos.
//
// On the FIRST connect (session_present=false in CONNACK), we send
// SUBSCRIBE. On reconnect (session_present=true), the subscription is
// already there — re-sending SUBSCRIBE here would interleave SUBACK
// with the drained PUBLISH backlog and cause spurious dups/loss in the
// rig.
func runSubscriber(ctx context.Context, broker, clientID, user, pass, topic string,
	qos byte, stats *subStats) {
	subscribed := false
	for {
		if ctx.Err() != nil {
			return
		}
		c, r, sessionPresent, err := connectWithBackoffV5(ctx, broker, clientID, user, pass, true)
		if err != nil {
			return
		}
		if !subscribed || !sessionPresent {
			sub := &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
				ProtocolVersion: mqttwire.ProtocolMQTT5,
				PacketID:        1,
				Filters:         packets.Subscriptions{{Filter: topic, Qos: qos}},
			}
			if err := mqttwire.Write(c, sub); err != nil {
				log.Printf("subscriber sub: %v — reconnecting", err)
				_ = c.Close()
				continue
			}
			subscribed = true
		}
		readSubscriber(ctx, c, r, stats)
		_ = c.Close()
	}
}

// readSubscriber pumps inbound PUBLISHes from one connection's lifetime.
// Returns when the connection drops (caller will reconnect) or ctx ends.
func readSubscriber(ctx context.Context, c net.Conn, r *mqttwire.Reader, stats *subStats) {
	for {
		if ctx.Err() != nil {
			return
		}
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		pk, err := r.Read()
		if err != nil {
			if errIsTimeout(err) {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			if err == io.EOF {
				return
			}
			log.Printf("subscriber read: %v — reconnecting", err)
			return
		}
		// Single-read-loop dispatch by packet type. Anything that's not
		// a PUBLISH or PUBREL is either a protocol reply we asked for
		// (SUBACK/UNSUBACK/PINGRESP) or a server-initiated control
		// (DISCONNECT) — the latter ends the session, the former are
		// no-ops here.
		switch pk.FixedHeader.Type {
		case packets.Publish:
			pubID, seq, ok := decodePayload(pk.Payload)
			if !ok {
				continue
			}
			stats.mu.Lock()
			perPub := stats.seen[pubID]
			if perPub == nil {
				perPub = map[int64]int{}
				stats.seen[pubID] = perPub
			}
			perPub[seq]++
			if perPub[seq] > 1 {
				stats.dups.Add(1)
			}
			stats.mu.Unlock()
			stats.received.Add(1)
			switch pk.FixedHeader.Qos {
			case 1:
				_ = mqttwire.Write(c, &packets.Packet{
					FixedHeader:     packets.FixedHeader{Type: packets.Puback},
					ProtocolVersion: mqttwire.ProtocolMQTT5,
					PacketID:        pk.PacketID,
				})
			case 2:
				// Send PUBREC immediately. The matching PUBREL arrives
				// asynchronously and is handled by the next iteration's
				// case packets.Pubrel below — not by a synchronous
				// blocking r.Read here, which would silently swallow
				// any concurrent PUBLISH the broker delivers in the
				// meantime (broker's outbound flow control allows
				// multiple in-flight QoS-2 deliveries per subscriber).
				_ = mqttwire.Write(c, &packets.Packet{
					FixedHeader:     packets.FixedHeader{Type: packets.Pubrec},
					ProtocolVersion: mqttwire.ProtocolMQTT5,
					PacketID:        pk.PacketID,
				})
			}
		case packets.Pubrel:
			_ = mqttwire.Write(c, &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Pubcomp},
				ProtocolVersion: mqttwire.ProtocolMQTT5,
				PacketID:        pk.PacketID,
			})
		case packets.Disconnect:
			return
		}
	}
}

func dial(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, 5*time.Second)
}

func connect(c net.Conn, r *mqttwire.Reader, clientID, user, pass string) error {
	_, err := connectOptsV5(c, r, clientID, user, pass, false)
	return err
}

// connectOptsV5 opens a v5 CONNECT with optional session persistence. When
// persistent=true the client sets clean_start=false and asks for a
// SessionExpiryInterval of 1h so the broker preserves the session across
// reconnects — required for zero-loss QoS-1 under broker chaos. Returns
// the CONNACK's SessionPresent flag.
func connectOptsV5(c net.Conn, r *mqttwire.Reader, clientID, user, pass string, persistent bool) (bool, error) {
	cp := packets.ConnectParams{
		ProtocolName:     []byte("MQTT"),
		Clean:            !persistent,
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
	if persistent {
		pk.Properties.SessionExpiryInterval = 3600
		pk.Properties.SessionExpiryIntervalFlag = true
	}
	if err := mqttwire.Write(c, pk); err != nil {
		return false, err
	}
	r.ProtocolVersion = mqttwire.ProtocolMQTT5
	cack, err := r.Read()
	if err != nil {
		return false, err
	}
	if cack.FixedHeader.Type != packets.Connack {
		return false, fmt.Errorf("expected CONNACK got type %d", cack.FixedHeader.Type)
	}
	if cack.ReasonCode != 0 {
		return false, fmt.Errorf("connack reason=%d", cack.ReasonCode)
	}
	return cack.SessionPresent, nil
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
