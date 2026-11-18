package engine

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mochi-mqtt/server/v2/packets"

	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

// How messages reach a client
//
// A publish writes one deliveries row per subscriber and NOTIFYs each
// broker that owns one. From there:
//
//	listener goroutine   NotifyDelivery(msg)       never blocks
//	dispatcher goroutine Deliver(msg)              one query: which of my
//	                                               clients have a row
//	connection's sender  drainBatch                reads its client's rows
//	                                               in id order and writes
//
// Only a connection's own sender goroutine sends it deliveries, and it
// reads them from the table in id order. That one rule is what gives:
// ordering per client; a resumed session's backlog going out before
// anything newer; nothing reaching a client ahead of its CONNACK; and a
// client that has stopped reading holding up nobody but itself.
//
// The listener goroutine in particular must stay free: it is also what
// notices this broker's advisory lock is gone (see the listener package).

const (
	// drainBatchSize is how many queued deliveries the sender reads per
	// query.
	drainBatchSize = 64

	// dispatchTaskTimeout bounds one dispatcher task's database work.
	dispatchTaskTimeout = 30 * time.Second
)

// NotifyDelivery records that messageID has deliveries for clients of
// this broker. It returns at once; the lookup happens on the dispatcher.
func (e *Engine) NotifyDelivery(messageID int64) {
	e.dispatch(func(ctx context.Context) {
		if err := e.Deliver(ctx, messageID); err != nil {
			e.logger.Warn("deliver", "msg", messageID, "err", err)
		}
	})
}

// AfterListenerReconnect queues the work that follows the listener
// getting its Postgres session back: re-assert ownership of the sessions
// this broker holds, then wake every sender, because NOTIFYs sent while
// the listener was away went nowhere.
func (e *Engine) AfterListenerReconnect() {
	e.dispatch(func(ctx context.Context) {
		e.AttestOwnedSessions(ctx)
		e.KickAllDrains()
	})
}

// dispatch queues task for the dispatcher goroutine and never blocks.
// Tasks run one at a time in the order queued.
func (e *Engine) dispatch(task func(context.Context)) {
	e.dispatchMu.Lock()
	e.dispatchQ = append(e.dispatchQ, task)
	e.dispatchMu.Unlock()
	e.dispatchOnce.Do(func() { go e.runDispatcher() })
	select {
	case e.dispatchWake <- struct{}{}:
	default:
	}
}

func (e *Engine) runDispatcher() {
	for {
		select {
		case <-e.shutdown:
			return
		case <-e.dispatchWake:
		}
		for {
			e.dispatchMu.Lock()
			if len(e.dispatchQ) == 0 {
				e.dispatchQ = nil
				e.dispatchMu.Unlock()
				break
			}
			task := e.dispatchQ[0]
			e.dispatchQ[0] = nil
			e.dispatchQ = e.dispatchQ[1:]
			e.dispatchMu.Unlock()
			e.runDispatchTask(task)
		}
	}
}

func (e *Engine) runDispatchTask(task func(context.Context)) {
	defer func() {
		if r := recover(); r != nil {
			e.logger.Error("dispatcher task panic", "panic", r)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), dispatchTaskTimeout)
	defer cancel()
	task(ctx)
}

// Deliver wakes the sender of every local client that has a queued
// delivery for messageID. It sends nothing itself.
func (e *Engine) Deliver(ctx context.Context, messageID int64) error {
	start := time.Now()
	rows, err := e.pool.Query(ctx, `
		SELECT DISTINCT d.client_id
		  FROM deliveries d
		  JOIN sessions s ON s.client_id = d.client_id
		 WHERE d.message_id = $1
		   AND s.broker_id = $2
		   AND d.state = 0
	`, messageID, e.BrokerID())
	if err != nil {
		return err
	}
	clients, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	dur := time.Since(start)
	e.metrics.ObserveDeliveryStage("scan", dur)
	e.logSlowStage("delivery", "scan", dur, "msg_id", messageID)
	for _, id := range clients {
		if c, ok := e.ConnFor(id); ok {
			c.KickDrain()
		}
	}
	return nil
}

// queuedDelivery is one state=0 deliveries row with its message.
type queuedDelivery struct {
	id                int64
	qos               byte
	retained          bool
	topic             string
	payload           []byte
	props             []byte
	retain            bool
	expiresAt         *time.Time
	createdAt         time.Time
	subIDs            []int
	retainAsPublished bool
}

// drainBatch sends up to drainBatchSize of this client's queued
// deliveries, oldest first, and reports how many rows it disposed of
// (sent or dropped). Zero means there is nothing more it can do until
// something changes: a new delivery, or an acknowledgement freeing a
// Receive Maximum slot. Only the sender goroutine calls it.
func (c *Conn) drainBatch(ctx context.Context) (int, error) {
	// With the window full, QoS>0 rows have to wait, but QoS 0 behind
	// them need not: ordering is only promised within a QoS level.
	canQoS1 := c.inflightAvailable()
	rows, err := c.eng.pool.Query(ctx, `
		SELECT d.id, d.qos, d.retained,
		       m.topic, m.payload, m.properties, m.retain, m.expires_at, m.created_at,
		       COALESCE(sm.sub_ids, '{}'::int[]) AS sub_ids,
		       COALESCE(sm.retain_as_published, false) AS retain_as_published
		  FROM deliveries d
		  JOIN messages m ON m.id = d.message_id
		  LEFT JOIN LATERAL (
		    SELECT array_agg(sub.subscription_id ORDER BY sub.subscription_id)
		             FILTER (WHERE sub.subscription_id IS NOT NULL) AS sub_ids,
		           bool_or(sub.retain_as_published) AS retain_as_published
		      FROM subscriptions sub
		     WHERE sub.client_id = d.client_id
		       AND mqtt_topic_match(sub.topic_filter, m.topic)
		  ) sm ON true
		 WHERE d.client_id = $1
		   AND d.state = 0
		   AND ($2 OR d.qos = 0)
		 ORDER BY d.id
		 LIMIT $3
	`, c.clientID, canQoS1, drainBatchSize)
	if err != nil {
		return 0, err
	}
	batch, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (queuedDelivery, error) {
		var d queuedDelivery
		err := row.Scan(&d.id, &d.qos, &d.retained, &d.topic, &d.payload, &d.props,
			&d.retain, &d.expiresAt, &d.createdAt, &d.subIDs, &d.retainAsPublished)
		return d, err
	})
	if err != nil || len(batch) == 0 {
		return 0, err
	}

	// Build every packet first: a message that has expired, or that this
	// client could never accept, is dropped without touching flow control.
	type outgoing struct {
		d  queuedDelivery
		pk *packets.Packet
	}
	var (
		send    []outgoing
		drop    []int64
		qos0IDs []int64
	)
	for _, d := range batch {
		pk, reason := c.buildPublish(d, true)
		if pk == nil {
			drop = append(drop, d.id)
			c.eng.metrics.ObserveDeliveryDropped(reason)
			continue
		}
		send = append(send, outgoing{d, pk})
		if d.qos == 0 {
			qos0IDs = append(qos0IDs, d.id)
		}
	}
	disposed := 0
	if len(drop) > 0 {
		if _, err := c.eng.pool.Exec(ctx,
			`DELETE FROM deliveries WHERE id = ANY($1) AND state = 0`, drop); err != nil {
			return 0, err
		}
		disposed += len(drop)
	}

	// QoS 0 is at most once: the row is deleted before the write, in one
	// statement for the whole batch. A row this DELETE does not return
	// was taken by another connection for the same client (a takeover in
	// progress) and is not ours to send.
	claimed := make(map[int64]bool, len(qos0IDs))
	if len(qos0IDs) > 0 {
		rows, err := c.eng.pool.Query(ctx,
			`DELETE FROM deliveries WHERE id = ANY($1) AND state = 0 RETURNING id`, qos0IDs)
		if err != nil {
			return disposed, err
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return disposed, err
		}
		for _, id := range ids {
			claimed[id] = true
		}
	}

	for _, o := range send {
		if o.d.qos == 0 {
			if !claimed[o.d.id] {
				continue
			}
		} else {
			if !canQoS1 {
				continue
			}
			if !c.tryAcquireInflight() {
				canQoS1 = false
				continue
			}
			ok, err := c.claimInflight(ctx, o.d.id, o.pk)
			if err != nil {
				c.returnInflight()
				return disposed, err
			}
			if !ok {
				c.returnInflight()
				continue
			}
		}
		if c.eng.metrics != nil {
			if n := cap(c.inflight); n > 0 {
				c.eng.metrics.OutboundInflightSaturation.Observe(float64(len(c.inflight)) / float64(n))
			}
		}
		// Last, so that an alias is only ever registered by the PUBLISH
		// that announces it.
		c.applyTopicAlias(o.pk)
		writeStart := time.Now()
		err := c.write(o.pk)
		writeDur := time.Since(writeStart)
		c.eng.metrics.ObserveDeliveryStage("write", writeDur)
		c.eng.logSlowStage("delivery", "write", writeDur, "client", c.clientID, "topic", o.d.topic, "qos", o.d.qos)
		if err != nil {
			// write has closed the connection. A QoS>0 row stays
			// in flight and is resent when the session resumes.
			c.eng.metrics.ObserveDeliveryDropped("write_error")
			return disposed, err
		}
		disposed++
		c.eng.metrics.ObserveE2EPublishToDeliver(time.Since(o.d.createdAt))
	}
	return disposed, nil
}

// buildPublish turns a delivery into the PUBLISH for this client, without
// a packet id or topic alias yet. It returns nil and a drop reason for a
// message that must not be sent: one larger than the client's Maximum
// Packet Size [MQTT-3.1.2-25], or, when dropExpired is set, one whose
// Message Expiry Interval has passed [MQTT-3.3.2-5].
func (c *Conn) buildPublish(d queuedDelivery, dropExpired bool) (*packets.Packet, string) {
	if dropExpired && d.expiresAt != nil && !d.expiresAt.After(time.Now()) {
		return nil, "expired"
	}
	pk := &packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type:   packets.Publish,
			Qos:    d.qos,
			Retain: d.retained || (d.retainAsPublished && d.retain),
		},
		TopicName: d.topic,
		Payload:   d.payload,
	}
	if len(d.props) > 0 {
		var p packets.Properties
		if err := json.Unmarshal(d.props, &p); err == nil {
			pk.Properties = p
		}
	}
	if c.protocol == mqttwire.ProtocolMQTT5 {
		// The receiver is told what is left of the expiry budget.
		if d.expiresAt != nil {
			pk.Properties.MessageExpiryInterval = uint32(max(time.Until(*d.expiresAt).Seconds(), 1))
		}
		if len(d.subIDs) > 0 {
			pk.Properties.SubscriptionIdentifier = append([]int(nil), d.subIDs...)
		}
	}
	if c.maxPacketSize > 0 {
		// Measured in its largest form: with a packet id, and with both
		// the topic and an alias, as the first use of an alias carries.
		probe := *pk
		probe.ProtocolVersion = c.protocol
		if d.qos > 0 {
			probe.PacketID = 1
		}
		if c.topicAliasMaximumOut > 0 {
			probe.Properties.TopicAlias = 1
			probe.Properties.TopicAliasFlag = true
		}
		buf, err := mqttwire.Encode(&probe)
		if err != nil || uint32(len(buf)) > c.maxPacketSize {
			return nil, "oversized"
		}
	}
	return pk, ""
}

// claimInflight gives a queued QoS>0 delivery a packet id and marks it in
// flight. False means the row was no longer queued: another connection
// for the same client got to it first.
func (c *Conn) claimInflight(ctx context.Context, deliveryID int64, pk *packets.Packet) (bool, error) {
	start := time.Now()
	pid, err := c.AllocPacketID(ctx)
	if err != nil {
		return false, err
	}
	ct, err := c.eng.pool.Exec(ctx, `
		UPDATE deliveries SET packet_id=$1, state=1
		 WHERE id=$2 AND state=0
	`, int(pid), deliveryID)
	dur := time.Since(start)
	c.eng.metrics.ObserveDeliveryStage("alloc", dur)
	c.eng.logSlowStage("delivery", "alloc", dur, "client", c.clientID, "delivery_id", deliveryID)
	if err != nil {
		return false, err
	}
	if ct.RowsAffected() == 0 {
		return false, nil
	}
	pk.PacketID = pid
	return true, nil
}

// applyTopicAlias swaps the topic name for an alias when the client
// accepts them: the first PUBLISH for a topic carries both, later ones
// the alias alone.
func (c *Conn) applyTopicAlias(pk *packets.Packet) {
	if c.protocol != mqttwire.ProtocolMQTT5 || c.topicAliasMaximumOut == 0 {
		return
	}
	alias, fresh := c.resolveAliasForOutbound(pk.TopicName)
	if alias == 0 {
		return
	}
	pk.Properties.TopicAlias = alias
	pk.Properties.TopicAliasFlag = true
	if !fresh {
		pk.TopicName = ""
	}
}

// resumeInflight re-sends what a resumed session had in flight when its
// last connection ended: PUBLISH with DUP for messages not yet
// acknowledged, PUBREL for QoS 2 messages past PUBREC [MQTT-4.4.0-1].
// Each takes its Receive Maximum slot again. Runs once, after CONNACK and
// before the sender starts on the queued backlog.
func (c *Conn) resumeInflight(ctx context.Context) error {
	rows, err := c.eng.pool.Query(ctx, `
		SELECT d.id, d.qos, d.state, d.packet_id, d.retained,
		       m.topic, m.payload, m.properties, m.retain, m.expires_at, m.created_at,
		       COALESCE(sm.sub_ids, '{}'::int[]) AS sub_ids,
		       COALESCE(sm.retain_as_published, false) AS retain_as_published
		  FROM deliveries d
		  JOIN messages m ON m.id = d.message_id
		  LEFT JOIN LATERAL (
		    SELECT array_agg(sub.subscription_id ORDER BY sub.subscription_id)
		             FILTER (WHERE sub.subscription_id IS NOT NULL) AS sub_ids,
		           bool_or(sub.retain_as_published) AS retain_as_published
		      FROM subscriptions sub
		     WHERE sub.client_id = d.client_id
		       AND mqtt_topic_match(sub.topic_filter, m.topic)
		  ) sm ON true
		 WHERE d.client_id = $1
		   AND d.state IN (1, 2)
		   AND d.packet_id IS NOT NULL
		 ORDER BY d.id
	`, c.clientID)
	if err != nil {
		return err
	}
	type inflightRow struct {
		queuedDelivery
		state    byte
		packetID int
	}
	batch, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (inflightRow, error) {
		var r inflightRow
		err := row.Scan(&r.id, &r.qos, &r.state, &r.packetID, &r.retained, &r.topic, &r.payload, &r.props,
			&r.retain, &r.expiresAt, &r.createdAt, &r.subIDs, &r.retainAsPublished)
		return r, err
	})
	if err != nil {
		return err
	}
	if len(batch) > 0 {
		c.eng.logger.Debug("resuming in-flight deliveries", "client", c.clientID, "count", len(batch))
	}
	for _, r := range batch {
		if r.state == 2 {
			c.holdInflight()
			if err := c.write(&packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Pubrel, Qos: 1},
				PacketID:    uint16(r.packetID),
			}); err != nil {
				return err
			}
			continue
		}
		// Expiry only stops delivery from starting. This one was sent in
		// time once, and the resend is owed.
		pk, reason := c.buildPublish(r.queuedDelivery, false)
		if pk == nil {
			// Only possible if the client came back with a smaller
			// Maximum Packet Size than this message was sent under.
			if _, err := c.eng.pool.Exec(ctx, `DELETE FROM deliveries WHERE id=$1`, r.id); err != nil {
				return err
			}
			c.eng.metrics.ObserveDeliveryDropped(reason)
			continue
		}
		c.holdInflight()
		pk.FixedHeader.Dup = true
		pk.PacketID = uint16(r.packetID)
		c.applyTopicAlias(pk)
		if err := c.write(pk); err != nil {
			return err
		}
	}
	return nil
}
