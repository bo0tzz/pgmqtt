package engine

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"

	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

// Deliver is the receiver side of a publish: assign packet ids to all queued
// deliveries for messageID owned by this Pod, look up each client's local
// socket, and send the PUBLISH. Called by the listener's NOTIFY dispatcher.
//
// We collect the matching subscription's subscription_id (if set) so v5
// clients see the SubscriptionIdentifier property on the delivered PUBLISH.
// When multiple subscriptions match for the same client we pick the smallest
// non-null id — sufficient for v5 conformance since we already deduplicate
// to one delivery per client.
func (e *Engine) Deliver(ctx context.Context, messageID int64) error {
	totalStart := time.Now()
	defer func() {
		e.metrics.ObserveDeliveryStage("total", time.Since(totalStart))
	}()
	self := e.BrokerID()
	scanStart := time.Now()
	rows, err := e.pool.Query(ctx, `
		SELECT d.id, d.client_id, d.qos, d.state, d.packet_id,
		       m.topic, m.payload, m.properties, m.retain, m.expires_at, m.created_at,
		       COALESCE(sm.sub_ids, '{}'::int[]) AS sub_ids,
		       COALESCE(sm.retain_as_published, false) AS retain_as_published
		  FROM deliveries d
		  JOIN sessions s ON s.client_id = d.client_id
		  JOIN messages m ON m.id = d.message_id
		  LEFT JOIN LATERAL (
		    SELECT array_agg(sub.subscription_id ORDER BY sub.subscription_id)
		             FILTER (WHERE sub.subscription_id IS NOT NULL) AS sub_ids,
		           bool_or(sub.retain_as_published) AS retain_as_published
		      FROM subscriptions sub
		     WHERE sub.client_id = d.client_id
		       AND mqtt_topic_match(sub.topic_filter, m.topic)
		  ) sm ON true
		 WHERE d.message_id = $1
		   AND s.broker_id = $2
		   AND d.state = 0
		   AND (m.expires_at IS NULL OR m.expires_at > now())
	`, messageID, self)
	if err != nil {
		return err
	}
	defer rows.Close()
	e.metrics.ObserveDeliveryStage("scan", time.Since(scanStart))

	type item struct {
		deliveryID         int64
		clientID           string
		qos                byte
		state              byte
		packetID           *int
		topic              string
		payload            []byte
		props              []byte
		retain             bool
		expiresAt          *time.Time
		createdAt          time.Time
		subIDs             []int
		retainAsPublished  bool
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.deliveryID, &it.clientID, &it.qos, &it.state, &it.packetID,
			&it.topic, &it.payload, &it.props, &it.retain, &it.expiresAt, &it.createdAt, &it.subIDs, &it.retainAsPublished); err != nil {
			return err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if e.logger.Enabled(ctx, slog.LevelDebug) {
		e.logger.Debug("deliver scan", "msg", messageID, "broker", self, "rows", len(items))
	}
	for _, it := range items {
		retain := false
		if it.retainAsPublished {
			retain = it.retain
		}
		// Use the tracked variant so we can observe e2e latency only
		// when the PUBLISH actually went on the wire — sent=false means
		// the client wasn't local or had no flow-control slot, both of
		// which would skew the histogram with the server's wait time.
		sent, err := e.deliverOneTracked(ctx, it.deliveryID, it.clientID, it.qos, it.state, it.packetID, it.topic, it.payload, it.props, it.expiresAt, it.subIDs, retain, false)
		if err != nil {
			e.logger.Warn("deliver", "id", it.deliveryID, "client", it.clientID, "err", err)
			continue
		}
		if sent {
			e.metrics.ObserveE2EPublishToDeliver(time.Since(it.createdAt))
		}
	}
	return nil
}

// deliverOne writes a single PUBLISH to a connected client. Use
// deliverOnePub to set the wire RETAIN flag (subscription retainAsPublished).
func (e *Engine) deliverOne(ctx context.Context, deliveryID int64, clientID string, qos, state byte, currentPacketID *int, topic string, payload, props []byte, expiresAt *time.Time, subIDs []int, dup bool) error {
	_, err := e.deliverOneTracked(ctx, deliveryID, clientID, qos, state, currentPacketID, topic, payload, props, expiresAt, subIDs, false, dup)
	return err
}

func (e *Engine) deliverOnePub(ctx context.Context, deliveryID int64, clientID string, qos, state byte, currentPacketID *int, topic string, payload, props []byte, expiresAt *time.Time, subIDs []int, retain, dup bool) error {
	_, err := e.deliverOneTracked(ctx, deliveryID, clientID, qos, state, currentPacketID, topic, payload, props, expiresAt, subIDs, retain, dup)
	return err
}

// deliverOneTracked is like deliverOne but reports whether a flow-control
// slot was actually consumed (sent=true) vs. whether the delivery was left
// queued because no slot was available (sent=false). Internal helper used by
// the drain loop.
func (e *Engine) deliverOneTracked(ctx context.Context, deliveryID int64, clientID string, qos, state byte, currentPacketID *int, topic string, payload, props []byte, expiresAt *time.Time, subIDs []int, retain, dup bool) (sent bool, err error) {
	conn, ok := e.ConnFor(clientID)
	if !ok {
		// Not currently connected to this Pod (race with disconnect or
		// migration). Leave the delivery row; it'll be served on reconnect.
		return false, nil
	}
	// Drop expired messages — also delete the delivery row so it doesn't
	// re-trigger on reconnect.
	if expiresAt != nil && !expiresAt.After(time.Now()) {
		_, _ = e.pool.Exec(ctx, `DELETE FROM deliveries WHERE id=$1`, deliveryID)
		e.metrics.ObserveDeliveryDropped("expired")
		return false, nil
	}

	pk := &packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos, Dup: dup, Retain: retain},
		TopicName:   topic,
		Payload:     payload,
	}
	if len(props) > 0 {
		var p packets.Properties
		if err := json.Unmarshal(props, &p); err == nil {
			pk.Properties = p
		}
	}
	// v5: decrement the MessageExpiryInterval to the remaining time so the
	// receiver knows what's left of the budget.
	if expiresAt != nil && conn.protocol == mqttwire.ProtocolMQTT5 {
		remaining := time.Until(*expiresAt).Seconds()
		if remaining < 1 {
			remaining = 1
		}
		pk.Properties.MessageExpiryInterval = uint32(remaining)
	}
	if len(subIDs) > 0 && conn.protocol == mqttwire.ProtocolMQTT5 {
		pk.Properties.SubscriptionIdentifier = append([]int(nil), subIDs...)
	}

	// v5 outbound TopicAlias: replace topic name with an alias on subsequent
	// sends to the same client/topic, when the client advertised
	// TopicAliasMaximum > 0. First send carries both topic and alias.
	if conn.protocol == mqttwire.ProtocolMQTT5 && conn.topicAliasMaximumOut > 0 {
		if alias, fresh := conn.resolveAliasForOutbound(topic); alias > 0 {
			pk.Properties.TopicAlias = alias
			pk.Properties.TopicAliasFlag = true
			if !fresh {
				pk.TopicName = ""
			}
		}
	}

	// At this point we may want to assign a packet id and persist state — but
	// if the encoded packet would blow the client's MaximumPacketSize the
	// spec [MQTT-3.1.2-25] says drop it without delivery. We size-check here
	// to avoid mutating delivery state for messages we won't send.
	if conn.maxPacketSize > 0 {
		probe := *pk
		buf, err := mqttwire.Encode(&probe)
		if err == nil && uint32(len(buf)) > conn.maxPacketSize {
			_, _ = e.pool.Exec(ctx, `DELETE FROM deliveries WHERE id=$1`, deliveryID)
			e.metrics.ObserveDeliveryDropped("oversized")
			return false, nil
		}
	}
	// Flow control: take a slot for QoS>0. If none, leave the delivery in
	// state=0 — runDrainLoop will pick it up when a slot opens. (Resumed
	// inflight rows already hold their slot conceptually; we re-acquire
	// here for simplicity and rely on PUBACK to free it.)
	if qos > 0 && state == 0 {
		if !conn.tryAcquireInflight() {
			return false, nil
		}
	}

	if qos > 0 {
		var pid uint16
		if currentPacketID != nil && state >= 1 {
			pid = uint16(*currentPacketID)
		} else {
			// Allocate a packet id from the per-Conn in-memory counter
			// (replaces the per-delivery UPDATE on sessions.next_packet_id).
			// The state=0→1 UPDATE below remains the race-safe claim — if
			// RowsAffected==0 someone else already claimed the delivery,
			// so we release the slot and skip.
			allocStart := time.Now()
			allocated, err := conn.AllocPacketID(ctx)
			if err != nil {
				conn.returnInflight()
				return false, err
			}
			ct, err := e.pool.Exec(ctx, `
				UPDATE deliveries SET packet_id=$1, state=1
				 WHERE id=$2 AND state=0
			`, int(allocated), deliveryID)
			e.metrics.ObserveDeliveryStage("alloc", time.Since(allocStart))
			if err != nil {
				conn.returnInflight()
				return false, err
			}
			if ct.RowsAffected() == 0 {
				conn.returnInflight()
				return false, nil
			}
			pid = allocated
		}
		pk.PacketID = pid
	}

	// Saturation sample at the slot we just acquired (or at delivery time
	// for QoS-0 / resumed-inflight paths). For non-acquiring paths the
	// length-vs-cap ratio still reflects backpressure shape.
	if e.metrics != nil {
		if c := cap(conn.inflight); c > 0 {
			e.metrics.OutboundInflightSaturation.Observe(float64(len(conn.inflight)) / float64(c))
		}
	}

	// QoS-0: atomically claim the row before the wire write so a
	// concurrent caller (e.g. drainSessionQueue racing the NOTIFY-driven
	// Deliver fan-out for the same client) cannot also send the same
	// delivery. The v0.1.9 fix moved the DELETE inside the success path
	// but kept it AFTER conn.write, leaving a window where both callers
	// see the row in state=0 and both write to the wire. DELETE-RETURNING
	// makes the claim the serialisation point: only the caller whose
	// DELETE returned a row proceeds to write.
	if qos == 0 {
		ct, derr := e.pool.Exec(ctx, `DELETE FROM deliveries WHERE id=$1`, deliveryID)
		if derr != nil {
			// Best-effort: the orphan sweep catches the row if the
			// DELETE fails transiently. Don't write — we can't tell
			// whether we'd have won the claim, and a double-send is
			// worse than a missed one for QoS-0 (at-most-once).
			e.logger.Warn("qos-0 delivery claim", "id", deliveryID, "client", clientID, "err", derr)
			return false, nil
		}
		if ct.RowsAffected() == 0 {
			// Sister caller already claimed and is (or has) sending.
			return false, nil
		}
	}

	writeStart := time.Now()
	err = conn.write(pk)
	e.metrics.ObserveDeliveryStage("write", time.Since(writeStart))
	if err != nil {
		if qos > 0 {
			conn.returnInflight()
		}
		e.metrics.ObserveDeliveryDropped("write_error")
		return false, err
	}
	return true, nil
}

// drainSessionQueue sends queued / inflight deliveries on reconnect. Rows in
// state 0 (queued) become PUBLISH; state 1 (already-sent) become PUBLISH+DUP;
// state 2 (PUBREC received, awaiting PUBCOMP) become PUBREL.
//
// Logs the per-state row count at Debug level so chaos-related loss can be
// localized to "did takeover see the rows".
func (c *Conn) drainSessionQueue(ctx context.Context) error {
	rows, err := c.eng.pool.Query(ctx, `
		SELECT d.id, d.qos, d.state, d.packet_id,
		       m.topic, m.payload, m.properties, m.retain, m.expires_at,
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
		   AND d.state IN (0, 1, 2)
		   AND (m.expires_at IS NULL OR m.expires_at > now())
		 ORDER BY d.id
	`, c.clientID)
	if err != nil {
		return err
	}
	defer rows.Close()

	type item struct {
		deliveryID        int64
		qos               byte
		state             byte
		packetID          *int
		topic             string
		payload           []byte
		props             []byte
		retain            bool
		expiresAt         *time.Time
		subIDs            []int
		retainAsPublished bool
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.deliveryID, &it.qos, &it.state, &it.packetID, &it.topic, &it.payload, &it.props, &it.retain, &it.expiresAt, &it.subIDs, &it.retainAsPublished); err != nil {
			return err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(items) > 0 {
		var s0, s1, s2 int
		for _, it := range items {
			switch it.state {
			case 0:
				s0++
			case 1:
				s1++
			case 2:
				s2++
			}
		}
		c.eng.logger.Debug("drain resumed", "client", c.clientID,
			"queued", s0, "inflight", s1, "awaiting_pubcomp", s2)
	}

	for _, it := range items {
		switch it.state {
		case 2:
			if it.packetID == nil {
				continue
			}
			if err := c.write(&packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Pubrel, Qos: 1},
				PacketID:    uint16(*it.packetID),
			}); err != nil {
				return err
			}
		case 0, 1:
			dup := it.state == 1
			retain := false
			if it.retainAsPublished {
				retain = it.retain
			}
			if err := c.eng.deliverOnePub(ctx, it.deliveryID, c.clientID, it.qos, it.state, it.packetID, it.topic, it.payload, it.props, it.expiresAt, it.subIDs, retain, dup); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ = errors.New
