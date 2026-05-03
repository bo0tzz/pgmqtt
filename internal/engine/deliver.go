package engine

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
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
	self := e.BrokerID()
	rows, err := e.pool.Query(ctx, `
		SELECT d.id, d.client_id, d.qos, d.state, d.packet_id,
		       m.topic, m.payload, m.properties, m.retain, m.expires_at,
		       COALESCE(
		         (SELECT array_agg(s.subscription_id ORDER BY s.subscription_id)
		            FROM subscriptions s
		           WHERE s.client_id = d.client_id
		             AND s.subscription_id IS NOT NULL
		             AND mqtt_topic_match(s.topic_filter, m.topic)),
		         '{}'::int[]) AS sub_ids,
		       COALESCE(
		         (SELECT bool_or(s.retain_as_published)
		            FROM subscriptions s
		           WHERE s.client_id = d.client_id
		             AND mqtt_topic_match(s.topic_filter, m.topic)),
		         false) AS retain_as_published
		  FROM deliveries d
		  JOIN sessions s ON s.client_id = d.client_id
		  JOIN messages m ON m.id = d.message_id
		 WHERE d.message_id = $1
		   AND s.broker_id = $2
		   AND d.state IN (0, 1)
		   AND (m.expires_at IS NULL OR m.expires_at > now())
	`, messageID, self)
	if err != nil {
		return err
	}
	defer rows.Close()

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
		subIDs             []int
		retainAsPublished  bool
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.deliveryID, &it.clientID, &it.qos, &it.state, &it.packetID,
			&it.topic, &it.payload, &it.props, &it.retain, &it.expiresAt, &it.subIDs, &it.retainAsPublished); err != nil {
			return err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, it := range items {
		retain := false
		if it.retainAsPublished {
			retain = it.retain
		}
		if err := e.deliverOnePub(ctx, it.deliveryID, it.clientID, it.qos, it.state, it.packetID, it.topic, it.payload, it.props, it.expiresAt, it.subIDs, retain, false); err != nil {
			e.logger.Warn("deliver", "id", it.deliveryID, "client", it.clientID, "err", err)
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
		var pid int
		if currentPacketID != nil && state >= 1 {
			pid = *currentPacketID
		} else {
			tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{})
			if err != nil {
				conn.returnInflight()
				return false, err
			}
			row := tx.QueryRow(ctx, `SELECT mqtt_next_packet_id($1)`, clientID)
			if err := row.Scan(&pid); err != nil {
				_ = tx.Rollback(ctx)
				conn.returnInflight()
				return false, err
			}
			if _, err := tx.Exec(ctx, `
				UPDATE deliveries SET packet_id=$1, state=1
				 WHERE id=$2
			`, pid, deliveryID); err != nil {
				_ = tx.Rollback(ctx)
				conn.returnInflight()
				return false, err
			}
			if err := tx.Commit(ctx); err != nil {
				conn.returnInflight()
				return false, err
			}
		}
		pk.PacketID = uint16(pid)
	}

	if err := conn.write(pk); err != nil {
		if qos > 0 {
			conn.returnInflight()
		}
		return false, err
	}
	return true, nil
}

// drainSessionQueue sends queued / inflight deliveries on reconnect. Rows in
// state 0 (queued) become PUBLISH; state 1 (already-sent) become PUBLISH+DUP;
// state 2 (PUBREC received, awaiting PUBCOMP) become PUBREL.
func (c *Conn) drainSessionQueue(ctx context.Context) error {
	rows, err := c.eng.pool.Query(ctx, `
		SELECT d.id, d.qos, d.state, d.packet_id,
		       m.topic, m.payload, m.properties, m.retain, m.expires_at,
		       COALESCE(
		         (SELECT array_agg(s.subscription_id ORDER BY s.subscription_id)
		            FROM subscriptions s
		           WHERE s.client_id = d.client_id
		             AND s.subscription_id IS NOT NULL
		             AND mqtt_topic_match(s.topic_filter, m.topic)),
		         '{}'::int[]) AS sub_ids,
		       COALESCE(
		         (SELECT bool_or(s.retain_as_published)
		            FROM subscriptions s
		           WHERE s.client_id = d.client_id
		             AND mqtt_topic_match(s.topic_filter, m.topic)),
		         false) AS retain_as_published
		  FROM deliveries d
		  JOIN messages m ON m.id = d.message_id
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
