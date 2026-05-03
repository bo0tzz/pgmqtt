package engine

import (
	"context"
	"encoding/json"
	"errors"

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
		       m.topic, m.payload, m.properties,
		       COALESCE(
		         (SELECT array_agg(s.subscription_id ORDER BY s.subscription_id)
		            FROM subscriptions s
		           WHERE s.client_id = d.client_id
		             AND s.subscription_id IS NOT NULL
		             AND mqtt_topic_match(s.topic_filter, m.topic)),
		         '{}'::int[]) AS sub_ids
		  FROM deliveries d
		  JOIN sessions s ON s.client_id = d.client_id
		  JOIN messages m ON m.id = d.message_id
		 WHERE d.message_id = $1
		   AND s.broker_id = $2
		   AND d.state IN (0, 1)
	`, messageID, self)
	if err != nil {
		return err
	}
	defer rows.Close()

	type item struct {
		deliveryID int64
		clientID   string
		qos        byte
		state      byte
		packetID   *int
		topic      string
		payload    []byte
		props      []byte
		subIDs     []int
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.deliveryID, &it.clientID, &it.qos, &it.state, &it.packetID,
			&it.topic, &it.payload, &it.props, &it.subIDs); err != nil {
			return err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, it := range items {
		if err := e.deliverOne(ctx, it.deliveryID, it.clientID, it.qos, it.state, it.packetID, it.topic, it.payload, it.props, it.subIDs, false); err != nil {
			e.logger.Warn("deliver", "id", it.deliveryID, "client", it.clientID, "err", err)
		}
	}
	return nil
}

// deliverOne writes a single PUBLISH to a connected client, allocating a
// packet id atomically if needed. dup=true sets the DUP flag (used on session
// resume for state>=1 rows). Errors sending are non-fatal: the client may have
// just disconnected; the delivery row remains and will be drained on reconnect.
func (e *Engine) deliverOne(ctx context.Context, deliveryID int64, clientID string, qos, state byte, currentPacketID *int, topic string, payload, props []byte, subIDs []int, dup bool) error {
	conn, ok := e.ConnFor(clientID)
	if !ok {
		// Not currently connected to this Pod (race with disconnect or
		// migration). Leave the delivery row; it'll be served on reconnect.
		return nil
	}

	pk := &packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos, Dup: dup},
		TopicName:   topic,
		Payload:     payload,
	}
	if len(props) > 0 {
		var p packets.Properties
		if err := json.Unmarshal(props, &p); err == nil {
			pk.Properties = p
		}
	}
	if len(subIDs) > 0 && conn.protocol == mqttwire.ProtocolMQTT5 {
		pk.Properties.SubscriptionIdentifier = append([]int(nil), subIDs...)
	}

	if qos > 0 {
		var pid int
		if currentPacketID != nil && state >= 1 {
			pid = *currentPacketID
		} else {
			tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{})
			if err != nil {
				return err
			}
			row := tx.QueryRow(ctx, `SELECT mqtt_next_packet_id($1)`, clientID)
			if err := row.Scan(&pid); err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
			if _, err := tx.Exec(ctx, `
				UPDATE deliveries SET packet_id=$1, state=1
				 WHERE id=$2
			`, pid, deliveryID); err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				return err
			}
		}
		pk.PacketID = uint16(pid)
	}

	return conn.write(pk)
}

// drainSessionQueue sends queued / inflight deliveries on reconnect. Rows in
// state 0 (queued) become PUBLISH; state 1 (already-sent) become PUBLISH+DUP;
// state 2 (PUBREC received, awaiting PUBCOMP) become PUBREL.
func (c *Conn) drainSessionQueue(ctx context.Context) error {
	rows, err := c.eng.pool.Query(ctx, `
		SELECT d.id, d.qos, d.state, d.packet_id,
		       m.topic, m.payload, m.properties,
		       COALESCE(
		         (SELECT array_agg(s.subscription_id ORDER BY s.subscription_id)
		            FROM subscriptions s
		           WHERE s.client_id = d.client_id
		             AND s.subscription_id IS NOT NULL
		             AND mqtt_topic_match(s.topic_filter, m.topic)),
		         '{}'::int[]) AS sub_ids
		  FROM deliveries d
		  JOIN messages m ON m.id = d.message_id
		 WHERE d.client_id = $1
		   AND d.state IN (0, 1, 2)
		 ORDER BY d.id
	`, c.clientID)
	if err != nil {
		return err
	}
	defer rows.Close()

	type item struct {
		deliveryID int64
		qos        byte
		state      byte
		packetID   *int
		topic      string
		payload    []byte
		props      []byte
		subIDs     []int
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.deliveryID, &it.qos, &it.state, &it.packetID, &it.topic, &it.payload, &it.props, &it.subIDs); err != nil {
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
			if err := c.eng.deliverOne(ctx, it.deliveryID, c.clientID, it.qos, it.state, it.packetID, it.topic, it.payload, it.props, it.subIDs, dup); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ = errors.New
