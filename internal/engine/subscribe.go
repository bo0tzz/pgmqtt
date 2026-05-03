package engine

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/mochi-mqtt/server/v2/packets"

	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

// SUBACK reason codes.
const (
	subackQoS0               byte = 0x00
	subackQoS1               byte = 0x01
	subackQoS2               byte = 0x02
	subackUnspec             byte = 0x80
	subackTopicFilterInvalid byte = 0x8F
)

func (c *Conn) handleSubscribe(ctx context.Context, pk *packets.Packet) error {
	tx, err := c.eng.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	subID := 0
	if c.protocol == mqttwire.ProtocolMQTT5 && len(pk.Properties.SubscriptionIdentifier) > 0 {
		subID = pk.Properties.SubscriptionIdentifier[0]
	}

	codes := make([]byte, 0, len(pk.Filters))
	type retainedDispatch struct {
		filter string
		opts   subscriptionOpts
	}
	var dispatches []retainedDispatch

	for _, f := range pk.Filters {
		filter := f.Filter
		// $share/{group}/{filter} — accept the filter; share semantics not yet
		// implemented (best-effort: subscribe to the underlying filter).
		if _, real, ok := mqttwire.SharedSubscription(filter); ok {
			filter = real
		}
		if err := mqttwire.ValidateTopicFilter(filter); err != nil {
			codes = append(codes, subackTopicFilterInvalid)
			continue
		}
		opts := subscriptionOpts{
			QoS:               f.Qos,
			NoLocal:           f.NoLocal,
			RetainAsPublished: f.RetainAsPublished,
			RetainHandling:    f.RetainHandling,
			SubscriptionID:    subID,
		}
		// xmax=0 marks an INSERT; non-zero marks an UPDATE. We need this for
		// retain handling option 1 ("send retained only on new subscription").
		var subWasNew bool
		if err := tx.QueryRow(ctx, `
			INSERT INTO subscriptions
			    (client_id, topic_filter, qos, no_local, retain_as_published, retain_handling, subscription_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (client_id, topic_filter) DO UPDATE SET
			    qos=EXCLUDED.qos,
			    no_local=EXCLUDED.no_local,
			    retain_as_published=EXCLUDED.retain_as_published,
			    retain_handling=EXCLUDED.retain_handling,
			    subscription_id=EXCLUDED.subscription_id
			RETURNING (xmax = 0) AS new_row
		`, c.clientID, filter, f.Qos, f.NoLocal, f.RetainAsPublished, f.RetainHandling, nullInt(opts.SubscriptionID)).Scan(&subWasNew); err != nil {
			return err
		}
		switch f.Qos {
		case 0:
			codes = append(codes, subackQoS0)
		case 1:
			codes = append(codes, subackQoS1)
		case 2:
			codes = append(codes, subackQoS2)
		default:
			codes = append(codes, subackUnspec)
		}
		// Retain handling:
		//   0 = send retained on subscribe (always)
		//   1 = send retained only on a new subscription
		//   2 = never send retained on subscribe
		shouldDispatchRetained := false
		switch f.RetainHandling {
		case 0:
			shouldDispatchRetained = true
		case 1:
			shouldDispatchRetained = subWasNew
		case 2:
			shouldDispatchRetained = false
		}
		if shouldDispatchRetained {
			dispatches = append(dispatches, retainedDispatch{filter: filter, opts: opts})
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	resp := &packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Suback},
		PacketID:    pk.PacketID,
		ReasonCodes: codes,
	}
	if err := c.write(resp); err != nil {
		return err
	}
	if c.eng.metrics != nil {
		c.eng.metrics.SubscribesTotal.Inc()
	}

	for _, d := range dispatches {
		if err := c.dispatchRetainedForFilter(ctx, d.filter, d.opts); err != nil {
			// SUBACK already left the broker; the client cannot distinguish
			// "no retained existed" from "retained existed but failed to
			// deliver." Surface the counter so an operator can alert when
			// retained-replay is silently broken for a subscriber.
			c.eng.logger.Error("retained dispatch failed (post-SUBACK)",
				"client", c.clientID, "filter", d.filter, "err", err)
			if c.eng.metrics != nil {
				c.eng.metrics.RetainedDispatchFailedTotal.Inc()
			}
		}
	}
	return nil
}

type subscriptionOpts struct {
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
	SubscriptionID    int
}

// dispatchRetainedForFilter sends every retained message matching filter to
// this Conn as a PUBLISH (with retain=1 unless RetainAsPublished overrides it
// — but only published-not-as-retained logic applies for fresh sends; for
// retained-on-subscribe we always set retain=1 per [MQTT-3.3.1-8]).
func (c *Conn) dispatchRetainedForFilter(ctx context.Context, filter string, opts subscriptionOpts) error {
	rows, err := c.eng.pool.Query(ctx, `
		SELECT topic, payload, qos, properties
		  FROM retained
		 WHERE mqtt_topic_match($1, topic)
	`, filter)
	if err != nil {
		return err
	}
	defer rows.Close()

	type retained struct {
		topic   string
		payload []byte
		qos     int
		props   []byte
	}
	var batch []retained
	for rows.Next() {
		var r retained
		if err := rows.Scan(&r.topic, &r.payload, &r.qos, &r.props); err != nil {
			return err
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, r := range batch {
		effectiveQoS := byte(r.qos)
		if opts.QoS < effectiveQoS {
			effectiveQoS = opts.QoS
		}
		pk := &packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Retain: true, Qos: effectiveQoS},
			TopicName:   r.topic,
			Payload:     r.payload,
		}
		if len(r.props) > 0 {
			var p packets.Properties
			if err := json.Unmarshal(r.props, &p); err == nil {
				pk.Properties = p
			}
		}
		if opts.SubscriptionID != 0 && c.protocol == mqttwire.ProtocolMQTT5 {
			pk.Properties.SubscriptionIdentifier = []int{opts.SubscriptionID}
		}
		if effectiveQoS > 0 {
			// Need a packet id and a deliveries row so PUBACK/PUBREC can be tracked.
			msgID, err := c.persistRetainedDispatch(ctx, r.topic, r.payload, effectiveQoS, r.props)
			if err != nil {
				return err
			}
			pid, err := c.allocPacketID(ctx, msgID, effectiveQoS)
			if err != nil {
				return err
			}
			pk.PacketID = pid
		}
		if err := c.write(pk); err != nil {
			return err
		}
	}
	return nil
}

// persistRetainedDispatch creates a messages row + a deliveries row for this
// client so we can properly track QoS>0 retained delivery. We do not insert
// for other clients (this is a single-client retained replay, not a fanout).
func (c *Conn) persistRetainedDispatch(ctx context.Context, topic string, payload []byte, qos byte, props []byte) (int64, error) {
	tx, err := c.eng.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	var msgID int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO messages(topic, payload, qos, retain, properties)
		VALUES ($1, $2, $3, true, $4) RETURNING id
	`, topic, payload, qos, jsonOrNil(props)).Scan(&msgID); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO deliveries(client_id, message_id, qos, state)
		VALUES ($1, $2, $3, 0)
	`, c.clientID, msgID, qos); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return msgID, nil
}

// allocPacketID assigns a packet id to a queued delivery (state=0 -> 1).
// Uses the per-Conn in-memory allocator so the per-delivery UPDATE on
// sessions.next_packet_id is gone — the only DB mutation is the
// deliveries row state transition.
func (c *Conn) allocPacketID(ctx context.Context, msgID int64, _ byte) (uint16, error) {
	pid, err := c.AllocPacketID(ctx)
	if err != nil {
		return 0, err
	}
	ct, err := c.eng.pool.Exec(ctx, `
		UPDATE deliveries SET packet_id=$1, state=1
		 WHERE client_id=$2 AND message_id=$3 AND state=0
	`, int(pid), c.clientID, msgID)
	if err != nil {
		return 0, err
	}
	if ct.RowsAffected() == 0 {
		return 0, errors.New("retained delivery row missing")
	}
	return pid, nil
}

func nullInt(v int) any {
	if v == 0 {
		return nil
	}
	return v
}
