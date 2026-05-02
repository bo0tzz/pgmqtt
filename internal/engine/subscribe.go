package engine

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/mochi-mqtt/server/v2/packets"

	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

// SUBACK reason codes.
const (
	subackQoS0       byte = 0x00
	subackQoS1       byte = 0x01
	subackQoS2       byte = 0x02
	subackUnspec     byte = 0x80
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
		if _, err := tx.Exec(ctx, `
			INSERT INTO subscriptions
			    (client_id, topic_filter, qos, no_local, retain_as_published, retain_handling, subscription_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (client_id, topic_filter) DO UPDATE SET
			    qos=EXCLUDED.qos,
			    no_local=EXCLUDED.no_local,
			    retain_as_published=EXCLUDED.retain_as_published,
			    retain_handling=EXCLUDED.retain_handling,
			    subscription_id=EXCLUDED.subscription_id
		`, c.clientID, filter, f.Qos, f.NoLocal, f.RetainAsPublished, f.RetainHandling, nullInt(opts.SubscriptionID)); err != nil {
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
		// Retain handling: 0 = send retained on subscribe; 1 = send only on new sub; 2 = never.
		// Without per-row "was this sub new?" bookkeeping in this v1 we treat
		// 0 and 1 the same and skip retained on 2.
		if f.RetainHandling != 2 {
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

	for _, d := range dispatches {
		if err := c.dispatchRetainedForFilter(ctx, d.filter, d.opts); err != nil {
			c.eng.logger.Warn("retained dispatch", "client", c.clientID, "filter", d.filter, "err", err)
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
func (c *Conn) allocPacketID(ctx context.Context, msgID int64, _ byte) (uint16, error) {
	var pid int
	err := c.eng.pool.QueryRow(ctx, `
		WITH chosen AS (
			SELECT mqtt_next_packet_id($1) AS pid
		)
		UPDATE deliveries SET packet_id = (SELECT pid FROM chosen), state = 1
		 WHERE client_id=$1 AND message_id=$2 AND state=0
		RETURNING packet_id
	`, c.clientID, msgID).Scan(&pid)
	if err != nil {
		return 0, err
	}
	return uint16(pid), nil
}

func nullInt(v int) any {
	if v == 0 {
		return nil
	}
	return v
}

// Compile-time guard.
var _ = errors.New
