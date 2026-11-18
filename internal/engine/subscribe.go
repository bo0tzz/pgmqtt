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

// SUBACK reason codes.
const (
	subackQoS0               byte = 0x00
	subackQoS1               byte = 0x01
	subackQoS2               byte = 0x02
	subackUnspec             byte = 0x80
	subackTopicFilterInvalid byte = 0x8F
	subackSharedSubNotSupp   byte = 0x9E
)

func (c *Conn) handleSubscribe(ctx context.Context, pk *packets.Packet) error {
	// [MQTT-3.8.3-2]: SUBSCRIBE MUST contain at least one Topic Filter.
	// A zero-filter SUBSCRIBE previously produced an empty SUBACK, which
	// some clients treat as "all filters accepted" and silently never
	// receive any messages.
	if len(pk.Filters) == 0 {
		_ = c.write(&packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
			ReasonCode:  0x82, // Protocol Error
		})
		return errors.New("SUBSCRIBE with zero filters")
	}

	tx, err := c.eng.beginTxTimed(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	subID := 0
	if c.protocol == mqttwire.ProtocolMQTT5 && len(pk.Properties.SubscriptionIdentifier) > 0 {
		// [MQTT-3.8.2.1.2 / Reason Code 0x91]: a SUBSCRIBE Properties may
		// carry at most one SubscriptionIdentifier, range 1..268435455.
		// Multiple, zero, or out-of-range values are a Protocol Error
		// → DISCONNECT 0x82. Previously we silently took [0] and ignored
		// the rest, which gave non-conforming clients undefined dispatch.
		if len(pk.Properties.SubscriptionIdentifier) > 1 {
			_ = c.write(&packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
				ReasonCode:  0x82, // Protocol Error
			})
			return errors.New("subscribe: multiple SubscriptionIdentifier")
		}
		v := pk.Properties.SubscriptionIdentifier[0]
		if v <= 0 || v > 268435455 {
			_ = c.write(&packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
				ReasonCode:  0x82, // Protocol Error
			})
			return errors.New("subscribe: SubscriptionIdentifier out of range")
		}
		subID = v
	}

	codes := make([]byte, 0, len(pk.Filters))
	type retainedDispatch struct {
		filter string
		opts   subscriptionOpts
	}
	var dispatches []retainedDispatch

	for _, f := range pk.Filters {
		filter := f.Filter
		// $share/{group}/{filter} — we advertise SharedSubAvailable=0 in
		// CONNACK, so per [MQTT-3.8.4-7] / [MQTT-3.9.3] we MUST reject the
		// per-filter SUBSCRIBE with reason 0x9E (Shared Subscriptions not
		// supported). Previously we silently stripped the prefix and
		// subscribed to the underlying filter, which gave clients
		// duplicate-delivery rather than the documented shared semantics.
		if _, _, ok := mqttwire.SharedSubscription(filter); ok {
			codes = append(codes, subackSharedSubNotSupp)
			continue
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
	// [MQTT-3.3.2.3.3]: filter out expired retained rows here, not just at
	// janitor sweep time. The janitor runs every 5s, so without this
	// filter a SUBSCRIBE landing during the window between expiry and
	// sweep would replay an expired retained — and worse, would forward
	// the original MessageExpiryInterval verbatim instead of the
	// remaining budget. SELECT expires_at so we can rewrite MEI on the
	// outgoing PUBLISH.
	rows, err := c.eng.pool.Query(ctx, `
		SELECT topic, payload, qos, properties, expires_at
		  FROM retained
		 WHERE mqtt_topic_match($1, topic)
		   AND (expires_at IS NULL OR expires_at > now())
	`, filter)
	if err != nil {
		return err
	}
	defer rows.Close()

	type retained struct {
		topic     string
		payload   []byte
		qos       int
		props     []byte
		expiresAt *time.Time
	}
	var batch []retained
	for rows.Next() {
		var r retained
		if err := rows.Scan(&r.topic, &r.payload, &r.qos, &r.props, &r.expiresAt); err != nil {
			return err
		}
		batch = append(batch, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	queued := false
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
		// v5: rewrite MessageExpiryInterval to the remaining budget so a
		// subscriber joining N seconds after the retained PUBLISH sees
		// MEI-N, not the original MEI. Mirrors deliver.go:154-160.
		if r.expiresAt != nil && c.protocol == mqttwire.ProtocolMQTT5 {
			remaining := time.Until(*r.expiresAt).Seconds()
			if remaining < 1 {
				remaining = 1
			}
			pk.Properties.MessageExpiryInterval = uint32(remaining)
		}
		if opts.SubscriptionID != 0 && c.protocol == mqttwire.ProtocolMQTT5 {
			pk.Properties.SubscriptionIdentifier = []int{opts.SubscriptionID}
		}
		if effectiveQoS > 0 {
			// Queued like any other QoS>0 delivery, so that it goes out
			// under the client's Receive Maximum and is resent if the
			// connection drops first. The sender picks it up below.
			if err := c.queueRetained(ctx, r.topic, r.payload, effectiveQoS, r.props); err != nil {
				return err
			}
			queued = true
			continue
		}
		if err := c.write(pk); err != nil {
			if errors.Is(err, errPacketTooLarge) {
				// Too big for this client; the rest may still fit.
				c.eng.metrics.ObserveDeliveryDropped("oversized")
				continue
			}
			return err
		}
	}
	if queued {
		c.KickDrain()
	}
	return nil
}

// queueRetained queues one retained message for this client alone: a
// messages row of its own and a deliveries row marked retained, so the
// PUBLISH carries RETAIN=1 [MQTT-3.3.1-8].
func (c *Conn) queueRetained(ctx context.Context, topic string, payload []byte, qos byte, props []byte) error {
	tx, err := c.eng.beginTxTimed(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var msgID int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO messages(topic, payload, qos, retain, properties)
		VALUES ($1, $2, $3, true, $4) RETURNING id
	`, topic, payload, qos, jsonOrNil(props)).Scan(&msgID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO deliveries(client_id, message_id, qos, state, retained)
		VALUES ($1, $2, $3, 0, true)
	`, c.clientID, msgID, qos); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func nullInt(v int) any {
	if v == 0 {
		return nil
	}
	return v
}
