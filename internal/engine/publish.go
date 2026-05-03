package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mochi-mqtt/server/v2/packets"

	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

// handlePublish processes an inbound PUBLISH from the client and runs the
// publisher path: optional retain update, message insert, delivery fanout,
// notify peers. PUBACK / PUBREC are responded to per QoS.
func (c *Conn) handlePublish(ctx context.Context, pk *packets.Packet) error {
	if err := mqttwire.ValidateTopicName(pk.TopicName); err != nil {
		return err
	}
	// v5 inbound flow control: enforce serverReceiveMaximum on un-ACKed QoS>0
	// inbound PUBLISHes. [MQTT-3.3.4-9].
	if pk.FixedHeader.Qos > 0 && c.protocol == mqttwire.ProtocolMQTT5 {
		current := c.inboundInflight.Add(1)
		defer c.inboundInflight.Add(-1)
		if uint16(current) > serverReceiveMaximum {
			_ = c.write(&packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
				ReasonCode:  0x93, // Receive Maximum exceeded
			})
			return fmt.Errorf("receive maximum exceeded: %d", current)
		}
	}

	// v5 inbound TopicAlias validation. We advertise serverTopicAliasMaximum=0
	// so any client-side alias is a protocol violation per [MQTT-3.3.2-12].
	if c.protocol == mqttwire.ProtocolMQTT5 && pk.Properties.TopicAliasFlag {
		alias := pk.Properties.TopicAlias
		if alias == 0 || alias > serverTopicAliasMaximum {
			_ = c.write(&packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
				ReasonCode:  0x94, // Topic Alias invalid
			})
			return fmt.Errorf("invalid topic alias %d", alias)
		}
	}

	// QoS-2 inbound dedup: a duplicate PUBLISH (same packet_id, before
	// PUBREL) must NOT re-fan-out. We claim the (client_id, packet_id) pair
	// in inbound_qos2; ON CONFLICT is the dedup signal.
	if pk.FixedHeader.Qos == 2 {
		ct, err := c.eng.pool.Exec(ctx, `
			INSERT INTO inbound_qos2(client_id, packet_id) VALUES ($1, $2)
			ON CONFLICT DO NOTHING
		`, c.clientID, pk.PacketID)
		if err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			// Duplicate — re-send PUBREC without fanning out again.
			return c.write(&packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Pubrec},
				PacketID:    pk.PacketID,
			})
		}
	}

	props, err := propsToJSON(pk.Properties)
	if err != nil {
		return err
	}

	msgID, brokerIDs, err := c.eng.publishCore(ctx, publishCore{
		Topic:      pk.TopicName,
		Payload:    pk.Payload,
		QoS:        pk.FixedHeader.Qos,
		Retain:     pk.FixedHeader.Retain,
		Properties: props,
		Publisher:  c.clientID,
	})
	if err != nil {
		return err
	}

	if err := c.eng.notify.Notify(ctx, brokerIDs, msgID); err != nil {
		c.eng.logger.Warn("publish notify", "msg", msgID, "err", err)
	}

	switch pk.FixedHeader.Qos {
	case 0:
		return nil
	case 1:
		return c.write(&packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Puback},
			PacketID:    pk.PacketID,
		})
	case 2:
		return c.write(&packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Pubrec},
			PacketID:    pk.PacketID,
		})
	default:
		return errors.New("invalid qos")
	}
}

type publishCore struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Properties []byte // jsonb-encoded mochi Properties (or nil)
	Publisher  string // empty for synthesized (will, retained-fanout, etc.)
}

// publishCore performs the SQL portion of the publisher path. Retained writes
// run before the fanout transaction (so retain updates are durable even if
// nobody currently subscribes). The caller is responsible for emitting NOTIFY.
func (e *Engine) publishCore(ctx context.Context, p publishCore) (msgID int64, brokerIDs []uuid.UUID, err error) {
	if p.Retain {
		if len(p.Payload) == 0 {
			if _, err := e.pool.Exec(ctx, `DELETE FROM retained WHERE topic=$1`, p.Topic); err != nil {
				return 0, nil, err
			}
		} else {
			if _, err := e.pool.Exec(ctx, `
				INSERT INTO retained (topic, payload, qos, properties, expires_at, updated_at)
				VALUES ($1, $2, $3, $4, mqtt_retained_expires_at($4::jsonb), now())
				ON CONFLICT (topic) DO UPDATE SET
					payload=EXCLUDED.payload,
					qos=EXCLUDED.qos,
					properties=EXCLUDED.properties,
					expires_at=EXCLUDED.expires_at,
					updated_at=now()
			`, p.Topic, p.Payload, p.QoS, jsonOrNil(p.Properties)); err != nil {
				return 0, nil, err
			}
		}
	}

	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback(ctx)

	var publisher any
	if p.Publisher != "" {
		publisher = p.Publisher
	}

	row := tx.QueryRow(ctx, `
		SELECT msg_id, broker_ids
		  FROM mqtt_publish($1, $2, $3::smallint, $4, $5::jsonb, $6)
	`, p.Topic, p.Payload, p.QoS, p.Retain, jsonOrNil(p.Properties), publisher)

	var brokers []uuid.UUID
	if err := row.Scan(&msgID, &brokers); err != nil {
		return 0, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, nil, err
	}
	return msgID, brokers, nil
}

func jsonOrNil(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	// Sanity check it's valid JSON (the caller should already produce valid JSON).
	if !json.Valid(b) {
		return nil
	}
	return b
}
