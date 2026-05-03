package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mochi-mqtt/server/v2/packets"

	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

// handlePublish processes an inbound PUBLISH from the client and runs the
// publisher path: optional retain update, message insert, delivery fanout,
// notify peers. PUBACK / PUBREC are responded to per QoS.
func (c *Conn) handlePublish(ctx context.Context, pk *packets.Packet) error {
	startTotal := time.Now()
	defer func() {
		c.eng.metrics.ObservePublishStage("total", time.Since(startTotal))
	}()
	if err := mqttwire.ValidateTopicName(pk.TopicName); err != nil {
		return err
	}
	// v5 inbound flow control: enforce serverReceiveMaximum on un-ACKed QoS>0
	// inbound PUBLISHes. [MQTT-3.3.4-9]. The counter is decremented at the
	// receive-side ACK boundary: PUBACK for QoS 1, PUBCOMP for QoS 2 (which
	// only happens after PUBREL is received). Decrement-after-defer would
	// effectively mean "always 1" so flow control would never trip.
	if pk.FixedHeader.Qos > 0 && c.protocol == mqttwire.ProtocolMQTT5 {
		current := c.inboundInflight.Add(1)
		if uint16(current) > c.eng.serverReceiveMaximum() {
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
		if alias == 0 || alias > c.eng.serverTopicAliasMaximum() {
			_ = c.write(&packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
				ReasonCode:  0x94, // Topic Alias invalid
			})
			return fmt.Errorf("invalid topic alias %d", alias)
		}
	}

	props, err := propsToJSON(pk.Properties)
	if err != nil {
		return err
	}

	core := publishCore{
		Topic:      pk.TopicName,
		Payload:    pk.Payload,
		QoS:        pk.FixedHeader.Qos,
		Retain:     pk.FixedHeader.Retain,
		Properties: props,
		Publisher:  c.clientID,
	}
	if pk.FixedHeader.Qos == 2 {
		core.QoS2DedupKey = &qos2DedupKey{ClientID: c.clientID, PacketID: pk.PacketID}
	}

	res, err := c.eng.publishCore(ctx, core)
	if errors.Is(err, ErrQoS2Duplicate) {
		// QoS-2 retransmit before PUBREL — re-send PUBREC, no fanout.
		// Atomic dedup-and-publish runs in publishCore now; previously
		// the dedup INSERT lived outside the publishCore tx and could
		// silently swallow QoS-2 messages on partial failure.
		return c.write(&packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Pubrec},
			PacketID:    pk.PacketID,
		})
	}
	if err != nil {
		return err
	}
	if c.eng.metrics != nil {
		c.eng.metrics.PublishesTotal.WithLabelValues(strconv.Itoa(int(pk.FixedHeader.Qos))).Inc()
	}
	c.eng.logger.Debug("publish", "client", c.clientID, "topic", pk.TopicName,
		"qos", pk.FixedHeader.Qos, "msg", res.MessageID,
		"brokers", len(res.BrokerIDs), "broker_ids", res.BrokerIDs,
		"overflow", len(res.OverflowClients))

	if err := c.eng.notify.Notify(ctx, res.BrokerIDs, res.MessageID); err != nil {
		c.eng.logger.Warn("post-commit notify hook", "msg", res.MessageID, "err", err)
	}
	if len(res.OverflowClients) > 0 {
		c.eng.dispatchQuotaExceeded(ctx, res.OverflowClients)
	}

	switch pk.FixedHeader.Qos {
	case 0:
		return nil
	case 1:
		// PUBACK closes the inbound flow-control slot for QoS 1.
		startWrite := time.Now()
		err := c.write(&packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Puback},
			PacketID:    pk.PacketID,
		})
		c.eng.metrics.ObservePublishStage("response_write", time.Since(startWrite))
		c.inboundInflight.Add(-1)
		return err
	case 2:
		// PUBREC alone doesn't close the slot — we're still waiting for
		// PUBREL (which triggers PUBCOMP). The slot is released in
		// handlePubrel.
		startWrite := time.Now()
		err := c.write(&packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Pubrec},
			PacketID:    pk.PacketID,
		})
		c.eng.metrics.ObservePublishStage("response_write", time.Since(startWrite))
		return err
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

	// QoS2DedupKey, if non-zero, makes publishCore claim the
	// (client_id, packet_id) pair in inbound_qos2 inside the same tx as
	// the message INSERT. If the row already exists, the tx aborts and
	// publishCore returns ErrQoS2Duplicate so the caller can re-send
	// PUBREC without fanning out again. Atomic dedup-and-publish — the
	// previous "INSERT inbound_qos2 outside the tx, then BEGIN" pattern
	// was a silent QoS-2 message-loss surface (dedup row would persist
	// even if publishCore failed, causing the retry to hit ON CONFLICT
	// and skip fanout).
	QoS2DedupKey *qos2DedupKey
}

// qos2DedupKey identifies the QoS-2 publish for inbound dedup. PacketID
// is unique per client_id, so the pair is unique per in-flight QoS-2
// message.
type qos2DedupKey struct {
	ClientID string
	PacketID uint16
}

// ErrQoS2Duplicate signals that the QoS-2 PUBLISH was a duplicate of an
// already-claimed (client_id, packet_id). Caller should re-send PUBREC
// without invoking the fanout side-effects.
var ErrQoS2Duplicate = errors.New("qos2 inbound duplicate")

// publishResult bundles fanout outputs that publishCore returns to the
// caller. overflowClients are subscribers with QoS>0 deliveries dropped
// because their per-client deliveries depth is at the configured cap; the
// caller dispatches DISCONNECT 0x97 (Quota Exceeded) to each.
type publishResult struct {
	MessageID       int64
	BrokerIDs       []uuid.UUID
	OverflowClients []string
}

// publishCore performs the SQL portion of the publisher path. Retained writes
// runs the retained mutation, the message INSERT, the cross-pod
// pg_notify, and (for retained=true with empty payload) the retained-row
// DELETE all inside a single transaction. The caller is responsible for
// the post-commit Notifier hook used by the in-process test harness.
func (e *Engine) publishCore(ctx context.Context, p publishCore) (publishResult, error) {
	var res publishResult

	startBegin := time.Now()
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{})
	e.metrics.ObservePublishStage("tx_begin", time.Since(startBegin))
	if err != nil {
		return res, err
	}
	defer tx.Rollback(ctx)

	if p.QoS2DedupKey != nil {
		startQ2 := time.Now()
		ct, err := tx.Exec(ctx, `
			INSERT INTO inbound_qos2(client_id, packet_id) VALUES ($1, $2)
			ON CONFLICT DO NOTHING
		`, p.QoS2DedupKey.ClientID, p.QoS2DedupKey.PacketID)
		e.metrics.ObservePublishStage("qos2_dedup", time.Since(startQ2))
		if err != nil {
			return res, err
		}
		if ct.RowsAffected() == 0 {
			return res, ErrQoS2Duplicate
		}
	}

	if p.Retain {
		startRetain := time.Now()
		if len(p.Payload) == 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM retained WHERE topic=$1`, p.Topic); err != nil {
				return res, err
			}
		} else {
			if _, err := tx.Exec(ctx, `
				INSERT INTO retained (topic, payload, qos, properties, expires_at, updated_at)
				VALUES ($1, $2, $3, $4, mqtt_retained_expires_at($4::jsonb), now())
				ON CONFLICT (topic) DO UPDATE SET
					payload=EXCLUDED.payload,
					qos=EXCLUDED.qos,
					properties=EXCLUDED.properties,
					expires_at=EXCLUDED.expires_at,
					updated_at=now()
			`, p.Topic, p.Payload, p.QoS, jsonOrNil(p.Properties)); err != nil {
				return res, err
			}
		}
		e.metrics.ObservePublishStage("retain", time.Since(startRetain))
	}

	var publisher any
	if p.Publisher != "" {
		publisher = p.Publisher
	}

	startQuery := time.Now()
	row := tx.QueryRow(ctx, `
		SELECT msg_id, broker_ids, overflow_clients
		  FROM mqtt_publish($1, $2, $3::smallint, $4, $5::jsonb, $6, $7::int)
	`, p.Topic, p.Payload, p.QoS, p.Retain, jsonOrNil(p.Properties), publisher, e.maxQueuedDeliveries())

	var brokers []uuid.UUID
	var overflow []string
	if err := row.Scan(&res.MessageID, &brokers, &overflow); err != nil {
		return res, err
	}
	e.metrics.ObservePublishStage("mqtt_publish_query", time.Since(startQuery))

	// pg_notify INSIDE the publish tx, not after commit. Postgres queues
	// notifications during the tx and delivers them on COMMIT, so peer
	// notification is atomic with message durability — either both happen
	// or neither does. The post-commit Notifier hook stays in place for
	// the in-process test harness's same-pod Deliver short-circuit; in
	// production it's wired to a no-op since pg_notify is already done.
	if len(brokers) > 0 {
		startNotify := time.Now()
		channels := make([]string, len(brokers))
		for i, id := range brokers {
			channels[i] = "pgmqtt_" + id.String()
		}
		if _, err := tx.Exec(ctx,
			`SELECT pg_notify(c, $2) FROM unnest($1::text[]) AS c`,
			channels, strconv.FormatInt(res.MessageID, 10)); err != nil {
			return res, err
		}
		e.metrics.ObservePublishStage("notify", time.Since(startNotify))
	}

	startCommit := time.Now()
	if err := tx.Commit(ctx); err != nil {
		return res, err
	}
	e.metrics.ObservePublishStage("tx_commit", time.Since(startCommit))
	res.BrokerIDs = brokers
	res.OverflowClients = overflow
	return res, nil
}

// QuotaExceededLocally writes DISCONNECT 0x97 (Quota Exceeded) to the named
// client's currently-attached socket and tears it down. Called by the
// listener when a peer Pod NOTIFYs us that a publish overflowed this client's
// per-conn deliveries cap. Safe to call when the client isn't local — no-op.
func (e *Engine) QuotaExceededLocally(clientID string) {
	conn, ok := e.ConnFor(clientID)
	if !ok {
		return
	}
	e.logger.Info("quota exceeded — disconnecting", "client", clientID)
	if conn.protocol == mqttwire.ProtocolMQTT5 {
		_ = conn.write(&packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
			ReasonCode:  0x97, // Quota Exceeded
		})
	}
	if e.metrics != nil {
		e.metrics.QuotaExceededTotal.Inc()
	}
	conn.Shutdown()
}

// dispatchQuotaExceeded resolves each over-cap client's owning broker via
// the sessions table and emits the appropriate signal — local Disconnect
// for our own clients, NOTIFY pgmqtt_quota_<broker_id> for peers.
func (e *Engine) dispatchQuotaExceeded(ctx context.Context, clientIDs []string) {
	if len(clientIDs) == 0 {
		return
	}
	rows, err := e.pool.Query(ctx, `
		SELECT client_id, broker_id
		  FROM sessions
		 WHERE client_id = ANY($1) AND connected = true AND broker_id IS NOT NULL
	`, clientIDs)
	if err != nil {
		e.logger.Warn("quota lookup", "err", err)
		return
	}
	defer rows.Close()

	self := e.BrokerID()
	for rows.Next() {
		var cid string
		var bid uuid.UUID
		if err := rows.Scan(&cid, &bid); err != nil {
			e.logger.Warn("quota scan", "err", err)
			continue
		}
		if bid == self {
			e.QuotaExceededLocally(cid)
			continue
		}
		if err := e.quota.NotifyQuota(ctx, bid, cid); err != nil {
			e.logger.Warn("quota notify", "broker", bid, "client", cid, "err", err)
		}
	}
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
