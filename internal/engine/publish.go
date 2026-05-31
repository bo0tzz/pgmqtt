package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mochi-mqtt/server/v2/packets"

	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

// publishTimeout bounds the publish path against a wedged Postgres. The
// inbound read loop's ctx has no deadline; without this wrap, a stalled
// pgxpool.Acquire or in-flight statement would hang the conn until TCP
// keepalive killed it minutes later. 5 s is conservative — fast enough
// to surface DISCONNECT 0x88 to the publisher promptly, slow enough that
// a transiently-busy PG (autovacuum, brief failover bounce) doesn't trip
// it on healthy traffic.
const publishTimeout = 5 * time.Second

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
	// Note on $-prefix topics: [MQTT-4.7.2-1] says the server SHOULD
	// prevent clients from publishing to "$..." topics, but it's a
	// SHOULD not a MUST. Our broker doesn't use $SYS/* internally so
	// there's nothing to protect; allowing the publish keeps Paho's
	// test_dollar_topics conformance and matches mosquitto's default.
	// Wildcard subscriptions still don't match $-topics — that part is
	// a MUST and is enforced in mqtt_topic_match (migration 0001).
	// v5 inbound flow control: enforce serverReceiveMaximum on un-ACKed QoS>0
	// inbound PUBLISHes. [MQTT-3.3.4-9]. The counter is decremented at the
	// receive-side ACK boundary: PUBACK for QoS 1, PUBCOMP for QoS 2 (which
	// only happens after PUBREL is received). Decrement-after-defer would
	// effectively mean "always 1" so flow control would never trip.
	// Track whether the inbound-flow-control slot stays held until the
	// peer ACKs (QoS-1 PUBACK / QoS-2 PUBREL). Default: release on early
	// return so dup-PUBLISH and generic errors don't leak slots and
	// eventually trip "Receive Maximum exceeded" against a healthy
	// client. Set this to true only on the success paths where the
	// matching ACK handler is responsible for the decrement.
	releaseInbound := false
	if pk.FixedHeader.Qos > 0 && c.protocol == mqttwire.ProtocolMQTT5 {
		// [MQTT-3.3.4-9]: a QoS-2 retransmit (DUP=1) of an already-
		// claimed (client_id, packet_id) MUST NOT double-count against
		// ReceiveMaximum — the original slot is still held against the
		// in-flight QoS-2 message awaiting PUBREL. Probe inbound_qos2
		// before the unconditional Add(1); if the dedup row exists,
		// skip the increment and let publishCore return ErrQoS2Duplicate
		// (which re-sends PUBREC without fanout).
		skipInc := false
		if pk.FixedHeader.Qos == 2 && pk.FixedHeader.Dup {
			var exists bool
			if err := c.eng.pool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM inbound_qos2
					 WHERE client_id=$1 AND packet_id=$2
				)`, c.clientID, pk.PacketID).Scan(&exists); err == nil && exists {
				skipInc = true
			}
		}
		if !skipInc {
			current := c.inboundInflight.Add(1)
			releaseInbound = true
			if uint16(current) > c.eng.serverReceiveMaximum() {
				_ = c.write(&packets.Packet{
					FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
					ReasonCode:  0x93, // Receive Maximum exceeded
				})
				c.inboundInflight.Add(-1)
				return fmt.Errorf("receive maximum exceeded: %d", current)
			}
			defer func() {
				if releaseInbound {
					c.inboundInflight.Add(-1)
				}
			}()
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

	pubCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	res, err := c.eng.publishCore(pubCtx, core)
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
		// PG-down handling: surface a v5 DISCONNECT reason so the
		// publisher knows why we're tearing the conn down. Without
		// this, the client just sees a closed socket and reconnects
		// blindly — which contributes to the reconnect storm against
		// an already-ailing Postgres.
		c.disconnectForPublishError(err)
		return err
	}
	if c.eng.metrics != nil {
		c.eng.metrics.PublishesTotal.WithLabelValues(strconv.Itoa(int(pk.FixedHeader.Qos))).Inc()
	}
	if c.eng.logger.Enabled(ctx, slog.LevelDebug) {
		c.eng.logger.Debug("publish", "client", c.clientID, "topic", pk.TopicName,
			"qos", pk.FixedHeader.Qos, "msg", res.MessageID,
			"brokers", len(res.BrokerIDs), "broker_ids", res.BrokerIDs,
			"overflow", len(res.OverflowClients))
	}

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
		// PUBACK closes the inbound flow-control slot for QoS 1 — the
		// deferred release above handles the decrement, so don't keep
		// the slot held past this point.
		startWrite := time.Now()
		err := c.write(&packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Puback},
			PacketID:    pk.PacketID,
		})
		c.eng.metrics.ObservePublishStage("response_write", time.Since(startWrite))
		return err
	case 2:
		// PUBREC alone doesn't close the slot — we're still waiting for
		// PUBREL. handlePubrel does the decrement; suppress the deferred
		// release here.
		releaseInbound = false
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

// disconnectForPublishError emits a v5 DISCONNECT with a reason code
// that classifies the underlying Postgres failure, so a publisher can
// distinguish "broker unavailable" from "broker hung up randomly".
// Best-effort — write deadline is bounded by Conn.write's own logic.
// v3.1.1 has no DISCONNECT-with-reason from server; we just close.
func (c *Conn) disconnectForPublishError(err error) {
	if c.protocol != mqttwire.ProtocolMQTT5 {
		return
	}
	reason := byte(0x88) // Server unavailable
	if errors.Is(err, context.DeadlineExceeded) {
		reason = 0x88
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "53100": // disk_full
			reason = 0x99 // Payload format invalid → no, use 0x97? Actually 0x99
			// MQTT-5 0x97 Quota exceeded fits "no room"; 0x99 is Payload format invalid.
			// 0x9C Use another server is wrong. 0x88 Server unavailable best-fits
			// disk-full from the publisher's POV.
			reason = 0x88
		case "53200", "53300": // out_of_memory / too_many_connections
			reason = 0x97 // Quota exceeded
		case "57P01", "57P02", "57P03": // admin_shutdown / crash_shutdown / cannot_connect_now
			reason = 0x88
		}
	}
	_ = c.write(&packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
		ReasonCode:  reason,
	})
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
	tx, err := e.beginTxTimed(ctx, pgx.TxOptions{})
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
		SELECT msg_id, broker_ids, overflow_clients, delivered_count
		  FROM mqtt_publish($1, $2, $3::smallint, $4, $5::jsonb, $6, $7::int)
	`, p.Topic, p.Payload, p.QoS, p.Retain, jsonOrNil(p.Properties), publisher, e.maxQueuedDeliveries())

	var brokers []uuid.UUID
	var overflow []string
	var delivered int64
	if err := row.Scan(&res.MessageID, &brokers, &overflow, &delivered); err != nil {
		return res, err
	}
	e.metrics.ObservePublishStage("mqtt_publish_query", time.Since(startQuery))
	e.metrics.ObservePublishFanout(delivered)

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
	// Symmetric drop signal: count each over-cap subscriber as a dropped
	// delivery with reason="overflow". The QoS-0 "row deleted after
	// successful wire send" path already feeds DeliveriesDroppedTotal
	// via expired/oversized/write_error; this is the QoS≥1 analog for
	// the silent-skip-INSERT branch in mqtt_publish (slow-sub quota
	// trip). Without this counter the "why is sub X missing messages"
	// answer for over-cap drops requires log scraping. Each tripped
	// subscriber is also DISCONNECTed with 0x97 (Quota Exceeded), so
	// this counter trends with QuotaExceededTotal but at message
	// granularity rather than per-trip.
	if e.metrics != nil && len(overflow) > 0 {
		for range overflow {
			e.metrics.ObserveDeliveryDropped("overflow")
		}
	}
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
	// Two goroutines (local quota trip + cross-pod NOTIFY arriving in
	// the same window) can both pass the ConnFor check before either
	// disconnects the socket; quotaOnce gates the side-effects so the
	// metric counts the event once and the log/DISCONNECT-write happens
	// at most once per conn.
	conn.quotaOnce.Do(func() {
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
	})
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
	if err := rows.Err(); err != nil {
		e.logger.Warn("quota iterate", "err", err)
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
