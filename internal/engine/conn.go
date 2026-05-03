package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mochi-mqtt/server/v2/packets"

	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

const (
	connectTimeout        = 10 * time.Second
	defaultKeepalive      = 60 * time.Second
	maxConcurrentInflight = 1024 // upper bound on per-conn outgoing inflight messages
)

// Conn is a single client connection.
type Conn struct {
	eng    *Engine
	nc     net.Conn
	reader *mqttwire.Reader

	clientID string
	protocol byte

	writeMu sync.Mutex

	keepalive       time.Duration
	cleanStart      bool
	willTopic       string
	willPayload     []byte
	willQoS         byte
	willRetain      bool
	willProps       []byte // jsonb-serialised v5 will properties
	willDelay       *int32 // v5 WillDelayInterval (seconds); nil for v3.1.1
	sessionExpiry   *int32 // v5 SessionExpiryInterval (seconds); nil = "no value sent"
	maxPacketSize   uint32 // v5 MaximumPacketSize the client will accept; 0 = no limit
	receiveMaximum  uint16 // v5 ReceiveMaximum the client will accept; 65535 if unset
	inflight        chan struct{} // token-bucket; capacity = receiveMaximum
	drainKick       chan struct{} // single-slot signal to wake the drain loop

	inboundInflight atomic.Int32 // QoS>0 PUBLISHes received, pre-PUBACK/PUBCOMP

	// v5 topic alias maps. Outbound (server→client) aliases are allocated per
	// connection up to the client-advertised topicAliasMaximumOut. Inbound
	// (client→server) aliases are *not* accepted: serverTopicAliasMaximum=0,
	// so the broker advertises 0 in CONNACK and rejects any inbound alias>0.
	topicAliasMaximumOut uint16
	aliasOutMu           sync.Mutex
	aliasOut             map[string]uint16 // topic -> alias
	aliasOutNext         uint16

	closing           atomic.Bool
	gracefulRequested atomic.Bool
	closed            chan struct{}
	once              sync.Once
}

func newConn(e *Engine, nc net.Conn) *Conn {
	return &Conn{
		eng:    e,
		nc:     nc,
		reader: mqttwire.NewReader(nc),
		closed: make(chan struct{}),
	}
}

func (c *Conn) run(ctx context.Context) {
	defer c.shutdown()

	// Wait for CONNECT (must be first).
	if err := c.nc.SetReadDeadline(time.Now().Add(connectTimeout)); err != nil {
		c.eng.logger.Debug("set read deadline", "err", err)
		return
	}
	pk, err := c.reader.Read()
	if err != nil {
		c.eng.logger.Debug("read connect", "err", err)
		return
	}
	if pk.FixedHeader.Type != packets.Connect {
		c.eng.logger.Warn("first packet not CONNECT", "type", pk.FixedHeader.Type)
		return
	}
	if err := c.handleConnect(ctx, &pk); err != nil {
		c.eng.logger.Info("connect rejected", "err", err)
		return
	}

	// Main read loop.
	for {
		if err := c.armReadDeadline(); err != nil {
			return
		}
		pk, err := c.reader.Read()
		if err != nil {
			c.handleDisconnect(ctx, err)
			return
		}
		if err := c.dispatch(ctx, &pk); err != nil {
			// io.EOF / errClientDisconnect signals "this is a normal close" —
			// don't log at warn level. Real dispatch errors stay at warn.
			if !errors.Is(err, io.EOF) && !errors.Is(err, errClientDisconnect) {
				c.eng.logger.Warn("dispatch", "client", c.clientID, "type", pk.FixedHeader.Type, "err", err)
			}
			c.handleDisconnect(ctx, err)
			return
		}
	}
}

// errClientDisconnect is returned from handleGracefulDisconnect to terminate
// the read loop without being logged as an error.
var errClientDisconnect = errors.New("client disconnected")

func (c *Conn) armReadDeadline() error {
	if c.keepalive == 0 {
		return c.nc.SetReadDeadline(time.Time{})
	}
	return c.nc.SetReadDeadline(time.Now().Add(c.keepalive + c.eng.KeepAliveGrace))
}

func (c *Conn) dispatch(ctx context.Context, pk *packets.Packet) error {
	switch pk.FixedHeader.Type {
	case packets.Publish:
		return c.handlePublish(ctx, pk)
	case packets.Puback:
		return c.handlePuback(ctx, pk)
	case packets.Pubrec:
		return c.handlePubrec(ctx, pk)
	case packets.Pubrel:
		return c.handlePubrel(ctx, pk)
	case packets.Pubcomp:
		return c.handlePubcomp(ctx, pk)
	case packets.Subscribe:
		return c.handleSubscribe(ctx, pk)
	case packets.Unsubscribe:
		return c.handleUnsubscribe(ctx, pk)
	case packets.Pingreq:
		return c.write(&packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pingresp}})
	case packets.Disconnect:
		return c.handleGracefulDisconnect(ctx, pk)
	default:
		return fmt.Errorf("unsupported packet type: %d", pk.FixedHeader.Type)
	}
}

func (c *Conn) write(pk *packets.Packet) error {
	if c.closing.Load() {
		return errors.New("conn closing")
	}
	pk.ProtocolVersion = c.protocol
	if c.maxPacketSize > 0 {
		// Encode once to size-check; mochi's MaxSize gating in WritePacket
		// uses a similar approach.
		buf, err := mqttwire.Encode(pk)
		if err != nil {
			return err
		}
		if uint32(len(buf)) > c.maxPacketSize {
			return errPacketTooLarge
		}
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		if err := c.nc.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			return err
		}
		_, err = c.nc.Write(buf)
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.nc.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return mqttwire.Write(c.nc, pk)
}

// V5 server policy. The broker advertises these in CONNACK and enforces them
// during the session. Driven by config (see PGMQTT_RECEIVE_MAXIMUM /
// PGMQTT_TOPIC_ALIAS_MAXIMUM / PGMQTT_KEEPALIVE_MAX_SEC). The defaults here
// match the historical hardcoded values used pre-config plumbing.

func (e *Engine) serverReceiveMaximum() uint16 {
	if e.cfg != nil && e.cfg.V5ReceiveMaximum > 0 {
		return e.cfg.V5ReceiveMaximum
	}
	return 100
}

// serverTopicAliasMaximum: 0 means we don't accept client-side aliases —
// PUBLISH with TopicAlias>0 → DISCONNECT 0x94. Outbound aliases (server→
// client) are supported when the client advertises TopicAliasMaximum>0.
func (e *Engine) serverTopicAliasMaximum() uint16 {
	if e.cfg != nil {
		return e.cfg.V5TopicAliasMaximum
	}
	return 0
}

func (e *Engine) maxAllowedKeepalive() time.Duration {
	if e.cfg != nil && e.cfg.V5KeepaliveMax > 0 {
		return e.cfg.V5KeepaliveMax
	}
	return 60 * time.Second
}

// maxQueuedDeliveries is the per-client cap on the deliveries table. 0 means
// no cap (the SQL function treats 0 as unlimited).
func (e *Engine) maxQueuedDeliveries() int {
	if e.cfg != nil {
		return e.cfg.MaxQueuedDeliveriesPerClient
	}
	return 0
}

// SetMaxQueuedDeliveriesForTest overrides the per-client deliveries cap.
// Test-only; production code reads from config.
func (e *Engine) SetMaxQueuedDeliveriesForTest(n int) {
	if e.cfg == nil {
		return
	}
	e.cfg.MaxQueuedDeliveriesPerClient = n
}

// resolveAliasForOutbound returns (alias, isNew). If the client advertised
// TopicAliasMaximum=0, returns (0,false). Otherwise looks up an existing
// alias or allocates a new one when capacity remains.
func (c *Conn) resolveAliasForOutbound(topic string) (alias uint16, fresh bool) {
	if c.topicAliasMaximumOut == 0 || c.aliasOut == nil {
		return 0, false
	}
	c.aliasOutMu.Lock()
	defer c.aliasOutMu.Unlock()
	if a, ok := c.aliasOut[topic]; ok {
		return a, false
	}
	if c.aliasOutNext >= c.topicAliasMaximumOut {
		return 0, false
	}
	c.aliasOutNext++
	c.aliasOut[topic] = c.aliasOutNext
	return c.aliasOutNext, true
}

// errPacketTooLarge is returned by write when the encoded packet would
// exceed the v5 MaximumPacketSize the receiver advertised. Caller should
// drop the message and (for QoS>0) clean up the delivery row.
var errPacketTooLarge = errors.New("encoded packet exceeds receiver's MaximumPacketSize")

// tryAcquireInflight reserves a v5 ReceiveMaximum slot. Returns true if a
// slot was taken; the caller must call returnInflight when the corresponding
// PUBACK/PUBCOMP arrives. v3.1.1 connections always succeed (no flow ctrl).
func (c *Conn) tryAcquireInflight() bool {
	if c.protocol != mqttwire.ProtocolMQTT5 || c.inflight == nil {
		return true
	}
	select {
	case c.inflight <- struct{}{}:
		return true
	default:
		return false
	}
}

// returnInflight releases a flow-control slot and kicks the drain loop.
func (c *Conn) returnInflight() {
	if c.inflight == nil {
		return
	}
	select {
	case <-c.inflight:
	default:
	}
	if c.drainKick != nil {
		select {
		case c.drainKick <- struct{}{}:
		default:
		}
	}
}

// runDrainLoop wakes on drainKick and tries to send any state=0 deliveries
// for this client up to the in-flight cap.
func (c *Conn) runDrainLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closed:
			return
		case <-c.drainKick:
			// Drain deliveries until we hit the cap or run out of queued rows.
			for {
				if c.closing.Load() {
					return
				}
				done, err := c.drainOnce(ctx)
				if err != nil {
					c.eng.logger.Warn("drain", "client", c.clientID, "err", err)
					break
				}
				if done {
					break
				}
			}
		}
	}
}

// drainOnce sends a single queued delivery if a slot is available. Returns
// done=true when there's nothing to send or no slot. The slot accounting is
// done inside deliverOne — drainOnce just selects + dispatches.
func (c *Conn) drainOnce(ctx context.Context) (done bool, err error) {
	row := c.eng.pool.QueryRow(ctx, `
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
		  FROM deliveries d JOIN messages m ON m.id = d.message_id
		 WHERE d.client_id = $1 AND d.state = 0 AND d.qos > 0
		   AND (m.expires_at IS NULL OR m.expires_at > now())
		 ORDER BY d.id LIMIT 1
	`, c.clientID)
	var (
		deliveryID        int64
		qos, state        byte
		packetID          *int
		topic             string
		payload           []byte
		props             []byte
		retain            bool
		expiresAt         *time.Time
		subIDs            []int
		retainAsPublished bool
	)
	if err := row.Scan(&deliveryID, &qos, &state, &packetID, &topic, &payload, &props, &retain, &expiresAt, &subIDs, &retainAsPublished); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return true, nil
		}
		return true, err
	}
	wireRetain := false
	if retainAsPublished {
		wireRetain = retain
	}
	sent, err := c.eng.deliverOneTracked(ctx, deliveryID, c.clientID, qos, state, packetID, topic, payload, props, expiresAt, subIDs, wireRetain, false)
	if err != nil {
		return true, err
	}
	if !sent {
		// No slot available; leave queued, runDrainLoop wakes again on PUBACK.
		return true, nil
	}
	return false, nil
}

func (c *Conn) shutdown() {
	c.once.Do(func() {
		c.closing.Store(true)
		_ = c.nc.Close()
		close(c.closed)
		if c.clientID != "" {
			c.eng.unregisterConnIfSame(c.clientID, c)
		}
	})
}

// Shutdown closes the connection. Safe to call from outside the connection's
// goroutine — used by takeover and at server shutdown.
func (c *Conn) Shutdown() { c.shutdown() }

// ClientID returns the resolved client identifier (empty before CONNECT).
func (c *Conn) ClientID() string { return c.clientID }

// gracefulClose is invoked at server shutdown — sends a v5 Disconnect with
// reason code 0x8b (Server shutting down) when applicable, then closes.
func (c *Conn) gracefulClose() {
	if c.protocol == mqttwire.ProtocolMQTT5 {
		_ = c.write(&packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
			ReasonCode:  0x8B, // Server shutting down
		})
	}
	c.shutdown()
}

// handleDisconnect runs at the end of a connection's life. It implements:
//
//   - Will firing or scheduling. Graceful DISCONNECT (v3 + v5) discards the
//     will. v5 ungraceful with WillDelayInterval > 0 schedules the will to
//     fire after min(WillDelayInterval, SessionExpiryInterval) seconds; the
//     janitor fires it (or skips it if the client reconnects in time).
//   - Session lifecycle. v3.1.1 cleanSession=true deletes the row. v5 with
//     SessionExpiryInterval == 0 deletes; otherwise persists with
//     session_expires_at = now() + SessionExpiryInterval (or NULL = forever
//     when expiry is "0xFFFFFFFF" / never).
func (c *Conn) handleDisconnect(ctx context.Context, cause error) {
	if c.closing.Load() {
		return
	}
	if c.clientID == "" {
		return
	}
	graceful := c.gracefulRequested.Load() || errors.Is(cause, errClientDisconnect)
	if graceful {
		c.eng.logger.Debug("client disconnect (graceful)", "client", c.clientID)
	} else {
		logArgs := []any{"client", c.clientID, "cause", cause}
		if errors.Is(cause, io.EOF) {
			logArgs = append(logArgs, "kind", "eof")
		} else if errors.Is(cause, net.ErrClosed) {
			logArgs = append(logArgs, "kind", "closed")
		}
		c.eng.logger.Info("client disconnect (ungraceful)", logArgs...)
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Compute will-fire timing.
	willFireImmediate := false
	willFireAt := (*time.Time)(nil)
	if c.willTopic != "" {
		switch c.protocol {
		case mqttwire.ProtocolMQTT5:
			delay := c.willDelay
			if expiry := c.sessionExpiry; expiry != nil && (delay == nil || int64(*expiry) < int64(*delay)) {
				delay = expiry
			}
			if delay == nil || *delay == 0 {
				willFireImmediate = true
			} else {
				t := time.Now().Add(time.Duration(*delay) * time.Second)
				willFireAt = &t
			}
		default:
			willFireImmediate = true
		}
	}

	// Decide session lifetime.
	deleteSession := false
	persistExpiresAt := (*time.Time)(nil)
	switch c.protocol {
	case mqttwire.ProtocolMQTT311:
		deleteSession = c.cleanStart
	case mqttwire.ProtocolMQTT5:
		if c.sessionExpiry == nil || *c.sessionExpiry == 0 {
			deleteSession = true
		} else if *c.sessionExpiry != math.MaxInt32 && *c.sessionExpiry != -1 {
			t := time.Now().Add(time.Duration(*c.sessionExpiry) * time.Second)
			persistExpiresAt = &t
		}
	}

	if willFireImmediate && c.willTopic != "" {
		if err := c.fireWill(bgCtx); err != nil {
			c.eng.logger.Warn("fire will", "err", err)
		}
	}

	if deleteSession {
		_, err := c.eng.pool.Exec(bgCtx, `DELETE FROM sessions WHERE client_id=$1`, c.clientID)
		if err != nil {
			c.eng.logger.Warn("delete clean session", "client", c.clientID, "err", err)
		}
		return
	}

	_, err := c.eng.pool.Exec(bgCtx, `
		UPDATE sessions SET
			connected=false,
			broker_id=NULL,
			last_seen=now(),
			will_fire_at=$2,
			session_expires_at=$3
		WHERE client_id=$1`,
		c.clientID, willFireAt, persistExpiresAt)
	if err != nil {
		c.eng.logger.Warn("mark disconnected", "client", c.clientID, "err", err)
	}
}

// handleGracefulDisconnect implements MQTT-3.14 (v3.1.1) and v5 normal
// disconnect: the will is dropped (must not be sent), the session may persist
// or be cleaned per cleanStart. v5 SessionExpiryInterval extension is ignored
// here for v1 — we treat any non-zero as "keep until evicted".
func (c *Conn) handleGracefulDisconnect(_ context.Context, pk *packets.Packet) error {
	c.willTopic = "" // [MQTT-3.14.4-3]
	// v5 [MQTT-3.14.2.2.2]: a DISCONNECT may override SessionExpiryInterval.
	// Per spec, the server treats the packet as invalid if the original
	// CONNECT had SessionExpiryInterval=0 and DISCONNECT supplies non-zero.
	if c.protocol == mqttwire.ProtocolMQTT5 && pk.Properties.SessionExpiryIntervalFlag {
		v := int32(pk.Properties.SessionExpiryInterval)
		invalidIncrease := c.sessionExpiry != nil && *c.sessionExpiry == 0 && v != 0
		if !invalidIncrease {
			c.sessionExpiry = &v
		}
	}
	c.gracefulRequested.Store(true)
	return errClientDisconnect
}

// handlePuback / handlePubrec / handlePubrel / handlePubcomp — receiver-side
// acknowledgement of QoS>0 deliveries this Pod sent.

func (c *Conn) handlePuback(ctx context.Context, pk *packets.Packet) error {
	_, err := c.eng.pool.Exec(ctx,
		`DELETE FROM deliveries WHERE client_id=$1 AND packet_id=$2 AND qos=1`,
		c.clientID, pk.PacketID)
	c.returnInflight()
	return err
}

func (c *Conn) handlePubrec(ctx context.Context, pk *packets.Packet) error {
	if _, err := c.eng.pool.Exec(ctx,
		`UPDATE deliveries SET state=2 WHERE client_id=$1 AND packet_id=$2 AND qos=2 AND state=1`,
		c.clientID, pk.PacketID); err != nil {
		return err
	}
	return c.write(&packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pubrel, Qos: 1},
		PacketID:    pk.PacketID,
	})
}

// handlePubrel completes the QoS-2 publisher-side handshake. The matching
// row in inbound_qos2 is removed so the same packet_id may be reused, and
// the v5 inbound-flow-control slot is released.
func (c *Conn) handlePubrel(ctx context.Context, pk *packets.Packet) error {
	_, _ = c.eng.pool.Exec(ctx,
		`DELETE FROM inbound_qos2 WHERE client_id=$1 AND packet_id=$2`,
		c.clientID, pk.PacketID)
	err := c.write(&packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pubcomp},
		PacketID:    pk.PacketID,
	})
	c.inboundInflight.Add(-1)
	return err
}

func (c *Conn) handlePubcomp(ctx context.Context, pk *packets.Packet) error {
	_, err := c.eng.pool.Exec(ctx,
		`DELETE FROM deliveries WHERE client_id=$1 AND packet_id=$2 AND qos=2`,
		c.clientID, pk.PacketID)
	c.returnInflight()
	return err
}

func (c *Conn) handleUnsubscribe(ctx context.Context, pk *packets.Packet) error {
	tx, err := c.eng.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	codes := make([]byte, 0, len(pk.Filters))
	for _, f := range pk.Filters {
		ct, err := tx.Exec(ctx,
			`DELETE FROM subscriptions WHERE client_id=$1 AND topic_filter=$2`,
			c.clientID, f.Filter)
		if err != nil {
			return err
		}
		if ct.RowsAffected() > 0 {
			codes = append(codes, 0x00) // Success
		} else {
			codes = append(codes, 0x11) // No subscription existed (v5)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	resp := &packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Unsuback},
		PacketID:    pk.PacketID,
		ReasonCodes: codes,
	}
	return c.write(resp)
}

// jsonOrNil returns nil for an empty Properties to avoid storing empty JSONB.
func propsToJSON(p packets.Properties) ([]byte, error) {
	if isEmptyProps(p) {
		return nil, nil
	}
	return json.Marshal(p)
}

func isEmptyProps(p packets.Properties) bool {
	// Quick check: marshal+compare is wasteful but adequate for our purposes
	// since this only fires on packets that *might* carry properties. The
	// individual struct fields are too numerous to enumerate.
	b, err := json.Marshal(p)
	if err != nil {
		return false
	}
	return string(b) == `{}` || string(b) == "null"
}
