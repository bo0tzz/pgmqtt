package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mochi-mqtt/server/v2/packets"

	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

const (
	connectTimeout       = 10 * time.Second
	defaultKeepalive     = 60 * time.Second
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

	keepalive    time.Duration
	cleanStart   bool
	willTopic    string
	willPayload  []byte
	willQoS      byte
	willRetain   bool
	willProps    []byte // jsonb-serialised v5 will properties

	closing atomic.Bool
	closed  chan struct{}
	once    sync.Once
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
			c.eng.logger.Warn("dispatch", "client", c.clientID, "type", pk.FixedHeader.Type, "err", err)
			c.handleDisconnect(ctx, err)
			return
		}
	}
}

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
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.nc.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return mqttwire.Write(c.nc, pk)
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

// handleDisconnect runs the ungraceful path when the read loop fails for any
// reason: socket error, deadline, decode failure. Will (if armed) is fired
// here. Sessions row is updated to disconnected; ownership is *not* cleared
// for clean-start=false clients (so subscriptions/queued deliveries persist).
func (c *Conn) handleDisconnect(ctx context.Context, cause error) {
	if c.closing.Load() {
		return
	}
	if c.clientID == "" {
		return
	}
	logArgs := []any{"client", c.clientID, "cause", cause}
	if errors.Is(cause, io.EOF) {
		logArgs = append(logArgs, "kind", "eof")
	} else if errors.Is(cause, net.ErrClosed) {
		logArgs = append(logArgs, "kind", "closed")
	}
	c.eng.logger.Info("client disconnect (ungraceful)", logArgs...)

	// Detach from session BEFORE firing will so the will publish can re-acquire
	// the row's broker_id when a future client reconnects.
	if c.willTopic != "" {
		if err := c.fireWill(ctx); err != nil {
			c.eng.logger.Warn("fire will", "err", err)
		}
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if c.cleanStart {
		_, err := c.eng.pool.Exec(bgCtx, `DELETE FROM sessions WHERE client_id=$1`, c.clientID)
		if err != nil {
			c.eng.logger.Warn("delete clean session", "client", c.clientID, "err", err)
		}
	} else {
		_, err := c.eng.pool.Exec(bgCtx,
			`UPDATE sessions SET connected=false, broker_id=NULL, last_seen=now()
			 WHERE client_id=$1`, c.clientID)
		if err != nil {
			c.eng.logger.Warn("mark disconnected", "client", c.clientID, "err", err)
		}
	}
}

// handleGracefulDisconnect implements MQTT-3.14 (v3.1.1) and v5 normal
// disconnect: the will is dropped (must not be sent), the session may persist
// or be cleaned per cleanStart. v5 SessionExpiryInterval extension is ignored
// here for v1 — we treat any non-zero as "keep until evicted".
func (c *Conn) handleGracefulDisconnect(ctx context.Context, _ *packets.Packet) error {
	c.willTopic = "" // [MQTT-3.14.4-3]
	c.handleDisconnect(ctx, errors.New("client requested disconnect"))
	return io.EOF
}

// handlePuback / handlePubrec / handlePubrel / handlePubcomp — receiver-side
// acknowledgement of QoS>0 deliveries this Pod sent.

func (c *Conn) handlePuback(ctx context.Context, pk *packets.Packet) error {
	_, err := c.eng.pool.Exec(ctx,
		`DELETE FROM deliveries WHERE client_id=$1 AND packet_id=$2 AND qos=1`,
		c.clientID, pk.PacketID)
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

// handlePubrel processes a publisher-side QoS-2 acknowledgement: the publisher
// sent PUBLISH (state=publisher unfinished), got back PUBREC, sent PUBREL.
// We respond with PUBCOMP. (We don't currently track inbound QoS-2 dedup —
// noted as a gap in v1.)
func (c *Conn) handlePubrel(_ context.Context, pk *packets.Packet) error {
	return c.write(&packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pubcomp},
		PacketID:    pk.PacketID,
	})
}

func (c *Conn) handlePubcomp(ctx context.Context, pk *packets.Packet) error {
	_, err := c.eng.pool.Exec(ctx,
		`DELETE FROM deliveries WHERE client_id=$1 AND packet_id=$2 AND qos=2`,
		c.clientID, pk.PacketID)
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
