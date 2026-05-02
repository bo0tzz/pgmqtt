package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mochi-mqtt/server/v2/packets"

	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

// MQTT v5 Connack reason codes used here:
//
//	0x00 Success
//	0x80 Unspecified error
//	0x84 Unsupported protocol version
//	0x85 Client identifier not valid
//	0x86 Bad username or password
//	0x87 Not authorized
const (
	cackSuccess             byte = 0x00
	cackUnsupportedProtocol byte = 0x84
	cackBadCredentials      byte = 0x86
	cackNotAuthorized       byte = 0x87
	cackClientIDInvalid     byte = 0x85
	cackUnspecified         byte = 0x80
)

func (c *Conn) handleConnect(ctx context.Context, pk *packets.Packet) error {
	pv := pk.ProtocolVersion
	if pv != mqttwire.ProtocolMQTT311 && pv != mqttwire.ProtocolMQTT5 {
		_ = c.writeConnackReject(pv, cackUnsupportedProtocol)
		return fmt.Errorf("protocol version %d unsupported", pv)
	}
	c.protocol = pv

	// Resolve client ID. Empty client id is allowed only if Clean=true and we
	// generate one; v3.1.1 strict: zero-byte client id => session-resume false.
	clientID := pk.Connect.ClientIdentifier
	if clientID == "" {
		if !pk.Connect.Clean {
			_ = c.writeConnackReject(pv, cackClientIDInvalid)
			return fmt.Errorf("empty client id with clean=false")
		}
		clientID = "auto-" + uuid.NewString()
	}
	c.clientID = clientID
	c.cleanStart = pk.Connect.Clean
	c.keepalive = time.Duration(pk.Connect.Keepalive) * time.Second
	if c.keepalive == 0 {
		c.keepalive = defaultKeepalive
	}

	// Authenticate (always required for v1; anonymous opt-in is a follow-up).
	username := string(pk.Connect.Username)
	password := string(pk.Connect.Password)
	if err := Authenticate(ctx, c.eng.pool, username, password); err != nil {
		_ = c.writeConnackReject(pv, cackBadCredentials)
		return err
	}

	// Capture will from the CONNECT (decoded by codec).
	if pk.Connect.WillFlag {
		c.willTopic = pk.Connect.WillTopic
		c.willPayload = append([]byte(nil), pk.Connect.WillPayload...)
		c.willQoS = pk.Connect.WillQos
		c.willRetain = pk.Connect.WillRetain
		if pv == mqttwire.ProtocolMQTT5 {
			b, err := json.Marshal(pk.Connect.WillProperties)
			if err == nil && string(b) != "{}" && string(b) != "null" {
				c.willProps = b
			}
		}
	}

	// Take ownership in a single transaction.
	prevBroker, newSession, err := c.takeOwnership(ctx, pk)
	if err != nil {
		_ = c.writeConnackReject(pv, cackUnspecified)
		return err
	}

	// Notify the prior owner Pod (if any and different) so it can close the stale socket.
	if prevBroker != uuid.Nil && prevBroker != c.eng.BrokerID() {
		if err := c.eng.takeover.NotifyTakeover(ctx, prevBroker, c.clientID); err != nil {
			c.eng.logger.Warn("takeover notify", "prev", prevBroker, "err", err)
		}
	}

	// Register this Conn locally; if the prior owner was *us*, close it.
	if prev := c.eng.registerConn(c.clientID, c); prev != nil {
		prev.shutdown()
	}

	// Clean-start drops any persisted state from a prior session.
	if c.cleanStart {
		if _, err := c.eng.pool.Exec(ctx, `DELETE FROM subscriptions WHERE client_id=$1`, c.clientID); err != nil {
			return err
		}
		if _, err := c.eng.pool.Exec(ctx, `DELETE FROM deliveries WHERE client_id=$1`, c.clientID); err != nil {
			return err
		}
	}

	sessionPresent := !c.cleanStart && !newSession

	cack := &packets.Packet{
		FixedHeader:    packets.FixedHeader{Type: packets.Connack},
		SessionPresent: sessionPresent,
		ReasonCode:     cackSuccess,
	}
	if pv == mqttwire.ProtocolMQTT5 {
		// Echo a few reasonable server caps; nothing exotic.
		cack.Properties.MaximumQos = 2
		cack.Properties.MaximumQosFlag = true
		cack.Properties.RetainAvailable = 1
		cack.Properties.RetainAvailableFlag = true
		cack.Properties.WildcardSubAvailable = 1
		cack.Properties.WildcardSubAvailableFlag = true
		cack.Properties.SubIDAvailable = 1
		cack.Properties.SubIDAvailableFlag = true
		cack.Properties.SharedSubAvailable = 0
		cack.Properties.SharedSubAvailableFlag = true
		if pk.Connect.ClientIdentifier == "" {
			cack.Properties.AssignedClientID = c.clientID
		}
	}
	if err := c.write(cack); err != nil {
		return err
	}

	// Drain queued / inflight deliveries (state 0,1,2) for resumed sessions.
	if !c.cleanStart {
		if err := c.drainSessionQueue(ctx); err != nil {
			c.eng.logger.Warn("drain queue", "client", c.clientID, "err", err)
		}
	}
	return nil
}

func (c *Conn) writeConnackReject(pv byte, reason byte) error {
	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connack},
		ProtocolVersion: pv,
		ReasonCode:      reason,
	}
	if pv != mqttwire.ProtocolMQTT5 {
		// v3.1.1 has its own CONNACK return-code enum; map a few.
		switch reason {
		case cackBadCredentials:
			pk.ReasonCode = 0x04
		case cackNotAuthorized:
			pk.ReasonCode = 0x05
		case cackUnsupportedProtocol:
			pk.ReasonCode = 0x01
		case cackClientIDInvalid:
			pk.ReasonCode = 0x02
		default:
			pk.ReasonCode = 0x03
		}
	}
	c.protocol = pv
	return c.write(pk)
}

// takeOwnership performs the CONNECT take-over upsert. Returns the previous
// broker_id (uuid.Nil if first session) and whether this is a brand-new row.
func (c *Conn) takeOwnership(ctx context.Context, pk *packets.Packet) (prevBroker uuid.UUID, newSession bool, err error) {
	self := c.eng.BrokerID()
	tx, err := c.eng.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return uuid.Nil, false, err
	}
	defer tx.Rollback(ctx)

	var existed bool
	var existingBroker *uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT broker_id FROM sessions WHERE client_id=$1 FOR UPDATE`, c.clientID).
		Scan(&existingBroker)
	switch err {
	case nil:
		existed = true
		if existingBroker != nil {
			prevBroker = *existingBroker
		}
	case pgx.ErrNoRows:
		existed = false
	default:
		return uuid.Nil, false, err
	}

	expiry := defaultExpiryFor(pk)

	var willPayload []byte
	if c.willTopic != "" {
		willPayload = c.willPayload
	}

	if existed {
		_, err = tx.Exec(ctx, `
			UPDATE sessions SET
				broker_id=$2,
				connected=true,
				protocol_version=$3,
				clean_start=$4,
				expiry_interval=$5,
				will_topic=$6,
				will_payload=$7,
				will_qos=$8,
				will_retain=$9,
				will_delay=$10,
				will_properties=$11,
				last_seen=now()
			WHERE client_id=$1
		`,
			c.clientID, self, c.protocol, c.cleanStart, expiry,
			nullStr(c.willTopic), willPayload, nullByte(c.willQoS, c.willTopic != ""),
			nullBool(c.willRetain, c.willTopic != ""), willDelaySeconds(pk),
			nullJSON(c.willProps),
		)
	} else {
		_, err = tx.Exec(ctx, `
			INSERT INTO sessions
			    (client_id, broker_id, connected, protocol_version, clean_start, expiry_interval,
			     will_topic, will_payload, will_qos, will_retain, will_delay, will_properties)
			VALUES ($1, $2, true, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		`,
			c.clientID, self, c.protocol, c.cleanStart, expiry,
			nullStr(c.willTopic), willPayload, nullByte(c.willQoS, c.willTopic != ""),
			nullBool(c.willRetain, c.willTopic != ""), willDelaySeconds(pk),
			nullJSON(c.willProps),
		)
	}
	if err != nil {
		return uuid.Nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, false, err
	}
	return prevBroker, !existed, nil
}

func defaultExpiryFor(pk *packets.Packet) *int32 {
	if pk.ProtocolVersion == mqttwire.ProtocolMQTT5 {
		// v5 SessionExpiryInterval; 0 = no persistence beyond network connection.
		v := int32(pk.Properties.SessionExpiryInterval)
		return &v
	}
	if pk.Connect.Clean {
		zero := int32(0)
		return &zero
	}
	return nil // v3.1.1 clean=false → infinite
}

func willDelaySeconds(pk *packets.Packet) *int32 {
	if pk.ProtocolVersion != mqttwire.ProtocolMQTT5 {
		return nil
	}
	if !pk.Connect.WillFlag {
		return nil
	}
	v := int32(pk.Connect.WillProperties.WillDelayInterval)
	return &v
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func nullByte(v byte, present bool) any {
	if !present {
		return nil
	}
	return v
}
func nullBool(v bool, present bool) any {
	if !present {
		return nil
	}
	return v
}
func nullJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}
