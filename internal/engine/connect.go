package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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
//	0x8C Bad authentication method
const (
	cackSuccess             byte = 0x00
	cackUnsupportedProtocol byte = 0x84
	cackBadCredentials      byte = 0x86
	cackNotAuthorized       byte = 0x87
	cackClientIDInvalid     byte = 0x85
	cackUnspecified         byte = 0x80
	cackBadAuthMethod       byte = 0x8C
)

func (c *Conn) handleConnect(ctx context.Context, pk *packets.Packet) error {
	// Reject anything that isn't a well-formed CONNECT (bad protocol name,
	// reserved bits, will-no-payload, etc.). mochi's ConnectValidate covers
	// MQTT-3.1.2-{1..N}; we close the socket without CONNACK because the
	// protocol level is untrusted at this point.
	if code := pk.ConnectValidate(); code.Code != 0 {
		return fmt.Errorf("connect validate: %s", code.Reason)
	}
	pv := pk.ProtocolVersion
	if pv != mqttwire.ProtocolMQTT311 && pv != mqttwire.ProtocolMQTT5 {
		_ = c.writeConnackReject(pv, cackUnsupportedProtocol)
		return fmt.Errorf("protocol version %d unsupported", pv)
	}
	c.protocol = pv

	// Resolve client ID.
	//
	// v3.1.1 [MQTT-3.1.3-7]: empty client id is allowed only if Clean=1.
	// v5    [MQTT-3.1.3-6]:  empty client id requires CleanStart=1 AND
	//                       SessionExpiryInterval == 0 (or absent).
	//                       The server then assigns one and reflects it
	//                       back via AssignedClientIdentifier.
	clientID := pk.Connect.ClientIdentifier
	if clientID == "" {
		switch pv {
		case mqttwire.ProtocolMQTT5:
			seNonZero := pk.Properties.SessionExpiryIntervalFlag &&
				pk.Properties.SessionExpiryInterval != 0
			if !pk.Connect.Clean || seNonZero {
				_ = c.writeConnackReject(pv, cackClientIDInvalid)
				return fmt.Errorf("empty client id with clean_start=%v session_expiry=%v",
					pk.Connect.Clean, pk.Properties.SessionExpiryInterval)
			}
		default:
			if !pk.Connect.Clean {
				_ = c.writeConnackReject(pv, cackClientIDInvalid)
				return fmt.Errorf("empty client id with clean=false")
			}
		}
		clientID = "auto-" + uuid.NewString()
	}
	// Cap client_id length defensively. MQTT-5 allows arbitrary UTF-8
	// length, but storing absurd lengths bloats sessions/subscriptions
	// indexes and would push pg_notify payloads (which carry the
	// client_id for takeover/quota signals) past the 8 KB hard limit.
	if len(clientID) > 256 {
		_ = c.writeConnackReject(pv, cackClientIDInvalid)
		return fmt.Errorf("client id too long: %d bytes", len(clientID))
	}
	c.clientID = clientID
	c.cleanStart = pk.Connect.Clean
	clientKeepalive := time.Duration(pk.Connect.Keepalive) * time.Second
	c.keepalive = clientKeepalive
	// Server policy cap — for v5 we advertise the override via ServerKeepAlive.
	maxAllowedKeepalive := c.eng.maxAllowedKeepalive()
	keepaliveOverridden := false
	if c.keepalive == 0 {
		// Spec: keepalive=0 means "no enforcement". We don't honor that
		// (orphan-conn cleanup needs a deadline), so override to a sane
		// default and tell the v5 client via ServerKeepAlive.
		c.keepalive = defaultKeepalive
		keepaliveOverridden = true
	}
	if c.keepalive > maxAllowedKeepalive {
		c.keepalive = maxAllowedKeepalive
		keepaliveOverridden = true
	}

	// Reject MQTT 5 enhanced authentication. We don't advertise any
	// authentication method on CONNACK, so a client shouldn't send one;
	// if it does, MQTT-4.12.0-1 says we MAY return CONNACK 0x8C
	// (Bad authentication method). We do — silently ignoring the property
	// (current behaviour) is a spec hazard because the client thinks
	// password auth was bypassed in favour of its enhanced flow.
	if pv == mqttwire.ProtocolMQTT5 && len(pk.Properties.AuthenticationMethod) > 0 {
		_ = c.writeConnackReject(pv, cackBadAuthMethod)
		return fmt.Errorf("enhanced auth method %q not supported",
			pk.Properties.AuthenticationMethod)
	}

	// Authenticate. PGMQTT_ALLOW_ANONYMOUS=true skips this when no username
	// is supplied (still validates credentials when one is).
	username := string(pk.Connect.Username)
	password := string(pk.Connect.Password)
	if username != "" || !c.eng.cfg.AllowAnonymous {
		if err := Authenticate(ctx, c.eng.pool, username, password); err != nil {
			// Tick the per-IP auth-failure bucket so a stream of
			// bad-credential CONNECTs from a single source IP
			// eventually trips the penalty box (subsequent CONNECTs
			// are dropped pre-bcrypt). Done before the CONNACK
			// reject write so a connection refused mid-write still
			// counts towards the IP's failure budget.
			c.eng.recordAuthFailureFor(c.nc.RemoteAddr())
			_ = c.writeConnackReject(pv, cackBadCredentials)
			return err
		}
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
			d := pk.Connect.WillProperties.WillDelayInterval
			c.willDelay = &d
		}
	}
	// SessionExpiryInterval / ReceiveMaximum / MaximumPacketSize / TopicAliasMaximum (v5).
	if pv == mqttwire.ProtocolMQTT5 {
		// Only set sessionExpiry when the property was actually present
		// in the CONNECT — nil distinguishes "client said nothing" (which
		// the spec defines as 0) from "client said 0 explicitly," which
		// matters for the DISCONNECT increase-from-0 invalid-flag check
		// in handleGracefulDisconnect.
		if pk.Properties.SessionExpiryIntervalFlag {
			v := pk.Properties.SessionExpiryInterval
			c.sessionExpiry = &v
		}
		// [MQTT-3.1.2-25]: MaximumPacketSize=0 is a Protocol Error. mochi's
		// codec doesn't surface a presence flag for this property (a missing
		// property and value=0 both decode to MaximumPacketSize==0), so we
		// re-walk the raw CONNECT body to disambiguate. A present-and-zero
		// would otherwise disable our outbound size cap entirely
		// (c.write treats c.maxPacketSize==0 as "unlimited").
		present, mps, err := mqttwire.V5ConnectMaximumPacketSize(c.reader.LastConnectBody)
		if err != nil {
			_ = c.writeConnackReject(pv, cackUnspecified)
			return fmt.Errorf("parse CONNECT mps: %w", err)
		}
		if present && mps == 0 {
			_ = c.writeConnackReject(pv, 0x95) // Packet too large
			return fmt.Errorf("CONNECT MaximumPacketSize=0 is a Protocol Error")
		}
		c.maxPacketSize = pk.Properties.MaximumPacketSize
		c.receiveMaximum = pk.Properties.ReceiveMaximum
		c.topicAliasMaximumOut = pk.Properties.TopicAliasMaximum
		if c.topicAliasMaximumOut > 0 {
			c.aliasOut = make(map[string]uint16)
		}
	}
	if c.receiveMaximum == 0 {
		c.receiveMaximum = 65535 // [MQTT-3.1.2-26] default
	}
	// Honor the client's ReceiveMaximum but never allocate more than
	// `maxConcurrentInflight` slots — even at the spec default of 65535
	// per v5 Conn, this is purely server-side memory and a runaway
	// outbound queue is a bigger risk than capping a client's
	// theoretical inflight ceiling.
	inflightCap := int(c.receiveMaximum)
	if inflightCap > maxConcurrentInflight {
		inflightCap = maxConcurrentInflight
	}
	c.inflight = make(chan struct{}, inflightCap)
	c.drainKick = make(chan struct{}, 1)

	// Lift the codec's pre-CONNECT 1 MiB inbound size cap to the configured
	// server policy (PGMQTT_MAX_PACKET_SIZE; default 16 MiB). 0 leaves the
	// codec's PreConnectMaxPacketSize cap in place. The client's CONNECT
	// MaximumPacketSize property only governs what WE send TO them
	// (handled in c.write), not what we accept from them; the inbound cap
	// is purely server policy.
	if cap := c.eng.maxPacketSize(); cap > 0 {
		c.reader.SetMaxPacketSize(uint32(cap))
	}

	// Take ownership in a single transaction.
	prevBroker, prevToken, newSession, err := c.takeOwnership(ctx, pk)
	if err != nil {
		_ = c.writeConnackReject(pv, cackUnspecified)
		return err
	}
	c.eng.logger.Debug("takeover", "client", c.clientID,
		"prev_broker", prevBroker, "new_broker", c.eng.BrokerID(),
		"new_session", newSession)

	// Notify the prior owner Pod (if any and different) so it can close
	// the stale socket. prevToken pins which Conn over there is stale —
	// the receiver shuts down only the Conn whose sessionToken == prevToken
	// so that a late-arriving notification can't kill a Conn that has
	// since been re-taken-over back to that Pod.
	if prevBroker != uuid.Nil && prevBroker != c.eng.BrokerID() {
		if err := c.eng.takeover.NotifyTakeover(ctx, prevBroker, c.clientID, prevToken); err != nil {
			c.eng.logger.Warn("takeover notify", "prev", prevBroker, "err", err)
		}
	}

	// Register this Conn locally; if the prior owner was *us*, close it.
	// Mark the prior conn as taken-over so its handleDisconnect suppresses
	// will firing — same-pod takeover is also a session migration, not
	// the client dying (MQTT-3.1.2.5).
	if prev := c.eng.registerConn(c.clientID, c); prev != nil {
		prev.takenOver.Store(true)
		prev.shutdown()
	}

	// Clean-start drops any persisted state from a prior session, and
	// in any case we cancel pending will + session-expiry timers (the
	// client beat the timer to reconnect). All three writes share one
	// tx so a partial failure can't leave (e.g.) orphan subscriptions
	// after a "session is gone" CONNACK or, conversely, the prior
	// will_fire_at active after a successful clean reconnect.
	tx, err := c.eng.beginTxTimed(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if c.cleanStart {
		if _, err := tx.Exec(ctx, `DELETE FROM subscriptions WHERE client_id=$1`, c.clientID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM deliveries WHERE client_id=$1`, c.clientID); err != nil {
			return err
		}
		// inbound_qos2 has FK ON DELETE CASCADE on (client_id) → sessions, but
		// takeOwnership above re-uses the existing sessions row instead of
		// dropping it. Without this explicit DELETE, a fresh QoS-2 PUBLISH
		// from the new session that happens to reuse a packet_id from the
		// prior incarnation would hit ON CONFLICT DO NOTHING in publishCore
		// and be silently swallowed (the broker re-emits PUBREC but doesn't
		// fan out). Drop the stale tombstones to make cleanStart actually
		// clean.
		if _, err := tx.Exec(ctx, `DELETE FROM inbound_qos2 WHERE client_id=$1`, c.clientID); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx,
		`UPDATE sessions SET will_fire_at=NULL, session_expires_at=NULL WHERE client_id=$1`,
		c.clientID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
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
		if keepaliveOverridden {
			cack.Properties.ServerKeepAlive = uint16(c.keepalive / time.Second)
			cack.Properties.ServerKeepAliveFlag = true
		}
		cack.Properties.ReceiveMaximum = c.eng.serverReceiveMaximum()
		cack.Properties.TopicAliasMaximum = c.eng.serverTopicAliasMaximum()
		// Advertise our inbound MaximumPacketSize so well-behaved
		// clients don't blast a 50 MiB PUBLISH and then get a surprise
		// DISCONNECT 0x95. Spec default if absent is 256 MiB; we cap
		// well below that. Skip advertising when policy is "no cap".
		if cap := c.eng.maxPacketSize(); cap > 0 {
			cack.Properties.MaximumPacketSize = uint32(cap)
		}
	}
	if err := c.write(cack); err != nil {
		return err
	}

	// Drain queued / inflight deliveries (state 0,1,2) for resumed sessions.
	// Counter ordering matches the dead-broker-Inc fix shape: Inc the
	// success counter only AFTER drainSessionQueue returns nil. A failed
	// drain bumps a sibling _failures_total so we keep an attempt-rate
	// signal even when the drain itself is wedged on PG.
	if !c.cleanStart {
		if err := c.drainSessionQueue(ctx); err != nil {
			c.eng.logger.Warn("drain queue", "client", c.clientID, "err", err)
			if c.eng.metrics != nil {
				c.eng.metrics.DrainSessionQueueFailuresTotal.WithLabelValues("reconnect").Inc()
			}
		} else if c.eng.metrics != nil {
			c.eng.metrics.DrainSessionQueueTotal.WithLabelValues("reconnect").Inc()
		}
	}

	// Background drain loop: when PUBACK/PUBCOMP frees an in-flight slot it
	// kicks drainKick; we re-scan state=0 deliveries and send what fits.
	// Register on the engine WaitGroup so shutdownGracefully waits for the
	// goroutine to exit before pool.Close() runs — otherwise a drain
	// query can land on a closing pool and emit a noisy warning at every
	// shutdown.
	c.eng.wg.Add(1)
	go func() {
		defer c.eng.wg.Done()
		c.runDrainLoop(ctx)
	}()
	return nil
}

func (c *Conn) writeConnackReject(pv byte, reason byte) error {
	if c.eng.metrics != nil {
		c.eng.metrics.AuthFailuresTotal.WithLabelValues(authReasonLabel(reason)).Inc()
	}
	// Per [MQTT-3.1.4-5], when the protocol version is unsupported or
	// otherwise unknown, send a v3.1.1-shaped CONNACK with reason 0x01.
	// Echoing back the client's raw `pv` byte (which could be 0xFF or any
	// garbage) would produce a malformed wire CONNACK from the client's
	// point of view, so coerce to a valid version for the encoder.
	wirePV := pv
	if wirePV != mqttwire.ProtocolMQTT311 && wirePV != mqttwire.ProtocolMQTT5 {
		wirePV = mqttwire.ProtocolMQTT311
	}
	pk := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connack},
		ProtocolVersion: wirePV,
		ReasonCode:      reason,
	}
	if wirePV != mqttwire.ProtocolMQTT5 {
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
	c.protocol = wirePV
	return c.write(pk)
}

func authReasonLabel(reason byte) string {
	switch reason {
	case cackBadCredentials:
		return "bad_credentials"
	case cackNotAuthorized:
		return "not_authorized"
	case cackBadAuthMethod:
		return "bad_auth_method"
	case cackClientIDInvalid:
		return "client_id_invalid"
	case cackUnsupportedProtocol:
		return "unsupported_protocol"
	default:
		return "other"
	}
}

// takeOwnership performs the CONNECT take-over upsert. Returns the previous
// broker_id (uuid.Nil if first session), the previous session_token (uuid.Nil
// if no prior session), and whether this is a brand-new row.
//
// Every takeOwnership rotates the row's session_token so a peer's stale
// handleDisconnect can guard its DELETE on the token captured at takeover
// time and roll back instead of wiping the new conn's row. The previous
// token is also handed back so NotifyTakeover can target only the truly
// stale Conn on the prior owner pod (and not a newer Conn that might have
// since been re-taken-over back). Token is stored on Conn.sessionToken
// for handleDisconnect to read later.
func (c *Conn) takeOwnership(ctx context.Context, pk *packets.Packet) (prevBroker uuid.UUID, prevToken uuid.UUID, newSession bool, err error) {
	self := c.eng.BrokerID()
	tx, err := c.eng.beginTxTimed(ctx, pgx.TxOptions{})
	if err != nil {
		return uuid.Nil, uuid.Nil, false, err
	}
	defer tx.Rollback(ctx)

	var existed bool
	var existingBroker *uuid.UUID
	var existingToken *uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT broker_id, session_token FROM sessions WHERE client_id=$1 FOR UPDATE`, c.clientID).
		Scan(&existingBroker, &existingToken)
	switch err {
	case nil:
		existed = true
		if existingBroker != nil {
			prevBroker = *existingBroker
		}
		if existingToken != nil {
			prevToken = *existingToken
		}
	case pgx.ErrNoRows:
		existed = false
	default:
		return uuid.Nil, uuid.Nil, false, err
	}

	expiry := defaultExpiryFor(pk)

	var willPayload []byte
	if c.willTopic != "" {
		willPayload = c.willPayload
	}

	var newToken uuid.UUID
	if existed {
		err = tx.QueryRow(ctx, `
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
				session_token=gen_random_uuid(),
				last_seen=now()
			WHERE client_id=$1
			RETURNING session_token
		`,
			c.clientID, self, c.protocol, c.cleanStart, expiry,
			nullStr(c.willTopic), willPayload, nullByte(c.willQoS, c.willTopic != ""),
			nullBool(c.willRetain, c.willTopic != ""), willDelaySeconds(pk),
			nullJSON(c.willProps),
		).Scan(&newToken)
	} else {
		err = tx.QueryRow(ctx, `
			INSERT INTO sessions
			    (client_id, broker_id, connected, protocol_version, clean_start, expiry_interval,
			     will_topic, will_payload, will_qos, will_retain, will_delay, will_properties,
			     session_token)
			VALUES ($1, $2, true, $3, $4, $5, $6, $7, $8, $9, $10, $11, gen_random_uuid())
			RETURNING session_token
		`,
			c.clientID, self, c.protocol, c.cleanStart, expiry,
			nullStr(c.willTopic), willPayload, nullByte(c.willQoS, c.willTopic != ""),
			nullBool(c.willRetain, c.willTopic != ""), willDelaySeconds(pk),
			nullJSON(c.willProps),
		).Scan(&newToken)
	}
	if err != nil {
		return uuid.Nil, uuid.Nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, uuid.Nil, false, err
	}
	c.sessionToken = newToken
	return prevBroker, prevToken, !existed, nil
}

func defaultExpiryFor(pk *packets.Packet) *int32 {
	if pk.ProtocolVersion == mqttwire.ProtocolMQTT5 {
		// v5 SessionExpiryInterval; 0 = no persistence beyond network connection.
		// Spec field is uint32. The DB column expiry_interval is INT (int32);
		// values > MaxInt32 are absurd in practice (~68 years) so clamp to
		// MaxInt32. The in-memory sessionExpiry preserves the full uint32
		// range — this just bounds what we persist for record-keeping.
		v := clampUint32ToInt32(pk.Properties.SessionExpiryInterval)
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
	v := clampUint32ToInt32(pk.Connect.WillProperties.WillDelayInterval)
	return &v
}

// clampUint32ToInt32 clamps a uint32 to [0, MaxInt32] for storage in an
// INT column. Any value above MaxInt32 (~68 years in seconds) becomes
// MaxInt32 — for the SessionExpiryInterval / WillDelayInterval use case
// this is functionally equivalent to "indefinite" anyway.
func clampUint32ToInt32(v uint32) int32 {
	if v > uint32(math.MaxInt32) {
		return math.MaxInt32
	}
	return int32(v)
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
