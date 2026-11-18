package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mochi-mqtt/server/v2/packets"

	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

const (
	connectTimeout        = 10 * time.Second
	defaultKeepalive      = 60 * time.Second
	maxConcurrentInflight = 1024 // upper bound on per-conn outgoing inflight messages

	// closeNoticeTimeout bounds the DISCONNECT that precedes a
	// server-initiated close, which is a courtesy and must not hold the
	// close up.
	closeNoticeTimeout = 2 * time.Second
)

// writeTimeout is how long one packet may take to leave for a client that
// is not reading. Past it the connection is closed. A var so tests can
// shorten it.
var writeTimeout = 10 * time.Second

// Conn is a single client connection.
type Conn struct {
	eng    *Engine
	nc     net.Conn
	reader *mqttwire.Reader

	clientID string
	protocol byte

	// sessionToken is set by takeOwnership and used by finish to
	// scope its session-DELETE. Each takeOwnership rotates it via
	// gen_random_uuid(); a stale disconnect whose finish runs
	// after a peer takeover will see 0 rows match and roll back rather
	// than wipe the new conn's row. See migration 0012.
	sessionToken uuid.UUID

	writeMu sync.Mutex

	keepalive       time.Duration
	cleanStart      bool
	willTopic       string
	willPayload     []byte
	willQoS         byte
	willRetain      bool
	willProps       []byte // jsonb-serialised v5 will properties
	willDelay     *uint32 // v5 WillDelayInterval (seconds); nil for v3.1.1
	sessionExpiry *uint32 // v5 SessionExpiryInterval (seconds); nil = "absent property" (≡ 0 per spec)
	maxPacketSize   uint32 // v5 MaximumPacketSize the client will accept; 0 = no limit
	receiveMaximum  uint16 // v5 ReceiveMaximum the client will accept; 65535 if unset
	inflight        chan struct{} // token-bucket; capacity = receiveMaximum
	drainKick       chan struct{} // single-slot signal to wake the sender

	// inflightOverdraft counts in-flight messages resent on resume that
	// found the window already full (the client came back with a smaller
	// Receive Maximum). Their acknowledgements pay this off before any
	// slot is released, so the window never ends up wider than agreed.
	inflightOverdraft atomic.Int32

	// senderActive is set once the sender goroutine is running.
	// closeNotice holds a requested server-side close: 0 for none,
	// otherwise 0x100 | the v5 DISCONNECT reason code to send first.
	senderActive atomic.Bool
	closeNotice  atomic.Int32

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
	// takenOver is set by the listener's takeover dispatch immediately
	// before calling Shutdown on a stale Conn. finish reads it
	// to suppress will fire/schedule: a session-takeover-driven server-
	// side shutdown is NOT abnormal termination, so per MQTT-3.1.2.5
	// the Will Message must not be published. Without this, a client
	// that just successfully roamed to a new pod would have its previous
	// pod's stale conn fire the will (and with willRetain=true, stamp
	// "offline" on top of the device's new live presence).
	takenOver         atomic.Bool
	closed            chan struct{}
	once              sync.Once

	// quotaOnce gates the full QuotaExceededLocally side-effect chain
	// (log + DISCONNECT 0x97 write + metric Inc + Shutdown) against
	// repeated invocation for the same conn. Without this, a local quota
	// trip racing a cross-pod NOTIFY for the same client_id would have
	// both goroutines pass the ConnFor check and double-count
	// pgmqtt_quota_exceeded_total. Shutdown itself is already idempotent
	// (via `once` above) — this exists for the surrounding bookkeeping.
	quotaOnce sync.Once

	// Inbound rate limit (PUBLISH/SUBSCRIBE). Token-bucket: capacity equals
	// MaxInboundMsgsPerSec; refills at 1/cap each tick, capped at the
	// configured rate. 0 disables the limit. Burst=cap means a fully-filled
	// bucket can absorb a burst of `cap` messages, then steady-state at rate.
	rateMu     sync.Mutex
	rateTokens int
	rateLast   time.Time

	// Outbound packet-id allocator. Per-Conn in-memory counter; replaces the
	// per-delivery UPDATE on sessions.next_packet_id (which caused HOT-update
	// bloat over hours of operation). MQTT requires uniqueness only across
	// in-flight packets — durability across crashes is not required, so a
	// per-Conn counter seeded from MAX(packet_id) on takeover is sufficient.
	// Seeded lazily on first use under packetIDSeedMu so the seeding query
	// runs AFTER any clean-session DELETE has been committed. Transient
	// seed-time errors don't latch — the next AllocPacketID call retries the
	// seed. Once seeded successfully, packetIDSeeded is set and the lock is
	// no longer entered on the hot path.
	//
	// packetIDState holds the last *raw* counter value; AllocPacketID does
	// Add(1) and maps the result to 1..65535 via ((v-1)%65535)+1, which
	// skips 0 (reserved per spec) and wraps cleanly. Seed-on-takeover
	// stores MAX(packet_id) so the first allocation is MAX+1.
	packetIDSeedMu sync.Mutex
	packetIDSeeded atomic.Bool
	packetIDState  atomic.Uint32
}

func newConn(e *Engine, nc net.Conn) *Conn {
	c := &Conn{
		eng:    e,
		nc:     nc,
		reader: mqttwire.NewReader(nc),
		closed: make(chan struct{}),
	}
	if r := e.maxInboundRate(); r > 0 {
		c.rateTokens = r
		c.rateLast = time.Now()
	}
	return c
}

// maxInboundRate returns the per-conn rate-limit (msgs/s) for inbound
// PUBLISH/SUBSCRIBE. 0 disables the limit.
func (e *Engine) maxInboundRate() int {
	return int(e.maxInboundRateAtomic.Load())
}

// maxPacketSize returns the post-CONNECT inbound packet-size cap (bytes)
// configured for this Pod. 0 means "no Pod-level cap" — the codec falls
// back to the absolute upper bound (256 MiB).
func (e *Engine) maxPacketSize() int {
	return int(e.maxPacketSizeAtomic.Load())
}

func (c *Conn) run(ctx context.Context) {
	defer c.shutdown()
	defer func() {
		if r := recover(); r != nil {
			c.eng.logger.Error("conn run panic",
				"client", c.clientID, "panic", r, "stack", string(debug.Stack()))
		}
	}()
	// Every way out of this function is the end of the connection, and
	// if it got as far as owning a session the end has to be recorded:
	// whoever closed the socket, and whether or not CONNECT completed.
	var cause error
	defer func() { c.finish(cause) }()

	// Wait for CONNECT (must be first).
	if err := c.nc.SetReadDeadline(time.Now().Add(connectTimeout)); err != nil {
		c.eng.logger.Debug("set read deadline", "err", err)
		return
	}
	pk, err := c.reader.Read()
	if err != nil {
		// Pre-CONNECT: any read error (including ErrPacketTooLarge from a
		// peer announcing > 1 MiB remaining length) hard-closes the socket
		// without writing a CONNACK. The protocol level is untrusted at
		// this point — we don't know whether to send a v3 or v5 reply.
		if errors.Is(err, mqttwire.ErrPacketTooLarge) {
			c.eng.logger.Warn("pre-connect packet exceeded size cap", "remote", c.nc.RemoteAddr())
		} else {
			c.eng.logger.Debug("read connect", "err", err)
		}
		return
	}
	if pk.FixedHeader.Type != packets.Connect {
		c.eng.logger.Warn("first packet not CONNECT", "type", pk.FixedHeader.Type)
		return
	}
	if err := c.handleConnect(ctx, &pk); err != nil {
		// Infra drops are already Warn-logged with stage + remote by
		// dropConnectOnInfraError; don't double-log them as rejections.
		if !errors.Is(err, errConnectInfra) {
			c.eng.logger.Info("connect rejected", "err", err)
		}
		cause = err
		return
	}

	// Main read loop.
	for {
		if err := c.armReadDeadline(); err != nil {
			return
		}
		pk, err := c.reader.Read()
		if err != nil {
			// Post-CONNECT oversize: v5 gets DISCONNECT 0x95 (Packet Too
			// Large) so the client can distinguish "I sent something too
			// big" from a generic socket close. v3.1.1 has no DISCONNECT-
			// from-server, so we hard-close.
			if errors.Is(err, mqttwire.ErrPacketTooLarge) && c.protocol == mqttwire.ProtocolMQTT5 {
				_ = c.write(&packets.Packet{
					FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
					ReasonCode:  0x95,
				})
			}
			cause = err
			return
		}
		if err := c.dispatch(ctx, &pk); err != nil {
			// io.EOF / errClientDisconnect signals "this is a normal close" —
			// don't log at warn level. Real dispatch errors stay at warn.
			if !errors.Is(err, io.EOF) && !errors.Is(err, errClientDisconnect) {
				c.eng.logger.Warn("dispatch", "client", c.clientID, "type", pk.FixedHeader.Type, "err", err)
			}
			cause = err
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
	mult := c.eng.KeepAliveMultiplier
	if mult <= 0 {
		mult = 1.5
	}
	return c.nc.SetReadDeadline(time.Now().Add(time.Duration(float64(c.keepalive) * mult)))
}

func (c *Conn) dispatch(ctx context.Context, pk *packets.Packet) error {
	switch pk.FixedHeader.Type {
	case packets.Publish, packets.Subscribe:
		// Per-conn rate limit only applies to PUBLISH/SUBSCRIBE — the cost
		// drivers. Acks (PUBACK/PUBREC/PUBREL/PUBCOMP) and PINGREQ are
		// not metered: they're protocol-required responses to packets we
		// already counted, and rate-limiting them would deadlock QoS flows.
		if !c.tryConsumeRateToken() {
			_ = c.write(&packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
				ReasonCode:  0x96, // Message rate too high
			})
			if c.eng.metrics != nil {
				c.eng.metrics.RateLimitedTotal.Inc()
			}
			return fmt.Errorf("inbound rate limit exceeded")
		}
	}
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
	case packets.Auth:
		// We don't advertise an authentication method on CONNACK and reject
		// CONNECTs that supply AuthenticationMethod (cackBadAuthMethod), so
		// AUTH (3.15) cannot legitimately arrive mid-connection. Reply with
		// DISCONNECT 0x82 (Protocol error) per MQTT-4.12.0-2 instead of
		// silently dropping the socket via the default case, which left the
		// client unable to distinguish a TCP wedge from a server reject.
		_ = c.write(&packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
			ReasonCode:  0x82,
		})
		return fmt.Errorf("AUTH unexpected: enhanced auth not supported")
	default:
		return fmt.Errorf("unsupported packet type: %d", pk.FixedHeader.Type)
	}
}

// tryConsumeRateToken refills the bucket up to `cap` based on elapsed time
// (cap tokens per second) and consumes one. Returns false when the bucket is
// empty after refill.
func (c *Conn) tryConsumeRateToken() bool {
	cap := c.eng.maxInboundRate()
	if cap <= 0 {
		return true
	}
	c.rateMu.Lock()
	defer c.rateMu.Unlock()
	now := time.Now()
	if !c.rateLast.IsZero() {
		add := int(now.Sub(c.rateLast).Seconds() * float64(cap))
		if add > 0 {
			c.rateTokens += add
			if c.rateTokens > cap {
				c.rateTokens = cap
			}
			c.rateLast = now
		}
	} else {
		c.rateLast = now
		c.rateTokens = cap
	}
	if c.rateTokens <= 0 {
		return false
	}
	c.rateTokens--
	return true
}

// write encodes pk and writes it to the socket, serialised with every
// other writer of this connection. A write that fails or times out closes
// the connection: the stream may hold half a packet, and a client that
// has stopped reading is not going to start because we wait longer.
func (c *Conn) write(pk *packets.Packet) error {
	return c.writeWithin(pk, writeTimeout)
}

func (c *Conn) writeWithin(pk *packets.Packet, timeout time.Duration) error {
	if c.closing.Load() {
		return errors.New("conn closing")
	}
	pk.ProtocolVersion = c.protocol
	buf, err := mqttwire.Encode(pk)
	if err != nil {
		return err
	}
	if c.maxPacketSize > 0 && uint32(len(buf)) > c.maxPacketSize {
		return errPacketTooLarge
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.nc.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		c.shutdown()
		return err
	}
	if _, err := c.nc.Write(buf); err != nil {
		c.shutdown()
		return err
	}
	return nil
}

// V5 server policy. The broker advertises these in CONNACK and enforces
// them during the session. Driven by config (see PGMQTT_RECEIVE_MAXIMUM /
// PGMQTT_KEEPALIVE_MAX_SEC). Stored as atomics so test setters don't
// race the accept loop. The defaults match the historical hardcoded
// values used pre-config plumbing.

func (e *Engine) serverReceiveMaximum() uint16 {
	if v := e.receiveMaxV5Atomic.Load(); v > 0 {
		return uint16(v)
	}
	return 100
}

// serverTopicAliasMaximum is hardcoded to 0 — the broker does not
// maintain an inbound topic-alias map, so we tell clients (per
// [MQTT-3.3.2-12]) never to send TopicAlias > 0. Inbound PUBLISHes with
// a TopicAlias get DISCONNECT 0x94. Outbound aliases (server → client)
// are supported when the client advertises TopicAliasMaximum > 0 in
// CONNECT.
func (e *Engine) serverTopicAliasMaximum() uint16 { return 0 }

func (e *Engine) maxAllowedKeepalive() time.Duration {
	if v := e.keepaliveMaxV5Atomic.Load(); v > 0 {
		return time.Duration(v)
	}
	return 60 * time.Second
}

// maxQueuedDeliveries is the per-client cap on the deliveries table. 0 means
// no cap (the SQL function treats 0 as unlimited).
func (e *Engine) maxQueuedDeliveries() int {
	return int(e.maxQueuedAtomic.Load())
}

// SetMaxQueuedDeliveriesForTest overrides the per-client deliveries cap.
// Test-only; production code reads from config.
func (e *Engine) SetMaxQueuedDeliveriesForTest(n int) {
	e.maxQueuedAtomic.Store(int64(n))
}

// SetMaxConnectionsForTest overrides the per-Pod connection cap. Test-only.
func (e *Engine) SetMaxConnectionsForTest(n int) {
	e.maxConnsAtomic.Store(int64(n))
}

// SetMaxInboundRateForTest overrides the per-conn inbound msgs/s rate.
// Test-only.
func (e *Engine) SetMaxInboundRateForTest(n int) {
	e.maxInboundRateAtomic.Store(int64(n))
}

// SetMaxPacketSizeForTest overrides the post-CONNECT inbound packet-size
// cap. Test-only.
func (e *Engine) SetMaxPacketSizeForTest(n int) {
	e.maxPacketSizeAtomic.Store(int64(n))
}

// SetReceiveMaxV5ForTest overrides the v5 inbound ReceiveMaximum (the
// server-side cap on un-ACKed inbound QoS>0 PUBLISHes per conn).
// Test-only; production reads PGMQTT_RECEIVE_MAXIMUM.
func (e *Engine) SetReceiveMaxV5ForTest(n int) {
	e.receiveMaxV5Atomic.Store(int64(n))
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

// allocPacketIDMaxRetries bounds the collision-retry loop in AllocPacketID.
// Collisions only occur when an existing un-acked delivery already holds the
// candidate id — they should be vanishingly rare given the seed comes from
// MAX(packet_id) on takeover, but we still need a bound so a fully-saturated
// in-flight space (65k un-acked) returns an error rather than spinning.
const allocPacketIDMaxRetries = 8

// errNoPacketID is returned when AllocPacketID can't find a free id within
// allocPacketIDMaxRetries attempts. Callers should fail the delivery — the
// in-flight space for the client is effectively exhausted.
var errNoPacketID = errors.New("no free packet id (in-flight space exhausted)")

// ensurePacketIDSeeded runs the one-shot MAX(packet_id) lookup if not yet
// done. Returns the previous error so callers can distinguish "seed failed,
// retry next call" from "seed already done, proceed". Transient seed errors
// (context cancelled, DB hiccup) leave packetIDSeeded=false so the next
// AllocPacketID call retries.
func (c *Conn) ensurePacketIDSeeded(ctx context.Context) error {
	if c.packetIDSeeded.Load() {
		return nil
	}
	c.packetIDSeedMu.Lock()
	defer c.packetIDSeedMu.Unlock()
	if c.packetIDSeeded.Load() {
		return nil
	}
	var seed *int
	if err := c.eng.pool.QueryRow(ctx, `
		SELECT MAX(packet_id) FROM deliveries
		 WHERE client_id=$1 AND packet_id IS NOT NULL
	`, c.clientID).Scan(&seed); err != nil {
		return err
	}
	v := uint32(0)
	if seed != nil {
		s := *seed
		if s < 0 {
			s = 0
		}
		if s > 65535 {
			s = 65535
		}
		v = uint32(s)
	}
	c.packetIDState.Store(v)
	c.packetIDSeeded.Store(true)
	return nil
}

// nextPacketIDCandidate returns the next id in the wrap-modulo-65535 cycle
// (1..65535, skipping 0). Atomic increment; safe under concurrent callers.
func (c *Conn) nextPacketIDCandidate() uint16 {
	v := c.packetIDState.Add(1)
	// Map to 1..65535. ((v-1) mod 65535) + 1 avoids 0 (reserved per spec)
	// and wraps cleanly when v exceeds uint32 range — though uint32 won't
	// realistically wrap (~136 years at 1G allocs/s).
	return uint16(((v-1)%65535)+1)
}

// AllocPacketID returns the next packet id for an outbound QoS>0 delivery on
// this Conn. Lazy-seeds from MAX(deliveries.packet_id) on first use, so a
// resumed session never hands out an id that's still in flight from a prior
// connection. On collision (rare), the candidate is incremented and retried
// up to allocPacketIDMaxRetries times before returning errNoPacketID.
//
// The pid is reserved in the same caller's UPDATE deliveries WHERE state=0
// transaction — collisions surface there as a unique-violation, not here.
// This method only screens against *currently-known* un-acked rows; the SQL
// transition is the source of truth.
func (c *Conn) AllocPacketID(ctx context.Context) (uint16, error) {
	if err := c.ensurePacketIDSeeded(ctx); err != nil {
		return 0, err
	}
	for i := 0; i < allocPacketIDMaxRetries; i++ {
		pid := c.nextPacketIDCandidate()
		taken, err := c.packetIDInUse(ctx, pid)
		if err != nil {
			return 0, err
		}
		if !taken {
			return pid, nil
		}
	}
	return 0, errNoPacketID
}

// packetIDInUse reports whether (clientID, pid) already exists in deliveries.
// Used by AllocPacketID's collision-retry loop. Cheap: covered by the
// (client_id, packet_id) unique partial index.
func (c *Conn) packetIDInUse(ctx context.Context, pid uint16) (bool, error) {
	var exists bool
	err := c.eng.pool.QueryRow(ctx, `
		SELECT EXISTS(
		  SELECT 1 FROM deliveries
		   WHERE client_id=$1 AND packet_id=$2
		)
	`, c.clientID, int(pid)).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

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

// inflightAvailable reports whether a QoS>0 message could be sent now.
func (c *Conn) inflightAvailable() bool {
	if c.protocol != mqttwire.ProtocolMQTT5 || c.inflight == nil {
		return true
	}
	return len(c.inflight) < cap(c.inflight)
}

// holdInflight accounts for a message that is already in flight and has
// to be resent whether or not there is room for it.
func (c *Conn) holdInflight() {
	if !c.tryAcquireInflight() {
		c.inflightOverdraft.Add(1)
	}
}

// returnInflight releases a flow-control slot and wakes the sender.
func (c *Conn) returnInflight() {
	if c.inflight == nil {
		return
	}
	for {
		over := c.inflightOverdraft.Load()
		if over <= 0 {
			select {
			case <-c.inflight:
			default:
			}
			break
		}
		if c.inflightOverdraft.CompareAndSwap(over, over-1) {
			break
		}
	}
	c.KickDrain()
}

// KickDrain wakes the sender to look for queued deliveries. Safe to call
// from any goroutine and non-blocking: drainKick is a 1-slot channel and
// a pending kick coalesces with a second one. No-op for conns that
// haven't completed CONNECT (drainKick is allocated there).
func (c *Conn) KickDrain() {
	if c.drainKick == nil {
		return
	}
	select {
	case c.drainKick <- struct{}{}:
	default:
	}
}

// runSender is the connection's sender goroutine: the only thing that
// sends this client its deliveries (see deliver.go), and where a
// server-initiated close is carried out. It sleeps until kicked.
//
// It does not stop on ctx: that is cancelled the moment the broker is
// told to shut down, which is exactly when it is needed to say goodbye.
// Database calls made with a cancelled ctx fail at once, so it does not
// linger either; it ends when the connection is closed.
func (c *Conn) runSender(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			c.eng.logger.Error("sender panic",
				"client", c.clientID, "panic", r, "stack", string(debug.Stack()))
			// A half-broken sender is worse than a clean reconnect.
			c.shutdown()
		}
	}()
	for {
		select {
		case <-c.closed:
			return
		case <-c.drainKick:
		}
		for {
			if c.carryOutClose() || c.closing.Load() {
				return
			}
			disposed, err := c.drainBatch(ctx)
			if err != nil {
				c.eng.logger.Warn("drain", "client", c.clientID, "err", err)
				break
			}
			if disposed == 0 {
				break
			}
		}
	}
}

// Disconnect asks for this connection to be closed by the server, telling
// a v5 client why first. It never blocks, so it is safe on any goroutine,
// including ones that must not wait on a client (the listener, another
// client's publish, the shutdown loop).
func (c *Conn) Disconnect(reason byte) {
	c.closeNotice.CompareAndSwap(0, 0x100|int32(reason))
	if !c.senderActive.Load() {
		c.shutdown()
		return
	}
	// If the sender is stuck writing to a client that is not reading,
	// cut that write short rather than queue the close behind it. For a
	// WebSocket the deadline goes on the socket underneath: gorilla's
	// own is a plain field read by the write in progress.
	deadline := time.Now().Add(closeNoticeTimeout)
	if ws, ok := c.nc.(*wsConnAdapter); ok {
		_ = ws.ws.UnderlyingConn().SetWriteDeadline(deadline)
	} else {
		_ = c.nc.SetWriteDeadline(deadline)
	}
	c.KickDrain()
}

// carryOutClose performs a requested close, if there is one.
func (c *Conn) carryOutClose() bool {
	notice := c.closeNotice.Load()
	if notice == 0 {
		return false
	}
	if c.protocol == mqttwire.ProtocolMQTT5 {
		_ = c.writeWithin(&packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
			ReasonCode:  byte(notice),
		}, closeNoticeTimeout)
	}
	c.shutdown()
	return true
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

// ShutdownForTakeover marks the Conn as superseded by a peer takeover and
// then closes it. finish uses the takenOver flag to suppress
// the will (per MQTT-3.1.2.5 the Will Message is for ABNORMAL termination,
// not for server-side shutdown of a stale conn whose session has been
// adopted by a new owner). Safe to call from outside the conn's goroutine.
func (c *Conn) ShutdownForTakeover() {
	c.takenOver.Store(true)
	c.shutdown()
}

// SessionToken returns the per-conn session_token captured at takeover
// time. Used by the listener's takeover dispatch to ignore a late
// notification whose payload doesn't match this Conn's token.
func (c *Conn) SessionToken() uuid.UUID { return c.sessionToken }

// ClientID returns the resolved client identifier (empty before CONNECT).
func (c *Conn) ClientID() string { return c.clientID }

// gracefulClose is invoked at server shutdown: v5 clients are told the
// server is shutting down (0x8B) before the socket closes.
func (c *Conn) gracefulClose() { c.Disconnect(0x8B) }

// finish records the end of this connection's session: see
// Engine.endSession for what that involves. It runs once, when run
// returns, for every way a connection can end.
//
// Two cases do nothing. A Conn that never took ownership of a session row
// has nothing to end. A Conn superseded by a newer connection for the
// same client must not touch the row, which is the new connection's now,
// and its will is not published: the client did not go away, it moved
// (a deliberate reading of MQTT-3.1.2.5 for retained "offline" wills).
func (c *Conn) finish(cause error) {
	if c.sessionToken == uuid.Nil || c.takenOver.Load() {
		return
	}
	if c.gracefulRequested.Load() || errors.Is(cause, errClientDisconnect) {
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

	ctx, cancel := context.WithTimeout(context.Background(), endSessionTimeout)
	defer cancel()
	ended, err := c.eng.endSession(ctx, c.sessionEnd())
	switch {
	case err != nil:
		// The row still names this broker with no connection behind it;
		// endOrphanedSessions picks it up.
		c.eng.logger.Warn("end session", "client", c.clientID, "err", err)
	case !ended:
		c.eng.logger.Debug("session already taken over or ended", "client", c.clientID)
	}
}

// handleGracefulDisconnect implements MQTT-3.14 (v3.1.1) and v5 normal
// disconnect: the will is dropped (must not be sent) unless the v5 reason
// code is 0x04 ("Disconnect with Will Message"), the session may persist
// or be cleaned per cleanStart.
func (c *Conn) handleGracefulDisconnect(_ context.Context, pk *packets.Packet) error {
	// [MQTT-3.14.4-3]: v5 DISCONNECT with reason 0x04 means "publish the
	// will" — leave c.willTopic intact so finish publishes it. Every
	// other reason (0x00 success, 0x82 protocol error from server, etc.)
	// means "suppress the will". v3.1.1 has no reason codes; spec is
	// "suppress on graceful DISCONNECT", which is the default behavior.
	suppressWill := true
	if c.protocol == mqttwire.ProtocolMQTT5 && pk.ReasonCode == 0x04 {
		suppressWill = false
	}
	if suppressWill {
		c.willTopic = ""
	}
	// v5 [MQTT-3.14.2.2.2]: a DISCONNECT may override SessionExpiryInterval.
	// Per spec, the server treats the packet as a Protocol Error if the
	// original CONNECT had SessionExpiryInterval=0 (or was absent — spec
	// default is 0) and DISCONNECT supplies non-zero. Treat nil
	// (sessionExpiry property absent on CONNECT) the same as 0 so a
	// client can't sneak an extension by simply omitting the property.
	if c.protocol == mqttwire.ProtocolMQTT5 && pk.Properties.SessionExpiryIntervalFlag {
		v := pk.Properties.SessionExpiryInterval
		current := uint32(0)
		if c.sessionExpiry != nil {
			current = *c.sessionExpiry
		}
		invalidIncrease := current == 0 && v != 0
		if invalidIncrease {
			// [MQTT-3.14.2.2.2]: extending SessionExpiry from 0 is a
			// Protocol Error. Surface it as DISCONNECT 0x82 so the
			// client doesn't silently believe the extension stuck —
			// previously we just dropped the update with no signal.
			_ = c.write(&packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Disconnect},
				ReasonCode:  0x82, // Protocol Error
			})
		} else {
			c.sessionExpiry = &v
		}
	}
	c.gracefulRequested.Store(true)
	return errClientDisconnect
}

// handlePuback / handlePubrec / handlePubrel / handlePubcomp — receiver-side
// acknowledgement of QoS>0 deliveries this Pod sent.

func (c *Conn) handlePuback(ctx context.Context, pk *packets.Packet) error {
	ct, err := c.eng.pool.Exec(ctx,
		`DELETE FROM deliveries WHERE client_id=$1 AND packet_id=$2 AND qos=1`,
		c.clientID, pk.PacketID)
	if err != nil {
		return err
	}
	// Only release the outbound flow-control slot when we actually
	// completed a known QoS-1 delivery. A PUBACK for an unknown packet
	// id (bogus pid, replay, or a misbehaving v5 client) consumed no
	// slot — unconditionally calling returnInflight here would inflate
	// the outbound window past ReceiveMaximum on subsequent sends.
	// Spec-wise this is a Protocol Error and a v5 server SHOULD emit
	// DISCONNECT 0x82; that's owned by a separate change.
	if ct.RowsAffected() > 0 {
		c.returnInflight()
	}
	return nil
}

func (c *Conn) handlePubrec(ctx context.Context, pk *packets.Packet) error {
	// [MQTT-4.3.3 / 3.5.2.1] If the receiver returns PUBREC with a reason
	// code ≥ 0x80, the QoS-2 message is treated as completed: discard the
	// outbound delivery row, release the inflight slot, do NOT send PUBREL.
	if pk.ReasonCode >= 0x80 {
		ct, err := c.eng.pool.Exec(ctx,
			`DELETE FROM deliveries WHERE client_id=$1 AND packet_id=$2 AND qos=2`,
			c.clientID, pk.PacketID)
		// As in handlePuback: only a delivery we knew about held a slot.
		if err == nil && ct.RowsAffected() > 0 {
			c.returnInflight()
		}
		return err
	}
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
//
// [MQTT-3.6.2-1] PUBREL for an unknown packet id replies PUBCOMP with reason
// 0x92 (Packet Identifier not found) on v5; v3.1.1 has no reason field so we
// reply unconditionally.
func (c *Conn) handlePubrel(ctx context.Context, pk *packets.Packet) error {
	ct, err := c.eng.pool.Exec(ctx,
		`DELETE FROM inbound_qos2 WHERE client_id=$1 AND packet_id=$2`,
		c.clientID, pk.PacketID)
	if err != nil {
		return err
	}
	resp := &packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pubcomp},
		PacketID:    pk.PacketID,
	}
	if ct.RowsAffected() == 0 && c.protocol == mqttwire.ProtocolMQTT5 {
		resp.ReasonCode = 0x92 // Packet Identifier not found
	}
	werr := c.write(resp)
	// Only release the inflight slot when we actually completed a known
	// QoS-2 inbound; an unknown packet id consumed no slot.
	if ct.RowsAffected() > 0 {
		c.inboundInflight.Add(-1)
	}
	return werr
}

func (c *Conn) handlePubcomp(ctx context.Context, pk *packets.Packet) error {
	ct, err := c.eng.pool.Exec(ctx,
		`DELETE FROM deliveries WHERE client_id=$1 AND packet_id=$2 AND qos=2`,
		c.clientID, pk.PacketID)
	if err != nil {
		return err
	}
	// Same gating as handlePuback: a PUBCOMP for an unknown packet id
	// consumed no outbound slot; releasing one would inflate the window
	// past ReceiveMaximum.
	if ct.RowsAffected() > 0 {
		c.returnInflight()
	}
	return nil
}

func (c *Conn) handleUnsubscribe(ctx context.Context, pk *packets.Packet) error {
	tx, err := c.eng.beginTxTimed(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	codes := make([]byte, 0, len(pk.Filters))
	for _, f := range pk.Filters {
		// [MQTT-3.10.3-1]: malformed topic filter on UNSUBSCRIBE returns
		// reason 0x8F (Topic Filter invalid). Previously we'd run a
		// DELETE that couldn't possibly match and reported 0x11 (No
		// subscription existed), which is the wrong code for a wire-
		// level violation.
		if err := mqttwire.ValidateTopicFilter(f.Filter); err != nil {
			codes = append(codes, 0x8F)
			continue
		}
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
	if err := c.write(resp); err != nil {
		return err
	}
	if c.eng.metrics != nil {
		c.eng.metrics.UnsubscribesTotal.Inc()
	}
	return nil
}

// propsToJSON returns nil for an empty Properties so we don't store
// empty JSONB rows. The previous implementation marshalled twice on
// every PUBLISH carrying any v5 property — once to compare against
// "{}" (isEmptyProps) and once to actually capture the bytes. We keep
// the marshal-once form here: store the result, look at it, return
// nil-or-bytes accordingly.
func propsToJSON(p packets.Properties) ([]byte, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	if string(b) == emptyPropsJSON {
		return nil, nil
	}
	return b, nil
}

// emptyPropsJSON is how a Properties with nothing set marshals. The
// struct has no omitempty, so that is a few hundred bytes of zeroes
// rather than "{}"; storing it on every message (v3.1.1 included) and
// parsing it on every delivery is pure overhead.
var emptyPropsJSON = func() string {
	b, _ := json.Marshal(packets.Properties{})
	return string(b)
}()
