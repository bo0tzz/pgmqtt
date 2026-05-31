// Package mqtt wraps github.com/mochi-mqtt/server/v2/packets with framed
// read/write helpers for net.Conn-shaped transports. The packets subpackage
// itself is just a v3.1.1+v5 codec; we use it as such and ignore the rest of
// the mochi broker.
package mqtt

import (
	"bufio"
	"errors"
	"fmt"
	"io"

	"github.com/mochi-mqtt/server/v2/packets"
)

const (
	// MaxPacketSize is the absolute upper bound on packet size we will accept,
	// independent of any v5-negotiated limit. 256 MiB matches the MQTT v5
	// variable-byte-integer max (268435455 bytes). Smaller per-client limits
	// can be enforced by the engine.
	MaxPacketSize = 268435455

	// PreConnectMaxPacketSize is the cap applied BEFORE CONNECT has been
	// processed. A pre-auth client cannot be trusted to declare a 256 MiB
	// remaining length: with N concurrent sockets that would balloon broker
	// RAM by N*256 MiB before any allocation gating kicks in. 1 MiB is
	// generous for any reasonable CONNECT (username/password + properties +
	// will payload all fit comfortably) but small enough that even at the
	// MaxConnections cap the worst-case allocation is bounded.
	PreConnectMaxPacketSize = 1 << 20

	// ProtocolMQTT311 = 4 (per MQTT-3.1.2-2).
	ProtocolMQTT311 byte = 4
	// ProtocolMQTT5 = 5.
	ProtocolMQTT5 byte = 5
)

// ErrPacketTooLarge indicates the framed length exceeds the active size cap.
var ErrPacketTooLarge = errors.New("mqtt: packet too large")

// Reader reads MQTT packets from a buffered byte stream. The first packet must
// be CONNECT. After CONNECT is decoded, set ProtocolVersion so subsequent reads
// can decode v3.1.1- vs v5-specific layouts.
//
// The Reader applies a size cap on the variable-byte-integer remaining length
// before the body is allocated. The default cap is PreConnectMaxPacketSize
// (1 MiB) — bounded so an unauthenticated peer cannot announce a huge
// remaining length and force a multi-MiB allocation per socket. After CONNECT
// processing the engine raises this via SetMaxPacketSize to
// min(client_max_packet_size, server_max_packet_size).
type Reader struct {
	br              *bufio.Reader
	ProtocolVersion byte
	// maxPacketSize caps the value returned by DecodeLength before the body
	// allocation. 0 means "use PreConnectMaxPacketSize". Set via
	// SetMaxPacketSize after the engine has resolved the post-CONNECT cap.
	maxPacketSize uint32
	// LastConnectBody retains the raw v5 CONNECT body bytes after Read
	// returns a CONNECT packet. Needed by the engine to detect presence
	// of certain properties that mochi's Properties struct decodes
	// without setting a presence flag (notably MaximumPacketSize, where
	// value 0 collides with "absent" and is itself a Protocol Error per
	// [MQTT-3.1.2-25]). Cleared on every non-CONNECT Read so the slice
	// doesn't leak.
	LastConnectBody []byte
}

// NewReader wraps r. If r is already a *bufio.Reader it is reused.
func NewReader(r io.Reader) *Reader {
	if br, ok := r.(*bufio.Reader); ok {
		return &Reader{br: br}
	}
	return &Reader{br: bufio.NewReader(r)}
}

// SetMaxPacketSize sets the per-Reader inbound size cap, used after CONNECT
// processing. Caller resolves min(client_advertised, server_policy) and
// passes the result here. 0 reverts to PreConnectMaxPacketSize.
func (r *Reader) SetMaxPacketSize(n uint32) {
	r.maxPacketSize = n
}

// Read reads one packet. The Packet's ProtocolVersion field is populated from
// the Reader's setting (which must be set after a successful CONNECT decode).
func (r *Reader) Read() (packets.Packet, error) {
	var pk packets.Packet
	hb, err := r.br.ReadByte()
	if err != nil {
		return pk, err
	}
	if err := pk.FixedHeader.Decode(hb); err != nil {
		return pk, fmt.Errorf("decode fixed header: %w", err)
	}
	remaining, _, err := packets.DecodeLength(r.br)
	if err != nil {
		return pk, fmt.Errorf("decode length: %w", err)
	}
	if remaining > MaxPacketSize {
		return pk, ErrPacketTooLarge
	}
	// Apply the active inbound cap BEFORE allocating the body. Pre-CONNECT
	// (maxPacketSize == 0) we cap at PreConnectMaxPacketSize so a peer can't
	// blow our heap by declaring a multi-MiB remaining length on connection
	// open. Post-CONNECT the engine sets maxPacketSize to
	// min(client_max, server_max).
	cap := int(r.maxPacketSize)
	if cap == 0 {
		cap = PreConnectMaxPacketSize
	}
	if remaining > cap {
		return pk, ErrPacketTooLarge
	}
	pk.FixedHeader.Remaining = remaining
	pk.ProtocolVersion = r.ProtocolVersion

	body := make([]byte, remaining)
	if remaining > 0 {
		if _, err := io.ReadFull(r.br, body); err != nil {
			return pk, fmt.Errorf("read body: %w", err)
		}
	}
	if err := decodeBody(&pk, body); err != nil {
		return pk, err
	}
	// CONNECT carries its own protocol version; remember it for subsequent reads.
	if pk.FixedHeader.Type == packets.Connect {
		r.ProtocolVersion = pk.ProtocolVersion
		// Stash the raw body so the engine can detect property presence
		// where the decoded struct loses that information.
		r.LastConnectBody = body
	} else {
		r.LastConnectBody = nil
	}
	return pk, nil
}

func decodeBody(pk *packets.Packet, body []byte) error {
	// PublishDecode mutates body; copy it so we can safely keep references in
	// memory if the caller chooses.
	px := append([]byte(nil), body...)
	switch pk.FixedHeader.Type {
	case packets.Connect:
		return pk.ConnectDecode(px)
	case packets.Connack:
		return pk.ConnackDecode(px)
	case packets.Publish:
		return pk.PublishDecode(px)
	case packets.Puback:
		return pk.PubackDecode(px)
	case packets.Pubrec:
		return pk.PubrecDecode(px)
	case packets.Pubrel:
		return pk.PubrelDecode(px)
	case packets.Pubcomp:
		return pk.PubcompDecode(px)
	case packets.Subscribe:
		return pk.SubscribeDecode(px)
	case packets.Suback:
		return pk.SubackDecode(px)
	case packets.Unsubscribe:
		return pk.UnsubscribeDecode(px)
	case packets.Unsuback:
		return pk.UnsubackDecode(px)
	case packets.Pingreq, packets.Pingresp:
		return nil
	case packets.Disconnect:
		return pk.DisconnectDecode(px)
	case packets.Auth:
		return pk.AuthDecode(px)
	default:
		return fmt.Errorf("invalid packet type %d", pk.FixedHeader.Type)
	}
}

// Write encodes pk and writes it to w. ProtocolVersion on pk must be set
// (caller's responsibility — usually copied from the Reader after CONNECT).
func Write(w io.Writer, pk *packets.Packet) error {
	buf, err := Encode(pk)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// Encode returns the wire bytes for pk.
func Encode(pk *packets.Packet) ([]byte, error) {
	// mochi gates several v5 PUBLISH properties (ResponseTopic, CorrelationData,
	// ContentType, ResponseInformation, ServerReference) on Mods.AllowResponseInfo.
	// Per [MQTT-3.1.2-28] this only applies to CONNACK; on every other packet
	// the broker forwards client-set properties verbatim.
	if pk.FixedHeader.Type != packets.Connack {
		pk.Mods.AllowResponseInfo = true
	}

	var buf = newBuf()
	defer buf.Reset()

	switch pk.FixedHeader.Type {
	case packets.Connect:
		if err := pk.ConnectEncode(buf.b); err != nil {
			return nil, err
		}
	case packets.Connack:
		if err := pk.ConnackEncode(buf.b); err != nil {
			return nil, err
		}
	case packets.Publish:
		if err := pk.PublishEncode(buf.b); err != nil {
			return nil, err
		}
	case packets.Puback:
		if err := pk.PubackEncode(buf.b); err != nil {
			return nil, err
		}
	case packets.Pubrec:
		if err := pk.PubrecEncode(buf.b); err != nil {
			return nil, err
		}
	case packets.Pubrel:
		if err := pk.PubrelEncode(buf.b); err != nil {
			return nil, err
		}
	case packets.Pubcomp:
		if err := pk.PubcompEncode(buf.b); err != nil {
			return nil, err
		}
	case packets.Subscribe:
		if err := pk.SubscribeEncode(buf.b); err != nil {
			return nil, err
		}
	case packets.Suback:
		if err := pk.SubackEncode(buf.b); err != nil {
			return nil, err
		}
	case packets.Unsubscribe:
		if err := pk.UnsubscribeEncode(buf.b); err != nil {
			return nil, err
		}
	case packets.Unsuback:
		if err := pk.UnsubackEncode(buf.b); err != nil {
			return nil, err
		}
	case packets.Pingreq:
		if err := pk.PingreqEncode(buf.b); err != nil {
			return nil, err
		}
	case packets.Pingresp:
		if err := pk.PingrespEncode(buf.b); err != nil {
			return nil, err
		}
	case packets.Disconnect:
		if err := pk.DisconnectEncode(buf.b); err != nil {
			return nil, err
		}
	case packets.Auth:
		if err := pk.AuthEncode(buf.b); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("encode: invalid packet type %d", pk.FixedHeader.Type)
	}
	out := append([]byte(nil), buf.b.Bytes()...)
	return out, nil
}

// V5ConnectMaximumPacketSize parses the raw v5 CONNECT body and returns
// whether the MaximumPacketSize property was explicitly present and its
// value. mochi's Properties decoder collapses "absent" and "present with
// value 0" — both leave Properties.MaximumPacketSize == 0 — so we can't
// distinguish the legal "I have no opinion" case from the spec-illegal
// "Maximum Packet Size = 0" Protocol Error [MQTT-3.1.2-25] without
// re-walking the wire bytes.
//
// Returns (present, value, err). err is non-nil only on a malformed
// CONNECT body — in which case the caller should already be rejecting
// via mochi's ConnectDecode.
func V5ConnectMaximumPacketSize(body []byte) (bool, uint32, error) {
	// CONNECT body layout (v5, post-ConnectDecode):
	//   protocol name (2-byte len + N)
	//   protocol level (1 byte) — must be 5
	//   connect flags  (1 byte)
	//   keepalive      (2 bytes)
	//   properties     (varint length + N)
	//   payload        (clientID etc.)
	if len(body) < 2 {
		return false, 0, fmt.Errorf("connect body too short")
	}
	pnLen := int(body[0])<<8 | int(body[1])
	off := 2 + pnLen
	if len(body) < off+4 {
		return false, 0, fmt.Errorf("connect body truncated")
	}
	pv := body[off]
	off += 1 + 1 + 2 // protocol level, flags, keepalive
	if pv != 5 {
		return false, 0, nil // not v5 — no properties section
	}

	// Property block: varint length, then N bytes of (id, value...) pairs.
	propLen, n, err := decodeVarint(body[off:])
	if err != nil {
		return false, 0, err
	}
	off += n
	if propLen <= 0 {
		return false, 0, nil
	}
	end := off + propLen
	if end > len(body) {
		return false, 0, fmt.Errorf("property block extends past body")
	}

	// Walk the property block, returning early on PropMaximumPacketSize.
	// Stepping over each property requires knowing its wire shape; only
	// the v5 properties valid for CONNECT are handled here. An unknown
	// identifier is a malformed CONNECT (mochi would already have
	// rejected) — we return an error rather than risk a runaway scan.
	for off < end {
		id := body[off]
		off++
		switch id {
		case 0x11: // SessionExpiryInterval — uint32
			off += 4
		case 0x15: // AuthenticationMethod — UTF-8 string
			if off+2 > end {
				return false, 0, fmt.Errorf("string header truncated")
			}
			off += 2 + int(body[off])<<8 + int(body[off+1])
		case 0x16: // AuthenticationData — binary
			if off+2 > end {
				return false, 0, fmt.Errorf("binary header truncated")
			}
			off += 2 + int(body[off])<<8 + int(body[off+1])
		case 0x17: // RequestProblemInfo — byte
			off++
		case 0x19: // RequestResponseInfo — byte
			off++
		case 0x21: // ReceiveMaximum — uint16
			off += 2
		case 0x22: // TopicAliasMaximum — uint16
			off += 2
		case 0x26: // UserProperty — UTF-8 pair
			for i := 0; i < 2; i++ {
				if off+2 > end {
					return false, 0, fmt.Errorf("user property string truncated")
				}
				off += 2 + int(body[off])<<8 + int(body[off+1])
			}
		case 0x27: // MaximumPacketSize — uint32
			if off+4 > end {
				return false, 0, fmt.Errorf("mps value truncated")
			}
			v := uint32(body[off])<<24 |
				uint32(body[off+1])<<16 |
				uint32(body[off+2])<<8 |
				uint32(body[off+3])
			return true, v, nil
		default:
			return false, 0, fmt.Errorf("unknown CONNECT property 0x%X", id)
		}
		if off > end {
			return false, 0, fmt.Errorf("property block overflow")
		}
	}
	return false, 0, nil
}

// decodeVarint decodes a variable-byte integer (MQTT v5 property length
// prefix). Returns value, bytes consumed, and error.
func decodeVarint(b []byte) (int, int, error) {
	var v, mult, n int
	mult = 1
	for n = 0; n < 4; n++ {
		if n >= len(b) {
			return 0, 0, fmt.Errorf("varint truncated")
		}
		x := b[n]
		v += int(x&0x7F) * mult
		if x&0x80 == 0 {
			return v, n + 1, nil
		}
		mult *= 128
	}
	return 0, 0, fmt.Errorf("varint too long")
}
