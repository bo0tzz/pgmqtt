package engine_test

import (
	"bytes"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/bo0tzz/pgmqtt/internal/config"
	"github.com/bo0tzz/pgmqtt/internal/engine/enginetest"
	mqttwire "github.com/bo0tzz/pgmqtt/internal/mqtt"
)

func wsHarness(t *testing.T, origins ...string) *enginetest.Harness {
	t.Helper()
	return enginetest.NewHarnessConfig(t, func(c *config.Config) {
		c.WSAddr = "127.0.0.1:0"
		c.WSAllowedOrigins = origins
	}, nil)
}

func dialWS(t *testing.T, h *enginetest.Harness, origin string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	header := http.Header{}
	if origin != "" {
		header.Set("Origin", origin)
	}
	d := websocket.Dialer{Subprotocols: []string{"mqtt"}, HandshakeTimeout: 3 * time.Second}
	return d.Dial("ws://"+h.Engine.WSAddr().String()+"/mqtt", header)
}

func wsSend(t *testing.T, ws *websocket.Conn, pk *packets.Packet) {
	t.Helper()
	buf, err := mqttwire.Encode(pk)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := ws.WriteMessage(websocket.BinaryMessage, buf); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func wsRead(ws *websocket.Conn, timeout time.Duration) (*packets.Packet, error) {
	_ = ws.SetReadDeadline(time.Now().Add(timeout))
	_, data, err := ws.ReadMessage()
	if err != nil {
		return nil, err
	}
	r := mqttwire.NewReader(bytes.NewReader(data))
	r.ProtocolVersion = mqttwire.ProtocolMQTT5
	pk, err := r.Read()
	return &pk, err
}

func wsConnect(t *testing.T, ws *websocket.Conn, clientID string) {
	t.Helper()
	wsSend(t, ws, &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: mqttwire.ProtocolMQTT5,
		Connect: packets.ConnectParams{
			ProtocolName: []byte("MQTT"), Clean: true, Keepalive: 60, ClientIdentifier: clientID,
			Username: []byte("test"), UsernameFlag: true, Password: []byte("test"), PasswordFlag: true,
		},
	})
	pk, err := wsRead(ws, 3*time.Second)
	if err != nil || pk.FixedHeader.Type != packets.Connack || pk.ReasonCode != 0 {
		t.Fatalf("CONNACK over WebSocket: %+v %v", pk, err)
	}
}

// TestWebSocketPreConnectFrameLimit: before CONNECT a TCP client may send
// at most 1 MiB, and so may a WebSocket one. The WebSocket read limit was
// only set from the server's packet-size policy (16 MiB by default, none
// at 0), and gorilla buffers a whole frame before the MQTT reader sees
// any of it: a peer that had not even authenticated could make the
// broker wait for, and allocate, as much as it announced.
func TestWebSocketPreConnectFrameLimit(t *testing.T) {
	t.Parallel()
	h := wsHarness(t)

	ws, _, err := dialWS(t, h, "")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer ws.Close()
	// A masked binary frame announcing 2 MiB, and none of the payload.
	raw := ws.UnderlyingConn()
	header := []byte{0x82, 0x80 | 127, 0, 0, 0, 0, 0, 0x20, 0, 0, 1, 2, 3, 4}
	if _, err := raw.Write(header); err != nil {
		t.Fatalf("write frame header: %v", err)
	}
	_ = raw.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	for {
		if _, err := raw.Read(buf); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatal("broker is waiting for a 2 MiB frame from a peer that has not sent CONNECT")
			}
			break // closed, as it should be
		}
	}

	// Once connected, the server's own limit applies.
	ws2, _, err := dialWS(t, h, "")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer ws2.Close()
	wsConnect(t, ws2, "ws-big")
	wsSend(t, ws2, &packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName:   "ws/big", Payload: make([]byte, 2<<20), PacketID: 1,
	})
	ack, err := wsRead(ws2, 5*time.Second)
	if err != nil || ack.FixedHeader.Type != packets.Puback {
		t.Fatalf("2 MiB PUBLISH after CONNECT: %+v %v", ack, err)
	}
}

// TestWebSocketOriginAllowList: the allow-list is there to stop a
// browser being used against the broker from another site. Only browsers
// send Origin, so a client that sends none is not what it is for, and was
// rejected whenever the list was set.
func TestWebSocketOriginAllowList(t *testing.T) {
	t.Parallel()
	h := wsHarness(t, "https://dashboard.example")

	for _, tc := range []struct {
		origin string
		ok     bool
	}{
		{"", true},
		{"https://dashboard.example", true},
		{"https://evil.example", false},
	} {
		ws, resp, err := dialWS(t, h, tc.origin)
		if ws != nil {
			ws.Close()
		}
		if tc.ok && err != nil {
			t.Errorf("origin %q: rejected: %v", tc.origin, err)
		}
		if !tc.ok && (err == nil || resp == nil || resp.StatusCode != http.StatusForbidden) {
			t.Errorf("origin %q: want 403, got err=%v", tc.origin, err)
		}
	}
}
