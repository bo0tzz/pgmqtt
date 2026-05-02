package engine

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/gorilla/websocket"
)

// wsConnAdapter presents a gorilla websocket as a net.Conn so the same MQTT
// codec can be used over WebSocket. MQTT-over-WebSocket frames each MQTT
// packet (or any chunk) as a binary WebSocket frame; we concatenate frame
// payloads on read.
type wsConnAdapter struct {
	ws      *websocket.Conn
	readBuf []byte
	readErr error
	readPos int
}

func (w *wsConnAdapter) Close() error         { return w.ws.Close() }
func (w *wsConnAdapter) LocalAddr() net.Addr  { return w.ws.LocalAddr() }
func (w *wsConnAdapter) RemoteAddr() net.Addr { return w.ws.RemoteAddr() }
func (w *wsConnAdapter) SetDeadline(t time.Time) error {
	if err := w.ws.SetReadDeadline(t); err != nil {
		return err
	}
	return w.ws.SetWriteDeadline(t)
}
func (w *wsConnAdapter) SetReadDeadline(t time.Time) error  { return w.ws.SetReadDeadline(t) }
func (w *wsConnAdapter) SetWriteDeadline(t time.Time) error { return w.ws.SetWriteDeadline(t) }

func (w *wsConnAdapter) Read(p []byte) (int, error) {
	for w.readPos >= len(w.readBuf) {
		if w.readErr != nil {
			return 0, w.readErr
		}
		mt, data, err := w.ws.ReadMessage()
		if err != nil {
			w.readErr = err
			if isWSClose(err) {
				w.readErr = io.EOF
			}
			return 0, w.readErr
		}
		if mt != websocket.BinaryMessage {
			// MQTT-over-WS only uses binary frames; ignore others.
			continue
		}
		w.readBuf = data
		w.readPos = 0
	}
	n := copy(p, w.readBuf[w.readPos:])
	w.readPos += n
	return n, nil
}

func (w *wsConnAdapter) Write(p []byte) (int, error) {
	if err := w.ws.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func isWSClose(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	if _, ok := err.(*websocket.CloseError); ok {
		return true
	}
	return false
}
