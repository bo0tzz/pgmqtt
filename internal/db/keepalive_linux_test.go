//go:build linux

package db

import (
	"context"
	"net"
	"syscall"
	"testing"
)

// TestKeepaliveDialerSetsTheWholeSchedule reads the keepalive options back
// off a dialled socket. Setting only Dialer.KeepAlive, as this used to,
// leaves the probe interval and count at Go's defaults (15 s x 9), and a
// silently dead Postgres then takes minutes, not seconds, to notice.
func TestKeepaliveDialerSetsTheWholeSchedule(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	c, err := KeepaliveDialer(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	raw, err := c.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatalf("syscall conn: %v", err)
	}
	got := map[string]int{}
	ctlErr := raw.Control(func(fd uintptr) {
		for name, opt := range map[string][2]int{
			"SO_KEEPALIVE":  {syscall.SOL_SOCKET, syscall.SO_KEEPALIVE},
			"TCP_KEEPIDLE":  {syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE},
			"TCP_KEEPINTVL": {syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL},
			"TCP_KEEPCNT":   {syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT},
		} {
			v, err := syscall.GetsockoptInt(int(fd), opt[0], opt[1])
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}
			got[name] = v
		}
	})
	if ctlErr != nil {
		t.Fatalf("control: %v", ctlErr)
	}
	want := map[string]int{
		"SO_KEEPALIVE":  1,
		"TCP_KEEPIDLE":  int(KeepaliveIdle.Seconds()),
		"TCP_KEEPINTVL": int(KeepaliveInterval.Seconds()),
		"TCP_KEEPCNT":   KeepaliveCount,
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s = %d, want %d", k, got[k], w)
		}
	}
}
