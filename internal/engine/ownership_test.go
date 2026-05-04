package engine

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bo0tzz/pgmqtt/internal/config"
	"github.com/bo0tzz/pgmqtt/internal/db/dbtest"
)

// TestSweepOrphanedSocketsClosesNonSelfBroker seeds two sessions: one this
// pod owns (broker_id == self), and one a peer pod owns (broker_id !=
// self). Registers a *Conn for each in the local conns map, runs
// sweepOrphanedSockets, and asserts only the foreign-owned conn is
// shut down. Guards against the audit S1 fix regressing.
func TestSweepOrphanedSocketsClosesNonSelfBroker(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	cfg := &config.Config{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := New(context.Background(), cfg, pool, logger)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	self := uuid.New()
	peer := uuid.New()
	eng.SetBrokerID(self)

	if _, err := pool.Exec(context.Background(), `
		INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start) VALUES
		  ('mine',  $1, true, 5, false),
		  ('theirs',$2, true, 5, false)
	`, self, peer); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Build minimal *Conn shells with a closable net.Conn so Shutdown
	// observably closes them. closed flag is checked post-sweep.
	mineNC := &fakeNetConn{}
	theirsNC := &fakeNetConn{}
	mine := &Conn{eng: eng, clientID: "mine", nc: mineNC, closed: make(chan struct{})}
	theirs := &Conn{eng: eng, clientID: "theirs", nc: theirsNC, closed: make(chan struct{})}
	eng.registerConn("mine", mine)
	eng.registerConn("theirs", theirs)

	if err := eng.sweepOrphanedSockets(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	// "mine" must NOT be closed — broker_id matches self.
	if mineNC.closeCount.Load() != 0 {
		t.Errorf("mine conn was closed; broker_id matches self")
	}
	// "theirs" MUST have been closed — broker_id is a foreign UUID.
	if theirsNC.closeCount.Load() == 0 {
		t.Errorf("theirs conn was not closed; broker_id is foreign")
	}
}

// TestSweepOrphanedSocketsNoBrokerID short-circuits when the listener
// hasn't assigned a broker UUID yet (uuid.Nil). Avoids closing every conn
// during a startup window.
func TestSweepOrphanedSocketsNoBrokerID(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	cfg := &config.Config{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := New(context.Background(), cfg, pool, logger)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	// Note: BrokerID intentionally not set → uuid.Nil.

	if _, err := pool.Exec(context.Background(), `
		INSERT INTO sessions(client_id, broker_id, connected, protocol_version, clean_start)
		VALUES ('client', $1, true, 5, false)
	`, uuid.New()); err != nil {
		t.Fatalf("seed: %v", err)
	}

	nc := &fakeNetConn{}
	c := &Conn{eng: eng, clientID: "client", nc: nc, closed: make(chan struct{})}
	eng.registerConn("client", c)

	if err := eng.sweepOrphanedSockets(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if nc.closeCount.Load() != 0 {
		t.Errorf("conn was closed despite uuid.Nil self — startup-window guard regressed")
	}
}

// fakeNetConn is a no-op net.Conn that just records Close() calls.
// Shutdown closes c.nc, which is sufficient for the sweep to "close" the
// conn for test purposes (Shutdown also closes c.closed and unregisters).
type fakeNetConn struct {
	closeCount atomic.Int32
}

func (f *fakeNetConn) Read(_ []byte) (int, error)         { return 0, nil }
func (f *fakeNetConn) Write(_ []byte) (int, error)        { return 0, nil }
func (f *fakeNetConn) Close() error                       { f.closeCount.Add(1); return nil }
func (f *fakeNetConn) LocalAddr() net.Addr                { return nopAddr{} }
func (f *fakeNetConn) RemoteAddr() net.Addr               { return nopAddr{} }
func (f *fakeNetConn) SetDeadline(_ time.Time) error      { return nil }
func (f *fakeNetConn) SetReadDeadline(_ time.Time) error  { return nil }
func (f *fakeNetConn) SetWriteDeadline(_ time.Time) error { return nil }

type nopAddr struct{}

func (nopAddr) Network() string { return "fake" }
func (nopAddr) String() string  { return "fake" }
