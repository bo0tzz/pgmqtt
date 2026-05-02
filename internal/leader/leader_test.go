package leader_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/bo0tzz/pgmqtt/internal/db/dbtest"
	"github.com/bo0tzz/pgmqtt/internal/leader"
)

func warnLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func TestSingleLeader(t *testing.T) {
	t.Parallel()
	url := dbtest.FreshURL(t)
	ctx := context.Background()

	a, err := leader.Start(ctx, url, warnLogger())
	if err != nil {
		t.Fatalf("a: %v", err)
	}
	defer a.Stop()

	select {
	case <-a.Acquired():
	case <-time.After(3 * time.Second):
		t.Fatal("a never became leader")
	}
	if !a.IsLeader() {
		t.Fatal("a should be leader")
	}

	b, err := leader.Start(ctx, url, warnLogger())
	if err != nil {
		t.Fatalf("b: %v", err)
	}
	defer b.Stop()

	// b must NOT acquire while a is leader.
	select {
	case <-b.Acquired():
		t.Fatal("b became leader while a still holds the lock")
	case <-time.After(500 * time.Millisecond):
	}

	// Step a down — b should acquire.
	a.Stop()
	select {
	case <-b.Acquired():
	case <-time.After(3 * time.Second):
		t.Fatal("b never became leader after a stepped down")
	}
}
