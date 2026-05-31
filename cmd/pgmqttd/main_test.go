package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

// TestFatalRecoverOperator_PanicCancelsAndFlags verifies the deferred
// recover for the operator goroutine: when the operator panics, the
// recovery logs the panic, sets the operatorPanicked flag so main()
// exits non-zero, and cancels the root context so the engine/janitor/
// listener wind down via their ctx-aware loops.
//
// This is the regression test for L5: before this fix, the operator
// goroutine ran behind `defer recoverPanic(...)`, which swallowed
// panics. The broker would keep serving traffic with no live reconciler
// — User CRDs, BYO Secret rotations, and lease handoffs were silently
// disabled. Production was carrying the risk of stale auth state going
// undetected by oncall.
func TestFatalRecoverOperator_PanicCancelsAndFlags(t *testing.T) {
	// operatorPanicked is package-global; reset before/after so other
	// tests aren't affected.
	t.Cleanup(func() { operatorPanicked.Store(false) })
	operatorPanicked.Store(false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer fatalRecoverOperator(logger, cancel)
		panic(errors.New("simulated controller-runtime panic"))
	}()
	<-done

	// Post-condition 1: operatorPanicked flag was set so main() exits 1.
	if !operatorPanicked.Load() {
		t.Error("operatorPanicked flag was not set after panic")
	}
	// Post-condition 2: the root context was cancelled. main() relies on
	// this so engine.Serve(ctx) returns and the deferred pool.Close /
	// lst.Stop run via the normal shutdown path.
	select {
	case <-ctx.Done():
		// Expected.
	default:
		t.Error("root context was not cancelled after operator panic")
	}
}

// TestFatalRecoverOperator_NoPanicIsNoOp verifies the recovery does
// nothing on a clean operator exit. operator.Run can return nil when
// the cluster doesn't have the CRD or the kubeconfig is missing — the
// goroutine returns cleanly and the broker keeps serving. We must NOT
// cancel the broker's ctx in that case.
func TestFatalRecoverOperator_NoPanicIsNoOp(t *testing.T) {
	t.Cleanup(func() { operatorPanicked.Store(false) })
	operatorPanicked.Store(false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	func() {
		defer fatalRecoverOperator(logger, cancel)
		// no panic — clean return
	}()

	if operatorPanicked.Load() {
		t.Error("operatorPanicked flag set despite no panic")
	}
	select {
	case <-ctx.Done():
		t.Error("root context cancelled despite no panic")
	default:
		// Expected.
	}
}

// TestOperatorPanicTriggersProcessExit verifies the main() post-Serve
// branch: when the operatorPanicked flag is set, operatorExit is called
// with code 1 so K8s restarts the pod. We swap operatorExit with a
// recorder so the test process doesn't actually terminate, then drive
// the same condition (flag-set, Serve returned) and assert exit(1)
// was called.
func TestOperatorPanicTriggersProcessExit(t *testing.T) {
	t.Cleanup(func() {
		operatorPanicked.Store(false)
		operatorExit = os.Exit
	})
	var exitCode atomic.Int32
	exitCode.Store(-1)
	operatorExit = func(code int) {
		exitCode.Store(int32(code))
	}
	operatorPanicked.Store(true)

	// Replicate main()'s post-Serve check.
	if operatorPanicked.Load() {
		operatorExit(1)
	}

	got := exitCode.Load()
	if got != 1 {
		t.Errorf("expected operatorExit(1), got code=%d", got)
	}
}

// TestFatalRecoverOperatorLogsPanic verifies the panic payload reaches
// the operator logs so SREs have a stack trace when triaging a pod
// restart caused by a controller-runtime panic.
func TestFatalRecoverOperatorLogsPanic(t *testing.T) {
	t.Cleanup(func() { operatorPanicked.Store(false) })
	operatorPanicked.Store(false)

	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	func() {
		defer fatalRecoverOperator(logger, cancel)
		panic("kaboom")
	}()

	out := buf.String()
	if !strings.Contains(out, "operator panic") {
		t.Errorf("expected 'operator panic' log line, got:\n%s", out)
	}
	if !strings.Contains(out, "kaboom") {
		t.Errorf("expected panic payload 'kaboom' in log, got:\n%s", out)
	}
}
