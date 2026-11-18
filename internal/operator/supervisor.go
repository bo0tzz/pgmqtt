package operator

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"time"
)

const (
	// defaultCRDPollInterval is how often a Pod without the User CRD looks
	// for it. The CRD may be installed after the broker (crds.install=false,
	// or a chart that applies it in a later step).
	defaultCRDPollInterval = 30 * time.Second
	defaultMinBackoff      = time.Second
	defaultMaxBackoff      = time.Minute
	// defaultStableAfter is how long a manager has to run before its exit
	// counts as a new problem rather than a continuation of the last one.
	defaultStableAfter = 5 * time.Minute
)

// supervisor owns the lifetime of the operator inside a broker process. It
// waits for the User CRD, runs a manager, and when that manager stops for
// any reason other than shutdown, goes round again.
type supervisor struct {
	logger *slog.Logger

	// crdServed reports whether the API server serves the User kind. An
	// error means the question went unanswered, not that the answer is no.
	crdServed func(ctx context.Context) (bool, error)
	// runManager builds a manager and blocks until it stops.
	runManager func(ctx context.Context) error

	// Zero values take the defaults above; tests shorten them.
	crdPollInterval time.Duration
	minBackoff      time.Duration
	maxBackoff      time.Duration
	stableAfter     time.Duration
}

// run returns when ctx is cancelled, and for no other reason.
func (s *supervisor) run(ctx context.Context) {
	crdPoll := orDefault(s.crdPollInterval, defaultCRDPollInterval)
	stableAfter := orDefault(s.stableAfter, defaultStableAfter)
	backoff := backoff{
		min: orDefault(s.minBackoff, defaultMinBackoff),
		max: orDefault(s.maxBackoff, defaultMaxBackoff),
	}

	crdMissing := false
	for ctx.Err() == nil {
		var served bool
		err := s.guard("CRD discovery", func() (err error) {
			served, err = s.crdServed(ctx)
			return err
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			discoveryErrorsTotal.Inc()
			wait := backoff.next()
			s.logger.Warn("cannot tell whether the User CRD is installed; Users are not being reconciled by this pod",
				"err", err, "retry_in", wait)
			sleep(ctx, wait)
			continue
		}
		if !served {
			// Logged once per stretch of absence: running without the CRD
			// is a supported setup and should not fill the log.
			if !crdMissing {
				crdMissing = true
				s.logger.Info("User CRD (users.pgmqtt.io) is not installed; user reconciler idle until it is",
					"hint", "install the User CRD to enable", "recheck_every", crdPoll)
			}
			backoff.reset()
			sleep(ctx, crdPoll)
			continue
		}
		if crdMissing {
			crdMissing = false
			s.logger.Info("User CRD (users.pgmqtt.io) is now installed")
		}

		managerRunning.Set(1)
		started := time.Now()
		err = s.guard("operator manager", func() error { return s.runManager(ctx) })
		managerRunning.Set(0)
		if ctx.Err() != nil {
			return
		}

		ranFor := time.Since(started)
		if ranFor >= stableAfter {
			backoff.reset()
		}
		managerRestartsTotal.Inc()
		wait := backoff.next()
		s.logger.Error("operator manager stopped; Users are not being reconciled by this pod until it restarts",
			"err", err, "ran_for", ranFor.Round(time.Millisecond), "restart_in", wait)
		sleep(ctx, wait)
	}
}

// guard runs f and turns a panic on its goroutine into an error, so that a
// bug in building or starting a manager costs a restart of the manager
// rather than of the broker. Panics in goroutines the manager starts are
// out of reach here; controller-runtime recovers those in reconcilers.
func (s *supervisor) guard(what string, f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("operator panic", "in", what, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("panic in %s: %v", what, r)
		}
	}()
	return f()
}

func orDefault(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// sleep waits for d or until ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// backoff doubles from min to max. Up to a fifth is added at random so
// that broker Pods which lost the API server at the same moment do not
// all come back at the same moment.
type backoff struct {
	min, max time.Duration
	current  time.Duration
}

func (b *backoff) next() time.Duration {
	if b.current < b.min {
		b.current = b.min
	}
	d := b.current
	b.current = min(b.current*2, b.max)
	return d + rand.N(d/5+1)
}

func (b *backoff) reset() { b.current = 0 }
