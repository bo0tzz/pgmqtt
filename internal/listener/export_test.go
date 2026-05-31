package listener

// Test-only hooks bridging unexported package state to the external
// listener_test package. Production callers never see these.

// SetOSExitForTest installs a stub for osExit and returns a restore
// func. The default osExit is os.Exit; tests swap it out so that
// "listener decided to crash the pod" can be observed without actually
// killing the test process.
func SetOSExitForTest(f func(int)) (restore func()) {
	prev := osExit
	osExit = f
	return func() { osExit = prev }
}

// SetURLForTest swaps the listener's reconnect URL. Used by Stop()-
// during-reconnect tests to force the inner reconnect loop to fail
// (e.g. swap to an unreachable address) so the test can exercise the
// "Stop() racing reconnect" code path.
func (l *Listener) SetURLForTest(url string) {
	l.mu.Lock()
	l.url = url
	l.mu.Unlock()
}

// ReconnectInitialBackoffForTest is the package-private initial backoff
// constant; tests read it to scale their deadlines.
const ReconnectInitialBackoffForTest = reconnectInitialBackoff

// ReconnectMaxAttemptsForTest exposes reconnectMaxAttempts for tests.
const ReconnectMaxAttemptsForTest = reconnectMaxAttempts
