package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// TestHandlerRendersExpectedSeries asserts the registry exports each
// pgmqtt_* series we promise plus a Go runtime metric. Catches accidental
// renames / removals in the New() registration list.
func TestHandlerRendersExpectedSeries(t *testing.T) {
	m := New()
	m.Connections.Set(3)
	m.PublishesTotal.WithLabelValues("1").Inc()
	m.QuotaExceededTotal.Inc()
	m.RateLimitedTotal.Add(2)
	m.DeliveriesInflight.WithLabelValues("queued").Set(0)
	m.ObservePublishStage("total", 5*time.Millisecond)
	m.ObservePublishStage("tx_commit", 30*time.Millisecond)

	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	want := []string{
		"pgmqtt_connections",
		"pgmqtt_publishes_total",
		"pgmqtt_deliveries_inflight",
		"pgmqtt_takeovers_total",
		"pgmqtt_dead_brokers_handled_total",
		"pgmqtt_sessions_expired_total",
		"pgmqtt_wills_fired_total",
		"pgmqtt_quota_exceeded_total",
		"pgmqtt_rate_limited_total",
		"pgmqtt_publish_seconds_bucket",
		"pgmqtt_publish_seconds_count",
		"pgmqtt_publish_seconds_sum",
		"go_goroutines",
	}
	for _, w := range want {
		if !strings.Contains(out, w) {
			t.Errorf("/metrics missing %q", w)
		}
	}
	if !strings.Contains(out, `pgmqtt_publishes_total{qos="1"} 1`) {
		t.Errorf("publishes_total{qos=1} not rendered: %s", out)
	}
}

// TestHandlerMergesExtraGatherers asserts AddGatherer surfaces metrics from
// a foreign registry (the controller-runtime use case) on /metrics. We use
// a stand-in registry rather than depending on controller-runtime here so
// the metrics package stays free of that import.
func TestHandlerMergesExtraGatherers(t *testing.T) {
	m := New()

	extra := prometheus.NewRegistry()
	foreign := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "controller_runtime_reconcile_total_test",
		Help: "Stand-in for a metric owned by an external registry.",
	})
	extra.MustRegister(foreign)
	foreign.Inc()
	m.AddGatherer(extra)

	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	// Both our pgmqtt_* series and the merged-in foreign metric must render.
	if !strings.Contains(out, "pgmqtt_connections") {
		t.Errorf("local series missing: %s", out)
	}
	if !strings.Contains(out, "controller_runtime_reconcile_total_test") {
		t.Errorf("merged gatherer series missing: %s", out)
	}
	if !strings.Contains(out, "controller_runtime_reconcile_total_test 1") {
		t.Errorf("merged gatherer value missing: %s", out)
	}
}

// TestServeBindsAndShutsDown verifies Serve listens on the supplied address,
// answers /metrics, and exits cleanly when the context is cancelled. The
// server lifecycle is what the operator actually depends on; the registry
// content is exercised in the test above.
func TestServeBindsAndShutsDown(t *testing.T) {
	m := New()

	// Pick a free port up-front; pass it to Serve and check the URL works.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- m.Serve(ctx, addr, nil) }()

	// Poll briefly for the server to bind.
	deadline := time.Now().Add(2 * time.Second)
	var resp *http.Response
	for time.Now().Before(deadline) {
		resp, err = http.Get("http://" + addr + "/metrics")
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("metrics never bound: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("metrics status: %d", resp.StatusCode)
	}

	// Cancel context; Serve should return ErrServerClosed-ish.
	cancel()
	select {
	case err := <-errCh:
		// http.Server.ListenAndServe returns http.ErrServerClosed after
		// Shutdown — Serve wraps that with no annotation so we expect
		// either ErrServerClosed or nil.
		if err != nil && err != http.ErrServerClosed {
			t.Fatalf("unexpected serve error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not exit after ctx cancel")
	}
}
