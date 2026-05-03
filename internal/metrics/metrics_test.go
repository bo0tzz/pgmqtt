package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
