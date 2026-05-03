package operator_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/bo0tzz/pgmqtt/internal/metrics"
)

// TestPgmqttMetricsHandlerSurfacesControllerRuntime asserts that the
// production wiring (metrics.AddGatherer(ctrlmetrics.Registry) in
// cmd/pgmqttd/main.go) makes controller-runtime's collectors visible on
// pgmqtt's /metrics endpoint.
//
// Controller-runtime's reconcile_total / reconcile_time / etc. are
// CounterVec/HistogramVec values that don't appear in Gather() output
// until labelled instances are observed — and observation only happens
// inside the controller-runtime manager loop. Rather than spinning up a
// full manager (slow, requires kubeconfig), this test registers a
// controller_runtime_*-named counter directly on ctrlmetrics.Registry,
// then asserts metrics.Handler renders it. This proves the gatherer
// wire-up works end-to-end; the actual reconcile metrics will appear
// in production by the same path.
func TestPgmqttMetricsHandlerSurfacesControllerRuntime(t *testing.T) {
	// Probe must have a unique name across parallel tests since
	// ctrlmetrics.Registry is package-global.
	probe := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "controller_runtime_pgmqtt_wire_probe_total",
		Help: "Test-only counter used to verify controller-runtime metrics surface on pgmqtt /metrics.",
	})
	if err := ctrlmetrics.Registry.Register(probe); err != nil {
		t.Fatalf("register probe on ctrlmetrics.Registry: %v", err)
	}
	t.Cleanup(func() { ctrlmetrics.Registry.Unregister(probe) })
	probe.Inc()

	// Recreate the production wiring: a fresh pgmqtt registry plus the
	// controller-runtime registry attached as an extra gatherer.
	m := metrics.New()
	m.AddGatherer(ctrlmetrics.Registry)

	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	// pgmqtt's own metrics still render.
	if !strings.Contains(out, "pgmqtt_connections") {
		t.Errorf("pgmqtt_connections missing from /metrics:\n%s", out)
	}
	// And the controller-runtime registry's collectors are merged in.
	if !strings.Contains(out, "controller_runtime_pgmqtt_wire_probe_total 1") {
		t.Errorf("controller-runtime probe not rendered on /metrics:\n%s", out)
	}
}
