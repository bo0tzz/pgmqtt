// Package metrics owns the Prometheus registry and counters/gauges for
// pgmqttd. Wiring:
//
//   * NewRegistry returns a *prometheus.Registry with all pgmqtt_* metrics
//     pre-registered. Helm's metricsPort scrapes this via promhttp.Handler.
//   * Counters/gauges are exposed as package-level *prometheus.CounterVec
//     etc., with helpers (e.g. ObservePublish) so callers don't need to
//     name labels in-line.
//   * The pgxpool stats collector is registered alongside the static set so
//     /metrics also exposes connection-pool depth + acquire latency.
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics bundles all pgmqtt_* collectors + the registry that hands them to
// promhttp. One Metrics is shared across the engine, listener, janitor.
type Metrics struct {
	Reg *prometheus.Registry

	Connections        prometheus.Gauge
	PublishesTotal     *prometheus.CounterVec
	DeliveriesInflight *prometheus.GaugeVec
	TakeoversTotal     prometheus.Counter
	DeadBrokersTotal   prometheus.Counter
	SessionsExpired    prometheus.Counter
	WillsFiredTotal    prometheus.Counter
	QuotaExceededTotal prometheus.Counter
	RateLimitedTotal   prometheus.Counter
}

// New creates and registers a fresh Metrics. Call once per process.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Reg: reg,
		Connections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgmqtt_connections",
			Help: "Currently-open MQTT client connections accepted by this Pod (post-CONNACK).",
		}),
		PublishesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgmqtt_publishes_total",
			Help: "Inbound MQTT PUBLISH packets accepted by this Pod, labelled by QoS.",
		}, []string{"qos"}),
		DeliveriesInflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgmqtt_deliveries_inflight",
			Help: "Rows in the deliveries table by state (0=queued, 1=in-flight QoS≥1, 2=awaiting PUBCOMP).",
		}, []string{"state"}),
		TakeoversTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgmqtt_takeovers_total",
			Help: "Number of times an existing local connection was displaced by a new CONNECT for the same client_id.",
		}),
		DeadBrokersTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgmqtt_dead_brokers_handled_total",
			Help: "Total dead-broker scans handled by this Pod's janitor (each event clears one or more sessions).",
		}),
		SessionsExpired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgmqtt_sessions_expired_total",
			Help: "Total v5 sessions reaped due to SessionExpiryInterval.",
		}),
		WillsFiredTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgmqtt_wills_fired_total",
			Help: "Total wills fired (delayed or immediate, counted on emit).",
		}),
		QuotaExceededTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgmqtt_quota_exceeded_total",
			Help: "Total slow-subscriber DISCONNECT 0x97 events emitted by this Pod.",
		}),
		RateLimitedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgmqtt_rate_limited_total",
			Help: "Total inbound rate-limit DISCONNECT 0x96 events emitted by this Pod.",
		}),
	}

	reg.MustRegister(
		m.Connections,
		m.PublishesTotal,
		m.DeliveriesInflight,
		m.TakeoversTotal,
		m.DeadBrokersTotal,
		m.SessionsExpired,
		m.WillsFiredTotal,
		m.QuotaExceededTotal,
		m.RateLimitedTotal,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// RegisterPgxPool registers a collector that reports pgxpool stats every
// scrape: total/idle/in-use connections plus acquire latency.
func (m *Metrics) RegisterPgxPool(pool *pgxpool.Pool) {
	c := newPoolCollector(pool)
	m.Reg.MustRegister(c)
}

// Handler returns an http.Handler serving /metrics from this registry. Use
// with the metrics-listener helper in cmd/pgmqttd.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Reg, promhttp.HandlerOpts{Registry: m.Reg})
}

// Serve starts an HTTP server on addr with /metrics handled. Blocks until
// ctx is cancelled. Logs are intentionally minimal — the caller wraps.
func (m *Metrics) Serve(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	return srv.ListenAndServe()
}
