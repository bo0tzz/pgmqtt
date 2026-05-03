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
	dto "github.com/prometheus/client_model/go"
)

// Metrics bundles all pgmqtt_* collectors + the registry that hands them to
// promhttp. One Metrics is shared across the engine, listener, janitor.
type Metrics struct {
	Reg *prometheus.Registry

	// extraGatherers are additional Prometheus gatherers merged into the
	// /metrics response alongside Reg. Used to surface metrics that own
	// their own registry — notably controller-runtime's metrics.Registry,
	// which is package-global and can't be redirected at our Reg.
	extraGatherers []prometheus.Gatherer

	Connections        prometheus.Gauge
	PublishesTotal     *prometheus.CounterVec
	DeliveriesInflight *prometheus.GaugeVec
	TakeoversTotal     prometheus.Counter
	DeadBrokersTotal   prometheus.Counter
	SessionsExpired    prometheus.Counter
	WillsFiredTotal    prometheus.Counter
	QuotaExceededTotal prometheus.Counter
	RateLimitedTotal   prometheus.Counter

	// PublishStageSeconds attributes time across the QoS-1/QoS-2 inbound
	// publisher path. Exists so an operator can answer "where does PUBACK
	// latency come from?" without adding pg_stat_statements correlation.
	// Stages: total, qos2_dedup, retain, tx_begin, mqtt_publish_query,
	// tx_commit, notify, response_write (PUBACK or PUBREC).
	PublishStageSeconds *prometheus.HistogramVec
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
		PublishStageSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "pgmqtt_publish_seconds",
			Help: "Time spent in each stage of the inbound PUBLISH path. " +
				"Stages: total, qos2_dedup, retain, tx_begin, mqtt_publish_query, tx_commit, notify, response_write.",
			Buckets: []float64{
				.0001, .0002, .0005, .001, .002, .005,
				.01, .02, .05, .1, .25, .5, 1, 2.5, 5,
			},
		}, []string{"stage"}),
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
		m.PublishStageSeconds,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// ObservePublishStage records a duration sample for one stage of the publish
// path. Safe to call when m is nil — the engine constructs metrics on demand
// and unit tests run without one.
func (m *Metrics) ObservePublishStage(stage string, d time.Duration) {
	if m == nil {
		return
	}
	m.PublishStageSeconds.WithLabelValues(stage).Observe(d.Seconds())
}

// RegisterPgxPool registers a collector that reports pgxpool stats every
// scrape: total/idle/in-use connections plus acquire latency.
func (m *Metrics) RegisterPgxPool(pool *pgxpool.Pool) {
	c := newPoolCollector(pool)
	m.Reg.MustRegister(c)
}

// AddGatherer attaches an additional prometheus.Gatherer whose metrics will
// be merged into the /metrics response. Used to surface metrics owned by
// third-party packages that bind to their own registry — e.g. controller-
// runtime, whose package-global metrics.Registry cannot be redirected.
//
// Call before Handler / Serve. Safe to call with a nil gatherer (no-op).
func (m *Metrics) AddGatherer(g prometheus.Gatherer) {
	if m == nil || g == nil {
		return
	}
	m.extraGatherers = append(m.extraGatherers, g)
}

// Handler returns an http.Handler serving /metrics from this registry plus
// any extra gatherers registered via AddGatherer. Use with the metrics-
// listener helper in cmd/pgmqttd.
//
// Extra gatherers may carry metric families whose names collide with our
// own (notably go_* and process_* — controller-runtime's metrics.Registry
// registers its own copies). dedupeGatherer drops the duplicates from the
// extras side so prometheus.Gatherers doesn't fail the whole scrape.
func (m *Metrics) Handler() http.Handler {
	g := &dedupeGatherer{primary: m.Reg, extras: m.extraGatherers}
	return promhttp.HandlerFor(g, promhttp.HandlerOpts{Registry: m.Reg})
}

// dedupeGatherer merges metric families from primary + extras, taking the
// primary's family when names collide. This keeps go_* / process_* coming
// from our registry (where we registered them) rather than from any extra
// registry that also shipped them.
type dedupeGatherer struct {
	primary prometheus.Gatherer
	extras  []prometheus.Gatherer
}

func (d *dedupeGatherer) Gather() ([]*dto.MetricFamily, error) {
	out, err := d.primary.Gather()
	if err != nil {
		return out, err
	}
	seen := make(map[string]struct{}, len(out))
	for _, mf := range out {
		seen[mf.GetName()] = struct{}{}
	}
	for _, g := range d.extras {
		mfs, err := g.Gather()
		if err != nil {
			return out, err
		}
		for _, mf := range mfs {
			if _, dup := seen[mf.GetName()]; dup {
				continue
			}
			seen[mf.GetName()] = struct{}{}
			out = append(out, mf)
		}
	}
	return out, nil
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
