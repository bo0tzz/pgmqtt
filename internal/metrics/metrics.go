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

	// AuthFailuresTotal counts credential rejections at CONNECT.
	// Reasons: bad_credentials (CONNACK 0x86), not_authorized (0x87),
	// banned (0x88), bad_auth_method (0x8C — enhanced auth attempted).
	AuthFailuresTotal *prometheus.CounterVec

	// SubscribesTotal / UnsubscribesTotal — symmetric to PublishesTotal,
	// used to bound topic-churn driven load.
	SubscribesTotal   prometheus.Counter
	UnsubscribesTotal prometheus.Counter

	// JanitorTickSeconds attributes time across the per-tick sub-jobs of
	// janitor.Tick (dead_brokers, fire_due_wills, expire_sessions,
	// expire_retained, sweep_inbound_qos2, sweep_orphan_deliveries,
	// refresh_deliveries_gauge, refresh_subscriptions_gauge,
	// refresh_sessions_gauge, refresh_retained_gauge,
	// refresh_inbound_qos2_gauge, sweep_orphan_messages). One sub-job
	// blowing past the 1s tick interval would otherwise be invisible —
	// they all currently swallow into a single Warn log.
	JanitorTickSeconds *prometheus.HistogramVec

	// JanitorErrorsTotal counts per-sub-job errors so an operator can
	// alert on a specific job degrading without parsing logs.
	JanitorErrorsTotal *prometheus.CounterVec

	// Subscriptions / Sessions / RetainedCount / InboundQoS2Pending —
	// gauges refreshed by janitor each tick. Cardinality detection,
	// retained-flood detection, QoS-2 stuck detection.
	Subscriptions     prometheus.Gauge
	Sessions          prometheus.Gauge
	RetainedCount     prometheus.Gauge
	InboundQoS2Pending prometheus.Gauge

	// WillsNotifyFailedTotal counts cross-pod will-publish failures
	// where the post-commit Notifier hook returned an error. In
	// production this is a no-op so the counter is normally zero;
	// surfaces the test-harness InProcessNotifier failure mode.
	WillsNotifyFailedTotal prometheus.Counter

	// RetainedDispatchFailedTotal counts retained-message dispatch
	// failures during SUBSCRIBE that arrived after SUBACK was already
	// sent. Operator-visible since the client cannot otherwise
	// distinguish "no retained existed" from "retained existed but
	// failed to deliver."
	RetainedDispatchFailedTotal prometheus.Counter

	// DeliveryStageSeconds attributes time across the broker→subscriber
	// fanout path. Stages:
	//   total       — full Deliver() call duration (per NOTIFY)
	//   scan        — the SELECT in Deliver() (matching deliveries lookup)
	//   alloc       — per-row packet ID allocation + UPDATE
	//   write       — per-row socket write (PUBLISH on the wire)
	// The audit's outbound-side counterpart to PublishStageSeconds.
	DeliveryStageSeconds *prometheus.HistogramVec

	// WillFireLatenessSeconds — how late the janitor fired a delayed
	// will, measured as (now - will_fire_at) at fire time. A bounded
	// histogram so an operator can SLO "delayed wills fire within N s
	// of scheduled."
	WillFireLatenessSeconds prometheus.Histogram

	// OutboundInflightSaturation — distribution of (in-flight slots in
	// use) / (in-flight cap), sampled per-delivery. Slow-consumer
	// detection: if this skews to 1.0, the subscriber is consistently
	// at-or-above its ReceiveMaximum.
	OutboundInflightSaturation prometheus.Histogram

	// ConnectionsCapacityRatio — current accepted connections /
	// maxConnections, refreshed each engine ownership-sweep tick.
	// HPA-style scale-out signal.
	ConnectionsCapacityRatio prometheus.Gauge

	// ListenerRestartsTotal counts dedicated-listener-conn reconnect
	// events, labelled by reason. Reasons:
	//   wait_error        — WaitForNotification returned a non-EOF, non-
	//                       net.ErrClosed error; the listener tore down
	//                       its conn, slept with backoff, and re-acquired
	//                       LISTEN + advisory-lock.
	//   ctx_cancel        — parent context cancelled mid-loop (Stop()).
	//                       Recorded only for completeness; not an error.
	//   exhausted_retries — all reconnect attempts failed; the Pod is
	//                       about to os.Exit(1) so the kubelet replaces
	//                       it. Alert: any non-zero increment.
	ListenerRestartsTotal *prometheus.CounterVec
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
		AuthFailuresTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgmqtt_auth_failures_total",
			Help: "Credential rejections at CONNECT, labelled by reason " +
				"(bad_credentials, not_authorized, banned, bad_auth_method).",
		}, []string{"reason"}),
		SubscribesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgmqtt_subscribes_total",
			Help: "Inbound MQTT SUBSCRIBE packets accepted by this Pod.",
		}),
		UnsubscribesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgmqtt_unsubscribes_total",
			Help: "Inbound MQTT UNSUBSCRIBE packets accepted by this Pod.",
		}),
		JanitorTickSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "pgmqtt_janitor_tick_seconds",
			Help: "Per-sub-job duration of janitor.Tick. Labels: job.",
			Buckets: []float64{
				.0001, .0005, .001, .005, .01, .05, .1, .25, .5, 1, 2.5, 5, 10,
			},
		}, []string{"job"}),
		JanitorErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgmqtt_janitor_errors_total",
			Help: "Per-sub-job error counter for janitor.Tick. Labels: job.",
		}, []string{"job"}),
		Subscriptions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgmqtt_subscriptions",
			Help: "Rows in the subscriptions table (refreshed each janitor tick).",
		}),
		Sessions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgmqtt_sessions",
			Help: "Rows in the sessions table (refreshed each janitor tick).",
		}),
		RetainedCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgmqtt_retained_count",
			Help: "Rows in the retained table (refreshed each janitor tick).",
		}),
		InboundQoS2Pending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgmqtt_inbound_qos2_pending",
			Help: "Rows in the inbound_qos2 dedup table (refreshed each janitor tick).",
		}),
		WillsNotifyFailedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgmqtt_wills_notify_failed_total",
			Help: "Will-publish post-commit Notifier-hook failures (production no-op; surfaces in test rigs).",
		}),
		RetainedDispatchFailedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgmqtt_retained_dispatch_failed_total",
			Help: "Per-filter retained-message dispatch failures during SUBSCRIBE handling, post-SUBACK.",
		}),
		DeliveryStageSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "pgmqtt_delivery_seconds",
			Help: "Time spent in each stage of the broker→subscriber fanout path. " +
				"Stages: total (whole Deliver call), scan (SELECT), alloc (packet ID + UPDATE), write (socket write).",
			Buckets: []float64{
				.0001, .0002, .0005, .001, .002, .005,
				.01, .02, .05, .1, .25, .5, 1, 2.5, 5,
			},
		}, []string{"stage"}),
		WillFireLatenessSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "pgmqtt_will_fire_lateness_seconds",
			Help: "How late the janitor fired a delayed will (now - will_fire_at). " +
				"SLO: delayed wills should fire within ~1 s of their scheduled time at default tick interval.",
			Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300},
		}),
		OutboundInflightSaturation: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "pgmqtt_outbound_inflight_saturation",
			Help: "Per-delivery sample of (in-flight slots in use) / (in-flight cap). " +
				"Skews towards 1.0 = subscriber consistently at ReceiveMaximum (slow consumer).",
			Buckets: []float64{.1, .25, .5, .75, .9, .95, .99, 1},
		}),
		ConnectionsCapacityRatio: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgmqtt_connections_capacity_ratio",
			Help: "Current accepted connections / maxConnections cap (per Pod). " +
				"HPA scale-out signal; sustained > .8 calls for capacity bump or scale-out.",
		}),
		ListenerRestartsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgmqtt_listener_restarts_total",
			Help: "Dedicated-listener-connection reconnect events. " +
				"Reasons: wait_error (NOTIFY wait returned non-EOF error; conn was rebuilt), " +
				"ctx_cancel (parent context cancelled mid-loop), " +
				"exhausted_retries (reconnect failed N times; Pod exiting for kubelet replacement).",
		}, []string{"reason"}),
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
		m.AuthFailuresTotal,
		m.SubscribesTotal,
		m.UnsubscribesTotal,
		m.JanitorTickSeconds,
		m.JanitorErrorsTotal,
		m.Subscriptions,
		m.Sessions,
		m.RetainedCount,
		m.InboundQoS2Pending,
		m.WillsNotifyFailedTotal,
		m.RetainedDispatchFailedTotal,
		m.DeliveryStageSeconds,
		m.WillFireLatenessSeconds,
		m.OutboundInflightSaturation,
		m.ConnectionsCapacityRatio,
		m.ListenerRestartsTotal,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// ObserveDeliveryStage records a duration sample for one stage of the
// broker→subscriber fanout path. Safe to call when m is nil.
func (m *Metrics) ObserveDeliveryStage(stage string, d time.Duration) {
	if m == nil {
		return
	}
	m.DeliveryStageSeconds.WithLabelValues(stage).Observe(d.Seconds())
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
