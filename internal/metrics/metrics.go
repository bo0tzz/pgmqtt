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

	// ConnectDroppedTotal counts CONNECTs dropped pre-CONNACK by the
	// per-IP limiter (bcrypt-CPU DoS mitigation). Reasons:
	//   * rate_limit  — over the configured CONNECTs/sec budget for the
	//                   source IP. Socket closed without CONNACK.
	//   * penalty_box — IP exhausted its auth-failure budget; further
	//                   CONNECTs are dropped pre-bcrypt for the cool-off
	//                   window (default 60s).
	ConnectDroppedTotal *prometheus.CounterVec

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

	// Subscriptions / Sessions / RetainedCount / InboundQoS2Pending /
	// MessagesCount — gauges refreshed by janitor each tick. Cardinality
	// detection, retained-flood detection, QoS-2 stuck detection.
	// MessagesCount specifically catches the "deliveries drained but
	// orphan-messages sweep is lagging" shape that compounded the
	// v0.1.15 throughput-cliff investigation.
	Subscriptions     prometheus.Gauge
	Sessions          prometheus.Gauge
	RetainedCount     prometheus.Gauge
	InboundQoS2Pending prometheus.Gauge
	MessagesCount     prometheus.Gauge

	// JanitorSweptRowsTotal — rows acted on by each sweep job (deleted,
	// expired, fired, etc.). Per-tick increments are the right denominator
	// for "is the janitor keeping up with inflow?" — pair with the
	// publishes_total / state-gauge deltas to detect bloat building up.
	JanitorSweptRowsTotal *prometheus.CounterVec

	// NotifyQueueUsageRatio — pg_notification_queue_usage(), sampled by
	// the janitor. PG's notify queue is shared-memory and capped; once
	// it fills (one wedged listener can do that under sustained
	// publish load), every committing transaction in the cluster
	// errors at COMMIT with 54000. The single largest cause of "lights
	// are slow tonight" in a multi-pod deploy. Alert above 0.5.
	NotifyQueueUsageRatio prometheus.Gauge

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

	// DrainSessionQueueTotal counts invocations of drainSessionQueue —
	// the bulk re-send of state 0/1/2 deliveries to a resumed session
	// after CONNACK. Labelled by reason so a soak operator can confirm
	// "drain calls are bounded by reconnect rate" rather than firing
	// spuriously from another path. Today's only call site is the
	// CONNECT handler (reason=reconnect); the label exists so future
	// drain triggers (NOTIFY-driven re-scan, SUBSCRIBE-driven retained
	// fanout) can be distinguished without a metric rename.
	DrainSessionQueueTotal *prometheus.CounterVec

	// DrainSessionQueueFailuresTotal counts drainSessionQueue invocations
	// that returned an error before completing. Companion to
	// DrainSessionQueueTotal — the success counter Inc lives strictly
	// after the drain returns nil so dashboards aren't lied to about
	// successful drains. A growing _failures_total with flat _total is
	// the "PG-wedged on resume" shape.
	DrainSessionQueueFailuresTotal *prometheus.CounterVec

	// DeliveriesDroppedTotal counts delivery rows that were destroyed
	// before a successful wire write reached the subscriber. The MQTT
	// spec allows silent loss in each of these cases but the broker
	// previously had no aggregate signal for any of them — the May 2026
	// zigbee2mqtt-blackhole bug stayed invisible because there was no
	// counter behind the silent over-cap drop. Reasons:
	//   expired      — MessageExpiryInterval elapsed before delivery
	//                  ([MQTT-3.3.2-5]). Bounded by orphan-grace ×
	//                  publish rate at steady state.
	//   oversized    — Encoded packet exceeded the client's
	//                  MaximumPacketSize ([MQTT-3.1.2-25]). A sustained
	//                  rate here means a publisher is producing payloads
	//                  bigger than some subscriber configured for.
	//   write_error  — conn.write returned an error (broken socket, slow
	//                  client, kernel-level backpressure timeout). Each
	//                  drop is also logged at Warn from the Deliver loop.
	//   overflow     — The subscriber's per-client deliveries queue was
	//                  at MaxQueuedDeliveriesPerClient when fanout ran;
	//                  mqtt_publish skipped the INSERT for that client.
	//                  The subscriber is also disconnected with DISCONNECT
	//                  0x97 (Quota Exceeded). Sustained traffic here
	//                  means a slow subscriber is being torn down rather
	//                  than the broker stalling on its behalf.
	DeliveriesDroppedTotal *prometheus.CounterVec

	// PublishFanoutSubscribers — distribution of subscriber counts per
	// inbound publish, sampled at the mqtt_publish CTE return. Tracks the
	// per-message fanout shape an operator most cares about: "is one topic
	// driving thousands of inserts per publish?" Skewed long-tail = a hot
	// hub topic that's about to push the publish path into PG-CPU
	// saturation; the cpu-pg sweep showed mqtt_publish at 64% of total
	// PG exec time at QoS-1 saturation. 0 = no matching subscriber.
	PublishFanoutSubscribers prometheus.Histogram

	// EndToEndPublishToDeliverSeconds — histogram of (now() - messages.created_at)
	// observed on each successful per-row deliver. Bridges publish_seconds
	// (broker→PG) and delivery_seconds (PG→subscriber) into a single
	// number an operator can SLO. Captures the full latency: publisher
	// ACK + commit-to-NOTIFY + NOTIFY-to-Deliver + scan + write.
	EndToEndPublishToDeliverSeconds prometheus.Histogram

	// PgxAcquireSeconds — distribution of pool.Acquire / pool.BeginTx
	// wait time across the broker. Cross-cutting view of pool saturation
	// that the existing pgmqtt_pgx_acquire_duration_seconds_total counter
	// (sum, no buckets) cannot give p99 visibility for. Observed by the
	// engine + janitor BeginTx / Acquire helpers; queries via Query/Exec
	// hold the conn briefly enough that they are not separately timed.
	PgxAcquireSeconds prometheus.Histogram
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
		ConnectDroppedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgmqtt_connect_dropped_total",
			Help: "CONNECTs dropped pre-CONNACK by the per-IP limiter, " +
				"labelled by reason (rate_limit, penalty_box). " +
				"bcrypt-CPU DoS mitigation.",
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
		MessagesCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgmqtt_messages_count",
			Help: "Rows in the messages table (refreshed each janitor tick). " +
				"Pair with publishes_total / sweep yield to spot orphan-messages " +
				"sweep lagging the publish-side inflow.",
		}),
		JanitorSweptRowsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgmqtt_janitor_swept_rows_total",
			Help: "Rows acted on by each janitor sweep job (deleted, expired, " +
				"fired, etc.). Labels: job. Compare with the corresponding state " +
				"gauge to detect sweep falling behind inflow.",
		}, []string{"job"}),
		NotifyQueueUsageRatio: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgmqtt_pg_notify_queue_usage_ratio",
			Help: "pg_notification_queue_usage(), 0..1. Above ~0.5 means " +
				"a slow listener is letting NOTIFYs back up; at 1.0 every " +
				"committing tx in the cluster errors with SQLSTATE 54000.",
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
		DrainSessionQueueTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgmqtt_drain_session_queue_total",
			Help: "drainSessionQueue invocations that completed without error, " +
				"labelled by reason. Reasons: reconnect (CONNECT with " +
				"cleanStart=false; resume queued/inflight deliveries for the " +
				"returning session). Bounded by the reconnect rate; sustained " +
				"spikes here indicate flapping clients. Inc'd strictly AFTER " +
				"the drain returns nil — see pgmqtt_drain_session_queue_failures_total " +
				"for the error-path counter.",
		}, []string{"reason"}),
		DrainSessionQueueFailuresTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgmqtt_drain_session_queue_failures_total",
			Help: "drainSessionQueue invocations that returned an error " +
				"(typically PG unreachable during resume). Labelled by the " +
				"same reasons as pgmqtt_drain_session_queue_total. " +
				"Growing _failures_total with flat _total is the 'PG-wedged on " +
				"resume' shape.",
		}, []string{"reason"}),
		DeliveriesDroppedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgmqtt_deliveries_dropped_total",
			Help: "Delivery rows destroyed before a successful wire write. " +
				"Reasons: expired (MessageExpiryInterval elapsed), " +
				"oversized (encoded packet > client's MaximumPacketSize), " +
				"write_error (socket write failed), " +
				"overflow (per-client deliveries queue at cap — QoS≥1 publish " +
				"skipped the insert; the subscriber is also disconnected with " +
				"DISCONNECT 0x97 Quota Exceeded).",
		}, []string{"reason"}),
		PublishFanoutSubscribers: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "pgmqtt_publish_fanout_subscribers",
			Help: "Subscribers fanned out per inbound publish (deliveries-table inserts). " +
				"0 = no matching subscriber. Long tail = hot hub topic.",
			// Hub topics dominate the long tail; the buckets bracket
			// 0 (no matching sub), 1-10 (homelab norm), 100+ (gateway-
			// shaped fan-in), 1k+ (k8s-operator-class).
			Buckets: []float64{0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000},
		}),
		EndToEndPublishToDeliverSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "pgmqtt_e2e_publish_to_deliver_seconds",
			Help: "End-to-end latency from message INSERT (publisher's commit) to " +
				"successful PUBLISH write to subscriber. now() - messages.created_at " +
				"sampled per delivered row. Bridges publish_seconds and delivery_seconds.",
			Buckets: []float64{
				.0005, .001, .002, .005, .01, .02, .05, .1, .25, .5, 1, 2.5, 5,
			},
		}),
		PgxAcquireSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "pgmqtt_pgx_acquire_seconds",
			Help: "pgxpool BeginTx / Acquire wait time, observed by the engine + janitor " +
				"helpers. Cross-cutting saturation signal — sustained p99 above ~10 ms " +
				"on a healthy PG means the pool is starving callers. Complements the " +
				"existing pgmqtt_pgx_acquire_duration_seconds_total counter (sum, no buckets).",
			Buckets: []float64{
				.0001, .0002, .0005, .001, .002, .005,
				.01, .02, .05, .1, .25, .5, 1, 2.5, 5,
			},
		}),
	}

	// Pre-create label series at zero so /metrics surfaces them before any
	// traffic — cold-start visibility for dashboards and alerts.
	m.DrainSessionQueueTotal.WithLabelValues("reconnect")
	m.DrainSessionQueueFailuresTotal.WithLabelValues("reconnect")
	m.DeliveriesDroppedTotal.WithLabelValues("expired")
	m.DeliveriesDroppedTotal.WithLabelValues("oversized")
	m.DeliveriesDroppedTotal.WithLabelValues("write_error")
	m.DeliveriesDroppedTotal.WithLabelValues("overflow")

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
		m.ConnectDroppedTotal,
		m.SubscribesTotal,
		m.UnsubscribesTotal,
		m.JanitorTickSeconds,
		m.JanitorErrorsTotal,
		m.Subscriptions,
		m.Sessions,
		m.RetainedCount,
		m.InboundQoS2Pending,
		m.MessagesCount,
		m.JanitorSweptRowsTotal,
		m.NotifyQueueUsageRatio,
		m.WillsNotifyFailedTotal,
		m.RetainedDispatchFailedTotal,
		m.DeliveryStageSeconds,
		m.WillFireLatenessSeconds,
		m.OutboundInflightSaturation,
		m.ConnectionsCapacityRatio,
		m.ListenerRestartsTotal,
		m.DrainSessionQueueTotal,
		m.DrainSessionQueueFailuresTotal,
		m.DeliveriesDroppedTotal,
		m.PublishFanoutSubscribers,
		m.EndToEndPublishToDeliverSeconds,
		m.PgxAcquireSeconds,
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

// ObserveDeliveryDropped increments pgmqtt_deliveries_dropped_total for
// the given reason. Safe to call when m is nil.
func (m *Metrics) ObserveDeliveryDropped(reason string) {
	if m == nil {
		return
	}
	m.DeliveriesDroppedTotal.WithLabelValues(reason).Inc()
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

// ObservePublishFanout records the subscriber count for one publish.
// Safe to call when m is nil.
func (m *Metrics) ObservePublishFanout(subscribers int64) {
	if m == nil {
		return
	}
	m.PublishFanoutSubscribers.Observe(float64(subscribers))
}

// ObserveE2EPublishToDeliver records the end-to-end ingest→deliver latency
// for one delivered row. Safe to call when m is nil.
func (m *Metrics) ObserveE2EPublishToDeliver(d time.Duration) {
	if m == nil {
		return
	}
	m.EndToEndPublishToDeliverSeconds.Observe(d.Seconds())
}

// ObservePgxAcquire records one pool BeginTx / Acquire wait sample.
// Safe to call when m is nil.
func (m *Metrics) ObservePgxAcquire(d time.Duration) {
	if m == nil {
		return
	}
	m.PgxAcquireSeconds.Observe(d.Seconds())
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

// Pinger is the minimal pgxpool surface needed to drive a readiness
// probe. *pgxpool.Pool satisfies it without explicit declaration.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Serve starts an HTTP server on addr with /metrics handled. Blocks until
// ctx is cancelled. Logs are intentionally minimal — the caller wraps.
//
// If pool is non-nil, /healthz/ready also pings the pool with a tight
// timeout: the K8s readinessProbe wired to this endpoint flips the Pod
// out of the Service when Postgres is unreachable, so traffic shifts to
// healthy peers instead of stalling on a dead listener that still
// accepts TCP.
func (m *Metrics) Serve(ctx context.Context, addr string, pool Pinger) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz/live", func(w http.ResponseWriter, _ *http.Request) {
		// Liveness only fails when the process is broken in a way K8s
		// should restart through. We're alive iff the goroutine
		// answering this request runs. Don't fail liveness on PG blips
		// — there's nowhere to fail to.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/healthz/ready", func(w http.ResponseWriter, r *http.Request) {
		if pool == nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		probeCtx, cancel := context.WithTimeout(r.Context(), 1500*time.Millisecond)
		defer cancel()
		if err := pool.Ping(probeCtx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("postgres unavailable"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
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
