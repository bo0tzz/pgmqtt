package metrics

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// poolCollector emits pgxpool.Stat values as `pgmqtt_pgx_*` metrics on each
// scrape. We don't use library-supplied collectors here because pgxpool
// doesn't ship one; this implementation is small enough to maintain inline.
type poolCollector struct {
	pool *pgxpool.Pool

	total          *prometheus.Desc
	idle           *prometheus.Desc
	inUse          *prometheus.Desc
	acquired       *prometheus.Desc
	acquireSeconds *prometheus.Desc
}

func newPoolCollector(pool *pgxpool.Pool) *poolCollector {
	return &poolCollector{
		pool: pool,
		total: prometheus.NewDesc("pgmqtt_pgx_total_conns",
			"Total connections currently in the pgxpool (idle + in use).", nil, nil),
		idle: prometheus.NewDesc("pgmqtt_pgx_idle_conns",
			"Idle connections in the pgxpool.", nil, nil),
		inUse: prometheus.NewDesc("pgmqtt_pgx_in_use_conns",
			"In-use connections in the pgxpool.", nil, nil),
		acquired: prometheus.NewDesc("pgmqtt_pgx_acquire_count_total",
			"Cumulative successful Acquire calls.", nil, nil),
		acquireSeconds: prometheus.NewDesc("pgmqtt_pgx_acquire_duration_seconds_total",
			"Cumulative seconds spent waiting on Acquire (sum, not histogram).", nil, nil),
	}
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.total
	ch <- c.idle
	ch <- c.inUse
	ch <- c.acquired
	ch <- c.acquireSeconds
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(s.TotalConns()))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(s.IdleConns()))
	ch <- prometheus.MustNewConstMetric(c.inUse, prometheus.GaugeValue, float64(s.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.acquired, prometheus.CounterValue, float64(s.AcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.acquireSeconds, prometheus.CounterValue, s.AcquireDuration().Seconds())
}
