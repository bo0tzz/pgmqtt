package operator

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// userRehashTotal counts User-row password-hash rewrites observed by the
// reconciler, labelled by the trigger:
//
//   - cost_bump — operator.bcryptCost was raised above the cost embedded
//     in the stored hash, so the reconciler re-bcrypted the existing
//     password (no plaintext rotation) at the new cost.
//   - rotation — the cleartext password in the credentials Secret
//     differed from the previously-observed value (BYO secret update,
//     fresh User CR, or peer-rotated auto-gen Secret), so the reconciler
//     bcrypted the new value.
//
// Operators watch this counter to gauge rollout impact when raising
// bcryptCost: each existing User CR triggers exactly one cost_bump
// increment as it is reconciled. Bump cost in stages on large fleets.
var userRehashTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "pgmqtt_user_rehash_total",
	Help: "User-row password-hash rewrites by the operator, labelled by reason " +
		"(cost_bump = bcryptCost raised above stored hash cost; " +
		"rotation = cleartext password changed in the credentials Secret).",
}, []string{"reason"})

func init() {
	// Register on controller-runtime's package-global metrics.Registry so
	// the counter surfaces on the broker's /metrics endpoint via the
	// AddGatherer wiring in cmd/pgmqttd/main.go. Pre-create the two label
	// values so they appear at zero before any User reconcile fires —
	// gives operators a constant-cardinality target for alerts.
	ctrlmetrics.Registry.MustRegister(userRehashTotal)
	userRehashTotal.WithLabelValues("cost_bump").Add(0)
	userRehashTotal.WithLabelValues("rotation").Add(0)
}

// UserRehashTotalForTest returns the current value of
// pgmqtt_user_rehash_total{reason=<reason>} for use in test assertions.
// It is exported only for tests in the same module; production callers
// scrape the value via /metrics.
func UserRehashTotalForTest(reason string) float64 {
	c := userRehashTotal.WithLabelValues(reason)
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}
