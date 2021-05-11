package subscription

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                            string
	subscriptionOperationCount     *prometheus.CounterVec
	subscriptionOperationTimeTaken *prometheus.HistogramVec
)

func init() {
	env = os.Getenv("APP_ENV")

	subscriptionOperationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscription_operation_total_count",
	}, []string{"env", "operation"})

	subscriptionOperationTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscription_time_taken_for_operation_sec",
		Help:    "Time taken for each subscription operation",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.25, 200),
	}, []string{"env", "operation"})
}
