package brokerstore

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                           string
	brokerStoreOperationCount     *prometheus.CounterVec
	brokerStoreOperationTimeTaken *prometheus.HistogramVec
)

func init() {
	env = os.Getenv("APP_ENV")

	brokerStoreOperationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_broker_store_operation_total_count",
	}, []string{"env", "operation"})

	brokerStoreOperationTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_broker_store_time_taken_for_operation_sec",
		Help:    "Time taken for each broker store operation",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 25),
	}, []string{"env", "operation"})
}
