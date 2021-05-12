package brokerstore

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                             string
	brokerStoreOperationCount       *prometheus.CounterVec
	brokerStoreActiveConsumersCount *prometheus.GaugeVec
	brokerStoreOperationTimeTaken   *prometheus.HistogramVec
)

func init() {
	env = os.Getenv("APP_ENV")

	brokerStoreOperationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_broker_store_operation_total_count",
	}, []string{"env", "operation"})

	brokerStoreActiveConsumersCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metro_broker_store_active_consumers_count",
	}, []string{"env", "key"})

	brokerStoreOperationTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_broker_store_time_taken_for_operation_sec",
		Help:    "Time taken for each broker store operation",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.25, 100),
	}, []string{"env", "operation"})
}
