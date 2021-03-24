package registry

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                        string
	registryOperationCount     *prometheus.CounterVec
	registryOperationTimeTaken *prometheus.HistogramVec
)

func init() {
	env = os.Getenv("APP_ENV")

	registryOperationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_registry_operation_total_count",
	}, []string{"env", "operation"})

	registryOperationTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_registry_time_taken_for_operation_sec",
		Help:    "Time taken for each registry operation",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 25),
	}, []string{"env", "operation"})
}
