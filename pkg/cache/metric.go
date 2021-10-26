package cache

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                     string
	cacheOperationCount     *prometheus.CounterVec
	cacheOperationTimeTaken *prometheus.HistogramVec
)

func init() {
	env = os.Getenv("APP_ENV")

	cacheOperationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_cache_operation_total_count",
	}, []string{"env", "operation"})

	cacheOperationTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_cache_time_taken_for_operation_sec",
		Help:    "Time taken for each cache operation",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.25, 100),
	}, []string{"env", "operation"})
}
