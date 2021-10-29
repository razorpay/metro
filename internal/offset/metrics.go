package offset

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                      string
	offsetOperationCount     *prometheus.CounterVec
	offsetOperationTimeTaken *prometheus.HistogramVec
)

func init() {
	env = os.Getenv("APP_ENV")

	offsetOperationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_offset_operation_total_count",
	}, []string{"env", "operation"})

	offsetOperationTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_offset_time_taken_for_operation_sec",
		Help:    "Time taken for each project operation",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.25, 100),
	}, []string{"env", "operation"})
}
