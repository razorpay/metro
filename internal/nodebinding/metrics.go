package nodebinding

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                           string
	nodeBindingOperationCount     *prometheus.CounterVec
	nodeBindingOperationTimeTaken *prometheus.HistogramVec
)

func init() {
	env = os.Getenv("APP_ENV")

	nodeBindingOperationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_node_binding_operation_total_count",
	}, []string{"env", "operation"})

	nodeBindingOperationTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_node_binding_time_taken_for_operation_sec",
		Help:    "Time taken for each node binding operation",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 25),
	}, []string{"env", "operation"})
}
