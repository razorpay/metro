package node

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                    string
	nodeOperationCount     *prometheus.CounterVec
	nodeOperationTimeTaken *prometheus.HistogramVec
)

func init() {
	env = os.Getenv("APP_ENV")

	nodeOperationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_node_operation_total_count",
	}, []string{"env", "operation"})

	nodeOperationTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_node_time_taken_for_operation_sec",
		Help:    "Time taken for each node operation",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 25),
	}, []string{"env", "operation"})
}
