package topic

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                     string
	topicOperationCount     *prometheus.CounterVec
	topicOperationTimeTaken *prometheus.HistogramVec
)

func init() {
	env = os.Getenv("APP_ENV")

	topicOperationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_topic_operation_total_count",
	}, []string{"env", "operation"})

	topicOperationTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_topic_time_taken_for_operation_sec",
		Help:    "Time taken for each topic operation",
		Buckets: prometheus.ExponentialBuckets(0.0001, 1.25, 200),
	}, []string{"env", "operation"})
}
