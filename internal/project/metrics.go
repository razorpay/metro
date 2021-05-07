package project

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                       string
	projectOperationCount     *prometheus.CounterVec
	projectOperationTimeTaken *prometheus.HistogramVec
)

func init() {
	env = os.Getenv("APP_ENV")

	projectOperationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_project_operation_total_count",
	}, []string{"env", "operation"})

	projectOperationTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_project_time_taken_for_operation_sec",
		Help:    "Time taken for each project operation",
		Buckets: prometheus.ExponentialBuckets(0.0001, 1.25, 200),
	}, []string{"env", "operation"})
}
