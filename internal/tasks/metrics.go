package tasks

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                  string
	scheduledTaskLatency *prometheus.HistogramVec
)

func init() {
	env = os.Getenv("APP_ENV")

	scheduledTaskLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_worker_scheduled_task_latency",
		Help:    "Delay in picking up a scheduled a task",
		Buckets: prometheus.LinearBuckets(0.001, 0.005, 300),
	}, []string{"env"})
}
