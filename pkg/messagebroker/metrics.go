package messagebroker

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                             string
	messageBrokerOperationCount     *prometheus.CounterVec
	messageBrokerOperationTimeTaken *prometheus.HistogramVec
	messageBrokerOperationError     *prometheus.CounterVec
)

func init() {
	env = os.Getenv("APP_ENV")

	messageBrokerOperationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_message_broker_operation_total_count",
	}, []string{"env", "broker", "operation"})

	messageBrokerOperationTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_message_broker_time_taken_for_operation_sec",
		Help:    "Time taken for each message broker operation",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.25, 200),
	}, []string{"env", "broker", "operation"})

	messageBrokerOperationError = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_message_broker_operation_error_count",
	}, []string{"env", "broker", "operation", "error"})
}
