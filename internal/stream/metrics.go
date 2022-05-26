package stream

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                              string
	workerMessagesAckd               *prometheus.CounterVec
	workerMessagesNAckd              *prometheus.CounterVec
	workerPushEndpointHTTPStatusCode *prometheus.CounterVec
	workerPushEndpointTimeTaken      *prometheus.HistogramVec
	workerSubscriberErrors           *prometheus.CounterVec
	workerPushEndpointCallsCount     *prometheus.CounterVec
	workerEntityRestartCount         *prometheus.CounterVec
)

func init() {
	env = os.Getenv("APP_ENV")

	workerMessagesAckd = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_worker_messages_ackd",
	}, []string{"env", "topic", "subscription", "endpoint", "subscriberId"})

	workerMessagesNAckd = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_worker_messages_nackd",
	}, []string{"env", "topic", "subscription", "endpoint", "subscriberId"})

	workerPushEndpointHTTPStatusCode = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_worker_push_endpoint_http_status_code",
	}, []string{"env", "topic", "subscription", "endpoint", "code"})

	workerPushEndpointTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_worker_push_endpoint_time_taken_seconds",
		Help:    "Time taken to get response from push endpoint",
		Buckets: prometheus.LinearBuckets(0.001, 0.005, 300),
	}, []string{"env", "topic", "subscription", "endpoint"})

	workerSubscriberErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_worker_subscriber_errors",
	}, []string{"env", "topic", "subscription", "error", "subscriberId"})

	workerPushEndpointCallsCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_worker_push_endpoint_http_calls",
	}, []string{"env", "topic", "subscription", "endpoint", "subscriberId"})

	workerEntityRestartCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_worker_subscriber_restart_count",
	}, []string{"env", "entity", "topic", "subscription"})
}
