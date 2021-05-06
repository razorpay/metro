package worker

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                              string
	workerMessagesAckd               *prometheus.CounterVec
	workerMessagesNAckd              *prometheus.CounterVec
	workerPushEndpointCallsCount     *prometheus.CounterVec
	workerPushEndpointHTTPStatusCode *prometheus.CounterVec
	workerPushEndpointTimeTaken      *prometheus.HistogramVec
	workerSubscriberErrors           *prometheus.CounterVec
)

func init() {
	env = os.Getenv("APP_ENV")

	workerMessagesAckd = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_worker_messages_ackd",
	}, []string{"env", "topic", "subscription", "endpoint"})

	workerMessagesNAckd = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_worker_messages_nackd",
	}, []string{"env", "topic", "subscription", "endpoint"})

	workerPushEndpointCallsCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_worker_push_endpoint_http_calls",
	}, []string{"env", "topic", "subscription", "endpoint"})

	workerPushEndpointHTTPStatusCode = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_worker_push_endpoint_http_status_code",
	}, []string{"env", "topic", "subscription", "endpoint", "code"})

	workerPushEndpointHTTPStatusCode = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_worker_push_endpoint_http_status_code",
	}, []string{"env", "topic", "subscription", "endpoint", "code"})

	workerPushEndpointTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_worker_push_endpoint_time_taken_seconds",
		Help:    "Time taken to get response from push endpoint",
		Buckets: prometheus.ExponentialBuckets(0.0001, 1.25, 200),
	}, []string{"env", "topic", "subscription", "endpoint"})

	workerSubscriberErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_worker_subscriber_errors",
	}, []string{"env", "topic", "subscription", "error"})
}
