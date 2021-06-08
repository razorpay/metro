package credentials

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                          string
	credentialOperationCount     *prometheus.CounterVec
	credentialOperationTimeTaken *prometheus.HistogramVec
)

func init() {
	env = os.Getenv("APP_ENV")

	credentialOperationCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_credential_operation_total_count",
	}, []string{"env", "operation"})

	credentialOperationTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_credential_time_taken_for_operation_sec",
		Help:    "Time taken for each credential operation",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.25, 100),
	}, []string{"env", "operation"})
}
