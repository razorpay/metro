package hooks

import (
	"context"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                   string
	requestsReceivedTotal *prometheus.CounterVec
	responsesSentTotal    *prometheus.CounterVec
	responseDurations     *prometheus.HistogramVec
)

func init() {
	requestsReceivedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Number of HTTP requests received.",
		},
		[]string{"package", "server", "method", "env"},
	)

	responsesSentTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_responses_total",
			Help: "Number of HTTP responses sent.",
		},
		[]string{"package", "server", "method", "code", "env"},
	)

	responseDurations = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_durations_ms_histogram",
			Help:    "HTTP latency distributions histogram.",
			Buckets: []float64{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096},
		},
		[]string{"package", "server", "method", "code", "env"},
	)

	env = os.Getenv("APP_ENV")
}

var reqStartTimestampKey = new(int)

func markRequestStart(ctx context.Context) context.Context {
	return context.WithValue(ctx, reqStartTimestampKey, time.Now())
}

func getRequestStart(ctx context.Context) (time.Time, bool) {
	t, ok := ctx.Value(reqStartTimestampKey).(time.Time)
	return t, ok
}
