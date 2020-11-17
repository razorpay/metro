package hooks

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twitchtv/twirp"
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

// Metric returns function which puts unique request id into context.
func Metric() *twirp.ServerHooks {
	hooks := &twirp.ServerHooks{}

	// RequestReceived:
	hooks.RequestReceived = func(ctx context.Context) (context.Context, error) {
		ctx = markRequestStart(ctx)

		return ctx, nil
	}

	// RequestRouted:
	hooks.RequestRouted = func(ctx context.Context) (context.Context, error) {
		pkg, _ := twirp.PackageName(ctx)
		service, _ := twirp.ServiceName(ctx)
		method, _ := twirp.MethodName(ctx)

		requestsReceivedTotal.
			WithLabelValues(pkg, service, method, env).
			Inc()

		return ctx, nil
	}

	// ResponseSent:
	hooks.ResponseSent = func(ctx context.Context) {
		start, _ := getRequestStart(ctx)
		pkg, _ := twirp.PackageName(ctx)
		service, _ := twirp.ServiceName(ctx)
		method, _ := twirp.MethodName(ctx)
		statusCode, _ := twirp.StatusCode(ctx)

		duration := float64(time.Now().Sub(start).Milliseconds())

		responsesSentTotal.WithLabelValues(
			pkg, service, method,
			fmt.Sprintf("%v", statusCode),
			env,
		).Inc()

		responseDurations.WithLabelValues(
			pkg, service, method,
			fmt.Sprintf("%v", statusCode),
			env,
		).Observe(duration)
	}

	return hooks
}

var reqStartTimestampKey = new(int)

func markRequestStart(ctx context.Context) context.Context {
	return context.WithValue(ctx, reqStartTimestampKey, time.Now())
}

func getRequestStart(ctx context.Context) (time.Time, bool) {
	t, ok := ctx.Value(reqStartTimestampKey).(time.Time)
	return t, ok
}
