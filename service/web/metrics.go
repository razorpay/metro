package web

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                     string
	topicCacheHitCount      *prometheus.CounterVec
	topicCacheMissCount     *prometheus.CounterVec
	preWarmupCacheTimeTaken *prometheus.HistogramVec
)

func init() {
	env = os.Getenv("APP_ENV")

	topicCacheHitCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "topic_cache_hit_count",
	}, []string{"env", "operation"})

	topicCacheMissCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "topic_cache_miss_count",
	}, []string{"env", "operation"})

	preWarmupCacheTimeTaken = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pre_warmup_cache_time_taken_sec",
		Help:    "Time taken for each pre warmup cache",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.25, 100),
	}, []string{"env", "operation"})
}
