package stream

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                           string
	streamManagerActiveStreams    *prometheus.GaugeVec
	streamManagerSubscriberErrors *prometheus.CounterVec
)

func init() {
	env = os.Getenv("APP_ENV")

	streamManagerActiveStreams = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metro_stream_manager_active_streams",
	}, []string{"env", "stream_id", "subscription"})

	streamManagerSubscriberErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_stream_manager_subscriber_errors",
	}, []string{"env", "stream_id", "subscription", "error"})
}
