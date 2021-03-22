package stream

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	streamManagerActiveStreams *prometheus.GaugeVec
)

func init() {
	streamManagerActiveStreams = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metro_stream_manager_active_streams",
	}, []string{"stream_id", "subscription"})
}
