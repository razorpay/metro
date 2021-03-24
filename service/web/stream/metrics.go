package stream

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                        string
	streamManagerActiveStreams *prometheus.GaugeVec
)

func init() {
	env = os.Getenv("APP_ENV")

	streamManagerActiveStreams = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metro_stream_manager_active_streams",
	}, []string{"env", "stream_id", "subscription"})
}
