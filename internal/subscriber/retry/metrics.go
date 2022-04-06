package retry

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                                string
	subscriberTotalMessagesPushedToDLQ *prometheus.GaugeVec
)

func init() {
	env = os.Getenv("APP_ENV")

	subscriberTotalMessagesPushedToDLQ = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metro_subscriber_messages_pushed_to_dlq",
	}, []string{"env", "topic", "subscription"})
}
