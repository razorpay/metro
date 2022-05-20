package publisher

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                            string
	publisherMessagesPublished     *prometheus.CounterVec
	publisherLastMsgProcessingTime *prometheus.GaugeVec
)

func init() {
	env = os.Getenv("APP_ENV")

	publisherMessagesPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_publisher_messages_published",
	}, []string{"env", "topic"})

	publisherLastMsgProcessingTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metro_publisher_identify_last_message_processing_time",
	}, []string{"env", "topic"})
}
