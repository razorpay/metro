package publisher

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                        string
	publisherMessagesPublished *prometheus.CounterVec
)

func init() {
	env = os.Getenv("APP_ENV")

	publisherMessagesPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_publisher_messages_published",
	}, []string{"env", "topic"})
}
