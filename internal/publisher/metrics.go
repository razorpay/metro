package publisher

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	publisherMessagesPublished *prometheus.CounterVec
)

func init() {
	publisherMessagesPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_publisher_messages_published",
	}, []string{"topic"})
}
