package subscriber

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	env                                        string
	subscriberMessagesConsumed                 *prometheus.CounterVec
	subscriberMessagesRetried                  *prometheus.CounterVec
	subscriberMessagesAckd                     *prometheus.CounterVec
	subscriberMessagesModAckd                  *prometheus.CounterVec
	subscriberMessagesDeadlineEvicted          *prometheus.CounterVec
	subscriberTimeTakenFromPublishToConsumeMsg *prometheus.HistogramVec
	subscriberTimeTakenToAckMsg                *prometheus.HistogramVec
	subscriberTimeTakenToModAckMsg             *prometheus.HistogramVec
	subscriberMemoryMessagesCountTotal         *prometheus.GaugeVec
	subscriberPausedConsumersTotal             *prometheus.GaugeVec
)

func init() {
	env = os.Getenv("APP_ENV")

	subscriberMessagesConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_consumed",
	}, []string{"env", "topic", "subscription"})

	subscriberMessagesRetried = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_retried",
	}, []string{"env", "topic", "subscription"})

	subscriberMessagesAckd = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_ackd",
	}, []string{"env", "topic", "subscription"})

	subscriberMessagesModAckd = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_mod_ackd",
	}, []string{"env", "topic", "subscription"})

	subscriberMessagesDeadlineEvicted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_deadline_evicted",
	}, []string{"env", "topic", "subscription"})

	subscriberTimeTakenFromPublishToConsumeMsg = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscriber_time_from_publish_to_consume_msg_seconds",
		Help:    "Time taken for a message from publish to actually being consumed",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 25),
	}, []string{"env", "topic", "subscription"})

	subscriberTimeTakenToAckMsg = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscriber_time_to_ack_msg_seconds",
		Help:    "Time taken for a message from publish to actually being acknowledged",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 25),
	}, []string{"env", "topic", "subscription"})

	subscriberTimeTakenToModAckMsg = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscriber_time_to_mod_ack_msg_seconds",
		Help:    "Time taken for a message from publish to actually being mod-acknowledged",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 25),
	}, []string{"env", "topic", "subscription"})

	subscriberMemoryMessagesCountTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metro_subscriber_in_memory_messages_count",
	}, []string{"env", "topic", "subscription"})

	subscriberPausedConsumersTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metro_subscriber_paused_consumers",
	}, []string{"env", "topic", "subscription"})
}
