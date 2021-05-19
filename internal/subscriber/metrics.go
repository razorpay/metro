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
	subscriberTimeTakenInRequestChannelCase    *prometheus.HistogramVec
	subscriberTimeTakenInAckChannelCase        *prometheus.HistogramVec
	subscriberTimeTakenInModAckChannelCase     *prometheus.HistogramVec
	subscriberTimeTakenInDeadlineChannelCase   *prometheus.HistogramVec
	subscriberTimeTakenToRemoveMsgFromMemory   *prometheus.HistogramVec
	subscriberTimeTakenToIdentifyNextOffset    *prometheus.HistogramVec
	subscriberTimeTakenToPushToRetry           *prometheus.HistogramVec
)

func init() {
	env = os.Getenv("APP_ENV")

	subscriberMessagesConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_consumed",
	}, []string{"env", "topic", "subscription", "subscriberId"})

	subscriberMessagesRetried = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_retried",
	}, []string{"env", "topic", "subscription"})

	subscriberMessagesAckd = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_ackd",
	}, []string{"env", "topic", "subscription", "subscriberId"})

	subscriberMessagesModAckd = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_mod_ackd",
	}, []string{"env", "topic", "subscription", "subscriberId"})

	subscriberMessagesDeadlineEvicted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_deadline_evicted",
	}, []string{"env", "topic", "subscription"})

	subscriberTimeTakenFromPublishToConsumeMsg = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscriber_time_from_publish_to_consume_msg_seconds",
		Help:    "Time taken for a message from publish to actually being consumed",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.25, 100),
	}, []string{"env", "topic", "subscription"})

	subscriberTimeTakenToAckMsg = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscriber_time_to_ack_msg_seconds",
		Help:    "Time taken for a message from publish to actually being acknowledged",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.25, 100),
	}, []string{"env", "topic", "subscription"})

	subscriberTimeTakenToModAckMsg = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscriber_time_to_mod_ack_msg_seconds",
		Help:    "Time taken for a message from publish to actually being mod-acknowledged",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.25, 100),
	}, []string{"env", "topic", "subscription"})

	subscriberMemoryMessagesCountTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metro_subscriber_in_memory_messages_count",
	}, []string{"env", "topic", "subscription", "subscriberId"})

	subscriberPausedConsumersTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metro_subscriber_paused_consumers",
	}, []string{"env", "topic", "subscription", "subscriberId"})

	subscriberTimeTakenInRequestChannelCase = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscriber_request_channel_time_taken_seconds",
		Help:    "Time taken for the request channel case block execution",
		Buckets: prometheus.LinearBuckets(0.001, 0.005, 300),
	}, []string{"env", "topic", "subscription"})

	subscriberTimeTakenInAckChannelCase = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscriber_ack_channel_time_taken_seconds",
		Help:    "Time taken for the ack channel case block execution",
		Buckets: prometheus.LinearBuckets(0.001, 0.005, 300),
	}, []string{"env", "topic", "subscription"})

	subscriberTimeTakenInModAckChannelCase = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscriber_mod_ack_channel_time_taken_seconds",
		Help:    "Time taken for the mod ack channel case block execution",
		Buckets: prometheus.LinearBuckets(0.001, 0.005, 300),
	}, []string{"env", "topic", "subscription"})

	subscriberTimeTakenInDeadlineChannelCase = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscriber_deadline_channel_time_taken_seconds",
		Help:    "Time taken for the deadline channel case block execution",
		Buckets: prometheus.LinearBuckets(0.001, 0.005, 300),
	}, []string{"env", "topic", "subscription"})

	subscriberTimeTakenToRemoveMsgFromMemory = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscriber_remove_message_from_memory_time_taken_seconds",
		Help:    "Time taken for a message to be removed from the in-memory data structure",
		Buckets: prometheus.LinearBuckets(0.001, 0.005, 300),
	}, []string{"env"})

	subscriberTimeTakenToIdentifyNextOffset = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscriber_identify_next_offset_from_memory_time_taken_seconds",
		Help:    "Time taken to identify next offset to commit from the in-memory data structure",
		Buckets: prometheus.LinearBuckets(0.001, 0.005, 300),
	}, []string{"env"})

	subscriberTimeTakenToPushToRetry = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscriber_retry_time_taken_seconds",
		Help:    "Time taken to process retry message",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.1, 200),
	}, []string{"env"})
}
