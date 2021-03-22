package subscriber

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	subscriberMessagesConsumed         *prometheus.CounterVec
	subscriberMessagesRetried          *prometheus.CounterVec
	subscriberMessagesAckd             *prometheus.CounterVec
	subscriberMessagesModAckd          *prometheus.CounterVec
	subscriberMessagesDeadlineEvicted  *prometheus.CounterVec
	subscriberTimeTakenToAckMsg        *prometheus.HistogramVec
	subscriberMemoryMessagesCountTotal *prometheus.GaugeVec
	subscriberPausedConsumersTotal     *prometheus.GaugeVec
)

func init() {
	subscriberMessagesConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_consumed",
	}, []string{"topic", "subscription"})

	subscriberMessagesRetried = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_retried",
	}, []string{"topic", "subscription"})

	subscriberMessagesAckd = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_ackd",
	}, []string{"topic", "subscription"})

	subscriberMessagesModAckd = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_mod_ackd",
	}, []string{"topic", "subscription"})

	subscriberMessagesDeadlineEvicted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "metro_subscriber_messages_deadline_evicted",
	}, []string{"topic", "subscription"})

	subscriberTimeTakenToAckMsg = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "metro_subscriber_time_to_ack_msg_seconds",
		Help:    "Time taken for a message from publish to actually being acknowledged",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 25),
	}, []string{"topic", "subscription"})

	subscriberMemoryMessagesCountTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metro_subscriber_in_memory_messages_count",
	}, []string{"topic", "subscription"})

	subscriberPausedConsumersTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "metro_subscriber_paused_consumers",
	}, []string{"topic", "subscription"})
}
