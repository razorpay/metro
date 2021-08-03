package subscriber

import (
	"context"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/pkg/messagebroker"
)

// RetryMessageHandler defines the contract to process a retry-able broker message
type RetryMessageHandler interface {
	Do(ctx context.Context, msg messagebroker.ReceivedMessage) error
}

// PushToPrimaryTopic holds the needed instances to handle retry
type PushToPrimaryTopic struct {
	bs brokerstore.IBrokerStore
}

// NewPushToPrimaryTopicHandler inits a new retry handler
func NewPushToPrimaryTopicHandler(bs brokerstore.IBrokerStore) RetryMessageHandler {
	return &PushToPrimaryTopic{bs: bs}
}

// Do defines the retry action. In this case it will push the message back on to the primart topic for re-processing by subscriber
func (s *PushToPrimaryTopic) Do(ctx context.Context, msg messagebroker.ReceivedMessage) error {
	producer, err := s.bs.GetProducer(ctx, messagebroker.ProducerClientOptions{
		Topic:     msg.SourceTopic,
		TimeoutMs: defaultBrokerOperationsTimeoutMs,
	})
	if err != nil {
		return err
	}

	_, err = producer.SendMessage(ctx, messagebroker.SendMessageToTopicRequest{
		Topic:     msg.SourceTopic,
		Message:   msg.Data,
		TimeoutMs: int(defaultBrokerOperationsTimeoutMs),
		MessageHeader: messagebroker.MessageHeader{
			MessageID:         msg.MessageID,
			SourceTopic:       msg.SourceTopic,
			Subscription:      msg.Subscription,
			CurrentRetryCount: msg.CurrentRetryCount,
			MaxRetryCount:     msg.MaxRetryCount,
			CurrentTopic:      msg.CurrentTopic,
			NextDeliveryTime:  msg.NextDeliveryTime,
		},
	})

	if err != nil {
		return err
	}

	return nil
}
