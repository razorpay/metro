package retry

import (
	"context"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
)

// MessageHandler defines the contract to process a retry-able broker message
type MessageHandler interface {
	Do(ctx context.Context, msg messagebroker.ReceivedMessage) error
}

// PushToPrimaryRetryTopic holds the needed instances to handle retry
type PushToPrimaryRetryTopic struct {
	bs brokerstore.IBrokerStore
}

// NewPushToPrimaryRetryTopicHandler inits a new retry handler
func NewPushToPrimaryRetryTopicHandler(bs brokerstore.IBrokerStore) MessageHandler {
	return &PushToPrimaryRetryTopic{bs: bs}
}

// Do defines the retry action. In this case it will push the message back on to the primary retry topic for re-processing by subscriber
func (s *PushToPrimaryRetryTopic) Do(ctx context.Context, msg messagebroker.ReceivedMessage) error {
	logger.Ctx(ctx).Infow("retryhandler: comes to push to retry", msg.LogFields()...)
	producer, err := s.bs.GetProducer(ctx, messagebroker.ProducerClientOptions{
		Topic:     msg.RetryTopic,
		TimeoutMs: defaultBrokerOperationsTimeoutMs,
	})
	if err != nil {
		return err
	}

	_, err = producer.SendMessage(ctx, messagebroker.SendMessageToTopicRequest{
		Topic:         msg.RetryTopic,
		Message:       msg.Data,
		OrderingKey:   msg.OrderingKey,
		TimeoutMs:     int(defaultBrokerOperationsTimeoutMs),
		MessageHeader: msg.MessageHeader,
		Attributes:    msg.Attributes,
	})
	logger.Ctx(ctx).Infow("retryhandler: send to retry topic response", "logFields", msg.LogFields(), "topic", msg.RetryTopic)
	if err != nil {
		return err
	}

	return nil
}
