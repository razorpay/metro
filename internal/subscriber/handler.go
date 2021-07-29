package subscriber

import (
	"context"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/messagebroker"
)

// Handler ...
type Handler interface {
	Do(ctx context.Context, msg messagebroker.ReceivedMessage) error
}

// PushToEndpoint ...
type PushToEndpoint struct {
}

// NewPushToEndpointHandler ...
func NewPushToEndpointHandler(model *subscription.Model) Handler {
	return &PushToEndpoint{}
}

// Do ...
func (s *PushToEndpoint) Do(ctx context.Context, msg messagebroker.ReceivedMessage) error {
	panic("implement me")
}

// PushToPrimaryTopic ...
type PushToPrimaryTopic struct {
	bs brokerstore.IBrokerStore
}

// NewPushToPrimaryTopicHandler ...
func NewPushToPrimaryTopicHandler(bs brokerstore.IBrokerStore) Handler {
	return &PushToPrimaryTopic{bs: bs}
}

// Do ...
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
