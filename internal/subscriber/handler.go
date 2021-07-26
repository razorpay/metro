package subscriber

import (
	"context"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/messagebroker"
)

type Handler interface {
	Do(ctx context.Context, msg messagebroker.ReceivedMessage) error
}

type PushToEndpoint struct {
}

func NewPushToEndpointHandler(model *subscription.Model) Handler {
	return &PushToEndpoint{}
}

func (s *PushToEndpoint) Do(ctx context.Context, msg messagebroker.ReceivedMessage) error {
	panic("implement me")
}

type PushToPrimaryTopic struct {
	bs brokerstore.IBrokerStore
}

func NewPushToPrimaryTopicHandler(bs brokerstore.IBrokerStore) Handler {
	return &PushToPrimaryTopic{bs: bs}
}

func (s *PushToPrimaryTopic) Do(ctx context.Context, message messagebroker.ReceivedMessage) error {
	producer, err := s.bs.GetProducer(ctx, messagebroker.ProducerClientOptions{
		//Topic:     message.PrimaryTopic,
		TimeoutMs: defaultBrokerOperationsTimeoutMs,
	})
	if err != nil {
		return err
	}

	_, err = producer.SendMessage(ctx, messagebroker.SendMessageToTopicRequest{
		//Topic:       message.PrimaryTopic,
		Message:   message.Data,
		TimeoutMs: int(defaultBrokerOperationsTimeoutMs),
		MessageID: message.MessageID,
		//RetryCount:  message.CurrentRetryCount,
	})

	if err != nil {
		return err
	}

	return nil
}
