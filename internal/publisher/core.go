package publisher

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// Core implements IPublisher
type Core struct {
	bs brokerstore.IBrokerStore
}

// NewCore returns a new publisher
func NewCore(bs brokerstore.IBrokerStore) *Core {
	return &Core{bs}
}

// Publish messages
func (p *Core) Publish(ctx context.Context, req *metrov1.PublishRequest) ([]string, error) {
	producer, err := p.bs.GetOrCreateProducer(ctx, messagebroker.ProducerClientOptions{Topic: req.Topic, TimeoutSec: 50})
	if err != nil {
		logger.Ctx(ctx).Errorw("error in getting producer", "msg", err.Error())
		return nil, err
	}

	msgIDs := make([]string, 0)

	for _, msg := range req.Messages {
		// unset message id and publishtime if set
		msg.MessageId = ""
		msg.PublishTime = nil
		// marshal proto with all attributes for publishing
		dataWithMeta, err := proto.Marshal(msg)
		// TODO: check the scenario where one out of many messages fail in google pubsub
		if err != nil {
			return nil, fmt.Errorf("unable to marshal message")
		}
		// TODO: rationalise TimeoutSec
		msgResp, err := producer.SendMessages(ctx, messagebroker.SendMessageToTopicRequest{
			Topic:       req.Topic,
			Message:     dataWithMeta,
			OrderingKey: msg.OrderingKey,
			TimeoutSec:  1,
		})
		if err != nil {
			logger.Ctx(ctx).Errorw("error in sending messages", "msg", err.Error())
			return nil, err
		}
		msgIDs = append(msgIDs, msgResp.MessageID)
	}

	return msgIDs, nil
}
