package publisher

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// Core implements IPublisher
type Core struct {
	bs brokerstore.IBrokerStore
}

// ICore is an interface over publisher core
type ICore interface {
	Publish(ctx context.Context, req *metrov1.PublishRequest) ([]string, error)
}

// NewCore returns a new publisher
func NewCore(bs brokerstore.IBrokerStore) ICore {
	return &Core{bs}
}

// Publish messages
func (p *Core) Publish(ctx context.Context, req *metrov1.PublishRequest) ([]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "PublisherCore.Publish", opentracing.Tags{
		"topic": req.Topic,
	})
	defer span.Finish()

	producerOps := messagebroker.ProducerClientOptions{Topic: req.Topic, TimeoutMs: 500}
	producer, err := p.bs.GetProducer(ctx, producerOps)
	if err != nil {
		logger.Ctx(ctx).Errorw("error in getting producer", "msg", err.Error())
		return nil, err
	}

	msgIDs := make([]string, 0)

	orderingKey := ""
	if len(req.Messages) > 0 {
		orderingKey = req.Messages[0].OrderingKey
	}
	if orderingKey != "" {
		// Encode the ordering key so that it contains only alphanumeric characters
		orderingKey = base64.URLEncoding.EncodeToString([]byte(orderingKey))
	}

	for _, msg := range req.Messages {
		// unset message id and publishtime if set
		// TODO: check how pubsub
		msg.MessageId = ""
		msg.PublishTime = nil
		// marshal proto with all attributes for publishing
		dataWithMeta, err := proto.Marshal(msg)
		// TODO: check the scenario where one out of many messages fail in google pubsub
		if err != nil {
			return nil, fmt.Errorf("unable to marshal message")
		}
		msgResp, err := producer.SendMessage(ctx, messagebroker.SendMessageToTopicRequest{
			Topic:       req.Topic,
			Message:     dataWithMeta,
			OrderingKey: orderingKey, // All messages in request will have the same ordering key
		})
		if err != nil {
			// TODO : handle gracefully
			//if err.Error() == kafka.ErrMsgTimedOut.String() {
			//	logger.Ctx(ctx).Infow("got error, rotating producer", "error", err.Error())
			//	p.bs.RemoveProducer(ctx, producerOps)
			//}
			logger.Ctx(ctx).Errorw("error in sending messages", "msg", err.Error())
			return nil, err
		}
		msgIDs = append(msgIDs, msgResp.MessageID)
		publisherMessagesPublished.WithLabelValues(env, req.Topic).Inc()
		publisherLastMsgProcessingTime.WithLabelValues(env, req.Topic).SetToCurrentTime()
	}

	return msgIDs, nil
}
