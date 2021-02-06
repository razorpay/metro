package subscriber

import (
	"context"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Core implements ISubscriber
type Core struct {
	bs               brokerstore.IBrokerStore
	subscriptionCore subscription.ICore
}

// NewCore returns a new core
func NewCore(bs brokerstore.IBrokerStore, subscriptionCore subscription.ICore) *Core {
	return &Core{bs, subscriptionCore}
}

// Pull to pull messages
func (c *Core) Pull(ctx context.Context, req *PullRequest, timeoutInSec int) (metrov1.PullResponse, error) {
	t, err := c.subscriptionCore.GetTopicFromSubscriptionName(ctx, req.Subscription)
	if err != nil {
		return metrov1.PullResponse{}, err
	}
	consumer, err := c.bs.GetOrCreateConsumer(ctx, messagebroker.ConsumerClientOptions{Topic: strings.Replace(t, "/", "_", -1), GroupID: req.Subscription})
	if err != nil {
		return metrov1.PullResponse{}, err
	}
	// TODO: take number of messages as arg
	r, err := consumer.ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{10, timeoutInSec})
	if err != nil {
		return metrov1.PullResponse{}, err
	}
	sm := make([]*metrov1.ReceivedMessage, 0)
	for _, msg := range r.OffsetWithMessages {
		protoMsg := &metrov1.PubsubMessage{}
		err = proto.Unmarshal(msg.Data, protoMsg)
		if err != nil {
			return metrov1.PullResponse{}, err
		}
		// set messageID and publish time
		protoMsg.MessageId = msg.MessageID
		ts := &timestamppb.Timestamp{}
		ts.Seconds = msg.PublishTime.Unix() // test this
		protoMsg.PublishTime = ts
		// TODO: fix delivery attempt
		sm = append(sm, &metrov1.ReceivedMessage{AckId: msg.MessageID, Message: protoMsg, DeliveryAttempt: 1})
	}
	return metrov1.PullResponse{ReceivedMessages: sm}, nil
}

// Acknowledge messages
func (c *Core) Acknowledge(ctx context.Context, req *AcknowledgeRequest) error {
	return nil
}

// ModifyAckDeadline for messages
func (c *Core) ModifyAckDeadline(ctx context.Context, req *ModifyAckDeadlineRequest) error {
	return nil
}
