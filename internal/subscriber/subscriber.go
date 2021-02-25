package subscriber

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ISubscriber is interface over high level subscriber
type ISubscriber interface {
	Acknowledge(ctx context.Context, req *AcknowledgeRequest) error
	ModifyAckDeadline(ctx context.Context, req *ModifyAckDeadlineRequest) error
	// the grpc proto is used here as well, to optimise for serialization
	// and deserialisation, a little unclean but optimal
	// TODO: figure a better way out
	GetResponseChannel() chan metrov1.PullResponse
	GetRequestChannel() chan *PullRequest
	GetErrorChannel() chan error
	Stop()
	Run(ctx context.Context)
}

// Subscriber consumes messages from a topic
type Subscriber struct {
	bs                     brokerstore.IBrokerStore
	subscriptionCore       subscription.ICore
	requestChan            chan *PullRequest
	responseChan           chan metrov1.PullResponse
	eerChan                chan error
	closeChan              chan struct{}
	timeoutInSec           int
	consumer               messagebroker.Consumer
	cancelFunc             func()
	maxOutstandingMessages int64
	maxOutstandingBytes    int64
}

// Acknowledge messages
func (c *Subscriber) Acknowledge(ctx context.Context, req *AcknowledgeRequest) error {
	return nil
}

// ModifyAckDeadline for messages
func (c *Subscriber) ModifyAckDeadline(ctx context.Context, req *ModifyAckDeadlineRequest) error {
	return nil
}

// Run loop
func (c *Subscriber) Run(ctx context.Context) {
	for {
		select {
		case req := <-c.requestChan:
			//c.consumer.Resume()
			r, err := c.consumer.ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{req.MaxNumOfMessages, c.timeoutInSec})
			if err != nil {
				logger.Ctx(ctx).Errorw("error in receiving messages", "msg", err.Error())
				c.eerChan <- err
			}
			sm := make([]*metrov1.ReceivedMessage, 0)
			for _, msg := range r.OffsetWithMessages {
				protoMsg := &metrov1.PubsubMessage{}
				err = proto.Unmarshal(msg.Data, protoMsg)
				if err != nil {
					c.eerChan <- err
				}
				// set messageID and publish time
				protoMsg.MessageId = msg.MessageID
				ts := &timestamppb.Timestamp{}
				ts.Seconds = msg.PublishTime.Unix() // test this
				protoMsg.PublishTime = ts
				// TODO: fix delivery attempt
				sm = append(sm, &metrov1.ReceivedMessage{AckId: msg.MessageID, Message: protoMsg, DeliveryAttempt: 1})
			}
			c.responseChan <- metrov1.PullResponse{ReceivedMessages: sm}
		case <-ctx.Done():
			c.closeChan <- struct{}{}
			return
		}
	}
}

// GetRequestChannel returns the chan from where request is received
func (c *Subscriber) GetRequestChannel() chan *PullRequest {
	return c.requestChan
}

// GetResponseChannel returns the chan where response is written
func (c *Subscriber) GetResponseChannel() chan metrov1.PullResponse {
	return c.responseChan
}

// GetErrorChannel returns the channel where error is written
func (c *Subscriber) GetErrorChannel() chan error {
	return c.eerChan
}

// Stop the subscriber
func (c *Subscriber) Stop() {
	c.cancelFunc()
	<-c.closeChan
}
