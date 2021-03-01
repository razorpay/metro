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
	Acknowledge(ctx context.Context, req *AckMessage) error
	ModifyAckDeadline(ctx context.Context, req *AckMessage) error
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
	subscription           string
	topic                  string
	subscriberID           string
	bs                     brokerstore.IBrokerStore
	subscriptionCore       subscription.ICore
	requestChan            chan *PullRequest
	responseChan           chan metrov1.PullResponse
	errChan                chan error
	closeChan              chan struct{}
	timeoutInSec           int
	consumer               messagebroker.Consumer
	cancelFunc             func()
	maxOutstandingMessages int64
	maxOutstandingBytes    int64

	// data structures to hold messages in-memory
	// TODO : temp code. add proper DS
	offsetBasedMinHeap map[int32]*AckMessage

	// TODO: need a way to identify the last committed offset on a topic partition when a new subscriber is created
	// our counter will init to that value initially
	maxCommittedOffset int32
}

// Acknowledge messages
func (c *Subscriber) Acknowledge(ctx context.Context, req *AckMessage) error {
	// need to make this operation thread-safe

	// add to DS directly
	c.offsetBasedMinHeap[req.Offset] = req

	// call heapify on offsets

	offsetMarker := c.maxCommittedOffset + 1
	// check root node offset and compare with maxCommittedOffset
	for offsetMarker == c.offsetBasedMinHeap[0].Offset {

		// remove the root node
		delete(c.offsetBasedMinHeap, offsetMarker)

		// call heapify

		// increment offsetMarker to next offset
		offsetMarker++
	}

	// after the above loop breaks we would have the committable offset identified
	_, err := c.consumer.CommitByPartitionAndOffset(ctx, messagebroker.CommitOnTopicRequest{
		Topic:     req.Topic,
		Partition: req.Partition,
		Offset:    offsetMarker,
	})

	if err != nil {
		return err
	}

	// after successful commit to broker, make sure to re-init the maxCommittedOffset in subscriber
	c.maxCommittedOffset = offsetMarker

	return nil
}

// ModifyAckDeadline for messages
func (c *Subscriber) ModifyAckDeadline(ctx context.Context, req *AckMessage) error {
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
				c.errChan <- err
			}
			sm := make([]*metrov1.ReceivedMessage, 0)
			for _, msg := range r.OffsetWithMessages {
				protoMsg := &metrov1.PubsubMessage{}
				err = proto.Unmarshal(msg.Data, protoMsg)
				if err != nil {
					c.errChan <- err
				}
				// set messageID and publish time
				protoMsg.MessageId = msg.MessageID
				ts := &timestamppb.Timestamp{}
				ts.Seconds = msg.PublishTime.Unix() // test this
				protoMsg.PublishTime = ts
				// TODO: fix delivery attempt

				// instead of passing msgId as is, construct a proper ackId using pre-defined fields
				ackID := NewAckMessage(c.subscriberID, c.topic, msg.Partition, msg.Offset, msg.MessageID).BuildAckID()
				sm = append(sm, &metrov1.ReceivedMessage{AckId: ackID, Message: protoMsg, DeliveryAttempt: 1})
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
	return c.errChan
}

// Stop the subscriber
func (c *Subscriber) Stop() {
	c.cancelFunc()
	<-c.closeChan
}
