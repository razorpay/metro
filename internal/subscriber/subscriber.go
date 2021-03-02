package subscriber

import (
	"context"
	"time"

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
	GetID() string
	Acknowledge(ctx context.Context, req *AckMessage) error
	// TODO: fix ModifyAckDeadline definition and implementation
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

	// hold all consumed messages. this will help throttle based on maxOutstandingMessages and maxOutstandingBytes
	consumedMessages map[string]interface{}

	// TODO : temp code. add proper DS
	offsetBasedMinHeap map[int32]*AckMessage

	// TODO: need a way to identify the last committed offset on a topic partition when a new subscriber is created
	// our counter will init to that value initially
	maxCommittedOffset int32
}

// CanConsumeMore ...
func (s *Subscriber) CanConsumeMore() bool {
	return len(s.consumedMessages) <= int(s.maxOutstandingMessages)
}

// GetID ...
func (s *Subscriber) GetID() string {
	return s.subscriberID
}

// Acknowledge messages
func (s *Subscriber) Acknowledge(ctx context.Context, req *AckMessage) error {
	// need to make this operation thread-safe

	// add to DS directly
	s.offsetBasedMinHeap[req.Offset] = req

	// call heapify on offsets

	evictedMsgs := make([]*AckMessage, 0)
	offsetMarker := s.maxCommittedOffset + 1
	// check root node offset and compare with maxCommittedOffset
	for offsetMarker == s.offsetBasedMinHeap[0].Offset {

		// remove the root node
		evictedMsgs = append(evictedMsgs, s.offsetBasedMinHeap[0])
		delete(s.offsetBasedMinHeap, offsetMarker)

		// call heapify

		// increment offsetMarker to next offset
		offsetMarker++
	}

	// after the above loop breaks we would have the committable offset identified
	_, err := s.consumer.CommitByPartitionAndOffset(ctx, messagebroker.CommitOnTopicRequest{
		Topic:     req.Topic,
		Partition: req.Partition,
		Offset:    offsetMarker,
	})

	if err != nil {
		return err
	}

	// after successful commit to broker, make sure to re-init the maxCommittedOffset in subscriber
	s.maxCommittedOffset = offsetMarker

	// cleanup consumedMessages map to make space for more incoming messages
	if len(evictedMsgs) > 0 {
		for _, msg := range evictedMsgs {
			delete(s.consumedMessages, msg.MessageID)
		}
	}

	return nil
}

// ModifyAckDeadline for messages
func (s *Subscriber) ModifyAckDeadline(ctx context.Context, req *AckMessage) error {
	return nil
}

// Run loop
func (s *Subscriber) Run(ctx context.Context) {
	for {
		select {
		case req := <-s.requestChan:

			if s.CanConsumeMore() == false {
				logger.Ctx(ctx).Infow("reached max limit of consumed messages. please ack messages before proceeding")
				s.responseChan <- metrov1.PullResponse{}
				// TODO: remove this sleep and implement pause and resume
				time.Sleep(500 * time.Millisecond)
				continue
			}
			//s.consumer.Resume()
			r, err := s.consumer.ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{req.MaxNumOfMessages, s.timeoutInSec})
			if err != nil {
				logger.Ctx(ctx).Errorw("error in receiving messages", "msg", err.Error())
				s.errChan <- err
			}
			sm := make([]*metrov1.ReceivedMessage, 0)
			for _, msg := range r.OffsetWithMessages {
				protoMsg := &metrov1.PubsubMessage{}
				err = proto.Unmarshal(msg.Data, protoMsg)
				if err != nil {
					s.errChan <- err
				}
				// set messageID and publish time
				protoMsg.MessageId = msg.MessageID
				ts := &timestamppb.Timestamp{}
				ts.Seconds = msg.PublishTime.Unix() // test this
				protoMsg.PublishTime = ts
				// TODO: fix delivery attempt

				// store the processed messages in a map for limit checks
				s.consumedMessages[msg.MessageID] = msg
				// instead of passing msgId as is, construct a proper ackId using pre-defined fields
				ackID := NewAckMessage(s.subscriberID, s.topic, msg.Partition, msg.Offset, msg.MessageID).BuildAckID()
				sm = append(sm, &metrov1.ReceivedMessage{AckId: ackID, Message: protoMsg, DeliveryAttempt: 1})
			}
			s.responseChan <- metrov1.PullResponse{ReceivedMessages: sm}
		case <-ctx.Done():
			s.closeChan <- struct{}{}
			return
		}

		// TODO: see if we can handle ack / modack in this for{} itself. That way we can avoid taking locks on in-memory DSs
	}
}

// GetRequestChannel returns the chan from where request is received
func (s *Subscriber) GetRequestChannel() chan *PullRequest {
	return s.requestChan
}

// GetResponseChannel returns the chan where response is written
func (s *Subscriber) GetResponseChannel() chan metrov1.PullResponse {
	return s.responseChan
}

// GetErrorChannel returns the channel where error is written
func (s *Subscriber) GetErrorChannel() chan error {
	return s.errChan
}

// Stop the subscriber
func (s *Subscriber) Stop() {
	s.cancelFunc()
	<-s.closeChan
}
