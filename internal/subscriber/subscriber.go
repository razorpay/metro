package subscriber

import (
	"container/heap"
	"context"
	"time"

	"github.com/razorpay/metro/internal/subscriber/customheap"

	"github.com/golang/protobuf/proto"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	minAckDeadline = 10 * time.Minute
)

// ISubscriber is interface over high level subscriber
type ISubscriber interface {
	GetID() string
	Acknowledge(ctx context.Context, req *AckMessage) error
	// TODO: fix ModifyAckDeadline definition and implementation
	ModifyAckDeadline(ctx context.Context, req *ModAckMessage) error
	// the grpc proto is used here as well, to optimise for serialization
	// and deserialisation, a little unclean but optimal
	// TODO: figure a better way out
	GetResponseChannel() chan metrov1.PullResponse
	GetRequestChannel() chan *PullRequest
	GetAckChannel() chan *AckMessage
	GetModAckChannel() chan *ModAckMessage
	GetErrorChannel() chan error
	Stop()
	Run(ctx context.Context)
}

// Subscriber consumes messages from a topic
type Subscriber struct {
	subscription           string
	topic                  string
	retryTopic             string
	subscriberID           string
	bs                     brokerstore.IBrokerStore
	subscriptionCore       subscription.ICore
	requestChan            chan *PullRequest
	responseChan           chan metrov1.PullResponse
	ackChan                chan *AckMessage
	modAckChan             chan *ModAckMessage
	errChan                chan error
	closeChan              chan struct{}
	deadlineTickerChan     chan bool
	timeoutInSec           int
	consumer               messagebroker.Consumer
	retryConsumer          messagebroker.Consumer
	cancelFunc             func()
	maxOutstandingMessages int64
	maxOutstandingBytes    int64
	consumedMessageStats   map[TopicPartition]*ConsumptionMetadata
}

// CanConsumeMore ...
func (s *Subscriber) CanConsumeMore(tp TopicPartition) bool {
	return len(s.consumedMessageStats[tp].consumedMessages) <= int(s.maxOutstandingMessages)
}

// GetID ...
func (s *Subscriber) GetID() string {
	return s.subscriberID
}

// Acknowledge messages
func (s *Subscriber) Acknowledge(ctx context.Context, req *AckMessage) error {

	tp := req.ToTopicPartition()
	stats := s.consumedMessageStats[tp]

	_, err := s.consumer.CommitByPartitionAndOffset(ctx, messagebroker.CommitOnTopicRequest{
		Topic:     req.Topic,
		Partition: req.Partition,
		Offset:    req.Offset,
	})
	if err != nil {
		return err
	}

	// NOTE: do below in-memory operations only after a successful commit to broker
	delete(stats.consumedMessages, req.MessageID)

	// remove message from offsetBasedMinHeap
	indexOfMsgInOffsetBasedMinHeap := stats.offsetBasedMinHeap.MsgIDToIndexMapping[req.MessageID]
	msg := heap.Remove(&stats.offsetBasedMinHeap, indexOfMsgInOffsetBasedMinHeap).(customheap.AckMessageWithOffset)
	heap.Init(&stats.offsetBasedMinHeap)

	// remove same message from deadlineBasedMinHeap
	indexOfMsgInDeadlineBasedMinHeap := stats.deadlineBasedMinHeap.MsgIDToIndexMapping[msg.MsgID]
	heap.Remove(&stats.deadlineBasedMinHeap, indexOfMsgInDeadlineBasedMinHeap)
	heap.Init(&stats.deadlineBasedMinHeap)

	// after successful commit to broker, make sure to re-init the maxCommittedOffset in subscriber
	heapIndices := stats.offsetBasedMinHeap.Indices
	if len(heapIndices) > 0 {
		stats.maxCommittedOffset = heapIndices[0].Offset
	}

	return nil
}

// ModifyAckDeadline for messages
func (s *Subscriber) ModifyAckDeadline(_ context.Context, req *ModAckMessage) error {

	tp := req.ackMessage.ToTopicPartition()
	stats := s.consumedMessageStats[tp]

	if req.ackDeadline == 0 {
		// modAck with deadline = 0 means nack
		// https://github.com/googleapis/google-cloud-go/blob/pubsub/v1.10.0/pubsub/iterator.go#L348
		// TODO : remove from both heaps and push to retry queue
	}

	deadlineBasedHeap := stats.deadlineBasedMinHeap
	indexOfMsgInDeadlineBasedMinHeap := deadlineBasedHeap.MsgIDToIndexMapping[req.ackMessage.MessageID]

	// update the deadline of the identified message
	deadlineBasedHeap.Indices[indexOfMsgInDeadlineBasedMinHeap].AckDeadline = req.ackDeadline
	heap.Init(&deadlineBasedHeap)

	return nil
}

func (s *Subscriber) checkAndEvictBasedOnAckDeadline(_ context.Context) error {

	// do a deadline based eviction for all active topic-partition heaps
	for _, metadata := range s.consumedMessageStats {

		deadlineBasedHeap := metadata.deadlineBasedMinHeap

		// peek deadline heap
		peek := deadlineBasedHeap.Indices[0]

		// check eligibility for eviction
		if peek.HasHitDeadline() {
			evictedMsg1 := heap.Pop(&deadlineBasedHeap).(*customheap.AckMessageWithDeadline)

			offsetBasedHeap := metadata.offsetBasedMinHeap
			evictedMsg2 := heap.Remove(&offsetBasedHeap, offsetBasedHeap.MsgIDToIndexMapping[evictedMsg1.MsgID]).(*customheap.AckMessageWithOffset)

			delete(offsetBasedHeap.MsgIDToIndexMapping, evictedMsg1.MsgID)
			delete(deadlineBasedHeap.MsgIDToIndexMapping, evictedMsg2.MsgID)

			heap.Init(&offsetBasedHeap)
			heap.Init(&deadlineBasedHeap)

			// TODO : nack the evicted message as well
		}
	}

	return nil
}

// Run loop
func (s *Subscriber) Run(ctx context.Context) {
	go func(ctx context.Context, deadlineChan chan bool) {
		ticker := time.NewTicker(time.Duration(200) * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				deadlineChan <- true
			case <-ctx.Done():
				return
			}
		}
	}(ctx, s.deadlineTickerChan)
	for {
		select {
		case req := <-s.requestChan:
			// TODO : discuss pause / resume on maxOutstanding!

			// TODO: run receive request in another goroutine?
			resp1, err := s.consumer.ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: req.MaxNumOfMessages, TimeoutSec: s.timeoutInSec})
			if err != nil {
				logger.Ctx(ctx).Errorw("error in receiving messages", "msg", err.Error())
				s.errChan <- err
			}
			resp2, err := s.retryConsumer.ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: req.MaxNumOfMessages, TimeoutSec: s.timeoutInSec})
			if err != nil {
				logger.Ctx(ctx).Errorw("error in receiving retryable messages", "msg", err.Error())
				s.errChan <- err
			}
			// merge response from both the consumers
			responses := make([]*messagebroker.GetMessagesFromTopicResponse, 0)
			responses = append(responses, resp1)
			responses = append(responses, resp2)

			sm := make([]*metrov1.ReceivedMessage, 0)
			for _, resp := range responses {
				for _, msg := range resp.OffsetWithMessages {
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

					// store the processed r1 in a map for limit checks
					tp := NewTopicPartition(s.topic, msg.Partition)
					if _, ok := s.consumedMessageStats[tp]; !ok {
						// init the stats data store before updating
						s.consumedMessageStats[tp] = NewConsumptionMetadata()
					}

					ackDeadline := time.Now().Add(minAckDeadline).Unix()
					s.consumedMessageStats[tp].Store(&msg, ackDeadline)

					ackID := NewAckMessage(s.subscriberID, s.topic, msg.Partition, msg.Offset, int32(ackDeadline), msg.MessageID).BuildAckID()
					sm = append(sm, &metrov1.ReceivedMessage{AckId: ackID, Message: protoMsg, DeliveryAttempt: 1})
				}
			}
			s.responseChan <- metrov1.PullResponse{ReceivedMessages: sm}
		case ackRequest := <-s.ackChan:
			s.Acknowledge(ctx, ackRequest)
		case modAckRequest := <-s.modAckChan:
			s.ModifyAckDeadline(ctx, modAckRequest)
		case <-s.deadlineTickerChan:
			s.checkAndEvictBasedOnAckDeadline(ctx)
		case <-ctx.Done():
			s.closeChan <- struct{}{}
			return
		}
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

// GetAckChannel returns the chan from where ack is received
func (s *Subscriber) GetAckChannel() chan *AckMessage {
	return s.ackChan
}

// GetModAckChannel returns the chan where mod ack is written
func (s *Subscriber) GetModAckChannel() chan *ModAckMessage {
	return s.modAckChan
}

// Stop the subscriber
func (s *Subscriber) Stop() {
	s.cancelFunc()
	<-s.closeChan
}
