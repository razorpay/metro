package subscriber

import (
	"container/heap"
	"context"
	"time"

	"github.com/razorpay/metro/internal/subscriber/customheap"

	"github.com/golang/protobuf/proto"
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
	// not exporting acknowledge() and  modifyAckDeadline() intentionally so that
	// all operations happen over the channel
	acknowledge(ctx context.Context, req *AckMessage)
	modifyAckDeadline(ctx context.Context, req *ModAckMessage)
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
	subscriptionCore       subscription.ICore
	requestChan            chan *PullRequest
	responseChan           chan metrov1.PullResponse
	ackChan                chan *AckMessage
	modAckChan             chan *ModAckMessage
	errChan                chan error
	closeChan              chan struct{}
	deadlineTickerChan     chan bool
	timeoutInSec           int
	consumer               messagebroker.Consumer // consume messages from primary topic
	retryConsumer          messagebroker.Consumer // consume messages from retry topic
	retryProducer          messagebroker.Producer // produce messages to retry topic
	cancelFunc             func()
	maxOutstandingMessages int64
	maxOutstandingBytes    int64
	consumedMessageStats   map[TopicPartition]*ConsumptionMetadata
	isPaused               bool
}

// canConsumeMore looks at sum of all consumed messages in all the active topic partitions and checks threshold
func (s *Subscriber) canConsumeMore() bool {
	totalConsumedMsgsForTopic := 0
	for _, cm := range s.consumedMessageStats {
		totalConsumedMsgsForTopic += len(cm.consumedMessages)
	}
	return totalConsumedMsgsForTopic <= int(s.maxOutstandingMessages)
}

// GetID ...
func (s *Subscriber) GetID() string {
	return s.subscriberID
}

// commits existing message on primary topic and pushes message to the pre-defined retry topic
func (s *Subscriber) retry(ctx context.Context, retryMsg *RetryMessage) {
	// remove message from the primary topic
	_, err := s.consumer.CommitByPartitionAndOffset(ctx, messagebroker.CommitOnTopicRequest{
		Topic:     retryMsg.Topic,
		Partition: retryMsg.Partition,
		Offset:    retryMsg.Offset,
	})

	if err != nil {
		logger.Ctx(ctx).Errorw("subscriber: commit to primary topic failed", "topic", s.topic, "err", err.Error())
		s.errChan <- err
		return
	}

	// then push message to the retry topic
	_, err = s.retryProducer.SendMessage(ctx, messagebroker.SendMessageToTopicRequest{
		Topic:      s.retryTopic,
		Message:    retryMsg.Data,
		TimeoutSec: 50,
		MessageID:  retryMsg.MessageID,
	})

	if err != nil {
		logger.Ctx(ctx).Errorw("subscriber: push to retry topic failed", "topic", s.retryTopic, "err", err.Error())
		s.errChan <- err
	}
}

// acknowledge messages
func (s *Subscriber) acknowledge(ctx context.Context, req *AckMessage) {

	logger.Ctx(ctx).Infow("subscriber: got ack request", "ack_request", req.String())

	tp := req.ToTopicPartition()
	stats := s.consumedMessageStats[tp]

	if stats.offsetBasedMinHeap.IsEmpty() {
		return
	}

	// if somehow an ack request comes for a message that has met deadline eviction threshold
	if req.HasHitDeadline() {

		msgID := req.MessageID
		msg := stats.consumedMessages[msgID].(messagebroker.ReceivedMessage)

		// push to retry queue
		s.retry(ctx, NewRetryMessage(msg.Topic, msg.Partition, msg.Offset, msg.Data, msgID))

		removeMessageFromMemory(stats, req.MessageID)

		return
	}

	shouldCommit := false
	peek := stats.offsetBasedMinHeap.Indices[0]
	if req.Offset == peek.Offset {
		// NOTE: attempt a commit to broker only if the head of the offsetBasedMinHeap changes
		shouldCommit = true
	}

	if shouldCommit {
		_, err := s.consumer.CommitByPartitionAndOffset(ctx, messagebroker.CommitOnTopicRequest{
			Topic:     req.Topic,
			Partition: req.Partition,
			Offset:    req.Offset,
		})
		if err != nil {
			logger.Ctx(ctx).Errorw("subscriber: failed to commit message", "message", "peek", "error", err.Error())
			s.errChan <- err
			return
		}

		// after successful commit to broker, make sure to re-init the maxCommittedOffset in subscriber
		stats.maxCommittedOffset = req.Offset
	}

	removeMessageFromMemory(stats, req.MessageID)
}

// cleans up all occurrences for a given msgId from the internal data-structures
func removeMessageFromMemory(stats *ConsumptionMetadata, msgID string) {
	delete(stats.consumedMessages, msgID)

	// remove message from offsetBasedMinHeap
	indexOfMsgInOffsetBasedMinHeap := stats.offsetBasedMinHeap.MsgIDToIndexMapping[msgID]
	msg := heap.Remove(&stats.offsetBasedMinHeap, indexOfMsgInOffsetBasedMinHeap).(*customheap.AckMessageWithOffset)
	delete(stats.offsetBasedMinHeap.MsgIDToIndexMapping, msgID)
	heap.Init(&stats.offsetBasedMinHeap)

	// remove same message from deadlineBasedMinHeap
	indexOfMsgInDeadlineBasedMinHeap := stats.deadlineBasedMinHeap.MsgIDToIndexMapping[msg.MsgID]
	heap.Remove(&stats.deadlineBasedMinHeap, indexOfMsgInDeadlineBasedMinHeap)
	delete(stats.deadlineBasedMinHeap.MsgIDToIndexMapping, msgID)
	heap.Init(&stats.deadlineBasedMinHeap)
}

// modifyAckDeadline for messages
func (s *Subscriber) modifyAckDeadline(ctx context.Context, req *ModAckMessage) {

	logger.Ctx(ctx).Infow("subscriber: got mod ack request", "mod_ack_request", req.String())

	tp := req.ackMessage.ToTopicPartition()
	stats := s.consumedMessageStats[tp]

	deadlineBasedHeap := stats.deadlineBasedMinHeap
	if deadlineBasedHeap.IsEmpty() {
		return
	}

	if req.ackDeadline == 0 {
		// modAck with deadline = 0 means nack
		// https://github.com/googleapis/google-cloud-go/blob/pubsub/v1.10.0/pubsub/iterator.go#L348

		msgID := req.ackMessage.MessageID
		msg := stats.consumedMessages[msgID].(messagebroker.ReceivedMessage)

		// push to retry queue
		s.retry(ctx, NewRetryMessage(msg.Topic, msg.Partition, msg.Offset, msg.Data, msgID))

		// cleanup message from memory
		removeMessageFromMemory(stats, msgID)

		return
	}

	// NOTE: currently we are not supporting non-zero mod ack. below code implementation is to handle that in future
	indexOfMsgInDeadlineBasedMinHeap := deadlineBasedHeap.MsgIDToIndexMapping[req.ackMessage.MessageID]

	// update the deadline of the identified message
	deadlineBasedHeap.Indices[indexOfMsgInDeadlineBasedMinHeap].AckDeadline = req.ackDeadline
	heap.Init(&deadlineBasedHeap)
}

func (s *Subscriber) checkAndEvictBasedOnAckDeadline(ctx context.Context) {

	// do a deadline based eviction for all active topic-partition heaps
	for _, metadata := range s.consumedMessageStats {

		deadlineBasedHeap := metadata.deadlineBasedMinHeap
		if deadlineBasedHeap.IsEmpty() {
			continue
		}

		// peek deadline heap
		peek := deadlineBasedHeap.Indices[0]

		// check eligibility for eviction
		if peek.HasHitDeadline() {

			msgID := peek.MsgID
			msg := metadata.consumedMessages[msgID].(messagebroker.ReceivedMessage)

			// NOTE :  if push to retry queue fails due to any error, we do not delete from the deadline heap
			// this way the message is eligible to be retried
			s.retry(ctx, NewRetryMessage(msg.Topic, msg.Partition, msg.Offset, msg.Data, msgID))

			// cleanup message from memory only after a successful push to retry topic
			removeMessageFromMemory(metadata, peek.MsgID)

			logger.Ctx(ctx).Infow("subscriber: deadline eviction: message evicted", "msgId", peek.MsgID)
		}
	}
}

// Run loop
func (s *Subscriber) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(200) * time.Millisecond)
	go func(ctx context.Context, deadlineChan chan bool) {
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
			if s.canConsumeMore() == false {
				// check if consumer is paused once maxOutstanding messages limit is hit
				if s.isPaused == false {
					// if not, pause all topic-partitions for consumer
					for tp := range s.consumedMessageStats {
						s.consumer.Pause(ctx, messagebroker.PauseOnTopicRequest{
							Topic:     tp.topic,
							Partition: tp.partition,
						})
					}
				}
			} else {
				// resume consumer if paused and is allowed to consume more messages
				if s.isPaused {
					s.isPaused = false
					for tp := range s.consumedMessageStats {
						s.consumer.Resume(ctx, messagebroker.ResumeOnTopicRequest{
							Topic:     tp.topic,
							Partition: tp.partition,
						})
					}
				}
			}

			// TODO: run receive request in another goroutine?
			resp1, err := s.consumer.ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: req.MaxNumOfMessages, TimeoutSec: s.timeoutInSec})
			if err != nil {
				logger.Ctx(ctx).Errorw("subscriber: error in receiving messages", "msg", err.Error())
				s.errChan <- err
				return
			}
			logger.Ctx(ctx).Infow("subscriber: got messages from primary topic", "count", len(resp1.PartitionOffsetWithMessages), "messages", resp1.PartitionOffsetWithMessages)
			resp2, err := s.retryConsumer.ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: req.MaxNumOfMessages, TimeoutSec: s.timeoutInSec})
			if err != nil {
				logger.Ctx(ctx).Errorw("subscriber: error in receiving retryable messages", "msg", err.Error())
				s.errChan <- err
				return
			}
			logger.Ctx(ctx).Infow("subscriber: got messages from retry topic", "count", len(resp2.PartitionOffsetWithMessages), "messages", resp2.PartitionOffsetWithMessages)

			// merge response from both the consumers
			responses := make([]*messagebroker.GetMessagesFromTopicResponse, 0)
			responses = append(responses, resp1)
			responses = append(responses, resp2)

			sm := make([]*metrov1.ReceivedMessage, 0)
			for _, resp := range responses {
				for _, msg := range resp.PartitionOffsetWithMessages {
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
					tp := NewTopicPartition(msg.Topic, msg.Partition)
					if _, ok := s.consumedMessageStats[tp]; !ok {
						// init the stats data store before updating
						s.consumedMessageStats[tp] = NewConsumptionMetadata()

						// query and set the max committed offset for each topic partition
						resp, err := s.consumer.GetTopicMetadata(ctx, messagebroker.GetTopicMetadataRequest{
							Topic:     s.topic,
							Partition: msg.Partition,
						})

						if err != nil {
							s.errChan <- err

						}
						s.consumedMessageStats[tp].maxCommittedOffset = resp.Offset
					}

					ackDeadline := time.Now().Add(minAckDeadline).Unix()
					s.consumedMessageStats[tp].Store(msg, ackDeadline)

					ackID := NewAckMessage(s.subscriberID, msg.Topic, msg.Partition, msg.Offset, int32(ackDeadline), msg.MessageID).BuildAckID()
					sm = append(sm, &metrov1.ReceivedMessage{AckId: ackID, Message: protoMsg, DeliveryAttempt: 1})
				}
			}
			s.responseChan <- metrov1.PullResponse{ReceivedMessages: sm}
		case ackRequest := <-s.ackChan:
			s.acknowledge(ctx, ackRequest)
		case modAckRequest := <-s.modAckChan:
			s.modifyAckDeadline(ctx, modAckRequest)
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
