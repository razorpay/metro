package subscriber

import (
	"container/heap"
	"context"
	"time"

	"github.com/razorpay/metro/internal/brokerstore"

	"github.com/razorpay/metro/internal/subscriber/customheap"

	"github.com/golang/protobuf/proto"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	deadlineTickerInterval  = 200 * time.Millisecond
	minAckDeadline          = 10 * time.Minute
	maxMessageRetryAttempts = 2
)

// ISubscriber is interface over high level subscriber
type ISubscriber interface {
	GetID() string
	GetSubscription() string
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
	dlqTopic               string
	subscriberID           string
	subscriptionCore       subscription.ICore
	requestChan            chan *PullRequest
	responseChan           chan metrov1.PullResponse
	ackChan                chan *AckMessage
	modAckChan             chan *ModAckMessage
	deadlineTicker         *time.Ticker
	errChan                chan error
	closeChan              chan struct{}
	timeoutInMs            int
	consumer               messagebroker.Consumer // consume messages from primary topic
	retryProducer          messagebroker.Producer // produce messages to retry topic
	dlqProducer            messagebroker.Producer // produce messages to dlq topic
	cancelFunc             func()
	maxOutstandingMessages int64
	maxOutstandingBytes    int64
	consumedMessageStats   map[TopicPartition]*ConsumptionMetadata
	isPaused               bool
	ctx                    context.Context
	bs                     brokerstore.IBrokerStore
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

// GetSubscription ...
func (s *Subscriber) GetSubscription() string {
	return s.subscription
}

// commits existing message on primary topic and pushes message to the pre-defined retry topic
func (s *Subscriber) retry(ctx context.Context, retryMsg *RetryMessage) {

	// remove message from the primary topic
	_, err := s.consumer.CommitByPartitionAndOffset(ctx, messagebroker.CommitOnTopicRequest{
		Topic:     retryMsg.Topic,
		Partition: retryMsg.Partition,
		// add 1 to current offset
		// https://docs.confluent.io/5.5.0/clients/confluent-kafka-go/index.html#pkg-overview
		Offset: retryMsg.Offset + 1,
	})

	if err != nil {
		logger.Ctx(ctx).Errorw("subscriber: commit to primary topic failed",
			"topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID, "err", err.Error())
		s.errChan <- err
		return
	}

	// check max retries.
	if retryMsg.RetryCount >= maxMessageRetryAttempts {
		// then push message to the dlq topic
		logger.Ctx(ctx).Infow("subscriber: max retries exceeded. pushing to dlq topic", "retryMsg", retryMsg,
			"dlq", s.dlqTopic, "subscription", s.subscription, "subscriberId", s.subscriberID)
		_, err = s.dlqProducer.SendMessage(ctx, messagebroker.SendMessageToTopicRequest{
			Topic:     s.dlqTopic,
			Message:   retryMsg.Data,
			TimeoutMs: 50,
		})

		if err != nil {
			logger.Ctx(ctx).Errorw("subscriber: push to dlq topic failed", "topic", s.dlqTopic,
				"subscription", s.subscription, "subscriberId", s.subscriberID, "err", err.Error())
			s.errChan <- err
			return
		}

		subscriberMessagesRetried.WithLabelValues(env, s.dlqTopic, s.subscription).Inc()
		logger.Ctx(ctx).Infow("subscriber: msg pushed to dlq topic", "topic", s.dlqTopic,
			"subscription", s.subscription, "subscriberId", s.subscriberID, "dlqMsg", retryMsg)
		return
	}

	// then push message to the retry topic
	_, err = s.retryProducer.SendMessage(ctx, messagebroker.SendMessageToTopicRequest{
		Topic:      s.retryTopic,
		Message:    retryMsg.Data,
		TimeoutMs:  50,
		MessageID:  retryMsg.MessageID,
		RetryCount: retryMsg.incrementAndGetRetryCount(),
	})

	if err != nil {
		logger.Ctx(ctx).Errorw("subscriber: push to retry topic failed", "topic",
			"subscription", s.subscription, "subscriberId", s.subscriberID, s.retryTopic, "err", err.Error())
		s.errChan <- err
		return
	}

	subscriberMessagesRetried.WithLabelValues(env, s.retryTopic, s.subscription).Inc()
	logger.Ctx(ctx).Infow("subscriber: msg pushed to retry topic", "topic", s.topic, "subscription", s.subscription,
		"subscriberId", s.subscriberID, "retryMsg", retryMsg)
}

// acknowledge messages
func (s *Subscriber) acknowledge(ctx context.Context, req *AckMessage) {

	logger.Ctx(ctx).Infow("subscriber: got ack request", "ack_request", req.String(), "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID)

	tp := req.ToTopicPartition()
	stats := s.consumedMessageStats[tp]

	if stats.offsetBasedMinHeap.IsEmpty() {
		return
	}

	msgID := req.MessageID
	// check if message is present in-memory or not
	if _, ok := stats.consumedMessages[msgID]; !ok {
		logger.Ctx(ctx).Infow("subscriber: skipping ack as message not found in-memory", "ack_request", req.String(), "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID)
		return
	}

	msg := stats.consumedMessages[msgID].(messagebroker.ReceivedMessage)

	// if somehow an ack request comes for a message that has met deadline eviction threshold
	if req.HasHitDeadline() {

		// push to retry queue
		s.retry(ctx, NewRetryMessage(msg.Topic, msg.Partition, msg.Offset, msg.Data, msgID, msg.RetryCount))

		removeMessageFromMemory(stats, req.MessageID)

		return
	}

	offsetToCommit := req.Offset
	shouldCommit := false
	peek := stats.offsetBasedMinHeap.Indices[0]

	logger.Ctx(ctx).Infow("subscriber: offsets in ack", "req offset", req.Offset, "peek offset", peek.Offset, "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID)
	if offsetToCommit == peek.Offset {
		// NOTE: attempt a commit to broker only if the head of the offsetBasedMinHeap changes
		shouldCommit = true

		logger.Ctx(ctx).Infow("subscriber: evicted offsets", "stats.evictedButNotCommittedOffsets", stats.evictedButNotCommittedOffsets, "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID)
		// find if any previously evicted offsets can be committed as well
		// eg. if we get an commit for 5, check for 6,7,8...etc have previously been evicted.
		// in such cases we can commit the max contiguous offset available directly instead of 5.
		newOffset := offsetToCommit
		for {
			if stats.evictedButNotCommittedOffsets[newOffset+1] {
				delete(stats.evictedButNotCommittedOffsets, newOffset+1)
				newOffset++
				continue
			}
			if offsetToCommit != newOffset {
				logger.Ctx(ctx).Infow("subscriber: updating offset to commit", "old", offsetToCommit, "new", newOffset, "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID)
				offsetToCommit = newOffset
			}
			break
		}
	}

	if shouldCommit {
		_, err := s.consumer.CommitByPartitionAndOffset(ctx, messagebroker.CommitOnTopicRequest{
			Topic:     req.Topic,
			Partition: req.Partition,
			// add 1 to current offset
			// https://docs.confluent.io/5.5.0/clients/confluent-kafka-go/index.html#pkg-overview
			Offset: offsetToCommit + 1,
		})
		if err != nil {
			logger.Ctx(ctx).Errorw("subscriber: failed to commit message", "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID, "error", err.Error())
			s.errChan <- err
			return
		}
		// after successful commit to broker, make sure to re-init the maxCommittedOffset in subscriber
		stats.maxCommittedOffset = offsetToCommit
		logger.Ctx(ctx).Infow("subscriber: max committed offset new value", "offset", offsetToCommit, "topic-partition", tp, "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID)
	}

	removeMessageFromMemory(stats, req.MessageID)

	// add to eviction map only in case of any out of order eviction
	if offsetToCommit > stats.maxCommittedOffset {
		stats.evictedButNotCommittedOffsets[offsetToCommit] = true
	}

	subscriberMessagesAckd.WithLabelValues(env, s.topic, s.subscription).Inc()
	subscriberTimeTakenToAckMsg.WithLabelValues(env, s.topic, s.subscription).Observe(float64(time.Since(msg.PublishTime).Nanoseconds() / 1e9))
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

	logger.Ctx(ctx).Infow("subscriber: got mod ack request", "mod_ack_request", req.String(), "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID)

	tp := req.ackMessage.ToTopicPartition()
	stats := s.consumedMessageStats[tp]

	deadlineBasedHeap := stats.deadlineBasedMinHeap
	if deadlineBasedHeap.IsEmpty() {
		return
	}

	msgID := req.ackMessage.MessageID
	// check if message is present in-memory or not
	if _, ok := stats.consumedMessages[msgID]; !ok {
		logger.Ctx(ctx).Infow("subscriber: skipping mod ack as message not found in-memory", "mod_ack_request", req.String(), "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID)
		return
	}

	msg := stats.consumedMessages[msgID].(messagebroker.ReceivedMessage)

	if req.ackDeadline == 0 {
		// modAck with deadline = 0 means nack
		// https://github.com/googleapis/google-cloud-go/blob/pubsub/v1.10.0/pubsub/iterator.go#L348

		// push to retry queue
		s.retry(ctx, NewRetryMessage(msg.Topic, msg.Partition, msg.Offset, msg.Data, msgID, msg.RetryCount))

		// cleanup message from memory
		removeMessageFromMemory(stats, msgID)

		subscriberMessagesModAckd.WithLabelValues(env, s.topic, s.subscription).Inc()
		subscriberTimeTakenToModAckMsg.WithLabelValues(env, s.topic, s.subscription).Observe(float64(time.Since(msg.PublishTime).Nanoseconds() / 1e9))

		return
	}

	// NOTE: currently we are not supporting non-zero mod ack. below code implementation is to handle that in future
	indexOfMsgInDeadlineBasedMinHeap := deadlineBasedHeap.MsgIDToIndexMapping[req.ackMessage.MessageID]

	// update the deadline of the identified message
	deadlineBasedHeap.Indices[indexOfMsgInDeadlineBasedMinHeap].AckDeadline = req.ackDeadline
	heap.Init(&deadlineBasedHeap)

	subscriberMessagesModAckd.WithLabelValues(env, s.topic, s.subscription).Inc()
	subscriberTimeTakenToModAckMsg.WithLabelValues(env, s.topic, s.subscription).Observe(float64(time.Since(msg.PublishTime).Nanoseconds() / 1e9))
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
			if _, ok := metadata.consumedMessages[msgID]; !ok {
				// check if message is present in-memory or not
				continue
			}
			msg := metadata.consumedMessages[msgID].(messagebroker.ReceivedMessage)

			// NOTE :  if push to retry queue fails due to any error, we do not delete from the deadline heap
			// this way the message is eligible to be retried
			s.retry(ctx, NewRetryMessage(msg.Topic, msg.Partition, msg.Offset, msg.Data, msgID, msg.RetryCount))

			// cleanup message from memory only after a successful push to retry topic
			removeMessageFromMemory(metadata, peek.MsgID)

			logger.Ctx(ctx).Infow("subscriber: deadline eviction: message evicted", "msgId", peek.MsgID, "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID)
			subscriberMessagesDeadlineEvicted.WithLabelValues(env, s.topic, s.subscription).Inc()
		}
	}
}

// Run loop
func (s *Subscriber) Run(ctx context.Context) {
	for {
		select {
		case req := <-s.requestChan:
			if s.canConsumeMore() == false {
				logger.Ctx(ctx).Infow("subscriber: cannot consume more messages before acking", "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID)
				// check if consumer is paused once maxOutstanding messages limit is hit
				if s.isPaused == false {
					// if not, pause all topic-partitions for consumer
					for tp := range s.consumedMessageStats {
						s.consumer.Pause(ctx, messagebroker.PauseOnTopicRequest{
							Topic:     tp.topic,
							Partition: tp.partition,
						})
						logger.Ctx(ctx).Infow("subscriber: pausing consumer", "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID)
						subscriberPausedConsumersTotal.WithLabelValues(env, s.topic, s.subscription).Inc()
						s.isPaused = true

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
						logger.Ctx(ctx).Infow("subscriber: resuming consumer", "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID)
						subscriberPausedConsumersTotal.WithLabelValues(env, s.topic, s.subscription).Dec()
					}
				}
			}

			resp, err := s.consumer.ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: req.MaxNumOfMessages, TimeoutMs: s.timeoutInMs})
			if err != nil {
				logger.Ctx(ctx).Errorw("subscriber: error in receiving messages", "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID, "msg", err.Error())
				s.errChan <- err
				return
			}

			if len(resp.PartitionOffsetWithMessages) > 0 {
				logger.Ctx(ctx).Infow("subscriber: non-zero messages from topics", "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID, "message_count", len(resp.PartitionOffsetWithMessages), "messages", resp.PartitionOffsetWithMessages)
			}

			sm := make([]*metrov1.ReceivedMessage, 0)
			for _, msg := range resp.PartitionOffsetWithMessages {
				protoMsg := &metrov1.PubsubMessage{}
				err = proto.Unmarshal(msg.Data, protoMsg)
				if err != nil {
					s.errChan <- err
					continue
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
						continue
					}
					s.consumedMessageStats[tp].maxCommittedOffset = resp.Offset
				}

				ackDeadline := time.Now().Add(minAckDeadline).Unix()
				s.consumedMessageStats[tp].Store(msg, ackDeadline)

				ackID := NewAckMessage(s.subscriberID, msg.Topic, msg.Partition, msg.Offset, int32(ackDeadline), msg.MessageID).BuildAckID()
				sm = append(sm, &metrov1.ReceivedMessage{AckId: ackID, Message: protoMsg, DeliveryAttempt: 1})

				subscriberMessagesConsumed.WithLabelValues(env, msg.Topic, s.subscription).Inc()
				subscriberMemoryMessagesCountTotal.WithLabelValues(env, s.topic, s.subscription).Set(float64(len(s.consumedMessageStats[tp].consumedMessages)))
				subscriberTimeTakenFromPublishToConsumeMsg.WithLabelValues(env, s.topic, s.subscription).Observe(float64(time.Since(msg.PublishTime).Nanoseconds() / 1e9))
			}
			s.responseChan <- metrov1.PullResponse{ReceivedMessages: sm}

		case ackRequest := <-s.ackChan:
			s.acknowledge(ctx, ackRequest)
		case modAckRequest := <-s.modAckChan:
			s.modifyAckDeadline(ctx, modAckRequest)
		case <-s.deadlineTicker.C:
			s.checkAndEvictBasedOnAckDeadline(ctx)
		case <-ctx.Done():
			logger.Ctx(s.ctx).Infow("subscriber: <-ctx.Done() called", "topic", s.topic, "subscription", s.subscription, "subscriberId", s.subscriberID)

			// stop the ticker
			s.deadlineTicker.Stop()

			wasConsumerFound := s.bs.RemoveConsumer(s.ctx, s.subscriberID, messagebroker.ConsumerClientOptions{GroupID: s.subscription})
			if wasConsumerFound {
				// close consumer only if we are able to successfully find and delete consumer from the brokerStore.
				// if the entry is already deleted from brokerStore, that means some other goroutine has already closed the consumer.
				// in such cases do not attempt to close the consumer again else it will panic
				s.consumer.Close(s.ctx)
			}
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
	// close all active channels on stop
	defer close(s.requestChan)
	defer close(s.responseChan)
	defer close(s.ackChan)
	defer close(s.modAckChan)
	defer close(s.errChan)
	defer close(s.closeChan)

	s.cancelFunc()
	<-s.closeChan
}
