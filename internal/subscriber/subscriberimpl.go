package subscriber

import (
	"container/heap"
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/razorpay/metro/internal/offset"
	"github.com/razorpay/metro/internal/subscriber/customheap"
	"github.com/razorpay/metro/internal/subscriber/retry"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BasicImplementation provides implementation of subscriber functionalities
type BasicImplementation struct {
	maxOutstandingMessages int64
	maxOutstandingBytes    int64
	topic                  string
	subscriberID           string
	consumer               IConsumer // consume messages from primary topic and retry topic
	offsetCore             offset.ICore
	retrier                retry.IRetrier
	ctx                    context.Context
	subscription           *subscription.Model
	consumedMessageStats   map[TopicPartition]*ConsumptionMetadata
}

// GetSubscriberID ...
func (s *BasicImplementation) GetSubscriberID() string {
	return s.subscriberID
}

// GetSubscription ...
func (s *BasicImplementation) GetSubscription() *subscription.Model {
	return s.subscription
}

// GetConsumerLag returns perceived lag for the gievn Subscriber
func (s *BasicImplementation) GetConsumerLag() map[string]uint64 {

	lag, _ := s.consumer.GetConsumerLag(s.ctx)

	return lag
}

// CanConsumeMore looks at sum of all consumed messages in all the active topic partitions and checks threshold
func (s *BasicImplementation) CanConsumeMore() bool {
	totalConsumedMsgsForTopic := 0
	for _, cm := range s.consumedMessageStats {
		totalConsumedMsgsForTopic += len(cm.consumedMessages)
	}
	return totalConsumedMsgsForTopic <= int(s.maxOutstandingMessages)
}

// Pull pulls message from the broker and publishes it into the response channel
func (s *BasicImplementation) Pull(ctx context.Context, req *PullRequest, responseChan chan *metrov1.PullResponse, errChan chan error) {
	caseStartTime := time.Now()
	defer func() {
		subscriberTimeTakenInRequestChannelCase.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(caseStartTime).Seconds())
	}()

	if s.consumer == nil {
		logger.Ctx(ctx).Errorw("subscriber: consumer is nil", "logFields", getLogFields(s))
		return
	}

	messages, err := receiveMessages(ctx, s, s.consumer, req)
	if err != nil {
		notifyPullMessageError(ctx, s, err, responseChan, errChan)
		return
	}

	if len(messages) > 0 {
		logFields := getLogFields(s)
		logFields["messageCount"] = len(messages)
		logger.Ctx(ctx).Infow("subscriber: non-zero messages from topics", "logFields", logFields)
	}

	sm := make([]*metrov1.ReceivedMessage, 0)
	for _, msg := range messages {
		protoMsg := &metrov1.PubsubMessage{}
		err = proto.Unmarshal(msg.Data, protoMsg)
		if err != nil {
			logger.Ctx(ctx).Errorw("subscriber: error in proto unmarshal", "logFields", getLogFields(s), "error", err.Error())
			errChan <- err
			continue
		}

		// set messageID and publish time
		protoMsg.MessageId = msg.MessageID
		ts := &timestamppb.Timestamp{}
		ts.Seconds = msg.PublishTime.Unix()
		protoMsg.PublishTime = ts

		if len(protoMsg.Attributes) == 0 {
			protoMsg.Attributes = make(map[string]string, 1)
		}

		for _, attribute := range msg.Attributes {
			if val, ok := attribute[messagebroker.UberTraceID]; ok {
				protoMsg.Attributes[messagebroker.UberTraceID] = string(val)
			}
		}

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
				logger.Ctx(ctx).Errorw("subscriber: error in reading topic metadata", "logFields", getLogFields(s), "error", err.Error())
				errChan <- err
				continue
			}
			s.consumedMessageStats[tp].maxCommittedOffset = resp.Offset
		}

		ackDeadline := time.Now().Add(minAckDeadline).Unix()
		s.consumedMessageStats[tp].Store(msg, ackDeadline)

		ackMessage, err := NewAckMessage(s.subscriberID, msg.Topic, msg.Partition, msg.Offset, int32(ackDeadline), msg.MessageID)
		if err != nil {
			logger.Ctx(ctx).Errorw("subscriber: error in creating AckMessage", "logFields", getLogFields(s), "error", err.Error())
			errChan <- err
			continue
		}
		ackID := ackMessage.BuildAckID()
		sm = append(sm, &metrov1.ReceivedMessage{AckId: ackID, Message: protoMsg, DeliveryAttempt: msg.CurrentRetryCount + 1})

		subscriberMessagesConsumed.WithLabelValues(env, msg.Topic, s.subscription.Name, s.subscriberID).Inc()
		subscriberMemoryMessagesCountTotal.WithLabelValues(env, s.topic, s.subscription.Name, s.subscriberID).Set(float64(len(s.consumedMessageStats[tp].consumedMessages)))
		subscriberTimeTakenFromPublishToConsumeMsg.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(msg.PublishTime).Seconds())
	}

	sm = filterMessages(ctx, s, sm, errChan)

	if len(sm) > 0 {
		s.logInMemoryStats(ctx)
	}
	responseChan <- &metrov1.PullResponse{ReceivedMessages: sm}
}

// Acknowledge acknowledges a message pulled for delivery
func (s *BasicImplementation) Acknowledge(ctx context.Context, req *AckMessage, errChan chan error) {
	if req == nil {
		return
	}

	logFields := getLogFields(s)
	logFields["messageId"] = req.MessageID

	ackStartTime := time.Now()
	defer func() {
		logFields["ackTimeTaken"] = time.Now().Sub(ackStartTime).Seconds()
		logger.Ctx(ctx).Infow("subscriber: ack request end", "logFields", logFields)
	}()

	logger.Ctx(ctx).Infow("subscriber: got ack request", "logFields", logFields)

	tp := req.ToTopicPartition()
	stats := s.consumedMessageStats[tp]

	if stats.offsetBasedMinHeap.IsEmpty() {
		return
	}

	msgID := req.MessageID
	// check if message is present in-memory or not
	if _, ok := stats.consumedMessages[msgID]; !ok {
		logger.Ctx(ctx).Infow("subscriber: skipping ack as message not found in-memory", "logFields", logFields)
		return
	}

	msg := stats.consumedMessages[msgID].(messagebroker.ReceivedMessage)
	s.commitAndRemoveFromMemory(ctx, msg, errChan)

	// if somehow an ack request comes for a message that has met deadline eviction threshold
	if req.HasHitDeadline() {

		logger.Ctx(ctx).Infow("subscriberimpl: retry on ack since req has hit deadline", msg.LogFields()...)
		// push for retry
		s.retry(ctx, s, s.consumer, s.retrier, msg, errChan)

		return
	}

	subscriberMessagesAckd.WithLabelValues(env, s.topic, s.subscription.Name, s.subscriberID).Inc()
	subscriberTimeTakenToAckMsg.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(msg.PublishTime).Seconds())
	subscriberNumberOfRetainedAckedMessages.WithLabelValues(env, s.topic, s.subscription.Name).Inc()
	subscriberRetainedAckedMessagesSize.WithLabelValues(env, s.topic, s.subscription.Name).Add(float64(len(msg.Data)))
}

// ModAckDeadline modifies ack deadline
func (s *BasicImplementation) ModAckDeadline(ctx context.Context, req *ModAckMessage, errChan chan error) {
	if req == nil {
		return
	}

	logFields := getLogFields(s)
	logFields["messageId"] = req.AckMessage.MessageID
	logFields["currentTopic"] = req.AckMessage.Topic

	logger.Ctx(ctx).Infow("subscriber: got mod ack request", "logFields", logFields)

	tp := req.AckMessage.ToTopicPartition()
	stats := s.consumedMessageStats[tp]

	deadlineBasedHeap := stats.deadlineBasedMinHeap
	if deadlineBasedHeap.IsEmpty() {
		return
	}

	msgID := req.AckMessage.MessageID
	// check if message is present in-memory or not
	if _, ok := stats.consumedMessages[msgID]; !ok {
		logger.Ctx(ctx).Infow("subscriber: skipping mod ack as message not found in-memory", "logFields", logFields)
		return
	}

	msg := stats.consumedMessages[msgID].(messagebroker.ReceivedMessage)

	logger.Ctx(ctx).Infow("subscriber: logging message details", "consumedMessages", len(stats.consumedMessages), "msgId", msgID)
	if req.ackDeadline == 0 {
		// modAck with deadline = 0 means nack
		// https://github.com/googleapis/google-cloud-go/blob/pubsub/v1.10.0/pubsub/iterator.go#L348

		logger.Ctx(ctx).Infow("subscriberimpl: retry due to modack 0", msg.LogFields()...)
		// push to retry queue
		s.retry(ctx, s, s.consumer, s.retrier, msg, errChan)

		subscriberMessagesModAckd.WithLabelValues(env, s.topic, s.subscription.Name, s.subscriberID).Inc()
		subscriberTimeTakenToModAckMsg.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(msg.PublishTime).Seconds())

		return
	}

	// NOTE: currently we are not supporting non-zero mod ack. below code implementation is to handle that in future
	indexOfMsgInDeadlineBasedMinHeap := deadlineBasedHeap.MsgIDToIndexMapping[req.AckMessage.MessageID]

	// update the deadline of the identified message
	deadlineBasedHeap.Indices[indexOfMsgInDeadlineBasedMinHeap].AckDeadline = req.ackDeadline
	heap.Init(&deadlineBasedHeap)

	subscriberMessagesModAckd.WithLabelValues(env, s.topic, s.subscription.Name, s.subscriberID).Inc()
	subscriberTimeTakenToModAckMsg.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(msg.PublishTime).Seconds())
}

// EvictUnackedMessagesPastDeadline evicts messages past ack deadline
func (s *BasicImplementation) EvictUnackedMessagesPastDeadline(ctx context.Context, errChan chan error) {
	logFields := getLogFields(s)
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
			logger.Ctx(ctx).Infow("subscriberimpl: evict unacked msgs past deadline retry", msg.LogFields()...)
			// NOTE :  if push to retry queue fails due to any error, we do not delete from the deadline heap
			// this way the message is eligible to be retried
			s.retry(ctx, s, s.consumer, s.retrier, msg, errChan)

			logFields["messageId"] = peek.MsgID
			logger.Ctx(ctx).Infow("subscriber: deadline eviction: message evicted", "logFields", logFields)
			subscriberMessagesDeadlineEvicted.WithLabelValues(env, s.topic, s.subscription.Name).Inc()
		}
	}
}

// cleans up all occurrences for a given msgId from the internal data-structures
func (s *BasicImplementation) removeMessageFromMemory(ctx context.Context, stats *ConsumptionMetadata, msgID string) {
	if stats == nil || msgID == "" {
		return
	}

	start := time.Now()

	delete(stats.consumedMessages, msgID)

	// remove message from offsetBasedMinHeap
	indexOfMsgInOffsetBasedMinHeap := stats.offsetBasedMinHeap.MsgIDToIndexMapping[msgID]
	msg := heap.Remove(&stats.offsetBasedMinHeap, indexOfMsgInOffsetBasedMinHeap).(*customheap.AckMessageWithOffset)
	delete(stats.offsetBasedMinHeap.MsgIDToIndexMapping, msgID)

	// remove same message from deadlineBasedMinHeap
	indexOfMsgInDeadlineBasedMinHeap := stats.deadlineBasedMinHeap.MsgIDToIndexMapping[msg.MsgID]
	heap.Remove(&stats.deadlineBasedMinHeap, indexOfMsgInDeadlineBasedMinHeap)
	delete(stats.deadlineBasedMinHeap.MsgIDToIndexMapping, msgID)

	s.logInMemoryStats(ctx)

	subscriberMemoryMessagesCountTotal.WithLabelValues(env, s.topic, s.subscription.Name, s.subscriberID).Dec()
	subscriberTimeTakenToRemoveMsgFromMemory.WithLabelValues(env).Observe(time.Now().Sub(start).Seconds())
}

func (s *BasicImplementation) logInMemoryStats(ctx context.Context) {
	logger.Ctx(ctx).Infow("subscriber: in-memory stats", "logFields", getLogFields(s), "stats", s.GetConsumedMessagesStats())
}

// GetConsumedMessagesStats ...
func (s *BasicImplementation) GetConsumedMessagesStats() map[string]interface{} {
	st := make(map[string]interface{})

	for tp, stats := range s.consumedMessageStats {
		total := map[string]interface{}{
			"offsetBasedMinHeap_size":            stats.offsetBasedMinHeap.Len(),
			"deadlineBasedMinHeap_size":          stats.deadlineBasedMinHeap.Len(),
			"consumedMessages_size":              len(stats.consumedMessages),
			"evictedButNotCommittedOffsets_size": len(stats.evictedButNotCommittedOffsets),
			"maxCommittedOffset":                 stats.maxCommittedOffset,
		}
		st[tp.String()] = total
	}
	return st
}

func (s *BasicImplementation) retry(ctx context.Context, i Implementation, consumer IConsumer,
	retrier retry.IRetrier, msg messagebroker.ReceivedMessage, errChan chan error) {

	err := retryMessage(ctx, i, consumer, retrier, msg)
	if err != nil {
		logger.Ctx(ctx).Errorw("subscriberimpl: error in retryMessage", err.Error())
	}
	s.commitAndRemoveFromMemory(ctx, msg, errChan)
}

func (s *BasicImplementation) commitAndRemoveFromMemory(ctx context.Context, msg messagebroker.ReceivedMessage, errChan chan error) {
	logFields := getLogFields(s)
	logFields["parition"] = msg.Partition

	tp := TopicPartition{topic: msg.Topic, partition: msg.Partition}
	stats := s.consumedMessageStats[tp]

	offsetToCommit := msg.Offset
	shouldCommit := false
	peek := stats.offsetBasedMinHeap.Indices[0]

	logger.Ctx(ctx).Infow("subscriber: offsets in ack pre heap evaluation", "stats", stats, "logFields", logFields, "req offset", msg.Offset, "peek offset", peek.Offset, "msgId", msg.MessageID, "topic", msg.CurrentTopic, "partition", msg.Partition)
	if offsetToCommit == peek.Offset {
		start := time.Now()
		// NOTE: attempt a commit to broker only if the head of the offsetBasedMinHeap changes
		shouldCommit = true

		logger.Ctx(ctx).Infow("subscriber: evicted offsets", "logFields", logFields, "stats.evictedButNotCommittedOffsets", stats.evictedButNotCommittedOffsets)
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
				logger.Ctx(ctx).Infow("subscriber: updating offset to commit", "logFields", logFields, "old", offsetToCommit, "new", newOffset)
				offsetToCommit = newOffset
			}
			break
		}
		subscriberTimeTakenToIdentifyNextOffset.WithLabelValues(env).Observe(time.Now().Sub(start).Seconds())
	} else if offsetToCommit > peek.Offset && msg.CurrentTopic != msg.SourceTopic {
		// This is a stop-gap fix and will reduce strain on the system.
		// But this could mean messages are committed before they are ack'd/nack'd but after they're attempted in the delay queues.
		// In case of a nack in delay queues, they are anyway sent to retry.
		shouldCommit = true
	}

	logger.Ctx(ctx).Infow("subscriber: offsets in ack post heap evaluation", "stats", stats, "shouldCommit", shouldCommit, "logFields", logFields, "req offset", msg.Offset, "peek offset", peek.Offset, "msgId", msg.MessageID, "topic", msg.CurrentTopic, "partition", msg.Partition)

	if shouldCommit {
		offsetUpdated := true
		offsetModel := offset.Model{
			Topic:        s.subscription.Topic,
			Subscription: s.subscription.ExtractedSubscriptionName,
			Partition:    msg.Partition,
			LatestOffset: offsetToCommit + 1,
		}
		err := s.offsetCore.SetOffset(ctx, &offsetModel)
		if err != nil {
			// DO NOT terminate here since registry update failures should not affect message broker commits
			logger.Ctx(ctx).Errorw("subscriber: failed to store offset in registry", "logFields", logFields)
			offsetUpdated = false
		}
		_, err = s.consumer.CommitByPartitionAndOffset(ctx, messagebroker.CommitOnTopicRequest{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			// add 1 to current offset
			// https://docs.confluent.io/5.5.0/clients/confluent-kafka-go/index.html#pkg-overview
			Offset: offsetToCommit + 1,
		})
		if err != nil {
			logFields["error"] = err.Error()
			logger.Ctx(ctx).Errorw("subscriber: failed to commit message", "logFields", logFields)
			errChan <- err
			// Rollback will move latest commit to the last known successful commit.
			if offsetUpdated {
				err = s.offsetCore.RollBackOffset(ctx, &offsetModel)
				if err != nil {
					logger.Ctx(ctx).Errorw("subscriber: Failed to rollback offset", "logFields", logFields, "msg", err.Error())
				}
			}
		}
		// after successful commit to broker, make sure to re-init the maxCommittedOffset in subscriber
		stats.maxCommittedOffset = offsetToCommit
		logger.Ctx(ctx).Infow("subscriber: max committed offset new value", "logFields", logFields, "offsetToCommit", offsetToCommit, "topic", tp.topic, "partition", tp.partition)
	}

	s.removeMessageFromMemory(ctx, stats, msg.MessageID)

	// add to eviction map only in case of any out of order eviction
	if offsetToCommit > stats.maxCommittedOffset {
		logger.Ctx(ctx).Infow("subscriber: storing offset in memory due to offset mismatch", "messageID", msg.MessageID, "topic", msg.Topic, "partition", msg.Partition, "offsetToCommit", offsetToCommit, "maxOffset", stats.maxCommittedOffset, "evictedButNotCommittedOffsets", len(stats.evictedButNotCommittedOffsets))
		stats.evictedButNotCommittedOffsets[offsetToCommit] = true
	}
}
