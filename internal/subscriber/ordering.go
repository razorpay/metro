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

// OrderedImplementation implements a subscriber that delivers messages in order
type OrderedImplementation struct {
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
	pausedMessages         []messagebroker.ReceivedMessage
	sequenceManager        OrderingSequenceManager
	impl                   Implementation
}

// CanConsumeMore looks at sum of all consumed messages in all the active topic partitions and checks threshold
func (s *OrderedImplementation) CanConsumeMore() bool {
	totalConsumedMsgsForTopic := 0
	for _, cm := range s.consumedMessageStats {
		totalConsumedMsgsForTopic += len(cm.consumedMessages)
	}
	return totalConsumedMsgsForTopic <= int(s.maxOutstandingMessages)
}

// cleans up all occurrences for a given msgId from the internal data-structures
func (s *OrderedImplementation) removeMessageFromMemory(ctx context.Context, stats *ConsumptionMetadata, msgID string) {
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

func (s *OrderedImplementation) logInMemoryStats(ctx context.Context) {
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
	logger.Ctx(ctx).Infow("subscriber: in-memory stats", "logFields", getLogFields(s), "stats", st)
}

// EvictUnackedMessagesPastDeadline evicts messages past acknowledgement deadline
func (s *OrderedImplementation) EvictUnackedMessagesPastDeadline(ctx context.Context, errChan chan error) {
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

			// NOTE :  if push to retry queue fails due to any error, we do not delete from the deadline heap
			// this way the message is eligible to be retried
			retryMessage(ctx, s, s.consumer, s.retrier, msg, errChan)

			// cleanup message from memory only after a successful push to retry topic
			s.removeMessageFromMemory(ctx, metadata, peek.MsgID)

			logFields["messageId"] = peek.MsgID
			logger.Ctx(ctx).Infow("subscriber: deadline eviction: message evicted", "logFields", logFields)
			subscriberMessagesDeadlineEvicted.WithLabelValues(env, s.topic, s.subscription.Name).Inc()
		}
	}
}

// ModAckDeadline modifies the acknowledgement deadline of message
func (s *OrderedImplementation) ModAckDeadline(ctx context.Context, req *ModAckMessage, errChan chan error) {
	if req == nil {
		return
	}

	logFields := getLogFields(s)
	logFields["messageId"] = req.AckMessage.MessageID

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

	if req.ackDeadline == 0 {
		// modAck with deadline = 0 means nack
		// https://github.com/googleapis/google-cloud-go/blob/pubsub/v1.10.0/pubsub/iterator.go#L348

		// push to retry queue
		retryMessage(ctx, s, s.consumer, s.retrier, msg, errChan)

		// cleanup message from memory
		s.removeMessageFromMemory(ctx, stats, msgID)

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

// Acknowledge acknowleges a pulled message
func (s *OrderedImplementation) Acknowledge(ctx context.Context, req *AckMessage, errChan chan error) {
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

	// if somehow an ack request comes for a message that has met deadline eviction threshold
	if req.HasHitDeadline() {

		logger.Ctx(ctx).Infow("subscriber: msg hit deadline", "logFields", logFields)

		// push for retry
		retryMessage(ctx, s, s.consumer, s.retrier, msg, errChan)
		s.removeMessageFromMemory(ctx, stats, req.MessageID)

		return
	}

	offsetToCommit := req.Offset
	shouldCommit := false
	peek := stats.offsetBasedMinHeap.Indices[0]

	logger.Ctx(ctx).Infow("subscriber: offsets in ack", "logFields", logFields, "req offset", req.Offset, "peek offset", peek.Offset)
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
	}

	if shouldCommit {
		offsetUpdated := true
		offsetModel := offset.Model{
			Topic:        s.subscription.Topic,
			Subscription: s.subscription.ExtractedSubscriptionName,
			Partition:    req.Partition,
			LatestOffset: offsetToCommit + 1,
		}
		err := s.offsetCore.SetOffset(ctx, &offsetModel)
		if err != nil {
			// DO NOT terminate here since registry update failures should not affect message broker commits
			logger.Ctx(ctx).Errorw("subscriber: failed to store offset in registry", "logFields", logFields)
			offsetUpdated = false
		}
		_, err = s.consumer.CommitByPartitionAndOffset(ctx, messagebroker.CommitOnTopicRequest{
			Topic:     req.Topic,
			Partition: req.Partition,
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
			return
		}
		// after successful commit to broker, make sure to re-init the maxCommittedOffset in subscriber
		stats.maxCommittedOffset = offsetToCommit
		logger.Ctx(ctx).Infow("subscriber: max committed offset new value", "logFields", logFields, "offsetToCommit", offsetToCommit, "topic-partition", tp)
	}

	s.removeMessageFromMemory(ctx, stats, req.MessageID)

	// add to eviction map only in case of any out of order eviction
	if offsetToCommit > stats.maxCommittedOffset {
		stats.evictedButNotCommittedOffsets[offsetToCommit] = true
	}

	subscriberMessagesAckd.WithLabelValues(env, s.topic, s.subscription.Name, s.subscriberID).Inc()
	subscriberTimeTakenToAckMsg.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(msg.PublishTime).Seconds())
}

// GetSubscriberID ...
func (s *OrderedImplementation) GetSubscriberID() string {
	return s.subscriberID
}

// GetSubscription ...
func (s *OrderedImplementation) GetSubscription() *subscription.Model {
	return s.subscription
}

// Pull pulls a message from broker and publishes onto the response channel
func (s *OrderedImplementation) Pull(ctx context.Context, req *PullRequest, responseChan chan *metrov1.PullResponse, errChan chan error) {
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

	// Assign sequence numbers to messages
	for _, message := range messages {
		if message.Topic == s.topic && message.RequiresOrdering() { // only for messages from primary topic that have an ordering key
			seq, err := s.sequenceManager.GetOrderedSequenceNum(ctx, s.subscription, message)
			if err != nil {
				notifyPullMessageError(ctx, s, err, responseChan, errChan)
				return
			}
			message.CurrentSequence, message.PrevSequence = seq.CurrentSequenceNum, seq.PrevSequenceNum
		}
	}

	if !(s.consumer.IsPaused(ctx) && s.consumer.IsPrimaryPaused(ctx)) && len(s.pausedMessages) > 0 {
		// if consumer is not paused, pop the paused messages and append it to the begining of the message list
		// clear the paused message list
		logger.Ctx(ctx).Infow("subscriber: resuming messages that were paused", "logFields", getLogFields(s), "count", len(s.pausedMessages))
		messages = append(s.pausedMessages, messages...)
		s.pausedMessages = make([]messagebroker.ReceivedMessage, 0)
	}

	if messages, err = s.filterMessagesForOrdering(ctx, messages); err != nil {
		notifyPullMessageError(ctx, s, err, responseChan, errChan)
		return
	}

	if len(messages) > 0 {
		logFields := getLogFields(s)
		logFields["messageCount"] = len(messages)
		logger.Ctx(ctx).Infow("subscriber: non-zero messages after filtering for order", "logFields", logFields)
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
		// check if the subscription has any filter applied and check if the message satisfies it if any
		if checkFilterCriteria(ctx, s, protoMsg) {
			sm = append(sm, &metrov1.ReceivedMessage{AckId: ackID, Message: protoMsg, DeliveryAttempt: msg.CurrentRetryCount + 1})
		} else {
			// self acknowledging the message as it does not need to be delivered for this subscription
			//
			// Using subscriber's Acknowledge func instead of directly acking to consumer because of the following reason:
			// Let there be 3 messages - Msg-1, Msg-2 and Msg-3. Msg-1 and Msg-3 satisfies the filter criteria while Msg-3 doesn't.
			// If we directly ack Msg-2 using brokerStore consumer, in Kafka, new committed offset will be set to 2.
			// Now if for some reason, delivery of Msg-1 fails, we will not be able to retry it as the broker's committed offset is already set to 2.
			// To avoid this, subscriber Acknowledge is used which will wait for Msg-1 status before committing the offsets.
			s.Acknowledge(ctx, ackMessage.(*AckMessage).WithContext(ctx), errChan)
		}

		subscriberMessagesConsumed.WithLabelValues(env, msg.Topic, s.subscription.Name, s.subscriberID).Inc()
		subscriberMemoryMessagesCountTotal.WithLabelValues(env, s.topic, s.subscription.Name, s.subscriberID).Set(float64(len(s.consumedMessageStats[tp].consumedMessages)))
		subscriberTimeTakenFromPublishToConsumeMsg.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(msg.PublishTime).Seconds())
	}

	if len(sm) > 0 {
		s.logInMemoryStats(ctx)
	}
	responseChan <- &metrov1.PullResponse{ReceivedMessages: sm}
}

func (s *OrderedImplementation) filterMessagesForOrdering(ctx context.Context, messages []messagebroker.ReceivedMessage) ([]messagebroker.ReceivedMessage, error) {
	primaryTopicMessages := make([]messagebroker.ReceivedMessage, 0)
	retryTopicMessages := make([]messagebroker.ReceivedMessage, 0)
	orderingKeyPartitionMap := make(map[string]int32)
	orderingKeyStatus := make(map[string]*lastSequenceStatus)

	filteredMessages := make([]messagebroker.ReceivedMessage, 0)

	for _, message := range messages {
		if message.Topic == s.topic {
			primaryTopicMessages = append(primaryTopicMessages, message)
			if _, exists := orderingKeyPartitionMap[message.OrderingKey]; !exists && message.RequiresOrdering() {
				orderingKeyPartitionMap[message.OrderingKey] = message.Partition
			}

		} else {
			retryTopicMessages = append(retryTopicMessages, message)
		}
	}

	for key := range orderingKeyPartitionMap {
		status, err := s.sequenceManager.GetLastSequenceStatus(ctx, s.subscription, orderingKeyPartitionMap[key], key)
		if err != nil {
			return nil, err
		}
		orderingKeyStatus[key] = status
	}

	for i, message := range primaryTopicMessages {
		status := orderingKeyStatus[message.OrderingKey]
		// if status exists, and there is a failure in the previous message of the sequence
		// pause the subscriber. other messages before the current message can be delvered.
		if status != nil && status.SequenceNum <= message.PrevSequence && status.Status == sequenceFailure {
			logger.Ctx(ctx).Infow(
				"subscriber: previous message pending",
				"orderingKey", message.OrderingKey,
				"messageSequence", message.CurrentSequence,
				"prevSequence", message.PrevSequence,
				"waitingFor", status.SequenceNum,
				"status", status.Status,
			)
			s.pausedMessages = primaryTopicMessages[i:]
			if err := s.consumer.PausePrimaryConsumer(ctx); err != nil {
				return nil, err
			}
			break
		} else {
			filteredMessages = primaryTopicMessages[:i+1]
		}
	}

	filteredMessages = append(retryTopicMessages, filteredMessages...)
	return filteredMessages, nil
}
