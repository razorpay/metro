package subscriber

import (
	"container/heap"
	"context"
	"strings"
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
	consumedMessageStats   map[TopicPartition]*OrderedConsumptionMetadata
	pausedMessages         []messagebroker.ReceivedMessage
	sequenceManager        OrderingSequenceManager
}

// GetSubscriberID ...
func (s *OrderedImplementation) GetSubscriberID() string {
	return s.subscriberID
}

// GetSubscription ...
func (s *OrderedImplementation) GetSubscription() *subscription.Model {
	return s.subscription
}

// CanConsumeMore looks at sum of all consumed messages in all the active topic partitions and checks threshold
func (s *OrderedImplementation) CanConsumeMore() bool {
	totalConsumedMsgsForTopic := 0
	for _, cm := range s.consumedMessageStats {
		totalConsumedMsgsForTopic += len(cm.consumedMessages)
	}
	return totalConsumedMsgsForTopic <= int(s.maxOutstandingMessages)
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
	for i := range messages {
		message := messages[i]
		if s.isPrimaryTopic(message.Topic) && message.RequiresOrdering() { // only for messages from primary topic that have an ordering key
			seq, err := s.sequenceManager.GetOrderedSequenceNum(ctx, s.subscription, message)
			if err != nil {
				notifyPullMessageError(ctx, s, err, responseChan, errChan)
				return
			}
			messages[i].CurrentSequence, messages[i].PrevSequence = seq.CurrentSequenceNum, seq.PrevSequenceNum
		}
	}

	if !(s.consumer.IsPaused(ctx) || s.consumer.IsPrimaryPaused(ctx)) && len(s.pausedMessages) > 0 {
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
			s.consumedMessageStats[tp] = NewOrderedConsumptionMetadata()

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

	if len(sm) > 0 {
		s.logInMemoryStats(ctx)
	}
	responseChan <- &metrov1.PullResponse{ReceivedMessages: sm}
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

		stats.offsetStatusMap[req.Offset] = offsetStatusRetry
		err := s.evictMessages(ctx, tp)
		if err != nil {
			errChan <- err
		}
		return
	}

	stats.offsetStatusMap[req.Offset] = offsetStatusAcknowledge

	err := s.evictMessages(ctx, tp)

	subscriberMessagesAckd.WithLabelValues(env, s.topic, s.subscription.Name, s.subscriberID).Inc()
	subscriberTimeTakenToAckMsg.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(msg.PublishTime).Seconds())

	if err != nil {
		errChan <- err
	}
	return
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
		stats.offsetStatusMap[msg.Offset] = offsetStatusRetry

		err := s.evictMessages(ctx, tp)

		subscriberMessagesModAckd.WithLabelValues(env, s.topic, s.subscription.Name, s.subscriberID).Inc()
		subscriberTimeTakenToModAckMsg.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(msg.PublishTime).Seconds())

		if err != nil {
			errChan <- err
		}

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

// EvictUnackedMessagesPastDeadline evicts messages past acknowledgement deadline
func (s *OrderedImplementation) EvictUnackedMessagesPastDeadline(ctx context.Context, errChan chan error) {
	logFields := getLogFields(s)
	// do a deadline based eviction for all active topic-partition heaps
	for tp, metadata := range s.consumedMessageStats {
		deadlineBasedHeap := metadata.deadlineBasedMinHeap
		for i := 0; i < deadlineBasedHeap.Len(); i++ {
			peek := deadlineBasedHeap.Indices[0]
			if peek.HasHitDeadline() {
				msgID := peek.MsgID
				if _, ok := metadata.consumedMessages[msgID]; !ok {
					// check if message is present in-memory or not
					continue
				}
				msg := metadata.consumedMessages[msgID].(messagebroker.ReceivedMessage)
				if _, ok := metadata.offsetStatusMap[msg.Offset]; !ok {
					metadata.offsetStatusMap[msg.Offset] = offsetStatusRetry

					logFields["messageId"] = peek.MsgID
					logger.Ctx(ctx).Infow("subscriber: deadline eviction: message evicted", "logFields", logFields)
					subscriberMessagesDeadlineEvicted.WithLabelValues(env, s.topic, s.subscription.Name).Inc()
				}
			}
		}
		s.evictMessages(ctx, tp)
	}
}

func (s *OrderedImplementation) filterMessagesForOrdering(ctx context.Context, messages []messagebroker.ReceivedMessage) ([]messagebroker.ReceivedMessage, error) {
	primaryTopicMessages := make([]messagebroker.ReceivedMessage, 0)
	retryTopicMessages := make([]messagebroker.ReceivedMessage, 0)
	orderingKeyPartitionMap := make(map[string]int32)
	orderingKeyStatus := make(map[string]*lastSequenceStatus)

	filteredMessages := make([]messagebroker.ReceivedMessage, 0)

	for _, message := range messages {
		if s.isPrimaryTopic(message.Topic) {
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
		// pause the subscriber on primary topic. other messages before the current message can be delvered.
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

func (s *OrderedImplementation) fetchOrderingKeyStatus(ctx context.Context, partition int32, orderingKeySet map[string]interface{}) (map[string]*lastSequenceStatus, error) {
	statusMap := make(map[string]*lastSequenceStatus)
	for orderingKey := range orderingKeySet {
		status, err := s.sequenceManager.GetLastSequenceStatus(ctx, s.subscription, partition, orderingKey)
		if err != nil {
			return nil, err
		}
		statusMap[orderingKey] = status
	}
	return statusMap, nil
}

func (s *OrderedImplementation) updateOrderingKeyStatus(ctx context.Context, partition int32, keyStatusMap map[string]*lastSequenceStatus) error {
	for orderingKey, status := range keyStatusMap {
		if err := s.sequenceManager.SetLastSequenceStatus(ctx, s.subscription, partition, orderingKey, status); err != nil {
			return err
		}
	}
	return nil
}

func (s *OrderedImplementation) updateOrderingKeySequence(ctx context.Context, partition int32, keySeqMap map[string]int32) error {
	for orderingKey, sequenceNum := range keySeqMap {
		if err := s.sequenceManager.SetOrderedSequenceNum(ctx, s.subscription, partition, orderingKey, sequenceNum); err != nil {
			return err
		}
	}
	return nil
}

func (s *OrderedImplementation) deleteOrderingKey(ctx context.Context, partition int32, keyStatusMap map[string]*lastSequenceStatus) error {
	for key, status := range keyStatusMap {
		if (status != nil) && (status.Status == sequenceSuccess) || (status.Status == sequenceDLQ) {
			lastSeq, err := s.sequenceManager.GetLastMessageSequenceNum(ctx, s.subscription, partition, key)
			if err != nil {
				return err
			}
			if lastSeq == status.SequenceNum {
				if err = s.sequenceManager.DeleteSequence(ctx, s.subscription, partition, key); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *OrderedImplementation) evictMessages(ctx context.Context, tp TopicPartition) error {
	logFields := getLogFields(s)
	logFields["topic-parition"] = tp

	cm := s.consumedMessageStats[tp]
	if cm == nil {
		logger.Ctx(ctx).Infow("subscriber: consumption metadata not found for topic-partition", "logFields", logFields)
		return nil
	}

	orderingKeySet := map[string]interface{}{}
	offsetsToCommit := make([]*customheap.AckMessageWithOffset, 0)
	messageIDs := make([]string, 0)
	for i := 0; i < cm.offsetBasedMinHeap.Len(); i++ { // find all offsets in ascending order that can be committed
		peek := cm.offsetBasedMinHeap.Indices[i]
		_, ok := cm.offsetStatusMap[peek.Offset]
		if !ok {
			break
		}
		logger.Ctx(ctx).Infow("subscriber: ack/nack for offset received", "logFields", logFields, "offset", peek.Offset)

		if _, ok = cm.consumedMessages[peek.MsgID]; !ok {
			logger.Ctx(ctx).Error("subscriber: message not found in consumed messages map", "logFields", logFields, "messageID", peek.MsgID)
			break
		}
		msg := cm.consumedMessages[peek.MsgID].(messagebroker.ReceivedMessage)

		if msg.RequiresOrdering() {
			orderingKeySet[msg.OrderingKey] = nil
		}
		offsetsToCommit = append(offsetsToCommit, peek)
		messageIDs = append(messageIDs, msg.MessageID)
	}

	if len(offsetsToCommit) == 0 {
		return nil
	}

	keyStatusMap, err := s.fetchOrderingKeyStatus(ctx, tp.partition, orderingKeySet)
	if err != nil {
		logger.Ctx(ctx).Error("subscriber: could not fetch ordering key status", "logFields", getLogFields(s), "error", err)
		return err
	}

	maxSequenceNums := map[string]int32{}
	msgStatuses := make([]offsetStatus, 0)
	for i := 0; i < len(offsetsToCommit); i++ {
		offset := offsetsToCommit[i]
		msgStatus := cm.offsetStatusMap[offset.Offset]
		msg := cm.consumedMessages[offset.MsgID].(messagebroker.ReceivedMessage)

		if msg.RequiresOrdering() {
			if keyStatus := keyStatusMap[msg.OrderingKey]; keyStatus == nil { // status not recorded. consider this the first message in order
				keyStatusMap[msg.OrderingKey] = &lastSequenceStatus{
					SequenceNum: msg.CurrentSequence,
					Status:      s.offsetStatusToSequenceStatus(msgStatus, msg.CurrentRetryCount),
				}
			} else {
				if keyStatus.Status == sequenceFailure && keyStatus.SequenceNum <= msg.PrevSequence {
					// previous message in the ordering key is pending
					// push the current message to retry queue
					msgStatus = offsetStatusRetry
					logger.Ctx(ctx).Infow(
						"subscriber: previous message in order pending to be delivered",
						"logFields", getLogFields(s),
						"messageID", msg.MessageID,
						"currentSequence", msg.CurrentSequence,
						"prevSequence", msg.PrevSequence,
						"orderStatus", keyStatus,
					)
				} else {
					// update the ordering status with current message status
					keyStatus.SequenceNum = msg.CurrentSequence
					keyStatus.Status = s.offsetStatusToSequenceStatus(msgStatus, msg.CurrentRetryCount)
				}
			}

			maxSeqNum := maxSequenceNums[msg.OrderingKey]
			if msg.CurrentSequence > maxSeqNum {
				maxSequenceNums[msg.OrderingKey] = maxSeqNum
			}
		}
		msgStatuses = append(msgStatuses, msgStatus)
	}

	for i := 0; i < len(offsetsToCommit); i++ {
		status := msgStatuses[i]
		msg := cm.consumedMessages[offsetsToCommit[i].MsgID].(messagebroker.ReceivedMessage)
		seqStatus := s.offsetStatusToSequenceStatus(status, msg.CurrentRetryCount)
		if status == offsetStatusRetry {
			if err := retryMessage(ctx, s, s.consumer, s.retrier, msg); err != nil {
				return err
			}
		}
		if (seqStatus == sequenceSuccess || seqStatus == sequenceDLQ) &&
			len(s.pausedMessages) > 0 &&
			s.pausedMessages[0].OrderingKey == msg.OrderingKey &&
			msg.CurrentSequence >= s.pausedMessages[0].PrevSequence {
			// if current messages is successful or pushed to DLQ, and there is a paused message
			// that is next in sequence of the current message, unpause primary consumer.
			logger.Ctx(ctx).Infow(
				"subscriber: resuming primary consumer",
				"logFields", getLogFields(s),
				"orderingKey", msg.OrderingKey,
				"unpausedSequence", s.pausedMessages[0].CurrentSequence,
				"unpausedPrevSequence", s.pausedMessages[0].PrevSequence,
				"messageSequence", msg.CurrentSequence,
				"messagePrevSequence", msg.PrevSequence,
			)
			if err := s.consumer.ResumePrimaryConsumer(ctx); err != nil {
				logger.Ctx(ctx).Error("Could not unpause the primary consumer", "logFields", getLogFields(s))
				return err
			}
		}

	}

	if s.isPrimaryTopic(tp.topic) {
		if err = s.updateOrderingKeySequence(ctx, tp.partition, maxSequenceNums); err != nil {
			logger.Ctx(ctx).Errorw(
				"subscriber: could not update sequence offset",
				"logFields", getLogFields(s),
				"error", err,
			)
			return err
		}
	}

	if err = s.updateOrderingKeyStatus(ctx, tp.partition, keyStatusMap); err != nil {
		logger.Ctx(ctx).Errorw(
			"subscriber: could not update sequence status",
			"logFields", getLogFields(s),
			"error", err,
		)
		return err
	}

	maxCommittedOffset := offsetsToCommit[len(offsetsToCommit)-1].Offset
	_, err = s.consumer.CommitByPartitionAndOffset(ctx, messagebroker.CommitOnTopicRequest{
		Topic:     tp.topic,
		Partition: tp.partition,
		// add 1 to current offset
		// https://docs.confluent.io/5.5.0/clients/confluent-kafka-go/index.html#pkg-overview
		Offset: maxCommittedOffset + 1,
	})
	if err != nil {
		return err
	}

	s.removeMessagesFromMemory(ctx, cm, messageIDs...)
	cm.maxCommittedOffset = maxCommittedOffset

	s.deleteOrderingKey(ctx, tp.partition, keyStatusMap)

	return nil
}

// cleans up all occurrences for a given msgIds from the internal data-structures
func (s *OrderedImplementation) removeMessagesFromMemory(ctx context.Context, stats *OrderedConsumptionMetadata, msgIDs ...string) {
	if stats == nil {
		return
	}
	for _, msgID := range msgIDs {
		start := time.Now()
		delete(stats.consumedMessages, msgID)

		// remove message from offsetBasedMinHeap
		indexOfMsgInOffsetBasedMinHeap := stats.offsetBasedMinHeap.MsgIDToIndexMapping[msgID]
		msg := heap.Remove(&stats.offsetBasedMinHeap, indexOfMsgInOffsetBasedMinHeap).(*customheap.AckMessageWithOffset)
		delete(stats.offsetBasedMinHeap.MsgIDToIndexMapping, msgID)
		delete(stats.offsetStatusMap, msg.Offset)

		// remove same message from deadlineBasedMinHeap
		indexOfMsgInDeadlineBasedMinHeap := stats.deadlineBasedMinHeap.MsgIDToIndexMapping[msg.MsgID]
		heap.Remove(&stats.deadlineBasedMinHeap, indexOfMsgInDeadlineBasedMinHeap)
		delete(stats.deadlineBasedMinHeap.MsgIDToIndexMapping, msgID)

		subscriberMemoryMessagesCountTotal.WithLabelValues(env, s.topic, s.subscription.Name, s.subscriberID).Dec()
		subscriberTimeTakenToRemoveMsgFromMemory.WithLabelValues(env).Observe(time.Now().Sub(start).Seconds())
	}
	s.logInMemoryStats(ctx)
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
			"offsetStatus":                       stats.offsetStatusMap,
		}
		st[tp.String()] = total
	}
	logger.Ctx(ctx).Infow("subscriber: in-memory stats", "logFields", getLogFields(s), "stats", st)
}

func (s *OrderedImplementation) isPrimaryTopic(topic string) bool {
	brokerTopic := strings.ReplaceAll(s.topic, "/", "_")
	return brokerTopic == topic
}

func (s *OrderedImplementation) offsetStatusToSequenceStatus(os offsetStatus, retryCount int32) sequenceStatus {
	status := sequenceSuccess
	if os == offsetStatusRetry {
		status = sequenceFailure
		if retryCount+1 >= s.subscription.DeadLetterPolicy.MaxDeliveryAttempts {
			status = sequenceDLQ
		}
	}
	return status
}
