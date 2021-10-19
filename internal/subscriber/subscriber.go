package subscriber

import (
	"container/heap"
	"context"
	"strconv"
	"time"

	"github.com/razorpay/metro/internal/subscriber/retry"

	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/offset"
	"github.com/razorpay/metro/internal/subscriber/customheap"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

const (
	deadlineTickerInterval = 200 * time.Millisecond
	minAckDeadline         = 10 * time.Minute
)

// ISubscriber is interface over high level subscriber
type ISubscriber interface {
	GetID() string
	GetSubscriptionName() string
	GetResponseChannel() chan *metrov1.PullResponse
	GetRequestChannel() chan *PullRequest
	GetAckChannel() chan *AckMessage
	GetModAckChannel() chan *ModAckMessage
	GetErrorChannel() chan error
	Stop()
	Run(ctx context.Context)
}

// Subscriber consumes messages from a topic
type Subscriber struct {
	subscription           *subscription.Model
	topic                  string
	subscriberID           string
	subscriptionCore       subscription.ICore
	offsetCore             offset.ICore
	requestChan            chan *PullRequest
	responseChan           chan *metrov1.PullResponse
	ackChan                chan *AckMessage
	modAckChan             chan *ModAckMessage
	deadlineTicker         *time.Ticker
	errChan                chan error
	closeChan              chan struct{}
	timeoutInMs            int
	consumer               IConsumer // consume messages from primary topic and retry topic
	cancelFunc             func()
	maxOutstandingMessages int64
	maxOutstandingBytes    int64
	consumedMessageStats   map[TopicPartition]*ConsumptionMetadata
	ctx                    context.Context
	bs                     brokerstore.IBrokerStore
	retrier                retry.IRetrier
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

// GetSubscriptionName ...
func (s *Subscriber) GetSubscriptionName() string {
	return s.subscription.Name
}

// pushes message to retrier and commit existing message on primary topic
func (s *Subscriber) retry(ctx context.Context, msg messagebroker.ReceivedMessage) {

	// for older subscriptions, delayConfig will not get auto-created
	if s.retrier == nil {
		logger.Ctx(ctx).Infow("subscriber: skipping retry as retrier not configured", "logFields", s.getLogFields())
		return
	}

	// prepare message headers to be used by retrier
	msg.SourceTopic = s.subscription.Topic
	msg.RetryTopic = s.subscription.GetRetryTopic() // push retried messages to primary retry topic
	msg.CurrentTopic = s.subscription.Topic         // initially these will be same
	msg.Subscription = s.subscription.Name
	msg.CurrentRetryCount = msg.CurrentRetryCount + 1 // should be zero to begin with
	msg.MaxRetryCount = s.subscription.DeadLetterPolicy.MaxDeliveryAttempts
	msg.DeadLetterTopic = s.subscription.DeadLetterPolicy.DeadLetterTopic
	msg.InitialDelayInterval = s.subscription.RetryPolicy.MinimumBackoff

	err := s.retrier.Handle(ctx, msg)
	if err != nil {
		logger.Ctx(ctx).Errorw("subscriber: push to retrier failed", "logFields", s.getLogFields(), "error", err.Error())
		s.errChan <- err
		return
	}

	// commit on the primary topic after message has been submitted for retry
	_, err = s.consumer.CommitByPartitionAndOffset(ctx, messagebroker.CommitOnTopicRequest{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		// add 1 to current offset
		// https://docs.confluent.io/5.5.0/clients/confluent-kafka-go/index.html#pkg-overview
		Offset: msg.Offset + 1,
	})
	if err != nil {
		logger.Ctx(ctx).Errorw("subscriber: failed to commit message", "logFields", s.getLogFields(), "error", err.Error())
		s.errChan <- err
		return
	}
}

// acknowledge messages
func (s *Subscriber) acknowledge(req *AckMessage) {
	span, ctx := opentracing.StartSpanFromContext(req.ctx, "Subscriber:Ack", opentracing.Tags{
		"subscriber":   req.SubscriberID,
		"topic":        req.Topic,
		"subscription": s.subscription.Name,
		"message_id":   req.MessageID,
		"partition":    req.Partition,
	})
	defer span.Finish()

	if req == nil {
		return
	}

	logFields := s.getLogFields()
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
		s.retry(ctx, msg)
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
		registryOffset := strconv.Itoa(int(offsetToCommit) + 1)
		offsetModel := offset.Model{
			Topic:        s.subscription.Topic,
			Subscription: s.subscription.ExtractedSubscriptionName,
			Partition:    req.Partition,
			LatestOffset: registryOffset,
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
			s.errChan <- err
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

// cleans up all occurrences for a given msgId from the internal data-structures
func (s *Subscriber) removeMessageFromMemory(ctx context.Context, stats *ConsumptionMetadata, msgID string) {
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

// modifyAckDeadline for messages
func (s *Subscriber) modifyAckDeadline(req *ModAckMessage) {
	span, ctx := opentracing.StartSpanFromContext(req.ctx, "Subscriber:ModifyAck", opentracing.Tags{
		"subscriber":   req.AckMessage.SubscriberID,
		"topic":        req.AckMessage.Topic,
		"subscription": s.subscription.Name,
		"message_id":   req.AckMessage.MessageID,
		"partition":    req.AckMessage.Partition,
	})
	defer span.Finish()

	if req == nil {
		return
	}

	logFields := s.getLogFields()
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
		s.retry(ctx, msg)

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

func (s *Subscriber) checkAndEvictBasedOnAckDeadline(ctx context.Context) {

	logFields := s.getLogFields()
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
			s.retry(ctx, msg)

			// cleanup message from memory only after a successful push to retry topic
			s.removeMessageFromMemory(ctx, metadata, peek.MsgID)

			logFields["messageId"] = peek.MsgID
			logger.Ctx(ctx).Infow("subscriber: deadline eviction: message evicted", "logFields", logFields)
			subscriberMessagesDeadlineEvicted.WithLabelValues(env, s.topic, s.subscription.Name).Inc()
		}
	}
}

// Run loop
func (s *Subscriber) Run(ctx context.Context) {
	logger.Ctx(ctx).Infow("subscriber: started running subscriber", "logFields", s.getLogFields())
	for {
		select {
		case req := <-s.requestChan:
			// on channel closure, we can get nil data
			if req == nil || ctx.Err() != nil {
				continue
			}
			caseStartTime := time.Now()
			s.pull(req)
			subscriberTimeTakenInRequestChannelCase.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(caseStartTime).Seconds())
		case ackRequest := <-s.ackChan:
			caseStartTime := time.Now()
			if ackRequest == nil || ctx.Err() != nil {
				continue
			}
			s.acknowledge(ackRequest)
			subscriberTimeTakenInAckChannelCase.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(caseStartTime).Seconds())
		case modAckRequest := <-s.modAckChan:
			caseStartTime := time.Now()
			if modAckRequest == nil || ctx.Err() != nil {
				continue
			}
			s.modifyAckDeadline(modAckRequest)
			subscriberTimeTakenInModAckChannelCase.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(caseStartTime).Seconds())
		case <-s.deadlineTicker.C:
			caseStartTime := time.Now()
			if ctx.Err() != nil {
				continue
			}
			s.checkAndEvictBasedOnAckDeadline(ctx)
			subscriberTimeTakenInDeadlineChannelCase.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(caseStartTime).Seconds())
		case err := <-s.errChan:
			if ctx.Err() != nil {
				continue
			}
			if err != nil {
				logger.Ctx(ctx).Errorw("subscriber: got error on errCh channel", "logFields", s.getLogFields(), "error", err.Error())
			}
		case <-ctx.Done():
			logger.Ctx(ctx).Infow("subscriber: <-ctx.Done() called", "logFields", s.getLogFields())

			s.deadlineTicker.Stop()

			// dereference the in-memory map of messages
			s.consumedMessageStats = nil

			// close the response channel to stop any new message processing
			close(s.responseChan)
			close(s.errChan)

			s.consumer.Close(ctx)
			close(s.closeChan)
			return
		}
	}
}

func (s *Subscriber) pull(req *PullRequest) {
	span, ctx := opentracing.StartSpanFromContext(req.ctx, "Subscriber:Pull", opentracing.Tags{
		"subscriber":   s.subscriberID,
		"subscription": s.subscription.Name,
		"topic":        s.subscription.Topic,
	})
	defer span.Finish()

	// wrapping this code block in an anonymous function so that defer on time-taken metric can be scoped
	func() {
		caseStartTime := time.Now()
		defer func() {
			subscriberTimeTakenInRequestChannelCase.WithLabelValues(env, s.topic, s.subscription.Name).Observe(time.Now().Sub(caseStartTime).Seconds())
		}()

		if s.consumer == nil {
			return
		}

		// wrapping this code block in an anonymous function so that defer on time-taken metric can be scoped
		if s.canConsumeMore() == false {
			logger.Ctx(ctx).Infow("subscriber: cannot consume more messages before acking", "logFields", s.getLogFields())
			// check if consumer is paused once maxOutstanding messages limit is hit
			if s.consumer.IsPaused(ctx) == false {
				s.consumer.PauseConsumer(ctx)
			}
		} else {
			// resume consumer if paused and is allowed to consume more messages
			if s.consumer.IsPaused(ctx) {
				s.consumer.ResumeConsumer(ctx)
			}
		}

		sm := make([]*metrov1.ReceivedMessage, 0)
		resp, err := s.consumer.ReceiveMessages(ctx, messagebroker.GetMessagesFromTopicRequest{NumOfMessages: req.MaxNumOfMessages, TimeoutMs: s.timeoutInMs})
		if err != nil {
			logger.Ctx(ctx).Errorw("subscriber: error in receiving messages", "logFields", s.getLogFields(), "error", err.Error())

			// Write empty data on the response channel in case of error, this is needed because sender blocks
			// on the response channel in a goroutine after sending request, error channel is not read until
			// response channel blocking call returns
			s.responseChan <- &metrov1.PullResponse{ReceivedMessages: sm}

			// send error details via error channel
			s.errChan <- err
			return
		}

		if len(resp.Messages) > 0 {
			logFields := s.getLogFields()
			logFields["messageCount"] = len(resp.Messages)
			logger.Ctx(ctx).Infow("subscriber: non-zero messages from topics", "logFields", logFields)
		}

		for _, msg := range resp.Messages {
			protoMsg := &metrov1.PubsubMessage{}
			err = proto.Unmarshal(msg.Data, protoMsg)
			if err != nil {
				logger.Ctx(ctx).Errorw("subscriber: error in proto unmarshal", "logFields", s.getLogFields(), "error", err.Error())
				s.errChan <- err
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
					logger.Ctx(ctx).Errorw("subscriber: error in reading topic metadata", "logFields", s.getLogFields(), "error", err.Error())
					s.errChan <- err
					continue
				}
				s.consumedMessageStats[tp].maxCommittedOffset = resp.Offset
			}

			ackDeadline := time.Now().Add(minAckDeadline).Unix()
			s.consumedMessageStats[tp].Store(msg, ackDeadline)

			ackMessage, err := NewAckMessage(s.subscriberID, msg.Topic, msg.Partition, msg.Offset, int32(ackDeadline), msg.MessageID)
			if err != nil {
				logger.Ctx(ctx).Errorw("subscriber: error in creating AckMessage", "logFields", s.getLogFields(), "error", err.Error())
				s.errChan <- err
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
		s.responseChan <- &metrov1.PullResponse{ReceivedMessages: sm}
	}()
}

func (s *Subscriber) logInMemoryStats(ctx context.Context) {
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
	logger.Ctx(ctx).Infow("subscriber: in-memory stats", "logFields", s.getLogFields(), "stats", st)
}

// returns a map of common fields to be logged
func (s *Subscriber) getLogFields() map[string]interface{} {
	return map[string]interface{}{
		"topic":        s.topic,
		"subscription": s.subscription.Name,
		"subscriberId": s.subscriberID,
	}
}

// GetRequestChannel returns the chan from where request is received
func (s *Subscriber) GetRequestChannel() chan *PullRequest {
	return s.requestChan
}

// GetResponseChannel returns the chan where response is written
func (s *Subscriber) GetResponseChannel() chan *metrov1.PullResponse {
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

	// gracefully shutdown the retrier delay consumers
	s.retrier.Stop(s.ctx)

	s.cancelFunc()

	<-s.closeChan
}
