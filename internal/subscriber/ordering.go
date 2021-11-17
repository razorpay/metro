package subscriber

import (
	"context"

	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
)

func (s *Subscriber) isOrderedSubscriber() bool {
	return s.subscription.EnableMessageOrdering == true
}

func (s *Subscriber) filterMessagesForOrdering(ctx context.Context, messages []messagebroker.ReceivedMessage) ([]messagebroker.ReceivedMessage, error) {
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
