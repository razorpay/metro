package subscriber

import (
	"context"
	"fmt"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
)

var (
	cannotResume = fmt.Errorf("consumer was not paused")
)

type iConsumer interface {
	PauseConsumer(ctx context.Context) error
	IsPaused(ctx context.Context) bool
	ResumeConsumer(ctx context.Context) error

	PausePrimaryConsumer(ctx context.Context) error
	IsPrimaryPaused(ctx context.Context) bool
	ResumePrimaryConsumer(ctx context.Context) error

	ReceiveMessages(ctx context.Context, req messagebroker.GetMessagesFromTopicRequest) (*messagebroker.GetMessagesFromTopicResponse, error)
	GetTopicMetadata(ctx context.Context, req messagebroker.GetTopicMetadataRequest) (messagebroker.GetTopicMetadataResponse, error)
	CommitByPartitionAndOffset(ctx context.Context, req messagebroker.CommitOnTopicRequest) (messagebroker.CommitOnTopicResponse, error)

	Close(ctx context.Context) error
}

// consumerManager - a wrapper around the messagebroker.Consumer.
// Responsible for state management(pause, resume) of the consumer.
type consumerManager struct {
	consumerPaused    bool // pause consumption from all topics
	pausePrimaryTopic bool // pause consumption from primary topic
	subscriberID      string
	subscriptionName  string
	primaryTopic      string
	retryTopic        string
	consumer          messagebroker.Consumer
	brokerStore       brokerstore.IBrokerStore
}

func NewConsumerManager(ctx context.Context, bs brokerstore.IBrokerStore, subscriberID string, subscriptionName,
	topicName, retryTopicName string) (iConsumer, error) {
	consumer, err := bs.GetConsumer(
		ctx,
		messagebroker.ConsumerClientOptions{
			Topics:          []string{topicName, retryTopicName},
			GroupID:         subscriptionName,
			GroupInstanceID: subscriberID,
		},
	)
	if err != nil {
		return nil, err
	}

	return &consumerManager{
		consumerPaused:    false,
		pausePrimaryTopic: false,
		subscriberID:      subscriberID,
		subscriptionName:  subscriptionName,
		primaryTopic:      topicName,
		retryTopic:        retryTopicName,
		brokerStore:       bs,
		consumer:          consumer,
	}, nil
}

func (c *consumerManager) getLogFields() map[string]interface{} {
	return map[string]interface{}{
		"subscriberID": c.subscriberID,
		"subscription": c.subscriptionName,
	}
}

func (c *consumerManager) pausePrimaryConsumer(ctx context.Context) error {
	logger.Ctx(ctx).Infow("consumer manager: pausing primary consumer", "logFields", c.getLogFields(), "topic", c.primaryTopic)
	err := c.consumer.Pause(ctx, messagebroker.PauseOnTopicRequest{Topic: c.primaryTopic})
	if err != nil {
		subscriberPausedConsumersTotal.WithLabelValues(env, c.primaryTopic, c.subscriptionName, c.subscriberID).Inc()
	}
	return err
}

func (c *consumerManager) resumePrimaryConsumer(ctx context.Context) error {
	logger.Ctx(ctx).Infow("consumer manager: resuming primary consumer", "logFields", c.getLogFields(), "topic", c.primaryTopic)
	err := c.consumer.Resume(ctx, messagebroker.ResumeOnTopicRequest{Topic: c.primaryTopic})
	if err != nil {
		subscriberPausedConsumersTotal.WithLabelValues(env, c.primaryTopic, c.subscriptionName, c.subscriberID).Dec()
	}
	return err
}

func (c *consumerManager) pauseRetryConsumer(ctx context.Context) error {
	logger.Ctx(ctx).Infow("consumer manager: pausing retry consumer", "logFields", c.getLogFields(), "topic", c.retryTopic)
	err := c.consumer.Pause(ctx, messagebroker.PauseOnTopicRequest{Topic: c.retryTopic})
	if err != nil {
		subscriberPausedConsumersTotal.WithLabelValues(env, c.retryTopic, c.subscriptionName, c.subscriberID).Inc()
	}
	return err
}

func (c *consumerManager) resumeRetryConsumer(ctx context.Context) error {
	logger.Ctx(ctx).Infow("consumer manager: resuming retry consumer", "logFields", c.getLogFields(), "topic", c.retryTopic)
	err := c.consumer.Resume(ctx, messagebroker.ResumeOnTopicRequest{Topic: c.retryTopic})
	if err != nil {
		subscriberPausedConsumersTotal.WithLabelValues(env, c.retryTopic, c.subscriptionName, c.subscriberID).Dec()
	}
	return err
}

// PauseConsumer - Pauses consumption from both primary and retry topics.
// If primary topic is already paused, pauses just the retry consumer.
// Marks the consumer as paused. No messages will be returned on fetching messages.
func (c *consumerManager) PauseConsumer(ctx context.Context) error {
	err := c.pauseRetryConsumer(ctx)
	if err != nil {
		return err
	}

	if c.pausePrimaryTopic {
		// retry paused, primary already paused
		c.consumerPaused = true
		return nil
	}

	err = c.pausePrimaryConsumer(ctx)
	if err != nil {
		return err
	}
	// retry paused, primary paused
	c.consumerPaused = true
	return nil
}

// IsPaused - Checks if the entire consumer is paused.
func (c *consumerManager) IsPaused(ctx context.Context) bool {
	return c.consumerPaused
}

// ResumeConsumer - Resumes the consumption from retry and primary topics.
// If primary topic is explicitly paused, only retry topic consumption is resumed.
func (c *consumerManager) ResumeConsumer(ctx context.Context) error {
	if !c.consumerPaused {
		return cannotResume
	}

	err := c.resumeRetryConsumer(ctx)
	if err != nil {
		return err
	}
	subscriberPausedConsumersTotal.WithLabelValues(env, c.retryTopic, c.subscriptionName, c.subscriberID).Dec()

	if c.pausePrimaryTopic {
		// Retry consumer resumed, primary consumer not to be resumed
		c.consumerPaused = false
		return nil
	}

	err = c.resumePrimaryConsumer(ctx)
	if err != nil {
		return err
	}

	// Retry consumer resumed, primary consumer resumed
	c.consumerPaused = false
	subscriberPausedConsumersTotal.WithLabelValues(env, c.primaryTopic, c.subscriptionName, c.subscriberID).Dec()
	return nil
}

// PausePrimaryConsumer - Pauses only the primary consumer. If the entire consumer is
// paused, the primary consumer is marked as explicitly paused. Explicitly need to call
// ResumePrimaryConsumer to resume consumption from primary topic.
func (c *consumerManager) PausePrimaryConsumer(ctx context.Context) error {
	if c.consumerPaused {
		// Consumer already paused, set primary topic status as paused
		c.pausePrimaryTopic = true
		return nil
	}

	err := c.pausePrimaryConsumer(ctx)
	if err != nil {
		return err
	}
	c.pausePrimaryTopic = true

	return nil
}

// IsPrimaryPaused - Checks if the primary consumer is explicitly paused.
func (c *consumerManager) IsPrimaryPaused(ctx context.Context) bool {
	return c.pausePrimaryTopic
}

// ResumePrimaryConsumer - Resumes primary consumer that was explicitly paused.
func (c *consumerManager) ResumePrimaryConsumer(ctx context.Context) error {
	if !c.pausePrimaryTopic {
		return cannotResume
	}

	if c.consumerPaused {
		// consumer paused. Will resume on unpausing consumer
		c.pausePrimaryTopic = false
		return nil
	}

	err := c.resumePrimaryConsumer(ctx)
	if err != nil {
		return err
	}

	c.pausePrimaryTopic = false
	return nil
}

// CommitByPartitionAndOffset - Commits the offsets onto the broker.
func (c *consumerManager) CommitByPartitionAndOffset(ctx context.Context,
	req messagebroker.CommitOnTopicRequest) (messagebroker.CommitOnTopicResponse, error) {
	return c.consumer.CommitByPartitionAndOffset(ctx, req)
}

// ReceiveMessages - Read messages from the subscribed topic.
func (c *consumerManager) ReceiveMessages(ctx context.Context,
	req messagebroker.GetMessagesFromTopicRequest) (*messagebroker.GetMessagesFromTopicResponse, error) {
	return c.consumer.ReceiveMessages(ctx, req)
}

// GetTopicMetadata ...
func (c *consumerManager) GetTopicMetadata(ctx context.Context,
	req messagebroker.GetTopicMetadataRequest) (messagebroker.GetTopicMetadataResponse, error) {
	return c.consumer.GetTopicMetadata(ctx, req)
}

// Close - Removes the consumer from the brokerstore and closes the consumer.
func (c *consumerManager) Close(ctx context.Context) error {
	wasFound := c.brokerStore.RemoveConsumer(ctx, messagebroker.ConsumerClientOptions{
		GroupID:         c.subscriptionName,
		GroupInstanceID: c.subscriberID,
	})
	if wasFound {
		// close consumer only if we are able to successfully find and delete consumer from the brokerStore.
		// if the entry is already deleted from brokerStore, that means some other goroutine has already closed the consumer.
		// in such cases do not attempt to close the consumer again else it will panic
		return c.consumer.Close(ctx)
	}
	return nil
}
