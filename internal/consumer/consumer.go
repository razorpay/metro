package consumer

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/messagebroker"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// IConsumer defines the set of methods to access a consumer object
type IConsumer interface {
	Run() error
	Acknowledge(ctx context.Context, req *ParsedAcknowledgeRequest)
	ModifyAckDeadline(ctx context.Context, req *ParsedModifyAckDeadlineRequest)
	Fetch(ctx context.Context, messageCount int) (*metrov1.PullResponse, error)
}

// Consumer entity represents a single subscription-partition specific client
type Consumer struct {
	computedHash           int
	subscriberID           string
	subscription           *subscription.Model
	subscriberCore         subscriber.ICore
	subscriptionSubscriber subscriber.ISubscriber
	ctx                    context.Context
	errChan                chan error
}

// DefaultNumMessageCount ...
var DefaultNumMessageCount int32 = 10

// NewConsumer intializes a consumer entity
func NewConsumer(ctx context.Context, computedHash int, subscriberID string, subscription *subscription.Model, subCore subscriber.ICore, subs subscriber.ISubscriber) *Consumer {
	con := &Consumer{
		ctx:                    ctx,
		computedHash:           computedHash,
		subscriberID:           subscriberID,
		subscription:           subscription,
		subscriptionSubscriber: subs,
		errChan:                make(chan error),
	}
	return con
}

// Fetch retrieves messages for a given consumer, it takes ackDeadline, retry and maxMessages into account.
func (c *Consumer) Fetch(ctx context.Context, messageCount int) (*metrov1.PullResponse, error) {
	respChan := make(chan *metrov1.PullResponse)
	defer close(respChan)
	c.subscriptionSubscriber.GetRequestChannel() <- (&subscriber.PullRequest{
		MaxNumOfMessages: int32(messageCount),
		RespChan:         respChan,
	}).WithContext(ctx)

	select {
	case resp := <-respChan:
		return resp, nil
	case <-ctx.Done():
		return &metrov1.PullResponse{}, ctx.Err()
	}

}

// Acknowledge send an ACK for a set of messages
func (c *Consumer) Acknowledge(ctx context.Context, ackMsgs []*subscriber.AckMessage) {
	for _, ackMsg := range ackMsgs {
		c.subscriptionSubscriber.GetAckChannel() <- ackMsg.WithContext(ctx)
	}
}

// ModifyAckDeadline allows modification of Ack deadline for a messages(s).
// Deadline of 0 indicates a Nack operation.
func (c *Consumer) ModifyAckDeadline(ctx context.Context, mackMsgs []*subscriber.AckMessage) {
	for _, modAckMsg := range mackMsgs {
		modAckReq := subscriber.NewModAckMessage(modAckMsg, modAckMsg.Deadline)
		modAckReq = modAckReq.WithContext(ctx)
		c.subscriptionSubscriber.GetModAckChannel() <- modAckReq
	}
}

// Run ensures that the lifecycle of a consumer is instantiated.
func (c *Consumer) Run() error {
	// stream ack timeout
	streamAckDeadlineSecs := int32(30) // init with some sane value
	timeout := time.NewTicker(time.Duration(streamAckDeadlineSecs) * time.Second)
	for {
		select {
		case <-c.ctx.Done():
			logger.Ctx(c.ctx).Infow("stopping subscriber from <-s.ctx.Done()")
			c.stop()
			return c.ctx.Err()
		case <-timeout.C:
			logger.Ctx(c.ctx).Infow("stopping subscriber from <-timeout.C")
			c.stop()
			return fmt.Errorf("stream: ack deadline seconds crossed")
		case err := <-c.errChan:
			logger.Ctx(c.ctx).Infow("stopping subscriber from err := <-s.errChan")
			c.stop()
			if err == io.EOF {
				// return will close stream from server side
				logger.Ctx(c.ctx).Errorw("stream: EOF received from client")
			} else if err != nil {
				logger.Ctx(c.ctx).Errorw("stream: error received from client", "error", err.Error())
			}
			return nil
		case err := <-c.subscriptionSubscriber.GetErrorChannel():
			// streamManagerSubscriberErrors.WithLabelValues(env, s.subscriberID, s.subscriptionSubscriber.GetSubscriptionName(), err.Error()).Inc()
			if messagebroker.IsErrorRecoverable(err) {
				// no need to stop the subscriber in such cases. just log and return
				logger.Ctx(c.ctx).Errorw("subscriber: got recoverable error", err.Error())
				return nil
			}

			logger.Ctx(c.ctx).Errorw("subscriber: got un-recoverable error", "error", err.Error())
			logger.Ctx(c.ctx).Infow("stopping subscriber from err := <-s.subscriptionSubscriber.GetErrorChannel()")
			c.stop()
			return err

		default:
			timeout.Reset(time.Duration(streamAckDeadlineSecs) * time.Second)
		}
	}
}

func (c *Consumer) stop() {
	c.subscriptionSubscriber.Stop()
	c.closeSubscriberChannels()

	logger.Ctx(c.ctx).Infow("stopped subscriber...", "subscriberId", c.subscriberID)

}

func (c *Consumer) closeSubscriberChannels() {
	close(c.errChan)
	close(c.subscriptionSubscriber.GetRequestChannel())
	close(c.subscriptionSubscriber.GetAckChannel())
	close(c.subscriptionSubscriber.GetModAckChannel())
}
