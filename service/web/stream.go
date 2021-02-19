package web

import (
	"context"

	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

type pullStream struct {
	clientID               string
	subscriberCore         subscriber.ICore
	subscriptionSubscriber subscriber.ISubscriber
	responseChan           chan<- metrov1.PullResponse
	ctx                    context.Context
	cancelFunc             func()
	stopChan               chan struct{}
}

func (s pullStream) run() {
	for {
		select {
		case <-s.ctx.Done():
			s.subscriptionSubscriber.Stop()
			s.stopChan <- struct{}{}
		default:
			s.subscriptionSubscriber.GetRequestChannel() <- &subscriber.PullRequest{10}
			select {
			case res := <-s.subscriptionSubscriber.GetResponseChannel():
				logger.Ctx(s.ctx).Info("got msgs")
				s.responseChan <- res
			}
		}
	}
}

func (s pullStream) acknowledge(ctx context.Context, req *subscriber.AcknowledgeRequest) error {
	return s.subscriptionSubscriber.Acknowledge(ctx, req)
}

func (s pullStream) modifyAckDeadline(ctx context.Context, req *subscriber.ModifyAckDeadlineRequest) error {
	return s.subscriptionSubscriber.ModifyAckDeadline(ctx, req)
}

func (s pullStream) stop() {
	s.cancelFunc()
	<-s.stopChan
}

func newPullStream(ctx context.Context, clientID string, subscription string, subscriberCore subscriber.ICore, responseChan chan<- metrov1.PullResponse) (*pullStream, error) {
	subs, err := subscriberCore.NewSubscriber(ctx, clientID, subscription, 10, 0, 0)
	if err != nil {
		return nil, err
	}
	nCtx, cancelFunc := context.WithCancel(ctx)
	return &pullStream{clientID: clientID, subscriberCore: subscriberCore, subscriptionSubscriber: subs, responseChan: responseChan, ctx: nCtx, cancelFunc: cancelFunc}, nil
}
