package web

import (
	"context"

	"github.com/razorpay/metro/internal/subscriber"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

type pullStream struct {
	clientID               string
	subscriberCore         subscriber.ICore
	subscriptionSubscriber subscriber.ISubscriber
	responseChan           chan<- metrov1.PullResponse
	cancelFunc             func()
	stopChan               chan struct{}
}

func (s pullStream) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.subscriptionSubscriber.Stop()
			s.stopChan <- struct{}{}
		default:
			s.subscriptionSubscriber.GetRequestChannel() <- &subscriber.PullRequest{10}
			select {
			case res := <-s.subscriptionSubscriber.GetResponseChannel():
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
	nCtx, cancelFunc := context.WithCancel(ctx)
	subs, err := subscriberCore.NewSubscriber(nCtx, clientID, subscription, 10, 0, 0)
	if err != nil {
		return nil, err
	}
	pr := &pullStream{clientID: clientID, subscriberCore: subscriberCore, subscriptionSubscriber: subs, responseChan: responseChan, cancelFunc: cancelFunc}
	go pr.run(nCtx)
	return pr, nil
}
