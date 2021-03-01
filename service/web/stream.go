package web

import (
	"context"
	"github.com/razorpay/metro/pkg/logger"

	"github.com/razorpay/metro/internal/subscriber"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

type pullStream struct {
	clientID               string
	subscriberID           string
	subscriberCore         subscriber.ICore
	subscriptionSubscriber subscriber.ISubscriber
	responseChan           chan metrov1.PullResponse
	cancelFunc             func()
	stopChan               chan struct{}
	server                 metrov1.Subscriber_StreamingPullServer
}

func (s pullStream) run(ctx context.Context) {
	for {
		select {
		case res := <-s.responseChan:
			err := s.server.Send(&metrov1.StreamingPullResponse{ReceivedMessages: res.ReceivedMessages})
			if err != nil {
				logger.Ctx(ctx).Errorw("error in send", "msg", err.Error())
				s.stop()
				return
			}
			logger.Ctx(ctx).Infow("StreamingPullResponse sent", "numOfMessages", len(res.ReceivedMessages))
		case <-ctx.Done():
			return
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

func (s pullStream) acknowledge(ctx context.Context, req *subscriber.AckMessage) error {
	return s.subscriptionSubscriber.Acknowledge(ctx, req)
}

func (s pullStream) modifyAckDeadline(ctx context.Context, req *subscriber.AckMessage) error {
	return s.subscriptionSubscriber.ModifyAckDeadline(ctx, req)
}

func (s pullStream) stop() {
	s.cancelFunc()
	<-s.stopChan
}

func newPullStream(server metrov1.Subscriber_StreamingPullServer, clientID string, subscription string, subscriberCore subscriber.ICore, responseChan chan metrov1.PullResponse) (*pullStream, error) {
	nCtx, cancelFunc := context.WithCancel(server.Context())
	subs, err := subscriberCore.NewSubscriber(nCtx, clientID, subscription, 10, 0, 0)
	if err != nil {
		return nil, err
	}
	pr := &pullStream{server: server, clientID: clientID, subscriberCore: subscriberCore, subscriptionSubscriber: subs, responseChan: responseChan, cancelFunc: cancelFunc, subscriberID: subs.GetId()}
	go pr.run(nCtx)
	return pr, nil
}
