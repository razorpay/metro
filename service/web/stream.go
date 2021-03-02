package web

import (
	"context"
	"io"
	"time"

	"github.com/razorpay/metro/pkg/logger"

	"github.com/razorpay/metro/internal/subscriber"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

type pullStream struct {
	clientID               string
	subscriberID           string
	subscriberCore         subscriber.ICore
	subscriptionSubscriber subscriber.ISubscriber
	cancelFunc             func()
	stopChan               chan struct{}

	// stream server constructs below
	ctx          context.Context
	server       metrov1.Subscriber_StreamingPullServer
	reqChan      chan *metrov1.StreamingPullRequest
	errChan      chan error
	responseChan chan metrov1.PullResponse
}

// DefaultNumMessagesToReadOffStream ...
var DefaultNumMessagesToReadOffStream int32 = 10

func (s *pullStream) run() {
	go s.receive()

	// stream ack timeout
	timeout := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-s.ctx.Done():
			s.stop()
			s.server.SendMsg(s.ctx.Err())
		case <-timeout.C:
			s.stop()
		case err := <-s.errChan:
			s.stop()

			if err == io.EOF {
				// return will close stream from server side
				logger.Ctx(s.ctx).Errorw("EOF received from client")
			}
			if err != nil {
				logger.Ctx(s.ctx).Errorw("error received from client", "msg", err.Error())
			}

			s.server.SendMsg(err.Error())
		case req := <-s.reqChan:
			// reset stream ack deadline seconds
			timeout.Stop()
			timeout = time.NewTicker(time.Duration(req.StreamAckDeadlineSeconds) * time.Second)

			s.subscriptionSubscriber.GetRequestChannel() <- &subscriber.PullRequest{DefaultNumMessagesToReadOffStream}
			select {
			case res := <-s.subscriptionSubscriber.GetResponseChannel():
				s.responseChan <- res
			}

		case res := <-s.responseChan:
			err := s.server.Send(&metrov1.StreamingPullResponse{ReceivedMessages: res.ReceivedMessages})
			if err != nil {
				logger.Ctx(s.ctx).Errorw("error in send", "msg", err.Error())
				s.stop()
				return
			}
			logger.Ctx(s.ctx).Infow("StreamingPullResponse sent", "numOfMessages", len(res.ReceivedMessages))
		default:
			timeout.Stop()
			timeout = time.NewTicker(5 * time.Second) // TODO: decide a value here

			// once stream is established, we can continuously send messages over it
			s.subscriptionSubscriber.GetRequestChannel() <- &subscriber.PullRequest{DefaultNumMessagesToReadOffStream}
			select {
			case res := <-s.subscriptionSubscriber.GetResponseChannel():
				s.responseChan <- res
			}
		}
	}
}

// read requests / error off the active server stream
func (s *pullStream) receive() {
	req, err := s.server.Recv()
	if err != nil {
		s.errChan <- err
	}
	s.reqChan <- req
}

func (s *pullStream) acknowledge(ctx context.Context, req *subscriber.AckMessage) error {
	return s.subscriptionSubscriber.Acknowledge(ctx, req)
}

func (s *pullStream) modifyAckDeadline(ctx context.Context, req *subscriber.AckMessage) error {
	return s.subscriptionSubscriber.ModifyAckDeadline(ctx, req)
}

func (s *pullStream) stop() {
	s.subscriptionSubscriber.Stop()
	s.cancelFunc()
	<-s.stopChan
}

func newPullStream(server metrov1.Subscriber_StreamingPullServer, clientID string, subscription string, subscriberCore subscriber.ICore) (*pullStream, error) {
	nCtx, cancelFunc := context.WithCancel(server.Context())
	subs, err := subscriberCore.NewSubscriber(nCtx, clientID, subscription, 10, 0, 0)
	if err != nil {
		return nil, err
	}
	pr := &pullStream{
		ctx:                    server.Context(),
		server:                 server,
		clientID:               clientID,
		subscriberCore:         subscriberCore,
		subscriptionSubscriber: subs,
		responseChan:           make(chan metrov1.PullResponse),
		cancelFunc:             cancelFunc,
		subscriberID:           subs.GetID(),
		reqChan:                make(chan *metrov1.StreamingPullRequest),
		errChan:                make(chan error),
	}

	go pr.run()
	return pr, nil
}
