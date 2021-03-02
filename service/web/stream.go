package web

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"golang.org/x/sync/errgroup"
)

type pullStream struct {
	clientID               string
	subscriberID           string
	subscriberCore         subscriber.ICore
	subscriptionSubscriber subscriber.ISubscriber

	// stream server constructs below
	ctx          context.Context
	server       metrov1.Subscriber_StreamingPullServer
	reqChan      chan *metrov1.StreamingPullRequest
	errChan      chan error
	responseChan chan metrov1.PullResponse
}

// DefaultNumMessagesToReadOffStream ...
var DefaultNumMessagesToReadOffStream int32 = 10

func (s *pullStream) run() error {
	// stream ack timeout
	streamAckDeadlineSecs := int32(5) // init with some sane value
	timeout := time.NewTicker(time.Duration(streamAckDeadlineSecs) * time.Second)
	go s.receive()
	for {
		select {
		case <-s.ctx.Done():
			s.stop()
			return s.ctx.Err()
		case <-timeout.C:
			s.stop()
			return fmt.Errorf("stream ack deadline seconds crossed")
		case err := <-s.errChan:
			s.stop()
			if err == io.EOF {
				// return will close stream from server side
				logger.Ctx(s.ctx).Errorw("EOF received from client")
			}
			if err != nil {
				logger.Ctx(s.ctx).Errorw("error received from client", "msg", err.Error())
			}
			return nil
		case req := <-s.reqChan:
			parsedReq, parseErr := newParsedStreamingPullRequest(req)
			if parseErr != nil {
				logger.Ctx(s.ctx).Errorw("error is parsing pull request", "request", req, "error", parseErr.Error())
				return parseErr
			}
			if parsedReq.HasAcknowledgement() {
				// request to ack messages
				for _, v := range parsedReq.AckMessages {
					err := s.modifyAckDeadline(s.ctx, v)
					if err != nil {
						return merror.ToGRPCError(err)
					}
				}
			}
			if parsedReq.HasModifyAcknowledgement() {
				// TODO: implement
			}
			// reset stream ack deadline seconds
			if req.StreamAckDeadlineSeconds != 0 {
				timeout.Stop()
				streamAckDeadlineSecs = req.StreamAckDeadlineSeconds
				timeout = time.NewTicker(time.Duration(streamAckDeadlineSecs) * time.Second)
			}
		default:
			// once stream is established, we can continuously send messages over it
			s.subscriptionSubscriber.GetRequestChannel() <- &subscriber.PullRequest{DefaultNumMessagesToReadOffStream}
			select {
			case res := <-s.subscriptionSubscriber.GetResponseChannel():
				if len(res.ReceivedMessages) > 0 {
					err := s.server.Send(&metrov1.StreamingPullResponse{ReceivedMessages: res.ReceivedMessages})
					if err != nil {
						logger.Ctx(s.ctx).Errorw("error in send", "msg", err.Error())
						s.stop()
						return nil
					}
					logger.Ctx(s.ctx).Infow("StreamingPullResponse sent", "numOfMessages", len(res.ReceivedMessages))
				}
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
	// TODO: implement
	return nil
}

func (s *pullStream) stop() {
	s.subscriptionSubscriber.Stop()
}

func newPullStream(server metrov1.Subscriber_StreamingPullServer, clientID string, subscription string, subscriberCore subscriber.ICore, errGroup *errgroup.Group) (*pullStream, error) {
	//nCtx, cancelFunc := context.WithCancel(server.Context())
	subs, err := subscriberCore.NewSubscriber(server.Context(), clientID, subscription, 10, 0, 0)
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
		subscriberID:           subs.GetID(),
		reqChan:                make(chan *metrov1.StreamingPullRequest),
		errChan:                make(chan error),
	}

	errGroup.Go(pr.run)
	return pr, nil
}
