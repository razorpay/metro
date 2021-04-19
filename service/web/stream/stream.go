package stream

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"

	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"golang.org/x/sync/errgroup"
)

// IStream ...
type IStream interface {
	run() error
	stop()
	receive()
	acknowledge(ctx context.Context, req *subscriber.AckMessage)
	modifyAckDeadline(ctx context.Context, req *subscriber.ModAckMessage)
}

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
	cleanupCh    chan cleanupMessage
}

// DefaultNumMessagesToReadOffStream ...
var DefaultNumMessagesToReadOffStream int32 = 10

func (s *pullStream) run() error {
	// stream ack timeout
	streamAckDeadlineSecs := int32(30) // init with some sane value
	timeout := time.NewTicker(time.Duration(streamAckDeadlineSecs) * time.Second)
	for {
		go s.receive()
		select {
		case <-s.ctx.Done():
			logger.Ctx(s.ctx).Infow("stopping subscriber from <-s.ctx.Done()")
			s.stop()
			return s.ctx.Err()
		case <-timeout.C:
			logger.Ctx(s.ctx).Infow("stopping subscriber from <-timeout.C")
			s.stop()
			return fmt.Errorf("stream: ack deadline seconds crossed")
		case err := <-s.errChan:
			logger.Ctx(s.ctx).Infow("stopping subscriber from err := <-s.errChan")
			s.stop()
			if err == io.EOF {
				// return will close stream from server side
				logger.Ctx(s.ctx).Errorw("stream: EOF received from client")
			}
			if err != nil {
				logger.Ctx(s.ctx).Errorw("stream: error received from client", "error", err.Error())
			}
			return nil
		case err := <-s.subscriptionSubscriber.GetErrorChannel():
			if isErrorRecoverable(err) {
				// no need to stop the subscriber in such cases. just log and return
				logger.Ctx(s.ctx).Errorw("subscriber: got recoverable error", err.Error())
				return nil
			}

			logger.Ctx(s.ctx).Errorw("subscriber: got un-recoverable error", "error", err.Error())
			logger.Ctx(s.ctx).Infow("stopping subscriber from err := <-s.subscriptionSubscriber.GetErrorChannel()")
			s.stop()
			return err

		case req := <-s.reqChan:
			logger.Ctx(s.ctx).Infow("got pull request", "request", req)
			parsedReq, parseErr := NewParsedStreamingPullRequest(req)
			if parseErr != nil {
				logger.Ctx(s.ctx).Errorw("stream: error is parsing pull request", "request", req, "error", parseErr.Error())
				return parseErr
			}

			if parsedReq.HasAcknowledgement() {
				// request to ack messages
				for _, ackMsg := range parsedReq.AckMessages {
					s.subscriptionSubscriber.GetAckChannel() <- ackMsg
				}
			}

			if parsedReq.HasModifyAcknowledgement() {
				// request to mod ack messages
				for _, modAckMsg := range parsedReq.AckMessages {
					s.subscriptionSubscriber.GetModAckChannel() <- subscriber.NewModAckMessage(modAckMsg, parsedReq.ModifyDeadlineMsgIdsWithSecs[modAckMsg.MessageID])
				}
			}

			// reset stream ack deadline seconds
			if req.StreamAckDeadlineSeconds != 0 {
				streamAckDeadlineSecs = req.StreamAckDeadlineSeconds
				timeout.Reset(time.Duration(streamAckDeadlineSecs) * time.Second)
			}
		default:
			// once stream is established, we can continuously send messages over it
			req := &subscriber.PullRequest{MaxNumOfMessages: DefaultNumMessagesToReadOffStream}
			s.subscriptionSubscriber.GetRequestChannel() <- req
			logger.Ctx(s.ctx).Debugw("stream: sending default pull request over stream", "req", req)
			select {
			case res := <-s.subscriptionSubscriber.GetResponseChannel():
				if len(res.ReceivedMessages) > 0 {
					err := s.server.Send(&metrov1.StreamingPullResponse{ReceivedMessages: res.ReceivedMessages})
					if err != nil {
						logger.Ctx(s.ctx).Errorw("error in send", "msg", err.Error())
						s.stop()
						return nil
					}
					logger.Ctx(s.ctx).Debugw("stream: StreamingPullResponse sent", "numOfMessages", len(res.ReceivedMessages), "subscriber", s.subscriberID)
				}
			}
			timeout.Reset(time.Duration(streamAckDeadlineSecs) * time.Second)
		}
	}
}

// read requests / error off the active server stream
func (s *pullStream) receive() {
	req, err := s.server.Recv()
	if err != nil {
		s.errChan <- err
		return
	}
	s.reqChan <- req
}

func (s *pullStream) acknowledge(_ context.Context, req *subscriber.AckMessage) {
	s.subscriptionSubscriber.GetAckChannel() <- req
}

func (s *pullStream) modifyAckDeadline(_ context.Context, req *subscriber.ModAckMessage) {
	s.subscriptionSubscriber.GetModAckChannel() <- req
}

func (s *pullStream) stop() {
	s.subscriptionSubscriber.Stop()
	logger.Ctx(s.ctx).Infow("stopped subscriber...", "subscriberId", s.subscriberID)

	// notify stream manager to cleanup subscriber held in-memory
	s.cleanupCh <- cleanupMessage{
		subscriberID: s.subscriberID,
		subscription: s.subscriptionSubscriber.GetSubscription(),
	}
}

func newPullStream(server metrov1.Subscriber_StreamingPullServer, clientID string, subscription string, subscriberCore subscriber.ICore, errGroup *errgroup.Group, cleanupCh chan cleanupMessage) (*pullStream, error) {

	// use the clientID as the subscriberID if provided
	subscriberID := clientID
	if subscriberID == "" {
		subscriberID = uuid.New().String()
	}

	subs, err := subscriberCore.NewSubscriber(server.Context(), subscriberID, subscription, 100, 50, 5000)
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
		subscriberID:           subscriberID,
		reqChan:                make(chan *metrov1.StreamingPullRequest),
		errChan:                make(chan error),
		cleanupCh:              cleanupCh,
	}

	errGroup.Go(pr.run)
	return pr, nil
}
