package web

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/razorpay/metro/internal/merror"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/logger"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

type subscriberserver struct {
	subscriptionCore *subscription.Core

	psm IStreamManger
}

func newSubscriberServer(subscriptionCore *subscription.Core, psm IStreamManger) *subscriberserver {
	return &subscriberserver{subscriptionCore, psm}
}

// CreateSubscription to create a new subscription
func (s subscriberserver) CreateSubscription(ctx context.Context, req *metrov1.Subscription) (*metrov1.Subscription, error) {
	logger.Ctx(ctx).Infow("received request to create subscription", "name", req.Name, "topic", req.Topic)
	m, err := subscription.GetValidatedModelForCreate(ctx, req)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	err = s.subscriptionCore.CreateSubscription(ctx, m)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	return req, nil
}

// Acknowledge a message
func (s subscriberserver) Acknowledge(ctx context.Context, req *metrov1.AcknowledgeRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("received request to ack messages")
	return new(emptypb.Empty), nil
}

// Pull messages
func (s subscriberserver) Pull(ctx context.Context, req *metrov1.PullRequest) (*metrov1.PullResponse, error) {
	logger.Ctx(ctx).Infow("received request to pull messages")
	// non streaming pull not to be supported
	/*
		res, err := s.subscriberCore.Pull(ctx, &subscriber.PullRequest{req.Subscription, 0, 0}, 2, xid.New().String()) // TODO: fix
		if err != nil {
			logger.Ctx(ctx).Errorw("pull response errored", "msg", err.Error())
			return nil, merror.ToGRPCError(err)
		}
		return &metrov1.PullResponse{ReceivedMessages: res.ReceivedMessages}, nil

	*/
	return &metrov1.PullResponse{}, nil
}

// StreamingPull ...
func (s subscriberserver) StreamingPull(server metrov1.Subscriber_StreamingPullServer) error {
	// TODO: check if the requested subscription is push based and handle it the way pubsub does
	ctx := server.Context()

	reqChan := make(chan *metrov1.StreamingPullRequest)
	errChan := make(chan error)
	streamResponseChan := make(chan metrov1.PullResponse)

	for {
		// read messages off the pull stream server
		go receive(server, reqChan, errChan)

		select {
		case req := <-reqChan:
			parsedReq, parseErr := newParsedStreamingPullRequest(req)
			if parseErr != nil {
				logger.Ctx(ctx).Errorw("error is parsing pull request", "request", req, "error", parseErr.Error())
				return nil
			}

			// request to init a new stream
			if parsedReq.HasSubscription() {
				err := s.psm.CreateNewStream(server, parsedReq)
				if err != nil {
					return merror.ToGRPCError(err)
				}
			} else if parsedReq.HasAcknowledgement() {
				// request to acknowledge existing messages
				err := s.psm.Acknowledge(server, parsedReq)
				if err != nil {
					return merror.ToGRPCError(err)
				}
			}
		}
	}

	var pullStream *pullStream
	var req *metrov1.StreamingPullRequest
	var timeout *time.Ticker
	timeout = time.NewTicker(5 * time.Second) // init with some sane value

	for {
		// receive request in a goroutine, to timeout on stream ack deadline seconds

		logger.Ctx(ctx).Info("loop")
		select {
		case <-ctx.Done():
			logger.Ctx(ctx).Infow("returning after context is done")
			if pullStream != nil {
				pullStream.stop()
			}
			return ctx.Err()
		// stream ack deadline
		case <-timeout.C:
			if pullStream != nil {
				pullStream.stop()
			}
			return fmt.Errorf("stream ack deadline seconds crossed")
		case err := <-errChan:
			if pullStream != nil {
				pullStream.stop()
			}
			if err == io.EOF {
				// return will close stream from server side
				logger.Ctx(ctx).Info("EOF received from client")
				return nil
			}
			if err != nil {
				logger.Ctx(ctx).Infow("error received from client", "msg", err.Error())
				return nil
			}
		case req = <-reqChan:
			// reset stream ack deadline seconds
			timeout.Stop()
			timeout = time.NewTicker(time.Duration(req.StreamAckDeadlineSeconds) * time.Second)
			ackReq, modAckReq, err := subscriber.FromProto(req)
			if err != nil {
				return merror.ToGRPCError(err)
			}
			// TODO: check and add if error is returned when subscription is changed in subsequent requests
			// continue on ping request
			if ackReq.IsEmpty() && modAckReq.IsEmpty() {
				continue
			}
			// if its the first req and subscriber is not yet initialised
			if pullStream == nil {
				if req.Subscription == "" {
					return fmt.Errorf("subscription name empty")
				}
				pullStream, err = newPullStream(ctx,
					req.ClientId,
					req.Subscription,
					nil,
					//s.subscriberCore,
					streamResponseChan,
				)
				if err != nil {
					return merror.ToGRPCError(err)
				}
				go func() {
					for {
						select {
						case res := <-streamResponseChan:
							err := server.Send(&metrov1.StreamingPullResponse{ReceivedMessages: res.ReceivedMessages})
							if err != nil {
								logger.Ctx(ctx).Errorw("error in send", "msg", err.Error())
								pullStream.stop()
								return
							}
							logger.Ctx(ctx).Infow("StreamingPullResponse sent", "numOfMessages", len(res.ReceivedMessages))
						case <-ctx.Done():
							return
						}
					}
				}()
			}
			err = pullStream.acknowledge(ctx, ackReq)
			if err != nil {
				return merror.ToGRPCError(err)
			}
			err = pullStream.modifyAckDeadline(ctx, modAckReq)
			if err != nil {
				return merror.ToGRPCError(err)
			}
		}
	}
	return nil
}

// DeleteSubscription deletes a subscription
func (s subscriberserver) DeleteSubscription(ctx context.Context, req *metrov1.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("received request to delete subscription", "name", req.Subscription)
	m, err := subscription.GetValidatedModelForDelete(ctx, &metrov1.Subscription{Name: req.Subscription})
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	err = s.subscriptionCore.DeleteSubscription(ctx, m)
	if err != nil {
		return nil, merror.ToGRPCError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s subscriberserver) ModifyAckDeadline(ctx context.Context, in *metrov1.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	logger.Ctx(ctx).Infow("received request to modack messages")
	return &emptypb.Empty{}, nil
}

func receive(server metrov1.Subscriber_StreamingPullServer, requestChan chan *metrov1.StreamingPullRequest, errChan chan error) {
	req, err := server.Recv()
	if err != nil {
		errChan <- err
	}
	requestChan <- req
}
