package stream

import (
	"context"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"golang.org/x/sync/errgroup"
)

// IStreamManger ...
type IStreamManger interface {
	CreateNewStream(server metrov1.Subscriber_StreamingPullServer, req *ParsedStreamingPullRequest, errGroup *errgroup.Group) error
	Acknowledge(ctx context.Context, req *ParsedStreamingPullRequest) error
	ModifyAcknowledgement(ctx context.Context, req *ParsedStreamingPullRequest) error
}

// StreamManger ...
type StreamManger struct {
	// manage active streams by stream_id / subscriber_id
	// in case of graceful shutdown nack all messages held in these streams
	pullStreams map[string]IStream

	subscriptionCore subscription.ICore

	bs brokerstore.IBrokerStore

	// TODO: heap code. will remove. maintain a distributed counter for active streams per subscription
	activeStreamCount map[string]uint32
}

// NewStreamManager ...
func NewStreamManager(subscriptionCore subscription.ICore, bs brokerstore.IBrokerStore) IStreamManger {
	return &StreamManger{
		pullStreams:       make(map[string]IStream),
		subscriptionCore:  subscriptionCore,
		activeStreamCount: make(map[string]uint32),
		bs:                bs,
	}
}

// CreateNewStream ...
func (s *StreamManger) CreateNewStream(server metrov1.Subscriber_StreamingPullServer, req *ParsedStreamingPullRequest, errGroup *errgroup.Group) error {
	var (
		// query allow concurrency for subscription from DB
		allowedSubscriptionConcurrency uint32
		// query active streams for subscription
		activeSubscriptionStreamsCount uint32
	)

	if activeSubscriptionStreamsCount+1 > allowedSubscriptionConcurrency {
		// TODO : uncomment later on
		//return errors.New("reached max active stream limit for subscription")
	}

	pullStream, err := newPullStream(server,
		req.ClientID,
		req.Subscription,
		subscriber.NewCore(s.bs, s.subscriptionCore),
		errGroup,
	)
	if err != nil {
		return err
	}

	// store all active pull streams in a map
	s.pullStreams[pullStream.subscriberID] = pullStream

	return nil

}

// Acknowledge ...
func (s *StreamManger) Acknowledge(ctx context.Context, req *ParsedStreamingPullRequest) error {
	for _, ackMsg := range req.AckMessages {
		if ackMsg.MatchesOriginatingMessageServer() {
			// find active stream
			if pullStream, ok := s.pullStreams[ackMsg.SubscriberID]; ok {
				pullStream.Acknowledge(ctx, ackMsg)
			}
		} else {
			// proxy request to the correct server
		}
	}

	return nil
}

// ModifyAcknowledgement ...
func (s *StreamManger) ModifyAcknowledgement(ctx context.Context, req *ParsedStreamingPullRequest) error {
	// TODO: implement
	return nil
}
