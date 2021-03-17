package stream

import (
	"context"
	"sync"

	"github.com/razorpay/metro/pkg/logger"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"golang.org/x/sync/errgroup"
)

// IManager ...
type IManager interface {
	CreateNewStream(server metrov1.Subscriber_StreamingPullServer, req *ParsedStreamingPullRequest, errGroup *errgroup.Group) error
	Acknowledge(ctx context.Context, req *ParsedStreamingPullRequest) error
	ModifyAcknowledgement(ctx context.Context, req *ParsedStreamingPullRequest) error
}

// Manager ...
type Manager struct {
	// manage active streams by stream_id / subscriber_id
	// in case of graceful shutdown nack all messages held in these streams
	pullStreams       map[string]IStream
	subscriptionCore  subscription.ICore
	bs                brokerstore.IBrokerStore
	activeStreamCount map[string]uint32 // TODO: will remove. maintain a distributed counter for active streams per subscription
	cleanupCh         chan string       // listens for closed subscribers
	mutex             *sync.Mutex
	ctx               context.Context
}

// NewStreamManager ...
func NewStreamManager(ctx context.Context, subscriptionCore subscription.ICore, bs brokerstore.IBrokerStore) IManager {
	mgr := &Manager{
		pullStreams:       make(map[string]IStream),
		subscriptionCore:  subscriptionCore,
		activeStreamCount: make(map[string]uint32),
		bs:                bs,
		cleanupCh:         make(chan string),
		mutex:             &sync.Mutex{},
		ctx:               ctx,
	}

	go mgr.run()

	return mgr
}

func (s *Manager) run() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case subscriberID := <-s.cleanupCh:
			logger.Ctx(s.ctx).Infow("manager: got request to cleanup subscriber", "subscriberID", subscriberID)
			s.mutex.Lock()
			if _, ok := s.pullStreams[subscriberID]; ok {
				delete(s.pullStreams, subscriberID)
				logger.Ctx(s.ctx).Infow("manager: deleted subscriber from store", "subscriberID", subscriberID)
			} else {
				logger.Ctx(s.ctx).Infow("manager: skipping cleanup for subscriber", "subscriberID", subscriberID)
			}
			s.mutex.Unlock()
		}
	}
}

// CreateNewStream ...
func (s *Manager) CreateNewStream(server metrov1.Subscriber_StreamingPullServer, req *ParsedStreamingPullRequest, errGroup *errgroup.Group) error {
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
		s.cleanupCh,
	)
	if err != nil {
		return err
	}

	logger.Ctx(server.Context()).Infow("created new pull stream", "subscriberID", pullStream.subscriberID)

	// store all active pull streams in a map
	s.mutex.Lock()
	s.pullStreams[pullStream.subscriberID] = pullStream
	s.mutex.Unlock()

	return nil
}

// Acknowledge ...
func (s *Manager) Acknowledge(ctx context.Context, req *ParsedStreamingPullRequest) error {
	for _, ackMsg := range req.AckMessages {
		if ackMsg.MatchesOriginatingMessageServer() {
			// find active stream
			if pullStream, ok := s.pullStreams[ackMsg.SubscriberID]; ok {
				pullStream.acknowledge(ctx, ackMsg)
			}
		} else {
			// proxy request to the correct server
		}
	}

	return nil
}

// ModifyAcknowledgement ...
func (s *Manager) ModifyAcknowledgement(ctx context.Context, req *ParsedStreamingPullRequest) error {
	for _, ackMsg := range req.AckMessages {
		// non zero ack deadline is not supported, hence continue
		if req.ModifyDeadlineMsgIdsWithSecs[ackMsg.MessageID] != 0 {
			continue
		}
		if ackMsg.MatchesOriginatingMessageServer() {
			// find active stream
			if pullStream, ok := s.pullStreams[ackMsg.SubscriberID]; ok {
				pullStream.modifyAckDeadline(ctx, subscriber.NewModAckMessage(ackMsg, req.ModifyDeadlineMsgIdsWithSecs[ackMsg.MessageID]))
			}
		} else {
			// proxy request to the correct server
		}
	}

	return nil
}
