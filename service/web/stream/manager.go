package stream

import (
	"context"
	"sync"

	"google.golang.org/grpc"

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
	Acknowledge(ctx context.Context, parsedReq *ParsedStreamingPullRequest) error
	ModifyAcknowledgement(ctx context.Context, req *ParsedStreamingPullRequest) error
}

// Manager ...
type Manager struct {
	// manage active streams by stream_id / subscriber_id
	// in case of graceful shutdown nack all messages held in these streams
	pullStreams       map[string]IStream
	subscriptionCore  subscription.ICore
	bs                brokerstore.IBrokerStore
	activeStreamCount map[string]uint32   // TODO: will remove. maintain a distributed counter for active streams per subscription
	cleanupCh         chan cleanupMessage // listens for closed subscribers
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
		cleanupCh:         make(chan cleanupMessage),
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
		case cleanupMessage := <-s.cleanupCh:
			logger.Ctx(s.ctx).Infow("manager: got request to cleanup subscriber", "cleanupMessage", cleanupMessage)
			s.mutex.Lock()
			if _, ok := s.pullStreams[cleanupMessage.subscriberID]; ok {
				streamManagerActiveStreams.WithLabelValues(env, cleanupMessage.subscriberID, cleanupMessage.subscription).Dec()
				delete(s.pullStreams, cleanupMessage.subscriberID)
				logger.Ctx(s.ctx).Infow("manager: deleted subscriber from store", "cleanupMessage", cleanupMessage)
			} else {
				logger.Ctx(s.ctx).Infow("manager: skipping cleanup for subscriber", "cleanupMessage", cleanupMessage)
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
	streamManagerActiveStreams.WithLabelValues(env, pullStream.subscriberID, req.Subscription).Inc()
	s.mutex.Unlock()

	return nil
}

// Acknowledge ...
func (s *Manager) Acknowledge(ctx context.Context, parsedReq *ParsedStreamingPullRequest) error {

	// holds a map of ackMsgs to their corresponding originating server addresses
	msgsToBeProxied := make(map[string][]*subscriber.AckMessage, 0)

	for _, ackMsg := range parsedReq.AckMessages {
		if ackMsg.MatchesOriginatingMessageServer() {
			// find active stream
			if pullStream, ok := s.pullStreams[ackMsg.SubscriberID]; ok {
				pullStream.acknowledge(ctx, ackMsg)
			}
		} else {
			proxyAddr := ackMsg.ServerAddress
			if _, ok := msgsToBeProxied[proxyAddr]; !ok {
				// init empty slice
				msgsToBeProxied[proxyAddr] = make([]*subscriber.AckMessage, 0)
			}
			msgsToBeProxied[proxyAddr] = append(msgsToBeProxied[proxyAddr], ackMsg)
		}
	}

	if len(msgsToBeProxied) > 0 {
		// proxy request to the correct server
		for proxyAddr, ackMsgs := range msgsToBeProxied {
			conn, err := grpc.Dial(proxyAddr, []grpc.DialOption{grpc.WithInsecure()}...)
			if err != nil {
				return err
			}

			ackIds := collectAckIds(ackMsgs)
			logger.Ctx(ctx).Infow("manager: acknowledge proxy request", "proxyAddr", proxyAddr, "ackIds", ackIds)

			proxyAckRequest := &metrov1.AcknowledgeRequest{
				Subscription: parsedReq.Subscription,
				AckIds:       ackIds,
			}
			client := metrov1.NewSubscriberClient(conn)
			_, aerr := client.Acknowledge(ctx, proxyAckRequest)
			if aerr != nil {
				logger.Ctx(ctx).Errorw("manager: acknowledge proxy request failed", "proxyAddr", proxyAddr, "error", aerr.Error())
				// on error, try to proxy remaining requests
				continue
			}
			logger.Ctx(ctx).Infow("manager: acknowledge proxy request succeeded", "proxyAddr", proxyAddr, "ackIds", ackIds)
		}
	}
	return nil
}

func collectAckIds(msgs []*subscriber.AckMessage) []string {
	ackIds := make([]string, 0)

	for _, msg := range msgs {
		ackIds = append(ackIds, msg.AckID)
	}
	return ackIds
}

// ModifyAcknowledgement ...
func (s *Manager) ModifyAcknowledgement(ctx context.Context, req *ParsedStreamingPullRequest) error {
	// holds a map of modAckMsgs to their corresponding originating server addresses
	msgsToBeProxied := make(map[string][]*subscriber.AckMessage, 0)

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
			proxyAddr := ackMsg.ServerAddress
			if _, ok := msgsToBeProxied[proxyAddr]; !ok {
				// init empty slice
				msgsToBeProxied[proxyAddr] = make([]*subscriber.AckMessage, 0)
			}
			msgsToBeProxied[proxyAddr] = append(msgsToBeProxied[proxyAddr], ackMsg)
		}
	}

	if len(msgsToBeProxied) > 0 {
		// proxy request to the correct server
		for proxyAddr, ackMsgs := range msgsToBeProxied {
			conn, err := grpc.Dial(proxyAddr, []grpc.DialOption{grpc.WithInsecure()}...)
			if err != nil {
				return err
			}

			ackIds := collectAckIds(ackMsgs)
			logger.Ctx(ctx).Infow("manager: modack proxy request", "proxyAddr", proxyAddr, "ackIds", ackIds)

			proxyModAckRequest := &metrov1.ModifyAckDeadlineRequest{
				Subscription: req.Subscription,
				AckIds:       ackIds,
				// pick up ack deadline time for any one of the message and set in the request
				// this is usually the same for all given ack_ids
				AckDeadlineSeconds: req.ModifyDeadlineMsgIdsWithSecs[ackMsgs[0].MessageID],
			}
			client := metrov1.NewSubscriberClient(conn)
			_, aerr := client.ModifyAckDeadline(ctx, proxyModAckRequest)
			if aerr != nil {
				logger.Ctx(ctx).Errorw("manager: modack proxy request failed", "proxyAddr", proxyAddr, "error", aerr.Error())
				// on error, try to proxy remaining requests
				continue
			}
			logger.Ctx(ctx).Errorw("manager: modack proxy request succeeded", "proxyAddr", proxyAddr, "ackIds", ackIds)
		}
	}
	return nil
}

// cleanupMessage ...
type cleanupMessage struct {
	subscriberID string
	subscription string
}
