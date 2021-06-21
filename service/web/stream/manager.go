package stream

import (
	"context"
	"fmt"
	"strings"
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
	grpcServerAddr    string
	ctx               context.Context
}

// NewStreamManager ...
func NewStreamManager(ctx context.Context, subscriptionCore subscription.ICore, bs brokerstore.IBrokerStore, grpcServerAddr string) IManager {
	mgr := &Manager{
		pullStreams:       make(map[string]IStream),
		subscriptionCore:  subscriptionCore,
		activeStreamCount: make(map[string]uint32),
		bs:                bs,
		cleanupCh:         make(chan cleanupMessage),
		mutex:             &sync.Mutex{},
		grpcServerAddr:    grpcServerAddr,
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

	// get subscription Model details
	subModel, err := s.subscriptionCore.Get(server.Context(), req.Subscription)
	if err != nil {
		logger.Ctx(server.Context()).Errorf("error fetching subscription: %s", err.Error())
		return err
	}

	pullStream, err := newPullStream(server,
		req.ClientID,
		subModel,
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
			newAckProxyRequest(proxyAddr, ackMsgs, parsedReq, s.grpcServerAddr).do(ctx)
		}
	}
	return nil
}

// ModifyAcknowledgement ...
func (s *Manager) ModifyAcknowledgement(ctx context.Context, parsedReq *ParsedStreamingPullRequest) error {
	// holds a map of modAckMsgs to their corresponding originating server addresses
	msgsToBeProxied := make(map[string][]*subscriber.AckMessage, 0)

	for _, ackMsg := range parsedReq.AckMessages {
		// non zero ack deadline is not supported, hence continue
		if parsedReq.ModifyDeadlineMsgIdsWithSecs[ackMsg.MessageID] != 0 {
			continue
		}
		if ackMsg.MatchesOriginatingMessageServer() {
			// find active stream
			if pullStream, ok := s.pullStreams[ackMsg.SubscriberID]; ok {
				pullStream.modifyAckDeadline(ctx, subscriber.NewModAckMessage(ackMsg, parsedReq.ModifyDeadlineMsgIdsWithSecs[ackMsg.MessageID]))
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
			newModAckProxyRequest(proxyAddr, ackMsgs, parsedReq, s.grpcServerAddr).do(ctx)
		}
	}
	return nil
}

// proxy request to the specified server
func (pr *proxyRequest) do(ctx context.Context) {

	// from config grpcServerAddr would be read as => 0.0.0.0:8081
	// we extract the port(8081) out of the host+grpc_port string and append it to the ack_msg_addr
	// all proxy grpc calls would be made on this ack_msg_addr:grpc_port destination.
	grpcPort := strings.Split(pr.grpcServerAddr, ":")[1]
	destinationHostWithPort := fmt.Sprintf("%v:%v", pr.addr, grpcPort)

	conn, err := grpc.Dial(destinationHostWithPort, []grpc.DialOption{grpc.WithInsecure()}...)
	if err != nil {
		logger.Ctx(ctx).Errorw("manager: failed to connect to proxy host", "proxyAddr", destinationHostWithPort, "error", err.Error())
		return
	}

	ackIds := collectAckIds(pr.ackMsgs)
	client := metrov1.NewSubscriberClient(conn)

	var proxyError error

	logger.Ctx(ctx).Infow("manager: proxy request", "proxyAddr", destinationHostWithPort, "ackIds", ackIds, "requestType", pr.requestType)

	if pr.isAckRequestType() {
		proxyAckRequest := &metrov1.AcknowledgeRequest{
			Subscription: pr.parsedReq.Subscription,
			AckIds:       ackIds,
		}

		_, proxyError = client.Acknowledge(ctx, proxyAckRequest)
	} else {
		proxyModAckRequest := &metrov1.ModifyAckDeadlineRequest{
			Subscription: pr.parsedReq.Subscription,
			AckIds:       ackIds,
			// pick up ack deadline time for any one of the message and set in the request
			// this is usually the same for all given ack_ids
			AckDeadlineSeconds: pr.parsedReq.ModifyDeadlineMsgIdsWithSecs[pr.ackMsgs[0].MessageID],
		}

		_, proxyError = client.ModifyAckDeadline(ctx, proxyModAckRequest)
	}

	if proxyError != nil {
		logger.Ctx(ctx).Errorw("manager: proxy request failed", "proxyAddr", destinationHostWithPort, "requestType", pr.requestType, "error", proxyError.Error())
		// on error, try to proxy remaining requests
		return
	}

	logger.Ctx(ctx).Infow("manager: proxy request succeeded", "proxyAddr", destinationHostWithPort, "ackIds", ackIds, "requestType", pr.requestType)
}

func collectAckIds(msgs []*subscriber.AckMessage) []string {
	ackIds := make([]string, 0)

	for _, msg := range msgs {
		ackIds = append(ackIds, msg.AckID)
	}
	return ackIds
}

// cleanupMessage ...
type cleanupMessage struct {
	subscriberID string
	subscription string
}
