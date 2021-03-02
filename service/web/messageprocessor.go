package web

import (
	"context"
	"strings"

	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// IStreamManger ...
type IStreamManger interface {
	CreateNewStream(server metrov1.Subscriber_StreamingPullServer, req *ParsedStreamingPullRequest) error
	Acknowledge(ctx context.Context, req *ParsedStreamingPullRequest) error
	ModifyAcknowledgement(ctx context.Context, req *ParsedStreamingPullRequest) error
}

// StreamManger ...
type StreamManger struct {
	// manage active streams by stream_id / subscriber_id
	// in case of graceful shutdown nack all messages held in these streams
	pullStreams map[string]*pullStream

	subscriptionCore subscription.ICore

	bs brokerstore.IBrokerStore

	// TODO: temp code. will remove. maintain a distributed counter for active streams per subscription
	activeStreamCount map[string]uint32
}

// NewStreamManager ...
func NewStreamManager(subscriptionCore subscription.ICore) IStreamManger {
	return &StreamManger{
		pullStreams:       make(map[string]*pullStream),
		subscriptionCore:  subscriptionCore,
		activeStreamCount: make(map[string]uint32),
	}
}

// CreateNewStream ...
func (s *StreamManger) CreateNewStream(server metrov1.Subscriber_StreamingPullServer, req *ParsedStreamingPullRequest) error {
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
	)
	if err != nil {
		return err
	}

	s.pullStreams[pullStream.subscriberID] = pullStream

	return nil

}

// Acknowledge ...
func (s *StreamManger) Acknowledge(ctx context.Context, req *ParsedStreamingPullRequest) error {
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
func (s *StreamManger) ModifyAcknowledgement(ctx context.Context, req *ParsedStreamingPullRequest) error {
	for _, ackMsg := range req.AckMessages {
		if ackMsg.MatchesOriginatingMessageServer() {
			// find active stream
			if pullStream, ok := s.pullStreams[ackMsg.SubscriberID]; ok {
				pullStream.modifyAckDeadline(ctx, ackMsg)
			}
		} else {
			// proxy request to the correct server
		}

	}

	return nil
}

// ParsedStreamingPullRequest ...
type ParsedStreamingPullRequest struct {
	ClientID                     string
	Subscription                 string
	AckIDs                       []string
	AckMessages                  []*subscriber.AckMessage
	ModifyDeadlineMsgIdsWithSecs map[string]int32
}

// HasSubscription ...
func (r *ParsedStreamingPullRequest) HasSubscription() bool {
	return len(strings.Trim(r.Subscription, " ")) > 0
}

// HasAcknowledgement ...
func (r *ParsedStreamingPullRequest) HasAcknowledgement() bool {
	return r.AckMessages != nil && len(r.AckMessages) > 0
}

// HasModifyAcknowledgement ...
func (r *ParsedStreamingPullRequest) HasModifyAcknowledgement() bool {
	return r.ModifyDeadlineMsgIdsWithSecs != nil && len(r.ModifyDeadlineMsgIdsWithSecs) > 0
}

func newParsedStreamingPullRequest(req *metrov1.StreamingPullRequest) (*ParsedStreamingPullRequest, error) {
	parsedReq := &ParsedStreamingPullRequest{}

	// TODO : add validations and throw error
	parsedReq.Subscription = req.Subscription
	if req.AckIds != nil && len(req.AckIds) > 0 {
		ackMessages := make([]*subscriber.AckMessage, 0)
		modifyDeadlineMsgIdsWithSecs := make(map[string]int32)
		parsedReq.AckIDs = req.AckIds
		for index, ackID := range req.AckIds {
			ackMessage := subscriber.ParseAckID(ackID)
			ackMessages = append(ackMessages, ackMessage)
			modifyDeadlineMsgIdsWithSecs[ackMessage.MessageID] = req.ModifyDeadlineSeconds[index]
		}
		parsedReq.AckIDs = req.AckIds
		parsedReq.AckMessages = ackMessages
		parsedReq.ModifyDeadlineMsgIdsWithSecs = modifyDeadlineMsgIdsWithSecs
	}

	return parsedReq, nil
}

func newParsedAcknowledgeRequest(req *metrov1.AcknowledgeRequest) (*ParsedStreamingPullRequest, error) {
	parsedReq := &ParsedStreamingPullRequest{}

	// TODO : add validations and throw error
	parsedReq.Subscription = req.Subscription
	if req.AckIds != nil && len(req.AckIds) > 0 {
		ackMessages := make([]*subscriber.AckMessage, 0)
		parsedReq.AckIDs = req.AckIds
		for _, ackID := range req.AckIds {
			ackMessage := subscriber.ParseAckID(ackID)
			ackMessages = append(ackMessages, ackMessage)
		}
		parsedReq.AckIDs = req.AckIds
		parsedReq.AckMessages = ackMessages
	}

	return parsedReq, nil
}

func newParsedModifyAckDeadlineRequest(req *metrov1.ModifyAckDeadlineRequest) (*ParsedStreamingPullRequest, error) {
	parsedReq := &ParsedStreamingPullRequest{}

	// TODO : add validations and throw error
	parsedReq.Subscription = req.Subscription
	if req.AckIds != nil && len(req.AckIds) > 0 {
		ackMessages := make([]*subscriber.AckMessage, 0)
		modifyDeadlineMsgIdsWithSecs := make(map[string]int32)
		parsedReq.AckIDs = req.AckIds
		for _, ackID := range req.AckIds {
			ackMessage := subscriber.ParseAckID(ackID)
			ackMessages = append(ackMessages, ackMessage)
			modifyDeadlineMsgIdsWithSecs[ackMessage.MessageID] = req.AckDeadlineSeconds
		}
		parsedReq.AckIDs = req.AckIds
		parsedReq.AckMessages = ackMessages
		parsedReq.ModifyDeadlineMsgIdsWithSecs = modifyDeadlineMsgIdsWithSecs
	}

	return parsedReq, nil
}
