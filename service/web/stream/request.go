package stream

import (
	"context"
	"strings"

	"github.com/razorpay/metro/internal/subscriber"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

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

// NewParsedStreamingPullRequest ...
func NewParsedStreamingPullRequest(req *metrov1.StreamingPullRequest) (*ParsedStreamingPullRequest, error) {
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

// NewParsedAcknowledgeRequest ...
func NewParsedAcknowledgeRequest(req *metrov1.AcknowledgeRequest) (*ParsedStreamingPullRequest, error) {
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

// NewParsedModifyAckDeadlineRequest ...
func NewParsedModifyAckDeadlineRequest(req *metrov1.ModifyAckDeadlineRequest) (*ParsedStreamingPullRequest, error) {
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

// requestType specifies the type of proxy request, ack or modack
type requestType int

const (
	ack requestType = iota
	modAck
)

type proxyRequest struct {
	ctx         context.Context
	addr        string
	ackMsgs     []*subscriber.AckMessage
	parsedReq   *ParsedStreamingPullRequest
	requestType requestType
}

func newProxyRequest(ctx context.Context, addr string, ackMsgs []*subscriber.AckMessage, parsedReq *ParsedStreamingPullRequest, requestType requestType) *proxyRequest {
	return &proxyRequest{
		ctx:         ctx,
		addr:        addr,
		ackMsgs:     ackMsgs,
		parsedReq:   parsedReq,
		requestType: requestType,
	}
}

func (pr *proxyRequest) isAckRequestType() bool {
	return pr.requestType == ack
}
