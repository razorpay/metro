package stream

import (
	"strings"

	"github.com/razorpay/metro/internal/subscriber"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// ParsedStreamingPullRequest ...
type ParsedStreamingPullRequest struct {
	ClientID                     string
	Subscription                 string
	AckIDs                       []string
	ModAckIDs                    []string
	AckMessages                  []*subscriber.AckMessage
	ModAckMessages               []*subscriber.AckMessage
	ModifyDeadlineMsgIdsWithSecs map[string]int32
}

// ParsedPullRequest ...
type ParsedPullRequest struct {
	Subscription string
	MaxMessages  int
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
	return r.ModAckMessages != nil && len(r.ModAckMessages) > 0
}

// NewParsedStreamingPullRequest ...
func NewParsedStreamingPullRequest(req *metrov1.StreamingPullRequest) (*ParsedStreamingPullRequest, error) {
	parsedReq := &ParsedStreamingPullRequest{}

	// TODO : add validations and throw error
	parsedReq.Subscription = req.Subscription
	if req.AckIds != nil && len(req.AckIds) > 0 {
		ackMessages := make([]*subscriber.AckMessage, 0)
		parsedReq.AckIDs = req.AckIds
		for _, ackID := range req.AckIds {
			ackMessage, err := subscriber.ParseAckID(ackID)
			if err != nil {
				return nil, err
			}
			ackMessages = append(ackMessages, ackMessage)
		}
		parsedReq.AckIDs = req.AckIds
		parsedReq.AckMessages = ackMessages
	}
	if req.ModifyDeadlineAckIds != nil && len(req.ModifyDeadlineAckIds) > 0 {
		ackMessages := make([]*subscriber.AckMessage, 0)
		deadlineMap := map[string]int32{}
		for index, ackID := range req.ModifyDeadlineAckIds {
			ackMessage, err := subscriber.ParseAckID(ackID)
			if err != nil {
				return nil, err
			}
			ackMessages = append(ackMessages, ackMessage)
			deadlineMap[ackMessage.MessageID] = req.ModifyDeadlineSeconds[index]
		}
		parsedReq.ModifyDeadlineMsgIdsWithSecs = deadlineMap
		parsedReq.ModAckIDs = req.ModifyDeadlineAckIds
		parsedReq.ModAckMessages = ackMessages
	}

	return parsedReq, nil
}

// NewParsedPullRequest ...
func NewParsedPullRequest(req *metrov1.PullRequest) (*ParsedPullRequest, error) {
	parsedReq := &ParsedPullRequest{
		Subscription: req.Subscription,
		MaxMessages:  int(req.MaxMessages),
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
			ackMessage, err := subscriber.ParseAckID(ackID)
			if err != nil {
				return nil, err
			}
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
		parsedReq.ModAckIDs = req.AckIds
		for _, ackID := range req.AckIds {
			ackMessage, err := subscriber.ParseAckID(ackID)
			if err != nil {
				return nil, err
			}
			ackMessages = append(ackMessages, ackMessage)
			modifyDeadlineMsgIdsWithSecs[ackMessage.MessageID] = req.AckDeadlineSeconds
		}
		parsedReq.ModAckIDs = req.AckIds
		parsedReq.ModAckMessages = ackMessages
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
	addr           string
	grpcServerAddr string
	ackMsgs        []*subscriber.AckMessage
	parsedReq      *ParsedStreamingPullRequest
	requestType    requestType
}

func newAckProxyRequest(addr string, ackMsgs []*subscriber.AckMessage, parsedReq *ParsedStreamingPullRequest, grpcServerAddr string) *proxyRequest {
	return newProxyRequest(addr, ackMsgs, parsedReq, grpcServerAddr, ack)
}

func newModAckProxyRequest(addr string, ackMsgs []*subscriber.AckMessage, parsedReq *ParsedStreamingPullRequest, grpcServerAddr string) *proxyRequest {
	return newProxyRequest(addr, ackMsgs, parsedReq, grpcServerAddr, modAck)
}

func newProxyRequest(addr string, ackMsgs []*subscriber.AckMessage, parsedReq *ParsedStreamingPullRequest, grpcServerAddr string, requestType requestType) *proxyRequest {
	return &proxyRequest{
		addr:           addr,
		grpcServerAddr: grpcServerAddr,
		ackMsgs:        ackMsgs,
		parsedReq:      parsedReq,
		requestType:    requestType,
	}
}

func (pr *proxyRequest) isAckRequestType() bool {
	return pr.requestType == ack
}
