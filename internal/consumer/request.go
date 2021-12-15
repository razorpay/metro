package consumer

import (
	"github.com/razorpay/metro/internal/subscriber"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// ParsedFetchRequest ...
type ParsedFetchRequest struct {
	Subscription string
	MessageCount int
	Partition    int
}

// ParsedAcknowledgeRequest ...
type ParsedAcknowledgeRequest struct {
	Subscription string
	AckIDs       []string
	AckMessages  []*subscriber.AckMessage
}

//ParsedModifyAckDeadlineRequest ...
type ParsedModifyAckDeadlineRequest struct {
	Subscription                 string
	AckIDs                       []string
	AckMessages                  []*subscriber.AckMessage
	ModifyDeadlineMsgIdsWithSecs map[string]int32
}

// NewParsedFetchRequest ...
func NewParsedFetchRequest(req *metrov1.FetchRequest) (*ParsedFetchRequest, error) {
	parsedReq := &ParsedFetchRequest{}
	parsedReq.Subscription = req.Subscription
	parsedReq.MessageCount = int(req.MaxMessages)
	parsedReq.Partition = int(req.Partition)

	return parsedReq, nil
}

// NewParsedAcknowledgeRequest ...
func NewParsedAcknowledgeRequest(req *metrov1.AcknowledgeRequest) (*ParsedAcknowledgeRequest, error) {
	parsedReq := &ParsedAcknowledgeRequest{}

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
func NewParsedModifyAckDeadlineRequest(req *metrov1.ModifyAckDeadlineRequest) (*ParsedModifyAckDeadlineRequest, error) {
	parsedReq := &ParsedModifyAckDeadlineRequest{}

	// TODO : add validations and throw error
	parsedReq.Subscription = req.Subscription
	if req.AckIds != nil && len(req.AckIds) > 0 {
		ackMessages := make([]*subscriber.AckMessage, 0)
		modifyDeadlineMsgIdsWithSecs := make(map[string]int32)
		parsedReq.AckIDs = req.AckIds
		for _, ackID := range req.AckIds {
			ackMessage, err := subscriber.ParseAckID(ackID)
			if err != nil {
				return nil, err
			}
			ackMessage.Deadline = req.AckDeadlineSeconds
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
	addr           string
	grpcServerAddr string
	ackMsgs        []*subscriber.AckMessage
	parsedReq      *metrov1.PullRequest
	requestType    requestType
}
