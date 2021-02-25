package subscriber

import (
	"fmt"

	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// PullRequest ...
type PullRequest struct {
	MaxNumOfMessages int32
}

// AcknowledgeRequest ...
type AcknowledgeRequest struct {
	AckIDs []string
}

// IsEmpty returns true if its an empty request
func (ar *AcknowledgeRequest) IsEmpty() bool {
	if len(ar.AckIDs) == 0 {
		return true
	}
	return false
}

// ModifyAckDeadlineRequest ...
type ModifyAckDeadlineRequest struct {
	// The initial ACK deadline given to messages is 10s
	// https://godoc.org/cloud.google.com/go/pubsub#hdr-Deadlines
	ModifyDeadlineSeconds []int32
	ModifyDeadlineAckIDs  []string
}

// IsEmpty returns true if its an empty request
func (mr *ModifyAckDeadlineRequest) IsEmpty() bool {
	if len(mr.ModifyDeadlineAckIDs) == 0 {
		return true
	}
	return false
}

// FromProto returns different structs for pull, ack and modack
func FromProto(req *metrov1.StreamingPullRequest) (*AcknowledgeRequest, *ModifyAckDeadlineRequest, error) {
	if len(req.ModifyDeadlineAckIds) != len(req.ModifyDeadlineSeconds) {
		return nil, nil, fmt.Errorf("length of modify_deadline_ack_ids and modify_deadline_seconds not same")
	}
	ar := &AcknowledgeRequest{
		req.AckIds,
	}
	mr := &ModifyAckDeadlineRequest{
		req.ModifyDeadlineSeconds,
		req.ModifyDeadlineAckIds,
	}
	return ar, mr, nil
}
