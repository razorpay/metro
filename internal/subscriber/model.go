package subscriber

import (
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// PullRequest ...
type PullRequest struct {
	MaxNumOfMessages int32
}

// AcknowledgeRequest ...
type AcknowledgeRequest struct {
	Subscription string
	AckIDs       []string
}

// ModifyAckDeadlineRequest ...
type ModifyAckDeadlineRequest struct {
	Subscription string
	// The initial ACK deadline given to messages is 10s
	// https://godoc.org/cloud.google.com/go/pubsub#hdr-Deadlines
	ModifyDeadlineSeconds []int32
	ModifyDeadlineAckIDs  []string
}

// FromProto returns different structs for pull, ack and modack
func FromProto(req *metrov1.StreamingPullRequest) (*AcknowledgeRequest, *ModifyAckDeadlineRequest) {
	ar := &AcknowledgeRequest{
		req.Subscription,
		req.AckIds,
	}
	mr := &ModifyAckDeadlineRequest{
		req.Subscription,
		req.ModifyDeadlineSeconds,
		req.ModifyDeadlineAckIds,
	}
	return ar, mr
}
