package subscriber

import (
	"context"

	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// ISubscriber is interface over high level subscriber
type ISubscriber interface {
	// the grpc proto is used here as well, to optimise for serialization
	// and deserialisation, a little unclean but optimal
	// TODO: figure a better way out
	Pull(ctx context.Context, req *PullRequest, timeoutInSec int, id string) (metrov1.PullResponse, error)
	Acknowledge(ctx context.Context, req *AcknowledgeRequest) error
	ModifyAckDeadline(ctx context.Context, req *ModifyAckDeadlineRequest) error
}
