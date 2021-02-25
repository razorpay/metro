package publisher

import (
	"context"

	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

// IPublisher is interface over high level publisher
type IPublisher interface {
	// the grpc proto is used here as well, to optimise for serialization
	// a little unclean but optimal
	Publish(ctx context.Context, req *metrov1.PublishRequest) ([]string, error)
}
