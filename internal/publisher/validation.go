package publisher

import (
	"context"

	"github.com/razorpay/metro/internal/merror"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
)

const (
	maxOrderingKeyLength int = 1024
)

var (
	orderingKeyTooLongError   = merror.Newf(merror.InvalidArgument, "Ordering key length exceeds max allowed length of %v", maxOrderingKeyLength)
	multipleOrderingKeysError = merror.New(merror.InvalidArgument, "In a single publish request, all messages must have same ordering key")
	messagesEmptyError        = merror.New(merror.InvalidArgument, "Must have atleast 1 message to publish")
)

// ValidatePublishRequest validates the publish request
func ValidatePublishRequest(ctx context.Context, req *metrov1.PublishRequest) (err error) {
	if err = validateMessages(ctx, req); err != nil {
		return err
	}
	if err = validateOrderingKey(ctx, req); err != nil {
		return err
	}
	return nil
}

func validateMessages(ctx context.Context, req *metrov1.PublishRequest) error {
	if len(req.Messages) == 0 {
		return messagesEmptyError
	}
	return nil
}

func validateOrderingKey(ctx context.Context, req *metrov1.PublishRequest) error {
	orderingKey := req.Messages[0].OrderingKey
	for _, msg := range req.Messages {
		if msg.OrderingKey != orderingKey {
			return multipleOrderingKeysError
		}
	}

	if len(orderingKey) > maxOrderingKeyLength {
		return orderingKeyTooLongError
	}
	return nil
}
