package job

import (
	"context"

	"github.com/razorpay/metro/internal/boot"
	"github.com/razorpay/metro/pkg/logger"

	"github.com/razorpay/metro/pkg/errors"
	"github.com/razorpay/metro/pkg/worker"
	"github.com/rs/xid"
)

var (
	userHandler IUserHandler
)

type IUserHandler interface {
	Activate(ctx context.Context, id string) errors.IError
}

// RegisterExecutionServer stores the transaction server for further use
func RegisterUserHandler(handler IUserHandler) {
	userHandler = handler
}

// Base common configs for job are kept here
type Base struct {
	worker.Job
	RequestID string
}

// NewBase returns the base job struct
// it'll clone the config and use the same to create new struct
func NewBase(ctx context.Context, cfg worker.Job) Base {
	// always creates copy of data value before creating the base job
	// as this will be updated while job processing.
	base := cfg

	return Base{
		Job:       base,
		RequestID: boot.GetRequestID(ctx),
	}
}

// BeforeHandle will add the required data to the context
// before every message starts processing
func (b *Base) BeforeHandle(ctx context.Context) context.Context {
	if b.RequestID == "" {
		b.RequestID = xid.New().String()
	}

	ctx = boot.WithRequestID(ctx, b.RequestID)

	req := map[string]interface{}{
		"reqId": b.RequestID,
	}

	return context.WithValue(ctx, logger.LoggerCtxKey, boot.Logger(ctx).WithFields(req))
}

// OnError in case of error while processing job there method will be called
func (b *Base) OnError(ctx context.Context, err error) {
	boot.Logger(ctx).WithError(err).Error("job processing failed")
}

// OnSuccess if the job has completed without any error then this method will be called
func (b *Base) OnSuccess(ctx context.Context) {
	boot.Logger(ctx).Error("job processed successfully")
}
