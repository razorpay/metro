package job

import (
	"context"
	"log"
	"time"

	"github.com/razorpay/metro/internal/boot"
	"github.com/razorpay/metro/pkg/worker"
)

var userApproveConfig = worker.Job{
	Name:       "user_approve",
	QueueName:  boot.Config.Job.UserApprove,
	MaxRetries: 5,
	Timeout:    60 * time.Second,
}

type UserApprove struct {
	Base
	UserID string
}

func init() {
	sc := &UserApprove{
		Base: NewBase(context.Background(), userApproveConfig),
	}

	if err := boot.Worker.
		Register(sc); err != nil {
		log.Fatal(err)
	}
}

// NewUserApproveJob fetched the user id and approve the activation status
func NewUserApproveJob(ctx context.Context, userID string) *UserApprove {
	return &UserApprove{
		Base:   NewBase(ctx, userApproveConfig),
		UserID: userID,
	}
}

// Handle handler for the job
func (job *UserApprove) Handle(ctx context.Context) error {
	return userHandler.Activate(ctx, job.UserID)
}
