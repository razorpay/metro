package worker_test

import (
	"context"
	"errors"
	"time"

	"github.com/razorpay/metro/pkg/worker"
)

type Job struct {
	worker.Job
}

type TestJob struct {
	worker.Job
}

func (j *TestJob) Handle(ctx context.Context) error {
	return nil
}

type DelayedJob struct {
	worker.Job
}

func (j *DelayedJob) Handle(ctx context.Context) error {
	time.Sleep(3 * time.Second)
	return nil
}

type FailureJob struct {
	worker.Job
}

func (j *FailureJob) Handle(ctx context.Context) error {
	return errors.New("failed to complete job")
}

type UnregisteredJob struct {
	worker.Job
}

type TimeoutJob struct {
	worker.Job
}

func (j *TimeoutJob) Handle(ctx context.Context) error {
	time.Sleep(2 * time.Second)
	return nil
}

type RawJob struct {
	content string
	worker.Job
}

func (j *RawJob) Construct(msg string) (worker.IJob, error) {
	return &RawJob{
		content: msg,
		Job: worker.Job{
			Name:      "raw_job",
			QueueName: "temp_queue",
		},
	}, nil
}

func (j *RawJob) Handle(ctx context.Context) error {
	return nil
}
