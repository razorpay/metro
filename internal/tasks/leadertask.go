package tasks

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/razorpay/metro/internal/common"
	"github.com/razorpay/metro/internal/node"
	"github.com/razorpay/metro/pkg/leaderelection"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
)

// LeaderTask runs the leader election process and runs the task associated
type LeaderTask struct {
	id       string
	name     string
	ttl      time.Duration
	registry registry.IRegistry
	nodeCore node.ICore
	task     ITask
}

// NewLeaderTask creates LeaderTask instance
func NewLeaderTask(
	id string,
	registry registry.IRegistry,
	nodeCore node.ICore,
	task ITask,
	options ...Option,
) (ITask, error) {
	options = append(defaultOptions(), options...)

	leaderTask := &LeaderTask{
		id:       id,
		registry: registry,
		nodeCore: nodeCore,
		task:     task,
	}

	for _, option := range options {
		option(leaderTask)
	}

	return leaderTask, nil
}

func defaultOptions() []Option {
	return []Option{
		WithTTL(30 * time.Second),
		WithName("metro/metro-worker"),
	}
}

// WithTTL defines the TTL for the registry session
func WithTTL(ttl time.Duration) Option {
	return func(task ITask) {
		leaderTask := task.(*LeaderTask)
		leaderTask.ttl = ttl
	}
}

// WithName defines the Name for the registry session creation
func WithName(name string) Option {
	return func(task ITask) {
		leaderTask := task.(*LeaderTask)
		leaderTask.name = name
	}
}

// Run the task
func (sm *LeaderTask) Run(ctx context.Context) error {
	logger.Ctx(ctx).Infow("starting worker leader task")

	// Create a registry session
	sessionID, err := sm.registry.Register(ctx, sm.name, sm.ttl)
	if err != nil {
		return err
	}

	// Run the tasks
	taskGroup, gctx := errgroup.WithContext(ctx)

	// Renew session periodically
	taskGroup.Go(func() error {
		return sm.registry.RenewPeriodic(gctx, sessionID, sm.ttl, gctx.Done())
	})

	// Acquire the node path using sessionID
	taskGroup.Go(func() error {
		return sm.acquireNode(gctx, sessionID)
	})

	// Run LeaderElection using sessionID
	taskGroup.Go(func() error {
		return sm.runLeaderElection(gctx, sessionID)
	})

	err = taskGroup.Wait()
	logger.Ctx(ctx).Infow("exiting from worker leader task", "error", err)
	return err
}

func (sm *LeaderTask) acquireNode(ctx context.Context, sessionID string) error {
	err := sm.nodeCore.AcquireNode(ctx, &node.Model{
		ID: sm.id,
	}, sessionID)

	return err
}

func (sm *LeaderTask) runLeaderElection(ctx context.Context, sessionID string) error {
	// Init Leader Election
	candidate, err := leaderelection.New(
		sm.id,
		sessionID,
		leaderelection.Config{
			LockPath: common.GetBasePrefix() + "leader/election",
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) error {
					return sm.lead(ctx)
				},
				OnStoppedLeading: func(ctx context.Context) {
					sm.stepDown(ctx)
				},
			},
		}, sm.registry)

	if err != nil {
		return err
	}

	// Run Leader Election
	return candidate.Run(ctx)
}

func (sm *LeaderTask) lead(ctx context.Context) error {
	logger.Ctx(ctx).Infof("Node %s elected as new leader", sm.id)
	return sm.task.Run(ctx)
}

func (sm *LeaderTask) stepDown(ctx context.Context) {
	logger.Ctx(ctx).Infof("Node %s stepping down from leader", sm.id)
}
