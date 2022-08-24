package stream

import (
	"context"

	"github.com/razorpay/metro/internal/subscriber"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/pkg/httpclient"
	"github.com/razorpay/metro/pkg/logger"
)

// PushStreamManager manages push stream
type PushStreamManager struct {
	ctx        context.Context
	cancelFunc func()
	doneCh     chan struct{}
	config     *httpclient.Config
	ps         *PushStream
}

// NewPushStreamManager return a push stream manager obj which is used to manage push stream
func NewPushStreamManager(ctx context.Context, nodeID string, subName string, subscriptionCore subscription.ICore, subscriberCore subscriber.ICore, config *httpclient.Config) (*PushStreamManager, error) {
	ps, err := newPushStream(ctx, nodeID, subName, subscriptionCore, subscriberCore, config)
	if err != nil {
		return nil, err
	}
	ctx, cancelFunc := context.WithCancel(ctx)
	return &PushStreamManager{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		ps:         ps,
		doneCh:     make(chan struct{}),
		config:     config,
	}, nil
}

// Run starts the push stream manager that is used to manage underlying stream
func (psm *PushStreamManager) Run() {
	defer close(psm.doneCh)

	logger.Ctx(psm.ctx).Infow("push stream manager: started running stream manager", "subscription", psm.ps.subscription.Name)
	psm.startPushStream()

	go func() {
		for {
			select {
			case <-psm.ctx.Done():
				if err := psm.ps.Stop(); err != nil {
					logger.Ctx(psm.ctx).Infow("push stream manager: error stopping stream", "subscription", psm.ps.subscription.Name, "error", err.Error())
				}
				return
			case err := <-psm.ps.GetErrorChannel():
				logger.Ctx(psm.ctx).Infow("push stream manager: restarting stream handler", "subscription", psm.ps.subscription.Name, "error", err.Error())
				psm.restartPushStream()
			}
		}
	}()
}

// Stop stops the stream manager along with the underlying stream
func (psm *PushStreamManager) Stop() {
	logger.Ctx(psm.ctx).Infow("push stream manager: stop invoked", "subscription", psm.ps.subscription.Name)
	psm.cancelFunc()
	<-psm.doneCh
}

func newPushStream(ctx context.Context, nodeID string, subName string, subscriptionCore subscription.ICore, subscriberCore subscriber.ICore, config *httpclient.Config) (*PushStream, error) {
	ps, err := NewPushStream(ctx, nodeID, subName, subscriptionCore, subscriberCore, config)
	if err != nil {
		logger.Ctx(ctx).Errorw("push stream manager: Failed to setup push stream for subscription", "logFields", map[string]interface{}{
			"subscription": subName,
			"nodeID":       nodeID,
		})
		return nil, err
	}
	return ps, nil
}

func (psm *PushStreamManager) startPushStream() {
	// run the stream in a separate go routine, this go routine is not part of the worker error group
	// as the worker should continue to run if a single subscription stream exists with error
	go func(ctx context.Context) {
		err := psm.ps.Start()
		if err != nil {
			logger.Ctx(ctx).Errorw(
				"push stream manager: stream exited",
				"subscription", psm.ps.subscription.Name,
				"error", err.Error(),
			)
		}
	}(psm.ctx)
}

func (psm *PushStreamManager) restartPushStream() {
	psm.ps.Stop()
	psm.ps, _ = newPushStream(psm.ctx, psm.ps.nodeID, psm.ps.subscription.Name, psm.ps.subscriptionCore, psm.ps.subscriberCore, psm.config)
	psm.startPushStream()
	workerComponentRestartCount.WithLabelValues(env, "stream", psm.ps.subscription.Topic, psm.ps.subscription.Name).Inc()
}
