package registry

import (
	"context"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"github.com/razorpay/metro/pkg/logger"
)

// ConsulWatcher implements consul watch handler and stores the users handler function
type ConsulWatcher struct {
	ctx    context.Context
	Config *WatchConfig
	plan   *watch.Plan
	client *api.Client
}

// NewConsulWatcher is used to create a new struct of type WatchHandler
func NewConsulWatcher(ctx context.Context, watchConfig *WatchConfig, plan *watch.Plan, client *api.Client) IWatcher {
	return &ConsulWatcher{
		ctx:    ctx,
		Config: watchConfig,
		plan:   plan,
		client: client,
	}
}

// Handler implements the consul watch handler method and invokes the requester handler
func (cwh *ConsulWatcher) handler(index uint64, result interface{}) {
	pairs, ok := result.(api.KVPairs)
	if !ok {
		logger.Ctx(cwh.ctx).Errorw("failed to parse consul watch results", "data", result)
		return
	}

	results := []Pair{}

	for i := range pairs {
		results = append(results, Pair{
			Key:   pairs[i].Key,
			Value: pairs[i].Value,
		})
	}

	cwh.Config.Handler(results)
}

// StartWatch will start the watch
func (cwh *ConsulWatcher) StartWatch() error {
	cwh.plan.Handler = cwh.handler

	return cwh.plan.RunWithClientAndLogger(cwh.client, nil)
}

// StopWatch will cleanup the active consul watches
func (cwh *ConsulWatcher) StopWatch() {
	cwh.plan.Stop()
}
