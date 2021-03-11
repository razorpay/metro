package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/razorpay/metro/internal/subscriber"

	"github.com/razorpay/metro/pkg/scheduler"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/node"
	"github.com/razorpay/metro/internal/nodebinding"
	"github.com/razorpay/metro/internal/project"
	internalserver "github.com/razorpay/metro/internal/server"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/leaderelection"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Service for worker
type Service struct {
	ctx              context.Context
	grpcServer       *grpc.Server
	httpServer       *http.Server
	health           *health.Core
	workerConfig     *Config
	registryConfig   *registry.Config
	registry         registry.IRegistry
	candidate        *leaderelection.Candidate
	node             *node.Model
	doneCh           chan struct{}
	stopCh           chan struct{}
	workgrp          *errgroup.Group
	leadgrp          *errgroup.Group
	brokerStore      brokerstore.IBrokerStore
	projectCore      project.ICore
	nodeCore         node.ICore
	topicCore        topic.ICore
	subscriptionCore subscription.ICore
	nodeBindingCore  nodebinding.ICore
	nodeCache        []*node.Model
	subCache         []*subscription.Model
	nodebindingCache []*nodebinding.Model
	pushHandlers     map[string]*pushStream
	scheduler        *scheduler.Scheduler
	subscriber       subscriber.ICore
}

// NewService creates an instance of new worker
func NewService(ctx context.Context, workerConfig *Config, registryConfig *registry.Config) *Service {
	return &Service{
		ctx:            ctx,
		workerConfig:   workerConfig,
		registryConfig: registryConfig,
		node: &node.Model{
			ID: uuid.New().String(),
		},
		doneCh:           make(chan struct{}),
		stopCh:           make(chan struct{}),
		nodeCache:        []*node.Model{},
		subCache:         []*subscription.Model{},
		nodebindingCache: []*nodebinding.Model{},
		pushHandlers:     map[string]*pushStream{},
	}
}

// Start implements all the tasks for worker and waits until one of the task fails
func (svc *Service) Start() error {
	// close the done channel when this function returns
	defer close(svc.doneCh)

	var (
		err  error
		gctx context.Context
	)

	svc.workgrp, gctx = errgroup.WithContext(svc.ctx)

	// Define server handlers
	healthCore, err := health.NewCore(nil) //TODO: Add checkers
	if err != nil {
		return err
	}

	// Init the Registry
	// TODO: move to component init ?
	svc.registry, err = registry.NewRegistry(svc.ctx, svc.registryConfig)
	if err != nil {
		return err
	}

	svc.brokerStore, err = brokerstore.NewBrokerStore(svc.workerConfig.Broker.Variant, &svc.workerConfig.Broker.BrokerConfig)
	if err != nil {
		return err
	}

	svc.projectCore = project.NewCore(project.NewRepo(svc.registry))

	svc.topicCore = topic.NewCore(topic.NewRepo(svc.registry), svc.projectCore, svc.brokerStore)

	svc.subscriptionCore = subscription.NewCore(subscription.NewRepo(svc.registry), svc.projectCore, svc.topicCore)

	svc.nodeCore = node.NewCore(node.NewRepo(svc.registry))

	svc.nodeBindingCore = nodebinding.NewCore(nodebinding.NewRepo(svc.registry))

	svc.scheduler, err = scheduler.New(scheduler.LoadBalance)

	svc.subscriber = subscriber.NewCore(svc.brokerStore, svc.subscriptionCore)

	if err != nil {
		return err
	}

	// Register Node with registry
	err = svc.nodeCore.CreateNode(svc.ctx, svc.node)
	if err != nil {
		return err
	}

	// Init Leader Election
	svc.candidate, err = leaderelection.New(svc.node,
		leaderelection.Config{
			Name:          "metro/metro-worker",
			LockPath:      "metro/leader/election",
			LeaseDuration: 30 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) error {
					return svc.lead(ctx)
				},
				OnStoppedLeading: func() {
					svc.stepDown()
				},
			},
		}, svc.registry)

	if err != nil {
		return err
	}

	// Run leader election
	svc.workgrp.Go(func() error {
		logger.Ctx(gctx).Info("starting leader election")
		return svc.candidate.Run(gctx)
	})

	// Build NodeBinding Cache
	svc.nodebindingCache, err = svc.nodeBindingCore.List(svc.ctx, svc.node.ID)
	if err != nil {
		return nil
	}

	// Watch for the subscription assignment changes
	var watcher registry.IWatcher
	svc.workgrp.Go(func() error {
		wh := registry.WatchConfig{
			WatchType: "keyprefix",
			WatchPath: fmt.Sprintf("metro/nodebinding/%s/", svc.node.ID),
			Handler: func(ctx context.Context, pairs []registry.Pair) {
				logger.Ctx(ctx).Infow("node subscriptions", "pairs", pairs)
				err = svc.handleNodeBindingUpdates(pairs)
				if err != nil {
					logger.Ctx(ctx).Errorw("error processing nodebinding updates", "error", err)
				}
			},
		}

		watcher, err = svc.registry.Watch(gctx, &wh)
		if err != nil {
			return err
		}

		return watcher.StartWatch()
	})

	svc.workgrp.Go(func() error {
		var err error
		select {
		case <-gctx.Done():
			err = gctx.Err()
		case <-svc.stopCh:
			err = fmt.Errorf("signal received, stopping worker")
		}

		if watcher != nil {
			watcher.StopWatch()
		}

		return err
	})

	grpcServer, err := internalserver.StartGRPCServer(
		svc.workgrp,
		svc.workerConfig.Interfaces.API.GrpcServerAddress,
		func(server *grpc.Server) error {
			metrov1.RegisterHealthCheckAPIServer(server, health.NewServer(healthCore))
			return nil
		},
		getInterceptors()...,
	)
	if err != nil {
		return err
	}

	httpServer, err := internalserver.StartHTTPServer(
		svc.workgrp,
		svc.workerConfig.Interfaces.API.HTTPServerAddress,
		func(mux *runtime.ServeMux) error {
			err := metrov1.RegisterHealthCheckAPIHandlerFromEndpoint(gctx, mux, svc.workerConfig.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
			if err != nil {
				return err
			}

			mux.Handle("GET", runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1}, []string{"v1", "metrics"}, "")), func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
				promhttp.Handler().ServeHTTP(w, r)
			})
			return nil
		})

	if err != nil {
		return err
	}

	svc.grpcServer = grpcServer
	svc.httpServer = httpServer
	svc.health = healthCore

	err = svc.workgrp.Wait()
	if err != nil {
		logger.Ctx(gctx).Info("worker service error: %s", err.Error())
	}
	return err
}

// Stop the service
func (svc *Service) Stop() error {
	// signal to stop all go routines
	close(svc.stopCh)

	// signal the grpc server go routine
	svc.grpcServer.GracefulStop()

	// signal http server go routine
	err := svc.httpServer.Shutdown(svc.ctx)

	if err != nil {
		return err
	}

	// wait until all goroutines are done
	<-svc.doneCh

	return nil
}

func (svc *Service) lead(ctx context.Context) error {
	logger.Ctx(ctx).Infof("Node %s elected as new leader", svc.node.ID)

	var (
		nodeWatcher, subWatcher registry.IWatcher
		err                     error
		gctx                    context.Context
	)

	// create nodeCache
	logger.Ctx(ctx).Infof("getting active nodes for cache")
	svc.nodeCache, err = svc.nodeCore.List(svc.ctx, "")
	if err != nil {
		return err
	}

	// create subscription cache
	logger.Ctx(ctx).Infof("getting active subscriptions for cache")
	svc.subCache, err = svc.subscriptionCore.List(svc.ctx, "")
	if err != nil {
		return err
	}

	svc.leadgrp, gctx = errgroup.WithContext(ctx)

	nwh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: "metro/nodes",
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("nodes watch handler data", "pairs", pairs)
			err = svc.handleNodeUpdates(pairs)
			if err != nil {
				logger.Ctx(ctx).Errorw("error processing node updates", "error", err)
			}
		},
	}

	logger.Ctx(ctx).Infof("setting watch on nodes")
	nodeWatcher, err = svc.registry.Watch(gctx, &nwh)
	if err != nil {
		return err
	}

	swh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: "metro/subscriptions",
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("subscriptions watch handler data", "pairs", pairs)
			err = svc.handleSubUpdates(pairs)
			if err != nil {
				logger.Ctx(ctx).Errorw("error processing subscription updates", "error", err)
			}
		},
	}

	logger.Ctx(ctx).Infof("setting watch on subscriptions")
	subWatcher, err = svc.registry.Watch(gctx, &swh)
	if err != nil {
		return err
	}

	// watch for nodes addition/deletion, for any changes a rebalance might be required
	svc.leadgrp.Go(func() error {
		return nodeWatcher.StartWatch()
	})

	// watch the Subscriptions path for new subscriptions and rebalance
	svc.leadgrp.Go(func() error {
		return subWatcher.StartWatch()
	})

	// wait for done channel to be closed and stop watches if received done
	svc.leadgrp.Go(func() error {
		<-gctx.Done()

		if nodeWatcher != nil {
			nodeWatcher.StopWatch()
		}

		if subWatcher != nil {
			subWatcher.StopWatch()
		}

		logger.Ctx(gctx).Info("leader context returned done")
		return gctx.Err()
	})

	return nil
}

func (svc *Service) stepDown() {
	logger.Ctx(svc.ctx).Infof("Node %s stepping down from leader", svc.node.ID)

	// wait for leader go routines to terminate
	err := svc.leadgrp.Wait()

	if err != nil {
		logger.Ctx(svc.ctx).Errorf("Error in leader group go routine termination : %s", err.Error())
	}
}

func (svc *Service) handleNodeBindingUpdates(newBindingPairs []registry.Pair) error {
	oldBindings := svc.nodebindingCache
	var newBindings []*nodebinding.Model

	for _, pair := range newBindingPairs {
		nb := nodebinding.Model{}
		err := json.Unmarshal(pair.Value, &nb)
		if err != nil {
			return err
		}
		newBindings = append(newBindings, &nb)
	}

	for _, old := range oldBindings {
		found := false
		for _, newBinding := range newBindings {
			if old.Key() == newBinding.Key() {
				found = true
				break
			}
		}

		if !found {
			logger.Ctx(svc.ctx).Infow("binding removed", "key", old.Key())
			handler := svc.pushHandlers[old.Key()]
			err := handler.Stop()
			if err != nil {
				return err
			}
			delete(svc.pushHandlers, old.Key())
		}
	}

	for _, newBinding := range newBindings {
		found := false
		for _, old := range oldBindings {
			if old.Key() == newBinding.Key() {
				found = true
				break
			}
		}

		if !found {
			logger.Ctx(svc.ctx).Infow("binding added", "key", newBinding.Key())
			handler := NewPushStream(newBinding.NodeID, newBinding.SubscriptionID, svc.subscriptionCore, svc.subscriber)
			svc.workgrp.Go(handler.Start)
			svc.pushHandlers[newBinding.Key()] = handler
		}
	}

	svc.nodebindingCache = newBindings
	return nil
}

func (svc *Service) handleSubUpdates(newSubsPairs []registry.Pair) error {
	oldSubs := svc.subCache
	var newSubs []*subscription.Model

	for _, pair := range newSubsPairs {
		sub := subscription.Model{}
		err := json.Unmarshal(pair.Value, &sub)
		if err != nil {
			return err
		}
		newSubs = append(newSubs, &sub)
	}

	for _, old := range oldSubs {
		found := false
		for _, newSub := range newSubs {
			if old.Key() == newSub.Key() {
				found = true
				break
			}
		}

		if !found {
			logger.Ctx(svc.ctx).Infow("sub removed", "key", old.Key())
			for _, nb := range svc.nodebindingCache {
				if nb.SubscriptionID == old.Key() {
					err := svc.nodeBindingCore.DeleteNodeBinding(svc.ctx, nb)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	for _, newSub := range newSubs {
		found := false
		for _, old := range oldSubs {
			if old.Key() == newSub.Key() {
				found = true
				break
			}
		}

		if !found {
			logger.Ctx(svc.ctx).Infow("sub added", "key", newSub.Key())

			logger.Ctx(svc.ctx).Infow("scheduling subscription on nodes", "key", newSub.Key(), "nodecache", svc.nodeCache, "bindings", svc.nodebindingCache)

			nb, err := svc.scheduler.Schedule(newSub, svc.nodebindingCache, svc.nodeCache)
			if err != nil {
				return err
			}

			err = svc.nodeBindingCore.CreateNodeBinding(svc.ctx, nb)
			if err != nil {
				return err
			}
		}
	}

	svc.subCache = newSubs
	return nil
}

func (svc *Service) handleNodeUpdates(newNodePairs []registry.Pair) error {
	oldNodes := svc.nodeCache
	var newNodes []*node.Model

	for _, pair := range newNodePairs {
		model := node.Model{}
		err := json.Unmarshal(pair.Value, &model)
		if err != nil {
			return err
		}
		newNodes = append(newNodes, &model)
	}

	for _, old := range oldNodes {
		found := false
		for _, newNode := range newNodes {
			if old.Key() == newNode.Key() {
				found = true
				break
			}
		}

		if !found {
			logger.Ctx(svc.ctx).Infow("node removed", "key", old.Key())

			for _, nb := range svc.nodebindingCache {
				if nb.NodeID == old.ID {
					err := svc.nodeBindingCore.DeleteNodeBinding(svc.ctx, nb)
					if err != nil {
						return err
					}
				}
			}

		}
	}

	for _, newNode := range newNodes {
		found := false
		for _, old := range oldNodes {
			if old.Key() == newNode.Key() {
				found = true
				break
			}
		}

		if !found {
			logger.Ctx(svc.ctx).Infow("node added", "key", newNode.Key())
			// Do nothing for now
		}
	}

	svc.nodeCache = newNodes

	return nil
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{}
}
