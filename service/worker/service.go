package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/razorpay/metro/internal/common"

	"github.com/razorpay/metro/internal/subscriber"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/razorpay/metro/internal/brokerstore"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/node"
	"github.com/razorpay/metro/internal/nodebinding"
	"github.com/razorpay/metro/internal/project"
	"github.com/razorpay/metro/internal/server"
	"github.com/razorpay/metro/internal/subscription"
	"github.com/razorpay/metro/internal/topic"
	"github.com/razorpay/metro/pkg/leaderelection"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
	"github.com/razorpay/metro/pkg/scheduler"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Service for worker
type Service struct {
	ctx                context.Context
	grpcServer         *grpc.Server
	httpServer         *http.Server
	internalHTTPServer *http.Server
	health             *health.Core
	workerConfig       *Config
	registryConfig     *registry.Config
	registry           registry.IRegistry
	candidate          *leaderelection.Candidate
	node               *node.Model
	doneCh             chan struct{}
	stopCh             chan struct{}
	workgrp            *errgroup.Group
	leadgrp            *errgroup.Group
	brokerStore        brokerstore.IBrokerStore
	projectCore        project.ICore
	nodeCore           node.ICore
	topicCore          topic.ICore
	subscriptionCore   subscription.ICore
	nodeBindingCore    nodebinding.ICore
	nodeCache          []*node.Model
	subCache           []*subscription.Model
	nodebindingCache   []*nodebinding.Model
	nbwatch            chan []registry.Pair
	pushHandlers       map[string]*PushStream
	scheduler          *scheduler.Scheduler
	subscriber         subscriber.ICore
	nbwatcher          registry.IWatcher
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
		pushHandlers:     map[string]*PushStream{},
		nbwatch:          make(chan []registry.Pair),
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

	svc.subscriptionCore = subscription.NewCore(
		subscription.NewRepo(svc.registry),
		svc.projectCore,
		svc.topicCore)

	svc.nodeCore = node.NewCore(node.NewRepo(svc.registry))

	svc.nodeBindingCore = nodebinding.NewCore(nodebinding.NewRepo(svc.registry))

	svc.subscriber = subscriber.NewCore(svc.brokerStore, svc.subscriptionCore)

	svc.scheduler, err = scheduler.New(scheduler.LoadBalance)

	if err != nil {
		return err
	}

	// Init Leader Election
	svc.candidate, err = leaderelection.New(svc.node,
		leaderelection.Config{
			Name:          "metro/metro-worker",
			LockPath:      common.GetBasePrefix() + "leader/election",
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

	// Watch for the subscription assignment changes
	svc.workgrp.Go(func() error {
		prefix := fmt.Sprintf(common.GetBasePrefix()+nodebinding.Prefix+"%s/", svc.node.ID)
		logger.Ctx(gctx).Infow("setting up node subscriptions watch", "prefix", prefix)

		wh := registry.WatchConfig{
			WatchType: "keyprefix",
			WatchPath: prefix,
			Handler: func(ctx context.Context, pairs []registry.Pair) {
				logger.Ctx(ctx).Infow("node subscriptions", "pairs", pairs)
				svc.nbwatch <- pairs
			},
		}

		svc.nbwatcher, err = svc.registry.Watch(gctx, &wh)
		if err != nil {
			return err
		}

		err = svc.nbwatcher.StartWatch()

		// close the node binding data channel on watch terminations
		logger.Ctx(gctx).Infow("watch terminated, closing the node binding data channel")
		close(svc.nbwatch)

		return err
	})

	svc.workgrp.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				logger.Ctx(gctx).Infow("nodebinding handler routine exiting as group context done")
				return gctx.Err()
			case pairs := <-svc.nbwatch:
				logger.Ctx(gctx).Infow("received data from node bindings channel", "pairs", pairs)
				err = svc.handleNodeBindingUpdates(gctx, pairs)
				if err != nil {
					logger.Ctx(gctx).Infow("error processing nodebinding updates", "error", err)
					return err
				}
			}
		}
	})

	svc.workgrp.Go(func() error {
		var err error
		select {
		case <-gctx.Done():
			err = gctx.Err()
		case <-svc.stopCh:
			err = fmt.Errorf("signal received, stopping worker")
		}

		return err
	})

	grpcServer, err := server.StartGRPCServer(
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

	httpServer, err := server.StartHTTPServer(
		svc.workgrp,
		svc.workerConfig.Interfaces.API.HTTPServerAddress,
		func(mux *runtime.ServeMux) error {
			err := metrov1.RegisterHealthCheckAPIHandlerFromEndpoint(gctx, mux, svc.workerConfig.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
			if err != nil {
				return err
			}

			return nil
		})

	if err != nil {
		return err
	}

	internalHTTPServer, err := server.StartInternalHTTPServer(svc.workgrp, svc.workerConfig.Interfaces.API.InternalHTTPServerAddress)
	if err != nil {
		return err
	}

	svc.grpcServer = grpcServer
	svc.httpServer = httpServer
	svc.internalHTTPServer = internalHTTPServer
	svc.health = healthCore

	err = svc.workgrp.Wait()
	if err != nil {
		logger.Ctx(gctx).Infof("worker service error: %s", err.Error())
	}
	return err
}

// Stop the service
func (svc *Service) Stop() error {
	logger.Ctx(svc.ctx).Infow("metro stop invoked")

	// First we stop the node bindings watch, this will ensure that no new bindings are created
	// otherwise leaderelction termination will cause node to be removed, and then nodebindings to be deleted
	if svc.nbwatcher != nil {
		logger.Ctx(svc.ctx).Infow("stopping the node subscription watch")
		svc.nbwatcher.StopWatch()
	}

	// signal to stop all go routines
	close(svc.stopCh)

	// stop all push stream handlers
	logger.Ctx(svc.ctx).Infow("metro stop: stopping all push handlers")
	for _, handler := range svc.pushHandlers {
		err := handler.Stop()
		if err != nil {
			logger.Ctx(svc.ctx).Infow("error stopping stream handler", "error", err)
		}
	}

	// signal the grpc server go routine
	svc.grpcServer.GracefulStop()

	// signal http server go routine
	err := svc.httpServer.Shutdown(svc.ctx)
	if err != nil {
		return err
	}

	err = svc.internalHTTPServer.Shutdown(svc.ctx)
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

	nodeWatchData := make(chan struct{})
	subWatchData := make(chan struct{})

	svc.leadgrp, gctx = errgroup.WithContext(ctx)

	nwh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: common.GetBasePrefix() + node.Prefix,
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("nodes watch handler data", "pairs", pairs)
			nodeWatchData <- struct{}{}
		},
	}

	logger.Ctx(ctx).Infof("setting watch on nodes")
	nodeWatcher, err = svc.registry.Watch(gctx, &nwh)
	if err != nil {
		return err
	}

	swh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: common.GetBasePrefix() + subscription.Prefix,
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("subscriptions watch handler data", "pairs", pairs)
			subWatchData <- struct{}{}
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

	// handle node and subscription updates
	svc.leadgrp.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case <-subWatchData:
				allSubs, serr := svc.subscriptionCore.List(gctx, subscription.Prefix)
				if serr != nil {
					logger.Ctx(gctx).Errorw("error fetching new subscription list", "error", serr)
					return err
				}
				// Filter Push Subscriptions
				newSubs := []*subscription.Model{}
				for _, sub := range allSubs {
					if sub.IsPush() {
						newSubs = append(newSubs, sub)
					}
				}

				svc.subCache = newSubs
				serr = svc.refreshNodeBindings(gctx)
				if serr != nil {
					// just log the error, we want to retry the sub update failures
					logger.Ctx(gctx).Infow("error processing subscription updates", "error", serr)
				}
			case <-nodeWatchData:
				nodes, nerr := svc.nodeCore.List(gctx, node.Prefix)
				if nerr != nil {
					logger.Ctx(gctx).Errorw("error fetching new node list", "error", nerr)
					return nerr
				}
				svc.nodeCache = nodes
				nerr = svc.refreshNodeBindings(gctx)
				if nerr != nil {
					logger.Ctx(gctx).Infow("error processing node updates", "error", nerr)
				}

			}
		}
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

		close(nodeWatchData)
		close(subWatchData)

		logger.Ctx(gctx).Info("leader context returned done")
		return gctx.Err()
	})

	// wait for leader go routines to terminate
	err = svc.leadgrp.Wait()

	if err != nil {
		logger.Ctx(gctx).Errorf("Error in leader group go routines : %s", err.Error())
	}

	return err
}

func (svc *Service) stepDown() {
	logger.Ctx(svc.ctx).Infof("Node %s stepping down from leader", svc.node.ID)
}

func (svc *Service) handleNodeBindingUpdates(ctx context.Context, newBindingPairs []registry.Pair) error {
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
			logger.Ctx(ctx).Infow("binding removed", "key", old.Key())
			handler := svc.pushHandlers[old.Key()]
			handler.Stop()
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
			logger.Ctx(ctx).Infow("binding added", "key", newBinding.Key())
			handler := NewPushStream(ctx, newBinding.ID, newBinding.SubscriptionID, svc.subscriptionCore, svc.subscriber, &svc.workerConfig.HTTPClientConfig)

			// run the stream in a separate go routine, this go routine is not part of the worker error group
			// as the worker should continue to run if a single subscription stream exists with error
			go func(ctx context.Context) {
				err := handler.Start()
				if err != nil {
					logger.Ctx(ctx).Errorw("[worker]: push stream handler exited",
						"subscription", newBinding.SubscriptionID,
						"error", err.Error())
				}
			}(ctx)

			svc.pushHandlers[newBinding.Key()] = handler
		}
	}

	svc.nodebindingCache = newBindings
	return nil
}

func (svc *Service) refreshNodeBindings(ctx context.Context) error {
	// fetch all current nodebindings across all nodes
	nodeBindings, err := svc.nodeBindingCore.List(ctx, nodebinding.Prefix)
	if err != nil {
		logger.Ctx(ctx).Errorw("error fetching new node binding list", "error", err)
		return err
	}

	// Delete any binding where subscription is removed, This needs to be handlled before nodes updates
	// as node update will cause subscsriptions to be rescheduled on other nodes
	validBindings := []*nodebinding.Model{}

	// TODO: Optimize O(N*M) to O(N+M)
	for _, nb := range nodeBindings {
		found := false
		for _, sub := range svc.subCache {
			if sub.Name == nb.SubscriptionID {
				found = true
				break
			}
		}

		if !found {
			svc.nodeBindingCore.DeleteNodeBinding(ctx, nb)
		} else {
			validBindings = append(validBindings, nb)
		}
	}
	// update the binding list after deletions
	nodeBindings = validBindings

	// Reschedule any binding where node is removed
	validBindings = []*nodebinding.Model{}
	invalidBindings := []*nodebinding.Model{}

	// TODO: Optimize O(N*M) to O(N+M)
	for _, nb := range nodeBindings {
		found := false
		for _, node := range svc.nodeCache {
			if node.ID == nb.NodeID {
				found = true
				break
			}
		}

		if !found {
			invalidBindings = append(invalidBindings, nb)
			svc.nodeBindingCore.DeleteNodeBinding(ctx, nb)
		} else {
			validBindings = append(validBindings, nb)
		}
	}

	// update the binding list after deletions
	nodeBindings = validBindings

	// Reschedule Bindings which are invalid due to node failures
	for _, nb := range invalidBindings {
		logger.Ctx(ctx).Infow("rescheduling subscription on nodes", "key", nb.SubscriptionID)

		sub, serr := svc.subscriptionCore.Get(ctx, nb.SubscriptionID)
		if serr != nil {
			return serr
		}

		serr = svc.scheduleSubscription(ctx, sub, nodeBindings)
		if serr != nil {
			return serr
		}
	}

	// Create bindings for new subscriptions
	for _, sub := range svc.subCache {
		found := false
		for _, nb := range nodeBindings {
			if sub.Name == nb.SubscriptionID {
				found = true
				break
			}
		}

		if !found {
			logger.Ctx(ctx).Infow("scheduling subscription on nodes", "subscription", sub.Name, "topic", sub.Topic)

			topicM, terr := svc.topicCore.Get(ctx, sub.Topic)
			if terr != nil {
				return terr
			}

			for i := 0; i < topicM.NumPartitions; i++ {
				serr := svc.scheduleSubscription(ctx, sub, nodeBindings)
				if serr != nil {
					return serr
				}
			}
		}
	}
	return nil
}

func (svc *Service) scheduleSubscription(ctx context.Context, sub *subscription.Model, nodeBindings []*nodebinding.Model) error {
	nb, serr := svc.scheduler.Schedule(sub, nodeBindings, svc.nodeCache)
	if serr != nil {
		return serr
	}

	berr := svc.nodeBindingCore.CreateNodeBinding(ctx, nb)
	if berr != nil {
		return berr
	}

	nodeBindings = append(nodeBindings, nb)
	return nil
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{}
}
