package worker

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/razorpay/metro/internal/brokerstore"

	"github.com/razorpay/metro/internal/topic"

	"github.com/razorpay/metro/internal/project"

	"github.com/razorpay/metro/internal/subscription"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/node"
	internalserver "github.com/razorpay/metro/internal/server"
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
	nodeCache        []string
	subCache         []string
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
		doneCh: make(chan struct{}),
		stopCh: make(chan struct{}),
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

	// Register Node with registry
	err = svc.nodeCore.CreateNode(svc.ctx, svc.node)
	if err != nil {
		return err
	}

	// Init Leader Election
	svc.candidate, err = leaderelection.New(svc.node.ID,
		leaderelection.Config{
			Name:          "metro/metro-worker",
			NodePath:      svc.node.Key(),
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

	// Watch for the subscription assignment changes
	var watcher registry.IWatcher
	svc.workgrp.Go(func() error {
		wh := registry.WatchConfig{
			WatchType: "keyprefix",
			WatchPath: fmt.Sprintf("metro/nodebinding/%s/", svc.node.ID),
			Handler: func(ctx context.Context, pairs []registry.Pair) {
				logger.Ctx(ctx).Infow("node subscriptions", "pairs", pairs)
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
	logger.Ctx(gctx).Info("worker service error: %s", err.Error())
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
	svc.nodeCache, err = svc.nodeCore.ListKeys(svc.ctx)
	if err != nil {
		return err
	}

	// create subscription cache
	logger.Ctx(ctx).Infof("getting active subscriptions for cache")
	svc.subCache, err = svc.subscriptionCore.ListKeys(svc.ctx)
	if err != nil {
		return err
	}

	svc.leadgrp, gctx = errgroup.WithContext(ctx)

	nwh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: "metro/nodes",
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("nodes watch handler data", "pairs", pairs)
			newNodes := registry.GetKeys(pairs)
			svc.handleNodeUpdates(newNodes)
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
			logger.Ctx(ctx).Infow("nodes watch handler data", "pairs", pairs)
			newSubs := registry.GetKeys(pairs)
			svc.handleSubUpdates(newSubs)
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
	svc.leadgrp.Wait()
}

func (svc *Service) handleSubUpdates(newSubs []string) error {
	oldSubs := svc.subCache
	for _, old := range oldSubs {
		found := false
		for _, new := range newSubs {
			if old == new {
				found = true
				break
			}
		}

		if !found {
			logger.Ctx(svc.ctx).Infow("sub removed", "sub_key", old)
		}
	}

	for _, new := range newSubs {
		found := false
		for _, old := range oldSubs {
			if old == new {
				found = true
				break
			}
		}

		if !found {
			logger.Ctx(svc.ctx).Infow("sub added", "sub_key", new)
		}
	}

	svc.subCache = newSubs
	return nil
}

func (svc *Service) handleNodeUpdates(newNodes []string) error {
	oldNodes := svc.nodeCache

	for _, old := range oldNodes {
		found := false
		for _, new := range newNodes {
			if old == new {
				found = true
				break
			}
		}

		if !found {
			logger.Ctx(svc.ctx).Infow("node removed", "node_key", old)
		}
	}

	for _, new := range newNodes {
		found := false
		for _, old := range oldNodes {
			if old == new {
				found = true
				break
			}
		}

		if !found {
			logger.Ctx(svc.ctx).Infow("node added", "node_key", new)
		}
	}

	svc.nodeCache = newNodes

	return nil
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{}
}
