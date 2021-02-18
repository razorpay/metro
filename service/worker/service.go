package worker

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	internalserver "github.com/razorpay/metro/internal/server"
	metrov1 "github.com/razorpay/metro/rpc/proto/v1"
	"google.golang.org/grpc"

	"github.com/google/uuid"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/pkg/leaderelection"
	"github.com/razorpay/metro/pkg/logger"
	"github.com/razorpay/metro/pkg/registry"
	"golang.org/x/sync/errgroup"
)

// Service for worker
type Service struct {
	ctx            context.Context
	grpcServer     *grpc.Server
	httpServer     *http.Server
	health         *health.Core
	workerConfig   *Config
	registryConfig *registry.Config
	registry       registry.IRegistry
	candidate      *leaderelection.Candidate
	nodeID         string
	doneCh         chan struct{}
	stopCh         chan struct{}
	workgrp        *errgroup.Group
	leadgrp        *errgroup.Group
}

// NewService creates an instance of new worker
func NewService(ctx context.Context, workerConfig *Config, registryConfig *registry.Config) *Service {
	return &Service{
		ctx:            ctx,
		workerConfig:   workerConfig,
		registryConfig: registryConfig,
		nodeID:         uuid.New().String(),
		doneCh:         make(chan struct{}),
		stopCh:         make(chan struct{}),
	}
}

// Start implements all the tasks for worker and waits until one of the task fails
func (c *Service) Start() error {
	// close the done channel when this function returns
	defer close(c.doneCh)

	var (
		err  error
		gctx context.Context
	)

	c.workgrp, gctx = errgroup.WithContext(c.ctx)

	// Define server handlers
	healthCore, err := health.NewCore(nil) //TODO: Add checkers
	if err != nil {
		return err
	}

	// Init the Registry
	// TODO: move to component init ?
	c.registry, err = registry.NewRegistry(c.ctx, c.registryConfig)
	if err != nil {
		return err
	}

	// Init Leader Election
	c.candidate, err = leaderelection.New(c.nodeID,
		leaderelection.Config{
			// TODO: read values from workerConfig
			Name:          "metro/metro-worker",
			NodePath:      fmt.Sprintf("metro/nodes/%s", c.nodeID),
			LockPath:      "metro/leader/election",
			LeaseDuration: 30 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) error {
					return c.lead(ctx)
				},
				OnStoppedLeading: func() {
					c.stepDown()
				},
			},
		}, c.registry)

	if err != nil {
		return err
	}

	// Run leader election
	c.workgrp.Go(func() error {
		logger.Ctx(gctx).Info("starting leader election")
		return c.candidate.Run(gctx)
	})

	// Watch for the subscription assignment changes
	var watcher registry.IWatcher
	c.workgrp.Go(func() error {
		wh := registry.WatchConfig{
			WatchType: "keyprefix",
			WatchPath: fmt.Sprintf("metro/nodes/%s/subscriptions", c.nodeID),
			Handler: func(ctx context.Context, pairs []registry.Pair) {
				logger.Ctx(ctx).Infow("node subscriptions", "pairs", pairs)
			},
		}

		watcher, err = c.registry.Watch(gctx, &wh)
		if err != nil {
			return err
		}

		return watcher.StartWatch()
	})

	c.workgrp.Go(func() error {
		var err error
		select {
		case <-gctx.Done():
			err = gctx.Err()
		case <-c.stopCh:
			err = fmt.Errorf("signal received, stopping worker")
		}

		if watcher != nil {
			watcher.StopWatch()
		}

		return err
	})

	grpcServer, err := internalserver.StartGRPCServer(
		c.workgrp,
		c.workerConfig.Interfaces.API.GrpcServerAddress,
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
		c.workgrp,
		c.workerConfig.Interfaces.API.HTTPServerAddress,
		func(mux *runtime.ServeMux) error {
			err := metrov1.RegisterHealthCheckAPIHandlerFromEndpoint(gctx, mux, c.workerConfig.Interfaces.API.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
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

	c.grpcServer = grpcServer
	c.httpServer = httpServer
	c.health = healthCore

	err = c.workgrp.Wait()
	logger.Ctx(gctx).Info("worker service error: %s", err.Error())
	return err
}

// Stop the service
func (c *Service) Stop() error {
	// signal to stop all go routines
	close(c.stopCh)

	// wait until all goroutines are done
	<-c.doneCh

	return nil
}

func (c *Service) lead(ctx context.Context) error {
	logger.Ctx(ctx).Infof("Node %s elected as new leader", c.nodeID)

	var (
		nodeWatcher, subWatcher registry.IWatcher
		err                     error
		gctx                    context.Context
	)
	c.leadgrp, gctx = errgroup.WithContext(ctx)

	nwh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: "metro/nodes",
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("nodes watch handler data", "pairs", pairs)
		},
	}

	nodeWatcher, err = c.registry.Watch(gctx, &nwh)
	if err != nil {
		return err
	}

	swh := registry.WatchConfig{
		WatchType: "keyprefix",
		WatchPath: "metro/subscriptions",
		Handler: func(ctx context.Context, pairs []registry.Pair) {
			logger.Ctx(ctx).Infow("nodes watch handler data", "pairs", pairs)
		},
	}

	subWatcher, err = c.registry.Watch(gctx, &swh)
	if err != nil {
		return err
	}

	// watch for nodes addition/deletion, for any changes a rebalance might be required
	c.leadgrp.Go(func() error {
		return nodeWatcher.StartWatch()
	})

	// watch the Subscriptions path for new subscriptions and rebalance
	c.leadgrp.Go(func() error {
		return subWatcher.StartWatch()
	})

	// wait for done channel to be closed and stop watches if received done
	c.leadgrp.Go(func() error {
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

func (c *Service) stepDown() {
	logger.Ctx(c.ctx).Infof("Node %s stepping down from leader", c.nodeID)

	// wait for leader go routines to terminate
	c.leadgrp.Wait()
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{}
}
