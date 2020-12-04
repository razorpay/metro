package producer

import (
	"context"
	"log"
	"mime"
	"net/http"

	"github.com/razorpay/metro/pkg/messagebroker"
	producerv1 "github.com/razorpay/metro/rpc/metro/producer/v1"

	"github.com/razorpay/metro/internal/config"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rakyll/statik/fs"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/server"
	healthv1 "github.com/razorpay/metro/rpc/common/health/v1"
	_ "github.com/razorpay/metro/statik"
	"google.golang.org/grpc"
)

type Service struct {
	ctx    context.Context
	srv    *server.Server
	health *health.Core
	config *config.Service
}

func NewService(ctx context.Context, config *config.Service) *Service {
	return &Service{
		ctx:    ctx,
		config: config,
	}
}

func (svc *Service) Start(errChan chan<- error) {
	// Define server handlers

	healthCore, err := health.NewCore(nil)
	if err != nil {
		errChan <- err
	}

	mb, err := messagebroker.NewBroker(messagebroker.Kafka, &svc.config.BrokerConfig)
	if err != nil {
		errChan <- err
	}
	brokerCore, err := NewCore(mb)
	if err != nil {
		errChan <- err
	}

	s, err := server.NewServer(svc.config.Interfaces.Api, func(server *grpc.Server) error {
		healthv1.RegisterHealthCheckAPIServer(server, health.NewServer(healthCore))
		producerv1.RegisterProducerServer(server, NewServer(brokerCore))
		return nil
	}, func(mux *runtime.ServeMux) error {
		err := healthv1.RegisterHealthCheckAPIHandlerFromEndpoint(svc.ctx, mux, svc.config.Interfaces.Api.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
		if err != nil {
			return err
		}

		err = producerv1.RegisterProducerHandlerFromEndpoint(svc.ctx, mux, svc.config.Interfaces.Api.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
		if err != nil {
			return err
		}

		return nil
	},
		getInterceptors()...,
	)

	if err != nil {
		errChan <- err
	}

	s.Start(errChan)

	svc.srv = s
	svc.health = healthCore

	err = runOpenAPIHandler()
	if err != nil {
		errChan <- err
	}
}

func (svc *Service) Stop() error {
	return svc.srv.Stop(svc.ctx, svc.health)
}

func getInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{}
}

// runOpenAPIHandler serves an OpenAPI UI.
// Adapted from https://github.com/philips/grpc-gateway-example/blob/a269bcb5931ca92be0ceae6130ac27ae89582ecc/cmd/serve.go#L63
func runOpenAPIHandler() error {
	mime.AddExtensionType(".svg", "image/svg+xml")

	statikFS, err := fs.New()
	if err != nil {
		return err
	}
	http.Handle("/", http.FileServer(statikFS))
	log.Println("Listening on :3000...")
	err = http.ListenAndServe(":3000", nil)
	if err != nil {
		return err
	}
	return nil
}
