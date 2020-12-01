package producer

import (
	"context"
	"log"
	"mime"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/razorpay/metro/internal/boot"
	healthv1 "github.com/razorpay/metro/rpc/common/health/v1"

	"github.com/razorpay/metro/internal/config"

	"github.com/rakyll/statik/fs"
	"github.com/razorpay/metro/internal/health"
	"github.com/razorpay/metro/internal/server"
	"google.golang.org/grpc"
)

type Service struct {
	ctx    context.Context
	srv    *server.Server
	health *health.Core
	config config.Config
}

func NewService(ctx context.Context) *Service {
	return &Service{ctx: ctx}
}

func (svc *Service) Start() {
	// Define server handlers

	healthCore, err := health.NewCore(nil)
	if err != nil {
		panic(err)
	}

	s, err := server.NewServer(boot.Config.App.Interfaces.Api, func(server *grpc.Server) error {
		healthv1.RegisterHealthCheckAPIServer(server, health.NewServer(healthCore))

		return nil
	}, func(mux *runtime.ServeMux) error {
		err := healthv1.RegisterHealthCheckAPIHandlerFromEndpoint(svc.ctx, mux, boot.Config.App.Interfaces.Api.GrpcServerAddress, []grpc.DialOption{grpc.WithInsecure()})
		if err != nil {
			return err
		}

		return nil
	},
		getInterceptors()...,
	)

	if err != nil {
		panic(err)
	}

	err = s.Start()
	if err != nil {
		panic(err)
	}
	svc.srv = s
	svc.health = healthCore

	err = runOpenAPIHandler()
	if err != nil {
		panic(err)
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
		// Panic since this is a permanent error.
		panic("creating OpenAPI filesystem: " + err.Error())
	}
	http.Handle("/", http.FileServer(statikFS))
	log.Println("Listening on :3000...")
	err = http.ListenAndServe(":3000", nil)
	if err != nil {
		return err
	}
	return nil
}
