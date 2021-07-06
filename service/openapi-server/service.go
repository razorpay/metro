package openapiserver

import (
	"context"
	"github.com/razorpay/metro/pkg/logger"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/rakyll/statik/fs"
)

const metroAPIPrefix = "/v1"

// Service for openapi-server
type Service struct {
	config *Config
	server http.Server
}

// NewService creates an instance of new producer service
func NewService(config *Config) *Service {
	return &Service{
		config: config,
	}
}

// Start the OpenAPI server, shutdown on ctx.Done()
func (svc *Service) Start(ctx context.Context) error {
	return svc.runOpenAPIHandler(ctx)
}

// Stop the OpenAPI server
func (svc *Service) Stop(ctx context.Context){
	err := svc.server.Shutdown(ctx)
	if err != nil {
		logger.Ctx(ctx).Warnw("failed to shutdown the openapi server", "error", err.Error())
	}
}

// runOpenAPIHandler serves an OpenAPI UI.
// Adapted from https://github.com/philips/grpc-gateway-example/blob/a269bcb5931ca92be0ceae6130ac27ae89582ecc/cmd/serve.go#L63
func (svc *Service) runOpenAPIHandler(ctx context.Context) error {
	mime.AddExtensionType(".svg", "image/svg+xml")

	statikFS, err := fs.New()
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.RequestURI, metroAPIPrefix) {
			httputil.NewSingleHostReverseProxy(&url.URL{
				Scheme: "http",
				Host:   svc.config.GRPCGatewayAddress,
			}).ServeHTTP(w, r)
		} else {
			http.FileServer(statikFS).ServeHTTP(w, r)
		}
	})

	server := http.Server{Addr: svc.config.HTTPServerAddress, Handler: mux}
	err = server.ListenAndServe()
	if err != nil {
		return err
	}
	svc.server = server
	return nil
}
