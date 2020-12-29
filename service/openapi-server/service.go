package openapiserver

import (
	"context"
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
	ctx    context.Context
}

// NewService creates an instance of new producer service
func NewService(ctx context.Context, config *Config) *Service {
	return &Service{
		ctx:    ctx,
		config: config,
	}
}

// Start the service
func (svc *Service) Start() error {
	err := svc.runOpenAPIHandler()

	return err
}

// Stop the service
func (svc *Service) Stop() error {
	return svc.server.Shutdown(svc.ctx)
}

// runOpenAPIHandler serves an OpenAPI UI.
// Adapted from https://github.com/philips/grpc-gateway-example/blob/a269bcb5931ca92be0ceae6130ac27ae89582ecc/cmd/serve.go#L63
func (svc *Service) runOpenAPIHandler() error {
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
