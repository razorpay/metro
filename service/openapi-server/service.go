package openapiserver

import (
	"context"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/rakyll/statik/fs"

	"github.com/razorpay/metro/pkg/logger"
)

const metroAPIPrefix = "/v1"

// Service for openapi-server
type Service struct {
	config  *Config
	errChan chan error
}

// NewService creates an instance of new producer service
func NewService(config *Config) (*Service, error) {
	return &Service{
		config: config,
	}, nil
}

// Start the OpenAPI server, shutdown on ctx.Done()
func (svc *Service) Start(ctx context.Context) error {
	return svc.runOpenAPIHandler(ctx)
}

func (svc *Service) GetErrorChannel() chan error {
	return nil
}

// runOpenAPIHandler serves an OpenAPI UI.
// Adapted from https://github.com/philips/grpc-gateway-example/blob/a269bcb5931ca92be0ceae6130ac27ae89582ecc/cmd/serve.go#L63
func (svc *Service) runOpenAPIHandler(ctx context.Context) error {
	err := mime.AddExtensionType(".svg", "image/svg+xml")
	if err != nil {
		return err
	}

	statikFS, err := fs.New()
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.RequestURI, metroAPIPrefix) {
			// Change Host header to avoid CORS conflict
			r.Host = svc.config.ProxyHostAddress
			httputil.NewSingleHostReverseProxy(&url.URL{
				Scheme: svc.config.Scheme,
				Host:   svc.config.ProxyHostAddress,
			}).ServeHTTP(w, r)
		} else {
			http.FileServer(statikFS).ServeHTTP(w, r)
		}
	})

	server := http.Server{Addr: svc.config.HTTPServerAddress, Handler: mux}

	// Stop the server when context is Done
	go func() {
		<-ctx.Done()

		// create a new ctx for server shutdown with timeout
		// we are not using a cancelled context here as that returns immediately
		newServerCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := server.Shutdown(newServerCtx)
		if err != nil {
			logger.Ctx(ctx).Infow("http server shutdown failed with err", "error", err.Error())
		}

	}()

	// Run the server
	err = server.ListenAndServe()

	if err != nil {
		logger.Ctx(ctx).Errorw("openapi-server returned with error", "error", err.Error())
	}

	return err
}
