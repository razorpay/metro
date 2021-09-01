package metroclient

import (
	"context"
	"encoding/base64"
	"fmt"

	"cloud.google.com/go/pubsub"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	authHeader string = "Authorization"
	basicAuth  string = "basic"
)

// Credentials - Holds username and password of metro client
type Credentials struct {
	Username string
	Password string
}

// NewMetroClient - returns an initialized metro's per topic publisher client
func NewMetroClient(endpoint string, isTLSEnabled bool, projectID string, credentials Credentials) (*pubsub.Client, error) {
	opts, err := getClientOptions(endpoint, isTLSEnabled, credentials)
	if err != nil {
		return nil, err
	}
	return pubsub.NewClient(context.Background(), projectID, opts...)
}

// getClientOptions - returns the options to create the pubsub client
func getClientOptions(endpoint string, isTLSEnabled bool, credentials Credentials) ([]option.ClientOption, error) {
	if isTLSEnabled {
		return []option.ClientOption{
			option.WithEndpoint(endpoint),
			option.WithTokenSource(&credentialsProvider{credentials}),
		}, nil
	}
	// for non-secure gRPC endpoints, cannot set a token source
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure(), grpc.WithPerRPCCredentials(&credentialsProvider{credentials}))
	if err != nil {
		return nil, err
	}
	return []option.ClientOption{
		option.WithGRPCConn(conn),
	}, nil
}

// credentialsProvider - provides credentials for gRPC requests
// implements google.golang.org/grpc/credentials credentials.PerRPCCredentials
// and golang.org/x/oauth2 oauth2.TokenSource
type credentialsProvider struct {
	credentials Credentials
}

// GetRequestMetadata - gets the current request metadata
func (c *credentialsProvider) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	token, err := c.Token()
	if err != nil {
		return nil, err
	}

	return map[string]string{
		authHeader: fmt.Sprintf("%s %s", token.Type(), token.AccessToken),
	}, nil
}

// RequireTransportSecurity - indicates if credentials require transport layer security
func (c *credentialsProvider) RequireTransportSecurity() bool {
	return false
}

// Token - returns a token for api authentication/authorization
func (c *credentialsProvider) Token() (*oauth2.Token, error) {
	encodedCredentials := base64.StdEncoding.EncodeToString([]byte(
		c.credentials.Username + ":" + c.credentials.Password),
	)

	return &oauth2.Token{
		AccessToken: encodedCredentials,
		TokenType:   basicAuth,
	}, nil
}
