package interceptors

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"strings"

	"github.com/razorpay/metro/internal/credentials"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	authorizationHeaderKey = "authorization"
)

// UnaryServerAuthInterceptor creates an authenticator interceptor with the given AuthFunc
func UnaryServerAuthInterceptor(authFunc grpc_auth.AuthFunc) grpc.UnaryServerInterceptor {
	return grpc_auth.UnaryServerInterceptor(authFunc)
}

func getUserPasswordProjectID(ctx context.Context) (user string, password []byte, projectID string, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		err = status.Error(codes.Unauthenticated, "could not parse incoming context")
		return
	}

	// `uri` gets set inside `runtime.WithMetadata()`. check `server.go` for implementation
	// this is ideally a single-valued slice
	if val := md.Get("uri"); val != nil && len(val) > 0 {
		projectID = extractProjectIDFromURI(val[0])
	}

	headers := md.Get(authorizationHeaderKey)
	if len(headers) != 1 {
		err = status.Error(codes.Unauthenticated, "invalid authorization header")
		return
	}

	user, password, err = decodeAuthorizationHeader(headers[0])
	return
}

func decodeAuthorizationHeader(header string) (user string, password []byte, err error) {
	components := strings.Split(header, " ")
	if len(components) != 2 || components[0] != "Basic" {
		err = status.Error(codes.Unauthenticated, "authorization header malformed")
		return
	}

	decoded, err := base64.StdEncoding.DecodeString(components[1])
	if err != nil {
		err = status.Error(codes.Unauthenticated, err.Error())
		return
	}

	c := strings.Split(string(decoded), ":")
	user, password = c[0], []byte(c[1])
	return
}

// secureCompare: This function compares two values for their equality
func secureCompare(expected, actual string) bool {
	if subtle.ConstantTimeEq(int32(len(expected)), int32(len(actual))) == 1 {
		return subtle.ConstantTimeCompare([]byte(expected), []byte(actual)) == 1
	}

	return false
}

// extractProjectIDFromURI : This function extracts the projectID from the HTTP request URI.
// Example1: extractProjectIDFromURI("/v1/projects/project1/topics/t123") = project1
// Example2: extractProjectIDFromURI("/v1/projects/project7/subscriptions/s123") = project7
// Example3: extractProjectIDFromURI("/v1/admin/topic/t987") = ""
func extractProjectIDFromURI(uri string) string {
	if uri == "" {
		return ""
	}

	parts := strings.Split(uri, "/")
	if len(parts) >= 4 && parts[2] == "projects" && parts[3] != "" {
		return parts[3]
	}
	return ""
}

// AppAuth implements app project based basic auth validations
func AppAuth(ctx context.Context, credentialCore credentials.ICore) (context.Context, error) {
	user, password, uriProjectID, err := getUserPasswordProjectID(ctx)
	if err != nil {
		return ctx, err
	}

	if !credentials.IsValidUsername(user) {
		return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
	}

	projectID := credentials.GetProjectIDFromUsername(user)
	// lookup the credential
	credential, err := credentialCore.Get(ctx, projectID, user)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
	}

	// match the credential projectID and the uri projectID
	// this way we enforce that the credential is accessing only its own projectID's resources
	if !strings.EqualFold(uriProjectID, credential.GetProjectID()) {
		return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
	}

	expectedPassword := credential.GetPassword()
	// check the header password matches the expected password
	if !secureCompare(expectedPassword, string(password)) {
		return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
	}

	newCtx := context.WithValue(ctx, credentials.CtxKey.String(), credentials.NewCredential(user, string(password)))
	return newCtx, nil
}

// AdminAuth implements admin credentials based basic auth validations
func AdminAuth(ctx context.Context, admin *credentials.Model) (context.Context, error) {
	user, password, _, err := getUserPasswordProjectID(ctx)
	if err != nil {
		return ctx, err
	}

	if !secureCompare(admin.Username, user) || !secureCompare(admin.Password, string(password)) {
		return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
	}

	return ctx, nil
}
