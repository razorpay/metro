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

func getUserPassword(ctx context.Context) (user string, password []byte, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		err = status.Error(codes.Unauthenticated, "could not parse incoming context")
		return
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

// AppAuth implements app project based basic auth validations
func AppAuth(ctx context.Context, credentialCore credentials.ICore) (context.Context, error) {
	user, password, err := getUserPassword(ctx)
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

	expectedPassword := credential.GetPassword()
	// check the header password matches the expected password
	if !secureCompare(expectedPassword, string(password)) {
		return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
	}

	newCtx := context.WithValue(ctx, credentials.CtxKey, credentials.NewCredential(user, string(password)))
	return newCtx, nil
}

// AdminAuth implements admin credentials based basic auth validations
func AdminAuth(ctx context.Context, admin *credentials.Model) (context.Context, error) {
	user, password, err := getUserPassword(ctx)
	if err != nil {
		return ctx, err
	}

	if !secureCompare(admin.Username, user) || !secureCompare(admin.Password, string(password)) {
		return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
	}

	return ctx, nil
}
