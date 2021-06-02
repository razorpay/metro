package interceptors

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"regexp"
	"strings"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/razorpay/metro/internal/auth"
	"github.com/razorpay/metro/internal/project"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	authorizationHeaderKey = "authorization"
)

var usernameRegex *regexp.Regexp

func init() {
	var err error
	usernameRegex, err = regexp.Compile("([a-z][a-z0-9-]{5,29})__[a-zA-Z0-9]{6}")
	if err != nil {
		panic(err)
	}
}

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
func AppAuth(ctx context.Context, projectCore project.ICore) (context.Context, error) {
	user, password, err := getUserPassword(ctx)
	if err != nil {
		return ctx, err
	}

	// check if the username is of the expected format
	if !usernameRegex.MatchString(user) {
		return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
	}

	// extract out the projectID from the username
	projectID := strings.Split(user, project.PartSeparator)[0]

	// lookup the project
	project, err := projectCore.Get(ctx, projectID)
	if err != nil {
		return ctx, err
	}

	// check if the project auth map contains the given username
	if _, ok := project.AppAuth[user]; !ok {
		return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
	}

	// extract out the user specific auth details
	authM := project.AppAuth[user]

	expectedPassword := authM.GetPassword()
	// check the header password matches the expected password
	if !secureCompare(expectedPassword, string(password)) {
		return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
	}

	return ctx, nil
}

// AdminAuth implements admin credentials based basic auth validations
func AdminAuth(ctx context.Context, admin *auth.Auth) (context.Context, error) {
	user, password, err := getUserPassword(ctx)
	if err != nil {
		return ctx, err
	}

	if !secureCompare(admin.Username, string(user)) || !secureCompare(admin.Password, string(password)) {
		return nil, status.Error(codes.Unauthenticated, "Unauthenticated")
	}

	return ctx, nil
}
