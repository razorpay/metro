package hooks

import (
	"context"
	"net/http"

	"github.com/twitchtv/twirp"

	"github.com/razorpay/metro/internal/boot"
)

type contextkey int

const (
	authUserCtxKey contextkey = iota
	authPassCtxKey
)

func Auth() *twirp.ServerHooks {
	hooks := &twirp.ServerHooks{}

	hooks.RequestReceived = func(ctx context.Context) (context.Context, error) {
		user, _ := ctx.Value(authUserCtxKey).(string)
		pass, _ := ctx.Value(authPassCtxKey).(string)

		if user == "" || pass == "" {
			return ctx, twirp.NewError(twirp.Unauthenticated, "empty username/password")
		}

		// Dynamically retrieves expected password for user from config.
		// See: internal/config's Auth.
		if boot.Config.Auth.Username == user && boot.Config.Auth.Password == pass {
			return ctx, nil
		}

		return ctx, twirp.NewError(twirp.Unauthenticated, "invalid username/password for authentication")
	}

	return hooks
}

func WithAuth(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		authUser, authPass, _ := r.BasicAuth()
		ctx = context.WithValue(ctx, authUserCtxKey, authUser)
		ctx = context.WithValue(ctx, authPassCtxKey, authPass)

		r = r.WithContext(ctx)
		h.ServeHTTP(w, r)
	})
}
