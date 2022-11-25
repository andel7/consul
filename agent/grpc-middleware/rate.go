package middleware

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"

	recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"

	"github.com/hashicorp/consul/agent/consul/rate"
)

// ServerRateLimiterMiddleware implements a ServerInHandle function to perform
// RPC rate limiting at the cheapest possible point (before the full request has
// been decoded).
func ServerRateLimiterMiddleware(limiter RateLimiter, panicHandler recovery.RecoveryHandlerFunc, logger Logger) tap.ServerInHandle {
	return func(ctx context.Context, info *tap.Info) (_ context.Context, retErr error) {
		// This function is called before unary and stream RPC interceptors, so we
		// must handle our own panics here.
		defer func() {
			if r := recover(); r != nil {
				retErr = panicHandler(r)
			}
		}()

		peer, ok := peer.FromContext(ctx)
		if !ok {
			// This should never happen!
			return ctx, status.Error(codes.Internal, "gRPC rate limit middleware unable to read peer")
		}

		operationType, ok := rpcRateLimitSpecs[info.FullMethodName]
		if !ok {
			logger.Warn("failed to determine which rate limit to apply to RPC", "rpc", info.FullMethodName)
		}

		err := limiter.Allow(rate.Operation{
			Name:       info.FullMethodName,
			SourceAddr: peer.Addr,
			Type:       operationType,
		})

		switch {
		case err == nil:
			return ctx, nil
		case errors.Is(err, rate.ErrRetryElsewhere):
			return ctx, status.Error(codes.ResourceExhausted, err.Error())
		default:
			return ctx, status.Error(codes.Unavailable, err.Error())
		}
	}
}

//go:generate mockery --name RateLimiter --inpackage
type RateLimiter interface {
	Allow(rate.Operation) error
}

// NullRateLimiter returns a RateLimiter that allows every operation.
func NullRateLimiter() RateLimiter {
	return nullRateLimiter{}
}

type nullRateLimiter struct{}

func (nullRateLimiter) Allow(rate.Operation) error { return nil }
