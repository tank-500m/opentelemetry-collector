// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package memorylimiterextension // import "go.opentelemetry.io/collector/extension/memorylimiterextension"

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/extensionmiddleware"
	"go.opentelemetry.io/collector/extension/memorylimiterextension/internal/metadata"
	"go.opentelemetry.io/collector/internal/memorylimiter"
)

var (
	httpTransportAttrs = attribute.NewSet(
		attribute.String("transport", "http"),
	)
	grpcTransportAttrs = attribute.NewSet(
		attribute.String("transport", "grpc"),
	)
)

var (
	_ extensionmiddleware.GRPCServer = (*memoryLimiterExtension)(nil)
	_ extensionmiddleware.HTTPServer = (*memoryLimiterExtension)(nil)
)

type memoryLimiterExtension struct {
	memLimiter       *memorylimiter.MemoryLimiter
	telemetryBuilder *metadata.TelemetryBuilder
	// grpcRefusedErr is returned for refused gRPC requests.
	grpcRefusedErr error
}

// newMemoryLimiter returns a new memorylimiter extension.
func newMemoryLimiter(cfg *Config, logger *zap.Logger, telemetrySettings component.TelemetrySettings) (*memoryLimiterExtension, error) {
	// OTLP clients retry RESOURCE_EXHAUSTED only if the status includes RetryInfo, see
	// https://opentelemetry.io/docs/specs/otlp/#failures. No retry delay is set, so
	// clients retry with their own backoff.
	grpcRefusedStatus, err := status.New(codes.ResourceExhausted, "RESOURCE_EXHAUSTED").
		WithDetails(&errdetails.RetryInfo{})
	if err != nil {
		return nil, err
	}

	telemetryBuilder, err := metadata.NewTelemetryBuilder(telemetrySettings)
	if err != nil {
		return nil, err
	}

	ml, err := memorylimiter.NewMemoryLimiter(cfg, logger)
	if err != nil {
		telemetryBuilder.Shutdown()
		return nil, err
	}

	return &memoryLimiterExtension{
		memLimiter:       ml,
		telemetryBuilder: telemetryBuilder,
		grpcRefusedErr:   grpcRefusedStatus.Err(),
	}, nil
}

func (ml *memoryLimiterExtension) Start(ctx context.Context, host component.Host) error {
	return ml.memLimiter.Start(ctx, host)
}

func (ml *memoryLimiterExtension) Shutdown(ctx context.Context) error {
	err := ml.memLimiter.Shutdown(ctx)
	ml.telemetryBuilder.Shutdown()
	return err
}

// MustRefuse returns if the caller should deny because memory has reached it's configured limits
func (ml *memoryLimiterExtension) MustRefuse() bool {
	return ml.memLimiter.MustRefuse()
}

// GetHTTPHandler implements extensionmiddleware.HTTPServer
func (ml *memoryLimiterExtension) GetHTTPHandler(_ context.Context) (extensionmiddleware.WrapHTTPHandlerFunc, error) {
	return ml.wrapHTTPHandler, nil
}

func (ml *memoryLimiterExtension) wrapHTTPHandler(_ context.Context, base http.Handler) (http.Handler, error) {
	return http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		if ml.MustRefuse() {
			ml.telemetryBuilder.MemorylimiterRefusedRequests.Add(
				req.Context(),
				1,
				metric.WithAttributeSet(httpTransportAttrs),
			)
			http.Error(resp, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
			return
		}
		base.ServeHTTP(resp, req)
	}), nil
}

func (ml *memoryLimiterExtension) GetGRPCServerOptions(_ context.Context) ([]grpc.ServerOption, error) {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(ml.grpcUnaryInterceptor),
		grpc.ChainStreamInterceptor(ml.grpcStreamInterceptor),
	}, nil
}

func (ml *memoryLimiterExtension) grpcUnaryInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if ml.MustRefuse() {
		ml.telemetryBuilder.MemorylimiterRefusedRequests.Add(
			ctx,
			1,
			metric.WithAttributeSet(grpcTransportAttrs),
		)
		return nil, ml.grpcRefusedErr
	}

	return handler(ctx, req)
}

func (ml *memoryLimiterExtension) grpcStreamInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()

	if ml.MustRefuse() {
		ml.telemetryBuilder.MemorylimiterRefusedRequests.Add(
			ctx,
			1,
			metric.WithAttributeSet(grpcTransportAttrs),
		)
		return ml.grpcRefusedErr
	}

	return handler(srv, ss)
}
