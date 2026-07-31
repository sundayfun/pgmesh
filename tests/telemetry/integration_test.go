//go:build integration

package telemetry

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/sundayfun/pgmesh"
	"github.com/sundayfun/pgmesh/tests/internal/storetest"
	fixture "github.com/sundayfun/pgmesh/tests/same_package"
)

type cachingUsers struct {
	fixture.Users

	mu   sync.Mutex
	user *fixture.User
}

func (s *cachingUsers) GetUser(
	ctx context.Context,
	arg *fixture.GetUserT,
	options ...fixture.QueryOption,
) (*fixture.User, error) {
	if len(options) != 0 {
		return s.Users.GetUser(ctx, arg, options...)
	}

	s.mu.Lock()
	user := s.user
	s.mu.Unlock()
	if user != nil {
		return user, nil
	}

	user, err := s.Users.GetUser(ctx, arg, fixture.ReadFromPrimary())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.user = user
	s.mu.Unlock()
	return user, nil
}

func TestPostgresQueryGroupFactoryTelemetryHierarchy(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	harness.Insert(t, "shard0-primary", 91, 2, "telemetry")

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	queries := harness.NewShardedStore(
		t,
		fixture.WithTracerProvider(tracerProvider),
		fixture.WithUsersFactory(func(internalStore fixture.Users) fixture.Users {
			return &cachingUsers{
				Users: internalStore,
				mu:    sync.Mutex{},
				user:  nil,
			}
		}),
	)
	arg := &fixture.GetUserT{TenantKey: storetest.TenantKey(2), ID: 91}

	_, err := queries.Users().GetUser(t.Context(), arg)
	require.NoError(t, err)
	_, err = queries.Users().GetUser(t.Context(), arg)
	require.NoError(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 4)
	wrapperSpans := make([]sdktrace.ReadOnlySpan, 0, 2)
	logicalSpans := make([]sdktrace.ReadOnlySpan, 0, 1)
	physicalSpans := make([]sdktrace.ReadOnlySpan, 0, 1)
	for _, span := range spans {
		switch {
		case strings.HasPrefix(span.Name(), "pgmesh.query.wrapper."):
			wrapperSpans = append(wrapperSpans, span)
		case strings.HasPrefix(span.Name(), "pgmesh.query.logical."):
			logicalSpans = append(logicalSpans, span)
		case strings.HasPrefix(span.Name(), "pgmesh.query.physical."):
			physicalSpans = append(physicalSpans, span)
		default:
			require.Failf(t, "unexpected span", "%s", span.Name())
		}
	}
	require.Len(t, wrapperSpans, 2)
	require.Len(t, logicalSpans, 1)
	require.Len(t, physicalSpans, 1)

	delegatedWrapperID := ""
	delegated := make([]bool, 0, len(wrapperSpans))
	for _, span := range wrapperSpans {
		attributes := spanAttributeMap(span)
		executed, ok := attributes[pgmesh.AttributeWrapperDelegated].(bool)
		require.True(t, ok)
		delegated = append(delegated, executed)
		if executed {
			delegatedWrapperID = span.SpanContext().SpanID().String()
		}
	}
	assert.Equal(t, []bool{true, false}, delegated)
	assert.Equal(t, delegatedWrapperID, logicalSpans[0].Parent().SpanID().String())
	assert.Equal(t, logicalSpans[0].SpanContext().SpanID(), physicalSpans[0].Parent().SpanID())
	assert.NotContains(t, spanAttributeMap(physicalSpans[0]), pgmesh.AttributeWrapperDelegated)
}

func spanAttributeMap(span sdktrace.ReadOnlySpan) map[string]any {
	attributes := make(map[string]any, len(span.Attributes()))
	for _, item := range span.Attributes() {
		attributes[string(item.Key)] = item.Value.AsInterface()
	}
	return attributes
}
