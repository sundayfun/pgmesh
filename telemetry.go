package pgmesh

import (
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/sundayfun/pgmesh"

// MetricQueryDuration is the OpenTelemetry histogram of routed query durations
// in seconds. Its count also reports completed query throughput. The configured
// MeterProvider owns exporting and shutdown; pgmesh never shuts it down.
const MetricQueryDuration = "pgmesh.query.duration"

// MetricStoreDuration is the OpenTelemetry histogram of factory-wrapped store
// method durations in seconds. Its count reports completed wrapper throughput.
const MetricStoreDuration = "pgmesh.store.duration"

// OpenTelemetry attribute keys recorded on store and routed query telemetry.
const (
	// AttributeStoreName identifies the generated store query group.
	AttributeStoreName = "pgmesh.store.name"
	// AttributeQueryName identifies the generated query method.
	AttributeQueryName = "pgmesh.query.name"
	// AttributeQueryKind identifies whether a routed query is a read or write.
	AttributeQueryKind = "pgmesh.query.kind"
	// AttributeReplicaSet identifies the selected physical replica set.
	AttributeReplicaSet = "pgmesh.route.replica_set"
	// AttributeRouteMode identifies the database path selected for a query.
	AttributeRouteMode = "pgmesh.route.mode"
	// AttributeInternalStoreExecuted reports whether a factory wrapper entered
	// the generated internal store implementation.
	AttributeInternalStoreExecuted = "pgmesh.store.internal_executed"
)

// QueryKind classifies a routed query as a read or write.
type QueryKind string

// Query kinds recorded by generated routed query methods.
const (
	// QueryKindRead identifies a read query.
	QueryKindRead QueryKind = "read"
	// QueryKindWrite identifies a write query.
	QueryKindWrite QueryKind = "write"
)

// RouteMode describes the database path selected for a routed query.
type RouteMode string

// Route modes recorded after a query resolves to a shard.
const (
	// RouteModeRead indicates a read routed through the replica load balancer.
	RouteModeRead RouteMode = "read"
	// RouteModePrimary indicates a read or write routed directly to the primary.
	RouteModePrimary RouteMode = "primary"
	// RouteModeTransaction indicates a query executed on an explicit transaction.
	RouteModeTransaction RouteMode = "transaction"
)

type queryTelemetry struct {
	tracer        trace.Tracer
	storeDuration metric.Float64Histogram
	queryDuration metric.Float64Histogram
	logger        *slog.Logger
}

// QuerySpan records tracing, metrics, and logging for one routed query. The
// generated store calls End exactly once; callers using StartSpan directly must
// do the same.
type QuerySpan struct {
	ctx           context.Context
	span          trace.Span
	queryDuration metric.Float64Histogram
	started       time.Time
	attributes    []attribute.KeyValue
	logger        *slog.Logger
	logAttributes []slog.Attr
}

// StoreSpan records tracing, metrics, and logging around one factory-wrapped
// generated store method. The generated wrapper calls End exactly once.
type StoreSpan struct {
	ctx           context.Context
	span          trace.Span
	storeDuration metric.Float64Histogram
	started       time.Time
	attributes    []attribute.KeyValue
	logger        *slog.Logger
	logAttributes []slog.Attr
	execution     *internalExecutionState
}

type internalExecutionState struct {
	owner    any
	executed atomic.Bool
}

type internalExecutionContextKey struct{}

func newQueryTelemetry(
	tracerProvider trace.TracerProvider,
	meterProvider metric.MeterProvider,
) (queryTelemetry, error) {
	var telemetry queryTelemetry
	telemetry.setTracerProvider(tracerProvider)
	if err := telemetry.setMeterProvider(meterProvider); err != nil {
		return queryTelemetry{}, err
	}
	return telemetry, nil
}

func (t *queryTelemetry) setTracerProvider(provider trace.TracerProvider) {
	if provider == nil {
		provider = otel.GetTracerProvider()
	}
	t.tracer = provider.Tracer(
		instrumentationName,
		trace.WithSchemaURL(semconv.SchemaURL),
	)
}

func (t *queryTelemetry) setMeterProvider(provider metric.MeterProvider) error {
	if provider == nil {
		provider = otel.GetMeterProvider()
	}
	meter := provider.Meter(
		instrumentationName,
		metric.WithSchemaURL(semconv.SchemaURL),
	)
	queryDuration, err := meter.Float64Histogram(
		MetricQueryDuration,
		metric.WithDescription("Duration of routed pgmesh queries"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10,
		),
	)
	if err != nil {
		return err
	}
	t.queryDuration = queryDuration
	storeDuration, err := meter.Float64Histogram(
		MetricStoreDuration,
		metric.WithDescription("Duration of factory-wrapped pgmesh store methods"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10,
		),
	)
	if err != nil {
		return err
	}
	t.storeDuration = storeDuration
	return nil
}

// StartStoreSpan starts telemetry around a factory-wrapped generated store
// method. Generated internal methods mark the returned event through ctx while
// creating their own child query spans.
//
//nolint:spancheck // The generated caller ends the returned StoreSpan.
func (m *Mesh[R, W, SK]) StartStoreSpan(
	ctx context.Context,
	storeName string,
	queryName string,
	kind QueryKind,
) (context.Context, *StoreSpan) {
	attributes := []attribute.KeyValue{
		attribute.String(AttributeStoreName, storeName),
		attribute.String(AttributeQueryName, queryName),
		attribute.String(AttributeQueryKind, string(kind)),
	}
	ctx, span := m.telemetry.tracer.Start(
		ctx,
		"pgmesh.store."+storeName+"."+queryName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attributes...),
	)
	execution := &internalExecutionState{
		owner:    m,
		executed: atomic.Bool{},
	}
	ctx = context.WithValue(ctx, internalExecutionContextKey{}, execution)
	return ctx, &StoreSpan{
		ctx:           ctx,
		span:          span,
		storeDuration: m.telemetry.storeDuration,
		started:       time.Now(),
		attributes:    attributes,
		logger:        m.telemetry.logger,
		logAttributes: []slog.Attr{
			slog.String("store_name", storeName),
			slog.String("query_name", queryName),
			slog.String("query_kind", string(kind)),
		},
		execution: execution,
	}
}

// StartSpan starts telemetry for a routed query and returns the span context so
// database instrumentation can create child spans.
//
//nolint:spancheck // The generated caller ends the returned QuerySpan.
func (m *Mesh[R, W, SK]) StartSpan(
	ctx context.Context,
	storeName string,
	queryName string,
	kind QueryKind,
) (context.Context, *QuerySpan) {
	if execution, ok := ctx.Value(internalExecutionContextKey{}).(*internalExecutionState); ok &&
		execution.owner == m {
		execution.executed.Store(true)
	}
	attributes := []attribute.KeyValue{
		attribute.String(AttributeStoreName, storeName),
		attribute.String(AttributeQueryName, queryName),
		attribute.String(AttributeQueryKind, string(kind)),
	}
	ctx, span := m.telemetry.tracer.Start(
		ctx,
		"pgmesh.query."+storeName+"."+queryName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attributes...),
	)
	return ctx, &QuerySpan{
		ctx:           ctx,
		span:          span,
		queryDuration: m.telemetry.queryDuration,
		started:       time.Now(),
		attributes:    attributes,
		logger:        m.telemetry.logger,
		logAttributes: []slog.Attr{
			slog.String("store_name", storeName),
			slog.String("query_name", queryName),
			slog.String("query_kind", string(kind)),
		},
	}
}

// SetRoute records the selected virtual shard for debug logging and the bounded
// physical-route attributes used by tracing and metrics.
func (s *QuerySpan) SetRoute(
	vshard uint64,
	replicaSet string,
	mode RouteMode,
) {
	routeAttributes := []attribute.KeyValue{
		attribute.String(AttributeReplicaSet, replicaSet),
		attribute.String(AttributeRouteMode, string(mode)),
	}
	s.span.SetAttributes(routeAttributes...)
	s.attributes = append(s.attributes, routeAttributes...)
	s.logAttributes = append(
		s.logAttributes,
		slog.String("vshard", strconv.FormatUint(vshard, 10)),
		slog.String("replica_set", replicaSet),
		slog.String("route_mode", string(mode)),
	)
}

// SetMultiRoute records the routing mode for one logical operation targeting
// zero or more physical replica sets. It deliberately omits a virtual-shard
// index and replica-set name because no single value represents the operation.
func (s *QuerySpan) SetMultiRoute(mode RouteMode) {
	routeAttributes := []attribute.KeyValue{
		attribute.String(AttributeRouteMode, string(mode)),
	}
	s.span.SetAttributes(routeAttributes...)
	s.attributes = append(s.attributes, routeAttributes...)
	s.logAttributes = append(
		s.logAttributes,
		slog.String("route_mode", string(mode)),
	)
}

// End records metrics and a debug log, records err if present, then ends the
// routed query span. The configured providers and logger remain caller-owned.
func (s *QuerySpan) End(err error) {
	duration := time.Since(s.started)
	metricAttributes := append([]attribute.KeyValue(nil), s.attributes...)
	if err != nil {
		errorType := semconv.ErrorType(err)
		s.span.RecordError(err)
		s.span.SetAttributes(errorType)
		s.span.SetStatus(codes.Error, err.Error())
		metricAttributes = append(metricAttributes, errorType)
	}
	recordOptions := metric.WithAttributes(metricAttributes...)
	s.queryDuration.Record(s.ctx, duration.Seconds(), recordOptions)
	if s.logger != nil && s.logger.Enabled(s.ctx, slog.LevelDebug) {
		logAttributes := append(
			append([]slog.Attr(nil), s.logAttributes...),
			slog.Bool("failed", err != nil),
			slog.Duration("duration", duration),
		)
		if err != nil {
			logAttributes = append(logAttributes, slog.Any("error", err))
		}
		s.logger.LogAttrs(s.ctx, slog.LevelDebug, "pgmesh query completed", logAttributes...)
	}
	s.span.End()
}

// End records metrics and a debug log, records err if present, then ends the
// factory-wrapped store span. The configured providers and logger remain
// caller-owned.
func (s *StoreSpan) End(err error) {
	duration := time.Since(s.started)
	internalStoreExecuted := s.execution.executed.Load()
	executionAttribute := attribute.Bool(
		AttributeInternalStoreExecuted,
		internalStoreExecuted,
	)
	s.span.SetAttributes(executionAttribute)
	metricAttributes := append(
		append([]attribute.KeyValue(nil), s.attributes...),
		executionAttribute,
	)
	if err != nil {
		errorType := semconv.ErrorType(err)
		s.span.RecordError(err)
		s.span.SetAttributes(errorType)
		s.span.SetStatus(codes.Error, err.Error())
		metricAttributes = append(metricAttributes, errorType)
	}
	recordOptions := metric.WithAttributes(metricAttributes...)
	s.storeDuration.Record(s.ctx, duration.Seconds(), recordOptions)
	if s.logger != nil && s.logger.Enabled(s.ctx, slog.LevelDebug) {
		logAttributes := append(
			append([]slog.Attr(nil), s.logAttributes...),
			slog.Bool("internal_store_executed", internalStoreExecuted),
			slog.Bool("failed", err != nil),
			slog.Duration("duration", duration),
		)
		if err != nil {
			logAttributes = append(logAttributes, slog.Any("error", err))
		}
		s.logger.LogAttrs(s.ctx, slog.LevelDebug, "pgmesh store completed", logAttributes...)
	}
	s.span.End()
}
