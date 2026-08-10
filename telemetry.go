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

// MetricStoreQueryDuration is the OpenTelemetry histogram of optional
// application-wrapped store query durations in seconds. Its count reports
// completed store call throughput.
const MetricStoreQueryDuration = "pgmesh.query.store.duration"

// MetricLogicalQueryDuration is the OpenTelemetry histogram of logical
// generated-query durations in seconds. Fan-out work contributes one data
// point.
const MetricLogicalQueryDuration = "pgmesh.query.logical.duration"

// MetricPhysicalQueryDuration is the OpenTelemetry histogram of physical
// database-query durations in seconds. Its count reports per-node query
// throughput. The configured MeterProvider owns exporting and shutdown;
// pgmesh never shuts it down.
const MetricPhysicalQueryDuration = "pgmesh.query.physical.duration"

// MetricPhysicalQueryInFlight is the OpenTelemetry up-down counter of
// physical database queries currently in flight. It is grouped by the same
// bounded query and target attributes as MetricPhysicalQueryDuration.
const MetricPhysicalQueryInFlight = "pgmesh.query.physical.inflight"

// MetricCopyBatchRows is the OpenTelemetry histogram of attempted rows per
// physical COPY operation.
const MetricCopyBatchRows = "pgmesh.copy.batch.rows"

// MetricCopyBatchSubmissions is the OpenTelemetry histogram of logical
// submission fragments represented in each physical COPY operation.
const MetricCopyBatchSubmissions = "pgmesh.copy.batch.submissions"

// MetricCopyBatchFlushes counts physical COPY operations by flush reason.
const MetricCopyBatchFlushes = "pgmesh.copy.batch.flushes"

// MetricCopyBatchDuration is the OpenTelemetry histogram of physical COPY
// execution durations in seconds.
const MetricCopyBatchDuration = "pgmesh.copy.batch.duration"

// MetricCopyBatchQueueDuration is the OpenTelemetry histogram of time from the
// oldest row's admission until physical COPY execution begins, in seconds.
const MetricCopyBatchQueueDuration = "pgmesh.copy.batch.queue.duration"

// OpenTelemetry attribute keys recorded on store and routed query telemetry.
const (
	// AttributeStoreName identifies the generated store query group.
	AttributeStoreName = "pgmesh.store.name"
	// AttributeQueryName identifies the generated query method.
	AttributeQueryName = "pgmesh.query.name"
	// AttributeQueryKind identifies whether a routed query is a read or write.
	AttributeQueryKind = "pgmesh.query.kind"
	// AttributeReplicaSetName identifies the selected replica set.
	AttributeReplicaSetName = "pgmesh.replica_set.name"
	// AttributeVirtualShardIndex identifies the selected virtual shard when one
	// exact virtual shard can be attributed to an execution.
	AttributeVirtualShardIndex = "pgmesh.virtual_shard.index"
	// AttributeNodeName identifies a stable node within a replica set.
	AttributeNodeName = "pgmesh.node.name"
	// AttributeNodeRole identifies whether the node is a primary or read replica.
	AttributeNodeRole = "pgmesh.node.role"
	// AttributeRouteMode identifies the database path selected for a query.
	AttributeRouteMode = "pgmesh.route.mode"
	// AttributeRouteScope identifies single-replica-set versus fan-out operations.
	AttributeRouteScope = "pgmesh.route.scope"
	// AttributeRouteReplicaSetCount reports the number of replica sets
	// selected on spans and logs. It is excluded from metrics to bound cardinality.
	AttributeRouteReplicaSetCount = "pgmesh.route.replica_set_count"
	// AttributeStoreDelegated reports whether an application-wrapped store call
	// delegated to the generated logical query implementation.
	AttributeStoreDelegated = "pgmesh.store.delegated"
	// AttributeCopyBatchFlushReason identifies why a physical COPY batch was
	// made ready for execution.
	AttributeCopyBatchFlushReason = "pgmesh.copy.batch.flush_reason"
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

// Route modes recorded after a query resolves to a database path.
const (
	// RouteModeRead indicates a read routed through the replica load balancer.
	RouteModeRead RouteMode = "read"
	// RouteModePrimary indicates a read or write routed directly to the primary.
	RouteModePrimary RouteMode = "primary"
	// RouteModeTransaction indicates a query executed on an explicit transaction.
	RouteModeTransaction RouteMode = "transaction"
	// RouteModeUnresolved indicates that routing failed before selecting a path.
	RouteModeUnresolved RouteMode = "unresolved"
)

// RouteScope classifies the fan-out of one logical operation.
type RouteScope string

const (
	// RouteScopeSingle identifies an operation targeting at most one replica set.
	RouteScopeSingle RouteScope = "single"
	// RouteScopeFanout identifies an operation targeting multiple replica sets.
	RouteScopeFanout RouteScope = "fanout"
	// RouteScopeUnresolved indicates that routing did not resolve a scope.
	RouteScopeUnresolved RouteScope = "unresolved"
)

type meshTelemetry struct {
	tracer                 trace.Tracer
	storeDuration          metric.Float64Histogram
	logicalDuration        metric.Float64Histogram
	physicalDuration       metric.Float64Histogram
	physicalInFlight       metric.Int64UpDownCounter
	copyBatchRows          metric.Int64Histogram
	copyBatchSubmissions   metric.Int64Histogram
	copyBatchFlushes       metric.Int64Counter
	copyBatchDuration      metric.Float64Histogram
	copyBatchQueueDuration metric.Float64Histogram
	logger                 *slog.Logger
}

// LogicalQuerySpan records tracing, metrics, and logging for one routed query.
// The generated store calls End exactly once; direct callers must do the same.
type LogicalQuerySpan struct {
	ctx              context.Context
	span             trace.Span
	tracer           trace.Tracer
	logicalDuration  metric.Float64Histogram
	physicalDuration metric.Float64Histogram
	physicalInFlight metric.Int64UpDownCounter
	started          time.Time
	attributes       []attribute.KeyValue
	storeName        string
	queryName        string
	kind             QueryKind
	routeMode        RouteMode
	routeScope       RouteScope
	replicaSetCount  int
	logger           *slog.Logger
	logAttributes    []slog.Attr
}

// PhysicalQuerySpan records one database execution on one resolved node.
type PhysicalQuerySpan struct {
	ctx              context.Context
	span             trace.Span
	physicalDuration metric.Float64Histogram
	physicalInFlight metric.Int64UpDownCounter
	started          time.Time
	metricAttributes []attribute.KeyValue
	logger           *slog.Logger
	logAttributes    []slog.Attr
}

// StoreQuerySpan records tracing, metrics, and logging around one
// factory-wrapped generated store method. The wrapper calls End exactly once.
type StoreQuerySpan struct {
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

func newMeshTelemetry(
	tracerProvider trace.TracerProvider,
	meterProvider metric.MeterProvider,
) (meshTelemetry, error) {
	var telemetry meshTelemetry
	telemetry.setTracerProvider(tracerProvider)
	if err := telemetry.setMeterProvider(meterProvider); err != nil {
		return meshTelemetry{}, err
	}
	return telemetry, nil
}

func (t *meshTelemetry) setTracerProvider(provider trace.TracerProvider) {
	if provider == nil {
		provider = otel.GetTracerProvider()
	}
	t.tracer = provider.Tracer(
		instrumentationName,
		trace.WithSchemaURL(semconv.SchemaURL),
	)
}

func (t *meshTelemetry) setMeterProvider(provider metric.MeterProvider) error {
	if provider == nil {
		provider = otel.GetMeterProvider()
	}
	meter := provider.Meter(
		instrumentationName,
		metric.WithSchemaURL(semconv.SchemaURL),
	)
	logicalDuration, err := meter.Float64Histogram(
		MetricLogicalQueryDuration,
		metric.WithDescription("Duration of logical pgmesh generated-query calls"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.001, 0.005, 0.01, 0.03, 0.05, 0.075, 0.1, 0.125, 0.25, 0.5, 0.75, 1, 3, 5, 10,
		),
	)
	if err != nil {
		return err
	}
	t.logicalDuration = logicalDuration
	physicalDuration, err := meter.Float64Histogram(
		MetricPhysicalQueryDuration,
		metric.WithDescription("Duration of physical pgmesh database queries"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.001, 0.005, 0.01, 0.03, 0.05, 0.075, 0.1, 0.125, 0.25, 0.5, 0.75, 1, 3, 5, 10,
		),
	)
	if err != nil {
		return err
	}
	t.physicalDuration = physicalDuration
	physicalInFlight, err := meter.Int64UpDownCounter(
		MetricPhysicalQueryInFlight,
		metric.WithDescription("Physical pgmesh database queries currently in flight"),
		metric.WithUnit("{query}"),
	)
	if err != nil {
		return err
	}
	t.physicalInFlight = physicalInFlight
	storeDuration, err := meter.Float64Histogram(
		MetricStoreQueryDuration,
		metric.WithDescription("Duration of application-wrapped pgmesh query methods"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.001, 0.005, 0.01, 0.03, 0.05, 0.075, 0.1, 0.125, 0.25, 0.5, 0.75, 1, 3, 5, 10,
		),
	)
	if err != nil {
		return err
	}
	t.storeDuration = storeDuration
	copyBatchRows, err := meter.Int64Histogram(
		MetricCopyBatchRows,
		metric.WithDescription("Attempted rows per physical pgmesh COPY operation"),
		metric.WithUnit("{row}"),
		metric.WithExplicitBucketBoundaries(
			1, 2, 4, 5, 8, 12, 16, 20, 24, 28, 32, 36, 64, 128, 256, 512,
		),
	)
	if err != nil {
		return err
	}
	t.copyBatchRows = copyBatchRows
	copyBatchSubmissions, err := meter.Int64Histogram(
		MetricCopyBatchSubmissions,
		metric.WithDescription("Logical submission fragments per physical pgmesh COPY operation"),
		metric.WithUnit("{submission}"),
		metric.WithExplicitBucketBoundaries(
			1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 48, 64, 128, 256,
		),
	)
	if err != nil {
		return err
	}
	t.copyBatchSubmissions = copyBatchSubmissions
	copyBatchFlushes, err := meter.Int64Counter(
		MetricCopyBatchFlushes,
		metric.WithDescription("Physical pgmesh COPY operations by flush reason"),
		metric.WithUnit("{batch}"),
	)
	if err != nil {
		return err
	}
	t.copyBatchFlushes = copyBatchFlushes
	copyBatchDuration, err := meter.Float64Histogram(
		MetricCopyBatchDuration,
		metric.WithDescription("Duration of physical pgmesh COPY operations"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.001, 0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.1, 0.2, 0.3, 0.4, 0.5, 1, 5, 10,
		),
	)
	if err != nil {
		return err
	}
	t.copyBatchDuration = copyBatchDuration
	copyBatchQueueDuration, err := meter.Float64Histogram(
		MetricCopyBatchQueueDuration,
		metric.WithDescription("Time from oldest row admission until physical pgmesh COPY execution"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.0001, 0.0005, 0.001, 0.003, 0.004, 0.005, 0.01, 0.03, 0.05, 0.075, 0.1, 0.5, 1, 5,
		),
	)
	if err != nil {
		return err
	}
	t.copyBatchQueueDuration = copyBatchQueueDuration
	return nil
}

// NewCopyBatchObserver returns an observer used by generated stores to record
// one metric point for every physical COPY operation on a replica set.
func (m *Mesh[R, W, K]) NewCopyBatchObserver(
	storeName string,
	queryName string,
	route RouteMetadata,
) CopyBatchObserver {
	baseAttributes := []attribute.KeyValue{
		attribute.String(AttributeStoreName, storeName),
		attribute.String(AttributeQueryName, queryName),
		attribute.String(AttributeQueryKind, string(QueryKindWrite)),
		attribute.String(AttributeReplicaSetName, route.ReplicaSetName),
		attribute.String(AttributeNodeName, route.NodeName),
		attribute.String(AttributeNodeRole, string(route.NodeRole)),
		attribute.String(AttributeRouteMode, string(RouteModePrimary)),
	}
	if route.HasVirtualShard {
		baseAttributes = append(
			baseAttributes,
			attribute.String(AttributeVirtualShardIndex, strconv.FormatUint(route.VirtualShardIndex, 10)),
		)
	}
	return func(ctx context.Context, observation CopyBatchObservation) {
		attributes := append(
			append([]attribute.KeyValue(nil), baseAttributes...),
			attribute.String(
				AttributeCopyBatchFlushReason,
				string(observation.Reason),
			),
		)
		if observation.Err != nil {
			attributes = append(attributes, semconv.ErrorType(observation.Err))
		}
		recordOptions := metric.WithAttributes(attributes...)
		m.telemetry.copyBatchRows.Record(
			ctx,
			int64(observation.RowCount),
			recordOptions,
		)
		m.telemetry.copyBatchSubmissions.Record(
			ctx,
			int64(observation.SubmissionCount),
			recordOptions,
		)
		m.telemetry.copyBatchFlushes.Add(ctx, 1, recordOptions)
		m.telemetry.copyBatchDuration.Record(
			ctx,
			observation.ExecutionDuration.Seconds(),
			recordOptions,
		)
		m.telemetry.copyBatchQueueDuration.Record(
			ctx,
			observation.QueueDuration.Seconds(),
			recordOptions,
		)
	}
}

// StartStoreQuerySpan starts telemetry around a factory-wrapped generated store
// method. Generated internal methods mark the returned event through ctx while
// creating their own child query spans.
//
//nolint:spancheck // The generated caller ends the returned StoreQuerySpan.
func (m *Mesh[R, W, K]) StartStoreQuerySpan(
	ctx context.Context,
	storeName string,
	queryName string,
	kind QueryKind,
) (context.Context, *StoreQuerySpan) {
	attributes := []attribute.KeyValue{
		attribute.String(AttributeStoreName, storeName),
		attribute.String(AttributeQueryName, queryName),
		attribute.String(AttributeQueryKind, string(kind)),
	}
	ctx, span := m.telemetry.tracer.Start(
		ctx,
		"pgmesh.query.store."+storeName+"."+queryName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attributes...),
	)
	execution := &internalExecutionState{
		owner:    m,
		executed: atomic.Bool{},
	}
	ctx = context.WithValue(ctx, internalExecutionContextKey{}, execution)
	return ctx, &StoreQuerySpan{
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

// StartLogicalQuerySpan starts telemetry for one logical generated operation.
// Physical executions are recorded as child spans through
// StartPhysicalQuerySpan.
//
//nolint:spancheck // The generated caller ends the returned LogicalQuerySpan.
func (m *Mesh[R, W, K]) StartLogicalQuerySpan(
	ctx context.Context,
	storeName string,
	queryName string,
	kind QueryKind,
) (context.Context, *LogicalQuerySpan) {
	if execution, ok := ctx.Value(internalExecutionContextKey{}).(*internalExecutionState); ok &&
		execution.owner == m {
		execution.executed.Store(true)
	}
	attributes := []attribute.KeyValue{
		attribute.String(AttributeStoreName, storeName),
		attribute.String(AttributeQueryName, queryName),
		attribute.String(AttributeQueryKind, string(kind)),
		attribute.String(AttributeRouteMode, string(RouteModeUnresolved)),
		attribute.String(AttributeRouteScope, string(RouteScopeUnresolved)),
	}
	ctx, span := m.telemetry.tracer.Start(
		ctx,
		"pgmesh.query.logical."+storeName+"."+queryName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attributes...),
	)
	return ctx, &LogicalQuerySpan{
		ctx:              ctx,
		span:             span,
		tracer:           m.telemetry.tracer,
		logicalDuration:  m.telemetry.logicalDuration,
		physicalDuration: m.telemetry.physicalDuration,
		physicalInFlight: m.telemetry.physicalInFlight,
		started:          time.Now(),
		attributes:       attributes[:3],
		storeName:        storeName,
		queryName:        queryName,
		kind:             kind,
		routeMode:        RouteModeUnresolved,
		routeScope:       RouteScopeUnresolved,
		replicaSetCount:  0,
		logger:           m.telemetry.logger,
		logAttributes: []slog.Attr{
			slog.String("store_name", storeName),
			slog.String("query_name", queryName),
			slog.String("query_kind", string(kind)),
		},
	}
}

// SetRoute records the route mode and number of resolved replica sets.
// A count of zero or one has single-route scope; larger counts have fan-out
// scope.
func (s *LogicalQuerySpan) SetRoute(mode RouteMode, replicaSetCount int) {
	scope := RouteScopeSingle
	if replicaSetCount > 1 {
		scope = RouteScopeFanout
	}
	s.routeMode = mode
	s.routeScope = scope
	s.replicaSetCount = replicaSetCount
	routeAttributes := []attribute.KeyValue{
		attribute.String(AttributeRouteMode, string(mode)),
		attribute.String(AttributeRouteScope, string(scope)),
		attribute.Int(AttributeRouteReplicaSetCount, replicaSetCount),
	}
	s.span.SetAttributes(routeAttributes...)
}

// StartPhysicalQuerySpan starts one physical database-query child of this
// operation. The returned context must be passed to the selected route's
// Target.
func (s *LogicalQuerySpan) StartPhysicalQuerySpan(
	ctx context.Context,
	route RouteMetadata,
	mode RouteMode,
) (context.Context, *PhysicalQuerySpan) {
	return startPhysicalQuerySpan(
		ctx,
		s.tracer,
		s.physicalDuration,
		s.physicalInFlight,
		s.logger,
		s.storeName,
		s.queryName,
		s.kind,
		route,
		mode,
	)
}

// StartPhysicalQuerySpan starts a physical database-query span without a
// LogicalQuerySpan. It is used for asynchronous COPY batches.
func (m *Mesh[R, W, K]) StartPhysicalQuerySpan(
	ctx context.Context,
	storeName string,
	queryName string,
	kind QueryKind,
	route RouteMetadata,
	mode RouteMode,
) (context.Context, *PhysicalQuerySpan) {
	return startPhysicalQuerySpan(
		ctx,
		m.telemetry.tracer,
		m.telemetry.physicalDuration,
		m.telemetry.physicalInFlight,
		m.telemetry.logger,
		storeName,
		queryName,
		kind,
		route,
		mode,
	)
}

//nolint:spancheck // The generated caller ends the returned PhysicalQuerySpan.
func startPhysicalQuerySpan(
	ctx context.Context,
	tracer trace.Tracer,
	physicalDuration metric.Float64Histogram,
	physicalInFlight metric.Int64UpDownCounter,
	logger *slog.Logger,
	storeName string,
	queryName string,
	kind QueryKind,
	route RouteMetadata,
	mode RouteMode,
) (context.Context, *PhysicalQuerySpan) {
	node := route.NodeName
	role := route.NodeRole
	if mode == RouteModeTransaction {
		node = "transaction"
		role = NodeRoleTransaction
	}
	metricAttributes := []attribute.KeyValue{
		attribute.String(AttributeStoreName, storeName),
		attribute.String(AttributeQueryName, queryName),
		attribute.String(AttributeQueryKind, string(kind)),
		attribute.String(AttributeReplicaSetName, route.ReplicaSetName),
		attribute.String(AttributeNodeName, node),
		attribute.String(AttributeNodeRole, string(role)),
		attribute.String(AttributeRouteMode, string(mode)),
	}
	if route.HasVirtualShard {
		metricAttributes = append(
			metricAttributes,
			attribute.String(AttributeVirtualShardIndex, strconv.FormatUint(route.VirtualShardIndex, 10)),
		)
	}
	spanAttributes := append([]attribute.KeyValue(nil), metricAttributes...)
	logAttributes := []slog.Attr{
		slog.String("store_name", storeName),
		slog.String("query_name", queryName),
		slog.String("query_kind", string(kind)),
		slog.String("replica_set", route.ReplicaSetName),
		slog.String("node", node),
		slog.String("node_role", string(role)),
		slog.String("route_mode", string(mode)),
	}
	if route.HasVirtualShard {
		logAttributes = append(
			logAttributes,
			slog.String("virtual_shard", strconv.FormatUint(route.VirtualShardIndex, 10)),
		)
	}
	ctx, span := tracer.Start(
		ctx,
		"pgmesh.query.physical."+storeName+"."+queryName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(spanAttributes...),
	)
	physicalInFlight.Add(
		ctx,
		1,
		metric.WithAttributes(metricAttributes...),
	)
	return ctx, &PhysicalQuerySpan{
		ctx:              ctx,
		span:             span,
		physicalDuration: physicalDuration,
		physicalInFlight: physicalInFlight,
		started:          time.Now(),
		metricAttributes: metricAttributes,
		logger:           logger,
		logAttributes:    logAttributes,
	}
}

// End records the logical operation metric and span.
func (s *LogicalQuerySpan) End(err error) {
	duration := time.Since(s.started)
	metricAttributes := append(
		append([]attribute.KeyValue(nil), s.attributes...),
		attribute.String(AttributeRouteMode, string(s.routeMode)),
		attribute.String(AttributeRouteScope, string(s.routeScope)),
	)
	if err != nil {
		errorType := semconv.ErrorType(err)
		s.span.RecordError(err)
		s.span.SetAttributes(errorType)
		s.span.SetStatus(codes.Error, err.Error())
		metricAttributes = append(metricAttributes, errorType)
	}
	recordOptions := metric.WithAttributes(metricAttributes...)
	s.logicalDuration.Record(s.ctx, duration.Seconds(), recordOptions)
	if s.logger != nil && s.logger.Enabled(s.ctx, slog.LevelDebug) {
		logAttributes := append(
			append([]slog.Attr(nil), s.logAttributes...),
			slog.String("route_mode", string(s.routeMode)),
			slog.String("route_scope", string(s.routeScope)),
			slog.Int("replica_set_count", s.replicaSetCount),
			slog.Bool("failed", err != nil),
			slog.Duration("duration", duration),
		)
		if err != nil {
			logAttributes = append(logAttributes, slog.Any("error", err))
		}
		s.logger.LogAttrs(s.ctx, slog.LevelDebug, "pgmesh logical query completed", logAttributes...)
	}
	s.span.End()
}

// End records one physical database-query metric and span.
func (s *PhysicalQuerySpan) End(err error) {
	duration := time.Since(s.started)
	s.physicalInFlight.Add(
		s.ctx,
		-1,
		metric.WithAttributes(s.metricAttributes...),
	)
	metricAttributes := append([]attribute.KeyValue(nil), s.metricAttributes...)
	if err != nil {
		errorType := semconv.ErrorType(err)
		s.span.RecordError(err)
		s.span.SetAttributes(errorType)
		s.span.SetStatus(codes.Error, err.Error())
		metricAttributes = append(metricAttributes, errorType)
	}
	s.physicalDuration.Record(
		s.ctx,
		duration.Seconds(),
		metric.WithAttributes(metricAttributes...),
	)
	if s.logger != nil && s.logger.Enabled(s.ctx, slog.LevelDebug) {
		logAttributes := append(
			append([]slog.Attr(nil), s.logAttributes...),
			slog.Bool("failed", err != nil),
			slog.Duration("duration", duration),
		)
		if err != nil {
			logAttributes = append(logAttributes, slog.Any("error", err))
		}
		s.logger.LogAttrs(s.ctx, slog.LevelDebug, "pgmesh physical query completed", logAttributes...)
	}
	s.span.End()
}

// End records metrics and a debug log, records err if present, then ends the
// factory-wrapped store span. The configured providers and logger remain
// caller-owned.
func (s *StoreQuerySpan) End(err error) {
	duration := time.Since(s.started)
	delegated := s.execution.executed.Load()
	delegatedAttribute := attribute.Bool(
		AttributeStoreDelegated,
		delegated,
	)
	s.span.SetAttributes(delegatedAttribute)
	metricAttributes := append(
		append([]attribute.KeyValue(nil), s.attributes...),
		delegatedAttribute,
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
			slog.Bool("store_delegated", delegated),
			slog.Bool("failed", err != nil),
			slog.Duration("duration", duration),
		)
		if err != nil {
			logAttributes = append(logAttributes, slog.Any("error", err))
		}
		s.logger.LogAttrs(s.ctx, slog.LevelDebug, "pgmesh store query completed", logAttributes...)
	}
	s.span.End()
}
