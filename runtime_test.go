package pgmesh_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/sundayfun/pgmesh"
)

type fakeWriter struct {
	name    string
	mirrors []*fakeWriter
}

type tenantID int64

type failingMeterProvider struct {
	metric.MeterProvider

	err error
}

func (p failingMeterProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return failingMeter{err: p.err}
}

type failingMeter struct {
	metric.Meter

	err error
}

func (m failingMeter) Float64Histogram(
	string,
	...metric.Float64HistogramOption,
) (metric.Float64Histogram, error) {
	return nil, m.err
}

func (w *fakeWriter) WithMirrors(mirrors ...*fakeWriter) *fakeWriter {
	return &fakeWriter{name: w.name, mirrors: append(append([]*fakeWriter(nil), w.mirrors...), mirrors...)}
}

func node(name string) pgmesh.Node[string, *fakeWriter] {
	return pgmesh.NewNode(name+"-read", &fakeWriter{name: name + "-write"})
}

func TestShardHashers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		hasher pgmesh.ShardHasher[uint64]
		key    uint64
		want   uint64
	}{
		{name: "constant ignores zero key", hasher: pgmesh.NewConstantShardHasher[uint64](7), key: 0, want: 7},
		{name: "constant ignores nonzero key", hasher: pgmesh.NewConstantShardHasher[uint64](7), key: 99, want: 7},
		{name: "modular zero", hasher: pgmesh.NewModuloShardHasher[uint64](4), key: 0, want: 0},
		{name: "modular in range", hasher: pgmesh.NewModuloShardHasher[uint64](4), key: 3, want: 3},
		{name: "modular wraps", hasher: pgmesh.NewModuloShardHasher[uint64](4), key: 9, want: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, test.hasher.Hash(test.key))
		})
	}
}

func TestModuloShardHasherRejectsZeroVirtualShards(t *testing.T) {
	t.Parallel()
	require.PanicsWithValue(
		t,
		"pgmesh: virtual shard count must not be zero",
		func() { pgmesh.NewModuloShardHasher[uint64](0) },
	)
}

func TestModuloShardHasherSupportsSignedAndNamedKeys(t *testing.T) {
	t.Parallel()

	hasher := pgmesh.NewModuloShardHasher[tenantID](3)
	tests := []struct {
		name string
		key  tenantID
		want uint64
	}{
		{name: "zero", key: 0, want: 0},
		{name: "positive", key: 5, want: 2},
		{name: "negative", key: -1, want: 2},
		{name: "negative wraps", key: -5, want: 1},
		{name: "negative multiple maps to zero", key: -6, want: 0},
		{name: "minimum int64 does not overflow", key: tenantID(-1 << 63), want: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, hasher.Hash(test.key))
		})
	}
}

func TestVirtualShardRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from uint64
		to   uint64
		want []uint64
	}{
		{name: "empty equal bounds", from: 2, to: 2, want: []uint64{}},
		{name: "empty reversed bounds", from: 3, to: 2, want: []uint64{}},
		{name: "single", from: 2, to: 3, want: []uint64{2}},
		{name: "half open range", from: 2, to: 6, want: []uint64{2, 3, 4, 5}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, pgmesh.VirtualShardRange(test.from, test.to))
		})
	}
}

func TestReplicaSetRoutesReadsAndWrites(t *testing.T) {
	t.Parallel()

	primary := node("primary")
	replica0 := node("replica0")
	replica1 := node("replica1")
	mirror0 := node("mirror0")
	mirror1 := node("mirror1")

	replicaSet := pgmesh.NewReplicaSet("main", primary, []pgmesh.Node[string, *fakeWriter]{replica0, replica1}).
		WithWriteMirrors(mirror0.Writer(), mirror1.Writer())

	assert.Equal(t, "replica0-read", replicaSet.Reader())
	assert.Equal(t, "replica1-read", replicaSet.Reader())
	assert.Equal(t, "replica0-read", replicaSet.Reader())

	writer := replicaSet.Writer()
	assert.Equal(t, "primary-write", writer.name)
	require.Len(t, writer.mirrors, 2)
	assert.Equal(t, "mirror0-write", writer.mirrors[0].name)
	assert.Equal(t, "mirror1-write", writer.mirrors[1].name)
	assert.Empty(t, primary.Writer().mirrors, "routing must not mutate the primary node")
	assert.Equal(t, 2, replicaSet.WriteMirrorCount())
}

func TestReplicaSetMirrorCopiesAreImmutableAndOrdered(t *testing.T) {
	t.Parallel()

	base := pgmesh.NewReplicaSet("main", node("primary"), nil)
	first := base.WithWriteMirrors(node("mirror0").Writer())
	second := first.WithWriteMirrors(node("mirror1").Writer(), node("mirror2").Writer())

	assert.Zero(t, base.WriteMirrorCount())
	assert.Equal(t, 1, first.WriteMirrorCount())
	assert.Equal(t, 3, second.WriteMirrorCount())
	assert.Empty(t, base.Writer().mirrors)
	assert.Equal(t, []string{"mirror0-write"}, writerNames(first.Writer().mirrors))
	assert.Equal(
		t,
		[]string{"mirror0-write", "mirror1-write", "mirror2-write"},
		writerNames(second.Writer().mirrors),
	)
}

func writerNames(writers []*fakeWriter) []string {
	names := make([]string, 0, len(writers))
	for _, writer := range writers {
		names = append(names, writer.name)
	}
	return names
}

func TestQueryTelemetryRecordsRoutingAndErrors(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug}))

	replicaSet := pgmesh.NewReplicaSet("main", node("main"), nil).
		WithWriteMirrors(node("mirror").Writer())
	mesh, err := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
		WithTracerProvider(tracerProvider).
		WithMeterProvider(meterProvider).
		WithLogger(logger).
		WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).
		MapVirtualShard(0, replicaSet).
		Build()
	require.NoError(t, err)

	ctx, span := mesh.StartLogicalQuerySpan(
		t.Context(),
		"UserStore",
		"CreateUser",
		pgmesh.QueryKindWrite,
	)
	assert.True(t, trace.SpanFromContext(ctx).SpanContext().IsValid())
	shard, err := mesh.Resolve(0)
	require.NoError(t, err)
	route := shard.WriteRoute()
	span.SetRoute(pgmesh.RouteModePrimary, 1)
	_, physicalSpan := span.StartPhysicalQuerySpan(ctx, route.Metadata(), pgmesh.RouteModePrimary)
	queryErr := errors.New("write failed")
	physicalSpan.End(queryErr)
	span.End(queryErr)

	spans := recorder.Ended()
	require.Len(t, spans, 2)
	recordedSpan := spans[0]
	assert.Equal(t, "pgmesh.query.physical.UserStore.CreateUser", recordedSpan.Name())
	assert.Equal(t, codes.Error, recordedSpan.Status().Code)
	assert.Equal(t, queryErr.Error(), recordedSpan.Status().Description)

	attributes := make(map[attribute.Key]attribute.Value)
	for _, item := range recordedSpan.Attributes() {
		attributes[item.Key] = item.Value
	}
	assert.Equal(t, "UserStore", attributes[pgmesh.AttributeStoreName].AsString())
	assert.Equal(t, "CreateUser", attributes[pgmesh.AttributeQueryName].AsString())
	assert.Equal(t, "write", attributes[pgmesh.AttributeQueryKind].AsString())
	assert.Equal(t, "*errors.errorString", attributes[attribute.Key("error.type")].AsString())
	assert.Equal(t, "0", attributes[pgmesh.AttributeVirtualShardIndex].AsString())
	assert.Equal(t, "main", attributes[pgmesh.AttributeReplicaSetName].AsString())
	assert.Equal(t, "primary", attributes[pgmesh.AttributeNodeName].AsString())
	assert.Equal(t, "primary", attributes[pgmesh.AttributeNodeRole].AsString())
	assert.Equal(t, "primary", attributes[pgmesh.AttributeRouteMode].AsString())
	assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeStoreDelegated))
	assert.NotContains(t, attributes, attribute.Key("pgmesh.route.write_mirror_count"))
	require.Len(t, recordedSpan.Events(), 1)
	assert.Equal(t, "exception", recordedSpan.Events()[0].Name)
	operation := spans[1]
	assert.Equal(t, "pgmesh.query.logical.UserStore.CreateUser", operation.Name())
	operationAttributes := attributeMap(operation.Attributes())
	assert.Equal(t, "single", operationAttributes[pgmesh.AttributeRouteScope].AsString())
	assert.Equal(t, int64(1), operationAttributes[pgmesh.AttributeRouteReplicaSetCount].AsInt64())
	assert.NotContains(t, operationAttributes, attribute.Key(pgmesh.AttributeReplicaSetName))

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	require.Len(t, metrics.ScopeMetrics, 1)
	require.Len(t, metrics.ScopeMetrics[0].Metrics, 3)
	queryHistogram := metricHistogram(t, metrics, pgmesh.MetricPhysicalQueryDuration)
	require.Len(t, queryHistogram.DataPoints, 1)
	assert.Equal(t, uint64(1), queryHistogram.DataPoints[0].Count)
	assertMetricAttributes(t, queryHistogram.DataPoints[0].Attributes.ToSlice())
	operationHistogram := metricHistogram(t, metrics, pgmesh.MetricLogicalQueryDuration)
	require.Len(t, operationHistogram.DataPoints, 1)
	operationMetricAttributes := attributeMap(operationHistogram.DataPoints[0].Attributes.ToSlice())
	assert.Equal(t, "single", operationMetricAttributes[pgmesh.AttributeRouteScope].AsString())
	assert.NotContains(t, operationMetricAttributes, attribute.Key(pgmesh.AttributeReplicaSetName))
	assert.NotContains(t, operationMetricAttributes, attribute.Key(pgmesh.AttributeRouteReplicaSetCount))

	logLines := strings.Split(strings.TrimSpace(logOutput.String()), "\n")
	require.Len(t, logLines, 2)
	var queryLog map[string]any
	require.NoError(t, json.Unmarshal([]byte(logLines[0]), &queryLog))
	assert.Equal(t, "pgmesh physical query completed", queryLog["msg"])
	assert.Equal(t, "main", queryLog["replica_set"])
	assert.Equal(t, "0", queryLog["virtual_shard"])
	assert.Equal(t, "primary", queryLog["node"])
	assert.Equal(t, "primary", queryLog["node_role"])
	assert.Equal(t, queryErr.Error(), queryLog["error"])
	var operationLog map[string]any
	require.NoError(t, json.Unmarshal([]byte(logLines[1]), &operationLog))
	assert.Equal(t, "pgmesh logical query completed", operationLog["msg"])
	assert.Equal(t, "single", operationLog["route_scope"])
	assert.InDelta(t, 1, operationLog["replica_set_count"], 0)
}

func TestQueryTelemetryRecordsSuccessfulQueries(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })

	replicaSet := pgmesh.NewReplicaSet(
		"main",
		node("primary"),
		[]pgmesh.Node[string, *fakeWriter]{node("replica0"), node("replica1")},
	)
	mesh, err := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
		WithTracerProvider(tracerProvider).
		WithMeterProvider(meterProvider).
		WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).
		MapVirtualShard(0, replicaSet).
		Build()
	require.NoError(t, err)

	shard, err := mesh.Resolve(0)
	require.NoError(t, err)
	for range 2 {
		ctx, span := mesh.StartLogicalQuerySpan(t.Context(), "UserStore", "GetUser", pgmesh.QueryKindRead)
		span.SetRoute(pgmesh.RouteModeRead, 1)
		route := shard.ReadRoute()
		_, physicalSpan := span.StartPhysicalQuerySpan(ctx, route.Metadata(), pgmesh.RouteModeRead)
		physicalSpan.End(nil)
		span.End(nil)
	}

	spans := recorder.Ended()
	require.Len(t, spans, 4)
	nodes := make(map[string]struct{}, 2)
	for _, index := range []int{0, 2} {
		assert.Equal(t, codes.Unset, spans[index].Status().Code)
		attributes := attributeMap(spans[index].Attributes())
		nodes[attributes[pgmesh.AttributeNodeName].AsString()] = struct{}{}
		assert.Equal(t, "read_replica", attributes[pgmesh.AttributeNodeRole].AsString())
		assert.Equal(t, "main", attributes[pgmesh.AttributeReplicaSetName].AsString())
		assert.Equal(t, "read", attributes[pgmesh.AttributeRouteMode].AsString())
	}
	assert.Equal(t, map[string]struct{}{"replica-0": {}, "replica-1": {}}, nodes)

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	require.Len(t, metrics.ScopeMetrics, 1)
	require.Len(t, metrics.ScopeMetrics[0].Metrics, 3)
	queryHistogram := metricHistogram(t, metrics, pgmesh.MetricPhysicalQueryDuration)
	require.Len(t, queryHistogram.DataPoints, 2)
	metricNodes := make(map[string]uint64, 2)
	for _, point := range queryHistogram.DataPoints {
		attributes := attributeMap(point.Attributes.ToSlice())
		metricNodes[attributes[pgmesh.AttributeNodeName].AsString()] = point.Count
		assert.Equal(t, "read_replica", attributes[pgmesh.AttributeNodeRole].AsString())
		assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeVirtualShardIndex))
	}
	assert.Equal(t, map[string]uint64{"replica-0": 1, "replica-1": 1}, metricNodes)
	operationHistogram := metricHistogram(t, metrics, pgmesh.MetricLogicalQueryDuration)
	require.Len(t, operationHistogram.DataPoints, 1)
	assert.Equal(t, uint64(2), operationHistogram.DataPoints[0].Count)
}

func TestQueryTelemetryRecordsPhysicalQueriesInFlight(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })

	mesh, err := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
		WithMeterProvider(meterProvider).
		WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).
		MapVirtualShard(0, pgmesh.NewReplicaSet("main", node("primary"), nil)).
		Build()
	require.NoError(t, err)
	shard, err := mesh.Resolve(0)
	require.NoError(t, err)
	route := shard.ReadRoute()

	_, first := mesh.StartPhysicalQuerySpan(
		t.Context(),
		"UserStore",
		"GetUser",
		pgmesh.QueryKindRead,
		route.Metadata(),
		pgmesh.RouteModeRead,
	)
	_, second := mesh.StartPhysicalQuerySpan(
		t.Context(),
		"UserStore",
		"GetUser",
		pgmesh.QueryKindRead,
		route.Metadata(),
		pgmesh.RouteModeRead,
	)

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	inFlight := metricInt64Sum(t, metrics, pgmesh.MetricPhysicalQueryInFlight)
	require.Len(t, inFlight.DataPoints, 1)
	assert.Equal(t, int64(2), inFlight.DataPoints[0].Value)
	assert.False(t, inFlight.IsMonotonic)
	attributes := attributeMap(inFlight.DataPoints[0].Attributes.ToSlice())
	assert.Equal(t, "UserStore", attributes[pgmesh.AttributeStoreName].AsString())
	assert.Equal(t, "GetUser", attributes[pgmesh.AttributeQueryName].AsString())
	assert.Equal(t, "read", attributes[pgmesh.AttributeQueryKind].AsString())
	assert.Equal(t, "main", attributes[pgmesh.AttributeReplicaSetName].AsString())
	assert.Equal(t, "primary", attributes[pgmesh.AttributeNodeName].AsString())
	assert.Equal(t, "primary", attributes[pgmesh.AttributeNodeRole].AsString())
	assert.Equal(t, "read", attributes[pgmesh.AttributeRouteMode].AsString())
	assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeVirtualShardIndex))

	first.End(nil)
	metrics = metricdata.ResourceMetrics{}
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	inFlight = metricInt64Sum(t, metrics, pgmesh.MetricPhysicalQueryInFlight)
	require.Len(t, inFlight.DataPoints, 1)
	assert.Equal(t, int64(1), inFlight.DataPoints[0].Value)

	second.End(nil)
	metrics = metricdata.ResourceMetrics{}
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	inFlight = metricInt64Sum(t, metrics, pgmesh.MetricPhysicalQueryInFlight)
	require.Len(t, inFlight.DataPoints, 1)
	assert.Zero(t, inFlight.DataPoints[0].Value)
}

func TestCopyBatchObserverRecordsPhysicalCopyMetrics(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("copy failed")
	tests := []struct {
		name          string
		reason        pgmesh.CopyBatchFlushReason
		err           error
		wantErrorType bool
	}{
		{
			name:   "successful full batch",
			reason: pgmesh.CopyBatchFlushReasonFull,
		},
		{
			name:          "failed manual batch",
			reason:        pgmesh.CopyBatchFlushReasonManual,
			err:           sentinel,
			wantErrorType: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			reader := sdkmetric.NewManualReader()
			meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })

			mesh, err := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
				WithMeterProvider(meterProvider).
				WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).
				MapVirtualShard(0, pgmesh.NewReplicaSet("main", node("primary"), nil)).
				Build()
			require.NoError(t, err)
			route := mesh.ReplicaSets()[0].WriteRoute()
			observer := mesh.NewCopyBatchObserver("Users", "CopyUsers", route.Metadata())
			observer(t.Context(), pgmesh.CopyBatchObservation{
				RowCount:          3,
				SubmissionCount:   2,
				Reason:            test.reason,
				QueueDuration:     3 * time.Millisecond,
				ExecutionDuration: 7 * time.Millisecond,
				Err:               test.err,
			})

			var metrics metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &metrics))

			rows := metricInt64Histogram(t, metrics, pgmesh.MetricCopyBatchRows)
			require.Len(t, rows.DataPoints, 1)
			assert.Equal(t, uint64(1), rows.DataPoints[0].Count)
			assert.Equal(t, int64(3), rows.DataPoints[0].Sum)

			submissions := metricInt64Histogram(t, metrics, pgmesh.MetricCopyBatchSubmissions)
			require.Len(t, submissions.DataPoints, 1)
			assert.Equal(t, int64(2), submissions.DataPoints[0].Sum)

			flushes := metricInt64Sum(t, metrics, pgmesh.MetricCopyBatchFlushes)
			require.Len(t, flushes.DataPoints, 1)
			assert.Equal(t, int64(1), flushes.DataPoints[0].Value)
			assert.True(t, flushes.IsMonotonic)

			execution := metricHistogram(t, metrics, pgmesh.MetricCopyBatchDuration)
			require.Len(t, execution.DataPoints, 1)
			assert.InDelta(t, 0.007, execution.DataPoints[0].Sum, 0.000_000_1)

			queue := metricHistogram(t, metrics, pgmesh.MetricCopyBatchQueueDuration)
			require.Len(t, queue.DataPoints, 1)
			assert.InDelta(t, 0.003, queue.DataPoints[0].Sum, 0.000_000_1)

			attributes := attributeMap(rows.DataPoints[0].Attributes.ToSlice())
			assert.Equal(t, "Users", attributes[pgmesh.AttributeStoreName].AsString())
			assert.Equal(t, "CopyUsers", attributes[pgmesh.AttributeQueryName].AsString())
			assert.Equal(t, "write", attributes[pgmesh.AttributeQueryKind].AsString())
			assert.Equal(t, "main", attributes[pgmesh.AttributeReplicaSetName].AsString())
			assert.Equal(t, "primary", attributes[pgmesh.AttributeNodeName].AsString())
			assert.Equal(t, "primary", attributes[pgmesh.AttributeNodeRole].AsString())
			assert.Equal(t, "primary", attributes[pgmesh.AttributeRouteMode].AsString())
			assert.Equal(
				t,
				string(test.reason),
				attributes[pgmesh.AttributeCopyBatchFlushReason].AsString(),
			)
			_, hasErrorType := attributes[attribute.Key("error.type")]
			assert.Equal(t, test.wantErrorType, hasErrorType)
			assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeVirtualShardIndex))
		})
	}
}

func TestOpenMeshTelemetryOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(*testing.T) (
			pgmesh.OpenMeshOption,
			func(*pgmesh.Mesh[string, *fakeWriter, uint64]),
		)
	}{
		{
			name: "tracer provider",
			setup: func(t *testing.T) (
				pgmesh.OpenMeshOption,
				func(*pgmesh.Mesh[string, *fakeWriter, uint64]),
			) {
				recorder := tracetest.NewSpanRecorder()
				provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
				t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
				return pgmesh.WithTracerProvider(provider), func(mesh *pgmesh.Mesh[string, *fakeWriter, uint64]) {
					_, span := mesh.StartLogicalQuerySpan(t.Context(), "Users", "GetUser", pgmesh.QueryKindRead)
					span.End(nil)
					ended := recorder.Ended()
					require.Len(t, ended, 1)
					assert.Equal(t, "pgmesh.query.logical.Users.GetUser", ended[0].Name())
				}
			},
		},
		{
			name: "meter provider",
			setup: func(t *testing.T) (
				pgmesh.OpenMeshOption,
				func(*pgmesh.Mesh[string, *fakeWriter, uint64]),
			) {
				reader := sdkmetric.NewManualReader()
				provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
				t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
				return pgmesh.WithMeterProvider(provider), func(mesh *pgmesh.Mesh[string, *fakeWriter, uint64]) {
					_, span := mesh.StartLogicalQuerySpan(t.Context(), "Users", "GetUser", pgmesh.QueryKindRead)
					span.End(nil)
					var metrics metricdata.ResourceMetrics
					require.NoError(t, reader.Collect(t.Context(), &metrics))
					histogram := metricHistogram(t, metrics, pgmesh.MetricLogicalQueryDuration)
					require.Len(t, histogram.DataPoints, 1)
					assert.Equal(t, uint64(1), histogram.DataPoints[0].Count)
				}
			},
		},
		{
			name: "logger",
			setup: func(t *testing.T) (
				pgmesh.OpenMeshOption,
				func(*pgmesh.Mesh[string, *fakeWriter, uint64]),
			) {
				var output bytes.Buffer
				logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
				return pgmesh.WithLogger(logger), func(mesh *pgmesh.Mesh[string, *fakeWriter, uint64]) {
					_, span := mesh.StartLogicalQuerySpan(t.Context(), "Users", "GetUser", pgmesh.QueryKindRead)
					span.SetRoute(pgmesh.RouteModeRead, 1)
					span.End(nil)
					var record map[string]any
					require.NoError(t, json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record))
					assert.Equal(t, "pgmesh logical query completed", record["msg"])
					assert.Equal(t, "single", record["route_scope"])
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			option, verify := test.setup(t)
			mesh, err := pgmesh.OpenMesh(
				t.Context(),
				1,
				func(_ context.Context, dsn string) (pgmesh.Node[string, *fakeWriter], error) {
					return node(dsn), nil
				},
				pgmesh.NewConstantShardHasher[uint64](0),
				pgmesh.WithReplicaSet("main", "primary"),
				pgmesh.WithVirtualShardMapping("main", []uint64{0}),
				option,
			)
			require.NoError(t, err)
			verify(mesh)
		})
	}
}

func TestStoreTelemetryRecordsFactoryShortCircuit(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug}))

	mesh, err := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
		WithTracerProvider(tracerProvider).
		WithMeterProvider(meterProvider).
		WithLogger(logger).
		WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).
		MapVirtualShard(0, pgmesh.NewReplicaSet("main", node("main"), nil)).
		Build()
	require.NoError(t, err)

	_, storeSpan := mesh.StartStoreQuerySpan(t.Context(), "Users", "GetUser", pgmesh.QueryKindRead)
	storeSpan.End(nil)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "pgmesh.query.store.Users.GetUser", spans[0].Name())
	attributes := attributeMap(spans[0].Attributes())
	assert.Equal(t, "Users", attributes[pgmesh.AttributeStoreName].AsString())
	assert.Equal(t, "GetUser", attributes[pgmesh.AttributeQueryName].AsString())
	assert.False(t, attributes[pgmesh.AttributeStoreDelegated].AsBool())
	assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeReplicaSetName))
	assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeRouteMode))

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	histogram := metricHistogram(t, metrics, pgmesh.MetricStoreQueryDuration)
	require.Len(t, histogram.DataPoints, 1)
	metricAttributes := attributeMap(histogram.DataPoints[0].Attributes.ToSlice())
	assert.False(t, metricAttributes[pgmesh.AttributeStoreDelegated].AsBool())
	assert.Equal(t, "Users", metricAttributes[pgmesh.AttributeStoreName].AsString())

	var logRecord map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(logOutput.Bytes()), &logRecord))
	assert.Equal(t, "pgmesh store query completed", logRecord["msg"])
	assert.Equal(t, "Users", logRecord["store_name"])
	assert.Equal(t, "GetUser", logRecord["query_name"])
	assert.Equal(t, false, logRecord["store_delegated"])
	assert.NotContains(t, logRecord, "replica_set")
	assert.NotContains(t, logRecord, "route_mode")
}

func TestStoreTelemetryCreatesDedicatedInternalQueryChild(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug}))

	mesh, err := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
		WithTracerProvider(tracerProvider).
		WithMeterProvider(meterProvider).
		WithLogger(logger).
		WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).
		MapVirtualShard(0, pgmesh.NewReplicaSet("main", node("main"), nil)).
		Build()
	require.NoError(t, err)

	ctx, storeSpan := mesh.StartStoreQuerySpan(t.Context(), "Users", "GetUser", pgmesh.QueryKindRead)
	ctx, querySpan := mesh.StartLogicalQuerySpan(ctx, "Users", "GetUser", pgmesh.QueryKindRead)
	querySpan.SetRoute(pgmesh.RouteModeRead, 1)
	shard, err := mesh.Resolve(0)
	require.NoError(t, err)
	route := shard.ReadRoute()
	_, physicalSpan := querySpan.StartPhysicalQuerySpan(ctx, route.Metadata(), pgmesh.RouteModeRead)
	physicalSpan.End(nil)
	querySpan.End(nil)
	storeSpan.End(nil)

	spans := recorder.Ended()
	require.Len(t, spans, 3)
	query := spans[0]
	operation := spans[1]
	store := spans[2]
	assert.Equal(t, "pgmesh.query.physical.Users.GetUser", query.Name())
	assert.Equal(t, "pgmesh.query.logical.Users.GetUser", operation.Name())
	assert.Equal(t, "pgmesh.query.store.Users.GetUser", store.Name())
	assert.Equal(t, store.SpanContext().SpanID(), operation.Parent().SpanID())
	assert.Equal(t, operation.SpanContext().SpanID(), query.Parent().SpanID())
	queryAttributes := attributeMap(query.Attributes())
	assert.Equal(t, "main", queryAttributes[pgmesh.AttributeReplicaSetName].AsString())
	assert.Equal(t, "primary", queryAttributes[pgmesh.AttributeNodeName].AsString())
	assert.NotContains(t, queryAttributes, attribute.Key(pgmesh.AttributeStoreDelegated))
	storeAttributes := attributeMap(store.Attributes())
	assert.True(t, storeAttributes[pgmesh.AttributeStoreDelegated].AsBool())
	assert.NotContains(t, storeAttributes, attribute.Key(pgmesh.AttributeReplicaSetName))
	assert.NotContains(t, storeAttributes, attribute.Key(pgmesh.AttributeRouteMode))

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	storeHistogram := metricHistogram(t, metrics, pgmesh.MetricStoreQueryDuration)
	require.Len(t, storeHistogram.DataPoints, 1)
	storeMetricAttributes := attributeMap(storeHistogram.DataPoints[0].Attributes.ToSlice())
	assert.True(t, storeMetricAttributes[pgmesh.AttributeStoreDelegated].AsBool())
	operationHistogram := metricHistogram(t, metrics, pgmesh.MetricLogicalQueryDuration)
	require.Len(t, operationHistogram.DataPoints, 1)
	queryHistogram := metricHistogram(t, metrics, pgmesh.MetricPhysicalQueryDuration)
	require.Len(t, queryHistogram.DataPoints, 1)
	queryMetricAttributes := attributeMap(queryHistogram.DataPoints[0].Attributes.ToSlice())
	assert.Equal(t, "main", queryMetricAttributes[pgmesh.AttributeReplicaSetName].AsString())
	assert.Equal(t, "primary", queryMetricAttributes[pgmesh.AttributeNodeName].AsString())
	assert.NotContains(t, queryMetricAttributes, attribute.Key(pgmesh.AttributeStoreDelegated))

	logLines := strings.Split(strings.TrimSpace(logOutput.String()), "\n")
	require.Len(t, logLines, 3)
	var queryLog map[string]any
	require.NoError(t, json.Unmarshal([]byte(logLines[0]), &queryLog))
	assert.Equal(t, "pgmesh physical query completed", queryLog["msg"])
	assert.Equal(t, "main", queryLog["replica_set"])
	assert.NotContains(t, queryLog, "store_delegated")
	var operationLog map[string]any
	require.NoError(t, json.Unmarshal([]byte(logLines[1]), &operationLog))
	assert.Equal(t, "pgmesh logical query completed", operationLog["msg"])
	var storeLog map[string]any
	require.NoError(t, json.Unmarshal([]byte(logLines[2]), &storeLog))
	assert.Equal(t, "pgmesh store query completed", storeLog["msg"])
	assert.Equal(t, true, storeLog["store_delegated"])
	assert.NotContains(t, storeLog, "replica_set")
}

func TestStoreTelemetryRecordsWrapperErrorsWithoutInternalExecution(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })

	mesh, err := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
		WithTracerProvider(tracerProvider).
		WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).
		MapVirtualShard(0, pgmesh.NewReplicaSet("main", node("main"), nil)).
		Build()
	require.NoError(t, err)

	_, storeSpan := mesh.StartStoreQuerySpan(t.Context(), "Users", "GetUser", pgmesh.QueryKindRead)
	wrapperErr := errors.New("cache unavailable")
	storeSpan.End(wrapperErr)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, codes.Error, spans[0].Status().Code)
	attributes := attributeMap(spans[0].Attributes())
	assert.False(t, attributes[pgmesh.AttributeStoreDelegated].AsBool())
	assert.Equal(t, "*errors.errorString", attributes[attribute.Key("error.type")].AsString())
}

func TestStoreTelemetryExecutionMarkerIsMeshScoped(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })

	mesh, err := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
		WithTracerProvider(tracerProvider).
		WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).
		MapVirtualShard(0, pgmesh.NewReplicaSet("main", node("main"), nil)).
		Build()
	require.NoError(t, err)
	otherMesh, err := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
		WithTracerProvider(tracerProvider).
		WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).
		MapVirtualShard(0, pgmesh.NewReplicaSet("other", node("other"), nil)).
		Build()
	require.NoError(t, err)

	ctx, storeSpan := mesh.StartStoreQuerySpan(t.Context(), "Users", "GetUser", pgmesh.QueryKindRead)
	_, querySpan := otherMesh.StartLogicalQuerySpan(ctx, "Users", "GetUser", pgmesh.QueryKindRead)
	querySpan.End(nil)
	storeSpan.End(nil)

	spans := recorder.Ended()
	require.Len(t, spans, 2)
	storeAttributes := attributeMap(spans[1].Attributes())
	assert.False(t, storeAttributes[pgmesh.AttributeStoreDelegated].AsBool())
}

func TestQueryTelemetryRecordsMultiRoute(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug}))

	mesh, err := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](2).
		WithTracerProvider(tracerProvider).
		WithMeterProvider(meterProvider).
		WithLogger(logger).
		WithShardHasher(pgmesh.NewModuloShardHasher[uint64](2)).
		MapVirtualShard(0, pgmesh.NewReplicaSet("zero", node("zero"), nil)).
		MapVirtualShard(1, pgmesh.NewReplicaSet("one", node("one"), nil)).
		Build()
	require.NoError(t, err)

	ctx, span := mesh.StartLogicalQuerySpan(t.Context(), "UserStore", "DeleteAll", pgmesh.QueryKindWrite)
	replicaSets := mesh.ReplicaSets()
	span.SetRoute(pgmesh.RouteModePrimary, len(replicaSets))
	for _, replicaSet := range replicaSets {
		route := replicaSet.WriteRoute()
		_, physicalSpan := span.StartPhysicalQuerySpan(
			ctx,
			route.Metadata(),
			pgmesh.RouteModePrimary,
		)
		physicalSpan.End(nil)
	}
	span.End(nil)

	spans := recorder.Ended()
	require.Len(t, spans, 3)
	attributes := attributeMap(spans[2].Attributes())
	assert.Equal(t, "pgmesh.query.logical.UserStore.DeleteAll", spans[2].Name())
	assert.Equal(t, "primary", attributes[pgmesh.AttributeRouteMode].AsString())
	assert.Equal(t, "fanout", attributes[pgmesh.AttributeRouteScope].AsString())
	assert.Equal(t, int64(2), attributes[pgmesh.AttributeRouteReplicaSetCount].AsInt64())
	assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeReplicaSetName))
	physicalShards := make(map[string]struct{}, 2)
	for _, physical := range spans[:2] {
		physicalAttributes := attributeMap(physical.Attributes())
		physicalShards[physicalAttributes[pgmesh.AttributeReplicaSetName].AsString()] = struct{}{}
		assert.Equal(t, "primary", physicalAttributes[pgmesh.AttributeNodeName].AsString())
		assert.NotContains(t, physicalAttributes, attribute.Key(pgmesh.AttributeVirtualShardIndex))
	}
	assert.Equal(t, map[string]struct{}{"zero": {}, "one": {}}, physicalShards)

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	require.Len(t, metrics.ScopeMetrics, 1)
	require.Len(t, metrics.ScopeMetrics[0].Metrics, 3)
	operationHistogram := metricHistogram(t, metrics, pgmesh.MetricLogicalQueryDuration)
	require.Len(t, operationHistogram.DataPoints, 1)
	metricAttributes := attributeMap(operationHistogram.DataPoints[0].Attributes.ToSlice())
	assert.Equal(t, "primary", metricAttributes[pgmesh.AttributeRouteMode].AsString())
	assert.Equal(t, "fanout", metricAttributes[pgmesh.AttributeRouteScope].AsString())
	assert.NotContains(t, metricAttributes, attribute.Key(pgmesh.AttributeRouteReplicaSetCount))
	assert.NotContains(t, metricAttributes, attribute.Key(pgmesh.AttributeReplicaSetName))
	queryHistogram := metricHistogram(t, metrics, pgmesh.MetricPhysicalQueryDuration)
	require.Len(t, queryHistogram.DataPoints, 2)
	for _, point := range queryHistogram.DataPoints {
		queryAttributes := attributeMap(point.Attributes.ToSlice())
		assert.Contains(t, physicalShards, queryAttributes[pgmesh.AttributeReplicaSetName].AsString())
	}

	logLines := strings.Split(strings.TrimSpace(logOutput.String()), "\n")
	require.Len(t, logLines, 3)
	var operationLog map[string]any
	require.NoError(t, json.Unmarshal([]byte(logLines[2]), &operationLog))
	assert.Equal(t, "fanout", operationLog["route_scope"])
	assert.InDelta(t, 2, operationLog["replica_set_count"], 0)
}

func attributeMap(items []attribute.KeyValue) map[attribute.Key]attribute.Value {
	attributes := make(map[attribute.Key]attribute.Value, len(items))
	for _, item := range items {
		attributes[item.Key] = item.Value
	}
	return attributes
}

func metricHistogram(
	t *testing.T,
	metrics metricdata.ResourceMetrics,
	name string,
) metricdata.Histogram[float64] {
	t.Helper()

	for _, scope := range metrics.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name != name {
				continue
			}
			histogram, ok := measurement.Data.(metricdata.Histogram[float64])
			require.True(t, ok)
			return histogram
		}
	}
	require.FailNow(t, "metric not found", name)
	return metricdata.Histogram[float64]{}
}

func metricInt64Sum(
	t *testing.T,
	metrics metricdata.ResourceMetrics,
	name string,
) metricdata.Sum[int64] {
	t.Helper()

	for _, scope := range metrics.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name != name {
				continue
			}
			sum, ok := measurement.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			return sum
		}
	}
	require.FailNow(t, "metric not found", name)
	return metricdata.Sum[int64]{}
}

func metricInt64Histogram(
	t *testing.T,
	metrics metricdata.ResourceMetrics,
	name string,
) metricdata.Histogram[int64] {
	t.Helper()

	for _, scope := range metrics.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name != name {
				continue
			}
			histogram, ok := measurement.Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			return histogram
		}
	}
	require.FailNow(t, "metric not found", name)
	return metricdata.Histogram[int64]{}
}

func assertMetricAttributes(t *testing.T, items []attribute.KeyValue) {
	t.Helper()

	attributes := make(map[attribute.Key]attribute.Value)
	for _, item := range items {
		attributes[item.Key] = item.Value
	}
	assert.Equal(t, "UserStore", attributes[pgmesh.AttributeStoreName].AsString())
	assert.Equal(t, "CreateUser", attributes[pgmesh.AttributeQueryName].AsString())
	assert.Equal(t, "write", attributes[pgmesh.AttributeQueryKind].AsString())
	assert.Equal(t, "*errors.errorString", attributes[attribute.Key("error.type")].AsString())
	assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeVirtualShardIndex))
	assert.Equal(t, "main", attributes[pgmesh.AttributeReplicaSetName].AsString())
	assert.Equal(t, "primary", attributes[pgmesh.AttributeNodeName].AsString())
	assert.Equal(t, "primary", attributes[pgmesh.AttributeNodeRole].AsString())
	assert.Equal(t, "primary", attributes[pgmesh.AttributeRouteMode].AsString())
	assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeStoreDelegated))
	assert.NotContains(t, attributes, attribute.Key("pgmesh.route.write_mirror_count"))
}

func TestReplicaSetFallsBackToPrimaryReader(t *testing.T) {
	t.Parallel()

	replicaSet := pgmesh.NewReplicaSet("main", node("primary"), nil)
	assert.Equal(t, "primary-read", replicaSet.Reader())
}

func TestReplicaSetRoundRobinIsConcurrent(t *testing.T) {
	t.Parallel()

	replicaSet := pgmesh.NewReplicaSet(
		"main",
		node("primary"),
		[]pgmesh.Node[string, *fakeWriter]{node("replica0"), node("replica1")},
	)

	const calls = 1000
	results := make(chan string, calls)
	var group sync.WaitGroup
	for range calls {
		group.Go(func() {
			results <- replicaSet.Reader()
		})
	}
	group.Wait()
	close(results)

	counts := map[string]int{}
	for result := range results {
		counts[result]++
	}
	assert.Equal(t, calls/2, counts["replica0-read"])
	assert.Equal(t, calls/2, counts["replica1-read"])
}

func TestMeshBuilderRoutesAndListsPhysicalShardsDeterministically(t *testing.T) {
	t.Parallel()

	shardA := pgmesh.NewReplicaSet("a", node("a"), nil)
	shardB := pgmesh.NewReplicaSet("b", node("b"), nil)
	mesh, err := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](4).
		WithShardHasher(pgmesh.NewModuloShardHasher[uint64](4)).
		MapVirtualShard(0, shardB).
		MapVirtualShard(1, shardA).
		MapVirtualShard(2, shardB).
		MapVirtualShard(3, shardA).
		Build()
	require.NoError(t, err)

	routed, err := mesh.Resolve(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), routed.VirtualShardIndex())
	assert.Equal(t, "a", routed.ReplicaSetName())

	all := mesh.ReplicaSets()
	require.Len(t, all, 2)
	assert.Equal(t, "b", all[0].Name())
	assert.Equal(t, "a", all[1].Name())
	all[0] = nil
	assert.Equal(t, "b", mesh.ReplicaSets()[0].Name(), "ReplicaSets must return a defensive slice")
}

func TestMeshRejectsOutOfRangeHasherResult(t *testing.T) {
	t.Parallel()

	mesh, err := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
		WithShardHasher(pgmesh.NewConstantShardHasher[uint64](2)).
		MapVirtualShard(0, pgmesh.NewReplicaSet("main", node("main"), nil)).
		Build()
	require.NoError(t, err)

	_, err = mesh.Resolve(1)
	assert.ErrorIs(t, err, pgmesh.ErrVirtualShardOutOfRange)
}

func TestMeshBuilderValidation(t *testing.T) {
	t.Parallel()

	replicaSet := pgmesh.NewReplicaSet("main", node("main"), nil)
	tests := []struct {
		name string
		make func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error)
		want error
	}{
		{
			name: "no virtual shards",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](0).
					WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).Build()
			},
			want: pgmesh.ErrNoVirtualShards,
		},
		{
			name: "no hasher",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).MapVirtualShard(0, replicaSet).Build()
			},
			want: pgmesh.ErrNilShardHasher,
		},
		{
			name: "missing virtual shard",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
					WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).Build()
			},
			want: pgmesh.ErrVirtualShardNotMapped,
		},
		{
			name: "duplicate virtual shard",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
					WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).
					MapVirtualShard(0, replicaSet).MapVirtualShard(0, replicaSet).Build()
			},
			want: pgmesh.ErrVirtualShardAlreadyMapped,
		},
		{
			name: "map out of range",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
					WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).MapVirtualShard(1, replicaSet).Build()
			},
			want: pgmesh.ErrVirtualShardOutOfRange,
		},
		{
			name: "empty replica set name",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
					WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).
					MapVirtualShard(0, pgmesh.NewReplicaSet("", node("main"), nil)).Build()
			},
			want: pgmesh.ErrEmptyReplicaSetName,
		},
		{
			name: "nil replica set",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
					WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).MapVirtualShard(0, nil).Build()
			},
			want: pgmesh.ErrNilReplicaSet,
		},
		{
			name: "duplicate physical name",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](2).
					WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).
					MapVirtualShard(0, pgmesh.NewReplicaSet("same", node("a"), nil)).
					MapVirtualShard(1, pgmesh.NewReplicaSet("same", node("b"), nil)).Build()
			},
			want: pgmesh.ErrDuplicateReplicaSet,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := test.make()
			assert.ErrorIs(t, err, test.want)
		})
	}
}

func TestMeshBuilderPreservesFirstError(t *testing.T) {
	t.Parallel()

	meterErr := errors.New("meter initialization failed")
	builder := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
		WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).
		MapVirtualShard(1, pgmesh.NewReplicaSet("main", node("main"), nil)).
		WithMeterProvider(failingMeterProvider{err: meterErr}).
		MapVirtualShard(0, nil)

	_, err := builder.Build()
	require.ErrorIs(t, err, pgmesh.ErrVirtualShardOutOfRange)
	assert.NotErrorIs(t, err, meterErr)
}

func TestMeshBuilderReportsMeterInitializationFailure(t *testing.T) {
	t.Parallel()

	meterErr := errors.New("meter initialization failed")
	_, err := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](1).
		WithShardHasher(pgmesh.NewConstantShardHasher[uint64](0)).
		WithMeterProvider(failingMeterProvider{err: meterErr}).
		MapVirtualShard(0, pgmesh.NewReplicaSet("main", node("main"), nil)).
		Build()

	require.ErrorIs(t, err, meterErr)
	assert.ErrorContains(t, err, "configure OpenTelemetry metrics")
}

func TestOpenMeshBuildsTopologyAndMirrors(t *testing.T) {
	t.Parallel()

	created := make([]string, 0)
	mesh, err := pgmesh.OpenMesh(
		t.Context(),
		2,
		func(_ context.Context, dsn string) (pgmesh.Node[string, *fakeWriter], error) {
			created = append(created, dsn)
			return node(dsn), nil
		},
		pgmesh.NewModuloShardHasher[uint64](2),
		pgmesh.WithReplicaSet("a", "a-primary", "a-replica"),
		pgmesh.WithReplicaSet("b", "b-primary"),
		pgmesh.WithVirtualShardMapping("a", []uint64{0}, "b"),
		pgmesh.WithVirtualShardMapping("b", []uint64{1}),
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"a-primary", "a-replica", "b-primary"}, created)

	shard, err := mesh.Resolve(0)
	require.NoError(t, err)
	assert.Equal(t, "a-replica-read", shard.ReadRoute().Target)
	writer := shard.WriteRoute().Target
	require.Len(t, writer.mirrors, 1)
	assert.Equal(t, "b-primary-write", writer.mirrors[0].name)
}

func TestMeshOptionsCloneInputs(t *testing.T) {
	t.Parallel()

	replicaDSNs := []string{"replica"}
	virtualShards := []uint64{0}
	mirrors := []string{"mirror"}
	replicaSetOption := pgmesh.WithReplicaSet("main", "primary", replicaDSNs...)
	mappingOption := pgmesh.WithVirtualShardMapping("main", virtualShards, mirrors...)

	replicaDSNs[0] = ""
	virtualShards[0] = 1
	mirrors[0] = "missing"

	created := make([]string, 0)
	_, err := pgmesh.OpenMesh(
		t.Context(),
		1,
		func(_ context.Context, dsn string) (pgmesh.Node[string, *fakeWriter], error) {
			created = append(created, dsn)
			return node(dsn), nil
		},
		pgmesh.NewConstantShardHasher[uint64](0),
		replicaSetOption,
		pgmesh.WithReplicaSet("mirror", "mirror-primary"),
		mappingOption,
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"primary", "replica", "mirror-primary"}, created)
}

func TestOpenMeshValidation(t *testing.T) {
	t.Parallel()

	type input struct {
		virtualShardCount uint64
		openNode          pgmesh.NodeOpener[string, *fakeWriter]
		hasher            pgmesh.ShardHasher[uint64]
		options           []pgmesh.OpenMeshOption
	}
	valid := func() input {
		return input{
			virtualShardCount: 1,
			openNode: func(context.Context, string) (pgmesh.Node[string, *fakeWriter], error) {
				return node("main"), nil
			},
			hasher: pgmesh.NewConstantShardHasher[uint64](0),
			options: []pgmesh.OpenMeshOption{
				pgmesh.WithReplicaSet("main", "primary"),
				pgmesh.WithVirtualShardMapping("main", []uint64{0}),
			},
		}
	}

	tests := []struct {
		name string
		edit func(*input)
		want error
	}{
		{
			name: "no replica sets",
			edit: func(config *input) {
				config.options = []pgmesh.OpenMeshOption{
					pgmesh.WithVirtualShardMapping("main", []uint64{0}),
				}
			},
			want: pgmesh.ErrNoReplicaSets,
		},
		{
			name: "empty name",
			edit: func(config *input) {
				config.options[0] = pgmesh.WithReplicaSet("", "primary")
			},
			want: pgmesh.ErrEmptyReplicaSetName,
		},
		{
			name: "whitespace name",
			edit: func(config *input) {
				config.options[0] = pgmesh.WithReplicaSet(" \t", "primary")
			},
			want: pgmesh.ErrEmptyReplicaSetName,
		},
		{name: "duplicate name", edit: func(config *input) {
			config.options = append(config.options, pgmesh.WithReplicaSet("main", "other"))
		}, want: pgmesh.ErrDuplicateReplicaSet},
		{
			name: "empty DSN",
			edit: func(config *input) {
				config.options[0] = pgmesh.WithReplicaSet("main", "")
			},
			want: pgmesh.ErrEmptyDSN,
		},
		{
			name: "whitespace primary DSN",
			edit: func(config *input) {
				config.options[0] = pgmesh.WithReplicaSet("main", " \t")
			},
			want: pgmesh.ErrEmptyDSN,
		},
		{
			name: "empty replica DSN",
			edit: func(config *input) {
				config.options[0] = pgmesh.WithReplicaSet("main", "primary", "")
			},
			want: pgmesh.ErrEmptyDSN,
		},
		{
			name: "no node opener",
			edit: func(config *input) { config.openNode = nil },
			want: pgmesh.ErrNilNodeOpener,
		},
		{
			name: "no hasher",
			edit: func(config *input) { config.hasher = nil },
			want: pgmesh.ErrNilShardHasher,
		},
		{
			name: "no virtual shards",
			edit: func(config *input) { config.virtualShardCount = 0 },
			want: pgmesh.ErrNoVirtualShards,
		},
		{name: "unknown main", edit: func(config *input) {
			config.options[1] = pgmesh.WithVirtualShardMapping("missing", []uint64{0})
		}, want: pgmesh.ErrUnknownReplicaSet},
		{name: "unknown mirror", edit: func(config *input) {
			config.options[1] = pgmesh.WithVirtualShardMapping("main", []uint64{0}, "missing")
		}, want: pgmesh.ErrUnknownReplicaSet},
		{
			name: "self mirror",
			edit: func(config *input) {
				config.options[1] = pgmesh.WithVirtualShardMapping("main", []uint64{0}, "main")
			},
			want: pgmesh.ErrMirrorConfiguration,
		},
		{
			name: "duplicate mirror",
			edit: func(config *input) {
				config.options = []pgmesh.OpenMeshOption{
					pgmesh.WithReplicaSet("main", "primary"),
					pgmesh.WithReplicaSet("mirror", "mirror"),
					pgmesh.WithVirtualShardMapping("main", []uint64{0}, "mirror", "mirror"),
				}
			},
			want: pgmesh.ErrMirrorConfiguration,
		},
		{
			name: "missing virtual shard",
			edit: func(config *input) {
				config.options = config.options[:1]
			},
			want: pgmesh.ErrVirtualShardNotMapped,
		},
		{name: "duplicate virtual shard", edit: func(config *input) {
			config.options = append(config.options, pgmesh.WithVirtualShardMapping("main", []uint64{0}))
		}, want: pgmesh.ErrVirtualShardAlreadyMapped},
		{
			name: "out of range",
			edit: func(config *input) {
				config.options[1] = pgmesh.WithVirtualShardMapping("main", []uint64{1})
			},
			want: pgmesh.ErrVirtualShardOutOfRange,
		},
		{
			name: "inconsistent mirrors",
			edit: func(config *input) {
				config.virtualShardCount = 2
				config.options = []pgmesh.OpenMeshOption{
					pgmesh.WithReplicaSet("main", "primary"),
					pgmesh.WithReplicaSet("mirror", "mirror"),
					pgmesh.WithVirtualShardMapping("main", []uint64{0}),
					pgmesh.WithVirtualShardMapping("main", []uint64{1}, "mirror"),
				}
			},
			want: pgmesh.ErrMirrorConfiguration,
		},
		{
			name: "inconsistent mirror order",
			edit: func(config *input) {
				config.virtualShardCount = 2
				config.options = []pgmesh.OpenMeshOption{
					pgmesh.WithReplicaSet("main", "primary"),
					pgmesh.WithReplicaSet("mirror-a", "mirror-a"),
					pgmesh.WithReplicaSet("mirror-b", "mirror-b"),
					pgmesh.WithVirtualShardMapping("main", []uint64{0}, "mirror-a", "mirror-b"),
					pgmesh.WithVirtualShardMapping("main", []uint64{1}, "mirror-b", "mirror-a"),
				}
			},
			want: pgmesh.ErrMirrorConfiguration,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := valid()
			test.edit(&config)
			_, err := pgmesh.OpenMesh(
				t.Context(),
				config.virtualShardCount,
				config.openNode,
				config.hasher,
				config.options...,
			)
			assert.ErrorIs(t, err, test.want)
		})
	}

	_, err := pgmesh.OpenMesh(
		t.Context(),
		1,
		valid().openNode,
		valid().hasher,
		pgmesh.WithReplicaSet("main", "primary"),
		pgmesh.WithVirtualShardMapping("main", []uint64{0}),
		nil,
	)
	assert.ErrorContains(t, err, "mesh option 2 is nil")
}

func TestOpenMeshWrapsNodeOpenerError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("connect failed")
	tests := []struct {
		name     string
		replicas []string
		factory  func(context.Context, string) (pgmesh.Node[string, *fakeWriter], error)
		want     string
	}{
		{
			name: "primary",
			factory: func(context.Context, string) (pgmesh.Node[string, *fakeWriter], error) {
				return pgmesh.Node[string, *fakeWriter]{}, sentinel
			},
			want: fmt.Sprintf("primary node for replica set %q", "main"),
		},
		{
			name:     "replica",
			replicas: []string{"replica"},
			factory: func(_ context.Context, dsn string) (pgmesh.Node[string, *fakeWriter], error) {
				if dsn == "primary" {
					return node("primary"), nil
				}
				return pgmesh.Node[string, *fakeWriter]{}, sentinel
			},
			want: fmt.Sprintf("replica node for replica set %q", "main"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := pgmesh.OpenMesh(
				t.Context(),
				1,
				test.factory,
				pgmesh.NewConstantShardHasher[uint64](0),
				pgmesh.WithReplicaSet("main", "primary", test.replicas...),
				pgmesh.WithVirtualShardMapping("main", []uint64{0}),
			)
			require.ErrorIs(t, err, sentinel)
			assert.ErrorContains(t, err, test.want)
		})
	}
}
