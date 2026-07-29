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
		{name: "constant ignores zero key", hasher: pgmesh.ConstantShardHashFor[uint64](7), key: 0, want: 7},
		{name: "constant ignores nonzero key", hasher: pgmesh.ConstantShardHashFor[uint64](7), key: 99, want: 7},
		{name: "modular zero", hasher: pgmesh.ModularShardHashFor[uint64](4), key: 0, want: 0},
		{name: "modular in range", hasher: pgmesh.ModularShardHashFor[uint64](4), key: 3, want: 3},
		{name: "modular wraps", hasher: pgmesh.ModularShardHashFor[uint64](4), key: 9, want: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, test.hasher.Hash(test.key))
		})
	}
}

func TestModularShardHashRejectsZeroVirtualShards(t *testing.T) {
	t.Parallel()
	require.PanicsWithValue(
		t,
		"pgmesh: numVShards must not be zero",
		func() { pgmesh.ModularShardHashFor[uint64](0) },
	)
}

func TestModularShardHashSupportsSignedAndNamedKeys(t *testing.T) {
	t.Parallel()

	hasher := pgmesh.ModularShardHashFor[tenantID](3)
	tests := []struct {
		name string
		key  tenantID
		want uint64
	}{
		{name: "zero", key: 0, want: 0},
		{name: "positive", key: 5, want: 2},
		{name: "negative", key: -1, want: 2},
		{name: "negative wraps", key: -5, want: 1},
		{name: "minimum int64 does not overflow", key: tenantID(-1 << 63), want: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, hasher.Hash(test.key))
		})
	}
}

func TestVShardRange(t *testing.T) {
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
			assert.Equal(t, test.want, pgmesh.VShardRange(test.from, test.to))
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

	assert.Equal(t, "replica0-read", replicaSet.Read())
	assert.Equal(t, "replica1-read", replicaSet.Read())
	assert.Equal(t, "replica0-read", replicaSet.Read())

	writer := replicaSet.Write()
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
	assert.Empty(t, base.Write().mirrors)
	assert.Equal(t, []string{"mirror0-write"}, writerNames(first.Write().mirrors))
	assert.Equal(
		t,
		[]string{"mirror0-write", "mirror1-write", "mirror2-write"},
		writerNames(second.Write().mirrors),
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
	mesh, err := pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
		WithTracerProvider(tracerProvider).
		WithMeterProvider(meterProvider).
		WithLogger(logger).
		WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).
		Link(0, replicaSet).
		Build()
	require.NoError(t, err)

	ctx, span := mesh.StartSpan(
		t.Context(),
		"UserStore",
		"CreateUser",
		pgmesh.QueryKindWrite,
	)
	assert.True(t, trace.SpanFromContext(ctx).SpanContext().IsValid())
	span.SetRoute(0, "main", pgmesh.RouteModePrimary)
	queryErr := errors.New("write failed")
	span.End(queryErr)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	recordedSpan := spans[0]
	assert.Equal(t, "pgmesh.query.UserStore.CreateUser", recordedSpan.Name())
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
	assert.NotContains(t, attributes, attribute.Key("pgmesh.route.vshard"))
	assert.Equal(t, "main", attributes[pgmesh.AttributeReplicaSet].AsString())
	assert.Equal(t, "primary", attributes[pgmesh.AttributeRouteMode].AsString())
	assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeInternalStoreExecuted))
	assert.NotContains(t, attributes, attribute.Key("pgmesh.route.write_mirror_count"))
	require.Len(t, recordedSpan.Events(), 1)
	assert.Equal(t, "exception", recordedSpan.Events()[0].Name)

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	require.Len(t, metrics.ScopeMetrics, 1)
	require.Len(t, metrics.ScopeMetrics[0].Metrics, 1)
	measurement := metrics.ScopeMetrics[0].Metrics[0]
	assert.Equal(t, pgmesh.MetricQueryDuration, measurement.Name)
	data, ok := measurement.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, data.DataPoints, 1)
	assert.Equal(t, uint64(1), data.DataPoints[0].Count)
	assert.GreaterOrEqual(t, data.DataPoints[0].Sum, 0.0)
	assertMetricAttributes(t, data.DataPoints[0].Attributes.ToSlice())

	var logRecord map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(logOutput.Bytes()), &logRecord))
	assert.Equal(t, "DEBUG", logRecord["level"])
	assert.Equal(t, "pgmesh query completed", logRecord["msg"])
	assert.Equal(t, "UserStore", logRecord["store_name"])
	assert.Equal(t, "CreateUser", logRecord["query_name"])
	assert.Equal(t, "write", logRecord["query_kind"])
	assert.Equal(t, true, logRecord["failed"])
	assert.Equal(t, "0", logRecord["vshard"])
	assert.Equal(t, "main", logRecord["replica_set"])
	assert.Equal(t, "primary", logRecord["route_mode"])
	assert.NotContains(t, logRecord, "internal_store_executed")
	assert.NotContains(t, logRecord, "write_mirror_count")
	assert.Equal(t, queryErr.Error(), logRecord["error"])
	assert.Contains(t, logRecord, "duration")
}

func TestQueryTelemetryRecordsSuccessfulQueries(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })

	mesh, err := pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
		WithTracerProvider(tracerProvider).
		WithMeterProvider(meterProvider).
		WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).
		Link(0, pgmesh.NewReplicaSet("main", node("main"), nil)).
		Build()
	require.NoError(t, err)

	_, span := mesh.StartSpan(t.Context(), "UserStore", "GetUser", pgmesh.QueryKindRead)
	span.SetRoute(0, "main", pgmesh.RouteModeRead)
	span.End(nil)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, codes.Unset, spans[0].Status().Code)
	assert.Empty(t, spans[0].Events())
	spanAttributes := attributeMap(spans[0].Attributes())
	assert.NotContains(t, spanAttributes, attribute.Key("error.type"))
	assert.Equal(t, "UserStore", spanAttributes[pgmesh.AttributeStoreName].AsString())
	assert.Equal(t, "read", spanAttributes[pgmesh.AttributeRouteMode].AsString())
	assert.NotContains(t, spanAttributes, attribute.Key(pgmesh.AttributeInternalStoreExecuted))

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	require.Len(t, metrics.ScopeMetrics, 1)
	require.Len(t, metrics.ScopeMetrics[0].Metrics, 1)
	data, ok := metrics.ScopeMetrics[0].Metrics[0].Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, data.DataPoints, 1)
	metricAttributes := attributeMap(data.DataPoints[0].Attributes.ToSlice())
	assert.NotContains(t, metricAttributes, attribute.Key("error.type"))
	assert.Equal(t, "UserStore", metricAttributes[pgmesh.AttributeStoreName].AsString())
	assert.Equal(t, "read", metricAttributes[pgmesh.AttributeRouteMode].AsString())
	assert.NotContains(t, metricAttributes, attribute.Key(pgmesh.AttributeInternalStoreExecuted))
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

	mesh, err := pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
		WithTracerProvider(tracerProvider).
		WithMeterProvider(meterProvider).
		WithLogger(logger).
		WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).
		Link(0, pgmesh.NewReplicaSet("main", node("main"), nil)).
		Build()
	require.NoError(t, err)

	_, storeSpan := mesh.StartStoreSpan(t.Context(), "Users", "GetUser", pgmesh.QueryKindRead)
	storeSpan.End(nil)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "pgmesh.store.Users.GetUser", spans[0].Name())
	attributes := attributeMap(spans[0].Attributes())
	assert.Equal(t, "Users", attributes[pgmesh.AttributeStoreName].AsString())
	assert.Equal(t, "GetUser", attributes[pgmesh.AttributeQueryName].AsString())
	assert.False(t, attributes[pgmesh.AttributeInternalStoreExecuted].AsBool())
	assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeReplicaSet))
	assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeRouteMode))

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	histogram := metricHistogram(t, metrics, pgmesh.MetricStoreDuration)
	require.Len(t, histogram.DataPoints, 1)
	metricAttributes := attributeMap(histogram.DataPoints[0].Attributes.ToSlice())
	assert.False(t, metricAttributes[pgmesh.AttributeInternalStoreExecuted].AsBool())
	assert.Equal(t, "Users", metricAttributes[pgmesh.AttributeStoreName].AsString())

	var logRecord map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(logOutput.Bytes()), &logRecord))
	assert.Equal(t, "pgmesh store completed", logRecord["msg"])
	assert.Equal(t, "Users", logRecord["store_name"])
	assert.Equal(t, "GetUser", logRecord["query_name"])
	assert.Equal(t, false, logRecord["internal_store_executed"])
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

	mesh, err := pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
		WithTracerProvider(tracerProvider).
		WithMeterProvider(meterProvider).
		WithLogger(logger).
		WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).
		Link(0, pgmesh.NewReplicaSet("main", node("main"), nil)).
		Build()
	require.NoError(t, err)

	ctx, storeSpan := mesh.StartStoreSpan(t.Context(), "Users", "GetUser", pgmesh.QueryKindRead)
	_, querySpan := mesh.StartSpan(ctx, "Users", "GetUser", pgmesh.QueryKindRead)
	querySpan.SetRoute(0, "main", pgmesh.RouteModeRead)
	querySpan.End(nil)
	storeSpan.End(nil)

	spans := recorder.Ended()
	require.Len(t, spans, 2)
	query := spans[0]
	store := spans[1]
	assert.Equal(t, "pgmesh.query.Users.GetUser", query.Name())
	assert.Equal(t, "pgmesh.store.Users.GetUser", store.Name())
	assert.Equal(t, store.SpanContext().SpanID(), query.Parent().SpanID())
	queryAttributes := attributeMap(query.Attributes())
	assert.Equal(t, "main", queryAttributes[pgmesh.AttributeReplicaSet].AsString())
	assert.NotContains(t, queryAttributes, attribute.Key(pgmesh.AttributeInternalStoreExecuted))
	storeAttributes := attributeMap(store.Attributes())
	assert.True(t, storeAttributes[pgmesh.AttributeInternalStoreExecuted].AsBool())
	assert.NotContains(t, storeAttributes, attribute.Key(pgmesh.AttributeReplicaSet))
	assert.NotContains(t, storeAttributes, attribute.Key(pgmesh.AttributeRouteMode))

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	storeHistogram := metricHistogram(t, metrics, pgmesh.MetricStoreDuration)
	require.Len(t, storeHistogram.DataPoints, 1)
	storeMetricAttributes := attributeMap(storeHistogram.DataPoints[0].Attributes.ToSlice())
	assert.True(t, storeMetricAttributes[pgmesh.AttributeInternalStoreExecuted].AsBool())
	queryHistogram := metricHistogram(t, metrics, pgmesh.MetricQueryDuration)
	require.Len(t, queryHistogram.DataPoints, 1)
	queryMetricAttributes := attributeMap(queryHistogram.DataPoints[0].Attributes.ToSlice())
	assert.Equal(t, "main", queryMetricAttributes[pgmesh.AttributeReplicaSet].AsString())
	assert.NotContains(t, queryMetricAttributes, attribute.Key(pgmesh.AttributeInternalStoreExecuted))

	logLines := strings.Split(strings.TrimSpace(logOutput.String()), "\n")
	require.Len(t, logLines, 2)
	var queryLog map[string]any
	require.NoError(t, json.Unmarshal([]byte(logLines[0]), &queryLog))
	assert.Equal(t, "pgmesh query completed", queryLog["msg"])
	assert.Equal(t, "main", queryLog["replica_set"])
	assert.NotContains(t, queryLog, "internal_store_executed")
	var storeLog map[string]any
	require.NoError(t, json.Unmarshal([]byte(logLines[1]), &storeLog))
	assert.Equal(t, "pgmesh store completed", storeLog["msg"])
	assert.Equal(t, true, storeLog["internal_store_executed"])
	assert.NotContains(t, storeLog, "replica_set")
}

func TestStoreTelemetryRecordsWrapperErrorsWithoutInternalExecution(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })

	mesh, err := pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
		WithTracerProvider(tracerProvider).
		WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).
		Link(0, pgmesh.NewReplicaSet("main", node("main"), nil)).
		Build()
	require.NoError(t, err)

	_, storeSpan := mesh.StartStoreSpan(t.Context(), "Users", "GetUser", pgmesh.QueryKindRead)
	wrapperErr := errors.New("cache unavailable")
	storeSpan.End(wrapperErr)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, codes.Error, spans[0].Status().Code)
	attributes := attributeMap(spans[0].Attributes())
	assert.False(t, attributes[pgmesh.AttributeInternalStoreExecuted].AsBool())
	assert.Equal(t, "*errors.errorString", attributes[attribute.Key("error.type")].AsString())
}

func TestStoreTelemetryExecutionMarkerIsMeshScoped(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })

	mesh, err := pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
		WithTracerProvider(tracerProvider).
		WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).
		Link(0, pgmesh.NewReplicaSet("main", node("main"), nil)).
		Build()
	require.NoError(t, err)
	otherMesh, err := pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
		WithTracerProvider(tracerProvider).
		WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).
		Link(0, pgmesh.NewReplicaSet("other", node("other"), nil)).
		Build()
	require.NoError(t, err)

	ctx, storeSpan := mesh.StartStoreSpan(t.Context(), "Users", "GetUser", pgmesh.QueryKindRead)
	_, querySpan := otherMesh.StartSpan(ctx, "Users", "GetUser", pgmesh.QueryKindRead)
	querySpan.End(nil)
	storeSpan.End(nil)

	spans := recorder.Ended()
	require.Len(t, spans, 2)
	storeAttributes := attributeMap(spans[1].Attributes())
	assert.False(t, storeAttributes[pgmesh.AttributeInternalStoreExecuted].AsBool())
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

	mesh, err := pgmesh.NewBuilder[string, *fakeWriter, uint64](2).
		WithTracerProvider(tracerProvider).
		WithMeterProvider(meterProvider).
		WithLogger(logger).
		WithHasher(pgmesh.ModularShardHashFor[uint64](2)).
		Link(0, pgmesh.NewReplicaSet("zero", node("zero"), nil)).
		Link(1, pgmesh.NewReplicaSet("one", node("one"), nil)).
		Build()
	require.NoError(t, err)

	_, span := mesh.StartSpan(t.Context(), "UserStore", "DeleteAll", pgmesh.QueryKindWrite)
	span.SetMultiRoute(pgmesh.RouteModePrimary)
	span.End(nil)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	attributes := attributeMap(spans[0].Attributes())
	assert.Equal(t, "primary", attributes[pgmesh.AttributeRouteMode].AsString())
	assert.NotContains(t, attributes, attribute.Key("pgmesh.route.physical_shard_count"))
	assert.NotContains(t, attributes, attribute.Key("pgmesh.route.write_mirror_count"))
	assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeReplicaSet))

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	require.Len(t, metrics.ScopeMetrics, 1)
	require.Len(t, metrics.ScopeMetrics[0].Metrics, 1)
	data, ok := metrics.ScopeMetrics[0].Metrics[0].Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, data.DataPoints, 1)
	metricAttributes := attributeMap(data.DataPoints[0].Attributes.ToSlice())
	assert.Equal(t, "primary", metricAttributes[pgmesh.AttributeRouteMode].AsString())
	assert.NotContains(t, metricAttributes, attribute.Key("pgmesh.route.physical_shard_count"))
	assert.NotContains(t, metricAttributes, attribute.Key("pgmesh.route.write_mirror_count"))
	assert.NotContains(t, metricAttributes, attribute.Key(pgmesh.AttributeReplicaSet))

	var logRecord map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(logOutput.Bytes()), &logRecord))
	assert.NotContains(t, logRecord, "physical_shard_count")
	assert.NotContains(t, logRecord, "write_mirror_count")
	assert.NotContains(t, logRecord, "vshard")
	assert.NotContains(t, logRecord, "replica_set")
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
	assert.NotContains(t, attributes, attribute.Key("pgmesh.route.vshard"))
	assert.Equal(t, "main", attributes[pgmesh.AttributeReplicaSet].AsString())
	assert.Equal(t, "primary", attributes[pgmesh.AttributeRouteMode].AsString())
	assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeInternalStoreExecuted))
	assert.NotContains(t, attributes, attribute.Key("pgmesh.route.write_mirror_count"))
}

func TestReplicaSetFallsBackToPrimaryReader(t *testing.T) {
	t.Parallel()

	replicaSet := pgmesh.NewReplicaSet("main", node("primary"), nil)
	assert.Equal(t, "primary-read", replicaSet.Read())
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
			results <- replicaSet.Read()
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

func TestBuilderRoutesAndListsPhysicalShardsDeterministically(t *testing.T) {
	t.Parallel()

	shardA := pgmesh.NewReplicaSet("a", node("a"), nil)
	shardB := pgmesh.NewReplicaSet("b", node("b"), nil)
	mesh, err := pgmesh.NewBuilder[string, *fakeWriter, uint64](4).
		WithHasher(pgmesh.ModularShardHashFor[uint64](4)).
		Link(0, shardB).
		Link(1, shardA).
		Link(2, shardB).
		Link(3, shardA).
		Build()
	require.NoError(t, err)

	routed, err := mesh.Shard(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), routed.VShardIndex())
	assert.Equal(t, "a", routed.Name())

	all := mesh.AllShards()
	require.Len(t, all, 2)
	assert.Equal(t, "b", all[0].Name())
	assert.Equal(t, "a", all[1].Name())
	all[0] = nil
	assert.NotNil(t, mesh.AllShards()[0], "AllShards must return a defensive slice")
}

func TestMeshRejectsOutOfRangeHasherResult(t *testing.T) {
	t.Parallel()

	mesh, err := pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
		WithHasher(pgmesh.ConstantShardHashFor[uint64](2)).
		Link(0, pgmesh.NewReplicaSet("main", node("main"), nil)).
		Build()
	require.NoError(t, err)

	_, err = mesh.Shard(1)
	assert.ErrorIs(t, err, pgmesh.ErrVShardOutOfRange)
}

func TestBuilderValidation(t *testing.T) {
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
				return pgmesh.NewBuilder[string, *fakeWriter, uint64](0).
					WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).Build()
			},
			want: pgmesh.ErrNoVShards,
		},
		{
			name: "no hasher",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewBuilder[string, *fakeWriter, uint64](1).Link(0, replicaSet).Build()
			},
			want: pgmesh.ErrNoShardHasher,
		},
		{
			name: "missing virtual shard",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
					WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).Build()
			},
			want: pgmesh.ErrMissingVShard,
		},
		{
			name: "duplicate virtual shard",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
					WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).
					Link(0, replicaSet).Link(0, replicaSet).Build()
			},
			want: pgmesh.ErrDuplicateVShard,
		},
		{
			name: "link out of range",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
					WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).Link(1, replicaSet).Build()
			},
			want: pgmesh.ErrVShardOutOfRange,
		},
		{
			name: "empty replica set name",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
					WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).
					Link(0, pgmesh.NewReplicaSet("", node("main"), nil)).Build()
			},
			want: pgmesh.ErrEmptyReplicaSetName,
		},
		{
			name: "nil replica set",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
					WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).Link(0, nil).Build()
			},
			want: pgmesh.ErrNilReplicaSet,
		},
		{
			name: "duplicate physical name",
			make: func() (*pgmesh.Mesh[string, *fakeWriter, uint64], error) {
				return pgmesh.NewBuilder[string, *fakeWriter, uint64](2).
					WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).
					Link(0, pgmesh.NewReplicaSet("same", node("a"), nil)).
					Link(1, pgmesh.NewReplicaSet("same", node("b"), nil)).Build()
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

func TestBuilderPreservesFirstError(t *testing.T) {
	t.Parallel()

	meterErr := errors.New("meter initialization failed")
	builder := pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
		WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).
		Link(1, pgmesh.NewReplicaSet("main", node("main"), nil)).
		WithMeterProvider(failingMeterProvider{err: meterErr}).
		Link(0, nil)

	_, err := builder.Build()
	require.ErrorIs(t, err, pgmesh.ErrVShardOutOfRange)
	assert.NotErrorIs(t, err, meterErr)
}

func TestBuilderReportsMeterInitializationFailure(t *testing.T) {
	t.Parallel()

	meterErr := errors.New("meter initialization failed")
	_, err := pgmesh.NewBuilder[string, *fakeWriter, uint64](1).
		WithHasher(pgmesh.ConstantShardHashFor[uint64](0)).
		WithMeterProvider(failingMeterProvider{err: meterErr}).
		Link(0, pgmesh.NewReplicaSet("main", node("main"), nil)).
		Build()

	require.ErrorIs(t, err, meterErr)
	assert.ErrorContains(t, err, "configure OpenTelemetry metrics")
}

func TestCreateMeshBuildsTopologyAndMirrors(t *testing.T) {
	t.Parallel()

	created := make([]string, 0)
	mesh, err := pgmesh.CreateMesh(
		t.Context(),
		2,
		func(_ context.Context, dsn string) (pgmesh.Node[string, *fakeWriter], error) {
			created = append(created, dsn)
			return node(dsn), nil
		},
		pgmesh.ModularShardHashFor[uint64](2),
		pgmesh.WithReplicaSet("a", "a-primary", "a-replica"),
		pgmesh.WithReplicaSet("b", "b-primary"),
		pgmesh.WithVShardMapping("a", []uint64{0}, "b"),
		pgmesh.WithVShardMapping("b", []uint64{1}),
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"a-primary", "a-replica", "b-primary"}, created)

	shard, err := mesh.Shard(0)
	require.NoError(t, err)
	assert.Equal(t, "a-replica-read", shard.Read())
	writer := shard.Write()
	require.Len(t, writer.mirrors, 1)
	assert.Equal(t, "b-primary-write", writer.mirrors[0].name)
}

func TestMeshOptionsCloneInputs(t *testing.T) {
	t.Parallel()

	replicaDSNs := []string{"replica"}
	vshards := []uint64{0}
	mirrors := []string{"mirror"}
	replicaSetOption := pgmesh.WithReplicaSet("main", "primary", replicaDSNs...)
	mappingOption := pgmesh.WithVShardMapping("main", vshards, mirrors...)

	replicaDSNs[0] = ""
	vshards[0] = 1
	mirrors[0] = "missing"

	created := make([]string, 0)
	_, err := pgmesh.CreateMesh(
		t.Context(),
		1,
		func(_ context.Context, dsn string) (pgmesh.Node[string, *fakeWriter], error) {
			created = append(created, dsn)
			return node(dsn), nil
		},
		pgmesh.ConstantShardHashFor[uint64](0),
		replicaSetOption,
		pgmesh.WithReplicaSet("mirror", "mirror-primary"),
		mappingOption,
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"primary", "replica", "mirror-primary"}, created)
}

func TestCreateMeshValidation(t *testing.T) {
	t.Parallel()

	type input struct {
		numVShards uint64
		createNode pgmesh.NodeFactory[string, *fakeWriter]
		hasher     pgmesh.ShardHasher[uint64]
		options    []pgmesh.MeshOption
	}
	valid := func() input {
		return input{
			numVShards: 1,
			createNode: func(context.Context, string) (pgmesh.Node[string, *fakeWriter], error) {
				return node("main"), nil
			},
			hasher: pgmesh.ConstantShardHashFor[uint64](0),
			options: []pgmesh.MeshOption{
				pgmesh.WithReplicaSet("main", "primary"),
				pgmesh.WithVShardMapping("main", []uint64{0}),
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
				config.options = []pgmesh.MeshOption{
					pgmesh.WithVShardMapping("main", []uint64{0}),
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
			name: "no factory",
			edit: func(config *input) { config.createNode = nil },
			want: pgmesh.ErrNoNodeFactory,
		},
		{
			name: "no hasher",
			edit: func(config *input) { config.hasher = nil },
			want: pgmesh.ErrNoShardHasher,
		},
		{
			name: "no virtual shards",
			edit: func(config *input) { config.numVShards = 0 },
			want: pgmesh.ErrNoVShards,
		},
		{name: "unknown main", edit: func(config *input) {
			config.options[1] = pgmesh.WithVShardMapping("missing", []uint64{0})
		}, want: pgmesh.ErrUnknownReplicaSet},
		{name: "unknown mirror", edit: func(config *input) {
			config.options[1] = pgmesh.WithVShardMapping("main", []uint64{0}, "missing")
		}, want: pgmesh.ErrUnknownReplicaSet},
		{
			name: "self mirror",
			edit: func(config *input) {
				config.options[1] = pgmesh.WithVShardMapping("main", []uint64{0}, "main")
			},
			want: pgmesh.ErrMirrorConfiguration,
		},
		{
			name: "duplicate mirror",
			edit: func(config *input) {
				config.options = []pgmesh.MeshOption{
					pgmesh.WithReplicaSet("main", "primary"),
					pgmesh.WithReplicaSet("mirror", "mirror"),
					pgmesh.WithVShardMapping("main", []uint64{0}, "mirror", "mirror"),
				}
			},
			want: pgmesh.ErrMirrorConfiguration,
		},
		{
			name: "missing vshard",
			edit: func(config *input) {
				config.options = config.options[:1]
			},
			want: pgmesh.ErrMissingVShard,
		},
		{name: "duplicate vshard", edit: func(config *input) {
			config.options = append(config.options, pgmesh.WithVShardMapping("main", []uint64{0}))
		}, want: pgmesh.ErrDuplicateVShard},
		{
			name: "out of range",
			edit: func(config *input) {
				config.options[1] = pgmesh.WithVShardMapping("main", []uint64{1})
			},
			want: pgmesh.ErrVShardOutOfRange,
		},
		{
			name: "inconsistent mirrors",
			edit: func(config *input) {
				config.numVShards = 2
				config.options = []pgmesh.MeshOption{
					pgmesh.WithReplicaSet("main", "primary"),
					pgmesh.WithReplicaSet("mirror", "mirror"),
					pgmesh.WithVShardMapping("main", []uint64{0}),
					pgmesh.WithVShardMapping("main", []uint64{1}, "mirror"),
				}
			},
			want: pgmesh.ErrMirrorConfiguration,
		},
		{
			name: "inconsistent mirror order",
			edit: func(config *input) {
				config.numVShards = 2
				config.options = []pgmesh.MeshOption{
					pgmesh.WithReplicaSet("main", "primary"),
					pgmesh.WithReplicaSet("mirror-a", "mirror-a"),
					pgmesh.WithReplicaSet("mirror-b", "mirror-b"),
					pgmesh.WithVShardMapping("main", []uint64{0}, "mirror-a", "mirror-b"),
					pgmesh.WithVShardMapping("main", []uint64{1}, "mirror-b", "mirror-a"),
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
			_, err := pgmesh.CreateMesh(
				t.Context(),
				config.numVShards,
				config.createNode,
				config.hasher,
				config.options...,
			)
			assert.ErrorIs(t, err, test.want)
		})
	}

	_, err := pgmesh.CreateMesh(
		t.Context(),
		1,
		valid().createNode,
		valid().hasher,
		pgmesh.WithReplicaSet("main", "primary"),
		pgmesh.WithVShardMapping("main", []uint64{0}),
		nil,
	)
	assert.ErrorContains(t, err, "mesh option 2 is nil")
}

func TestCreateMeshWrapsFactoryError(t *testing.T) {
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
			_, err := pgmesh.CreateMesh(
				t.Context(),
				1,
				test.factory,
				pgmesh.ConstantShardHashFor[uint64](0),
				pgmesh.WithReplicaSet("main", "primary", test.replicas...),
				pgmesh.WithVShardMapping("main", []uint64{0}),
			)
			require.ErrorIs(t, err, sentinel)
			assert.ErrorContains(t, err, test.want)
		})
	}
}
