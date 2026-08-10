package pgmesh

import (
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// MeshBuilder incrementally assembles and validates an immutable Mesh from
// existing nodes. A builder is intended for single-goroutine setup; the Mesh
// returned by Build can be shared when its configured nodes can be shared.
type MeshBuilder[R any, W MirrorableWriter[W], K any] struct {
	virtualShards []*ReplicaSet[R, W]
	hasher        ShardHasher[K]
	telemetry     meshTelemetry
	err           error
}

// NewMeshBuilder creates a builder with virtualShardCount unmapped virtual
// shards.
func NewMeshBuilder[R any, W MirrorableWriter[W], K any](virtualShardCount uint64) *MeshBuilder[R, W, K] {
	telemetry, telemetryErr := newMeshTelemetry(nil, nil)
	return &MeshBuilder[R, W, K]{
		virtualShards: make([]*ReplicaSet[R, W], virtualShardCount),
		hasher:        nil,
		telemetry:     telemetry,
		err:           telemetryErr,
	}
}

// WithTracerProvider configures the provider used for routed query spans.
// A nil provider uses the global OpenTelemetry tracer provider.
func (b *MeshBuilder[R, W, K]) WithTracerProvider(provider trace.TracerProvider) *MeshBuilder[R, W, K] {
	b.telemetry.setTracerProvider(provider)
	return b
}

// WithMeterProvider configures the provider used for routed query metrics.
// A nil provider uses the global OpenTelemetry meter provider.
func (b *MeshBuilder[R, W, K]) WithMeterProvider(provider metric.MeterProvider) *MeshBuilder[R, W, K] {
	if err := b.telemetry.setMeterProvider(provider); err != nil && b.err == nil {
		b.err = fmt.Errorf("configure OpenTelemetry metrics: %w", err)
	}
	return b
}

// WithLogger configures optional structured logging for routed queries.
// Completed queries are logged at Debug level. A nil logger disables logging.
func (b *MeshBuilder[R, W, K]) WithLogger(logger *slog.Logger) *MeshBuilder[R, W, K] {
	b.telemetry.logger = logger
	return b
}

// WithShardHasher configures the mapping from shard keys to virtual shards.
func (b *MeshBuilder[R, W, K]) WithShardHasher(hasher ShardHasher[K]) *MeshBuilder[R, W, K] {
	b.hasher = hasher
	return b
}

// MapVirtualShard assigns one virtual shard to a replica set. It records the
// first validation failure so topology setup remains fluent without panics.
func (b *MeshBuilder[R, W, K]) MapVirtualShard(
	virtualShardIndex uint64,
	replicaSet *ReplicaSet[R, W],
) *MeshBuilder[R, W, K] {
	if b.err != nil {
		return b
	}
	if virtualShardIndex >= uint64(len(b.virtualShards)) {
		b.err = fmt.Errorf("%w: %d", ErrVirtualShardOutOfRange, virtualShardIndex)
		return b
	}
	if replicaSet == nil {
		b.err = fmt.Errorf("%w: virtual shard %d", ErrNilReplicaSet, virtualShardIndex)
		return b
	}
	if b.virtualShards[virtualShardIndex] != nil {
		b.err = fmt.Errorf("%w: %d", ErrVirtualShardAlreadyMapped, virtualShardIndex)
		return b
	}
	b.virtualShards[virtualShardIndex] = replicaSet
	return b
}

// Build validates the topology and returns an immutable mesh. It retains the
// configured node and telemetry providers; callers remain responsible for
// shutting down database pools and OpenTelemetry SDK providers.
func (b *MeshBuilder[R, W, K]) Build() (*Mesh[R, W, K], error) {
	if b.err != nil {
		return nil, b.err
	}
	if len(b.virtualShards) == 0 {
		return nil, ErrNoVirtualShards
	}
	if b.hasher == nil {
		return nil, ErrNilShardHasher
	}

	virtualShards := make([]*ReplicaSet[R, W], len(b.virtualShards))
	replicaSets := make([]*ReplicaSet[R, W], 0)
	seen := make(map[*ReplicaSet[R, W]]struct{})
	seenNames := make(map[string]*ReplicaSet[R, W])
	for index, replicaSet := range b.virtualShards {
		if replicaSet == nil {
			return nil, fmt.Errorf("%w: %d", ErrVirtualShardNotMapped, index)
		}
		if strings.TrimSpace(replicaSet.Name()) == "" {
			return nil, fmt.Errorf("%w: virtual shard %d", ErrEmptyReplicaSetName, index)
		}
		if previous, ok := seenNames[replicaSet.Name()]; ok && previous != replicaSet {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateReplicaSet, replicaSet.Name())
		}
		seenNames[replicaSet.Name()] = replicaSet
		virtualShards[index] = replicaSet
		if _, ok := seen[replicaSet]; !ok {
			seen[replicaSet] = struct{}{}
			replicaSets = append(replicaSets, replicaSet)
		}
	}

	return &Mesh[R, W, K]{
		virtualShards: virtualShards,
		replicaSets:   replicaSets,
		hasher:        b.hasher,
		telemetry:     b.telemetry,
	}, nil
}
