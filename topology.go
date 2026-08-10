package pgmesh

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// NodeOpener opens the database node identified by a DSN. Nodes and their
// underlying pools remain caller-owned; pgmesh does not close them.
type NodeOpener[R any, W MirrorableWriter[W]] func(
	ctx context.Context,
	dsn string,
) (Node[R, W], error)

type replicaSetSpec struct {
	name        string
	primaryDSN  string
	replicaDSNs []string
}

type virtualShardMapping struct {
	virtualShardIndexes []uint64
	mainReplicaSet      string
	mirrorReplicaSets   []string
}

type openMeshConfig struct {
	replicaSets       []replicaSetSpec
	virtualShardCount uint64
	mappings          []virtualShardMapping
	tracerProvider    trace.TracerProvider
	meterProvider     metric.MeterProvider
	logger            *slog.Logger
}

// OpenMeshOption configures OpenMesh.
type OpenMeshOption func(config *openMeshConfig)

// WithReplicaSet registers a named primary and its optional read replicas.
// Repeated calls append replica sets in call order.
func WithReplicaSet(name, primaryDSN string, replicaDSNs ...string) OpenMeshOption {
	replicas := append([]string(nil), replicaDSNs...)
	return func(config *openMeshConfig) {
		spec := replicaSetSpec{
			name:        name,
			primaryDSN:  primaryDSN,
			replicaDSNs: replicas,
		}
		config.replicaSets = append(config.replicaSets, spec)
	}
}

// WithVirtualShardMapping maps virtual shards to a main replica set and optional
// ordered write mirrors. Repeated calls append mappings in call order.
func WithVirtualShardMapping(
	mainReplicaSet string,
	virtualShardIndexes []uint64,
	mirrorReplicaSets ...string,
) OpenMeshOption {
	indexes := append([]uint64(nil), virtualShardIndexes...)
	mirrorNames := append([]string(nil), mirrorReplicaSets...)
	return func(config *openMeshConfig) {
		config.mappings = append(config.mappings, virtualShardMapping{
			virtualShardIndexes: indexes,
			mainReplicaSet:      mainReplicaSet,
			mirrorReplicaSets:   mirrorNames,
		})
	}
}

// WithTracerProvider configures the provider used for routed query spans.
// A nil provider uses the global OpenTelemetry tracer provider.
func WithTracerProvider(provider trace.TracerProvider) OpenMeshOption {
	return func(config *openMeshConfig) {
		config.tracerProvider = provider
	}
}

// WithMeterProvider configures the provider used for routed query metrics.
// A nil provider uses the global OpenTelemetry meter provider.
func WithMeterProvider(provider metric.MeterProvider) OpenMeshOption {
	return func(config *openMeshConfig) {
		config.meterProvider = provider
	}
}

// WithLogger configures optional structured logging for routed queries.
// A nil logger disables logging.
func WithLogger(logger *slog.Logger) OpenMeshOption {
	return func(config *openMeshConfig) {
		config.logger = logger
	}
}

// VirtualShardRange returns the half-open virtual shard range [from, to).
func VirtualShardRange(from, to uint64) []uint64 {
	if to <= from {
		return []uint64{}
	}
	out := make([]uint64, 0, to-from)
	for index := from; index < to; index++ {
		out = append(out, index)
	}
	return out
}

// OpenMesh validates its configuration, opens its database nodes, and builds
// an immutable mesh. It calls openNode once for each primary and replica, in
// option order, and stops at the first error. Successfully created nodes are
// not closed on a later error and remain caller-owned.
func OpenMesh[R any, W MirrorableWriter[W], K any](
	ctx context.Context,
	virtualShardCount uint64,
	openNode NodeOpener[R, W],
	shardHasher ShardHasher[K],
	options ...OpenMeshOption,
) (*Mesh[R, W, K], error) {
	config := openMeshConfig{
		replicaSets:       nil,
		virtualShardCount: virtualShardCount,
		mappings:          nil,
		tracerProvider:    nil,
		meterProvider:     nil,
		logger:            nil,
	}
	for index, option := range options {
		if option == nil {
			return nil, fmt.Errorf("pgmesh: mesh option %d is nil", index)
		}
		option(&config)
	}
	if err := validateMeshConfig(&config, openNode, shardHasher); err != nil {
		return nil, err
	}

	replicaSets := make(map[string]*ReplicaSet[R, W], len(config.replicaSets))
	for _, spec := range config.replicaSets {
		primary, err := openNode(ctx, spec.primaryDSN)
		if err != nil {
			return nil, fmt.Errorf("open primary node for replica set %q: %w", spec.name, err)
		}
		replicas := make([]Node[R, W], 0, len(spec.replicaDSNs))
		for _, replicaDSN := range spec.replicaDSNs {
			replica, err := openNode(ctx, replicaDSN)
			if err != nil {
				return nil, fmt.Errorf("open replica node for replica set %q: %w", spec.name, err)
			}
			replicas = append(replicas, replica)
		}
		replicaSets[spec.name] = NewReplicaSet(spec.name, primary, replicas)
	}

	configured := make(map[string]*ReplicaSet[R, W], len(replicaSets))
	for _, mapping := range config.mappings {
		if _, ok := configured[mapping.mainReplicaSet]; ok {
			continue
		}
		main := replicaSets[mapping.mainReplicaSet]
		mirrors := make([]W, 0, len(mapping.mirrorReplicaSets))
		for _, name := range mapping.mirrorReplicaSets {
			mirrors = append(mirrors, replicaSets[name].primaryWriter())
		}
		configured[mapping.mainReplicaSet] = main.WithWriteMirrors(mirrors...)
	}

	builder := NewMeshBuilder[R, W, K](config.virtualShardCount).
		WithShardHasher(shardHasher).
		WithTracerProvider(config.tracerProvider).
		WithMeterProvider(config.meterProvider).
		WithLogger(config.logger)
	for _, mapping := range config.mappings {
		for _, virtualShardIndex := range mapping.virtualShardIndexes {
			builder.MapVirtualShard(virtualShardIndex, configured[mapping.mainReplicaSet])
		}
	}
	return builder.Build()
}

func validateMeshConfig[R any, W MirrorableWriter[W], K any](
	config *openMeshConfig,
	openNode NodeOpener[R, W],
	shardHasher ShardHasher[K],
) error {
	if len(config.replicaSets) == 0 {
		return ErrNoReplicaSets
	}
	if openNode == nil {
		return ErrNilNodeOpener
	}
	if shardHasher == nil {
		return ErrNilShardHasher
	}
	if config.virtualShardCount == 0 {
		return ErrNoVirtualShards
	}

	names := make(map[string]struct{}, len(config.replicaSets))
	for _, spec := range config.replicaSets {
		if strings.TrimSpace(spec.name) == "" {
			return ErrEmptyReplicaSetName
		}
		if _, ok := names[spec.name]; ok {
			return fmt.Errorf("%w: %q", ErrDuplicateReplicaSet, spec.name)
		}
		names[spec.name] = struct{}{}
		if strings.TrimSpace(spec.primaryDSN) == "" {
			return fmt.Errorf("%w: primary of %q", ErrEmptyDSN, spec.name)
		}
		for index, replicaDSN := range spec.replicaDSNs {
			if strings.TrimSpace(replicaDSN) == "" {
				return fmt.Errorf("%w: replica %d of %q", ErrEmptyDSN, index, spec.name)
			}
		}
	}

	mapped := make([]bool, config.virtualShardCount)
	mirrorConfigurations := make(map[string]string)
	for _, mapping := range config.mappings {
		if _, ok := names[mapping.mainReplicaSet]; !ok {
			return fmt.Errorf("%w: main %q", ErrUnknownReplicaSet, mapping.mainReplicaSet)
		}
		seenMirrors := make(map[string]struct{}, len(mapping.mirrorReplicaSets))
		for _, mirror := range mapping.mirrorReplicaSets {
			if _, ok := names[mirror]; !ok {
				return fmt.Errorf("%w: mirror %q", ErrUnknownReplicaSet, mirror)
			}
			if mirror == mapping.mainReplicaSet {
				return fmt.Errorf("%w: replica set %q cannot mirror itself", ErrMirrorConfiguration, mirror)
			}
			if _, ok := seenMirrors[mirror]; ok {
				return fmt.Errorf("%w: duplicate mirror %q", ErrMirrorConfiguration, mirror)
			}
			seenMirrors[mirror] = struct{}{}
		}
		configuration := strings.Join(mapping.mirrorReplicaSets, "\x00")
		if previous, ok := mirrorConfigurations[mapping.mainReplicaSet]; ok && previous != configuration {
			return fmt.Errorf("%w for %q", ErrMirrorConfiguration, mapping.mainReplicaSet)
		}
		mirrorConfigurations[mapping.mainReplicaSet] = configuration

		for _, virtualShardIndex := range mapping.virtualShardIndexes {
			if virtualShardIndex >= config.virtualShardCount {
				return fmt.Errorf("%w: %d", ErrVirtualShardOutOfRange, virtualShardIndex)
			}
			if mapped[virtualShardIndex] {
				return fmt.Errorf("%w: %d", ErrVirtualShardAlreadyMapped, virtualShardIndex)
			}
			mapped[virtualShardIndex] = true
		}
	}
	for index, ok := range mapped {
		if !ok {
			return fmt.Errorf("%w: %d", ErrVirtualShardNotMapped, index)
		}
	}
	return nil
}
