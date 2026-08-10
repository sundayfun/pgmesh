package pgmesh

import "fmt"

// Mesh routes logical shard keys through virtual shards to physical replica
// sets. Its topology is immutable after construction and safe for concurrent
// use.
type Mesh[R any, W MirrorableWriter[W], K any] struct {
	virtualShards []*ReplicaSet[R, W]
	replicaSets   []*ReplicaSet[R, W]
	hasher        ShardHasher[K]
	telemetry     meshTelemetry
}

// Resolve maps key to its virtual shard and physical replica set.
func (m *Mesh[R, W, K]) Resolve(key K) (ResolvedShard[R, W], error) {
	virtualShardIndex := m.hasher.Hash(key)
	if virtualShardIndex >= uint64(len(m.virtualShards)) {
		return ResolvedShard[R, W]{}, fmt.Errorf(
			"%w: got %d, valid range is [0,%d)",
			ErrVirtualShardOutOfRange,
			virtualShardIndex,
			len(m.virtualShards),
		)
	}
	return ResolvedShard[R, W]{
		virtualShardIndex: virtualShardIndex,
		replicaSet:        m.virtualShards[virtualShardIndex],
	}, nil
}

// ReplicaSets returns the mesh's physical replica sets, ordered by the first
// virtual shard mapped to each one.
func (m *Mesh[R, W, K]) ReplicaSets() []*ReplicaSet[R, W] {
	return append([]*ReplicaSet[R, W](nil), m.replicaSets...)
}
