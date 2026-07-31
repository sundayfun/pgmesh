package pgmesh

import "fmt"

type virtualShard[R any, W Mirrorable[W]] struct {
	index      uint64
	replicaSet *ReplicaSet[R, W]
}

// Shard is a routed virtual shard linked to a physical replica set.
type Shard[R any, W Mirrorable[W]] struct {
	*ReplicaSet[R, W]

	vshardIndex uint64
}

// NodeRole identifies the database node selected for a physical query.
type NodeRole string

const (
	// NodeRolePrimary identifies a replica set's writable primary node.
	NodeRolePrimary NodeRole = "primary"
	// NodeRoleReadReplica identifies a read-only replica node.
	NodeRoleReadReplica NodeRole = "read_replica"
	// NodeRoleTransaction identifies an externally supplied transaction whose
	// underlying database node is not observable by pgmesh.
	NodeRoleTransaction NodeRole = "transaction"
)

// Route is one fully resolved physical database target. Node is stable within
// a replica set: "primary" for its primary and "replica-N" for the Nth
// configured read replica.
type Route[T any] struct {
	Target       T
	VirtualShard uint64
	Shard        string
	Node         string
	Role         NodeRole
}

// RouteMetadata is the identity of a resolved database target.
type RouteMetadata struct {
	VirtualShard    uint64
	HasVirtualShard bool
	Shard           string
	Node            string
	Role            NodeRole
}

// Metadata returns the target identity used by telemetry.
func (r Route[T]) Metadata() RouteMetadata {
	return RouteMetadata{
		VirtualShard:    r.VirtualShard,
		HasVirtualShard: true,
		Shard:           r.Shard,
		Node:            r.Node,
		Role:            r.Role,
	}
}

// WithoutVirtualShard removes a virtual-shard attribution when one physical
// query combines work from several virtual shards or scans a physical shard.
func (r RouteMetadata) WithoutVirtualShard() RouteMetadata {
	r.HasVirtualShard = false
	return r
}

// VShardIndex returns the virtual shard index used to select this shard.
func (s *Shard[R, W]) VShardIndex() uint64 {
	return s.vshardIndex
}

// ReadRoute selects and describes the next read target. Callers must execute
// the query through the returned Target so selection and telemetry agree.
func (s *Shard[R, W]) ReadRoute() Route[R] {
	target := s.selectRead()
	return Route[R]{
		Target:       target.value,
		VirtualShard: s.vshardIndex,
		Shard:        s.Name(),
		Node:         target.name,
		Role:         target.role,
	}
}

// WriteRoute selects and describes the primary write target.
func (s *Shard[R, W]) WriteRoute() Route[W] {
	target := s.selectWrite()
	return Route[W]{
		Target:       target.value,
		VirtualShard: s.vshardIndex,
		Shard:        s.Name(),
		Node:         target.name,
		Role:         target.role,
	}
}

// Mesh routes logical shard keys through virtual shards to physical replica
// sets. Its topology is immutable after construction and safe for concurrent
// use.
type Mesh[R any, W Mirrorable[W], SK any] struct {
	vshards   []virtualShard[R, W]
	physical  []*Shard[R, W]
	hasher    ShardHasher[SK]
	telemetry queryTelemetry
}

// Shard resolves key to its virtual shard and physical replica set.
func (m *Mesh[R, W, SK]) Shard(key SK) (*Shard[R, W], error) {
	index := m.hasher.Hash(key)
	if index >= uint64(len(m.vshards)) {
		return nil, fmt.Errorf("%w: got %d, valid range is [0,%d)", ErrVShardOutOfRange, index, len(m.vshards))
	}
	vshard := m.vshards[index]
	return &Shard[R, W]{vshardIndex: index, ReplicaSet: vshard.replicaSet}, nil
}

// AllShards returns one entry per physical replica set in first-vshard order.
func (m *Mesh[R, W, SK]) AllShards() []*Shard[R, W] {
	return append([]*Shard[R, W](nil), m.physical...)
}
