package pgmesh

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

// Route is one resolved physical database target. Target is the database view
// that must execute the query; Metadata describes how it was selected.
type Route[T any] struct {
	Target   T
	metadata RouteMetadata
}

// RouteMetadata identifies a resolved database target without exposing it.
type RouteMetadata struct {
	VirtualShardIndex uint64
	HasVirtualShard   bool
	ReplicaSetName    string
	NodeName          string
	NodeRole          NodeRole
}

// Metadata returns the route identity used by telemetry.
func (r Route[T]) Metadata() RouteMetadata {
	return r.metadata
}

func (r Route[T]) withVirtualShard(virtualShardIndex uint64) Route[T] {
	r.metadata.VirtualShardIndex = virtualShardIndex
	r.metadata.HasVirtualShard = true
	return r
}

// WithoutVirtualShard removes virtual-shard attribution when one physical
// query combines work from several virtual shards or scans a physical shard.
func (r RouteMetadata) WithoutVirtualShard() RouteMetadata {
	r.VirtualShardIndex = 0
	r.HasVirtualShard = false
	return r
}
