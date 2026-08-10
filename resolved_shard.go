package pgmesh

// ResolvedShard binds a virtual shard selected from a key to its physical
// replica set.
type ResolvedShard[R any, W MirrorableWriter[W]] struct {
	virtualShardIndex uint64
	replicaSet        *ReplicaSet[R, W]
}

// VirtualShardIndex returns the resolved virtual shard index.
func (s ResolvedShard[R, W]) VirtualShardIndex() uint64 {
	return s.virtualShardIndex
}

// ReplicaSetName returns the selected physical replica set's name.
func (s ResolvedShard[R, W]) ReplicaSetName() string {
	return s.replicaSet.Name()
}

// ReadRoute selects and describes the next read target. Callers must execute
// the query through the returned Target so selection and telemetry agree.
func (s ResolvedShard[R, W]) ReadRoute() Route[R] {
	return s.replicaSet.ReadRoute().withVirtualShard(s.virtualShardIndex)
}

// WriteRoute selects and describes the primary write target.
func (s ResolvedShard[R, W]) WriteRoute() Route[W] {
	return s.replicaSet.WriteRoute().withVirtualShard(s.virtualShardIndex)
}
