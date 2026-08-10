package pgmesh

import "strconv"

// ReplicaSet represents one physical shard. Reads are balanced across replica
// readers, while writes always use the primary writer and its configured
// synchronous mirrors. ReplicaSet routing is safe for concurrent use when the
// configured nodes and writer values are safe for concurrent use.
type ReplicaSet[R any, W MirrorableWriter[W]] struct {
	name         string
	primary      Node[R, W]
	readTargets  *roundRobin[nodeTarget[R]]
	writeMirrors []W
}

type nodeTarget[T any] struct {
	value T
	name  string
	role  NodeRole
}

// NewReplicaSet creates a physical replica set. If replicas is empty, reads
// fall back to the primary node.
func NewReplicaSet[R any, W MirrorableWriter[W]](
	name string,
	primary Node[R, W],
	replicas []Node[R, W],
) *ReplicaSet[R, W] {
	readTargets := make([]nodeTarget[R], 0, len(replicas))
	if len(replicas) == 0 {
		readTargets = append(readTargets, nodeTarget[R]{
			value: primary.Reader(),
			name:  "primary",
			role:  NodeRolePrimary,
		})
	} else {
		for index, replica := range replicas {
			readTargets = append(readTargets, nodeTarget[R]{
				value: replica.Reader(),
				name:  "replica-" + strconv.Itoa(index),
				role:  NodeRoleReadReplica,
			})
		}
	}
	return &ReplicaSet[R, W]{
		name:         name,
		primary:      primary,
		readTargets:  newRoundRobin(readTargets),
		writeMirrors: nil,
	}
}

// Name returns the replica set's topology name.
func (s *ReplicaSet[R, W]) Name() string {
	return s.name
}

// Reader returns the next read view selected by round-robin balancing.
func (s *ReplicaSet[R, W]) Reader() R {
	return s.ReadRoute().Target
}

// Writer returns the primary write view configured with synchronous mirrors.
func (s *ReplicaSet[R, W]) Writer() W {
	return s.WriteRoute().Target
}

// ReadRoute selects and describes the next read target in the replica set.
func (s *ReplicaSet[R, W]) ReadRoute() Route[R] {
	target := s.selectRead()
	return Route[R]{
		Target: target.value,
		metadata: RouteMetadata{
			VirtualShardIndex: 0,
			HasVirtualShard:   false,
			PhysicalShardName: s.name,
			NodeName:          target.name,
			NodeRole:          target.role,
		},
	}
}

// WriteRoute selects and describes the replica set's primary write target.
func (s *ReplicaSet[R, W]) WriteRoute() Route[W] {
	target := s.selectWrite()
	return Route[W]{
		Target: target.value,
		metadata: RouteMetadata{
			VirtualShardIndex: 0,
			HasVirtualShard:   false,
			PhysicalShardName: s.name,
			NodeName:          target.name,
			NodeRole:          target.role,
		},
	}
}

func (s *ReplicaSet[R, W]) selectRead() nodeTarget[R] {
	return s.readTargets.nextItem()
}

func (s *ReplicaSet[R, W]) selectWrite() nodeTarget[W] {
	return nodeTarget[W]{
		value: s.primary.Writer().WithMirrors(s.writeMirrors...),
		name:  "primary",
		role:  NodeRolePrimary,
	}
}

// WriteMirrorCount returns the number of synchronous write mirrors.
func (s *ReplicaSet[R, W]) WriteMirrorCount() int {
	return len(s.writeMirrors)
}

func (s *ReplicaSet[R, W]) primaryWriter() W {
	return s.primary.Writer()
}

// WithWriteMirrors returns a copy with writes appended to its synchronous
// mirrors. It does not mutate the receiver, and Writer passes mirrors to the
// writer in the same order in which they were configured.
func (s *ReplicaSet[R, W]) WithWriteMirrors(mirrors ...W) *ReplicaSet[R, W] {
	configuredMirrors := append([]W(nil), s.writeMirrors...)
	configuredMirrors = append(configuredMirrors, mirrors...)
	return &ReplicaSet[R, W]{
		name:         s.name,
		primary:      s.primary,
		readTargets:  s.readTargets,
		writeMirrors: configuredMirrors,
	}
}
