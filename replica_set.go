package pgmesh

import "strconv"

// ReplicaSet represents one physical shard. Reads are balanced across replica
// readers, while writes always use the primary writer and its configured
// synchronous mirrors. ReplicaSet routing is safe for concurrent use when the
// configured nodes and writer values are safe for concurrent use.
type ReplicaSet[R any, W Mirrorable[W]] struct {
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
func NewReplicaSet[R any, W Mirrorable[W]](
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

// Read returns the next read view selected by round-robin balancing.
func (s *ReplicaSet[R, W]) Read() R {
	return s.selectRead().value
}

// Write returns the primary write view configured with synchronous mirrors.
func (s *ReplicaSet[R, W]) Write() W {
	return s.selectWrite().value
}

func (s *ReplicaSet[R, W]) selectRead() nodeTarget[R] {
	return s.readTargets.Next()
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
// mirrors. It does not mutate the receiver, and Write passes mirrors to the
// writer in the same order in which they were configured.
func (s *ReplicaSet[R, W]) WithWriteMirrors(writes ...W) *ReplicaSet[R, W] {
	mirrors := append([]W(nil), s.writeMirrors...)
	mirrors = append(mirrors, writes...)
	return &ReplicaSet[R, W]{
		name:         s.name,
		primary:      s.primary,
		readTargets:  s.readTargets,
		writeMirrors: mirrors,
	}
}
