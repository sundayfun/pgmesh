package pgmesh

import "errors"

var (
	// ErrNoReplicaSets indicates that a topology contains no replica sets.
	ErrNoReplicaSets = errors.New("pgmesh: at least one replica set is required")
	// ErrEmptyReplicaSetName indicates that a replica set has no name.
	ErrEmptyReplicaSetName = errors.New("pgmesh: replica set name must not be empty")
	// ErrDuplicateReplicaSet indicates that a topology reuses a replica set name.
	ErrDuplicateReplicaSet = errors.New("pgmesh: duplicate replica set")
	// ErrEmptyDSN indicates that a database connection has no DSN.
	ErrEmptyDSN = errors.New("pgmesh: connection DSN must not be empty")
	// ErrNoVirtualShards indicates that a topology contains no virtual shards.
	ErrNoVirtualShards = errors.New("pgmesh: at least one virtual shard is required")
	// ErrVirtualShardAlreadyMapped indicates that a virtual shard has already
	// been mapped.
	ErrVirtualShardAlreadyMapped = errors.New("pgmesh: virtual shard is already mapped")
	// ErrVirtualShardNotMapped indicates that a virtual shard has not been mapped.
	ErrVirtualShardNotMapped = errors.New("pgmesh: virtual shard is not mapped")
	// ErrVirtualShardOutOfRange indicates that a virtual shard index is outside
	// the topology.
	ErrVirtualShardOutOfRange = errors.New("pgmesh: virtual shard is out of range")
	// ErrNilShardHasher indicates that no shard-key hasher was configured.
	ErrNilShardHasher = errors.New("pgmesh: shard hasher is required")
	// ErrNilNodeOpener indicates that no database node opener was configured.
	ErrNilNodeOpener = errors.New("pgmesh: node opener is required")
	// ErrUnknownReplicaSet indicates that a shard mapping names an undefined replica set.
	ErrUnknownReplicaSet = errors.New("pgmesh: unknown replica set")
	// ErrNilReplicaSet indicates that a builder was given a nil replica set.
	ErrNilReplicaSet = errors.New("pgmesh: replica set must not be nil")
	// ErrMirrorConfiguration indicates that write-mirror mappings are inconsistent.
	ErrMirrorConfiguration = errors.New("pgmesh: inconsistent mirror configuration")
	// ErrCrossShardTransaction indicates that one transaction was supplied for
	// an operation targeting more than one physical shard.
	ErrCrossShardTransaction = errors.New("pgmesh: transaction cannot span physical shards")
	// ErrCopyBatchCountMismatch reports a successful physical COPY whose returned
	// row count does not match the number of rows supplied to it.
	ErrCopyBatchCountMismatch = errors.New("pgmesh: copy batch row count mismatch")
	// ErrNilCopyBatchFunc indicates that a copy batcher has no execution function.
	ErrNilCopyBatchFunc = errors.New("pgmesh: copy batch function is nil")
)
