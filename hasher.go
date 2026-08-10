package pgmesh

// ShardHasher maps an application shard key to a virtual shard index.
type ShardHasher[K any] interface {
	// Hash returns the virtual shard index for key.
	Hash(key K) uint64
}

type constantShardHasher[K any] struct {
	virtualShardIndex uint64
}

func (h constantShardHasher[K]) Hash(K) uint64 {
	return h.virtualShardIndex
}

// NewConstantShardHasher returns a hasher that always selects
// virtualShardIndex.
func NewConstantShardHasher[K any](virtualShardIndex uint64) ShardHasher[K] {
	return constantShardHasher[K]{virtualShardIndex: virtualShardIndex}
}

// IntegerShardKey is the set of integer types supported by
// NewModuloShardHasher.
type IntegerShardKey interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

type moduloShardHasher[K IntegerShardKey] struct {
	virtualShardCount uint64
}

func (h moduloShardHasher[K]) Hash(key K) uint64 {
	if key < 0 {
		magnitudeRemainder := (uint64(-(key + 1)) + 1) % h.virtualShardCount
		if magnitudeRemainder == 0 {
			return 0
		}
		return h.virtualShardCount - magnitudeRemainder
	}
	return uint64(key) % h.virtualShardCount
}

// NewModuloShardHasher returns a hasher that maps integer keys modulo
// virtualShardCount. Signed keys use Euclidean modulo, so negative values map
// into the same [0, virtualShardCount) range without overflowing at the
// minimum integer value. Named integer types are supported. It panics if
// virtualShardCount is zero.
func NewModuloShardHasher[K IntegerShardKey](virtualShardCount uint64) ShardHasher[K] {
	if virtualShardCount == 0 {
		panic("pgmesh: virtual shard count must not be zero")
	}
	return moduloShardHasher[K]{virtualShardCount: virtualShardCount}
}
