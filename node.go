package pgmesh

// MirrorableWriter is implemented by generated primary-capable query wrappers.
// WithMirrors must return a new value and leave the receiver unchanged.
type MirrorableWriter[W any] interface {
	// WithMirrors returns a copy that also writes to the supplied mirrors.
	WithMirrors(mirrors ...W) W
}

// Node contains the read-only and primary-capable views of one database
// connection. ReplicaSet exposes only Reader for replicas and Writer for the
// primary, preventing writes from accidentally being routed to replicas.
type Node[R any, W MirrorableWriter[W]] struct {
	reader R
	writer W
}

// NewNode creates a database node from its read-only and primary-capable views.
func NewNode[R any, W MirrorableWriter[W]](reader R, writer W) Node[R, W] {
	return Node[R, W]{reader: reader, writer: writer}
}

// Reader returns the node's read-only query view.
func (n Node[R, W]) Reader() R {
	return n.reader
}

// Writer returns the node's primary-capable query view.
func (n Node[R, W]) Writer() W {
	return n.writer
}
