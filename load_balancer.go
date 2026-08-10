package pgmesh

import "sync/atomic"

type roundRobin[T any] struct {
	items []T
	next  atomic.Uint64
}

func newRoundRobin[T any](items []T) *roundRobin[T] {
	return &roundRobin[T]{
		items: append([]T(nil), items...),
		next:  atomic.Uint64{},
	}
}

func (r *roundRobin[T]) nextItem() T {
	index := r.next.Add(1) - 1
	return r.items[index%uint64(len(r.items))]
}
