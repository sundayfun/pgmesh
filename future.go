package pgmesh

import (
	"context"
	"sync"
)

// Future is a repeatable, concurrency-safe handle for an asynchronous result.
// Canceling an Await call only stops that wait; it does not cancel the work.
type Future[T any] struct {
	done  chan struct{}
	once  sync.Once
	value T
	err   error
}

func newFuture[T any]() *Future[T] {
	var zero T
	return &Future[T]{
		done:  make(chan struct{}),
		once:  sync.Once{},
		value: zero,
		err:   nil,
	}
}

func (f *Future[T]) resolve(value T, err error) {
	f.once.Do(func() {
		f.value = value
		f.err = err
		close(f.done)
	})
}

// ResolvedFuture returns a Future that has already completed.
func ResolvedFuture[T any](value T, err error) *Future[T] {
	future := newFuture[T]()
	future.resolve(value, err)
	return future
}

// RunFuture starts work asynchronously and resolves the returned Future with its
// result. The function owns any cancellation semantics for the work itself.
func RunFuture[T any](work func() (T, error)) *Future[T] {
	future := newFuture[T]()
	go func() {
		value, err := work()
		future.resolve(value, err)
	}()
	return future
}

// Await waits for the Future or for ctx to be canceled. A later Await may still
// retrieve the completed operation after an earlier wait was canceled.
func (f *Future[T]) Await(ctx context.Context) (T, error) {
	select {
	case <-f.done:
		return f.value, f.err
	default:
	}

	select {
	case <-f.done:
		return f.value, f.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
