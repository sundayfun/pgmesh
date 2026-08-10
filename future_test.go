package pgmesh_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh"
)

func TestFutureConstructors(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("future failed")
	tests := []struct {
		name    string
		future  func() *pgmesh.Future[int]
		context func(*testing.T) context.Context
		want    int
		wantErr error
	}{
		{
			name:    "resolved value",
			future:  func() *pgmesh.Future[int] { return pgmesh.ResolvedFuture(42, nil) },
			context: func(t *testing.T) context.Context { return t.Context() },
			want:    42,
		},
		{
			name:    "resolved error",
			future:  func() *pgmesh.Future[int] { return pgmesh.ResolvedFuture(0, sentinel) },
			context: func(t *testing.T) context.Context { return t.Context() },
			wantErr: sentinel,
		},
		{
			name:   "resolved value wins over canceled wait",
			future: func() *pgmesh.Future[int] { return pgmesh.ResolvedFuture(7, nil) },
			context: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx
			},
			want: 7,
		},
		{
			name: "asynchronous value",
			future: func() *pgmesh.Future[int] {
				return pgmesh.RunFuture(func() (int, error) { return 99, nil })
			},
			context: func(t *testing.T) context.Context { return t.Context() },
			want:    99,
		},
		{
			name: "asynchronous error",
			future: func() *pgmesh.Future[int] {
				return pgmesh.RunFuture(func() (int, error) { return 0, sentinel })
			},
			context: func(t *testing.T) context.Context { return t.Context() },
			wantErr: sentinel,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := test.future().Await(test.context(t))
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				assert.Zero(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestFutureAwaitCancellationDoesNotCancelWork(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	future := pgmesh.RunFuture(func() (int, error) {
		<-release
		return 42, nil
	})
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	result, err := future.Await(canceled)
	assert.Zero(t, result)
	require.ErrorIs(t, err, context.Canceled)

	close(release)
	result, err = future.Await(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 42, result)
	result, err = future.Await(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 42, result)
}
