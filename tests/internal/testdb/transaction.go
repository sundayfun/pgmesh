package testdb

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

// Rollback closes a test transaction and ignores the expected post-commit
// ErrTxClosed result.
func Rollback(t testing.TB, tx pgx.Tx) {
	t.Helper()
	err := tx.Rollback(context.Background())
	if err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Errorf("rollback test transaction: %v", err)
	}
}
