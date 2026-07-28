package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sundayfun/pgmesh/examples/internal/sharded"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	pool, err := openDatabase(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	queries, err := sharded.NewStore(
		ctx,
		sharded.Singleton(pool, sharded.WithDatabaseName("accounts")),
	)
	if err != nil {
		return err
	}
	accounts := queries.Accounts()
	account, err := createAccount(ctx, accounts)
	if err != nil {
		return err
	}
	return loadAndPrintAccount(ctx, accounts, account)
}

func openDatabase(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := os.Getenv("SINGLE_DATABASE_DSN")
	if dsn == "" {
		return nil, errors.New("SINGLE_DATABASE_DSN is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

func createAccount(ctx context.Context, accounts sharded.AccountsWriter) (*sharded.Account, error) {
	account, err := accounts.UpsertAccount(ctx, &sharded.UpsertAccountT{
		ID:          1001,
		TenantID:    42,
		DisplayName: "single database",
	})
	if err != nil {
		return nil, fmt.Errorf("upsert account: %w", err)
	}
	return account, nil
}

func loadAndPrintAccount(
	ctx context.Context,
	accounts sharded.AccountsReader,
	account *sharded.Account,
) error {
	loaded, err := accounts.GetAccount(ctx, &sharded.GetAccountT{
		TenantID: account.TenantID,
		ID:       account.ID,
	})
	if err != nil {
		return fmt.Errorf("get account: %w", err)
	}
	fmt.Printf("account %d: %s\n", loaded.ID, loaded.DisplayName)
	return nil
}
