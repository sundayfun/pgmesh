package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sundayfun/pgmesh/examples/internal/sharded"
)

type databasePools struct {
	primary *pgxpool.Pool
	replica *pgxpool.Pool
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	pools, err := openDatabasePools(ctx)
	if err != nil {
		return err
	}
	defer pools.close()

	store, err := newAccountsStore(ctx, pools)
	if err != nil {
		return err
	}
	accounts := store.Accounts()
	account, err := writeAccount(ctx, accounts)
	if err != nil {
		return err
	}
	if err := printPrimaryRead(ctx, accounts, account); err != nil {
		return err
	}
	return printReplicaRead(ctx, accounts, account)
}

func openDatabasePools(ctx context.Context) (*databasePools, error) {
	primary, err := openPool(ctx, "RW_PRIMARY_DSN")
	if err != nil {
		return nil, err
	}
	replica, err := openPool(ctx, "RW_REPLICA_DSN")
	if err != nil {
		primary.Close()
		return nil, err
	}
	return &databasePools{primary: primary, replica: replica}, nil
}

func (p *databasePools) close() {
	p.replica.Close()
	p.primary.Close()
}

func newAccountsStore(ctx context.Context, pools *databasePools) (sharded.Store, error) {
	return sharded.NewStore(
		ctx,
		sharded.Singleton(
			pools.primary,
			sharded.WithDatabaseName("accounts"),
			sharded.WithReadReplicas(pools.replica),
		),
	)
}

func writeAccount(
	ctx context.Context,
	accounts sharded.AccountsWriter,
) (*sharded.Account, error) {
	account, err := accounts.UpsertAccount(ctx, &sharded.UpsertAccountT{
		ID:          2001,
		TenantID:    42,
		DisplayName: "primary write",
	})
	if err != nil {
		return nil, fmt.Errorf("write primary: %w", err)
	}
	return account, nil
}

func printPrimaryRead(
	ctx context.Context,
	accounts sharded.AccountsReader,
	account *sharded.Account,
) error {
	strong, err := accounts.GetAccount(ctx, accountKey(account), sharded.ReadFromPrimary())
	if err != nil {
		return fmt.Errorf("read primary: %w", err)
	}
	fmt.Printf("strong read: %s\n", strong.DisplayName)
	return nil
}

func printReplicaRead(
	ctx context.Context,
	accounts sharded.AccountsReader,
	account *sharded.Account,
) error {
	replicaCopy, err := accounts.GetAccount(ctx, accountKey(account))
	if err != nil {
		return fmt.Errorf("read replica (check replication and lag): %w", err)
	}
	fmt.Printf("replica read: %s\n", replicaCopy.DisplayName)
	return nil
}

func accountKey(account *sharded.Account) *sharded.GetAccountT {
	return &sharded.GetAccountT{TenantID: account.TenantID, ID: account.ID}
}

func openPool(ctx context.Context, environment string) (*pgxpool.Pool, error) {
	dsn := os.Getenv(environment)
	if dsn == "" {
		return nil, fmt.Errorf("%s is required", environment)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", environment, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping %s: %w", environment, err)
	}
	return pool, nil
}
