package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sundayfun/pgmesh"
	"github.com/sundayfun/pgmesh/examples/internal/sharded"
)

const virtualShardCount = 128

type tenantResolver struct{}

func (tenantResolver) TenantKey(key sharded.TenantKey) uint64 {
	if key.TenantID < 0 {
		panic("tenant ID must not be negative")
	}
	return uint64(key.TenantID)
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	shard0, err := openPool(ctx, "MULTI_SHARD0_DSN")
	if err != nil {
		return err
	}
	defer shard0.Close()
	shard1, err := openPool(ctx, "MULTI_SHARD1_DSN")
	if err != nil {
		return err
	}
	defer shard1.Close()

	store, err := sharded.NewStore(
		ctx,
		sharded.Sharded(
			virtualShardCount,
			pgmesh.NewModuloShardHasher[uint64](virtualShardCount),
			tenantResolver{},
			sharded.WithReplicaSet("shard-0", shard0),
			sharded.WithReplicaSet("shard-1", shard1),
			sharded.WithVirtualShardMapping("shard-0", pgmesh.VirtualShardRange(0, 64)),
			sharded.WithVirtualShardMapping("shard-1", pgmesh.VirtualShardRange(64, virtualShardCount)),
		),
	)
	if err != nil {
		return fmt.Errorf("create store: %w", err)
	}

	accounts := store.Accounts()
	baseID := time.Now().UnixNano()
	seed := []*sharded.UpsertAccountT{
		{
			TenantID:    20,
			ID:          baseID,
			DisplayName: "account on shard zero",
		},
		{
			TenantID:    100,
			ID:          baseID + 1,
			DisplayName: "account on shard one",
		},
	}
	for _, account := range seed {
		if _, upsertErr := accounts.UpsertAccount(ctx, account); upsertErr != nil {
			return fmt.Errorf("seed account %d: %w", account.ID, upsertErr)
		}
	}

	grouped, err := accounts.ListAccountsByIDs(
		ctx,
		[]*sharded.ListAccountsByIDsT{
			{TenantKey: seed[1].TenantKey, ID: seed[1].ID},
			{TenantKey: seed[0].TenantKey, ID: seed[0].ID},
		},
		sharded.ReadFromPrimary(),
	)
	if err != nil {
		return fmt.Errorf("group account lookup by shard: %w", err)
	}
	for index, account := range grouped {
		fmt.Printf("grouped result %d: tenant=%d name=%q\n", index, account.TenantID, account.DisplayName)
	}

	all, err := accounts.ListAllAccounts(ctx, sharded.ReadFromPrimary())
	if err != nil {
		return fmt.Errorf("scatter account list: %w", err)
	}
	fmt.Printf("scatter query returned %d total accounts\n", len(all))
	return nil
}

func openPool(ctx context.Context, environmentName string) (*pgxpool.Pool, error) {
	dsn := os.Getenv(environmentName)
	if dsn == "" {
		return nil, fmt.Errorf("%s is required", environmentName)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", environmentName, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping %s: %w", environmentName, err)
	}
	return pool, nil
}
