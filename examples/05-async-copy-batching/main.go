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
	shard0, err := openPool(ctx, "ASYNC_COPY_SHARD0_DSN")
	if err != nil {
		return err
	}
	defer shard0.Close()
	shard1, err := openPool(ctx, "ASYNC_COPY_SHARD1_DSN")
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
		sharded.WithCopyAccountsBatching(pgmesh.CopyBatchConfig{
			MaxRowsPerBatch:      100,
			Linger:               time.Hour,
			MaxConcurrentBatches: pgmesh.DefaultCopyBatchMaxConcurrentBatches,
		}),
	)
	if err != nil {
		return fmt.Errorf("create store: %w", err)
	}

	accounts := store.Accounts()
	baseID := time.Now().UnixNano()
	futures := []*pgmesh.Future[int64]{
		accounts.CopyAccountsAsync(ctx, []*sharded.CopyAccountsT{{
			TenantID:    20,
			ID:          baseID,
			DisplayName: "first shard-zero submission",
		}}),
		accounts.CopyAccountsAsync(ctx, []*sharded.CopyAccountsT{{
			TenantID:    21,
			ID:          baseID + 1,
			DisplayName: "second shard-zero submission",
		}}),
		accounts.CopyAccountsAsync(ctx, []*sharded.CopyAccountsT{{
			TenantID:    100,
			ID:          baseID + 2,
			DisplayName: "shard-one submission",
		}}),
	}

	flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := accounts.FlushCopyAccounts(flushCtx); err != nil {
		return fmt.Errorf("flush account copies: %w", err)
	}

	var total int64
	for index, future := range futures {
		count, err := future.Await(ctx)
		if err != nil {
			return fmt.Errorf("await account copy %d: %w", index, err)
		}
		total += count
	}
	fmt.Printf("asynchronous COPY wrote %d accounts\n", total)
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
