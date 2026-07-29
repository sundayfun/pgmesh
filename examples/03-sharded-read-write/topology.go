package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/sundayfun/pgmesh"
	"github.com/sundayfun/pgmesh/examples/internal/one"
	"github.com/sundayfun/pgmesh/examples/internal/sharded"
)

const numVShards = 128

type (
	accountStore  = sharded.Store
	settingsStore = one.Store
)

type tenantResolver struct{}

func (tenantResolver) TenantKey(key sharded.TenantKey) uint64 {
	if key.TenantID < 0 {
		panic("tenant ID must not be negative")
	}
	return uint64(key.TenantID)
}

func newAccountQueries(
	ctx context.Context,
	cfg config,
	pools *poolRegistry,
) (accountStore, error) {
	shard0Primary, err := pools.open(ctx, cfg.shard0Primary)
	if err != nil {
		return nil, err
	}
	shard0Replica, err := pools.open(ctx, cfg.shard0Replica)
	if err != nil {
		return nil, err
	}
	shard1Primary, err := pools.open(ctx, cfg.shard1Primary)
	if err != nil {
		return nil, err
	}
	shard1Replica, err := pools.open(ctx, cfg.shard1Replica)
	if err != nil {
		return nil, err
	}
	store, err := sharded.NewStore(
		ctx,
		sharded.Sharded(
			numVShards,
			pgmesh.ModularShardHashFor[uint64](numVShards),
			tenantResolver{},
			sharded.WithReplicaSet("shard-0", shard0Primary, shard0Replica),
			sharded.WithReplicaSet("shard-1", shard1Primary, shard1Replica),
			sharded.WithVShardMapping("shard-0", pgmesh.VShardRange(0, 64)),
			sharded.WithVShardMapping("shard-1", pgmesh.VShardRange(64, numVShards)),
		),
		sharded.WithLogger(slog.New(slog.NewTextHandler(
			os.Stderr,
			&slog.HandlerOptions{
				AddSource:   false,
				Level:       slog.LevelDebug,
				ReplaceAttr: nil,
			},
		))),
	)
	if err != nil {
		return nil, fmt.Errorf("create account store: %w", err)
	}
	return store, nil
}

func newSettingsStore(
	ctx context.Context,
	dsn string,
	pools *poolRegistry,
) (settingsStore, error) {
	pool, err := pools.open(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return one.NewStore(
		ctx,
		one.Singleton(pool, one.WithDatabaseName("settings")),
	)
}
