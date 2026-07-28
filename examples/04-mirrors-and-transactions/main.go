package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sundayfun/pgmesh"
	"github.com/sundayfun/pgmesh/examples/internal/sharded"
)

const (
	shard0Name       = "shard-0"
	shard1Name       = "shard-1"
	futureShard0Name = "future-shard-0"
	futureShard1Name = "future-shard-1"
	numVShards       = 2
)

type config struct {
	shard0Primary string
	shard0Replica string
	shard0Future  string
	shard1Primary string
	shard1Replica string
	shard1Future  string
}

type poolRegistry struct {
	byDSN map[string]*pgxpool.Pool
}

type tenantResolver struct{}

func (tenantResolver) Tenant(tenantID int64) uint64 {
	if tenantID < 0 {
		panic("tenant ID must not be negative")
	}
	return uint64(tenantID)
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	pools := newPoolRegistry()
	defer pools.close()

	store, err := createStore(ctx, cfg, pools)
	if err != nil {
		return err
	}
	accounts := store.Accounts()

	const tenantID int64 = 42
	const accountID int64 = 4001
	if dualWriteErr := dualWriteToFutureShard(ctx, accounts, tenantID, accountID); dualWriteErr != nil {
		return dualWriteErr
	}
	updated, err := updateInTransaction(ctx, cfg, pools, accounts, tenantID, accountID)
	if err != nil {
		return err
	}
	fmt.Printf("account %d: %s\n", updated.ID, updated.DisplayName)
	return nil
}

func loadConfig() (config, error) {
	values, err := requiredEnvironment(
		"ADV_SHARD0_PRIMARY_DSN",
		"ADV_SHARD0_REPLICA_DSN",
		"ADV_SHARD0_MIRROR_DSN",
		"ADV_SHARD1_PRIMARY_DSN",
		"ADV_SHARD1_REPLICA_DSN",
		"ADV_SHARD1_MIRROR_DSN",
	)
	if err != nil {
		return config{}, err
	}
	return config{
		shard0Primary: values[0],
		shard0Replica: values[1],
		shard0Future:  values[2],
		shard1Primary: values[3],
		shard1Replica: values[4],
		shard1Future:  values[5],
	}, nil
}

func newPoolRegistry() *poolRegistry {
	return &poolRegistry{byDSN: make(map[string]*pgxpool.Pool)}
}

func (r *poolRegistry) close() {
	for _, pool := range r.byDSN {
		pool.Close()
	}
}

func (r *poolRegistry) open(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	if pool, ok := r.byDSN[dsn]; ok {
		return pool, nil
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open node: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping node: %w", err)
	}
	r.byDSN[dsn] = pool
	return pool, nil
}

func (r *poolRegistry) pool(dsn string) (*pgxpool.Pool, error) {
	pool, ok := r.byDSN[dsn]
	if !ok {
		return nil, errors.New("pool for DSN is not registered")
	}
	return pool, nil
}

func createStore(
	ctx context.Context,
	cfg config,
	pools *poolRegistry,
) (sharded.Store, error) {
	shard0Primary, err := pools.open(ctx, cfg.shard0Primary)
	if err != nil {
		return nil, err
	}
	shard0Replica, err := pools.open(ctx, cfg.shard0Replica)
	if err != nil {
		return nil, err
	}
	shard0Future, err := pools.open(ctx, cfg.shard0Future)
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
	shard1Future, err := pools.open(ctx, cfg.shard1Future)
	if err != nil {
		return nil, err
	}
	store, err := sharded.NewStore(
		ctx,
		sharded.Sharded(
			numVShards,
			pgmesh.ModularShardHashFor[uint64](numVShards),
			tenantResolver{},
			sharded.WithReplicaSet(shard0Name, shard0Primary, shard0Replica),
			sharded.WithReplicaSet(shard1Name, shard1Primary, shard1Replica),
			sharded.WithReplicaSet(futureShard0Name, shard0Future),
			sharded.WithReplicaSet(futureShard1Name, shard1Future),
			sharded.WithVShardMapping(shard0Name, []uint64{0}, futureShard0Name),
			sharded.WithVShardMapping(shard1Name, []uint64{1}, futureShard1Name),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create store: %w", err)
	}
	return store, nil
}

func dualWriteToFutureShard(
	ctx context.Context,
	accounts sharded.AccountsWriter,
	tenantID int64,
	accountID int64,
) error {
	_, err := accounts.UpsertAccount(ctx, &sharded.UpsertAccountT{
		ID:          accountID,
		TenantID:    tenantID,
		DisplayName: "mirrored write",
	})
	if err != nil {
		return fmt.Errorf("dual-write account: %w", err)
	}
	return nil
}

func updateInTransaction(
	ctx context.Context,
	cfg config,
	pools *poolRegistry,
	accounts sharded.AccountsWriter,
	tenantID int64,
	accountID int64,
) (*sharded.Account, error) {
	shardName := shard0Name
	if (tenantResolver{}).Tenant(tenantID)%numVShards == 1 {
		shardName = shard1Name
	}
	dsn, err := cfg.primaryDSN(shardName)
	if err != nil {
		return nil, err
	}
	pool, err := pools.pool(dsn)
	if err != nil {
		return nil, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				log.Printf("rollback transaction: %v", rollbackErr)
			}
		}
	}()

	updated, err := accounts.UpdateAccountName(
		ctx,
		&sharded.UpdateAccountNameT{
			TenantID:    tenantID,
			ID:          accountID,
			DisplayName: "transactional update",
		},
		sharded.WithTx(tx),
	)
	if err != nil {
		return nil, fmt.Errorf("transactional update: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}
	committed = true
	return updated, nil
}

func (c config) primaryDSN(shardName string) (string, error) {
	switch shardName {
	case shard0Name:
		return c.shard0Primary, nil
	case shard1Name:
		return c.shard1Primary, nil
	default:
		return "", fmt.Errorf("unknown primary shard %q", shardName)
	}
}

func requiredEnvironment(names ...string) ([]string, error) {
	values := make([]string, len(names))
	for index, name := range names {
		value := os.Getenv(name)
		if value == "" {
			return nil, fmt.Errorf("%s is required", name)
		}
		values[index] = value
	}
	return values, nil
}
