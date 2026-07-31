package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sundayfun/pgmesh/examples/internal/one"
)

type settingCache struct {
	mu     sync.RWMutex
	values map[string]one.ApplicationSetting
}

func newSettingCache() *settingCache {
	return &settingCache{
		mu:     sync.RWMutex{},
		values: make(map[string]one.ApplicationSetting),
	}
}

func (c *settingCache) get(key string) (*one.ApplicationSetting, bool) {
	c.mu.RLock()
	setting, ok := c.values[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return &setting, true
}

func (c *settingCache) put(setting *one.ApplicationSetting) {
	if setting == nil {
		return
	}
	c.mu.Lock()
	c.values[setting.Key] = *setting
	c.mu.Unlock()
}

type cachedSettings struct {
	one.Settings

	cache *settingCache
}

func (s *cachedSettings) GetSetting(
	ctx context.Context,
	key string,
	options ...one.QueryOption,
) (*one.ApplicationSetting, error) {
	// Primary and transaction options carry consistency semantics. Bypass the
	// cache instead of accidentally weakening them.
	if len(options) != 0 {
		return s.Settings.GetSetting(ctx, key, options...)
	}
	if setting, ok := s.cache.get(key); ok {
		fmt.Printf("cache hit for %q\n", key)
		return setting, nil
	}
	fmt.Printf("cache miss for %q\n", key)
	setting, err := s.Settings.GetSetting(ctx, key)
	if err == nil {
		s.cache.put(setting)
	}
	return setting, err
}

func (s *cachedSettings) UpsertSetting(
	ctx context.Context,
	arg *one.UpsertSettingT,
	options ...one.QueryOption,
) (*one.ApplicationSetting, error) {
	setting, err := s.Settings.UpsertSetting(ctx, arg, options...)
	if err == nil {
		s.cache.put(setting)
	}
	return setting, err
}

var _ one.Settings = (*cachedSettings)(nil)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	dsn := os.Getenv("CACHE_DATABASE_DSN")
	if dsn == "" {
		return errors.New("CACHE_DATABASE_DSN is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer pool.Close()
	if pingErr := pool.Ping(ctx); pingErr != nil {
		return fmt.Errorf("ping database: %w", pingErr)
	}

	cache := newSettingCache()
	store, err := one.NewStore(
		ctx,
		one.Singleton(pool, one.WithDatabaseName("settings")),
		one.WithSettingsFactory(func(internal one.Settings) one.Settings {
			return &cachedSettings{Settings: internal, cache: cache}
		}),
	)
	if err != nil {
		return fmt.Errorf("create store: %w", err)
	}

	settings := store.Settings()
	for range 2 {
		setting, getErr := settings.GetSetting(ctx, "deployment_name")
		if getErr != nil {
			return fmt.Errorf("get setting: %w", getErr)
		}
		fmt.Printf("deployment: %s\n", setting.Value)
	}

	updated, err := settings.UpsertSetting(ctx, &one.UpsertSettingT{
		Key:   "deployment_name",
		Value: "pgmesh cache-aside example",
	})
	if err != nil {
		return fmt.Errorf("update setting: %w", err)
	}
	setting, err := settings.GetSetting(ctx, updated.Key)
	if err != nil {
		return fmt.Errorf("get updated setting: %w", err)
	}
	fmt.Printf("updated deployment: %s\n", setting.Value)
	return nil
}
