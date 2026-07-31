package testdb

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const IntegrationEnv = "PGMESH_INTEGRATION"

// Endpoint describes one PostgreSQL database used by the integration topology.
type Endpoint struct {
	Name        string
	DSNEnv      string
	PortEnv     string
	DefaultPort int
}

// Enabled reports whether Docker-backed integration tests were requested.
func Enabled() bool {
	return os.Getenv(IntegrationEnv) != ""
}

// DefaultEndpoints returns the five isolated databases in the test topology.
func DefaultEndpoints() []Endpoint {
	return []Endpoint{
		{Name: "shard0-primary", DSNEnv: "PGMESH_SHARD0_PRIMARY_DSN", PortEnv: "PGMESH_SHARD0_PRIMARY_PORT", DefaultPort: 25432},
		{Name: "shard0-replica0", DSNEnv: "PGMESH_SHARD0_REPLICA0_DSN", PortEnv: "PGMESH_SHARD0_REPLICA0_PORT", DefaultPort: 25433},
		{Name: "shard0-replica1", DSNEnv: "PGMESH_SHARD0_REPLICA1_DSN", PortEnv: "PGMESH_SHARD0_REPLICA1_PORT", DefaultPort: 25434},
		{Name: "shard1-primary", DSNEnv: "PGMESH_SHARD1_PRIMARY_DSN", PortEnv: "PGMESH_SHARD1_PRIMARY_PORT", DefaultPort: 25435},
		{Name: "shard0-mirror", DSNEnv: "PGMESH_SHARD0_MIRROR_DSN", PortEnv: "PGMESH_SHARD0_MIRROR_PORT", DefaultPort: 25436},
	}
}

// PrimaryEndpoint returns the shard-zero primary used by single-database tests.
func PrimaryEndpoint() Endpoint {
	return DefaultEndpoints()[0]
}

// DSN resolves the endpoint's full-DSN or port environment override.
func (e Endpoint) DSN() (string, error) {
	return ResolveDSN(os.Getenv(e.DSNEnv), os.Getenv(e.PortEnv), e.DefaultPort)
}

// ResolveDSN gives a full DSN precedence over a validated port override.
func ResolveDSN(dsnOverride, portOverride string, defaultPort int) (string, error) {
	if dsnOverride != "" {
		return dsnOverride, nil
	}
	port := defaultPort
	if portOverride != "" {
		parsed, err := strconv.Atoi(portOverride)
		if err != nil || parsed < 1 || parsed > 65535 {
			return "", fmt.Errorf("port override must be a valid TCP port: %q", portOverride)
		}
		port = parsed
	}
	return fmt.Sprintf("postgres://pgmesh:pgmesh@127.0.0.1:%d/pgmesh?sslmode=disable", port), nil
}

// OpenPool creates and verifies a PostgreSQL pool for an integration endpoint.
func OpenPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	pingContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingContext); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}
