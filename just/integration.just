import "base.just"

export COMPOSE := env("COMPOSE", "docker compose")
export COMPOSE_FILE := env("COMPOSE_FILE", "tests/docker-compose.yaml")
export COMPOSE_PROJECT := env("COMPOSE_PROJECT", "pgmesh-integration")
export PGMESH_SHARD0_PRIMARY_PORT := env("PGMESH_SHARD0_PRIMARY_PORT", "25432")
export PGMESH_SHARD0_REPLICA0_PORT := env("PGMESH_SHARD0_REPLICA0_PORT", "25433")
export PGMESH_SHARD0_REPLICA1_PORT := env("PGMESH_SHARD0_REPLICA1_PORT", "25434")
export PGMESH_SHARD1_PRIMARY_PORT := env("PGMESH_SHARD1_PRIMARY_PORT", "25435")
export PGMESH_SHARD0_MIRROR_PORT := env("PGMESH_SHARD0_MIRROR_PORT", "25436")

# Start the local PostgreSQL topology.
integration-up:
    {{ COMPOSE }} -p {{ COMPOSE_PROJECT }} -f {{ COMPOSE_FILE }} up -d --wait --remove-orphans

# Stop the local PostgreSQL topology and remove its volumes.
integration-down:
    {{ COMPOSE }} -p {{ COMPOSE_PROJECT }} -f {{ COMPOSE_FILE }} down --volumes --remove-orphans

# Run the Docker-backed integration tests.
integration-test:
    PGMESH_INTEGRATION=1 go test -p=1 -race -count=1 -tags=integration -timeout=2m ./tests/...

# Test one focused feature against an already-running PostgreSQL topology.
integration-test-feature feature:
    PGMESH_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=2m "./tests/{{ feature }}"

# Test one generated fixture case against an already-running PostgreSQL topology.
integration-test-case case:
    #!/usr/bin/env bash
    set -eu
    case "{{ case }}" in
      same_package)
        PGMESH_INTEGRATION=1 go test -p=1 -race -count=1 -tags=integration -timeout=2m ./tests/...
        ;;
      separate_package|command_shapes)
        PGMESH_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=2m "./tests/{{ case }}"
        ;;
      *)
        echo "unsupported generated fixture case: {{ case }}" >&2
        exit 2
        ;;
    esac

# Regenerate one case, run it against its own schema, and clean up.
integration-case case:
    #!/usr/bin/env bash
    set -eu
    export PGMESH_TEST_SCHEMA="./{{ case }}/schema.sql"
    cleanup() {
        "{{ just_executable() }}" integration-down
    }
    trap cleanup EXIT INT TERM
    "{{ just_executable() }}" generate-case "{{ case }}"
    "{{ just_executable() }}" integration-up
    "{{ just_executable() }}" integration-test-case "{{ case }}"

# Run all executable examples against the local topology.
examples-smoke:
    SINGLE_DATABASE_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_PRIMARY_PORT }}/pgmesh?sslmode=disable' go run ./examples/01-single-database
    RW_PRIMARY_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_PRIMARY_PORT }}/pgmesh?sslmode=disable' RW_REPLICA_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_PRIMARY_PORT }}/pgmesh?sslmode=disable' go run ./examples/02-read-write-split
    SHARD0_PRIMARY_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_PRIMARY_PORT }}/pgmesh?sslmode=disable' SHARD0_REPLICA_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_REPLICA0_PORT }}/pgmesh?sslmode=disable' SHARD1_PRIMARY_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD1_PRIMARY_PORT }}/pgmesh?sslmode=disable' SHARD1_REPLICA_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_REPLICA1_PORT }}/pgmesh?sslmode=disable' SETTINGS_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_PRIMARY_PORT }}/pgmesh?sslmode=disable' go run ./examples/03-sharded-read-write
    ADV_SHARD0_PRIMARY_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_PRIMARY_PORT }}/pgmesh?sslmode=disable' ADV_SHARD0_REPLICA_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_REPLICA0_PORT }}/pgmesh?sslmode=disable' ADV_SHARD0_MIRROR_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_MIRROR_PORT }}/pgmesh?sslmode=disable' ADV_SHARD1_PRIMARY_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD1_PRIMARY_PORT }}/pgmesh?sslmode=disable' ADV_SHARD1_REPLICA_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_REPLICA1_PORT }}/pgmesh?sslmode=disable' ADV_SHARD1_MIRROR_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_MIRROR_PORT }}/pgmesh?sslmode=disable' go run ./examples/04-mirrors-and-transactions
    ASYNC_COPY_SHARD0_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_PRIMARY_PORT }}/pgmesh?sslmode=disable' ASYNC_COPY_SHARD1_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD1_PRIMARY_PORT }}/pgmesh?sslmode=disable' go run ./examples/05-async-copy-batching
    CACHE_DATABASE_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_PRIMARY_PORT }}/pgmesh?sslmode=disable' go run ./examples/06-cache-aside
    MULTI_SHARD0_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD0_PRIMARY_PORT }}/pgmesh?sslmode=disable' MULTI_SHARD1_DSN='postgres://pgmesh:pgmesh@127.0.0.1:{{ PGMESH_SHARD1_PRIMARY_PORT }}/pgmesh?sslmode=disable' go run ./examples/07-multi-shard-queries

# Start, test, smoke-test, and clean up the local topology.
integration:
    #!/usr/bin/env bash
    set -eu
    cleanup() {
        "{{ just_executable() }}" integration-down
    }
    trap cleanup EXIT INT TERM
    "{{ just_executable() }}" integration-up
    "{{ just_executable() }}" integration-test
    "{{ just_executable() }}" examples-smoke
