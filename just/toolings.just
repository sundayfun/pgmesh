import "base.just"
tooling-bin-dir := project-root / "bin"

# sqlc tool settings.
sqlc-version := "v1.31.1"
sqlc-pkg := "github.com/sqlc-dev/sqlc/cmd/sqlc@" + sqlc-version
sqlc-dir := tooling-bin-dir / "sqlc-" + sqlc-version
sqlc := sqlc-dir / "sqlc" # SQLC path

# Ensure sqlc is installed.
ensure-sqlc: (_ensure-go-bin sqlc-pkg sqlc-dir)

# golangci-lint settings.
golangci-lint-version := "v2.12.2"
golangci-lint-pkg := "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@" + golangci-lint-version
golangci-lint-go-version := `go env GOVERSION`
golangci-lint-dir := tooling-bin-dir / "golangci-lint-" + golangci-lint-version + "-" + golangci-lint-go-version
golangci-lint := golangci-lint-dir / "golangci-lint"

# Ensure golangci-lint is installed.
ensure-golangci-lint: (_ensure-go-bin golangci-lint-pkg golangci-lint-dir)

# Install a Go command if its bin dir is missing.
#
# Example:
# _ensure-go-bin "github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1" "./bin/sqlc-v1.31.1"
_ensure-go-bin pkg dst-dir:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -d {{ dst-dir }} ]; then
      echo "{{ pkg }} is installed, skipped."
      exit 0
    fi
    echo "installing {{ pkg }} to {{ dst-dir }}"
    GOBIN={{ dst-dir }} go install {{ pkg }}
