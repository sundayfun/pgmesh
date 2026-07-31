import "toolings.just"

# Generate one sqlc plugin test case.
generate-case case: ensure-sqlc
    go build -o bin/sqlc-gen-store ./cmd/sqlc-gen-store
    cd "tests/{{ case }}" && {{ sqlc }} generate --file sqlc.yaml

# Generate every sqlc plugin test case.
generate-tests: ensure-sqlc
    go build -o bin/sqlc-gen-store ./cmd/sqlc-gen-store
    cd tests/same_package && {{ sqlc }} generate --file sqlc.yaml
    cd tests/separate_package && {{ sqlc }} generate --file sqlc.yaml
    cd tests/command_shapes && {{ sqlc }} generate --file sqlc.yaml
