package sqlcplugin

import (
	"context"
	"testing"

	"github.com/sqlc-dev/plugin-sdk-go/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateHonorsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	response, err := Generate(ctx, &plugin.GenerateRequest{})
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, response)
}

func TestParseOptionsRejectsRemovedFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		field string
		value string
	}{
		{name: "source package", field: "source_package", value: `"db"`},
		{name: "interface", field: "interface", value: `"Querier"`},
		{name: "type", field: "type", value: `"queryStore"`},
		{name: "target type", field: "target_type", value: `"Queries"`},
		{name: "target constructor", field: "target_constructor", value: `"New"`},
		{name: "receiver", field: "receiver", value: `"q"`},
		{name: "skip with tx", field: "skip_with_tx", value: `true`},
		{name: "read interface", field: "read_interface", value: `"readQuerier"`},
		{name: "write interface", field: "write_interface", value: `"writeQuerier"`},
		{name: "read type", field: "read_type", value: `"readQueries"`},
		{name: "write type", field: "write_type", value: `"writeQueries"`},
		{name: "read constructor", field: "read_constructor", value: `"newReadQueries"`},
		{name: "write constructor", field: "write_constructor", value: `"newWriteQueries"`},
		{name: "node constructor", field: "node_constructor", value: `"newStoreNode"`},
		{name: "sharded type", field: "sharded_type", value: `"meshStore"`},
		{name: "misspelled supported field", field: "constructer", value: `"NewStore"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := optionsRequest(`{"` + test.field + `":` + test.value + `}`)

			_, err := parseOptions(request)

			require.ErrorContains(t, err, `unknown field "`+test.field+`"`)
		})
	}
}

func TestParseOptionsRejectsRemovedOverrideAliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		options string
		field   string
	}{
		{
			name:    "postgres type",
			options: `{"overrides":[{"postgres_type":"text","go_type":"string"}]}`,
			field:   "postgres_type",
		},
		{
			name:    "null",
			options: `{"overrides":[{"db_type":"text","null":true,"go_type":"string"}]}`,
			field:   "null",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseOptions(optionsRequest(test.options))

			require.ErrorContains(t, err, `unknown field "`+test.field+`"`)
		})
	}
}

func TestParseOptionsDefaults(t *testing.T) {
	t.Parallel()

	opts, err := parseOptions(optionsRequest(`{}`))

	require.NoError(t, err)
	assert.Equal(t, defaultOutputFileName, opts.OutputFileName)
	assert.Equal(t, "generated", opts.PackageName)
	assert.Equal(t, defaultSQLPackage, opts.SQLPackage)
	assert.Equal(t, int32(1), *opts.QueryParameterLimit)
	assert.Equal(t, defaultStoreNew, opts.ConstructorName)
	assert.Equal(t, defaultStoreInterface, opts.StoreInterfaceName)
	assert.Equal(t, defaultRuntimePackage, opts.RuntimeImportPath)
	assert.Equal(t, defaultResolver, opts.ResolverInterfaceName)
	assert.Equal(t, defaultShardedNew, opts.ShardedConstructor)
	assert.False(t, opts.ExportSQLCTypes)
	assert.False(t, opts.EmitJSONTags)
}

func TestParseOptionsRetainsSupportedCustomizations(t *testing.T) {
	t.Parallel()

	opts, err := parseOptions(optionsRequest(`{
		"output_file_name":"mesh_store.go",
		"package":"store",
		"internal_import_path":"example.test/project/internal/db",
		"internal_import_alias":"db",
		"export_sqlc_types":true,
		"constructor":"BuildStore",
		"ignore_mirror_error":true,
		"store_interface":"QueryAPI",
		"runtime_import_path":"example.test/project/pgmesh",
		"resolver_interface":"Router",
		"sharded_constructor":"BuildSharded",
		"sql_package":"pgx/v5",
		"query_parameter_limit":3,
		"emit_params_struct_pointers":true,
		"emit_result_struct_pointers":true,
		"emit_pointers_for_null_types":true,
		"emit_pointers_for_null_enum_types":false,
		"emit_exact_table_names":true,
		"emit_json_tags":true,
		"rename":{"users":"People"},
		"overrides":[
			{"db_type":"text","go_type":"string"},
			{"column":"users.id","go_type":{"import":"example.test/types","package":"domain","type":"ID","pointer":true,"slice":true}}
		]
	}`))

	require.NoError(t, err)
	assert.Equal(t, "mesh_store.go", opts.OutputFileName)
	assert.Equal(t, "store", opts.PackageName)
	assert.Equal(t, "example.test/project/internal/db", opts.InternalImportPath)
	assert.Equal(t, "db", opts.InternalImportAlias)
	assert.True(t, opts.ExportSQLCTypes)
	assert.Equal(t, "BuildStore", opts.ConstructorName)
	assert.True(t, opts.IgnoreMirrorError)
	assert.Equal(t, "QueryAPI", opts.StoreInterfaceName)
	assert.Equal(t, "example.test/project/pgmesh", opts.RuntimeImportPath)
	assert.Equal(t, "Router", opts.ResolverInterfaceName)
	assert.Equal(t, "BuildSharded", opts.ShardedConstructor)
	assert.Equal(t, int32(3), *opts.QueryParameterLimit)
	assert.True(t, opts.EmitParamsStructPointers)
	assert.True(t, opts.EmitResultStructPointers)
	assert.True(t, opts.EmitPointersForNullTypes)
	require.NotNil(t, opts.EmitPointersForNullEnumType)
	assert.False(t, *opts.EmitPointersForNullEnumType)
	assert.True(t, opts.EmitExactTableNames)
	assert.True(t, opts.EmitJSONTags)
	assert.Equal(t, map[string]string{"users": "People"}, opts.Rename)
	require.Len(t, opts.Overrides, 2)
	assert.Equal(t, "string", opts.Overrides[0].typeName)
	assert.Equal(t, "[]*domain.ID", opts.Overrides[1].typeName)
	assert.Equal(t, "example.test/types", opts.Overrides[1].importPath)
	assert.Equal(t, "domain", opts.Overrides[1].importAlias)
}

func TestGenerateRetainsPublicNamesAndFixedInternals(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	request := optionsRequest(`{
		"constructor":"BuildStore",
		"store_interface":"QueryAPI",
		"resolver_interface":"Router",
		"sharded_constructor":"BuildSharded"
	}`)
	request.Queries = []*plugin.Query{{
		Name:     "GetUser",
		Cmd:      ":one",
		Comments: []string{"kind: read", "shard: tenant(tenant_id)", "store: Users"},
		Params: []*plugin.Parameter{{
			Number: 1,
			Column: &plugin.Column{Name: "tenant_id", Type: int8Type, NotNull: true},
		}},
		Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
	}}

	response, err := Generate(t.Context(), request)

	require.NoError(t, err)
	source := generatedSource(response)
	assert.Contains(t, source, "type QueryAPI interface")
	assert.Contains(t, source, "type Router[SK any] interface")
	assert.Contains(t, source, "func BuildStore(ctx context.Context, topology Topology, options ...StoreOption) (QueryAPI, error)")
	assert.Contains(
		t,
		source,
		"func BuildSharded[SK any](virtualShardCount uint64, shardHasher pgmesh.ShardHasher[SK], resolver Router[SK], options ...ShardedOption) Topology",
	)
	assert.Contains(t, source, "type queryStore struct")
	assert.Contains(t, source, "type readQueries struct")
	assert.Contains(t, source, "type writeQueries struct")
	assert.Contains(t, source, "type meshStore[SK any] struct")
	assert.Contains(t, source, "queries := New(database)")
	assert.Contains(t, source, "var _ Querier = (*queryStore)(nil)")
	assert.Contains(t, source, "func (q *queryStore) WithTx(tx pgx.Tx) *queryStore")
}

func TestParseOptionsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		options string
		want    string
	}{
		{name: "nested output path", options: `{"output_file_name":"gen/store.go"}`, want: "must be a visible base filename"},
		{name: "windows output path", options: `{"output_file_name":"gen\\store.go"}`, want: "must be a visible base filename"},
		{name: "non go output", options: `{"output_file_name":"store.txt"}`, want: "must be a visible base filename"},
		{name: "test output", options: `{"output_file_name":"store_test.go"}`, want: "not _test.go"},
		{name: "ignored output", options: `{"output_file_name":"_store.go"}`, want: "must be a visible base filename"},
		{name: "invalid package", options: `{"package":"store-api"}`, want: "must be a valid Go identifier"},
		{name: "keyword package", options: `{"package":"type"}`, want: "must be a valid Go identifier"},
		{name: "alias without import", options: `{"internal_import_alias":"db"}`, want: "requires internal_import_path"},
		{name: "exports without import", options: `{"export_sqlc_types":true}`, want: "requires internal_import_path"},
		{
			name:    "alias import conflict",
			options: `{"internal_import_path":"example.test/db","internal_import_alias":"pgx"}`,
			want:    "conflicts with a generated import",
		},
		{
			name:    "runtime path conflict",
			options: `{"runtime_import_path":"github.com/jackc/pgx/v5"}`,
			want:    "generated import alias",
		},
		{
			name: "internal path conflict",
			options: `{
				"runtime_import_path":"example.test/runtime",
				"internal_import_path":"example.test/runtime"
			}`,
			want: "generated import alias",
		},
		{name: "unexported constructor", options: `{"constructor":"buildStore"}`, want: "must be an exported Go identifier"},
		{name: "invalid store interface", options: `{"store_interface":"Query-API"}`, want: "must be a valid Go identifier"},
		{
			name:    "public name conflict",
			options: `{"constructor":"QueryAPI","store_interface":"QueryAPI"}`,
			want:    "conflicts with constructor",
		},
		{
			name:    "generated name conflict",
			options: `{"constructor":"Singleton"}`,
			want:    "conflicts with a generated declaration",
		},
		{
			name:    "source package name conflict",
			options: `{"store_interface":"Querier"}`,
			want:    "conflicts with a generated declaration",
		},
		{name: "invalid rename", options: `{"rename":{"users":"people-name"}}`, want: "must be a valid Go identifier"},
		{name: "empty rename key", options: `{"rename":{"":"People"}}`, want: "empty SQL identifier"},
		{name: "trailing json", options: `{} {}`, want: "expected one JSON object"},
		{
			name:    "empty override type",
			options: `{"overrides":[{"db_type":"text","go_type":{"type":""}}]}`,
			want:    "go_type must specify a nonempty type",
		},
		{
			name:    "invalid override type",
			options: `{"overrides":[{"db_type":"text","go_type":{"type":"Bad-Type"}}]}`,
			want:    "is not a valid Go type",
		},
		{
			name:    "invalid override package alias",
			options: `{"overrides":[{"db_type":"text","go_type":{"import":"example.test/types","package":"bad-alias","type":"ID"}}]}`,
			want:    "go_type package",
		},
		{
			name:    "invalid override import",
			options: `{"overrides":[{"db_type":"text","go_type":{"import":"example.test/bad path","type":"ID"}}]}`,
			want:    "go_type import",
		},
		{
			name:    "override alias conflicts with generated import",
			options: `{"overrides":[{"db_type":"text","go_type":{"import":"example.test/types","package":"pgx","type":"ID"}}]}`,
			want:    `import alias "pgx" conflicts`,
		},
		{
			name:    "override path conflicts with generated alias",
			options: `{"overrides":[{"db_type":"text","go_type":{"import":"github.com/jackc/pgx/v5","package":"database","type":"Identifier"}}]}`,
			want:    `conflicts with generated alias "pgx"`,
		},
		{
			name: "override aliases conflict with each other",
			options: `{"overrides":[
				{"db_type":"text","go_type":{"import":"example.test/one","package":"domain","type":"Name"}},
				{"db_type":"int8","go_type":{"import":"example.test/two","package":"domain","type":"ID"}}
			]}`,
			want: `import alias "domain" conflicts`,
		},
		{
			name:    "unknown go type field",
			options: `{"overrides":[{"db_type":"text","go_type":{"type":"Name","pointer_type":true}}]}`,
			want:    `unknown field "pointer_type"`,
		},
		{
			name:    "empty database type",
			options: `{"overrides":[{"db_type":"  ","go_type":"string"}]}`,
			want:    "must specify db_type or column",
		},
		{
			name:    "database type and column",
			options: `{"overrides":[{"db_type":"text","column":"users.name","go_type":"string"}]}`,
			want:    "cannot specify both db_type and column",
		},
		{
			name:    "invalid override pattern",
			options: `{"overrides":[{"column":"users.bad\\q","go_type":"string"}]}`,
			want:    "invalid escaped character",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseOptions(optionsRequest(test.options))

			require.ErrorContains(t, err, test.want)
		})
	}
}

func optionsRequest(pluginOptions string) *plugin.GenerateRequest {
	return &plugin.GenerateRequest{
		Settings: &plugin.Settings{
			Engine:  "postgresql",
			Codegen: &plugin.Codegen{Out: "generated"},
		},
		Catalog:       &plugin.Catalog{DefaultSchema: "public"},
		PluginOptions: []byte(pluginOptions),
	}
}
