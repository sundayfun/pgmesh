package sqlcplugin

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sqlc-dev/plugin-sdk-go/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateUsesArrayOverridesWithoutLeakingStructImports(t *testing.T) {
	t.Parallel()

	xidColumnType := &plugin.Identifier{Schema: "public", Name: "xid"}
	timestampType := &plugin.Identifier{Name: "timestamptz"}
	messageTable := &plugin.Identifier{Schema: "public", Name: "message"}

	resp, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{
			Engine: "postgresql",
			Codegen: &plugin.Codegen{
				Out: "internal",
			},
		},
		Catalog: &plugin.Catalog{
			DefaultSchema: "public",
			Schemas: []*plugin.Schema{{
				Name: "public",
				Tables: []*plugin.Table{{
					Rel: messageTable,
					Columns: []*plugin.Column{
						{Name: "id", NotNull: true, Type: xidColumnType},
						{Name: "created_at", NotNull: true, Type: timestampType},
					},
				}},
			}},
		},
		Queries: []*plugin.Query{{
			Name:     "ListMessagesWithIDs",
			Cmd:      ":many",
			Comments: []string{"kind: read", "store: Messages"},
			Text:     "SELECT * FROM message WHERE id = ANY(@ids::public.xid[])",
			Params: []*plugin.Parameter{{
				Number: 1,
				Column: &plugin.Column{
					Name:        "ids",
					Type:        xidColumnType,
					NotNull:     true,
					IsSqlcSlice: true,
				},
			}},
			Columns: []*plugin.Column{
				{Name: "id", NotNull: true, Type: xidColumnType, Table: messageTable},
				{Name: "created_at", NotNull: true, Type: timestampType, Table: messageTable},
			},
		}, {
			Name:     "CreateMessage",
			Cmd:      ":one",
			Comments: []string{"kind: write", "store: Messages", "CreateMessage can keep normal comments after the annotations."},
			Text:     "INSERT INTO message (id, created_at) VALUES ($1, $2) RETURNING *",
			Params: []*plugin.Parameter{{
				Number: 1,
				Column: &plugin.Column{
					Name:    "id",
					Type:    xidColumnType,
					NotNull: true,
				},
			}, {
				Number: 2,
				Column: &plugin.Column{
					Name:    "created_at",
					Type:    timestampType,
					NotNull: true,
				},
			}},
			Columns: []*plugin.Column{
				{Name: "id", NotNull: true, Type: xidColumnType, Table: messageTable},
				{Name: "created_at", NotNull: true, Type: timestampType, Table: messageTable},
			},
		}},
		PluginOptions: []byte(`{
			"package": "internal",
			"sql_package": "pgx/v5",
			"emit_params_struct_pointers": true,
			"emit_result_struct_pointers": true,
			"overrides": [
				{
					"db_type": "public.xid",
					"go_type": {
						"import": "github.com/sundayfun/siu/toolkit/xid",
						"type": "ID"
					}
				}
			]
		}`),
	})
	require.NoError(t, err)
	require.Equal(
		t,
		[]string{
			"store_querier_interfaces.go",
			"store_querier_read.go",
			"store_querier_write.go",
			"store_querier.go",
			"store_querier_messages.go",
			"store_querier_sharded.go",
		},
		generatedFileNames(resp),
	)
	interfaces := generatedFileContents(t, resp, "store_querier_interfaces.go")
	assert.Contains(t, interfaces, "type readQuerier interface")
	assert.Contains(t, interfaces, "type writeQuerier interface")
	assert.Contains(t, interfaces, "type Store interface")
	assert.NotContains(t, interfaces, "type MessagesReader interface")
	assert.NotContains(t, interfaces, "type MessagesWriter interface")
	assert.NotContains(t, interfaces, "type Messages interface")
	assert.NotContains(t, interfaces, "type readQueries struct")
	assert.NotContains(t, interfaces, "type writeQueries struct")

	read := generatedFileContents(t, resp, "store_querier_read.go")
	assert.Contains(t, read, "type readQueries struct")
	assert.NotContains(t, read, "type writeQueries struct")

	write := generatedFileContents(t, resp, "store_querier_write.go")
	assert.Contains(t, write, "type writeQueries struct")
	assert.NotContains(t, write, "type readQueries struct")

	store := generatedFileContents(t, resp, "store_querier.go")
	assert.Contains(t, store, "type meshStore[SK any] struct")
	assert.NotContains(t, store, "defaultShardKey")
	assert.Contains(t, store, "func NewStore(ctx context.Context, topology Topology, options ...StoreOption) (Store, error)")
	assert.Contains(t, store, "func Singleton(primary DBTX, options ...SingletonOption) Topology")
	assert.Contains(t, store, "// WithDatabaseName identifies the database in telemetry.")
	assert.Contains(t, store, "// WithReadReplicas appends databases used for round-robin reads.")
	assert.Contains(t, store, "// WithWriteMirrors appends databases that synchronously receive writes.")
	assert.NotContains(t, store, "type OneStore")
	assert.NotContains(t, store, "type ShardedStore")
	assert.NotContains(t, store, "func (q *groupedMeshStore[SK]) CreateMessage")
	assert.NotContains(t, store, "func (q *groupedMeshStore[SK]) ListMessagesWithIDs")
	assert.NotContains(t, generatedSource(resp), "oneStore")

	messages := generatedFileContents(t, resp, "store_querier_messages.go")
	assert.Contains(t, messages, "type MessagesReader interface")
	assert.Contains(t, messages, "type MessagesWriter interface")
	assert.Contains(t, messages, "type Messages interface")
	assert.Contains(t, messages, "func (q *meshStore[SK]) Messages() Messages")
	assert.Contains(t, messages, "func (q *groupedMeshStore[SK]) CreateMessage")
	assert.Contains(t, messages, "func (q *groupedMeshStore[SK]) ListMessagesWithIDs")

	sharded := generatedFileContents(t, resp, "store_querier_sharded.go")
	assert.NotContains(t, sharded, "type ShardedConfig")
	assert.NotContains(t, sharded, "type ShardDatabaseConfig")

	got := generatedSource(resp)
	assert.Contains(
		t,
		got,
		"type readQuerier interface {\n\t// ListMessagesWithIDs executes the generated ListMessagesWithIDs query.\n"+
			"\tListMessagesWithIDs(ctx context.Context, ids []xid.ID) ([]*Message, error)\n}",
	)
	assert.Contains(
		t,
		got,
		"type writeQuerier interface {\n\t// CreateMessage executes the generated CreateMessage query.\n"+
			"\tCreateMessage(ctx context.Context, arg *CreateMessageParams) (*Message, error)\n}",
	)
	assert.Contains(t, got, "type Store interface {\n\t// Messages returns the Messages query group.\n\tMessages() Messages\n}")
	assert.Contains(t, got, "type queryStore struct {\n\t*readQueries\n\t*writeQueries\n}")
	assert.Contains(t, got, "var _ readQuerier = (*readQueries)(nil)")
	assert.Contains(t, got, "var _ writeQuerier = (*writeQueries)(nil)")
	readBody := generatedMethodBody(t, got, "readQueries", "ListMessagesWithIDs")
	assert.NotContains(t, readBody, ".mirror(")
	assert.NotContains(t, readBody, "mirror.ListMessagesWithIDs")
	assert.Contains(t, readBody, "return rv0, nil")
	writeBody := generatedMethodBody(t, got, "writeQueries", "CreateMessage")
	assert.Contains(t, writeBody, "mirror.CreateMessage")
	assert.NotContains(t, got, `"time"`)
}

func TestGenerateGroupsPublicStoreQueries(t *testing.T) {
	t.Parallel()

	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{
			Engine:  "postgresql",
			Codegen: &plugin.Codegen{Out: "db"},
		},
		Catalog: &plugin.Catalog{DefaultSchema: "public"},
		Queries: []*plugin.Query{
			{
				Name:     "GetAccount",
				Cmd:      ":exec",
				Comments: []string{"kind: read", "store: Accounts"},
			},
			{
				Name:     "CreateAccount",
				Cmd:      ":exec",
				Comments: []string{"kind: write", "store: Accounts"},
			},
			{
				Name:     "Ping",
				Cmd:      ":exec",
				Comments: []string{"kind: read", "store: System"},
			},
		},
		PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5"}`),
	})
	require.NoError(t, err)

	got := generatedSource(response)
	reader := generatedInterfaceBody(t, got, "AccountsReader")
	assert.Contains(t, reader, "GetAccount(ctx context.Context, storeOptions ...QueryOption) error")
	assert.NotContains(t, reader, "CreateAccount")

	writer := generatedInterfaceBody(t, got, "AccountsWriter")
	assert.Contains(t, writer, "CreateAccount(ctx context.Context, storeOptions ...QueryOption) error")
	assert.NotContains(t, writer, "GetAccount")

	group := generatedInterfaceBody(t, got, "Accounts")
	assert.Contains(t, group, "AccountsReader")
	assert.Contains(t, group, "AccountsWriter")

	store := generatedInterfaceBody(t, got, "Store")
	assert.Contains(t, store, "Accounts() Accounts")
	assert.Contains(t, store, "System() System")
	assert.NotContains(t, store, "GetAccount")
	assert.NotContains(t, store, "CreateAccount")
	assert.NotContains(t, store, "Ping(ctx")

	assert.Contains(t, got, "type groupedMeshStore[SK any] struct")
	assert.Contains(t, got, "var _ Accounts = (*groupedMeshStore[uint8])(nil)")
	assert.Contains(t, got, "func (q *meshStore[SK]) Accounts() Accounts")
	assert.Contains(t, got, "return &groupedMeshStore[SK]{store: q}")
	assert.Contains(
		t,
		got,
		"func (q *groupedMeshStore[SK]) GetAccount(ctx context.Context, storeOptions ...QueryOption) (err error)",
	)
	groupedBody := generatedMethodBody(t, got, "groupedMeshStore[SK]", "GetAccount")
	assert.Contains(t, groupedBody, "q.store.mesh.StartSpan")
	assert.Contains(t, groupedBody, "q.store.mesh.Shard")
	assert.NotContains(t, groupedBody, "q.mesh")
	assert.Contains(t, got, "func (q *groupedMeshStore[SK]) Ping(")

	accounts := generatedFileContents(t, response, "store_querier_accounts.go")
	assert.Contains(t, accounts, "type Accounts interface")
	assert.Contains(t, accounts, "func (q *groupedMeshStore[SK]) GetAccount(")
	assert.Contains(t, accounts, "func (q *groupedMeshStore[SK]) CreateAccount(")
	assert.NotContains(t, accounts, "func (q *groupedMeshStore[SK]) Ping(")

	system := generatedFileContents(t, response, "store_querier_system.go")
	assert.Contains(t, system, "type System interface")
	assert.Contains(t, system, "func (q *groupedMeshStore[SK]) Ping(")
	assert.NotContains(t, system, "func (q *groupedMeshStore[SK]) GetAccount(")
}

func TestStoreGroupOutputFileNamesAvoidCollisions(t *testing.T) {
	t.Parallel()

	assert.Equal(
		t,
		[]string{
			"store_querier_group_interfaces.go",
			"store_querier_iam.go",
			"store_querier_group_iam.go",
		},
		storeGroupOutputFileNames("store_querier.go", []storeGroup{
			{name: "Interfaces"},
			{name: "IAM"},
			{name: "Iam"},
		}),
	)
}

func TestGenerateRejectsInvalidStoreGroups(t *testing.T) {
	t.Parallel()

	request := func(queries ...*plugin.Query) *plugin.GenerateRequest {
		return &plugin.GenerateRequest{
			Settings: &plugin.Settings{
				Engine:  "postgresql",
				Codegen: &plugin.Codegen{Out: "db"},
			},
			Catalog:       &plugin.Catalog{DefaultSchema: "public"},
			Queries:       queries,
			PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5"}`),
		}
	}
	query := func(name, store string) *plugin.Query {
		comments := []string{"kind: read"}
		if store != "" {
			comments = append(comments, "store: "+store)
		}
		return &plugin.Query{Name: name, Cmd: ":exec", Comments: comments}
	}

	tests := []struct {
		name    string
		request *plugin.GenerateRequest
		want    string
	}{
		{
			name:    "missing annotation",
			request: request(query("GetAccount", "")),
			want:    "missing required store annotation",
		},
		{
			name:    "root store interface",
			request: request(query("GetAccount", "Store")),
			want:    "conflicts with store interface",
		},
		{
			name: "another group derived interface",
			request: request(
				query("GetAccount", "Accounts"),
				query("GetAccountAudit", "AccountsReader"),
			),
			want: "declaration AccountsReader",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Generate(t.Context(), test.request)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func generatedMethodBody(t *testing.T, source, receiverType, methodName string) string {
	t.Helper()

	start := strings.Index(source, "func (q *"+receiverType+") "+methodName+"(")
	require.NotEqual(t, -1, start, "generated output missing %s.%s method", receiverType, methodName)
	rest := source[start:]
	end := strings.Index(rest, "\n}\n\n")
	if end == -1 {
		end = strings.Index(rest, "\n}\n")
	}
	require.NotEqual(t, -1, end, "generated output missing end of %s method", methodName)
	return rest[:end+3]
}

func generatedInterfaceBody(t *testing.T, source, name string) string {
	t.Helper()

	start := strings.Index(source, "type "+name+" interface {")
	require.NotEqual(t, -1, start, "generated output missing %s interface", name)
	rest := source[start:]
	end := strings.Index(rest, "\n}\n")
	require.NotEqual(t, -1, end, "generated output missing end of %s interface", name)
	return rest[:end+3]
}

func generatedSource(response *plugin.GenerateResponse) string {
	var source strings.Builder
	for _, file := range response.GetFiles() {
		source.Write(file.GetContents())
		source.WriteString("\n")
	}
	return source.String()
}

func generatedFileNames(response *plugin.GenerateResponse) []string {
	names := make([]string, 0, len(response.GetFiles()))
	for _, file := range response.GetFiles() {
		names = append(names, file.GetName())
	}
	return names
}

func generatedFileContents(t *testing.T, response *plugin.GenerateResponse, name string) string {
	t.Helper()
	for _, file := range response.GetFiles() {
		if file.GetName() == name {
			return string(file.GetContents())
		}
	}
	require.Failf(t, "generated response missing file", "missing %s", name)
	return ""
}

func TestNamedResultsSignatureAvoidsParameterAndReceiverNames(t *testing.T) {
	t.Parallel()

	signature, names, errName := namedResultsSignature(
		[]argument{{name: "result"}, {name: "err"}},
		[]string{"int64", "error"},
		"result2",
	)

	assert.Equal(t, " (result3 int64, err2 error)", signature)
	assert.Equal(t, []string{"result3", "err2"}, names)
	assert.Equal(t, "err2", errName)
}

func TestOutputPackageName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		out  string
		want string
	}{
		{name: "directory basename", out: "generated/internal", want: "internal"},
		{name: "current directory fallback", out: ".", want: "db"},
		{name: "invalid identifier fallback", out: "generated-db", want: "db"},
		{name: "keyword fallback", out: "type", want: "db"},
		{name: "empty fallback", want: "db"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := &plugin.GenerateRequest{Settings: &plugin.Settings{Codegen: &plugin.Codegen{Out: test.out}}}
			assert.Equal(t, test.want, outputPackageName(request))
		})
	}
}

func TestClassifyQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     *plugin.Query
		want      queryKind
		wantRoute *routeAnnotation
		wantStore string
		wantErr   string
	}{
		{
			name: "select",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"kind: read", "store: Messages"},
			},
			want:      queryKindRead,
			wantStore: "Messages",
		},
		{
			name: "shard route",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"kind: read", "shard: p2p(user_id, peer_id)", "store: Messages", "documentation"},
			},
			want:      queryKindRead,
			wantRoute: &routeAnnotation{name: "p2p", operands: []string{"user_id", "peer_id"}},
			wantStore: "Messages",
		},
		{
			name: "store group",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"kind: read", "store: Messages", "documentation"},
			},
			want:      queryKindRead,
			wantStore: "Messages",
		},
		{
			name: "shard route and store group",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"kind: read", "shard: inbox(user_id)", "store: Messages"},
			},
			want:      queryKindRead,
			wantRoute: &routeAnnotation{name: "inbox", operands: []string{"user_id"}},
			wantStore: "Messages",
		},
		{
			name: "shard route without operands",
			query: &plugin.Query{
				Name:     "GetGlobalSetting",
				Comments: []string{"kind: read", "shard: global()", "store: Settings"},
			},
			want:      queryKindRead,
			wantRoute: &routeAnnotation{name: "global", operands: nil},
			wantStore: "Settings",
		},
		{
			name: "insert returning",
			query: &plugin.Query{
				Name:     "CreateMessage",
				Comments: []string{"kind: write", "store: Messages"},
				Text:     "INSERT INTO message (id) VALUES ($1) RETURNING *",
			},
			want:      queryKindWrite,
			wantStore: "Messages",
		},
		{
			name: "allows comments after annotation",
			query: &plugin.Query{
				Name:     "UpdateMessage",
				Comments: []string{"kind: write", "store: Messages", "normal comment"},
			},
			want:      queryKindWrite,
			wantStore: "Messages",
		},
		{
			name: "falls back to leading sql comment",
			query: &plugin.Query{
				Name: "CreateMessage",
				Text: "-- name: CreateMessage :one\n-- kind: write\n-- store: Messages\nINSERT INTO message (id) VALUES ($1) RETURNING *",
			},
			want:      queryKindWrite,
			wantStore: "Messages",
		},
		{
			name: "kind annotation must be adjacent to sqlc name",
			query: &plugin.Query{
				Name: "CreateMessage",
				Text: "-- name: CreateMessage :one\n\n-- kind: write\nINSERT INTO message (id) VALUES ($1) RETURNING *",
			},
			wantErr: "kind annotation must immediately follow",
		},
		{
			name: "shard annotation must be adjacent to kind",
			query: &plugin.Query{
				Name: "ListMessages",
				Text: "-- name: ListMessages :many\n-- kind: read\n\n-- shard: inbox(user_id)\nSELECT 1",
			},
			wantErr: "shard annotation must immediately follow",
		},
		{
			name: "store annotation must be adjacent to metadata",
			query: &plugin.Query{
				Name: "ListMessages",
				Text: "-- name: ListMessages :many\n-- kind: read\n\n-- store: Messages\nSELECT 1",
			},
			wantErr: "store annotation must immediately follow",
		},
		{
			name: "shard annotation must be second",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"kind: read", "documentation", "shard: p2p(user_id, peer_id)"},
			},
			wantErr: "must immediately follow",
		},
		{
			name: "malformed shard annotation",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"kind: read", "shard: user_id"},
			},
			wantErr: "malformed shard annotation",
		},
		{
			name: "store annotation must follow shard metadata",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"kind: read", "store: Messages", "shard: inbox(user_id)"},
			},
			wantErr: "shard annotation must immediately follow",
		},
		{
			name: "store annotation must precede documentation",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"kind: read", "documentation", "store: Messages"},
			},
			wantErr: "store annotation must immediately follow",
		},
		{
			name: "store annotation must be exported",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"kind: read", "store: messages"},
			},
			wantErr: "expected an exported Go identifier",
		},
		{
			name: "invalid shard route name",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"kind: read", "shard: 1route(user_id)"},
			},
			wantErr: "invalid shard route name",
		},
		{
			name: "invalid shard operand",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"kind: read", "shard: inbox(1user_id)"},
			},
			wantErr: "invalid shard operand",
		},
		{
			name: "duplicate shard operand",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"kind: read", "shard: inbox(user_id, user_id)"},
			},
			wantErr: "repeats shard operand",
		},
		{
			name: "missing annotation",
			query: &plugin.Query{
				Name: "ListMessages",
				Text: "SELECT * FROM message",
			},
			wantErr: "missing required kind annotation",
		},
		{
			name: "missing store annotation",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"kind: read"},
			},
			wantErr: "missing required store annotation",
		},
		{
			name: "annotation must be first comment",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"normal comment", "kind: read"},
			},
			wantErr: "first comment must be kind annotation",
		},
		{
			name: "invalid annotation",
			query: &plugin.Query{
				Name:     "ListMessages",
				Comments: []string{"kind: maybe"},
			},
			wantErr: "invalid kind annotation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, route, store, err := classifyQuery(tt.query)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantRoute, route)
			assert.Equal(t, tt.wantStore, store)
		})
	}
}

func TestGenerateShardRoutedFacade(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	request := &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog:  &plugin.Catalog{DefaultSchema: "public"},
		Queries: []*plugin.Query{
			{
				Name:     "ListP2PMessages",
				Cmd:      ":many",
				Comments: []string{"kind: read", "shard: p2p(user_id, peer_id)", "store: Messages"},
				Params: []*plugin.Parameter{
					{Number: 1, Column: &plugin.Column{Name: "user_id", Type: int8Type, NotNull: true}},
					{Number: 2, Column: &plugin.Column{Name: "peer_id", Type: int8Type, NotNull: true}},
				},
				Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
			},
			{
				Name:     "CreateP2PMessage",
				Cmd:      ":one",
				Comments: []string{"kind: write", "shard: p2p(user_id, peer_id)", "store: Messages"},
				Params: []*plugin.Parameter{
					{Number: 1, Column: &plugin.Column{Name: "user_id", Type: int8Type, NotNull: true}},
					{Number: 2, Column: &plugin.Column{Name: "peer_id", Type: int8Type, NotNull: true}},
				},
				Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
			},
		},
		PluginOptions: []byte(`{
			"package":"db",
			"sql_package":"pgx/v5",
			"query_parameter_limit":1,
			"emit_params_struct_pointers":true
		}`),
	}

	response, err := Generate(t.Context(), request)
	require.NoError(t, err)
	got := generatedSource(response)
	checks := []string{
		`func NewStore(ctx context.Context, topology Topology, options ...StoreOption) (Store, error)`,
		`func Singleton(primary DBTX, options ...SingletonOption) Topology`,
		`func Sharded[SK any](numVShards uint64, shardHasher pgmesh.ShardHasher[SK], resolver ShardResolver[SK], options ...ShardedOption) Topology`,
		`func WithReplicaSet(name string, primary DBTX, replicas ...DBTX) ShardedOption`,
		`func WithVShardMapping(mainReplicaSet string, vshards []uint64, mirrorReplicaSets ...string) ShardedOption`,
		`func newStoreNode(database DBTX) pgmesh.Node[*readQueries, *queryStore]`,
		"type ShardResolver[SK any] interface {\n\t// P2P resolves the \"p2p\" shard route.\n" +
			"\tP2P(userID int64, peerID int64) SK\n}",
		"type meshStore[SK any] struct",
		"type Store interface",
		"func ReadFromPrimary() QueryOption",
		"func WithTx(tx pgx.Tx) QueryOption",
		"func (q *groupedMeshStore[SK]) ListP2PMessages(ctx context.Context, arg *ListP2PMessagesParams, storeOptions ...QueryOption) (result []int64, err error)",
		"var shardKey SK",
		"shardKey = q.store.resolver.P2P(arg.UserID, arg.PeerID)",
		`q.store.mesh.StartSpan(ctx, "Messages", "ListP2PMessages", pgmesh.QueryKindRead)`,
		`q.store.mesh.StartSpan(ctx, "Messages", "CreateP2PMessage", pgmesh.QueryKindWrite)`,
		"// Trace the query and record its returned error.",
		"defer func() { querySpan.End(err) }()",
		"// Resolve the shard key for this topology.",
		"// Apply options that can override the default route.",
		"querySpan.SetRoute(shard.VShardIndex(), shard.Name(), pgmesh.RouteModeRead)",
		"querySpan.SetRoute(shard.VShardIndex(), shard.Name(), pgmesh.RouteModeTransaction)",
		"return shard.Read().ListP2PMessages(ctx, arg)",
		"return shard.Write().WithTx(options.tx).ListP2PMessages(ctx, arg)",
		"target := shard.Write()",
		"querySpan.SetRoute(shard.VShardIndex(), shard.Name(), mode)",
		"return target.CreateP2PMessage(ctx, arg)",
	}
	for _, check := range checks {
		assert.Contains(t, got, check)
	}
	assert.NotContains(t, got, "WriteMirrorCount()")
	assert.NotContains(t, got, "writeMirrorCount")
	meshReadBody := generatedMethodBody(t, got, "groupedMeshStore[SK]", "ListP2PMessages")
	assert.NotContains(t, meshReadBody, "var queryErr error")
	assert.NotContains(t, meshReadBody, "queryErr =")
	assert.Contains(t, meshReadBody, "switch {")
	assert.Contains(t, meshReadBody, "case options.tx != nil:")
	assert.Contains(t, meshReadBody, "case options.primary:")
	assert.Contains(t, meshReadBody, "default:")
	assert.NotContains(t, meshReadBody, "if options.tx != nil")
	assert.NotContains(t, meshReadBody, "if options.primary")
	assert.Equal(t, 1, strings.Count(got, "type meshStore[SK any] struct"))
	assert.Equal(t, 1, strings.Count(got, "func (q *groupedMeshStore[SK]) ListP2PMessages("))
	assert.NotContains(t, got, "defaultShardKey")
	assert.NotContains(t, got, "type databaseStore struct")
	assert.NotContains(t, got, "type shardedStore[SK any] struct")
	assert.Contains(t, got, "func (q *queryStore) WithTx(tx pgx.Tx) *queryStore")
	assert.Contains(t, got, "return newQueryStore(q.writeQueries.main.WithTx(tx))")
}

func TestGenerateWrapsRoutingOnlyShardOperand(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	int4Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int4"}
	users := &plugin.Identifier{Schema: "public", Name: "users"}
	request := &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog: &plugin.Catalog{
			DefaultSchema: "public",
			Schemas: []*plugin.Schema{{
				Name: "public",
				Tables: []*plugin.Table{{
					Rel: users,
					Columns: []*plugin.Column{
						{Name: "id", Type: int8Type, NotNull: true, Table: users},
						{Name: "tenant_id", Type: int8Type, NotNull: true, Table: users},
					},
				}},
			}},
		},
		Queries: []*plugin.Query{{
			Name:     "ListTenantUsers",
			Cmd:      ":many",
			Comments: []string{"kind: read", "shard: tenant(tenant_id)", "store: Users"},
			Params: []*plugin.Parameter{
				{Number: 1, Column: &plugin.Column{Name: "limit", Type: int4Type, NotNull: true}},
				{Number: 2, Column: &plugin.Column{Name: "offset", Type: int4Type, NotNull: true}},
			},
			Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true, Table: users}},
		}},
		PluginOptions: []byte(`{
			"package":"db",
			"sql_package":"pgx/v5",
			"query_parameter_limit":1,
			"emit_params_struct_pointers":true
		}`),
	}
	response, err := Generate(t.Context(), request)
	require.NoError(t, err)

	got := generatedSource(response)
	assert.Contains(
		t,
		got,
		"type ListTenantUsersShardParams struct {\n\tLimit    int32\n\tOffset   int32\n\tTenantID int64\n}",
	)
	assert.Contains(
		t,
		got,
		"func (arg *ListTenantUsersShardParams) sqlcParams() *ListTenantUsersParams {\n"+
			"\treturn &ListTenantUsersParams{\n"+
			"\t\tLimit:  arg.Limit,\n"+
			"\t\tOffset: arg.Offset,\n"+
			"\t}\n}",
	)
	assert.Contains(
		t,
		got,
		"ListTenantUsers(ctx context.Context, arg *ListTenantUsersShardParams, storeOptions ...QueryOption) ([]int64, error)",
	)
	assert.Contains(
		t,
		got,
		"func (q *groupedMeshStore[SK]) ListTenantUsers(ctx context.Context, arg *ListTenantUsersShardParams, storeOptions ...QueryOption)",
	)
	body := generatedMethodBody(t, got, "groupedMeshStore[SK]", "ListTenantUsers")
	assert.Contains(t, body, "shardKey = q.store.resolver.Tenant(arg.TenantID)")
	assert.Contains(t, body, "return shard.Read().ListTenantUsers(ctx, arg.sqlcParams())")
	assert.Contains(t, got, "ListTenantUsers(ctx context.Context, arg *ListTenantUsersParams) ([]int64, error)")
	assert.Contains(t, got, "Tenant(tenantID int64) SK")

	request.PluginOptions = []byte(`{
		"package":"db",
		"sql_package":"pgx/v5",
		"query_parameter_limit":1
	}`)
	valueResponse, err := Generate(t.Context(), request)
	require.NoError(t, err)
	valueSource := generatedSource(valueResponse)
	assert.Contains(
		t,
		valueSource,
		"func (arg ListTenantUsersShardParams) sqlcParams() ListTenantUsersParams {\n"+
			"\treturn ListTenantUsersParams{",
	)
	assert.Contains(
		t,
		valueSource,
		"ListTenantUsers(ctx context.Context, arg ListTenantUsersShardParams, storeOptions ...QueryOption)",
	)
}

func TestGenerateGroupsListValuedManyByPhysicalShard(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	textType := &plugin.Identifier{Schema: "pg_catalog", Name: "text"}
	users := &plugin.Identifier{Schema: "public", Name: "users"}
	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog: &plugin.Catalog{
			DefaultSchema: "public",
			Schemas: []*plugin.Schema{{
				Name: "public",
				Tables: []*plugin.Table{{
					Rel: users,
					Columns: []*plugin.Column{
						{Name: "id", Type: int8Type, NotNull: true, Table: users},
						{Name: "tenant_id", Type: int8Type, NotNull: true, Table: users},
						{Name: "name", Type: textType, NotNull: true, Table: users},
					},
				}},
			}},
		},
		Queries: []*plugin.Query{
			{
				Name:     "GetUser",
				Cmd:      ":one",
				Comments: []string{"kind: read", "shard: tenant(tenant_id)", "store: Users"},
				Params: []*plugin.Parameter{
					{Number: 1, Column: &plugin.Column{Name: "tenant_id", Type: int8Type, NotNull: true}},
					{Number: 2, Column: &plugin.Column{Name: "id", Type: int8Type, NotNull: true}},
				},
				Columns: []*plugin.Column{
					{Name: "id", Type: int8Type, NotNull: true, Table: users},
					{Name: "tenant_id", Type: int8Type, NotNull: true, Table: users},
					{Name: "name", Type: textType, NotNull: true, Table: users},
				},
			},
			{
				Name:     "ListUsersByID",
				Cmd:      ":many",
				Comments: []string{"kind: read", "shard: tenant(tenant_id)", "store: Users"},
				Params: []*plugin.Parameter{{
					Number: 1,
					Column: &plugin.Column{
						Name:      "id",
						Type:      int8Type,
						NotNull:   true,
						IsArray:   true,
						ArrayDims: 1,
					},
				}},
				Columns: []*plugin.Column{
					{Name: "id", Type: int8Type, NotNull: true, Table: users},
					{Name: "tenant_id", Type: int8Type, NotNull: true, Table: users},
					{Name: "name", Type: textType, NotNull: true, Table: users},
				},
			},
		},
		PluginOptions: []byte(`{
			"package":"db",
			"sql_package":"pgx/v5",
			"query_parameter_limit":1,
			"emit_params_struct_pointers":true,
			"emit_result_struct_pointers":true
		}`),
	})
	require.NoError(t, err)

	got := generatedSource(response)
	assert.Contains(
		t,
		got,
		"type ListUsersByIDShardParams struct {\n\tID       int64\n\tTenantID int64\n}",
	)
	assert.Contains(
		t,
		got,
		"ListUsersByID(ctx context.Context, arg []*ListUsersByIDShardParams, storeOptions ...QueryOption) ([]*User, error)",
	)
	assert.Contains(t, got, "Tenant(tenantID int64) SK")
	body := generatedMethodBody(t, got, "groupedMeshStore[SK]", "ListUsersByID")
	assert.Contains(t, body, "if item == nil")
	assert.Contains(t, body, "lookupValue := item.ID")
	assert.Contains(t, body, "shardKey = q.store.resolver.Tenant(item.TenantID)")
	assert.Contains(t, body, "groupsByName[shard.Name()]")
	assert.Contains(t, body, "shardGroup.args = append(shardGroup.args, lookupValue)")
	assert.Contains(t, body, "shardGroup.shard.Read().ListUsersByID(ctx, shardGroup.args)")
	assert.Contains(t, body, "resultKey := any(row.ID)")
	assert.Contains(t, body, "rowsByGroup[orderedItem.shardName][orderedItem.key]")
	assert.Contains(t, body, "pgmesh.ErrCrossShardTransaction")
	assert.Contains(t, body, "reflect.ValueOf(lookupKey).Comparable()")

	response, err = Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog: &plugin.Catalog{
			DefaultSchema: "public",
			Schemas: []*plugin.Schema{{
				Name: "public",
				Tables: []*plugin.Table{{
					Rel: users,
					Columns: []*plugin.Column{
						{Name: "id", Type: int8Type, NotNull: true, Table: users},
						{Name: "tenant_id", Type: int8Type, NotNull: true, Table: users},
						{Name: "name", Type: textType, NotNull: true, Table: users},
					},
				}},
			}},
		},
		Queries: []*plugin.Query{{
			Name:     "ListUsersByID",
			Cmd:      ":many",
			Comments: []string{"kind: read", "shard: tenant(tenant_id)", "store: Users"},
			Params: []*plugin.Parameter{{
				Number: 1,
				Column: &plugin.Column{
					Name:      "id",
					Type:      int8Type,
					NotNull:   true,
					IsArray:   true,
					ArrayDims: 1,
				},
			}},
			Columns: []*plugin.Column{
				{Name: "id", Type: int8Type, NotNull: true, Table: users},
				{Name: "tenant_id", Type: int8Type, NotNull: true, Table: users},
				{Name: "name", Type: textType, NotNull: true, Table: users},
			},
		}},
		PluginOptions: []byte(`{
			"package":"db",
			"sql_package":"pgx/v5",
			"query_parameter_limit":1
		}`),
	})
	require.NoError(t, err)
	valueSource := generatedSource(response)
	assert.Contains(
		t,
		valueSource,
		"ListUsersByID(ctx context.Context, arg []ListUsersByIDShardParams, storeOptions ...QueryOption)",
	)
	valueBody := generatedMethodBody(t, valueSource, "groupedMeshStore[SK]", "ListUsersByID")
	assert.NotContains(t, valueBody, "if item == nil")
}

func TestGenerateScalarizesGroupedManyResolverOperand(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog:  &plugin.Catalog{DefaultSchema: "public"},
		Queries: []*plugin.Query{{
			Name:     "ListUsersByID",
			Cmd:      ":many",
			Comments: []string{"kind: read", "shard: user(id)", "store: Users"},
			Params: []*plugin.Parameter{{
				Number: 1,
				Column: &plugin.Column{
					Name:        "id",
					Type:        int8Type,
					NotNull:     true,
					IsSqlcSlice: true,
				},
			}},
			Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
		}},
		PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5","query_parameter_limit":1}`),
	})
	require.NoError(t, err)

	got := generatedSource(response)
	assert.Contains(t, got, "User(iD int64) SK")
	assert.Contains(
		t,
		got,
		"ListUsersByID(ctx context.Context, id []int64, storeOptions ...QueryOption) ([]int64, error)",
	)
	body := generatedMethodBody(t, got, "groupedMeshStore[SK]", "ListUsersByID")
	assert.Contains(t, body, "lookupValue := item")
	assert.Contains(t, body, "shardKey = q.store.resolver.User(item)")
	assert.Contains(t, body, "resultKey := any(row)")

	request := &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog:  &plugin.Catalog{DefaultSchema: "public"},
		Queries: []*plugin.Query{{
			Name:     "ListUsersByID",
			Cmd:      ":many",
			Comments: []string{"kind: read", "shard: user(id)", "store: Users"},
			Params: []*plugin.Parameter{{
				Number: 1,
				Column: &plugin.Column{
					Name:        "id",
					Type:        int8Type,
					NotNull:     true,
					IsSqlcSlice: true,
				},
			}},
			Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
		}},
		PluginOptions: []byte(`{
			"package":"db",
			"sql_package":"pgx/v5",
			"query_parameter_limit":0,
			"emit_params_struct_pointers":true
		}`),
	}
	structResponse, err := Generate(t.Context(), request)
	require.NoError(t, err)
	structSource := generatedSource(structResponse)
	assert.Contains(t, structSource, "type ListUsersByIDShardParams struct {\n\tID int64\n}")
	assert.Contains(
		t,
		structSource,
		"ListUsersByID(ctx context.Context, arg []*ListUsersByIDShardParams, storeOptions ...QueryOption)",
	)
	structBody := generatedMethodBody(t, structSource, "groupedMeshStore[SK]", "ListUsersByID")
	assert.Contains(
		t,
		structBody,
		"ListUsersByID(ctx, &ListUsersByIDParams{ID: shardGroup.args})",
	)
}

func TestGenerateLeavesOtherManyShapesSingleShardRouted(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog:  &plugin.Catalog{DefaultSchema: "public"},
		Queries: []*plugin.Query{
			{
				Name:     "ListTenantUsersByID",
				Cmd:      ":many",
				Comments: []string{"kind: read", "shard: tenant(tenant_id)", "store: Users"},
				Params: []*plugin.Parameter{
					{
						Number: 1,
						Column: &plugin.Column{
							Name:      "id",
							Type:      int8Type,
							NotNull:   true,
							IsArray:   true,
							ArrayDims: 1,
						},
					},
					{Number: 2, Column: &plugin.Column{Name: "tenant_id", Type: int8Type, NotNull: true}},
				},
				Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
			},
			{
				Name:     "ListUsersByIDMatrix",
				Cmd:      ":many",
				Comments: []string{"kind: read", "shard: user(id)", "store: Users"},
				Params: []*plugin.Parameter{{
					Number: 1,
					Column: &plugin.Column{
						Name:      "id",
						Type:      int8Type,
						NotNull:   true,
						IsArray:   true,
						ArrayDims: 2,
					},
				}},
				Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
			},
		},
		PluginOptions: []byte(`{
			"package":"db",
			"sql_package":"pgx/v5",
			"query_parameter_limit":1,
			"emit_params_struct_pointers":true
		}`),
	})
	require.NoError(t, err)

	got := generatedSource(response)
	assert.NotContains(t, got, "manyShardGroup")
	tenantBody := generatedMethodBody(t, got, "groupedMeshStore[SK]", "ListTenantUsersByID")
	assert.Contains(t, tenantBody, "shardKey = q.store.resolver.Tenant(arg.TenantID)")
	assert.Contains(t, tenantBody, "return shard.Read().ListTenantUsersByID(ctx, arg)")
	matrixBody := generatedMethodBody(t, got, "groupedMeshStore[SK]", "ListUsersByIDMatrix")
	assert.Contains(t, matrixBody, "shardKey = q.store.resolver.User(id)")
	assert.Contains(t, matrixBody, "return shard.Read().ListUsersByIDMatrix(ctx, id)")
}

func TestGenerateUsesRenamedModelFieldForRoutingOnlyShardOperand(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	users := &plugin.Identifier{Schema: "public", Name: "users"}
	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog: &plugin.Catalog{
			DefaultSchema: "public",
			Schemas: []*plugin.Schema{{
				Name: "public",
				Tables: []*plugin.Table{{
					Rel: users,
					Columns: []*plugin.Column{
						{Name: "id", Type: int8Type, NotNull: true, Table: users},
						{Name: "tenant_id", Type: int8Type, NotNull: true, Table: users},
					},
				}},
			}},
		},
		Queries: []*plugin.Query{{
			Name:     "GetShardUser",
			Cmd:      ":one",
			Comments: []string{"kind: read", "shard: tenant(tenant_id)", "store: Users"},
			Params: []*plugin.Parameter{{
				Number: 1,
				Column: &plugin.Column{Name: "id", Type: int8Type, NotNull: true, Table: users},
			}},
			Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true, Table: users}},
		}},
		PluginOptions: []byte(`{
			"package":"db",
			"sql_package":"pgx/v5",
			"rename":{"tenant_id":"Shard"}
		}`),
	})
	require.NoError(t, err)

	got := generatedSource(response)
	assert.Contains(t, got, "type GetShardUserShardParams struct {\n\tID    int64\n\tShard int64\n}")
	assert.Contains(t, got, "GetShardUser(ctx context.Context, arg GetShardUserShardParams, storeOptions ...QueryOption)")
	body := generatedMethodBody(t, got, "groupedMeshStore[SK]", "GetShardUser")
	assert.Contains(t, body, "shardKey = q.store.resolver.Tenant(arg.Shard)")
	assert.Contains(t, body, "return shard.Read().GetShardUser(ctx, arg.ID)")
	assert.Contains(t, got, "Tenant(shard int64) SK")
}

func TestGenerateAllowsCompatibleShardFieldsOnMultipleModels(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	users := &plugin.Identifier{Schema: "public", Name: "users"}
	accounts := &plugin.Identifier{Schema: "public", Name: "accounts"}
	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog: &plugin.Catalog{
			DefaultSchema: "public",
			Schemas: []*plugin.Schema{{
				Name: "public",
				Tables: []*plugin.Table{
					{
						Rel: users,
						Columns: []*plugin.Column{
							{Name: "id", Type: int8Type, NotNull: true, Table: users},
							{Name: "tenant_id", Type: int8Type, NotNull: true, Table: users},
						},
					},
					{
						Rel: accounts,
						Columns: []*plugin.Column{
							{Name: "id", Type: int8Type, NotNull: true, Table: accounts},
							{Name: "tenant_id", Type: int8Type, NotNull: true, Table: accounts},
						},
					},
				},
			}},
		},
		Queries: []*plugin.Query{{
			Name:     "ListMemberships",
			Cmd:      ":many",
			Comments: []string{"kind: read", "shard: tenant(tenant_id)", "store: Memberships"},
			Columns: []*plugin.Column{
				{Name: "user_id", OriginalName: "id", Type: int8Type, NotNull: true, Table: users},
				{Name: "account_id", OriginalName: "id", Type: int8Type, NotNull: true, Table: accounts},
			},
		}},
		PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5"}`),
	})
	require.NoError(t, err)

	got := generatedSource(response)
	assert.Contains(t, got, "type ListMembershipsShardParams struct {\n\tTenantID int64\n}")
	assert.Contains(t, got, "Tenant(tenantID int64) SK")
	body := generatedMethodBody(t, got, "groupedMeshStore[SK]", "ListMemberships")
	assert.Contains(t, body, "shardKey = q.store.resolver.Tenant(arg.TenantID)")
	assert.Contains(t, body, "return shard.Read().ListMemberships(ctx)")
}

func TestGeneratePrioritizesResultModelsForShardFieldLookup(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	textType := &plugin.Identifier{Name: "text"}
	users := &plugin.Identifier{Schema: "public", Name: "users"}
	audits := &plugin.Identifier{Schema: "public", Name: "audits"}
	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog: &plugin.Catalog{
			DefaultSchema: "public",
			Schemas: []*plugin.Schema{{
				Name: "public",
				Tables: []*plugin.Table{
					{
						Rel: users,
						Columns: []*plugin.Column{
							{Name: "id", Type: int8Type, NotNull: true, Table: users},
							{Name: "tenant_id", Type: int8Type, NotNull: true, Table: users},
						},
					},
					{
						Rel: audits,
						Columns: []*plugin.Column{{
							Name: "tenant_id", Type: textType, NotNull: true, Table: audits,
						}},
					},
				},
			}},
		},
		Queries: []*plugin.Query{{
			Name:     "ListUsers",
			Cmd:      ":many",
			Comments: []string{"kind: read", "shard: tenant(tenant_id)", "store: Users"},
			Params: []*plugin.Parameter{{
				Number: 1,
				Column: &plugin.Column{
					Name: "audit_filter", Type: textType, NotNull: true, Table: audits,
				},
			}},
			Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true, Table: users}},
		}},
		PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5"}`),
	})
	require.NoError(t, err)

	got := generatedSource(response)
	assert.Contains(t, got, "type ListUsersShardParams struct {\n\tAuditFilter string\n\tTenantID    int64\n}")
	assert.Contains(t, got, "Tenant(tenantID int64) SK")
}

func TestGenerateUsesSQLSourceTableWhenColumnProvenanceIsMissing(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	textType := &plugin.Identifier{Name: "text"}
	boolType := &plugin.Identifier{Schema: "pg_catalog", Name: "bool"}
	message := &plugin.Identifier{Schema: "public", Name: "message"}
	messageInbox := &plugin.Identifier{Schema: "public", Name: "message_inbox"}
	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog: &plugin.Catalog{
			DefaultSchema: "public",
			Schemas: []*plugin.Schema{{
				Name: "public",
				Tables: []*plugin.Table{
					{
						Rel: message,
						Columns: []*plugin.Column{
							{Name: "id", Type: int8Type, NotNull: true, Table: message},
							{Name: "user_id", Type: int8Type, NotNull: true, Table: message},
							{Name: "to_user_or_group_id", Type: int8Type, NotNull: true, Table: message},
							{Name: "in_group", Type: boolType, NotNull: true, Table: message},
						},
					},
					{
						Rel: messageInbox,
						Columns: []*plugin.Column{{
							Name:  "to_user_or_group_id",
							Type:  textType,
							Table: messageInbox,
						}},
					},
				},
			}},
		},
		Queries: []*plugin.Query{{
			Name:     "ListP2PMessageIDsByChat",
			Cmd:      ":many",
			Text:     `SELECT id::bigint FROM "message" WHERE user_id = $1`,
			Comments: []string{"kind: read", "shard: messageKey(user_id, to_user_or_group_id, in_group)", "store: QueryMessage"},
			Params: []*plugin.Parameter{{
				Number: 1,
				Column: &plugin.Column{Name: "user_id", Type: int8Type, NotNull: true},
			}},
			Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
		}},
		PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5"}`),
	})
	require.NoError(t, err)

	got := generatedSource(response)
	assert.Contains(
		t,
		got,
		"type ListP2PMessageIDsByChatShardParams struct {\n\tUserID          int64\n\tToUserOrGroupID int64\n\tInGroup         bool\n}",
	)
	assert.Contains(
		t,
		got,
		"MessageKey(userID int64, toUserOrGroupID int64, inGroup bool) SK",
	)
}

func TestGenerateRejectsMixedShardedAndUnshardedQueries(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	_, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog:  &plugin.Catalog{DefaultSchema: "public"},
		Queries: []*plugin.Query{
			{
				Name:     "GetUser",
				Cmd:      ":one",
				Comments: []string{"kind: read", "shard: tenant(tenant_id)", "store: Users"},
				Params: []*plugin.Parameter{{
					Number: 1,
					Column: &plugin.Column{Name: "tenant_id", Type: int8Type, NotNull: true},
				}},
				Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
			},
			{
				Name:     "ListUsers",
				Cmd:      ":many",
				Comments: []string{"kind: read", "store: Users"},
				Columns:  []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
			},
		},
		PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5"}`),
	})
	require.ErrorContains(t, err, "query ListUsers must declare shard metadata")
	require.ErrorContains(t, err, "move unsharded queries to another generated store")
}

func TestGenerateResolvesShardOperandsForIndividualParameters(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog:  &plugin.Catalog{DefaultSchema: "public"},
		Queries: []*plugin.Query{{
			Name:     "GetP2PMessage",
			Cmd:      ":one",
			Comments: []string{"kind: read", "shard: p2p(user_id, peer_id)", "store: Messages"},
			Params: []*plugin.Parameter{
				{Number: 1, Column: &plugin.Column{Name: "user_id", Type: int8Type, NotNull: true}},
				{Number: 2, Column: &plugin.Column{Name: "peer_id", Type: int8Type, NotNull: true}},
			},
			Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
		}},
		PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5","query_parameter_limit":2}`),
	})
	require.NoError(t, err)
	got := generatedSource(response)
	assert.Contains(t, got, "shardKey = q.store.resolver.P2P(userID, peerID)")
}

func TestGenerateIgnoreMirrorErrorOption(t *testing.T) {
	t.Parallel()

	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog:  &plugin.Catalog{DefaultSchema: "public"},
		Queries: []*plugin.Query{{
			Name:     "DeleteUser",
			Cmd:      ":exec",
			Comments: []string{"kind: write", "store: Users"},
		}},
		PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5","ignore_mirror_error":true}`),
	})
	require.NoError(t, err)

	got := generatedSource(response)
	mirrorBody := generatedMethodBody(t, got, "writeQueries", "mirror")
	assert.Contains(t, mirrorBody, "if err := fn(mirror); err != nil {\n\t\t\tcontinue")
	assert.NotContains(t, mirrorBody, "return err")
	assert.NotContains(t, got, `"database/sql"`)
	assert.NotContains(t, got, `"errors"`)
}

func TestGenerateEmptyQuerySetStillEmitsStoreConfiguration(t *testing.T) {
	t.Parallel()

	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings:      &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog:       &plugin.Catalog{DefaultSchema: "public"},
		PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5"}`),
	})
	require.NoError(t, err)
	assert.Contains(
		t,
		generatedSource(response),
		"func NewStore(ctx context.Context, topology Topology, options ...StoreOption) (Store, error)",
	)
}

func TestGenerateRejectsInvalidRoutingConfigurations(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	textType := &plugin.Identifier{Name: "text"}
	base := func(queries ...*plugin.Query) *plugin.GenerateRequest {
		return &plugin.GenerateRequest{
			Settings:      &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
			Catalog:       &plugin.Catalog{DefaultSchema: "public"},
			Queries:       queries,
			PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5"}`),
		}
	}
	groupedMany := func(columns ...*plugin.Column) *plugin.Query {
		return &plugin.Query{
			Name:     "ListByID",
			Cmd:      ":many",
			Comments: []string{"kind: read", "shard: entity(id)", "store: Entities"},
			Params: []*plugin.Parameter{{
				Number: 1,
				Column: &plugin.Column{
					Name:      "id",
					Type:      int8Type,
					NotNull:   true,
					IsArray:   true,
					ArrayDims: 1,
				},
			}},
			Columns: columns,
		}
	}

	tests := []struct {
		name    string
		request *plugin.GenerateRequest
		want    string
	}{
		{
			name: "unknown operand",
			request: base(&plugin.Query{
				Name: "GetMessage", Cmd: ":one", Comments: []string{"kind: read", "shard: inbox(missing_id)", "store: Messages"},
				Params:  []*plugin.Parameter{{Number: 1, Column: &plugin.Column{Name: "inbox_id", Type: int8Type, NotNull: true}}},
				Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
			}),
			want: "does not match a SQL parameter",
		},
		{
			name: "route rename must remain exported",
			request: func() *plugin.GenerateRequest {
				r := base(&plugin.Query{
					Name: "GetMessage", Cmd: ":one", Comments: []string{"kind: read", "shard: inbox(inbox_id)", "store: Messages"},
					Params:  []*plugin.Parameter{{Number: 1, Column: &plugin.Column{Name: "inbox_id", Type: int8Type, NotNull: true}}},
					Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
				})
				r.PluginOptions = []byte(`{"package":"db","sql_package":"pgx/v5","rename":{"inbox":"privateRoute"}}`)
				return r
			}(),
			want: "non-exported or invalid resolver method",
		},
		{
			name: "batch route",
			request: base(&plugin.Query{
				Name: "GetMessages", Cmd: ":batchmany", Comments: []string{"kind: read", "shard: inbox(inbox_id)", "store: Messages"},
				Params:  []*plugin.Parameter{{Number: 1, Column: &plugin.Column{Name: "inbox_id", Type: int8Type, NotNull: true}}},
				Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
			}),
			want: "grouped routing is supported only for :copyfrom",
		},
		{
			name:    "grouped many missing result key",
			request: base(groupedMany(&plugin.Column{Name: "other_id", Type: int8Type, NotNull: true})),
			want:    `result must expose lookup key "id"`,
		},
		{
			name:    "grouped many result key type mismatch",
			request: base(groupedMany(&plugin.Column{Name: "id", Type: textType, NotNull: true})),
			want:    `result key "id" has Go type string, want int64`,
		},
		{
			name: "grouped many ambiguous result key",
			request: base(groupedMany(
				&plugin.Column{Name: "id", Type: int8Type, NotNull: true},
				&plugin.Column{Name: "id", Type: int8Type, NotNull: true},
			)),
			want: `result exposes multiple fields for lookup key "id"`,
		},
		{
			name: "grouped many unnamed list parameter",
			request: base(&plugin.Query{
				Name:     "ListByID",
				Cmd:      ":many",
				Comments: []string{"kind: read", "shard: entity(id)", "store: Entities"},
				Params: []*plugin.Parameter{{
					Number: 1,
					Column: &plugin.Column{
						Type:        int8Type,
						NotNull:     true,
						IsSqlcSlice: true,
					},
				}},
				Columns: []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
			}),
			want: "list parameter must have a name",
		},
		{
			name: "conflicting route signatures",
			request: base(
				&plugin.Query{
					Name:     "GetByID",
					Cmd:      ":one",
					Comments: []string{"kind: read", "shard: entity(id)", "store: Entities"},
					Params:   []*plugin.Parameter{{Number: 1, Column: &plugin.Column{Name: "id", Type: int8Type, NotNull: true}}},
					Columns:  []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
				},
				&plugin.Query{
					Name:     "GetByName",
					Cmd:      ":one",
					Comments: []string{"kind: read", "shard: entity(name)", "store: Entities"},
					Params:   []*plugin.Parameter{{Number: 1, Column: &plugin.Column{Name: "name", Type: textType, NotNull: true}}},
					Columns:  []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
				},
			),
			want: "incompatible parameter types",
		},
		{
			name: "non pgx driver",
			request: func() *plugin.GenerateRequest {
				r := base(&plugin.Query{Name: "Delete", Cmd: ":exec", Comments: []string{"kind: write"}})
				r.PluginOptions = []byte(`{"package":"db","sql_package":"database/sql"}`)
				return r
			}(),
			want: "requires pgx/v5",
		},
		{
			name: "non postgres engine",
			request: func() *plugin.GenerateRequest {
				r := base(&plugin.Query{Name: "Delete", Cmd: ":exec", Comments: []string{"kind: write"}})
				r.Settings.Engine = "mysql"
				return r
			}(),
			want: "requires postgresql",
		},
		{
			name: "skip transaction support",
			request: func() *plugin.GenerateRequest {
				r := base(&plugin.Query{Name: "Delete", Cmd: ":exec", Comments: []string{"kind: write"}})
				r.PluginOptions = []byte(`{"package":"db","sql_package":"pgx/v5","skip_with_tx":true}`)
				return r
			}(),
			want: `unknown field "skip_with_tx"`,
		},
		{
			name: "negative parameter limit",
			request: func() *plugin.GenerateRequest {
				r := base(&plugin.Query{Name: "Delete", Cmd: ":exec", Comments: []string{"kind: write"}})
				r.PluginOptions = []byte(`{"package":"db","sql_package":"pgx/v5","query_parameter_limit":-1}`)
				return r
			}(),
			want: "must not be negative",
		},
		{
			name: "malformed options",
			request: func() *plugin.GenerateRequest {
				r := base(&plugin.Query{Name: "Delete", Cmd: ":exec", Comments: []string{"kind: write"}})
				r.PluginOptions = []byte(`{`)
				return r
			}(),
			want: "unmarshal plugin options",
		},
		{
			name: "invalid string override type",
			request: func() *plugin.GenerateRequest {
				r := base(&plugin.Query{Name: "Delete", Cmd: ":exec", Comments: []string{"kind: write"}})
				r.PluginOptions = []byte(`{
					"package":"db",
					"sql_package":"pgx/v5",
					"overrides":[{"db_type":"text","go_type":"LocalType"}]
				}`)
				return r
			}(),
			want: "is not a Go basic type",
		},
		{
			name: "override package without import",
			request: func() *plugin.GenerateRequest {
				r := base(&plugin.Query{Name: "Delete", Cmd: ":exec", Comments: []string{"kind: write"}})
				r.PluginOptions = []byte(`{
					"package":"db",
					"sql_package":"pgx/v5",
					"overrides":[{"db_type":"text","go_type":{"package":"custom","type":"Value"}}]
				}`)
				return r
			}(),
			want: "package requires an import path",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Generate(t.Context(), test.request)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestGenerateAllShardsQueries(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog:  &plugin.Catalog{DefaultSchema: "public"},
		Queries: []*plugin.Query{
			{
				Name:     "ListAll",
				Cmd:      ":many",
				Comments: []string{"kind: read", "shard: all()", "store: Reports"},
				Columns:  []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
			},
			{
				Name:     "DeleteAll",
				Cmd:      ":exec",
				Comments: []string{"kind: write", "shard: all()", "store: Reports"},
			},
			{
				Name:     "DeleteMatching",
				Cmd:      ":execrows",
				Comments: []string{"kind: write", "shard: all()", "store: Reports"},
			},
		},
		PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5"}`),
	})
	require.NoError(t, err)

	got := generatedSource(response)
	assert.Contains(t, got, "type ShardResolver[SK any] interface {\n}")
	assert.NotContains(t, got, "All() SK")
	assert.Contains(t, got, "func Sharded[SK any](")
	assert.Contains(t, got, "q.store.mesh.AllShards()")
	assert.Contains(t, got, "pgmesh.ErrCrossShardTransaction")
	assert.Contains(t, got, "querySpan.SetMultiRoute(")
	assert.Contains(t, got, "var group sync.WaitGroup")
	assert.Contains(t, got, `errors.Join(shardErrors...)`)
	assert.Contains(t, got, "ListAll(ctx context.Context, storeOptions ...QueryOption) ([]int64, error)")
	assert.Contains(t, got, "DeleteAll(ctx context.Context, storeOptions ...QueryOption) error")
	assert.Contains(t, got, "DeleteMatching(ctx context.Context, storeOptions ...QueryOption) (int64, error)")
	assert.Contains(t, got, `q.store.mesh.StartSpan(ctx, "Reports", "ListAll", pgmesh.QueryKindRead)`)
	assert.Contains(t, got, `q.store.mesh.StartSpan(ctx, "Reports", "DeleteAll", pgmesh.QueryKindWrite)`)
	assert.NotContains(t, got, "WriteMirrorCount()")
	assert.NotContains(t, got, "writeMirrorCount")
}

func TestGenerateRejectsUnsupportedAllShardsCommands(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	for _, command := range []string{
		":one",
		":execresult",
		":copyfrom",
		":batchexec",
		":batchone",
		":batchmany",
	} {
		t.Run(command, func(t *testing.T) {
			t.Parallel()
			query := &plugin.Query{
				Name:     "Unsupported",
				Cmd:      command,
				Comments: []string{"kind: write", "shard: all()", "store: Commands"},
			}
			if command == ":one" || command == ":batchone" || command == ":batchmany" {
				query.Columns = []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}}
			}
			if command == ":copyfrom" || strings.HasPrefix(command, ":batch") {
				query.Params = []*plugin.Parameter{{
					Number: 1,
					Column: &plugin.Column{Name: "id", Type: int8Type, NotNull: true},
				}}
			}
			_, err := Generate(t.Context(), &plugin.GenerateRequest{
				Settings:      &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
				Catalog:       &plugin.Catalog{DefaultSchema: "public"},
				Queries:       []*plugin.Query{query},
				PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5"}`),
			})
			require.ErrorContains(t, err, "unsupported command")
		})
	}

	_, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog:  &plugin.Catalog{DefaultSchema: "public"},
		Queries: []*plugin.Query{{
			Name:     "InvalidAll",
			Cmd:      ":many",
			Comments: []string{"kind: read", "shard: all(tenant_id)", "store: Reports"},
			Columns:  []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}},
		}},
		PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5"}`),
	})
	require.ErrorContains(t, err, `shard route "all" cannot declare operands`)
}

func TestGenerateGroupedCopyWithRoutingOnlyOperand(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	textType := &plugin.Identifier{Name: "text"}
	users := &plugin.Identifier{Schema: "public", Name: "users"}
	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog: &plugin.Catalog{
			DefaultSchema: "public",
			Schemas: []*plugin.Schema{{
				Name: "public",
				Tables: []*plugin.Table{{
					Rel: users,
					Columns: []*plugin.Column{
						{Name: "id", Type: int8Type, NotNull: true},
						{Name: "tenant_id", Type: int8Type, NotNull: true},
						{Name: "name", Type: textType, NotNull: true},
					},
				}},
			}},
		},
		Queries: []*plugin.Query{{
			Name:            "CopyUsers",
			Cmd:             ":copyfrom",
			Comments:        []string{"kind: write", "shard: tenant(tenant_id)", "store: Users"},
			InsertIntoTable: users,
			Params: []*plugin.Parameter{
				{Number: 1, Column: &plugin.Column{Name: "id", Type: int8Type, NotNull: true}},
				{Number: 2, Column: &plugin.Column{Name: "name", Type: textType, NotNull: true}},
			},
		}},
		PluginOptions: []byte(`{
			"package":"db",
			"sql_package":"pgx/v5",
			"emit_params_struct_pointers":true
		}`),
	})
	require.NoError(t, err)

	got := generatedSource(response)
	assert.Contains(
		t,
		got,
		"type CopyUsersShardParams struct {\n\tID       int64\n\tName     string\n\tTenantID int64\n}",
	)
	assert.Contains(
		t,
		got,
		"CopyUsers(ctx context.Context, arg []*CopyUsersShardParams, storeOptions ...QueryOption) (int64, error)",
	)
	assert.Contains(t, got, "shardKey = q.store.resolver.Tenant(item.TenantID)")
	assert.Contains(t, got, "shardGroup.args = append(shardGroup.args, item.sqlcParams())")
	assert.Contains(t, got, "target.CopyUsers(ctx, shardGroup.args)")
	assert.Contains(t, got, `q.store.mesh.StartSpan(ctx, "Users", "CopyUsers", pgmesh.QueryKindWrite)`)
	assert.NotContains(t, got, "WriteMirrorCount()")
	assert.NotContains(t, got, "writeMirrorCount")
}

func TestGenerateGroupedCopyWithScalarParameter(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog:  &plugin.Catalog{DefaultSchema: "public"},
		Queries: []*plugin.Query{{
			Name:     "CopyIDs",
			Cmd:      ":copyfrom",
			Comments: []string{"kind: write", "shard: entity(id)", "store: Entities"},
			Params: []*plugin.Parameter{{
				Number: 1,
				Column: &plugin.Column{Name: "id", Type: int8Type, NotNull: true},
			}},
		}},
		PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5"}`),
	})
	require.NoError(t, err)

	got := generatedSource(response)
	assert.Contains(t, got, "CopyIDs(ctx context.Context, id []int64, storeOptions ...QueryOption) (int64, error)")
	assert.Contains(t, got, "shardKey = q.store.resolver.Entity(item)")
	assert.Contains(t, got, "args  []int64")
	assert.Contains(t, got, "shardGroup.args = append(shardGroup.args, item)")
}

func TestGenerateSupportsAllNodeLevelCommands(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	tests := []struct {
		name      string
		command   string
		signature string
		body      []string
	}{
		{
			name:      "one",
			command:   ":one",
			signature: "Query0(ctx context.Context) (int64, error)",
			body:      []string{"q.main.Query0(ctx)", "return rv0, nil"},
		},
		{
			name:      "many",
			command:   ":many",
			signature: "Query1(ctx context.Context) ([]int64, error)",
			body:      []string{"q.main.Query1(ctx)", "return rv0, nil"},
		},
		{
			name:      "exec",
			command:   ":exec",
			signature: "Query2(ctx context.Context) error",
			body:      []string{"q.main.Query2(ctx)", "return nil"},
		},
		{
			name:      "exec rows",
			command:   ":execrows",
			signature: "Query3(ctx context.Context) (int64, error)",
			body:      []string{"q.main.Query3(ctx)", "return rv0, nil"},
		},
		{
			name:      "exec result",
			command:   ":execresult",
			signature: "Query4(ctx context.Context) (pgconn.CommandTag, error)",
			body:      []string{"q.main.Query4(ctx)", "return rv0, nil"},
		},
		{
			name:      "copy from",
			command:   ":copyfrom",
			signature: "Query5(ctx context.Context, id []int64) (int64, error)",
			body:      []string{"q.main.Query5(ctx, id)", "return rv0, nil"},
		},
		{
			name:      "batch exec",
			command:   ":batchexec",
			signature: "Query6(ctx context.Context, id []int64) *Query6BatchResults",
			body:      []string{"return q.main.Query6(ctx, id)"},
		},
		{
			name:      "batch one",
			command:   ":batchone",
			signature: "Query7(ctx context.Context, id []int64) *Query7BatchResults",
			body:      []string{"return q.main.Query7(ctx, id)"},
		},
		{
			name:      "batch many",
			command:   ":batchmany",
			signature: "Query8(ctx context.Context, id []int64) *Query8BatchResults",
			body:      []string{"return q.main.Query8(ctx, id)"},
		},
	}
	queries := make([]*plugin.Query, 0, len(tests))
	for index, test := range tests {
		query := &plugin.Query{
			Name:     fmt.Sprintf("Query%d", index),
			Cmd:      test.command,
			Comments: []string{"kind: read", "store: Commands"},
		}
		if test.command == ":one" || test.command == ":many" || test.command == ":batchone" || test.command == ":batchmany" {
			query.Columns = []*plugin.Column{{Name: "id", Type: int8Type, NotNull: true}}
		}
		if test.command == ":copyfrom" || strings.HasPrefix(test.command, ":batch") {
			query.Params = []*plugin.Parameter{{Number: 1, Column: &plugin.Column{Name: "id", Type: int8Type, NotNull: true}}}
		}
		queries = append(queries, query)
	}

	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings:      &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog:       &plugin.Catalog{DefaultSchema: "public"},
		Queries:       queries,
		PluginOptions: []byte(`{"package":"db","sql_package":"pgx/v5"}`),
	})
	require.NoError(t, err)
	got := generatedSource(response)
	for index, test := range tests {
		assert.Contains(t, got, test.signature, "command %s", test.command)
		body := generatedMethodBody(t, got, "readQueries", fmt.Sprintf("Query%d", index))
		for _, want := range test.body {
			assert.Contains(t, body, want, "command %s body", test.command)
		}
	}
}

func TestGenerateQualifiesSqlcTypesForSeparatePackage(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	tokenType := &plugin.Identifier{Schema: "public", Name: "token"}
	users := &plugin.Identifier{Schema: "public", Name: "users"}
	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "store"}},
		Catalog: &plugin.Catalog{
			DefaultSchema: "public",
			Schemas: []*plugin.Schema{{
				Name: "public",
				Tables: []*plugin.Table{{
					Rel: users,
					Columns: []*plugin.Column{
						{Name: "id", Type: int8Type, NotNull: true},
						{Name: "tenant_id", Type: int8Type, NotNull: true},
						{Name: "token", Type: tokenType, NotNull: true},
					},
				}},
			}},
		},
		Queries: []*plugin.Query{{
			Name:     "GetUser",
			Cmd:      ":one",
			Comments: []string{"kind: read", "shard: user(token)", "store: Users"},
			Params: []*plugin.Parameter{
				{Number: 1, Column: &plugin.Column{Name: "id", Type: int8Type, NotNull: true}},
				{Number: 2, Column: &plugin.Column{Name: "tenant_id", Type: int8Type, NotNull: true}},
				{Number: 3, Column: &plugin.Column{Name: "token", Type: tokenType, NotNull: true}},
			},
			Columns: []*plugin.Column{
				{Name: "id", Type: int8Type, NotNull: true, Table: users},
				{Name: "tenant_id", Type: int8Type, NotNull: true, Table: users},
				{Name: "token", Type: tokenType, NotNull: true, Table: users},
			},
		}},
		PluginOptions: []byte(`{
			"package":"store",
			"output_file_name":"generated_store.go",
			"internal_import_path":"example.test/project/internal/db",
			"internal_import_alias":"db",
			"runtime_import_path":"example.test/project/pgmesh",
			"sql_package":"pgx/v5",
			"query_parameter_limit":1,
			"emit_params_struct_pointers":true,
			"emit_result_struct_pointers":true,
			"overrides":[{"db_type":"public.token","go_type":{"type":"Token"}}]
		}`),
	})
	require.NoError(t, err)
	require.Equal(
		t,
		[]string{
			"generated_store_interfaces.go",
			"generated_store_read.go",
			"generated_store_write.go",
			"generated_store.go",
			"generated_store_users.go",
			"generated_store_sharded.go",
		},
		generatedFileNames(response),
	)

	got := generatedSource(response)
	checks := []string{
		`db "example.test/project/internal/db"`,
		`pgmesh "example.test/project/pgmesh"`,
		"GetUser(ctx context.Context, arg *db.GetUserParams) (*db.User, error)",
		"GetUser(ctx context.Context, arg *db.GetUserParams, storeOptions ...QueryOption) (*db.User, error)",
		"User(token db.Token) SK",
		"main *db.Queries",
		"func newReadQueries(q *db.Queries) *readQueries",
		"var _ db.Querier = (*queryStore)(nil)",
		"queries := db.New(database)",
	}
	for _, check := range checks {
		assert.Contains(t, got, check)
	}
}

func TestGenerateExportsSQLCTypesForSeparatePackage(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	statusType := &plugin.Identifier{Schema: "public", Name: "status"}
	users := &plugin.Identifier{Schema: "public", Name: "users"}
	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "store"}},
		Catalog: &plugin.Catalog{
			DefaultSchema: "public",
			Schemas: []*plugin.Schema{{
				Name:  "public",
				Enums: []*plugin.Enum{{Name: "status", Vals: []string{"active", "disabled"}}},
				Tables: []*plugin.Table{{
					Rel: users,
					Columns: []*plugin.Column{
						{Name: "id", Type: int8Type, NotNull: true},
						{Name: "tenant_id", Type: int8Type, NotNull: true},
						{Name: "status", Type: statusType},
					},
				}},
			}},
		},
		Queries: []*plugin.Query{
			{
				Name:     "CreateUser",
				Cmd:      ":one",
				Comments: []string{"kind: write", "store: Users"},
				Params: []*plugin.Parameter{
					{Number: 1, Column: &plugin.Column{Name: "id", Type: int8Type, NotNull: true}},
					{Number: 2, Column: &plugin.Column{Name: "tenant_id", Type: int8Type, NotNull: true}},
				},
				Columns: []*plugin.Column{
					{Name: "id", Type: int8Type, NotNull: true, Table: users},
					{Name: "tenant_id", Type: int8Type, NotNull: true, Table: users},
					{Name: "status", Type: statusType, Table: users},
				},
			},
			{
				Name:     "FindUser",
				Cmd:      ":one",
				Comments: []string{"kind: read", "store: Users"},
				Params: []*plugin.Parameter{
					{Number: 1, Column: &plugin.Column{Name: "id", Type: int8Type, NotNull: true}},
					{Number: 2, Column: &plugin.Column{Name: "tenant_id", Type: int8Type, NotNull: true}},
				},
				Columns: []*plugin.Column{
					{Name: "user_id", Type: int8Type, NotNull: true},
					{Name: "user_status", Type: statusType},
				},
			},
		},
		PluginOptions: []byte(`{
			"package":"store",
			"internal_import_path":"example.test/project/internal/db",
			"internal_import_alias":"db",
			"export_sqlc_types":true,
			"sql_package":"pgx/v5",
			"query_parameter_limit":1
		}`),
	})
	require.NoError(t, err)

	interfaces := generatedFileContents(t, response, "store_querier_interfaces.go")
	checks := []string{
		"type CreateUserParams = db.CreateUserParams",
		"type FindUserParams = db.FindUserParams",
		"type FindUserRow = db.FindUserRow",
		"type NullStatus = db.NullStatus",
		"type Status = db.Status",
		"type User = db.User",
	}
	for _, check := range checks {
		assert.Contains(t, interfaces, check)
	}
	assert.NotContains(t, interfaces, "type Queries =")
	assert.NotContains(t, interfaces, "type Querier =")

	usersFile := generatedFileContents(t, response, "store_querier_users.go")
	assert.Contains(
		t,
		usersFile,
		"CreateUser(ctx context.Context, arg CreateUserParams, storeOptions ...QueryOption) (User, error)",
	)
	assert.Contains(
		t,
		usersFile,
		"FindUser(ctx context.Context, arg FindUserParams, storeOptions ...QueryOption) (FindUserRow, error)",
	)
}

func TestGenerateRejectsExportedSQLCTypeNameConflicts(t *testing.T) {
	t.Parallel()

	storeType := &plugin.Identifier{Schema: "public", Name: "store"}
	_, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "store"}},
		Catalog: &plugin.Catalog{
			DefaultSchema: "public",
			Schemas: []*plugin.Schema{{
				Name:  "public",
				Enums: []*plugin.Enum{{Name: "store", Vals: []string{"primary"}}},
			}},
		},
		Queries: []*plugin.Query{{
			Name:     "GetStore",
			Cmd:      ":one",
			Comments: []string{"kind: read", "store: Stores"},
			Columns:  []*plugin.Column{{Name: "store", Type: storeType, NotNull: true}},
		}},
		PluginOptions: []byte(`{
			"package":"store",
			"internal_import_path":"example.test/project/internal/db",
			"export_sqlc_types":true,
			"sql_package":"pgx/v5"
		}`),
	})

	require.ErrorContains(
		t,
		err,
		"export_sqlc_types cannot alias sqlc type Store because it conflicts with store interface",
	)
}

func TestGenerateAppliesRenameAndNullableOptions(t *testing.T) {
	t.Parallel()

	int8Type := &plugin.Identifier{Schema: "pg_catalog", Name: "int8"}
	textType := &plugin.Identifier{Name: "text"}
	response, err := Generate(t.Context(), &plugin.GenerateRequest{
		Settings: &plugin.Settings{Engine: "postgresql", Codegen: &plugin.Codegen{Out: "db"}},
		Catalog:  &plugin.Catalog{DefaultSchema: "public"},
		Queries: []*plugin.Query{{
			Name:     "FindUser",
			Cmd:      ":one",
			Comments: []string{"kind: read", "shard: tenant(tenant_id)", "store: Users"},
			Params: []*plugin.Parameter{
				{Number: 1, Column: &plugin.Column{Name: "tenant_id", Type: int8Type, NotNull: true}},
				{Number: 2, Column: &plugin.Column{Name: "display_name", Type: textType}},
			},
			Columns: []*plugin.Column{{Name: "display_name", Type: textType}},
		}},
		PluginOptions: []byte(`{
			"package":"db",
			"sql_package":"pgx/v5",
			"query_parameter_limit":1,
			"emit_params_struct_pointers":true,
			"emit_pointers_for_null_types":true,
			"rename":{"tenant":"ResolveTenant","tenant_id":"AccountID","display_name":"Label"}
		}`),
	})
	require.NoError(t, err)

	got := generatedSource(response)
	checks := []string{
		"FindUser(ctx context.Context, arg *FindUserParams) (*string, error)",
		"ResolveTenant(accountID int64) SK",
		"shardKey = q.store.resolver.ResolveTenant(arg.AccountID)",
	}
	for _, check := range checks {
		assert.Contains(t, got, check)
	}
}
