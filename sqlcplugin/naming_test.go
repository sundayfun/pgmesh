package sqlcplugin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStructName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  string
		rename map[string]string
		want   string
	}{
		{name: "snake case", input: "user_profile", want: "UserProfile"},
		{name: "initialism", input: "user_id", want: "UserID"},
		{name: "punctuation", input: "audit-event", want: "AuditEvent"},
		{name: "leading number", input: "2fa_token", want: "_2faToken"},
		{name: "rename", input: "users", rename: map[string]string{"users": "People"}, want: "People"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, structName(test.input, &options{Rename: test.rename}))
		})
	}
}

func TestSingular(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "ids", want: "id"},
		{name: "user_ids", want: "user_id"},
		{name: "categories", want: "category"},
		{name: "statuses", want: "status"},
		{name: "metadata", want: "metadata"},
	}

	for _, test := range tests {
		assert.Equal(t, test.want, singular(test.name))
	}
}

func TestRouteMethodName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "tenant", want: "Tenant"},
		{name: "messageKey", want: "MessageKey"},
		{name: "message_key", want: "MessageKey"},
		{name: "user_id", want: "UserID"},
		{name: "p2p", want: "P2P"},
	}

	for _, test := range tests {
		assert.Equal(t, test.want, routeMethodName(test.name, &options{}))
	}
}

func TestPackageNameForImport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		importPath string
		want       string
	}{
		{name: "standard library", importPath: "encoding/json", want: "json"},
		{name: "versioned module", importPath: "github.com/jackc/pgx/v5", want: "pgx"},
		{name: "go prefix and suffix", importPath: "example.test/go-domain-go", want: "domain"},
		{name: "invalid package runes", importPath: "example.test/my-types", want: "my_types"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, packageNameForImport(test.importPath))
		})
	}
}

func TestSnakeCaseIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "Users", want: "users"},
		{name: "QueryMessage", want: "query_message"},
		{name: "IAM", want: "iam"},
		{name: "OAuth2API", want: "o_auth2_api"},
	}
	for _, test := range tests {
		assert.Equal(t, test.want, snakeCaseIdentifier(test.name))
	}
}
