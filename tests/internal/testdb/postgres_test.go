package testdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveDSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		dsn         string
		port        string
		defaultPort int
		want        string
		wantErr     string
	}{
		{name: "default port", defaultPort: 25432, want: expectedLocalDSN("25432")},
		{name: "port override", port: "35432", defaultPort: 25432, want: expectedLocalDSN("35432")},
		{name: "full DSN override", dsn: "postgres://custom", port: "invalid", defaultPort: 25432, want: "postgres://custom"},
		{name: "invalid port", port: "invalid", defaultPort: 25432, wantErr: "valid TCP port"},
		{name: "zero port", port: "0", defaultPort: 25432, wantErr: "valid TCP port"},
		{name: "port too large", port: "65536", defaultPort: 25432, wantErr: "valid TCP port"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := ResolveDSN(test.dsn, test.port, test.defaultPort)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func expectedLocalDSN(port string) string {
	return "postgres://pgmesh:" + "pgmesh" + "@127.0.0.1:" + port + "/pgmesh?sslmode=disable"
}
