//go:build integration

package typemapping

import (
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh/tests/internal/storetest"
	fixture "github.com/sundayfun/pgmesh/tests/same_package"
)

func TestPostgresScansNullableNetworkAndRangeTypes(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries := harness.NewShardedStore(t)

	_, err := harness.Pool("shard0-primary").Exec(
		t.Context(),
		`INSERT INTO analyses (id, tenant_id, summary, state, source, active_window)
		 VALUES
		 (360, 2, 'ready', 'complete', '192.0.2.10', '[2026-01-02 03:04:05+00,2026-01-03 03:04:05+00)'),
		 (361, 2, NULL, NULL, '2001:db8::10', '[2026-02-02 03:04:05+00,2026-02-03 03:04:05+00)')`,
	)
	require.NoError(t, err)

	populated, err := queries.Analyses().GetAnalysis(
		t.Context(),
		&fixture.GetAnalysisT{TenantKey: storetest.TenantKey(2), ID: 360},
		fixture.ReadFromPrimary(),
	)
	require.NoError(t, err)
	require.NotNil(t, populated.Description)
	assert.Equal(t, "ready", *populated.Description)
	assert.True(t, populated.State.Valid)
	assert.Equal(t, fixture.AnalysisStateComplete, populated.State.AnalysisState)
	assert.Equal(t, netip.MustParseAddr("192.0.2.10"), populated.Source)
	assert.True(t, populated.ActiveWindow.Valid)
	assert.Equal(t, "2026-01-02T03:04:05Z", populated.ActiveWindow.Lower.Time.UTC().Format(time.RFC3339))
	assert.Equal(t, "2026-01-03T03:04:05Z", populated.ActiveWindow.Upper.Time.UTC().Format(time.RFC3339))

	nullable, err := queries.Analyses().GetAnalysis(
		t.Context(),
		&fixture.GetAnalysisT{TenantKey: storetest.TenantKey(2), ID: 361},
		fixture.ReadFromPrimary(),
	)
	require.NoError(t, err)
	assert.Nil(t, nullable.Description)
	assert.False(t, nullable.State.Valid)
	assert.Equal(t, netip.MustParseAddr("2001:db8::10"), nullable.Source)
	assert.True(t, nullable.ActiveWindow.Valid)
}
