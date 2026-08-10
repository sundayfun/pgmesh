package pgmesh_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh"
)

func TestRouteMetadataDistinguishesPhysicalAndResolvedRoutes(t *testing.T) {
	t.Parallel()

	replicaSet := pgmesh.NewReplicaSet(
		"main",
		node("primary"),
		[]pgmesh.Node[string, *fakeWriter]{node("replica")},
	)
	mesh, err := pgmesh.NewMeshBuilder[string, *fakeWriter, uint64](3).
		WithShardHasher(pgmesh.NewConstantShardHasher[uint64](2)).
		MapVirtualShard(0, replicaSet).
		MapVirtualShard(1, replicaSet).
		MapVirtualShard(2, replicaSet).
		Build()
	require.NoError(t, err)
	resolved, err := mesh.Resolve(42)
	require.NoError(t, err)

	tests := []struct {
		name         string
		route        func() (string, pgmesh.RouteMetadata)
		wantTarget   string
		wantMetadata pgmesh.RouteMetadata
	}{
		{
			name: "physical read route",
			route: func() (string, pgmesh.RouteMetadata) {
				route := replicaSet.ReadRoute()
				return route.Target, route.Metadata()
			},
			wantTarget: "replica-read",
			wantMetadata: pgmesh.RouteMetadata{
				VirtualShardIndex: 0,
				HasVirtualShard:   false,
				ReplicaSetName:    "main",
				NodeName:          "replica-0",
				NodeRole:          pgmesh.NodeRoleReadReplica,
			},
		},
		{
			name: "physical write route",
			route: func() (string, pgmesh.RouteMetadata) {
				route := replicaSet.WriteRoute()
				return route.Target.name, route.Metadata()
			},
			wantTarget: "primary-write",
			wantMetadata: pgmesh.RouteMetadata{
				VirtualShardIndex: 0,
				HasVirtualShard:   false,
				ReplicaSetName:    "main",
				NodeName:          "primary",
				NodeRole:          pgmesh.NodeRolePrimary,
			},
		},
		{
			name: "resolved read route",
			route: func() (string, pgmesh.RouteMetadata) {
				route := resolved.ReadRoute()
				return route.Target, route.Metadata()
			},
			wantTarget: "replica-read",
			wantMetadata: pgmesh.RouteMetadata{
				VirtualShardIndex: 2,
				HasVirtualShard:   true,
				ReplicaSetName:    "main",
				NodeName:          "replica-0",
				NodeRole:          pgmesh.NodeRoleReadReplica,
			},
		},
		{
			name: "resolved write route",
			route: func() (string, pgmesh.RouteMetadata) {
				route := resolved.WriteRoute()
				return route.Target.name, route.Metadata()
			},
			wantTarget: "primary-write",
			wantMetadata: pgmesh.RouteMetadata{
				VirtualShardIndex: 2,
				HasVirtualShard:   true,
				ReplicaSetName:    "main",
				NodeName:          "primary",
				NodeRole:          pgmesh.NodeRolePrimary,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			target, metadata := test.route()
			assert.Equal(t, test.wantTarget, target)
			assert.Equal(t, test.wantMetadata, metadata)

			withoutVirtualShard := metadata.WithoutVirtualShard()
			assert.Zero(t, withoutVirtualShard.VirtualShardIndex)
			assert.False(t, withoutVirtualShard.HasVirtualShard)
			assert.Equal(t, metadata.ReplicaSetName, withoutVirtualShard.ReplicaSetName)
			assert.Equal(t, metadata.NodeName, withoutVirtualShard.NodeName)
			assert.Equal(t, metadata.NodeRole, withoutVirtualShard.NodeRole)
		})
	}
}
