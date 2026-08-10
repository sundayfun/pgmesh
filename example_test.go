package pgmesh_test

import (
	"context"
	"fmt"

	"github.com/sundayfun/pgmesh"
)

type exampleReader struct {
	node string
}

type exampleWriter struct {
	node    string
	mirrors []*exampleWriter
}

func (q *exampleWriter) WithMirrors(mirrors ...*exampleWriter) *exampleWriter {
	return &exampleWriter{
		node:    q.node,
		mirrors: append(append([]*exampleWriter(nil), q.mirrors...), mirrors...),
	}
}

func (q *exampleWriter) Put(value string) []string {
	writes := []string{q.node + ":" + value}
	for _, mirror := range q.mirrors {
		writes = append(writes, mirror.node+":"+value)
	}
	return writes
}

func exampleNode(name string) pgmesh.Node[*exampleReader, *exampleWriter] {
	return pgmesh.NewNode(
		&exampleReader{node: name},
		&exampleWriter{node: name, mirrors: nil},
	)
}

func ExampleNewMeshBuilder() {
	shard0 := pgmesh.NewReplicaSet(
		"shard-0",
		exampleNode("shard0-primary"),
		[]pgmesh.Node[*exampleReader, *exampleWriter]{
			exampleNode("shard0-replica0"),
			exampleNode("shard0-replica1"),
		},
	)
	shard1 := pgmesh.NewReplicaSet("shard-1", exampleNode("shard1-primary"), nil)

	mesh, err := pgmesh.NewMeshBuilder[*exampleReader, *exampleWriter, uint64](2).
		WithShardHasher(pgmesh.NewModuloShardHasher[uint64](2)).
		MapVirtualShard(0, shard0).
		MapVirtualShard(1, shard1).
		Build()
	if err != nil {
		panic(err)
	}

	routed, err := mesh.Resolve(2)
	if err != nil {
		panic(err)
	}
	fmt.Println(routed.ReplicaSetName(), routed.VirtualShardIndex())
	fmt.Println(routed.ReadRoute().Target.node)
	fmt.Println(routed.ReadRoute().Target.node)
	fmt.Println(routed.WriteRoute().Target.Put("message"))

	fallback, err := mesh.Resolve(3)
	if err != nil {
		panic(err)
	}
	fmt.Println(fallback.ReadRoute().Target.node)

	for _, replicaSet := range mesh.ReplicaSets() {
		fmt.Println(replicaSet.Name())
	}

	// Output:
	// shard-0 0
	// shard0-replica0
	// shard0-replica1
	// [shard0-primary:message]
	// shard1-primary
	// shard-0
	// shard-1
}

func ExampleOpenMesh() {
	mesh, err := pgmesh.OpenMesh(
		context.Background(),
		4,
		func(_ context.Context, dsn string) (
			pgmesh.Node[*exampleReader, *exampleWriter],
			error,
		) {
			return exampleNode(dsn), nil
		},
		pgmesh.NewModuloShardHasher[uint64](4),
		pgmesh.WithReplicaSet("east", "east-primary", "east-replica"),
		pgmesh.WithReplicaSet("west", "west-primary"),
		pgmesh.WithReplicaSet("archive", "archive-primary"),
		pgmesh.WithVirtualShardMapping("east", []uint64{0, 2}, "archive"),
		pgmesh.WithVirtualShardMapping("west", []uint64{1, 3}),
	)
	if err != nil {
		panic(err)
	}

	routed, err := mesh.Resolve(6)
	if err != nil {
		panic(err)
	}
	fmt.Println(routed.ReplicaSetName(), routed.VirtualShardIndex())
	fmt.Println(routed.ReadRoute().Target.node)
	fmt.Println(routed.WriteRoute().Target.Put("event"))

	// Output:
	// east 2
	// east-replica
	// [east-primary:event archive-primary:event]
}
