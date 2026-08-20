package samepackage

import (
	"context"
	"fmt"

	"github.com/sundayfun/pgmesh"
)

func ExampleStore() {
	log := &callLog{}
	queries, err := NewStore(
		context.Background(),
		Sharded(
			1,
			pgmesh.NewConstantShardHasher[uint64](0),
			tenantResolver{},
			WithReplicaSet(
				"main",
				&fakeDB{name: "primary", log: log},
				&fakeDB{name: "replica", log: log},
			),
			WithReplicaSet("mirror", &fakeDB{name: "mirror", log: log}),
			WithVirtualShardMapping("main", []uint64{0}, "mirror"),
		),
	)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	users := queries.Users()
	if _, err := users.GetUser(ctx, &GetUserT{TenantID: 10, ID: 20}); err != nil {
		panic(err)
	}
	if _, err := users.GetUser(ctx, &GetUserT{TenantID: 10, ID: 20}, ReadFromPrimary()); err != nil {
		panic(err)
	}
	if _, err := users.CreateUser(ctx, &CreateUserT{TenantID: 10, ID: 20, Name: "user"}); err != nil {
		panic(err)
	}

	fmt.Println(log.snapshot())

	// Output:
	// [replica primary primary mirror]
}
