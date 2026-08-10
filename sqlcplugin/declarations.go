package sqlcplugin

const (
	constructorOption          = "constructor"
	generatedOptionDeclaration = "generated option constructor"
	sqlcDeclaration            = "sqlc declaration"
)

func generatedDeclarations(opts *options) map[string]string {
	declarations := fixedGeneratedDeclarations(opts.InternalImportPath == "")
	declarations[opts.ConstructorName] = constructorOption
	declarations[opts.ResolverInterfaceName] = "resolver interface"
	declarations[opts.ShardedConstructor] = "sharded constructor"
	declarations[opts.StoreInterfaceName] = "store interface"
	return declarations
}

func fixedGeneratedDeclarations(includeSQLC bool) map[string]string {
	declarations := map[string]string{
		"QueryOption":             "generated query option",
		"ReadFromPrimary":         generatedOptionDeclaration,
		"ShardedOption":           "generated sharded option",
		"Singleton":               "generated singleton constructor",
		"SingletonOption":         "generated singleton option",
		"StoreOption":             "generated store option",
		"Topology":                "generated topology",
		"WithDatabaseName":        generatedOptionDeclaration,
		"WithLogger":              generatedOptionDeclaration,
		"WithMeterProvider":       generatedOptionDeclaration,
		"WithReadReplicas":        generatedOptionDeclaration,
		"WithReplicaSet":          generatedOptionDeclaration,
		"WithTracerProvider":      generatedOptionDeclaration,
		"WithTx":                  generatedOptionDeclaration,
		"WithVirtualShardMapping": generatedOptionDeclaration,
		"WithWriteMirrors":        generatedOptionDeclaration,
	}
	if includeSQLC {
		declarations["DBTX"] = sqlcDeclaration
		declarations[defaultTargetNew] = sqlcDeclaration
		declarations[defaultTargetType] = sqlcDeclaration
		declarations["Querier"] = sqlcDeclaration
	}
	return declarations
}
