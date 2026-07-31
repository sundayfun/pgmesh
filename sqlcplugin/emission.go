package sqlcplugin

import (
	"bytes"
	"fmt"
	"go/format"
	"path/filepath"
	"strings"

	"github.com/sqlc-dev/plugin-sdk-go/plugin"
)

func generateWrapper(
	opts *options,
	queries []generatedQuery,
	imports *importSet,
	catalog *plugin.Catalog,
) ([]*plugin.File, error) {
	routes, err := collectShardRoutes(queries)
	if err != nil {
		return nil, err
	}
	sharded := hasShardOperations(queries)
	groups, err := collectStoreGroups(queries, opts)
	if err != nil {
		return nil, err
	}
	groupFileNames := storeGroupOutputFileNames(opts.OutputFileName, groups)
	aliases, err := collectSQLCTypeAliases(opts, queries, catalog)
	if err != nil {
		return nil, err
	}
	if err := validateSQLCTypeAliases(opts, queries, groups, aliases); err != nil {
		return nil, err
	}
	if sharded {
		for _, query := range queries {
			if query.shardMode == shardModeNone {
				return nil, fmt.Errorf(
					"query %s must declare shard metadata because this store contains sharded queries; move unsharded queries to another generated store",
					query.methodName,
				)
			}
		}
	}
	if opts.InternalImportPath != "" {
		imports.addNamed(importAlias(opts.InternalImportAlias), opts.InternalImportPath)
	}
	imports.addNamed(defaultRuntimeAlias, opts.RuntimeImportPath)
	imports.add(defaultContext)
	imports.add(defaultErrorsPackage)
	imports.add(defaultFMT)
	imports.add(defaultReflect)
	imports.add(defaultSlog)
	imports.add(defaultSync)
	imports.add(defaultMetric)
	imports.add(defaultTrace)
	imports.add(defaultPGXPackage)
	if !opts.IgnoreMirrorError {
		imports.add(defaultDatabaseSQL)
		imports.add(defaultErrorsPackage)
	}

	files := make([]*plugin.File, 0, 5+len(groups))
	appendFile := func(name string, writeBody func(*bytes.Buffer)) error {
		content, generateErr := generateFile(opts, imports, writeBody)
		if generateErr != nil {
			return generateErr
		}
		files = append(files, &plugin.File{Name: name, Contents: content})
		return nil
	}

	if err := appendFile(derivedOutputFileName(opts.OutputFileName, "interfaces"), func(out *bytes.Buffer) {
		writeSQLCTypeAliases(out, opts, aliases)
		writeQueryInterfaces(out, opts, queries, groups)
		if sharded {
			writeShardKeyTypes(out, opts, routes)
			writeShardResolverInterface(out, opts, routes)
		}
	}); err != nil {
		return nil, err
	}
	if err := appendFile(derivedOutputFileName(opts.OutputFileName, "read"), func(out *bytes.Buffer) {
		writeReadQueries(out, opts, queries)
	}); err != nil {
		return nil, err
	}
	if err := appendFile(derivedOutputFileName(opts.OutputFileName, "write"), func(out *bytes.Buffer) {
		writeWriteQueries(out, opts, queries)
	}); err != nil {
		return nil, err
	}
	if err := appendFile(opts.OutputFileName, func(out *bytes.Buffer) {
		writeQueryOptions(out)
		writeQueryStore(out, opts)
		writeNodeConstructor(out, opts)
		writeStoreConfiguration(out, opts, queries, groups)
	}); err != nil {
		return nil, err
	}
	for index := range groups {
		group := &groups[index]
		if err := appendFile(groupFileNames[index], func(out *bytes.Buffer) {
			writeStoreGroup(out, opts, group)
		}); err != nil {
			return nil, err
		}
	}
	if err := appendFile(derivedOutputFileName(opts.OutputFileName, "sharded"), func(out *bytes.Buffer) {
		if sharded {
			writeShardedStore(out, opts, groups)
		}
	}); err != nil {
		return nil, err
	}

	return files, nil
}

func generateFile(opts *options, imports *importSet, writeBody func(*bytes.Buffer)) ([]byte, error) {
	var body bytes.Buffer
	writeBody(&body)

	var out bytes.Buffer
	fmt.Fprintf(&out, "%s\n\n", generatedHeader)
	fmt.Fprintf(&out, "package %s\n\n", opts.PackageName)
	writeImports(&out, usedImports(imports.sorted(), body.String()))
	out.Write(body.Bytes())

	formatted, err := format.Source(out.Bytes())
	if err != nil {
		return nil, fmt.Errorf("format generated wrapper: %w", err)
	}
	return formatted, nil
}

func derivedOutputFileName(outputFileName, section string) string {
	extension := filepath.Ext(outputFileName)
	stem := strings.TrimSuffix(outputFileName, extension)
	return stem + "_" + section + extension
}

func storeGroupOutputFileNames(outputFileName string, groups []storeGroup) []string {
	fixedSections := []string{"interfaces", "read", "write", "sharded"}
	used := map[string]struct{}{
		strings.ToLower(outputFileName): {},
	}
	for _, section := range fixedSections {
		name := derivedOutputFileName(outputFileName, section)
		used[strings.ToLower(name)] = struct{}{}
	}

	names := make([]string, len(groups))
	for index := range groups {
		group := &groups[index]
		section := snakeCaseIdentifier(group.name)
		for {
			name := derivedOutputFileName(outputFileName, section)
			key := strings.ToLower(name)
			if _, exists := used[key]; !exists {
				used[key] = struct{}{}
				names[index] = name
				break
			}
			section = "group_" + section
		}
	}
	return names
}

func usedImports(imports []importSpec, body string) []importSpec {
	used := make([]importSpec, 0, len(imports))
	for _, imp := range imports {
		qualifier := imp.name
		if qualifier == "" {
			qualifier = packageNameForImport(imp.path)
		}
		if strings.Contains(body, qualifier+".") {
			used = append(used, imp)
		}
	}
	return used
}

func writeQueryInterfaces(
	out *bytes.Buffer,
	opts *options,
	queries []generatedQuery,
	groups []storeGroup,
) {
	writeSplitInterface(out, defaultReadInterface, queries, queryKindRead)
	writeSplitInterface(out, defaultWriteInterface, queries, queryKindWrite)
	fmt.Fprintf(out, "// %s is the topology-independent generated query API.\n", opts.StoreInterfaceName)
	fmt.Fprintf(out, "type %s interface {\n", opts.StoreInterfaceName)
	for _, group := range groups {
		fmt.Fprintf(out, "\t// %s returns the %s query group.\n", group.name, group.name)
		fmt.Fprintf(out, "\t%s() %s\n", group.name, group.name)
	}
	out.WriteString("}\n\n")

	fmt.Fprintf(out, "var _ %s = (*%s)(nil)\n", defaultReadInterface, defaultReadType)
	fmt.Fprintf(out, "var _ %s = (*%s)(nil)\n", defaultWriteInterface, defaultWriteType)
	fmt.Fprintf(out, "var _ %s = (*%s)(nil)\n\n", targetName(opts, "Querier"), defaultStoreType)
}

func writeStoreGroup(out *bytes.Buffer, opts *options, group *storeGroup) {
	writeStoreParamAliases(out, group.queries)
	writeShardArgWrappers(out, opts, group.queries)
	writeCopyBatchTypesAndOptions(out, group.queries)
	writeStoreGroupInterfaces(out, opts, group)
	writeStoreTelemetryWrapper(out, opts, group)

	factoryName := storeFactoryOptionName(group.name)
	fmt.Fprintf(out, "// %s configures an optional wrapper for the %s query group.\n", factoryName, group.name)
	out.WriteString("// A nil factory leaves the generated query group unwrapped.\n")
	fmt.Fprintf(
		out,
		"func %s(create%s func(%s) %s) StoreOption {\n",
		factoryName,
		group.name,
		group.name,
		group.name,
	)
	fmt.Fprintf(
		out,
		"\treturn func(options *storeOptions) { options.factories.%s = create%s }\n",
		group.name,
		group.name,
	)
	out.WriteString("}\n\n")

	fmt.Fprintf(out, "var _ %s = (*%s[uint8])(nil)\n", group.name, defaultGroupType)
	fmt.Fprintf(out, "var _ %s = (*%s[uint8])(nil)\n\n", group.name, storeTelemetryTypeName(group.name))
	fmt.Fprintf(out, "// %s returns the %s query group.\n", group.name, group.name)
	fmt.Fprintf(
		out,
		"func (%s *%s[SK]) %s() %s {\n",
		defaultReceiverName,
		defaultMeshStoreType,
		group.name,
		group.name,
	)
	fmt.Fprintf(out, "\treturn %s.groups.%s\n", defaultReceiverName, group.name)
	out.WriteString("}\n\n")

	for index := range group.queries {
		query := &group.queries[index]
		writeMeshStoreQueryMethod(out, opts, query)
		if query.command == cmdCopyFrom {
			writeAsyncCopyQueryMethods(out, query)
		}
	}
}

func writeStoreTelemetryWrapper(out *bytes.Buffer, opts *options, group *storeGroup) {
	wrapperType := storeTelemetryTypeName(group.name)
	fmt.Fprintf(out, "type %s[SK any] struct {\n", wrapperType)
	fmt.Fprintf(out, "\tstore *%s[SK]\n", defaultMeshStoreType)
	fmt.Fprintf(out, "\ttarget %s\n", group.name)
	out.WriteString("}\n\n")

	for index := range group.queries {
		query := &group.queries[index]
		params := exportedSQLCArguments(opts, query.storeParams)
		results := exportedSQLCTypes(opts, query.results)
		traced := lastResultIsError(results)
		resultSignature := resultsSignature(results)
		var errName string
		if traced {
			resultSignature, _, errName = namedResultsSignature(
				params,
				results,
				defaultReceiverName,
				"storeOptions",
			)
		}

		fmt.Fprintf(
			out,
			"func (%s *%s[SK]) %s(%s)%s {\n",
			defaultReceiverName,
			wrapperType,
			query.methodName,
			storeParamsSignature(params),
			resultSignature,
		)
		if traced {
			fmt.Fprintf(
				out,
				"\tctx, storeSpan := %s.store.mesh.StartStoreSpan(ctx, %q, %q, %s)\n",
				defaultReceiverName,
				query.store,
				query.methodName,
				queryKindConstant(query.kind),
			)
			fmt.Fprintf(out, "\tdefer func() { storeSpan.End(%s) }()\n", errName)
		}
		args := append(argumentNames(params), "storeOptions...")
		fmt.Fprintf(
			out,
			"\treturn %s.target.%s(%s)\n",
			defaultReceiverName,
			query.methodName,
			strings.Join(args, ", "),
		)
		out.WriteString("}\n\n")
		if query.command == cmdCopyFrom {
			writeCopyTelemetryMethods(out, query, params, wrapperType)
		}
	}
}

func writeCopyTelemetryMethods(
	out *bytes.Buffer,
	query *generatedQuery,
	params []argument,
	wrapperType string,
) {
	enqueue := copyEnqueueMethodName(query.methodName)
	flush := copyFlushMethodName(query.methodName)
	fmt.Fprintf(out, "func (%s *%s[SK]) %s(%s) *pgmesh.Future[int64] {\n",
		defaultReceiverName,
		wrapperType,
		enqueue,
		paramsSignature(params),
	)
	fmt.Fprintf(
		out,
		"\tctx, storeSpan := %s.store.mesh.StartStoreSpan(ctx, %q, %q, %s)\n",
		defaultReceiverName,
		query.store,
		enqueue,
		queryKindConstant(query.kind),
	)
	fmt.Fprintf(
		out,
		"\tfuture := %s.target.%s(%s)\n",
		defaultReceiverName,
		enqueue,
		strings.Join(argumentNames(params), ", "),
	)
	out.WriteString("\treturn pgmesh.RunFuture(func() (int64, error) {\n")
	out.WriteString("\t\tcount, err := future.Await(context.Background())\n")
	out.WriteString("\t\tstoreSpan.End(err)\n")
	out.WriteString("\t\treturn count, err\n")
	out.WriteString("\t})\n")
	out.WriteString("}\n\n")

	fmt.Fprintf(
		out,
		"func (%s *%s[SK]) %s(ctx context.Context) error {\n",
		defaultReceiverName,
		wrapperType,
		flush,
	)
	fmt.Fprintf(out, "\treturn %s.target.%s(ctx)\n", defaultReceiverName, flush)
	out.WriteString("}\n\n")
}

func writeStoreGroupInterfaces(out *bytes.Buffer, opts *options, group *storeGroup) {
	reader := storeReaderInterfaceName(group.name)
	writer := storeWriterInterfaceName(group.name)

	fmt.Fprintf(out, "// %s exposes read queries in the %s store group.\n", reader, group.name)
	fmt.Fprintf(out, "type %s interface {\n", reader)
	for index := range group.queries {
		query := &group.queries[index]
		if query.kind == queryKindRead {
			writeStoreInterfaceMethod(out, opts, query)
		}
	}
	out.WriteString("}\n\n")

	fmt.Fprintf(out, "// %s exposes write queries in the %s store group.\n", writer, group.name)
	fmt.Fprintf(out, "type %s interface {\n", writer)
	for index := range group.queries {
		query := &group.queries[index]
		if query.kind == queryKindWrite {
			writeStoreInterfaceMethod(out, opts, query)
		}
	}
	out.WriteString("}\n\n")

	fmt.Fprintf(out, "// %s exposes all queries in its generated store group.\n", group.name)
	fmt.Fprintf(out, "type %s interface {\n", group.name)
	fmt.Fprintf(out, "\t%s\n", reader)
	fmt.Fprintf(out, "\t%s\n", writer)
	out.WriteString("}\n\n")
}

func writeStoreInterfaceMethod(out *bytes.Buffer, opts *options, query *generatedQuery) {
	fmt.Fprintf(out, "\t// %s executes the generated %s query.\n", query.methodName, query.methodName)
	fmt.Fprintf(
		out,
		"\t%s(%s)%s\n",
		query.methodName,
		storeParamsSignature(exportedSQLCArguments(opts, query.storeParams)),
		resultsSignature(exportedSQLCTypes(opts, query.results)),
	)
	if query.command != cmdCopyFrom {
		return
	}
	params := exportedSQLCArguments(opts, query.storeParams)
	fmt.Fprintf(
		out,
		"\t// %s accepts rows for asynchronous COPY.\n",
		copyEnqueueMethodName(query.methodName),
	)
	fmt.Fprintf(
		out,
		"\t%s(%s) *pgmesh.Future[int64]\n",
		copyEnqueueMethodName(query.methodName),
		paramsSignature(params),
	)
	fmt.Fprintf(
		out,
		"\t// %s drains asynchronous submissions accepted before its barrier.\n",
		copyFlushMethodName(query.methodName),
	)
	fmt.Fprintf(
		out,
		"\t%s(ctx context.Context) error\n",
		copyFlushMethodName(query.methodName),
	)
}

func writeStoreParamAliases(out *bytes.Buffer, queries []generatedQuery) {
	for _, query := range queries {
		if query.storeParamAlias == nil {
			continue
		}
		alias := query.storeParamAlias
		fmt.Fprintf(out, "// %s is the store parameter type for %s.\n", alias.name, query.methodName)
		fmt.Fprintf(out, "type %s = %s\n\n", alias.name, alias.target)
	}
}

func writeCopyBatchTypesAndOptions(out *bytes.Buffer, queries []generatedQuery) {
	for index := range queries {
		query := &queries[index]
		if query.command != cmdCopyFrom {
			continue
		}
		stateType := copyBatchStateTypeName(query.methodName)
		fmt.Fprintf(out, "type %s struct {\n", stateType)
		out.WriteString("\tmu sync.Mutex\n")
		out.WriteString("\tenabled bool\n")
		fmt.Fprintf(
			out,
			"\tbatchers map[string]*pgmesh.CopyBatcher[%s]\n",
			copyRowType(query),
		)
		out.WriteString("}\n\n")

		optionName := copyBatchOptionName(query.methodName)
		fmt.Fprintf(
			out,
			"// %s enables asynchronous micro-batching for %s.\n",
			optionName,
			query.methodName,
		)
		fmt.Fprintf(out, "func %s(config pgmesh.CopyBatchConfig) StoreOption {\n", optionName)
		out.WriteString("\tconfigured := config\n")
		fmt.Fprintf(
			out,
			"\treturn func(options *storeOptions) { options.copyBatching.%s = &configured }\n",
			copyBatchConfigFieldName(query.methodName),
		)
		out.WriteString("}\n\n")
	}
}

func writeShardArgWrappers(out *bytes.Buffer, opts *options, queries []generatedQuery) {
	for _, query := range queries {
		if query.shardArgs == nil {
			continue
		}
		fmt.Fprintf(
			out,
			"// %s combines SQL and routing parameters for %s.\n",
			query.shardArgs.name,
			query.methodName,
		)
		fmt.Fprintf(out, "type %s struct {\n", query.shardArgs.name)
		fmt.Fprintf(out, "\t%s\n", query.shardArgs.shardKeyType)
		for _, field := range query.shardArgs.fields {
			writeStructField(
				out,
				opts,
				field.name,
				exportedSQLCType(opts, field.typ),
				field.dbName,
			)
		}
		out.WriteString("}\n\n")
		writeSQLCArgReconstructor(out, opts, query.shardArgs)
	}
}

func writeSQLCArgReconstructor(out *bytes.Buffer, opts *options, wrapper *shardArgWrapper) {
	if wrapper.sqlcArg.isEmpty() {
		return
	}

	receiverType := wrapper.name
	if wrapper.sqlcArg.emitPointer {
		receiverType = "*" + receiverType
	}
	fmt.Fprintf(
		out,
		"func (arg %s) sqlcParams() %s {\n",
		receiverType,
		wrapper.sqlcArg.defineType(opts),
	)
	out.WriteString("\treturn ")
	if wrapper.sqlcArg.emitPointer {
		out.WriteString("&")
	}
	fmt.Fprintf(out, "%s{\n", targetName(opts, wrapper.sqlcArg.structType.name))
	for _, field := range wrapper.sqlcArg.structType.fields {
		expression := wrapper.sqlcFieldExpressions[field.name]
		fmt.Fprintf(out, "\t\t%s: %s,\n", field.name, expression)
	}
	out.WriteString("\t}\n")
	out.WriteString("}\n\n")
}

func writeReadQueries(out *bytes.Buffer, opts *options, queries []generatedQuery) {
	fmt.Fprintf(out, "// %s exposes read-only generated queries.\n", defaultReadType)
	fmt.Fprintf(out, "type %s struct {\n", defaultReadType)
	fmt.Fprintf(out, "\tmain *%s\n", targetName(opts, defaultTargetType))
	out.WriteString("}\n\n")

	fmt.Fprintf(
		out,
		"func %s(q *%s) *%s {\n",
		defaultReadNew,
		targetName(opts, defaultTargetType),
		defaultReadType,
	)
	fmt.Fprintf(out, "\treturn &%s{main: q}\n", defaultReadType)
	out.WriteString("}\n\n")

	out.WriteString("// WithTx returns a read wrapper that executes queries through tx.\n")
	fmt.Fprintf(out, "func (%s *%s) WithTx(tx pgx.Tx) *%s {\n", defaultReceiverName, defaultReadType, defaultReadType)
	fmt.Fprintf(out, "\treturn %s(%s.main.WithTx(tx))\n", defaultReadNew, defaultReceiverName)
	out.WriteString("}\n\n")

	writeQueryMethods(out, opts, defaultReadType, queries, queryKindRead, false)
}

func writeWriteQueries(out *bytes.Buffer, opts *options, queries []generatedQuery) {
	fmt.Fprintf(out, "// %s exposes primary-capable generated queries.\n", defaultWriteType)
	fmt.Fprintf(out, "type %s struct {\n", defaultWriteType)
	fmt.Fprintf(out, "\tmain *%s\n", targetName(opts, defaultTargetType))
	fmt.Fprintf(out, "\tmirrors []*%s\n", targetName(opts, defaultTargetType))
	out.WriteString("}\n\n")

	fmt.Fprintf(
		out,
		"func %s(q *%s, mirrors ...*%s) *%s {\n",
		defaultWriteNew,
		targetName(opts, defaultTargetType),
		targetName(opts, defaultTargetType),
		defaultWriteType,
	)
	fmt.Fprintf(out, "\treturn &%s{main: q, mirrors: mirrors}\n", defaultWriteType)
	out.WriteString("}\n\n")

	out.WriteString("// WithTx returns a write wrapper that executes queries through tx.\n")
	fmt.Fprintf(out, "func (%s *%s) WithTx(tx pgx.Tx) *%s {\n", defaultReceiverName, defaultWriteType, defaultWriteType)
	fmt.Fprintf(out, "\treturn %s(%s.main.WithTx(tx))\n", defaultWriteNew, defaultReceiverName)
	out.WriteString("}\n\n")

	out.WriteString("// WithMirrors returns a copy that also writes to the supplied mirrors.\n")
	fmt.Fprintf(
		out,
		"func (%s *%s) WithMirrors(qs ...*%s) *%s {\n",
		defaultReceiverName,
		defaultWriteType,
		defaultWriteType,
		defaultWriteType,
	)
	fmt.Fprintf(out, "\tvar mirrors []*%s\n", targetName(opts, defaultTargetType))
	fmt.Fprintf(out, "\tmirrors = append(mirrors, %s.mirrors...)\n", defaultReceiverName)
	out.WriteString("\tfor _, mirror := range qs {\n")
	out.WriteString("\t\tif mirror == nil {\n")
	out.WriteString("\t\t\tcontinue\n")
	out.WriteString("\t\t}\n")
	out.WriteString("\t\tmirrors = append(mirrors, mirror.main)\n")
	out.WriteString("\t\tmirrors = append(mirrors, mirror.mirrors...)\n")
	out.WriteString("\t}\n")
	fmt.Fprintf(out, "\treturn %s(%s.main, mirrors...)\n", defaultWriteNew, defaultReceiverName)
	out.WriteString("}\n\n")

	fmt.Fprintf(
		out,
		"func (%s *%s) mirror(fn func(*%s) error) error {\n",
		defaultReceiverName,
		defaultWriteType,
		targetName(opts, defaultTargetType),
	)
	fmt.Fprintf(out, "\tfor _, mirror := range %s.mirrors {\n", defaultReceiverName)
	out.WriteString("\t\tif err := fn(mirror); err != nil {\n")
	if opts.IgnoreMirrorError {
		out.WriteString("\t\t\tcontinue\n")
	} else {
		out.WriteString("\t\t\tif errors.Is(err, sql.ErrNoRows) {\n")
		out.WriteString("\t\t\t\tcontinue\n")
		out.WriteString("\t\t\t}\n")
		out.WriteString("\t\t\treturn err\n")
	}
	out.WriteString("\t\t}\n")
	out.WriteString("\t}\n")
	out.WriteString("\treturn nil\n")
	out.WriteString("}\n\n")

	writeQueryMethods(out, opts, defaultWriteType, queries, queryKindWrite, true)
}

func writeQueryStore(out *bytes.Buffer, opts *options) {
	fmt.Fprintf(out, "type %s struct {\n", defaultStoreType)
	fmt.Fprintf(out, "\t*%s\n", defaultReadType)
	fmt.Fprintf(out, "\t*%s\n", defaultWriteType)
	out.WriteString("}\n\n")

	fmt.Fprintf(
		out,
		"func %s(q *%s, mirrors ...*%s) *%s {\n",
		"newQueryStore",
		targetName(opts, defaultTargetType),
		targetName(opts, defaultTargetType),
		defaultStoreType,
	)
	fmt.Fprintf(out, "\treturn &%s{\n", defaultStoreType)
	fmt.Fprintf(out, "\t\t%s:  %s(q),\n", defaultReadType, defaultReadNew)
	fmt.Fprintf(out, "\t\t%s: %s(q, mirrors...),\n", defaultWriteType, defaultWriteNew)
	out.WriteString("\t}\n")
	out.WriteString("}\n\n")

	out.WriteString("// WithTx returns a store wrapper that executes queries through tx.\n")
	fmt.Fprintf(out, "func (%s *%s) WithTx(tx pgx.Tx) *%s {\n", defaultReceiverName, defaultStoreType, defaultStoreType)
	fmt.Fprintf(
		out,
		"\treturn %s(%s.%s.main.WithTx(tx))\n",
		"newQueryStore",
		defaultReceiverName,
		defaultWriteType,
	)
	out.WriteString("}\n\n")

	out.WriteString("// WithMirrors returns a copy that also writes to the supplied mirrors.\n")
	fmt.Fprintf(
		out,
		"func (%s *%s) WithMirrors(qs ...*%s) *%s {\n",
		defaultReceiverName,
		defaultStoreType,
		defaultStoreType,
		defaultStoreType,
	)
	fmt.Fprintf(out, "\tvar mirrors []*%s\n", targetName(opts, defaultTargetType))
	fmt.Fprintf(out, "\tmirrors = append(mirrors, %s.%s.mirrors...)\n", defaultReceiverName, defaultWriteType)
	out.WriteString("\tfor _, mirror := range qs {\n")
	fmt.Fprintf(out, "\t\tif mirror == nil || mirror.%s == nil {\n", defaultWriteType)
	out.WriteString("\t\t\tcontinue\n")
	out.WriteString("\t\t}\n")
	fmt.Fprintf(out, "\t\tmirrors = append(mirrors, mirror.%s.main)\n", defaultWriteType)
	fmt.Fprintf(out, "\t\tmirrors = append(mirrors, mirror.%s.mirrors...)\n", defaultWriteType)
	out.WriteString("\t}\n")
	fmt.Fprintf(
		out,
		"\treturn %s(%s.%s.main, mirrors...)\n",
		"newQueryStore",
		defaultReceiverName,
		defaultWriteType,
	)
	out.WriteString("}\n\n")
}

func writeQueryOptions(out *bytes.Buffer) {
	out.WriteString("type queryOptions struct {\n")
	out.WriteString("\tprimary bool\n")
	out.WriteString("\ttx pgx.Tx\n")
	out.WriteString("}\n\n")
	out.WriteString("// QueryOption customizes routing for one generated query call.\n")
	out.WriteString("type QueryOption func(*queryOptions)\n\n")
	out.WriteString("// ReadFromPrimary routes a read query to the primary database.\n")
	out.WriteString("func ReadFromPrimary() QueryOption {\n")
	out.WriteString("\treturn func(options *queryOptions) { options.primary = true }\n")
	out.WriteString("}\n\n")
	out.WriteString("// WithTx executes a query through tx and suppresses write mirrors.\n")
	out.WriteString("func WithTx(tx pgx.Tx) QueryOption {\n")
	out.WriteString("\treturn func(options *queryOptions) { options.tx = tx }\n")
	out.WriteString("}\n\n")
	out.WriteString("func applyQueryOptions(options ...QueryOption) queryOptions {\n")
	out.WriteString("\tvar result queryOptions\n")
	out.WriteString("\tfor _, option := range options {\n")
	out.WriteString("\t\tif option != nil { option(&result) }\n")
	out.WriteString("\t}\n")
	out.WriteString("\treturn result\n")
	out.WriteString("}\n\n")
}

func writeNodeConstructor(out *bytes.Buffer, opts *options) {
	fmt.Fprintf(
		out,
		"func %s(database %s) pgmesh.Node[*%s, *%s] {\n",
		defaultNodeNew,
		targetName(opts, "DBTX"),
		defaultReadType,
		defaultStoreType,
	)
	fmt.Fprintf(out, "\tqueries := %s(database)\n", targetName(opts, defaultTargetNew))
	fmt.Fprintf(
		out,
		"\treturn pgmesh.NewNode(%s(queries), %s(queries))\n",
		defaultReadNew,
		"newQueryStore",
	)
	out.WriteString("}\n\n")
}

func hasShardOperations(queries []generatedQuery) bool {
	for index := range queries {
		if queries[index].shardMode != shardModeNone {
			return true
		}
	}
	return false
}

func writeCopyBatchInitialization(out *bytes.Buffer, copies []generatedQuery) {
	if len(copies) == 0 {
		return
	}
	fmt.Fprintf(
		out,
		"func (%s *%s[SK]) initializeCopyBatchers(options storeOptions) error {\n",
		defaultReceiverName,
		defaultMeshStoreType,
	)
	for index := range copies {
		query := &copies[index]
		stateName := copyBatchStateFieldName(query.methodName)
		configName := stateName + "Config"
		enabledName := stateName + "Enabled"
		fmt.Fprintf(out, "\t%s := pgmesh.CopyBatchConfig{}\n", configName)
		fmt.Fprintf(out, "\t%s := false\n", enabledName)
		fmt.Fprintf(
			out,
			"\tif configured := options.copyBatching.%s; configured != nil {\n",
			copyBatchConfigFieldName(query.methodName),
		)
		fmt.Fprintf(out, "\t\t%s = *configured\n", configName)
		fmt.Fprintf(out, "\t\t%s = true\n", enabledName)
		out.WriteString("\t}\n")
		fmt.Fprintf(out, "\t%s := &%s{\n", stateName, copyBatchStateTypeName(query.methodName))
		fmt.Fprintf(out, "\t\tenabled: %s,\n", enabledName)
		fmt.Fprintf(
			out,
			"\t\tbatchers: make(map[string]*pgmesh.CopyBatcher[%s]),\n",
			copyRowType(query),
		)
		out.WriteString("\t}\n")
		fmt.Fprintf(out, "\tfor _, shard := range %s.mesh.AllShards() {\n", defaultReceiverName)
		out.WriteString("\t\troute := shard.WriteRoute()\n")
		fmt.Fprintf(
			out,
			"\t\tbatcher, err := pgmesh.NewCopyBatcher[%s](%s, func(ctx context.Context, rows %s) (count int64, queryErr error) {\n",
			copyRowType(query),
			configName,
			query.params[1].typ,
		)
		fmt.Fprintf(
			out,
			"\t\t\tqueryCtx, physicalQuerySpan := %s.mesh.StartQuerySpan(ctx, %q, %q, pgmesh.QueryKindWrite, route.Metadata().WithoutVirtualShard(), pgmesh.RouteModePrimary)\n",
			defaultReceiverName,
			query.store,
			query.methodName,
		)
		out.WriteString("\t\t\tdefer func() { physicalQuerySpan.End(queryErr) }()\n")
		fmt.Fprintf(out, "\t\t\treturn route.Target.%s(queryCtx, rows)\n", query.methodName)
		fmt.Fprintf(
			out,
			"\t\t}, %s.mesh.CopyBatchObserver(%q, %q, route.Metadata()))\n",
			defaultReceiverName,
			query.store,
			query.methodName,
		)
		out.WriteString("\t\tif err != nil {\n")
		fmt.Fprintf(
			out,
			"\t\t\treturn fmt.Errorf(%q, err)\n",
			"configure "+query.methodName+" copy batching: %w",
		)
		out.WriteString("\t\t}\n")
		fmt.Fprintf(out, "\t\t%s.batchers[shard.Name()] = batcher\n", stateName)
		out.WriteString("\t}\n")
		fmt.Fprintf(out, "\t%s.%s = %s\n", defaultReceiverName, stateName, stateName)
	}
	out.WriteString("\treturn nil\n")
	out.WriteString("}\n\n")
}

func writeStoreConfiguration(
	out *bytes.Buffer,
	opts *options,
	queries []generatedQuery,
	groups []storeGroup,
) {
	copies := copyQueries(queries)
	out.WriteString("type storeOptions struct {\n")
	out.WriteString("\ttracerProvider trace.TracerProvider\n")
	out.WriteString("\tmeterProvider metric.MeterProvider\n")
	out.WriteString("\tlogger *slog.Logger\n")
	if len(groups) > 0 {
		out.WriteString("\tfactories struct {\n")
		for _, group := range groups {
			fmt.Fprintf(out, "\t\t%s func(%s) %s\n", group.name, group.name, group.name)
		}
		out.WriteString("\t}\n")
	}
	if len(copies) > 0 {
		out.WriteString("\tcopyBatching struct {\n")
		for index := range copies {
			query := &copies[index]
			fmt.Fprintf(
				out,
				"\t\t%s *pgmesh.CopyBatchConfig\n",
				copyBatchConfigFieldName(query.methodName),
			)
		}
		out.WriteString("\t}\n")
	}
	out.WriteString("}\n\n")

	out.WriteString("// StoreOption customizes a generated store.\n")
	out.WriteString("type StoreOption func(*storeOptions)\n\n")
	out.WriteString("// WithTracerProvider configures the provider used for routed query spans.\n")
	out.WriteString("// A nil provider uses the global OpenTelemetry tracer provider.\n")
	out.WriteString("func WithTracerProvider(provider trace.TracerProvider) StoreOption {\n")
	out.WriteString("\treturn func(options *storeOptions) { options.tracerProvider = provider }\n")
	out.WriteString("}\n\n")
	out.WriteString("// WithMeterProvider configures the provider used for routed query metrics.\n")
	out.WriteString("// A nil provider uses the global OpenTelemetry meter provider.\n")
	out.WriteString("func WithMeterProvider(provider metric.MeterProvider) StoreOption {\n")
	out.WriteString("\treturn func(options *storeOptions) { options.meterProvider = provider }\n")
	out.WriteString("}\n\n")
	out.WriteString("// WithLogger configures optional structured logging for routed queries.\n")
	out.WriteString("// A nil logger disables logging.\n")
	out.WriteString("func WithLogger(logger *slog.Logger) StoreOption {\n")
	out.WriteString("\treturn func(options *storeOptions) { options.logger = logger }\n")
	out.WriteString("}\n\n")
	out.WriteString("func applyStoreOptions(options ...StoreOption) (storeOptions, error) {\n")
	out.WriteString("\tvar result storeOptions\n")
	out.WriteString("\tfor index, option := range options {\n")
	out.WriteString("\t\tif option == nil {\n")
	out.WriteString("\t\t\treturn storeOptions{}, fmt.Errorf(\"pgmesh: store option %d is nil\", index)\n")
	out.WriteString("\t\t}\n")
	out.WriteString("\t\toption(&result)\n")
	out.WriteString("\t}\n")
	out.WriteString("\treturn result, nil\n")
	out.WriteString("}\n\n")

	fmt.Fprintf(out, "// Topology is an opaque database topology for %s.\n", opts.StoreInterfaceName)
	out.WriteString("type Topology interface {\n")
	fmt.Fprintf(out, "\tbuildStore(context.Context, storeOptions) (%s, error)\n", opts.StoreInterfaceName)
	out.WriteString("}\n\n")

	out.WriteString("type singletonConfig struct {\n")
	out.WriteString("\tname string\n")
	fmt.Fprintf(out, "\tprimary %s\n", targetName(opts, "DBTX"))
	fmt.Fprintf(out, "\treplicas []%s\n", targetName(opts, "DBTX"))
	fmt.Fprintf(out, "\tmirrors []%s\n", targetName(opts, "DBTX"))
	out.WriteString("}\n\n")

	out.WriteString("// SingletonOption customizes a single-database topology.\n")
	out.WriteString("type SingletonOption func(*singletonConfig)\n\n")
	out.WriteString("// WithDatabaseName identifies the database in telemetry.\n")
	out.WriteString("// An empty name defaults to \"default\".\n")
	out.WriteString("func WithDatabaseName(name string) SingletonOption {\n")
	out.WriteString("\treturn func(config *singletonConfig) { config.name = name }\n")
	out.WriteString("}\n\n")
	out.WriteString("// WithReadReplicas appends databases used for round-robin reads.\n")
	fmt.Fprintf(out, "func WithReadReplicas(databases ...%s) SingletonOption {\n", targetName(opts, "DBTX"))
	fmt.Fprintf(out, "\treplicas := append([]%s(nil), databases...)\n", targetName(opts, "DBTX"))
	out.WriteString("\treturn func(config *singletonConfig) { config.replicas = append(config.replicas, replicas...) }\n")
	out.WriteString("}\n\n")
	out.WriteString("// WithWriteMirrors appends databases that synchronously receive writes.\n")
	fmt.Fprintf(out, "func WithWriteMirrors(databases ...%s) SingletonOption {\n", targetName(opts, "DBTX"))
	fmt.Fprintf(out, "\tmirrors := append([]%s(nil), databases...)\n", targetName(opts, "DBTX"))
	out.WriteString("\treturn func(config *singletonConfig) { config.mirrors = append(config.mirrors, mirrors...) }\n")
	out.WriteString("}\n\n")

	out.WriteString("type singletonTopology struct {\n")
	out.WriteString("\tconfig singletonConfig\n")
	out.WriteString("\terr error\n")
	out.WriteString("}\n\n")
	out.WriteString("// Singleton returns a topology with one primary database.\n")
	fmt.Fprintf(out, "func Singleton(primary %s, options ...SingletonOption) Topology {\n", targetName(opts, "DBTX"))
	out.WriteString("\tconfig := singletonConfig{name: \"default\", primary: primary}\n")
	out.WriteString("\tfor index, option := range options {\n")
	out.WriteString("\t\tif option == nil {\n")
	out.WriteString("\t\t\treturn singletonTopology{err: fmt.Errorf(\"pgmesh: singleton option %d is nil\", index)}\n")
	out.WriteString("\t\t}\n")
	out.WriteString("\t\toption(&config)\n")
	out.WriteString("\t}\n")
	out.WriteString("\treturn singletonTopology{config: config}\n")
	out.WriteString("}\n\n")

	fmt.Fprintf(out, "// %s creates the generated query API from an opaque topology configuration.\n", opts.ConstructorName)
	fmt.Fprintf(
		out,
		"func %s(ctx context.Context, topology Topology, options ...StoreOption) (%s, error) {\n",
		opts.ConstructorName,
		opts.StoreInterfaceName,
	)
	out.WriteString("\tif topology == nil {\n")
	out.WriteString("\t\treturn nil, fmt.Errorf(\"pgmesh: topology is nil\")\n")
	out.WriteString("\t}\n")
	out.WriteString("\tstoreOptions, err := applyStoreOptions(options...)\n")
	out.WriteString("\tif err != nil { return nil, err }\n")
	out.WriteString("\treturn topology.buildStore(ctx, storeOptions)\n")
	out.WriteString("}\n\n")

	fmt.Fprintf(out, "type %s[SK any] struct {\n", defaultMeshStoreType)
	fmt.Fprintf(out, "\tmesh *pgmesh.Mesh[*%s, *%s, SK]\n", defaultReadType, defaultStoreType)
	if hasShardOperations(queries) {
		fmt.Fprintf(out, "\tresolver %s[SK]\n", opts.ResolverInterfaceName)
	}
	for index := range copies {
		query := &copies[index]
		fmt.Fprintf(
			out,
			"\t%s *%s\n",
			copyBatchStateFieldName(query.methodName),
			copyBatchStateTypeName(query.methodName),
		)
	}
	if len(groups) > 0 {
		out.WriteString("\tgroups struct {\n")
		for _, group := range groups {
			fmt.Fprintf(out, "\t\t%s %s\n", group.name, group.name)
		}
		out.WriteString("\t}\n")
	}
	out.WriteString("}\n\n")
	fmt.Fprintf(out, "var _ %s = (*%s[uint8])(nil)\n\n", opts.StoreInterfaceName, defaultMeshStoreType)
	writeCopyBatchInitialization(out, copies)
	if len(groups) > 0 {
		fmt.Fprintf(out, "type %s[SK any] struct {\n", defaultGroupType)
		fmt.Fprintf(out, "\tstore *%s[SK]\n", defaultMeshStoreType)
		out.WriteString("}\n\n")

		fmt.Fprintf(
			out,
			"func (%s *%s[SK]) initializeGroups(options storeOptions) {\n",
			defaultReceiverName,
			defaultMeshStoreType,
		)
		for _, group := range groups {
			fmt.Fprintf(
				out,
				"\tinternal%s := &%s[SK]{store: %s}\n",
				group.name,
				defaultGroupType,
				defaultReceiverName,
			)
			fmt.Fprintf(out, "\t%s.groups.%s = internal%s\n", defaultReceiverName, group.name, group.name)
			fmt.Fprintf(out, "\tif create%s := options.factories.%s; create%s != nil {\n", group.name, group.name, group.name)
			fmt.Fprintf(
				out,
				"\t\t%s.groups.%s = &%s[SK]{store: %s, target: create%s(internal%s)}\n",
				defaultReceiverName,
				group.name,
				storeTelemetryTypeName(group.name),
				defaultReceiverName,
				group.name,
				group.name,
			)
			out.WriteString("\t}\n")
		}
		out.WriteString("}\n\n")
	}

	fmt.Fprintf(
		out,
		"func (c singletonTopology) buildStore(_ context.Context, options storeOptions) (%s, error) {\n",
		opts.StoreInterfaceName,
	)
	out.WriteString("\tif c.err != nil { return nil, c.err }\n")
	out.WriteString("\tconfig := c.config\n")
	out.WriteString("\tif config.name == \"\" { config.name = \"default\" }\n")
	out.WriteString("\tif config.primary == nil {\n")
	out.WriteString("\t\treturn nil, fmt.Errorf(\"pgmesh: database primary is nil\")\n")
	out.WriteString("\t}\n")
	out.WriteString("\tfor index, database := range config.replicas {\n")
	out.WriteString("\t\tif database == nil { return nil, fmt.Errorf(\"pgmesh: database replica %d is nil\", index) }\n")
	out.WriteString("\t}\n")
	out.WriteString("\tfor index, database := range config.mirrors {\n")
	out.WriteString("\t\tif database == nil { return nil, fmt.Errorf(\"pgmesh: database mirror %d is nil\", index) }\n")
	out.WriteString("\t}\n")
	fmt.Fprintf(out, "\tprimary := %s(config.primary)\n", defaultNodeNew)
	fmt.Fprintf(out, "\treplicas := make([]pgmesh.Node[*%s, *%s], 0, len(config.replicas))\n", defaultReadType, defaultStoreType)
	out.WriteString("\tfor _, database := range config.replicas {\n")
	fmt.Fprintf(out, "\t\treplicas = append(replicas, %s(database))\n", defaultNodeNew)
	out.WriteString("\t}\n")
	out.WriteString("\treplicaSet := pgmesh.NewReplicaSet(config.name, primary, replicas)\n")
	fmt.Fprintf(out, "\tmirrors := make([]*%s, 0, len(config.mirrors))\n", defaultStoreType)
	out.WriteString("\tfor _, database := range config.mirrors {\n")
	fmt.Fprintf(out, "\t\tmirrors = append(mirrors, %s(database).Writer())\n", defaultNodeNew)
	out.WriteString("\t}\n")
	out.WriteString("\treplicaSet = replicaSet.WithWriteMirrors(mirrors...)\n")
	fmt.Fprintf(out, "\tmesh, err := pgmesh.NewBuilder[*%s, *%s, uint8](1).\n", defaultReadType, defaultStoreType)
	out.WriteString("\t\tWithHasher(pgmesh.ConstantShardHashFor[uint8](0)).\n")
	out.WriteString("\t\tWithTracerProvider(options.tracerProvider).\n")
	out.WriteString("\t\tWithMeterProvider(options.meterProvider).\n")
	out.WriteString("\t\tWithLogger(options.logger).\n")
	out.WriteString("\t\tLink(0, replicaSet).\n")
	out.WriteString("\t\tBuild()\n")
	out.WriteString("\tif err != nil { return nil, err }\n")
	if len(groups) > 0 {
		fmt.Fprintf(out, "\tstore := &%s[uint8]{mesh: mesh}\n", defaultMeshStoreType)
		if len(copies) > 0 {
			out.WriteString("\tif err := store.initializeCopyBatchers(options); err != nil { return nil, err }\n")
		}
		out.WriteString("\tstore.initializeGroups(options)\n")
		out.WriteString("\treturn store, nil\n")
	} else {
		fmt.Fprintf(
			out,
			"\treturn &%s[uint8]{mesh: mesh}, nil\n",
			defaultMeshStoreType,
		)
	}
	out.WriteString("}\n\n")
}

func writeMeshStoreQueryMethod(out *bytes.Buffer, opts *options, query *generatedQuery) {
	switch query.shardMode {
	case shardModeAll:
		writeAllShardsQueryMethod(out, query)
		return
	case shardModeGroupedCopy:
		writeGroupedCopyQueryMethod(out, query)
		return
	case shardModeGroupedMany:
		writeGroupedManyQueryMethod(out, opts, query)
		return
	}

	receiverType := defaultGroupType
	store := defaultReceiverName + ".store"
	traced := lastResultIsError(query.results)
	resultSignature := resultsSignature(query.results)
	var resultNames []string
	var errName string
	if traced {
		resultSignature, resultNames, errName = namedResultsSignature(
			query.storeParams,
			query.results,
			defaultReceiverName,
			"storeOptions",
		)
	}
	fmt.Fprintf(out, "// %s executes the generated query on its target shard.\n", query.methodName)
	fmt.Fprintf(
		out,
		"func (%s *%s[SK]) %s(%s)%s {\n",
		defaultReceiverName,
		receiverType,
		query.methodName,
		storeParamsSignature(query.storeParams),
		resultSignature,
	)
	if traced {
		out.WriteString("\t// Trace the query and record its returned error.\n")
		writeInternalQuerySpanStart(out, store, query, errName)
		out.WriteString("\n")
	}

	out.WriteString("\t// Resolve the shard key for this topology.\n")
	out.WriteString("\tvar shardKey SK\n")
	if query.route != nil {
		fmt.Fprintf(out, "\tif %s.resolver != nil {\n", store)
		fmt.Fprintf(
			out,
			"\t\tshardKey = %s.resolver.%s(arg.%s)\n",
			store,
			query.route.methodName,
			query.route.methodName,
		)
		out.WriteString("\t}\n")
	}
	if traced {
		fmt.Fprintf(out, "\tshard, %s := %s.mesh.Shard(shardKey)\n", errName, store)
		fmt.Fprintf(out, "\tif %s != nil {\n", errName)
		fmt.Fprintf(out, "\t\treturn %s\n", strings.Join(resultNames, ", "))
		out.WriteString("\t}\n")
	} else {
		fmt.Fprintf(out, "\tshard, _ := %s.mesh.Shard(shardKey)\n", store)
	}

	out.WriteString("\n\t// Apply options that can override the default route.\n")
	out.WriteString("\toptions := applyQueryOptions(storeOptions...)\n")
	args := strings.Join(query.callArgs, ", ")
	if query.kind == queryKindRead {
		out.WriteString("\n\tswitch {\n")
		out.WriteString("\t// Transactional reads must use their transaction.\n")
		out.WriteString("\tcase options.tx != nil:\n")
		if traced {
			out.WriteString("\t\tquerySpan.SetRoute(pgmesh.RouteModeTransaction)\n")
		}
		out.WriteString("\t\troute := shard.WriteRoute()\n")
		out.WriteString("\t\ttarget := route.Target.WithTx(options.tx)\n")
		if traced {
			out.WriteString("\t\tctx, physicalQuerySpan := querySpan.StartQuerySpan(ctx, route.Metadata(), pgmesh.RouteModeTransaction)\n")
			fmt.Fprintf(out, "\t\tdefer func() { physicalQuerySpan.End(%s) }()\n", errName)
		}
		fmt.Fprintf(
			out,
			"\t\treturn target.%s(%s)\n",
			query.methodName,
			args,
		)

		out.WriteString("\n\t// Explicit primary reads bypass replicas.\n")
		out.WriteString("\tcase options.primary:\n")
		if traced {
			out.WriteString("\t\tquerySpan.SetRoute(pgmesh.RouteModePrimary)\n")
		}
		out.WriteString("\t\troute := shard.WriteRoute()\n")
		if traced {
			out.WriteString("\t\tctx, physicalQuerySpan := querySpan.StartQuerySpan(ctx, route.Metadata(), pgmesh.RouteModePrimary)\n")
			fmt.Fprintf(out, "\t\tdefer func() { physicalQuerySpan.End(%s) }()\n", errName)
		}
		fmt.Fprintf(out, "\t\treturn route.Target.%s(%s)\n", query.methodName, args)

		out.WriteString("\n\t// Ordinary reads use the shard's replica route.\n")
		out.WriteString("\tdefault:\n")
		if traced {
			out.WriteString("\t\tquerySpan.SetRoute(pgmesh.RouteModeRead)\n")
		}
		out.WriteString("\t\troute := shard.ReadRoute()\n")
		if traced {
			out.WriteString("\t\tctx, physicalQuerySpan := querySpan.StartQuerySpan(ctx, route.Metadata(), pgmesh.RouteModeRead)\n")
			fmt.Fprintf(out, "\t\tdefer func() { physicalQuerySpan.End(%s) }()\n", errName)
		}
		fmt.Fprintf(out, "\t\treturn route.Target.%s(%s)\n", query.methodName, args)
		out.WriteString("\t}\n")
	} else {
		out.WriteString("\n\t// Select the primary write route, or the transaction when provided.\n")
		out.WriteString("\troute := shard.WriteRoute()\n")
		out.WriteString("\ttarget := route.Target\n")
		if traced {
			out.WriteString("\tmode := pgmesh.RouteModePrimary\n")
		}
		out.WriteString("\tif options.tx != nil {\n")
		out.WriteString("\t\ttarget = target.WithTx(options.tx)\n")
		if traced {
			out.WriteString("\t\tmode = pgmesh.RouteModeTransaction\n")
		}
		out.WriteString("\t}\n")

		if traced {
			out.WriteString("\n\t// Execute the write after recording its resolved route.\n")
			out.WriteString("\tquerySpan.SetRoute(mode)\n")
			out.WriteString("\tctx, physicalQuerySpan := querySpan.StartQuerySpan(ctx, route.Metadata(), mode)\n")
			fmt.Fprintf(out, "\tdefer func() { physicalQuerySpan.End(%s) }()\n", errName)
		} else {
			out.WriteString("\n\t// Execute the write on the selected target.\n")
		}
		fmt.Fprintf(out, "\treturn target.%s(%s)\n", query.methodName, args)
	}
	out.WriteString("}\n\n")
}

func writeInternalQuerySpanStart(
	out *bytes.Buffer,
	store string,
	query *generatedQuery,
	errName string,
) {
	fmt.Fprintf(
		out,
		"\tctx, querySpan := %s.mesh.StartSpan(ctx, %q, %q, %s)\n",
		store,
		query.store,
		query.methodName,
		queryKindConstant(query.kind),
	)
	fmt.Fprintf(out, "\tdefer func() { querySpan.End(%s) }()\n", errName)
}

func writeGroupedManyQueryMethod(
	out *bytes.Buffer,
	opts *options,
	query *generatedQuery,
) {
	receiverType := defaultGroupType
	store := defaultReceiverName + ".store"
	spec := query.groupedMany
	resultSignature, resultNames, errName := namedResultsSignature(
		query.storeParams,
		query.results,
		defaultReceiverName,
		"storeOptions",
	)
	methodDescription := "groups lookup values by physical shard and restores input-key result order"
	if spec.resultKeyAccess.nullable {
		methodDescription += "; rows with NULL lookup keys are appended"
	}
	fmt.Fprintf(
		out,
		"// %s %s.\n",
		query.methodName,
		methodDescription,
	)
	fmt.Fprintf(
		out,
		"func (%s *%s[SK]) %s(%s)%s {\n",
		defaultReceiverName,
		receiverType,
		query.methodName,
		storeParamsSignature(query.storeParams),
		resultSignature,
	)
	writeInternalQuerySpanStart(out, store, query, errName)
	out.WriteString("\n")
	out.WriteString("\toptions := applyQueryOptions(storeOptions...)\n")
	out.WriteString("\ttype manyShardGroup struct {\n")
	fmt.Fprintf(
		out,
		"\t\tshard *pgmesh.Shard[*%s, *%s]\n",
		defaultReadType,
		defaultStoreType,
	)
	fmt.Fprintf(out, "\t\targs []%s\n", spec.elementType)
	out.WriteString("\t\trequested map[any]struct{}\n")
	out.WriteString("\t}\n")
	out.WriteString("\ttype manyOrderItem struct {\n")
	out.WriteString("\t\tshardName string\n")
	out.WriteString("\t\tkey any\n")
	out.WriteString("\t}\n")
	out.WriteString("\tgroupsByName := make(map[string]*manyShardGroup)\n")
	out.WriteString("\torderedItems := make([]manyOrderItem, 0)\n")
	inputName := query.storeParams[1].name
	fmt.Fprintf(out, "\tfor inputIndex, item := range %s {\n", inputName)
	if spec.itemIsPointer {
		out.WriteString("\t\tif item == nil {\n")
		fmt.Fprintf(
			out,
			"\t\t\t%s = fmt.Errorf(%q, inputIndex)\n",
			errName,
			"route "+query.methodName+" input %d: shard parameter is nil",
		)
		fmt.Fprintf(out, "\t\t\treturn %s\n", strings.Join(resultNames, ", "))
		out.WriteString("\t\t}\n")
	}
	lookupExpression := spec.lookupExpression
	fmt.Fprintf(out, "\t\tlookupValue := %s\n", lookupExpression)
	out.WriteString("\t\tlookupKey := any(lookupValue)\n")
	out.WriteString("\t\tif lookupKey != nil && !reflect.ValueOf(lookupKey).Comparable() {\n")
	fmt.Fprintf(
		out,
		"\t\t\t%s = fmt.Errorf(%q, inputIndex, lookupKey)\n",
		errName,
		"route "+query.methodName+" input %d: lookup key type %T is not comparable",
	)
	fmt.Fprintf(out, "\t\t\treturn %s\n", strings.Join(resultNames, ", "))
	out.WriteString("\t\t}\n")
	out.WriteString("\t\tvar shardKey SK\n")
	fmt.Fprintf(out, "\t\tif %s.resolver != nil {\n", store)
	fmt.Fprintf(
		out,
		"\t\t\tshardKey = %s.resolver.%s(item.%s)\n",
		store,
		query.route.methodName,
		query.route.methodName,
	)
	out.WriteString("\t\t}\n")
	fmt.Fprintf(out, "\t\tshard, routeErr := %s.mesh.Shard(shardKey)\n", store)
	out.WriteString("\t\tif routeErr != nil {\n")
	fmt.Fprintf(
		out,
		"\t\t\t%s = fmt.Errorf(%q, inputIndex, routeErr)\n",
		errName,
		"route "+query.methodName+" input %d: %w",
	)
	fmt.Fprintf(out, "\t\t\treturn %s\n", strings.Join(resultNames, ", "))
	out.WriteString("\t\t}\n")
	out.WriteString("\t\tshardGroup := groupsByName[shard.Name()]\n")
	out.WriteString("\t\tif shardGroup == nil {\n")
	fmt.Fprintf(
		out,
		"\t\t\tshardGroup = &manyShardGroup{shard: shard, args: make([]%s, 0), requested: make(map[any]struct{})}\n",
		spec.elementType,
	)
	out.WriteString("\t\t\tgroupsByName[shard.Name()] = shardGroup\n")
	out.WriteString("\t\t}\n")
	out.WriteString("\t\tif _, exists := shardGroup.requested[lookupKey]; exists { continue }\n")
	out.WriteString("\t\tshardGroup.requested[lookupKey] = struct{}{}\n")
	out.WriteString("\t\tshardGroup.args = append(shardGroup.args, lookupValue)\n")
	out.WriteString("\t\torderedItems = append(orderedItems, manyOrderItem{shardName: shard.Name(), key: lookupKey})\n")
	out.WriteString("\t}\n\n")

	out.WriteString("\tgroups := make([]*manyShardGroup, 0, len(groupsByName))\n")
	fmt.Fprintf(out, "\tfor _, shard := range %s.mesh.AllShards() {\n", store)
	out.WriteString("\t\tif shardGroup := groupsByName[shard.Name()]; shardGroup != nil {\n")
	out.WriteString("\t\t\tgroups = append(groups, shardGroup)\n")
	out.WriteString("\t\t}\n")
	out.WriteString("\t}\n")
	out.WriteString("\tif options.tx != nil && len(groups) > 1 {\n")
	out.WriteString("\t\tquerySpan.SetMultiRoute(pgmesh.RouteModeTransaction, len(groups))\n")
	fmt.Fprintf(out, "\t\t%s = pgmesh.ErrCrossShardTransaction\n", errName)
	fmt.Fprintf(out, "\t\treturn %s\n", strings.Join(resultNames, ", "))
	out.WriteString("\t}\n\n")

	if query.kind == queryKindRead {
		out.WriteString("\tmode := pgmesh.RouteModeRead\n")
		out.WriteString("\tif options.primary { mode = pgmesh.RouteModePrimary }\n")
	} else {
		out.WriteString("\tmode := pgmesh.RouteModePrimary\n")
	}
	out.WriteString("\tif options.tx != nil { mode = pgmesh.RouteModeTransaction }\n")
	out.WriteString("\tquerySpan.SetMultiRoute(mode, len(groups))\n")
	fmt.Fprintf(out, "\tif len(groups) == 0 { return %s }\n\n", strings.Join(resultNames, ", "))

	fmt.Fprintf(out, "\ttype manyResult struct {\n\t\tvalue %s\n\t\terr error\n\t}\n", query.results[0])
	out.WriteString("\tgroupResults := make([]manyResult, len(groups))\n")
	out.WriteString("\tvar waitGroup sync.WaitGroup\n")
	out.WriteString("\tfor index, shardGroup := range groups {\n")
	out.WriteString("\t\twaitGroup.Go(func() {\n")
	sqlcArg := groupedManySQLCArgument(opts, query, "shardGroup.args")
	if query.kind == queryKindRead {
		out.WriteString("\t\t\tswitch {\n")
		out.WriteString("\t\t\tcase options.tx != nil:\n")
		out.WriteString("\t\t\t\troute := shardGroup.shard.WriteRoute()\n")
		out.WriteString("\t\t\t\ttarget := route.Target.WithTx(options.tx)\n")
		out.WriteString(
			"\t\t\t\tqueryCtx, physicalQuerySpan := querySpan.StartQuerySpan(ctx, route.Metadata().WithoutVirtualShard(), mode)\n",
		)
		fmt.Fprintf(
			out,
			"\t\t\t\tgroupResults[index].value, groupResults[index].err = target.%s(queryCtx, %s)\n",
			query.methodName,
			sqlcArg,
		)
		out.WriteString("\t\t\t\tphysicalQuerySpan.End(groupResults[index].err)\n")
		out.WriteString("\t\t\tcase options.primary:\n")
		out.WriteString("\t\t\t\troute := shardGroup.shard.WriteRoute()\n")
		out.WriteString(
			"\t\t\t\tqueryCtx, physicalQuerySpan := querySpan.StartQuerySpan(ctx, route.Metadata().WithoutVirtualShard(), mode)\n",
		)
		fmt.Fprintf(
			out,
			"\t\t\t\tgroupResults[index].value, groupResults[index].err = route.Target.%s(queryCtx, %s)\n",
			query.methodName,
			sqlcArg,
		)
		out.WriteString("\t\t\t\tphysicalQuerySpan.End(groupResults[index].err)\n")
		out.WriteString("\t\t\tdefault:\n")
		out.WriteString("\t\t\t\troute := shardGroup.shard.ReadRoute()\n")
		out.WriteString(
			"\t\t\t\tqueryCtx, physicalQuerySpan := querySpan.StartQuerySpan(ctx, route.Metadata().WithoutVirtualShard(), mode)\n",
		)
		fmt.Fprintf(
			out,
			"\t\t\t\tgroupResults[index].value, groupResults[index].err = route.Target.%s(queryCtx, %s)\n",
			query.methodName,
			sqlcArg,
		)
		out.WriteString("\t\t\t\tphysicalQuerySpan.End(groupResults[index].err)\n")
		out.WriteString("\t\t\t}\n")
	} else {
		out.WriteString("\t\t\troute := shardGroup.shard.WriteRoute()\n")
		out.WriteString("\t\t\ttarget := route.Target\n")
		out.WriteString("\t\t\tif options.tx != nil { target = target.WithTx(options.tx) }\n")
		out.WriteString(
			"\t\t\tqueryCtx, physicalQuerySpan := querySpan.StartQuerySpan(ctx, route.Metadata().WithoutVirtualShard(), mode)\n",
		)
		fmt.Fprintf(
			out,
			"\t\t\tgroupResults[index].value, groupResults[index].err = target.%s(queryCtx, %s)\n",
			query.methodName,
			sqlcArg,
		)
		out.WriteString("\t\t\tphysicalQuerySpan.End(groupResults[index].err)\n")
	}
	out.WriteString("\t\t})\n")
	out.WriteString("\t}\n")
	out.WriteString("\twaitGroup.Wait()\n\n")

	out.WriteString("\tgroupErrors := make([]error, 0, len(groups))\n")
	out.WriteString("\tfor index, groupResult := range groupResults {\n")
	out.WriteString("\t\tif groupResult.err != nil {\n")
	fmt.Fprintf(
		out,
		"\t\t\tgroupErrors = append(groupErrors, fmt.Errorf(%q, groups[index].shard.Name(), groupResult.err))\n",
		"query "+query.methodName+" on replica set %q: %w",
	)
	out.WriteString("\t\t}\n")
	out.WriteString("\t}\n")
	fmt.Fprintf(out, "\t%s = errors.Join(groupErrors...)\n", errName)
	fmt.Fprintf(out, "\tif %s != nil { return %s }\n\n", errName, strings.Join(resultNames, ", "))

	rowType := strings.TrimPrefix(query.results[0], "[]")
	fmt.Fprintf(out, "\trowsByGroup := make(map[string]map[any][]%s, len(groups))\n", rowType)
	if spec.resultKeyAccess.nullable {
		fmt.Fprintf(out, "\tunkeyedRows := make([]%s, 0)\n", rowType)
	}
	out.WriteString("\tfor groupIndex, groupResult := range groupResults {\n")
	fmt.Fprintf(out, "\t\trowsByKey := make(map[any][]%s)\n", rowType)
	out.WriteString("\t\trowsByGroup[groups[groupIndex].shard.Name()] = rowsByKey\n")
	out.WriteString("\t\tfor resultIndex, row := range groupResult.value {\n")
	if spec.resultIsStruct && query.ret.emitPointer {
		out.WriteString("\t\t\tif row == nil {\n")
		fmt.Fprintf(
			out,
			"\t\t\t\tgroupErrors = append(groupErrors, fmt.Errorf(%q, groups[groupIndex].shard.Name(), resultIndex))\n",
			"query "+query.methodName+" on replica set %q returned nil row at result %d",
		)
		out.WriteString("\t\t\t\tcontinue\n")
		out.WriteString("\t\t\t}\n")
	}
	resultKeyExpression := "row"
	if spec.resultIsStruct {
		resultKeyExpression = "row." + spec.resultKeyField
	}
	if spec.resultKeyAccess.nullable {
		fmt.Fprintf(out, "\t\t\tresultLookupValue := %s\n", resultKeyExpression)
		switch {
		case spec.resultKeyAccess.pointer:
			out.WriteString("\t\t\tif resultLookupValue == nil {\n")
		case spec.resultKeyAccess.valid:
			out.WriteString("\t\t\tif !resultLookupValue.Valid {\n")
		}
		out.WriteString("\t\t\t\tunkeyedRows = append(unkeyedRows, row)\n")
		out.WriteString("\t\t\t\tcontinue\n")
		out.WriteString("\t\t\t}\n")
		resultKeyExpression = "resultLookupValue"
		if spec.resultKeyAccess.pointer {
			resultKeyExpression = "*" + resultKeyExpression
		} else if spec.resultKeyAccess.valueField != "" {
			resultKeyExpression += "." + spec.resultKeyAccess.valueField
		}
	}
	fmt.Fprintf(out, "\t\t\tresultKey := any(%s)\n", resultKeyExpression)
	out.WriteString("\t\t\tif resultKey != nil && !reflect.ValueOf(resultKey).Comparable() {\n")
	fmt.Fprintf(
		out,
		"\t\t\t\tgroupErrors = append(groupErrors, fmt.Errorf(%q, groups[groupIndex].shard.Name(), resultIndex, resultKey))\n",
		"query "+query.methodName+" on replica set %q result %d has non-comparable lookup key type %T",
	)
	out.WriteString("\t\t\t\tcontinue\n")
	out.WriteString("\t\t\t}\n")
	out.WriteString("\t\t\tif _, requested := groups[groupIndex].requested[resultKey]; !requested {\n")
	fmt.Fprintf(
		out,
		"\t\t\t\tgroupErrors = append(groupErrors, fmt.Errorf(%q, groups[groupIndex].shard.Name(), resultIndex))\n",
		"query "+query.methodName+" on replica set %q result %d has an unrequested lookup key",
	)
	out.WriteString("\t\t\t\tcontinue\n")
	out.WriteString("\t\t\t}\n")
	out.WriteString("\t\t\trowsByKey[resultKey] = append(rowsByKey[resultKey], row)\n")
	out.WriteString("\t\t}\n")
	out.WriteString("\t}\n")
	fmt.Fprintf(out, "\t%s = errors.Join(groupErrors...)\n", errName)
	fmt.Fprintf(out, "\tif %s != nil { return %s }\n\n", errName, strings.Join(resultNames, ", "))
	out.WriteString("\tfor _, orderedItem := range orderedItems {\n")
	fmt.Fprintf(
		out,
		"\t\t%s = append(%s, rowsByGroup[orderedItem.shardName][orderedItem.key]...)\n",
		resultNames[0],
		resultNames[0],
	)
	out.WriteString("\t}\n")
	if spec.resultKeyAccess.nullable {
		fmt.Fprintf(
			out,
			"\t%s = append(%s, unkeyedRows...)\n",
			resultNames[0],
			resultNames[0],
		)
	}
	fmt.Fprintf(out, "\treturn %s\n", strings.Join(resultNames, ", "))
	out.WriteString("}\n\n")
}

func groupedManySQLCArgument(opts *options, query *generatedQuery, groupedArgs string) string {
	if query.arg.structType == nil {
		return groupedArgs
	}
	prefix := ""
	if query.arg.emitPointer {
		prefix = "&"
	}
	return fmt.Sprintf(
		"%s%s{%s: %s}",
		prefix,
		targetName(opts, query.arg.structType.name),
		query.groupedMany.parameterField,
		groupedArgs,
	)
}

func writeAllShardsQueryMethod(out *bytes.Buffer, query *generatedQuery) {
	receiverType := defaultGroupType
	store := defaultReceiverName + ".store"
	resultSignature, resultNames, errName := namedResultsSignature(
		query.storeParams,
		query.results,
		defaultReceiverName,
		"storeOptions",
	)
	fmt.Fprintf(out, "// %s executes the generated query on every physical shard.\n", query.methodName)
	fmt.Fprintf(
		out,
		"func (%s *%s[SK]) %s(%s)%s {\n",
		defaultReceiverName,
		receiverType,
		query.methodName,
		storeParamsSignature(query.storeParams),
		resultSignature,
	)
	writeInternalQuerySpanStart(out, store, query, errName)
	out.WriteString("\n")
	out.WriteString("\toptions := applyQueryOptions(storeOptions...)\n")
	fmt.Fprintf(out, "\tshards := %s.mesh.AllShards()\n", store)
	out.WriteString("\tif options.tx != nil {\n")
	out.WriteString("\t\tquerySpan.SetMultiRoute(pgmesh.RouteModeTransaction, len(shards))\n")
	fmt.Fprintf(out, "\t\t%s = pgmesh.ErrCrossShardTransaction\n", errName)
	fmt.Fprintf(out, "\t\treturn %s\n", strings.Join(resultNames, ", "))
	out.WriteString("\t}\n\n")

	if query.kind == queryKindRead {
		out.WriteString("\tmode := pgmesh.RouteModeRead\n")
		out.WriteString("\tif options.primary { mode = pgmesh.RouteModePrimary }\n")
	} else {
		out.WriteString("\tmode := pgmesh.RouteModePrimary\n")
	}
	out.WriteString("\tquerySpan.SetMultiRoute(mode, len(shards))\n\n")

	valueType := ""
	if len(query.results) > 1 {
		valueType = query.results[0]
	}
	out.WriteString("\ttype shardResult struct {\n")
	if valueType != "" {
		fmt.Fprintf(out, "\t\tvalue %s\n", valueType)
	}
	out.WriteString("\t\terr error\n")
	out.WriteString("\t}\n")
	out.WriteString("\tshardResults := make([]shardResult, len(shards))\n")
	out.WriteString("\tvar group sync.WaitGroup\n")
	out.WriteString("\tfor index, shard := range shards {\n")
	out.WriteString("\t\tgroup.Go(func() {\n")
	args := strings.Join(query.callArgs, ", ")
	call := func(target string) {
		out.WriteString(
			"\t\t\tqueryCtx, physicalQuerySpan := querySpan.StartQuerySpan(ctx, route.Metadata().WithoutVirtualShard(), mode)\n",
		)
		if valueType == "" {
			fmt.Fprintf(
				out,
				"\t\t\tshardResults[index].err = %s.%s(%s)\n",
				target,
				query.methodName,
				strings.Replace(args, "ctx", "queryCtx", 1),
			)
			out.WriteString("\t\t\tphysicalQuerySpan.End(shardResults[index].err)\n")
			return
		}
		fmt.Fprintf(
			out,
			"\t\t\tshardResults[index].value, shardResults[index].err = %s.%s(%s)\n",
			target,
			query.methodName,
			strings.Replace(args, "ctx", "queryCtx", 1),
		)
		out.WriteString("\t\t\tphysicalQuerySpan.End(shardResults[index].err)\n")
	}
	if query.kind == queryKindRead {
		out.WriteString("\t\t\tif options.primary {\n")
		out.WriteString("\t\t\t\troute := shard.WriteRoute()\n")
		out.WriteString(
			"\t\t\t\tqueryCtx, physicalQuerySpan := querySpan.StartQuerySpan(ctx, route.Metadata().WithoutVirtualShard(), mode)\n",
		)
		if valueType == "" {
			fmt.Fprintf(
				out,
				"\t\t\t\tshardResults[index].err = route.Target.%s(%s)\n",
				query.methodName,
				strings.Replace(args, "ctx", "queryCtx", 1),
			)
		} else {
			fmt.Fprintf(
				out,
				"\t\t\t\tshardResults[index].value, shardResults[index].err = route.Target.%s(%s)\n",
				query.methodName,
				strings.Replace(args, "ctx", "queryCtx", 1),
			)
		}
		out.WriteString("\t\t\t\tphysicalQuerySpan.End(shardResults[index].err)\n")
		out.WriteString("\t\t\t\treturn\n")
		out.WriteString("\t\t\t}\n")
		out.WriteString("\t\t\troute := shard.ReadRoute()\n")
		call("route.Target")
	} else {
		out.WriteString("\t\t\troute := shard.WriteRoute()\n")
		call("route.Target")
	}
	out.WriteString("\t\t})\n")
	out.WriteString("\t}\n")
	out.WriteString("\tgroup.Wait()\n\n")

	out.WriteString("\tshardErrors := make([]error, 0, len(shards))\n")
	out.WriteString("\tfor index, shardResult := range shardResults {\n")
	out.WriteString("\t\tif shardResult.err != nil {\n")
	fmt.Fprintf(
		out,
		"\t\t\tshardErrors = append(shardErrors, fmt.Errorf(%q, shards[index].Name(), shardResult.err))\n",
		"query "+query.methodName+" on replica set %q: %w",
	)
	out.WriteString("\t\t}\n")
	out.WriteString("\t}\n")
	fmt.Fprintf(out, "\t%s = errors.Join(shardErrors...)\n", errName)
	fmt.Fprintf(out, "\tif %s != nil { return %s }\n", errName, strings.Join(resultNames, ", "))

	if valueType != "" {
		switch query.command {
		case cmdMany:
			out.WriteString("\tfor _, shardResult := range shardResults {\n")
			fmt.Fprintf(out, "\t\t%s = append(%s, shardResult.value...)\n", resultNames[0], resultNames[0])
			out.WriteString("\t}\n")
		case cmdExecRows:
			out.WriteString("\tfor _, shardResult := range shardResults {\n")
			fmt.Fprintf(out, "\t\t%s += shardResult.value\n", resultNames[0])
			out.WriteString("\t}\n")
		}
	}
	fmt.Fprintf(out, "\treturn %s\n", strings.Join(resultNames, ", "))
	out.WriteString("}\n\n")
}

func writeGroupedCopyQueryMethod(out *bytes.Buffer, query *generatedQuery) {
	receiverType := defaultGroupType
	store := defaultReceiverName + ".store"
	resultSignature, resultNames, errName := namedResultsSignature(
		query.storeParams,
		query.results,
		defaultReceiverName,
		"storeOptions",
	)
	fmt.Fprintf(out, "// %s groups rows by physical shard and executes one copy per group.\n", query.methodName)
	fmt.Fprintf(
		out,
		"func (%s *%s[SK]) %s(%s)%s {\n",
		defaultReceiverName,
		receiverType,
		query.methodName,
		storeParamsSignature(query.storeParams),
		resultSignature,
	)
	writeInternalQuerySpanStart(out, store, query, errName)
	out.WriteString("\n")
	out.WriteString("\toptions := applyQueryOptions(storeOptions...)\n")
	out.WriteString("\ttype copyShardGroup struct {\n")
	fmt.Fprintf(
		out,
		"\t\tshard *pgmesh.Shard[*%s, *%s]\n",
		defaultReadType,
		defaultStoreType,
	)
	fmt.Fprintf(out, "\t\targs %s\n", query.params[1].typ)
	out.WriteString("\t}\n")
	out.WriteString("\tgroupsByName := make(map[string]*copyShardGroup)\n")
	inputName := query.storeParams[1].name
	fmt.Fprintf(out, "\tfor inputIndex, item := range %s {\n", inputName)
	out.WriteString("\t\tvar shardKey SK\n")
	fmt.Fprintf(out, "\t\tif %s.resolver != nil {\n", store)
	fmt.Fprintf(
		out,
		"\t\t\tshardKey = %s.resolver.%s(item.%s)\n",
		store,
		query.route.methodName,
		query.route.methodName,
	)
	out.WriteString("\t\t}\n")
	fmt.Fprintf(out, "\t\tshard, routeErr := %s.mesh.Shard(shardKey)\n", store)
	out.WriteString("\t\tif routeErr != nil {\n")
	fmt.Fprintf(
		out,
		"\t\t\t%s = fmt.Errorf(%q, inputIndex, routeErr)\n",
		errName,
		"route "+query.methodName+" input %d: %w",
	)
	fmt.Fprintf(out, "\t\t\treturn %s\n", strings.Join(resultNames, ", "))
	out.WriteString("\t\t}\n")
	out.WriteString("\t\tshardGroup := groupsByName[shard.Name()]\n")
	out.WriteString("\t\tif shardGroup == nil {\n")
	fmt.Fprintf(
		out,
		"\t\t\tshardGroup = &copyShardGroup{shard: shard, args: make(%s, 0)}\n",
		query.params[1].typ,
	)
	out.WriteString("\t\t\tgroupsByName[shard.Name()] = shardGroup\n")
	out.WriteString("\t\t}\n")
	switch {
	case query.shardArgs == nil:
		out.WriteString("\t\tshardGroup.args = append(shardGroup.args, item)\n")
	case !query.shardArgs.sqlcArg.isEmpty():
		out.WriteString("\t\tshardGroup.args = append(shardGroup.args, item.sqlcParams())\n")
	default:
		expression := strings.Replace(query.callArgs[1], "arg.", "item.", 1)
		fmt.Fprintf(out, "\t\tshardGroup.args = append(shardGroup.args, %s)\n", expression)
	}
	out.WriteString("\t}\n\n")

	out.WriteString("\tgroups := make([]*copyShardGroup, 0, len(groupsByName))\n")
	fmt.Fprintf(out, "\tfor _, shard := range %s.mesh.AllShards() {\n", store)
	out.WriteString("\t\tif shardGroup := groupsByName[shard.Name()]; shardGroup != nil {\n")
	out.WriteString("\t\t\tgroups = append(groups, shardGroup)\n")
	out.WriteString("\t\t}\n")
	out.WriteString("\t}\n")
	out.WriteString("\tif options.tx != nil && len(groups) > 1 {\n")
	out.WriteString("\t\tquerySpan.SetMultiRoute(pgmesh.RouteModeTransaction, len(groups))\n")
	fmt.Fprintf(out, "\t\t%s = pgmesh.ErrCrossShardTransaction\n", errName)
	fmt.Fprintf(out, "\t\treturn %s\n", strings.Join(resultNames, ", "))
	out.WriteString("\t}\n\n")

	out.WriteString("\tmode := pgmesh.RouteModePrimary\n")
	out.WriteString("\tif options.tx != nil {\n")
	out.WriteString("\t\tmode = pgmesh.RouteModeTransaction\n")
	out.WriteString("\t}\n")
	out.WriteString("\tquerySpan.SetMultiRoute(mode, len(groups))\n")
	out.WriteString("\tif len(groups) == 0 { return ")
	out.WriteString(strings.Join(resultNames, ", "))
	out.WriteString(" }\n\n")

	out.WriteString("\ttype copyResult struct {\n")
	out.WriteString("\t\tcount int64\n")
	out.WriteString("\t\terr error\n")
	out.WriteString("\t}\n")
	out.WriteString("\tcopyResults := make([]copyResult, len(groups))\n")
	out.WriteString("\tvar waitGroup sync.WaitGroup\n")
	out.WriteString("\tfor index, shardGroup := range groups {\n")
	out.WriteString("\t\twaitGroup.Go(func() {\n")
	out.WriteString("\t\t\troute := shardGroup.shard.WriteRoute()\n")
	out.WriteString("\t\t\ttarget := route.Target\n")
	out.WriteString("\t\t\tif options.tx != nil { target = target.WithTx(options.tx) }\n")
	out.WriteString("\t\t\tqueryCtx, physicalQuerySpan := querySpan.StartQuerySpan(ctx, route.Metadata().WithoutVirtualShard(), mode)\n")
	fmt.Fprintf(
		out,
		"\t\t\tcopyResults[index].count, copyResults[index].err = target.%s(queryCtx, shardGroup.args)\n",
		query.methodName,
	)
	out.WriteString("\t\t\tphysicalQuerySpan.End(copyResults[index].err)\n")
	out.WriteString("\t\t})\n")
	out.WriteString("\t}\n")
	out.WriteString("\twaitGroup.Wait()\n\n")
	out.WriteString("\tcopyErrors := make([]error, 0, len(groups))\n")
	out.WriteString("\tfor index, copyResult := range copyResults {\n")
	out.WriteString("\t\tif copyResult.err != nil {\n")
	fmt.Fprintf(
		out,
		"\t\t\tcopyErrors = append(copyErrors, fmt.Errorf(%q, groups[index].shard.Name(), copyResult.err))\n",
		"query "+query.methodName+" on replica set %q: %w",
	)
	out.WriteString("\t\t}\n")
	out.WriteString("\t}\n")
	fmt.Fprintf(out, "\t%s = errors.Join(copyErrors...)\n", errName)
	fmt.Fprintf(out, "\tif %s != nil { return %s }\n", errName, strings.Join(resultNames, ", "))
	out.WriteString("\tfor _, copyResult := range copyResults {\n")
	fmt.Fprintf(out, "\t\t%s += copyResult.count\n", resultNames[0])
	out.WriteString("\t}\n")
	fmt.Fprintf(out, "\treturn %s\n", strings.Join(resultNames, ", "))
	out.WriteString("}\n\n")
}

func writeAsyncCopyQueryMethods(out *bytes.Buffer, query *generatedQuery) {
	writeAsyncCopyEnqueueMethod(out, query)
	writeAsyncCopyFlushMethod(out, query)
}

func writeAsyncCopyEnqueueMethod(out *bytes.Buffer, query *generatedQuery) {
	enqueue := copyEnqueueMethodName(query.methodName)
	inputName := query.storeParams[1].name
	stateField := copyBatchStateFieldName(query.methodName)
	fmt.Fprintf(
		out,
		"// %s accepts rows for asynchronous COPY.\n",
		enqueue,
	)
	fmt.Fprintf(
		out,
		"func (%s *%s[SK]) %s(%s) *pgmesh.Future[int64] {\n",
		defaultReceiverName,
		defaultGroupType,
		enqueue,
		paramsSignature(query.storeParams),
	)
	fmt.Fprintf(
		out,
		"\tctx, querySpan := %s.store.mesh.StartSpan(ctx, %q, %q, %s)\n",
		defaultReceiverName,
		query.store,
		enqueue,
		queryKindConstant(query.kind),
	)
	out.WriteString("\tfinish := func(future *pgmesh.Future[int64]) *pgmesh.Future[int64] {\n")
	out.WriteString("\t\treturn pgmesh.RunFuture(func() (int64, error) {\n")
	out.WriteString("\t\t\tcount, err := future.Await(context.Background())\n")
	out.WriteString("\t\t\tquerySpan.End(err)\n")
	out.WriteString("\t\t\treturn count, err\n")
	out.WriteString("\t\t})\n")
	out.WriteString("\t}\n\n")

	out.WriteString("\ttype asyncCopyShardGroup struct {\n")
	fmt.Fprintf(
		out,
		"\t\tshard *pgmesh.Shard[*%s, *%s]\n",
		defaultReadType,
		defaultStoreType,
	)
	fmt.Fprintf(out, "\t\targs %s\n", query.params[1].typ)
	out.WriteString("\t\tbatcher *pgmesh.CopyBatcher[")
	out.WriteString(copyRowType(query))
	out.WriteString("]\n")
	out.WriteString("\t}\n")

	if query.shardMode == shardModeGroupedCopy {
		out.WriteString("\tgroupsByName := make(map[string]*asyncCopyShardGroup)\n")
		fmt.Fprintf(out, "\tfor inputIndex, item := range %s {\n", inputName)
		out.WriteString("\t\tvar shardKey SK\n")
		fmt.Fprintf(out, "\t\tif %s.store.resolver != nil {\n", defaultReceiverName)
		fmt.Fprintf(
			out,
			"\t\t\tshardKey = %s.store.resolver.%s(item.%s)\n",
			defaultReceiverName,
			query.route.methodName,
			query.route.methodName,
		)
		out.WriteString("\t\t}\n")
		fmt.Fprintf(out, "\t\tshard, routeErr := %s.store.mesh.Shard(shardKey)\n", defaultReceiverName)
		out.WriteString("\t\tif routeErr != nil {\n")
		fmt.Fprintf(
			out,
			"\t\t\terr := fmt.Errorf(%q, inputIndex, routeErr)\n",
			"route "+enqueue+" input %d: %w",
		)
		out.WriteString("\t\t\treturn finish(pgmesh.ResolvedFuture[int64](0, err))\n")
		out.WriteString("\t\t}\n")
		out.WriteString("\t\tshardGroup := groupsByName[shard.Name()]\n")
		out.WriteString("\t\tif shardGroup == nil {\n")
		fmt.Fprintf(
			out,
			"\t\t\tshardGroup = &asyncCopyShardGroup{shard: shard, args: make(%s, 0)}\n",
			query.params[1].typ,
		)
		out.WriteString("\t\t\tgroupsByName[shard.Name()] = shardGroup\n")
		out.WriteString("\t\t}\n")
		writeCopyGroupAppend(out, query, "shardGroup.args", "item", "\t\t")
		out.WriteString("\t}\n")
		out.WriteString("\tgroups := make([]*asyncCopyShardGroup, 0, len(groupsByName))\n")
		fmt.Fprintf(out, "\tfor _, shard := range %s.store.mesh.AllShards() {\n", defaultReceiverName)
		out.WriteString("\t\tif shardGroup := groupsByName[shard.Name()]; shardGroup != nil {\n")
		out.WriteString("\t\t\tgroups = append(groups, shardGroup)\n")
		out.WriteString("\t\t}\n")
		out.WriteString("\t}\n")
		out.WriteString("\tquerySpan.SetMultiRoute(pgmesh.RouteModePrimary, len(groups))\n")
	} else {
		out.WriteString("\tvar shardKey SK\n")
		fmt.Fprintf(out, "\tshard, routeErr := %s.store.mesh.Shard(shardKey)\n", defaultReceiverName)
		out.WriteString("\tif routeErr != nil {\n")
		out.WriteString("\t\treturn finish(pgmesh.ResolvedFuture[int64](0, routeErr))\n")
		out.WriteString("\t}\n")
		fmt.Fprintf(
			out,
			"\tgroups := []*asyncCopyShardGroup{{shard: shard, args: %s}}\n",
			inputName,
		)
		out.WriteString("\tquerySpan.SetRoute(pgmesh.RouteModePrimary)\n")
	}

	out.WriteString("\tif len(groups) == 0 {\n")
	out.WriteString("\t\treturn finish(pgmesh.ResolvedFuture[int64](0, nil))\n")
	out.WriteString("\t}\n")
	fmt.Fprintf(out, "\tstate := %s.store.%s\n", defaultReceiverName, stateField)
	out.WriteString("\tstate.mu.Lock()\n")
	out.WriteString("\tif err := ctx.Err(); err != nil {\n")
	out.WriteString("\t\tstate.mu.Unlock()\n")
	out.WriteString("\t\treturn finish(pgmesh.ResolvedFuture[int64](0, err))\n")
	out.WriteString("\t}\n")
	out.WriteString("\tfor _, shardGroup := range groups {\n")
	out.WriteString("\t\tshardGroup.batcher = state.batchers[shardGroup.shard.Name()]\n")
	out.WriteString("\t\tif shardGroup.batcher == nil {\n")
	out.WriteString("\t\t\tstate.mu.Unlock()\n")
	fmt.Fprintf(
		out,
		"\t\t\terr := fmt.Errorf(%q, shardGroup.shard.Name())\n",
		"query "+enqueue+" has no copy batcher for replica set %q",
	)
	out.WriteString("\t\t\treturn finish(pgmesh.ResolvedFuture[int64](0, err))\n")
	out.WriteString("\t\t}\n")
	out.WriteString("\t}\n")
	out.WriteString("\tacceptedContext := context.WithoutCancel(ctx)\n")
	out.WriteString("\ttype asyncCopyResult struct {\n")
	out.WriteString("\t\tshardName string\n")
	out.WriteString("\t\tfuture *pgmesh.Future[int64]\n")
	out.WriteString("\t}\n")
	out.WriteString("\tasyncResults := make([]asyncCopyResult, 0, len(groups))\n")
	out.WriteString("\tfor _, shardGroup := range groups {\n")
	out.WriteString("\t\tvar future *pgmesh.Future[int64]\n")
	out.WriteString("\t\tif state.enabled {\n")
	out.WriteString("\t\t\tfuture = shardGroup.batcher.Submit(acceptedContext, shardGroup.args)\n")
	out.WriteString("\t\t} else {\n")
	out.WriteString("\t\t\tfuture = shardGroup.batcher.SubmitImmediate(acceptedContext, shardGroup.args)\n")
	out.WriteString("\t\t}\n")
	out.WriteString("\t\tasyncResults = append(asyncResults, asyncCopyResult{shardName: shardGroup.shard.Name(), future: future})\n")
	out.WriteString("\t}\n")
	out.WriteString("\tstate.mu.Unlock()\n")
	out.WriteString("\n\tcombined := pgmesh.RunFuture(func() (int64, error) {\n")
	out.WriteString("\t\tvar count int64\n")
	out.WriteString("\t\tcopyErrors := make([]error, 0, len(asyncResults))\n")
	out.WriteString("\t\tfor _, asyncResult := range asyncResults {\n")
	out.WriteString("\t\t\tshardCount, err := asyncResult.future.Await(context.Background())\n")
	out.WriteString("\t\t\tif err != nil {\n")
	fmt.Fprintf(
		out,
		"\t\t\t\tcopyErrors = append(copyErrors, fmt.Errorf(%q, asyncResult.shardName, err))\n",
		"query "+query.methodName+" on replica set %q: %w",
	)
	out.WriteString("\t\t\t\tcontinue\n")
	out.WriteString("\t\t\t}\n")
	out.WriteString("\t\t\tcount += shardCount\n")
	out.WriteString("\t\t}\n")
	out.WriteString("\t\tif err := errors.Join(copyErrors...); err != nil { return 0, err }\n")
	out.WriteString("\t\treturn count, nil\n")
	out.WriteString("\t})\n")
	out.WriteString("\treturn finish(combined)\n")
	out.WriteString("}\n\n")
}

func writeCopyGroupAppend(
	out *bytes.Buffer,
	query *generatedQuery,
	target string,
	item string,
	indent string,
) {
	switch {
	case query.shardArgs == nil:
		fmt.Fprintf(out, "%s%s = append(%s, %s)\n", indent, target, target, item)
	case !query.shardArgs.sqlcArg.isEmpty():
		fmt.Fprintf(out, "%s%s = append(%s, %s.sqlcParams())\n", indent, target, target, item)
	default:
		expression := strings.Replace(query.callArgs[1], "arg.", item+".", 1)
		fmt.Fprintf(out, "%s%s = append(%s, %s)\n", indent, target, target, expression)
	}
}

func writeAsyncCopyFlushMethod(out *bytes.Buffer, query *generatedQuery) {
	flush := copyFlushMethodName(query.methodName)
	stateField := copyBatchStateFieldName(query.methodName)
	fmt.Fprintf(
		out,
		"// %s drains asynchronous submissions accepted before its barrier.\n",
		flush,
	)
	fmt.Fprintf(
		out,
		"func (%s *%s[SK]) %s(ctx context.Context) error {\n",
		defaultReceiverName,
		defaultGroupType,
		flush,
	)
	fmt.Fprintf(out, "\tstate := %s.store.%s\n", defaultReceiverName, stateField)
	out.WriteString("\tstate.mu.Lock()\n")
	out.WriteString("\ttype copyFlushResult struct {\n")
	out.WriteString("\t\tshardName string\n")
	out.WriteString("\t\tfuture *pgmesh.Future[struct{}]\n")
	out.WriteString("\t}\n")
	fmt.Fprintf(
		out,
		"\tflushes := make([]copyFlushResult, 0, len(%s.store.mesh.AllShards()))\n",
		defaultReceiverName,
	)
	fmt.Fprintf(out, "\tfor _, shard := range %s.store.mesh.AllShards() {\n", defaultReceiverName)
	out.WriteString("\t\tif batcher := state.batchers[shard.Name()]; batcher != nil {\n")
	out.WriteString("\t\t\tflushes = append(flushes, copyFlushResult{shardName: shard.Name(), future: batcher.FlushAsync()})\n")
	out.WriteString("\t\t}\n")
	out.WriteString("\t}\n")
	out.WriteString("\tstate.mu.Unlock()\n")
	out.WriteString("\tflushErrors := make([]error, 0, len(flushes))\n")
	out.WriteString("\tfor _, flushResult := range flushes {\n")
	out.WriteString("\t\tif _, err := flushResult.future.Await(ctx); err != nil {\n")
	fmt.Fprintf(
		out,
		"\t\t\tflushErrors = append(flushErrors, fmt.Errorf(%q, flushResult.shardName, err))\n",
		"flush "+query.methodName+" on replica set %q: %w",
	)
	out.WriteString("\t\t\tif ctx.Err() != nil { break }\n")
	out.WriteString("\t\t}\n")
	out.WriteString("\t}\n")
	out.WriteString("\treturn errors.Join(flushErrors...)\n")
	out.WriteString("}\n\n")
}

func writeShardedStore(
	out *bytes.Buffer,
	opts *options,
	groups []storeGroup,
) {
	hasCopyQueries := false
	for groupIndex := range groups {
		if len(copyQueries(groups[groupIndex].queries)) > 0 {
			hasCopyQueries = true
			break
		}
	}
	out.WriteString("type shardDatabase struct {\n")
	out.WriteString("\tname string\n")
	fmt.Fprintf(out, "\tprimary %s\n", targetName(opts, "DBTX"))
	fmt.Fprintf(out, "\treplicas []%s\n", targetName(opts, "DBTX"))
	out.WriteString("}\n\n")

	out.WriteString("type shardMapping struct {\n")
	out.WriteString("\tmainReplicaSet string\n")
	out.WriteString("\tvshards []uint64\n")
	out.WriteString("\tmirrorReplicaSets []string\n")
	out.WriteString("}\n\n")

	out.WriteString("type shardedOptions struct {\n")
	out.WriteString("\treplicaSets []shardDatabase\n")
	out.WriteString("\tmappings []shardMapping\n")
	out.WriteString("}\n\n")

	out.WriteString("// ShardedOption customizes a sharded topology.\n")
	out.WriteString("type ShardedOption func(*shardedOptions)\n\n")
	out.WriteString("// WithReplicaSet appends a named primary and its optional read replicas.\n")
	fmt.Fprintf(
		out,
		"func WithReplicaSet(name string, primary %s, replicas ...%s) ShardedOption {\n",
		targetName(opts, "DBTX"),
		targetName(opts, "DBTX"),
	)
	fmt.Fprintf(out, "\tconfiguredReplicas := append([]%s(nil), replicas...)\n", targetName(opts, "DBTX"))
	out.WriteString("\treturn func(options *shardedOptions) {\n")
	out.WriteString("\t\toptions.replicaSets = append(options.replicaSets, shardDatabase{\n")
	out.WriteString("\t\t\tname: name, primary: primary, replicas: append([]")
	fmt.Fprintf(out, "%s(nil), configuredReplicas...),\n", targetName(opts, "DBTX"))
	out.WriteString("\t\t})\n")
	out.WriteString("\t}\n")
	out.WriteString("}\n\n")
	out.WriteString("// WithVShardMapping maps virtual shards to a main replica set and optional ordered write mirrors.\n")
	out.WriteString("func WithVShardMapping(mainReplicaSet string, vshards []uint64, mirrorReplicaSets ...string) ShardedOption {\n")
	out.WriteString("\tconfiguredVShards := append([]uint64(nil), vshards...)\n")
	out.WriteString("\tconfiguredMirrors := append([]string(nil), mirrorReplicaSets...)\n")
	out.WriteString("\treturn func(options *shardedOptions) {\n")
	out.WriteString("\t\toptions.mappings = append(options.mappings, shardMapping{\n")
	out.WriteString("\t\t\tmainReplicaSet: mainReplicaSet,\n")
	out.WriteString("\t\t\tvshards: append([]uint64(nil), configuredVShards...),\n")
	out.WriteString("\t\t\tmirrorReplicaSets: append([]string(nil), configuredMirrors...),\n")
	out.WriteString("\t\t})\n")
	out.WriteString("\t}\n")
	out.WriteString("}\n\n")

	fmt.Fprintf(out, "type shardedTopology[SK any] struct {\n")
	out.WriteString("\tnumVShards uint64\n")
	out.WriteString("\tshardHasher pgmesh.ShardHasher[SK]\n")
	fmt.Fprintf(out, "\tresolver %s[SK]\n", opts.ResolverInterfaceName)
	out.WriteString("\toptions shardedOptions\n")
	out.WriteString("\terr error\n")
	out.WriteString("}\n\n")
	fmt.Fprintf(out, "// %s returns an opaque sharded topology.\n", opts.ShardedConstructor)
	fmt.Fprintf(
		out,
		"func %s[SK any](numVShards uint64, shardHasher pgmesh.ShardHasher[SK], resolver %s[SK], options ...ShardedOption) Topology {\n",
		opts.ShardedConstructor,
		opts.ResolverInterfaceName,
	)
	out.WriteString("\tvar configured shardedOptions\n")
	out.WriteString("\tfor index, option := range options {\n")
	out.WriteString("\t\tif option == nil {\n")
	out.WriteString("\t\t\treturn shardedTopology[SK]{err: fmt.Errorf(\"pgmesh: sharded option %d is nil\", index)}\n")
	out.WriteString("\t\t}\n")
	out.WriteString("\t\toption(&configured)\n")
	out.WriteString("\t}\n")
	out.WriteString("\treturn shardedTopology[SK]{\n")
	out.WriteString("\t\tnumVShards: numVShards,\n")
	out.WriteString("\t\tshardHasher: shardHasher,\n")
	out.WriteString("\t\tresolver: resolver,\n")
	out.WriteString("\t\toptions: configured,\n")
	out.WriteString("\t}\n")
	out.WriteString("}\n\n")

	fmt.Fprintf(
		out,
		"func (c shardedTopology[SK]) buildStore(ctx context.Context, options storeOptions) (%s, error) {\n",
		opts.StoreInterfaceName,
	)
	out.WriteString("\tif c.err != nil { return nil, c.err }\n")
	out.WriteString("\tif c.resolver == nil {\n")
	out.WriteString("\t\treturn nil, fmt.Errorf(\"pgmesh: shard resolver is nil\")\n")
	out.WriteString("\t}\n")
	fmt.Fprintf(out, "\tnodes := make(map[string]%s)\n", targetName(opts, "DBTX"))
	out.WriteString("\tmeshOptions := make([]pgmesh.MeshOption, 0, len(c.options.replicaSets)+len(c.options.mappings)+3)\n")
	out.WriteString("\tfor setIndex, set := range c.options.replicaSets {\n")
	out.WriteString("\t\tprimaryDSN := fmt.Sprintf(\"pgmesh-internal://%d/primary\", setIndex)\n")
	out.WriteString("\t\tnodes[primaryDSN] = set.primary\n")
	out.WriteString("\t\treplicaDSNs := make([]string, 0, len(set.replicas))\n")
	out.WriteString("\t\tfor replicaIndex, database := range set.replicas {\n")
	out.WriteString("\t\t\tdsn := fmt.Sprintf(\"pgmesh-internal://%d/replica/%d\", setIndex, replicaIndex)\n")
	out.WriteString("\t\t\tnodes[dsn] = database\n")
	out.WriteString("\t\t\treplicaDSNs = append(replicaDSNs, dsn)\n")
	out.WriteString("\t\t}\n")
	out.WriteString("\t\tmeshOptions = append(meshOptions, pgmesh.WithReplicaSet(set.name, primaryDSN, replicaDSNs...))\n")
	out.WriteString("\t}\n")
	out.WriteString("\tfor _, mapping := range c.options.mappings {\n")
	out.WriteString("\t\tmeshOptions = append(meshOptions, pgmesh.WithVShardMapping(\n")
	out.WriteString("\t\t\tmapping.mainReplicaSet,\n")
	out.WriteString("\t\t\tmapping.vshards,\n")
	out.WriteString("\t\t\tmapping.mirrorReplicaSets...,\n")
	out.WriteString("\t\t))\n")
	out.WriteString("\t}\n")
	out.WriteString("\tmeshOptions = append(meshOptions,\n")
	out.WriteString("\t\tpgmesh.WithTracerProvider(options.tracerProvider),\n")
	out.WriteString("\t\tpgmesh.WithMeterProvider(options.meterProvider),\n")
	out.WriteString("\t\tpgmesh.WithLogger(options.logger),\n")
	out.WriteString("\t)\n")
	out.WriteString("\tmesh, err := pgmesh.CreateMesh(ctx, c.numVShards,\n")
	fmt.Fprintf(
		out,
		"\t\tfunc(_ context.Context, dsn string) (pgmesh.Node[*%s, *%s], error) {\n",
		defaultReadType,
		defaultStoreType,
	)
	out.WriteString("\t\t\tdatabase, ok := nodes[dsn]\n")
	out.WriteString("\t\t\tif !ok || database == nil {\n")
	fmt.Fprintf(
		out,
		"\t\t\t\treturn pgmesh.Node[*%s, *%s]{}, fmt.Errorf(\"pgmesh: database node %%q is nil\", dsn)\n",
		defaultReadType,
		defaultStoreType,
	)
	out.WriteString("\t\t\t}\n")
	fmt.Fprintf(out, "\t\t\treturn %s(database), nil\n", defaultNodeNew)
	out.WriteString("\t\t},\n")
	out.WriteString("\t\tc.shardHasher,\n")
	out.WriteString("\t\tmeshOptions...,\n")
	out.WriteString("\t)\n")
	out.WriteString("\tif err != nil { return nil, err }\n")
	if len(groups) > 0 {
		fmt.Fprintf(
			out,
			"\tstore := &%s[SK]{mesh: mesh, resolver: c.resolver}\n",
			defaultMeshStoreType,
		)
		if hasCopyQueries {
			out.WriteString("\tif err := store.initializeCopyBatchers(options); err != nil { return nil, err }\n")
		}
		out.WriteString("\tstore.initializeGroups(options)\n")
		out.WriteString("\treturn store, nil\n")
	} else {
		fmt.Fprintf(
			out,
			"\treturn &%s[SK]{mesh: mesh, resolver: c.resolver}, nil\n",
			defaultMeshStoreType,
		)
	}
	out.WriteString("}\n\n")
}

func writeShardKeyTypes(out *bytes.Buffer, opts *options, routes []shardRoute) {
	for _, route := range routes {
		fmt.Fprintf(
			out,
			"// %s is the shared shard key for the %q route.\n",
			route.methodName,
			route.name,
		)
		fmt.Fprintf(out, "type %s struct {\n", route.methodName)
		for _, operand := range route.operands {
			writeStructField(
				out,
				opts,
				operand.fieldName,
				exportedSQLCType(opts, operand.typ),
				operand.dbName,
			)
		}
		out.WriteString("}\n\n")
	}
}

func writeStructField(
	out *bytes.Buffer,
	opts *options,
	name string,
	typ string,
	dbName string,
) {
	fmt.Fprintf(out, "\t%s %s", name, typ)
	if opts.EmitJSONTags {
		fmt.Fprintf(out, " `json:%q`", dbName)
	}
	out.WriteByte('\n')
}

func writeShardResolverInterface(out *bytes.Buffer, opts *options, routes []shardRoute) {
	fmt.Fprintf(out, "// %s resolves generated query parameters to shard keys.\n", opts.ResolverInterfaceName)
	fmt.Fprintf(out, "type %s[SK any] interface {\n", opts.ResolverInterfaceName)
	for _, route := range routes {
		fmt.Fprintf(out, "\t// %s resolves the %q shard route.\n", route.methodName, route.name)
		fmt.Fprintf(out, "\t%s(key %s) SK\n", route.methodName, route.methodName)
	}
	out.WriteString("}\n\n")
}

func writeQueryMethods(
	out *bytes.Buffer,
	opts *options,
	receiverType string,
	queries []generatedQuery,
	kind queryKind,
	mirror bool,
) {
	for idx := range queries {
		query := &queries[idx]
		if query.kind != kind {
			continue
		}
		fmt.Fprintf(out, "// %s executes the generated %s query.\n", query.methodName, query.methodName)
		fmt.Fprintf(out, "func (%s *%s) %s(%s)%s {\n",
			defaultReceiverName,
			receiverType,
			query.methodName,
			paramsSignature(query.params),
			resultsSignature(query.results),
		)
		writeQueryMethodBody(out, opts, query, mirror)
		out.WriteString("}\n\n")
	}
}

func writeQueryMethodBody(out *bytes.Buffer, opts *options, query *generatedQuery, mirror bool) {
	args := callArguments(query.params)
	if !lastResultIsError(query.results) {
		fmt.Fprintf(out, "\treturn %s.main.%s(%s)\n", defaultReceiverName, query.methodName, args)
		return
	}

	nonErrorResults := query.results[:len(query.results)-1]
	if len(nonErrorResults) == 0 {
		fmt.Fprintf(out, "\tif err := %s.main.%s(%s); err != nil {\n", defaultReceiverName, query.methodName, args)
		out.WriteString("\t\treturn err\n")
		out.WriteString("\t}\n")
		if !mirror {
			out.WriteString("\treturn nil\n")
			return
		}
		if opts.IgnoreMirrorError {
			fmt.Fprintf(
				out,
				"\t_ = %s.mirror(func(%s *%s) error {\n",
				defaultReceiverName,
				mirrorReceiverName,
				targetName(opts, defaultTargetType),
			)
			fmt.Fprintf(out, "\t\treturn %s.%s(%s)\n", mirrorReceiverName, query.methodName, args)
			out.WriteString("\t})\n")
			out.WriteString("\treturn nil\n")
			return
		}
		fmt.Fprintf(
			out,
			"\treturn %s.mirror(func(%s *%s) error {\n",
			defaultReceiverName,
			mirrorReceiverName,
			targetName(opts, defaultTargetType),
		)
		fmt.Fprintf(out, "\t\treturn %s.%s(%s)\n", mirrorReceiverName, query.methodName, args)
		out.WriteString("\t})\n")
		return
	}

	resultVars := resultVariables(len(nonErrorResults))
	fmt.Fprintf(out, "\t%s := %s.main.%s(%s)\n",
		strings.Join(appendResultError(resultVars), ", "),
		defaultReceiverName,
		query.methodName,
		args,
	)
	out.WriteString("\tif err != nil {\n")
	for idx, result := range nonErrorResults {
		fmt.Fprintf(out, "\t\tvar zero%d %s\n", idx, result)
	}
	out.WriteString("\t\treturn ")
	for idx := range nonErrorResults {
		if idx > 0 {
			out.WriteString(", ")
		}
		fmt.Fprintf(out, "zero%d", idx)
	}
	out.WriteString(", err\n")
	out.WriteString("\t}\n")

	if !mirror {
		fmt.Fprintf(out, "\treturn %s, nil\n", strings.Join(resultVars, ", "))
		return
	}
	if opts.IgnoreMirrorError {
		fmt.Fprintf(
			out,
			"\t_ = %s.mirror(func(%s *%s) error {\n",
			defaultReceiverName,
			mirrorReceiverName,
			targetName(opts, defaultTargetType),
		)
		fmt.Fprintf(out, "\t\t%s := %s.%s(%s)\n",
			strings.Join(discardVariables(len(nonErrorResults)+1), ", "),
			mirrorReceiverName,
			query.methodName,
			args,
		)
		out.WriteString("\t\treturn err\n")
		out.WriteString("\t})\n")
		fmt.Fprintf(out, "\treturn %s, nil\n", strings.Join(resultVars, ", "))
		return
	}

	fmt.Fprintf(
		out,
		"\t%s := %s.mirror(func(%s *%s) error {\n",
		mirrorErrorName,
		defaultReceiverName,
		mirrorReceiverName,
		targetName(opts, defaultTargetType),
	)
	fmt.Fprintf(out, "\t\t%s := %s.%s(%s)\n",
		strings.Join(discardVariables(len(nonErrorResults)+1), ", "),
		mirrorReceiverName,
		query.methodName,
		args,
	)
	out.WriteString("\t\treturn err\n")
	out.WriteString("\t})\n")
	fmt.Fprintf(out, "\treturn %s, %s\n", strings.Join(resultVars, ", "), mirrorErrorName)
}

func callArguments(params []argument) string {
	args := make([]string, 0, len(params))
	for _, param := range params {
		args = append(args, param.name)
	}
	return strings.Join(args, ", ")
}

func lastResultIsError(results []string) bool {
	return len(results) > 0 && results[len(results)-1] == resultErrorName
}

func resultVariables(count int) []string {
	vars := make([]string, count)
	for idx := range vars {
		vars[idx] = fmt.Sprintf("rv%d", idx)
	}
	return vars
}

func appendResultError(vars []string) []string {
	out := make([]string, 0, len(vars)+1)
	out = append(out, vars...)
	out = append(out, "err")
	return out
}

func discardVariables(count int) []string {
	vars := make([]string, count)
	for idx := range count - 1 {
		vars[idx] = "_"
	}
	vars[count-1] = "err"
	return vars
}

func writeSplitInterface(out *bytes.Buffer, name string, queries []generatedQuery, kind queryKind) {
	fmt.Fprintf(out, "// %s exposes generated %s queries.\n", name, kind)
	fmt.Fprintf(out, "type %s interface {\n", name)
	for _, query := range queries {
		if query.kind != kind {
			continue
		}
		fmt.Fprintf(out, "\t// %s executes the generated %s query.\n", query.methodName, query.methodName)
		fmt.Fprintf(out, "\t%s(%s)%s\n", query.methodName, paramsSignature(query.params), resultsSignature(query.results))
	}
	out.WriteString("}\n\n")
}

func targetName(opts *options, name string) string {
	if opts.InternalImportPath == "" {
		return name
	}
	return internalQualifier(opts) + "." + name
}

func internalQualifier(opts *options) string {
	if opts.InternalImportAlias != "" {
		return opts.InternalImportAlias
	}
	return "internal"
}

func importAlias(alias string) string {
	if alias == "" || alias == "internal" {
		return ""
	}
	return alias
}

func writeImports(out *bytes.Buffer, imports []importSpec) {
	if len(imports) == 0 {
		return
	}
	out.WriteString("import (\n")
	for _, imp := range imports {
		if imp.name != "" {
			fmt.Fprintf(out, "\t%s %q\n", imp.name, imp.path)
			continue
		}
		fmt.Fprintf(out, "\t%q\n", imp.path)
	}
	out.WriteString(")\n\n")
}
