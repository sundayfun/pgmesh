package sqlcplugin

import (
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/jinzhu/inflection"
	"github.com/sqlc-dev/plugin-sdk-go/plugin"
)

type typeResolver struct {
	req     *plugin.GenerateRequest
	opts    *options
	imports *importSet
}

func querySpecs(req *plugin.GenerateRequest, opts *options) ([]generatedQuery, *importSet, error) {
	resolver := &typeResolver{
		req:     req,
		opts:    opts,
		imports: newImportSet(),
	}
	structs := buildStructs(req, opts, resolver)
	queries, err := buildQueries(req, opts, resolver, structs)
	if err != nil {
		return nil, nil, err
	}
	for _, query := range queries {
		if query.shardArgs == nil {
			continue
		}
		for _, field := range query.shardArgs.fields {
			resolver.addImportsForType(field.typ)
		}
	}
	return queries, resolver.imports, nil
}

type generatedQuery struct {
	methodName      string
	command         string
	kind            queryKind
	arg             queryValue
	ret             queryValue
	params          []argument
	storeParams     []argument
	callArgs        []string
	results         []string
	route           *shardRoute
	shardMode       shardMode
	store           string
	storeParamAlias *storeParamAlias
	shardArgs       *shardArgWrapper
	groupedMany     *groupedManySpec
}

type storeParamAlias struct {
	name   string
	target string
}

type storeGroup struct {
	name    string
	queries []generatedQuery
}

type shardRoute struct {
	name       string
	methodName string
	operands   []routeOperand
}

type routeAnnotation struct {
	name     string
	operands []string
}

type routeOperand struct {
	name       string
	typ        string
	expression string
	dbName     string
	fieldName  string
	external   bool
}

type shardArgWrapper struct {
	name    string
	fields  []argument
	sqlcArg queryValue
}

type groupedManySpec struct {
	parameterDBName string
	parameterField  string
	lookupDBName    string
	lookupField     string
	elementType     string
	resultKeyField  string
	resultIsStruct  bool
	resultKeyAccess groupedManyResultKeyAccess
	itemIsPointer   bool
}

type groupedManyResultKeyAccess struct {
	nullable   bool
	pointer    bool
	valid      bool
	valueField string
}

type shardMode uint8

const (
	shardModeNone shardMode = iota
	shardModeRouted
	shardModeAll
	shardModeGroupedCopy
	shardModeGroupedMany
)

const (
	shardAnnotationPrefix = "shard:"
	storeAnnotationPrefix = "store:"
	allShardsRouteName    = "all"
)

func collectShardRoutes(queries []generatedQuery) ([]shardRoute, error) {
	byName := make(map[string]shardRoute)
	for _, query := range queries {
		if query.route == nil {
			continue
		}
		if previous, ok := byName[query.route.methodName]; ok {
			if previous.name != query.route.name {
				return nil, fmt.Errorf(
					"shard routes %s and %s generate the same resolver method %s",
					previous.name, query.route.name, query.route.methodName,
				)
			}
			if !sameRouteSignature(previous, *query.route) {
				return nil, fmt.Errorf(
					"shard route %s is reused with incompatible parameter types",
					query.route.name,
				)
			}
			continue
		}
		byName[query.route.methodName] = *query.route
	}
	routes := make([]shardRoute, 0, len(byName))
	for _, route := range byName {
		routes = append(routes, route)
	}
	sort.Slice(routes, func(i, j int) bool { return routes[i].methodName < routes[j].methodName })
	return routes, nil
}

func collectStoreGroups(queries []generatedQuery, opts *options) ([]storeGroup, error) {
	byName := make(map[string][]generatedQuery)
	for _, query := range queries {
		if query.store == "" {
			continue
		}
		byName[query.store] = append(byName[query.store], query)
	}

	groups := make([]storeGroup, 0, len(byName))
	for name, groupedQueries := range byName {
		groups = append(groups, storeGroup{name: name, queries: groupedQueries})
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].name < groups[j].name })

	declarations := generatedDeclarations(opts)

	for _, group := range groups {
		for _, declaration := range []string{
			group.name,
			storeReaderInterfaceName(group.name),
			storeWriterInterfaceName(group.name),
			storeFactoryOptionName(group.name),
		} {
			if previous, exists := declarations[declaration]; exists {
				return nil, fmt.Errorf(
					"store group %s generates declaration %s that conflicts with %s",
					group.name,
					declaration,
					previous,
				)
			}
			declarations[declaration] = "store group " + group.name
		}
	}
	for _, query := range queries {
		if query.storeParamAlias != nil {
			if previous, exists := declarations[query.storeParamAlias.name]; exists {
				return nil, fmt.Errorf(
					"query %s generates store parameter alias %s that conflicts with %s",
					query.methodName,
					query.storeParamAlias.name,
					previous,
				)
			}
			declarations[query.storeParamAlias.name] = "store parameter alias for " + query.methodName
		}
		if query.shardArgs == nil {
			continue
		}
		if previous, exists := declarations[query.shardArgs.name]; exists {
			return nil, fmt.Errorf(
				"query %s generates shard parameter wrapper %s that conflicts with %s",
				query.methodName,
				query.shardArgs.name,
				previous,
			)
		}
		declarations[query.shardArgs.name] = "shard parameter wrapper for " + query.methodName
	}

	return groups, nil
}

func storeReaderInterfaceName(group string) string {
	return group + "Reader"
}

func storeWriterInterfaceName(group string) string {
	return group + "Writer"
}

func storeFactoryOptionName(group string) string {
	return "With" + group + "Factory"
}

func storeTelemetryTypeName(group string) string {
	return "telemetry" + group + "Store"
}

func sameRouteSignature(left, right shardRoute) bool {
	if len(left.operands) != len(right.operands) {
		return false
	}
	for index := range left.operands {
		if left.operands[index].typ != right.operands[index].typ {
			return false
		}
	}
	return true
}

type queryKind string

const (
	queryKindRead  queryKind = "read"
	queryKindWrite queryKind = "write"
)

type argument struct {
	name string
	typ  string
}

func argumentNames(params []argument) []string {
	names := make([]string, 0, len(params))
	for _, param := range params {
		names = append(names, param.name)
	}
	return names
}

func paramsSignature(params []argument) string {
	parts := make([]string, 0, len(params))
	for _, param := range params {
		parts = append(parts, param.name+" "+param.typ)
	}
	return strings.Join(parts, ", ")
}

func storeParamsSignature(params []argument) string {
	signature := paramsSignature(params)
	if signature != "" {
		signature += ", "
	}
	return signature + "storeOptions ...QueryOption"
}

func queryKindConstant(kind queryKind) string {
	if kind == queryKindWrite {
		return "pgmesh.QueryKindWrite"
	}
	return "pgmesh.QueryKindRead"
}

func resultsSignature(results []string) string {
	switch len(results) {
	case 0:
		return ""
	case 1:
		return " " + results[0]
	default:
		return " (" + strings.Join(results, ", ") + ")"
	}
}

func namedResultsSignature(
	params []argument,
	results []string,
	reservedNames ...string,
) (string, []string, string) {
	usedNames := make(map[string]struct{}, len(params)+len(results)+len(reservedNames))
	for _, param := range params {
		usedNames[param.name] = struct{}{}
	}
	for _, name := range reservedNames {
		usedNames[name] = struct{}{}
	}

	names := make([]string, len(results))
	for index := range results {
		preferredName := "result"
		if len(results) > 2 {
			preferredName = fmt.Sprintf("result%d", index)
		}
		if index == len(results)-1 && results[index] == resultErrorName {
			preferredName = "err"
		}
		names[index] = uniqueGeneratedName(preferredName, usedNames)
		usedNames[names[index]] = struct{}{}
	}

	parts := make([]string, len(results))
	for index, result := range results {
		parts[index] = names[index] + " " + result
	}

	var signature string
	if len(parts) > 0 {
		signature = " (" + strings.Join(parts, ", ") + ")"
	}

	errName := ""
	if len(results) > 0 && results[len(results)-1] == resultErrorName {
		errName = names[len(names)-1]
	}
	return signature, names, errName
}

func uniqueGeneratedName(preferred string, usedNames map[string]struct{}) string {
	if _, exists := usedNames[preferred]; !exists {
		return preferred
	}
	for suffix := 2; ; suffix++ {
		candidate := fmt.Sprintf("%s%d", preferred, suffix)
		if _, exists := usedNames[candidate]; !exists {
			return candidate
		}
	}
}

type queryValue struct {
	emit        bool           `exhaustruct:"optional"`
	emitPointer bool           `exhaustruct:"optional"`
	name        string         `exhaustruct:"optional"`
	dbName      string         `exhaustruct:"optional"`
	structType  *goStruct      `exhaustruct:"optional"`
	typ         string         `exhaustruct:"optional"`
	column      *plugin.Column `exhaustruct:"optional"`
}

func (v queryValue) isEmpty() bool {
	return v.typ == "" && v.name == "" && v.structType == nil
}

func (v queryValue) defineType(opts *options) string {
	typ := v.typ
	if typ == "" && v.structType != nil {
		typ = targetName(opts, v.structType.name)
	}
	if v.emitPointer && v.structType != nil {
		return "*" + typ
	}
	return typ
}

func (v queryValue) pairs(opts *options) []argument {
	if v.isEmpty() {
		return nil
	}
	if !v.emit && v.structType != nil {
		out := make([]argument, 0, len(v.structType.fields))
		for _, field := range v.structType.fields {
			out = append(out, argument{
				name: escape(lowerTitle(field.name)),
				typ:  field.typ,
			})
		}
		return out
	}
	return []argument{{
		name: escape(v.name),
		typ:  v.defineType(opts),
	}}
}

func (v queryValue) slicePair(opts *options) argument {
	return argument{name: escape(v.name), typ: "[]" + v.defineType(opts)}
}

type goStruct struct {
	table  *plugin.Identifier
	name   string
	fields []goField
}

type goField struct {
	name   string
	dbName string
	typ    string
	column *plugin.Column
}

func buildStructs(req *plugin.GenerateRequest, opts *options, resolver *typeResolver) []goStruct {
	var structs []goStruct
	for _, schema := range req.GetCatalog().GetSchemas() {
		if skippedSchema(schema.GetName()) {
			continue
		}
		for _, table := range schema.GetTables() {
			tableName := table.GetRel().GetName()
			if schema.GetName() != req.GetCatalog().GetDefaultSchema() {
				tableName = schema.GetName() + "_" + tableName
			}
			if !opts.EmitExactTableNames {
				tableName = singular(tableName)
			}

			strct := goStruct{
				table: &plugin.Identifier{
					Catalog: table.GetRel().GetCatalog(),
					Schema:  schema.GetName(),
					Name:    table.GetRel().GetName(),
				},
				name:   structName(tableName, opts),
				fields: nil,
			}
			for _, column := range table.GetColumns() {
				strct.fields = append(strct.fields, goField{
					name:   structName(column.GetName(), opts),
					dbName: column.GetName(),
					typ:    resolver.goType(column),
					column: column,
				})
			}
			structs = append(structs, strct)
		}
	}
	sort.Slice(structs, func(i, j int) bool {
		return structs[i].name < structs[j].name
	})
	return structs
}

func skippedSchema(name string) bool {
	return name == "pg_catalog" || name == "information_schema"
}

func singular(name string) string {
	switch strings.ToLower(name) {
	case "campus", "meta", "metadata":
		return name
	case "calories":
		return "calorie"
	case "waves":
		return "wave"
	default:
		return inflection.Singular(name)
	}
}

func buildQueries(
	req *plugin.GenerateRequest,
	opts *options,
	resolver *typeResolver,
	structs []goStruct,
) ([]generatedQuery, error) {
	out := make([]generatedQuery, 0, len(req.GetQueries()))
	for _, query := range req.GetQueries() {
		if query.GetName() == "" || query.GetCmd() == "" {
			continue
		}

		arg, err := buildQueryArg(query, opts, resolver)
		if err != nil {
			return nil, err
		}
		ret, err := buildQueryReturn(req, query, opts, resolver, structs)
		if err != nil {
			return nil, err
		}

		params := []argument{{name: "ctx", typ: "context.Context"}}
		if query.GetCmd() == cmdCopyFrom || strings.HasPrefix(query.GetCmd(), ":batch") {
			if !arg.isEmpty() {
				params = append(params, arg.slicePair(opts))
			}
		} else {
			params = append(params, arg.pairs(opts)...)
		}

		results, err := resultTypes(query, ret, opts)
		if err != nil {
			return nil, err
		}
		for _, result := range results {
			resolver.addImportsForType(result)
		}
		for _, param := range params {
			resolver.addImportsForType(param.typ)
		}

		kind, annotation, store, err := classifyQuery(query)
		if err != nil {
			return nil, err
		}
		var route *shardRoute
		shardMode := shardModeNone
		var groupedMany *groupedManySpec
		if annotation != nil {
			switch {
			case annotation.name == allShardsRouteName:
				if len(annotation.operands) != 0 {
					return nil, fmt.Errorf(
						"query %s shard route %q cannot declare operands",
						query.GetName(), allShardsRouteName,
					)
				}
				switch query.GetCmd() {
				case cmdMany, cmdExec, cmdExecRows:
					shardMode = shardModeAll
				default:
					return nil, fmt.Errorf(
						"query %s uses shard: all() with unsupported command %s; expected :many, :exec, or :execrows",
						query.GetName(), query.GetCmd(),
					)
				}
			case strings.HasPrefix(query.GetCmd(), ":batch"):
				return nil, fmt.Errorf(
					"query %s uses %s and cannot declare shard metadata; grouped routing is supported only for :copyfrom",
					query.GetName(), query.GetCmd(),
				)
			default:
				shardMode = shardModeRouted
				if query.GetCmd() == cmdCopyFrom {
					shardMode = shardModeGroupedCopy
				}
				if query.GetCmd() == cmdMany {
					groupedMany, err = buildGroupedManySpec(query, arg, ret, opts, resolver)
					if err != nil {
						return nil, err
					}
					if groupedMany != nil {
						shardMode = shardModeGroupedMany
					}
				}
				route, err = resolveShardRoute(
					query,
					arg,
					annotation,
					groupedMany,
					opts,
					structs,
					req.GetCatalog().GetDefaultSchema(),
				)
				if err != nil {
					return nil, err
				}
				if groupedMany != nil {
					scalarizeGroupedManyRoute(route, groupedMany)
				}
				if !lastResultIsError(results) {
					return nil, fmt.Errorf("query %s cannot be shard-routed because its generated result has no error", query.GetName())
				}
			}
		}

		out = append(out, generatedQuery{
			methodName:      query.GetName(),
			command:         query.GetCmd(),
			kind:            kind,
			arg:             arg,
			ret:             ret,
			params:          params,
			storeParams:     append([]argument(nil), params...),
			callArgs:        argumentNames(params),
			results:         results,
			route:           route,
			shardMode:       shardMode,
			store:           store,
			storeParamAlias: nil,
			shardArgs:       nil,
			groupedMany:     groupedMany,
		})
	}
	if err := wrapQueriesWithExternalShardOperands(out, opts); err != nil {
		return nil, err
	}
	aliasSQLCParamsForStore(out, opts)
	sort.Slice(out, func(i, j int) bool {
		return out[i].methodName < out[j].methodName
	})
	return out, nil
}

func buildGroupedManySpec(
	query *plugin.Query,
	arg queryValue,
	ret queryValue,
	opts *options,
	resolver *typeResolver,
) (*groupedManySpec, error) {
	if len(query.GetParams()) != 1 {
		return nil, nil
	}
	column := query.GetParams()[0].GetColumn()
	isOneDimensionalArray := column.GetIsSqlcSlice() ||
		column.GetIsArray() && column.GetArrayDims() == 1
	if !isOneDimensionalArray {
		return nil, nil
	}
	if column.GetName() == "" {
		return nil, fmt.Errorf(
			"query %s grouped :many list parameter must have a name so returned rows can be reordered",
			query.GetName(),
		)
	}

	parameterType := resolver.goType(column)
	elementType, ok := strings.CutPrefix(parameterType, "[]")
	if !ok || elementType == "" {
		return nil, fmt.Errorf(
			"query %s grouped :many parameter %q has non-list Go type %s",
			query.GetName(),
			column.GetName(),
			parameterType,
		)
	}

	parameterField := structName(column.GetName(), opts)
	if arg.structType != nil {
		if len(arg.structType.fields) != 1 {
			return nil, fmt.Errorf(
				"query %s grouped :many expected one generated SQL parameter field, got %d",
				query.GetName(),
				len(arg.structType.fields),
			)
		}
		parameterField = arg.structType.fields[0].name
	}

	resultKeyField, lookupDBName, resultIsStruct, resultKeyAccess, err := groupedManyResultKey(
		query.GetName(),
		ret,
		column.GetName(),
		elementType,
	)
	if err != nil {
		return nil, err
	}
	lookupField := structName(lookupDBName, opts)
	if resultIsStruct {
		lookupField = resultKeyField
	}

	return &groupedManySpec{
		parameterDBName: column.GetName(),
		parameterField:  parameterField,
		lookupDBName:    lookupDBName,
		lookupField:     lookupField,
		elementType:     elementType,
		resultKeyField:  resultKeyField,
		resultIsStruct:  resultIsStruct,
		resultKeyAccess: resultKeyAccess,
		itemIsPointer:   false,
	}, nil
}

func groupedManyResultKey(
	queryName string,
	ret queryValue,
	parameterDBName string,
	elementType string,
) (string, string, bool, groupedManyResultKeyAccess, error) {
	lookupDBNames := []string{parameterDBName}
	if singularName := singular(parameterDBName); singularName != parameterDBName {
		lookupDBNames = append(lookupDBNames, singularName)
	}

	if ret.structType == nil {
		for _, lookupDBName := range lookupDBNames {
			if ret.dbName != lookupDBName {
				continue
			}
			access, ok := groupedManyKeyAccess(ret.typ, elementType)
			if !ok {
				return "", "", false, groupedManyResultKeyAccess{}, fmt.Errorf(
					"query %s grouped :many result key %q has Go type %s, want %s",
					queryName,
					lookupDBName,
					ret.typ,
					elementType,
				)
			}
			return "", lookupDBName, false, access, nil
		}
		return "", "", false, groupedManyResultKeyAccess{}, fmt.Errorf(
			"query %s grouped :many result must expose lookup key %s; got scalar result %q",
			queryName,
			groupedManyLookupDescription(lookupDBNames),
			ret.dbName,
		)
	}

	for _, lookupDBName := range lookupDBNames {
		var matches []goField
		for _, field := range ret.structType.fields {
			if field.dbName == lookupDBName {
				matches = append(matches, field)
			}
		}
		switch len(matches) {
		case 0:
			continue
		case 1:
		default:
			return "", "", false, groupedManyResultKeyAccess{}, fmt.Errorf(
				"query %s grouped :many result exposes multiple fields for lookup key %q",
				queryName,
				lookupDBName,
			)
		}
		access, ok := groupedManyKeyAccess(matches[0].typ, elementType)
		if !ok {
			return "", "", false, groupedManyResultKeyAccess{}, fmt.Errorf(
				"query %s grouped :many result key %q has Go type %s, want %s",
				queryName,
				lookupDBName,
				matches[0].typ,
				elementType,
			)
		}
		return matches[0].name, lookupDBName, true, access, nil
	}

	return "", "", false, groupedManyResultKeyAccess{}, fmt.Errorf(
		"query %s grouped :many result must expose exactly one field for lookup key %s",
		queryName,
		groupedManyLookupDescription(lookupDBNames),
	)
}

func groupedManyKeyAccess(resultType, elementType string) (groupedManyResultKeyAccess, bool) {
	if resultType == elementType {
		return groupedManyResultKeyAccess{
			nullable:   false,
			pointer:    false,
			valid:      false,
			valueField: "",
		}, true
	}
	if resultType == "*"+elementType {
		return groupedManyResultKeyAccess{
			nullable:   true,
			pointer:    true,
			valid:      false,
			valueField: "",
		}, true
	}

	type nullableType struct {
		elementType string
		valueField  string
	}
	nullableTypes := map[string]nullableType{
		"pgtype.Int4":     {elementType: "int32", valueField: "Int32"},
		"pgtype.Int8":     {elementType: goTypeInt64, valueField: "Int64"},
		"pgtype.Int2":     {elementType: "int16", valueField: "Int16"},
		"pgtype.Float8":   {elementType: "float64", valueField: "Float64"},
		"pgtype.Float4":   {elementType: "float32", valueField: "Float32"},
		"pgtype.Bool":     {elementType: goTypeBool, valueField: "Bool"},
		"pgtype.Text":     {elementType: goTypeString, valueField: "String"},
		"sql.NullBool":    {elementType: goTypeBool, valueField: "Bool"},
		"sql.NullByte":    {elementType: "byte", valueField: "Byte"},
		"sql.NullFloat64": {elementType: "float64", valueField: "Float64"},
		"sql.NullInt16":   {elementType: "int16", valueField: "Int16"},
		"sql.NullInt32":   {elementType: "int32", valueField: "Int32"},
		"sql.NullInt64":   {elementType: goTypeInt64, valueField: "Int64"},
		"sql.NullString":  {elementType: goTypeString, valueField: "String"},
		"sql.NullTime":    {elementType: "time.Time", valueField: "Time"},
	}
	if nullable, ok := nullableTypes[resultType]; ok && nullable.elementType == elementType {
		return groupedManyResultKeyAccess{
			nullable:   true,
			pointer:    false,
			valid:      true,
			valueField: nullable.valueField,
		}, true
	}

	resultQualifier, resultName := splitTypeQualifier(resultType)
	elementQualifier, elementName := splitTypeQualifier(elementType)
	if resultQualifier == elementQualifier &&
		resultName == "Null"+elementName &&
		elementName != "" {
		return groupedManyResultKeyAccess{
			nullable:   true,
			pointer:    false,
			valid:      true,
			valueField: elementName,
		}, true
	}
	return groupedManyResultKeyAccess{
		nullable:   false,
		pointer:    false,
		valid:      false,
		valueField: "",
	}, false
}

func splitTypeQualifier(typ string) (string, string) {
	qualifier, name, ok := strings.Cut(typ, ".")
	if !ok {
		return "", typ
	}
	return qualifier, name
}

func groupedManyLookupDescription(lookupDBNames []string) string {
	if len(lookupDBNames) == 1 {
		return fmt.Sprintf("%q", lookupDBNames[0])
	}
	return fmt.Sprintf("%q or its singular form %q", lookupDBNames[0], lookupDBNames[1])
}

func scalarizeGroupedManyRoute(route *shardRoute, spec *groupedManySpec) {
	for index := range route.operands {
		operand := &route.operands[index]
		if operand.dbName == spec.parameterDBName && !operand.external {
			operand.typ = spec.elementType
		}
	}
}

func classifyQuery(query *plugin.Query) (queryKind, *routeAnnotation, string, error) {
	if err := validateRawAnnotationOrder(query); err != nil {
		return "", nil, "", err
	}
	comments := queryComments(query)
	if len(comments) == 0 {
		return "", nil, "", fmt.Errorf(
			"query %s is missing required kind annotation; add -- kind: read or -- kind: write immediately after -- name",
			query.GetName(),
		)
	}
	kind, err := queryKindFromComment(query.GetName(), comments[0])
	if err != nil {
		return "", nil, "", err
	}

	var route *routeAnnotation
	next := 1
	if len(comments) > next && annotationPrefix(comments[next]) == shardAnnotationPrefix {
		route, err = parseShardAnnotation(query.GetName(), comments[next])
		if err != nil {
			return "", nil, "", err
		}
		next++
	}

	var store string
	if len(comments) > next && annotationPrefix(comments[next]) == storeAnnotationPrefix {
		store, err = parseStoreAnnotation(query.GetName(), comments[next])
		if err != nil {
			return "", nil, "", err
		}
		next++
	}
	for _, comment := range comments[next:] {
		switch annotationPrefix(comment) {
		case shardAnnotationPrefix:
			return "", nil, "", fmt.Errorf(
				"query %s shard annotation must immediately follow its kind annotation",
				query.GetName(),
			)
		case storeAnnotationPrefix:
			return "", nil, "", fmt.Errorf(
				"query %s store annotation must immediately follow its kind or shard annotation",
				query.GetName(),
			)
		}
	}
	if store == "" {
		return "", nil, "", fmt.Errorf(
			"query %s is missing required store annotation; add -- store: ExportedGroup immediately after its optional shard annotation",
			query.GetName(),
		)
	}
	return kind, route, store, nil
}

func validateRawAnnotationOrder(query *plugin.Query) error {
	lines := strings.Split(query.GetText(), "\n")
	nameLine := -1
	for index, rawLine := range lines {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(rawLine)), "-- name:") {
			nameLine = index
			break
		}
	}
	if nameLine == -1 {
		return nil
	}
	if nameLine+1 >= len(lines) || !strings.HasPrefix(
		strings.ToLower(normalizeComment(lines[nameLine+1])),
		"kind:",
	) || !strings.HasPrefix(strings.TrimSpace(lines[nameLine+1]), "--") {
		return fmt.Errorf(
			"query %s kind annotation must immediately follow its sqlc name annotation",
			query.GetName(),
		)
	}
	next := nameLine + 2
	if next < len(lines) && annotationPrefix(lines[next]) == shardAnnotationPrefix &&
		strings.HasPrefix(strings.TrimSpace(lines[next]), "--") {
		next++
	}
	if next < len(lines) && annotationPrefix(lines[next]) == storeAnnotationPrefix &&
		strings.HasPrefix(strings.TrimSpace(lines[next]), "--") {
		next++
	}
	for index := next; index < len(lines); index++ {
		switch annotationPrefix(lines[index]) {
		case shardAnnotationPrefix:
			return fmt.Errorf(
				"query %s shard annotation must immediately follow its kind annotation",
				query.GetName(),
			)
		case storeAnnotationPrefix:
			return fmt.Errorf(
				"query %s store annotation must immediately follow its kind or shard annotation",
				query.GetName(),
			)
		}
	}
	return nil
}

func annotationPrefix(comment string) string {
	lower := strings.ToLower(normalizeComment(comment))
	switch {
	case strings.HasPrefix(lower, shardAnnotationPrefix):
		return shardAnnotationPrefix
	case strings.HasPrefix(lower, storeAnnotationPrefix):
		return storeAnnotationPrefix
	default:
		return ""
	}
}

func queryComments(query *plugin.Query) []string {
	if comments := query.GetComments(); len(comments) > 0 {
		return comments
	}
	var comments []string
	for rawLine := range strings.SplitSeq(query.GetText(), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(strings.ToLower(line), "-- name:") {
			continue
		}
		if strings.HasPrefix(line, "--") {
			comments = append(comments, line)
			continue
		}
		break
	}
	return comments
}

func parseShardAnnotation(queryName, comment string) (*routeAnnotation, error) {
	normalized := normalizeComment(comment)
	value := strings.TrimSpace(normalized[len("shard:"):])
	open := strings.IndexByte(value, '(')
	if open <= 0 || !strings.HasSuffix(value, ")") {
		return nil, fmt.Errorf(
			"query %s has malformed shard annotation %q; expected route(parameter, ...)",
			queryName, value,
		)
	}
	name := strings.TrimSpace(value[:open])
	if !validRouteIdentifier(name) {
		return nil, fmt.Errorf("query %s has invalid shard route name %q", queryName, name)
	}
	body := strings.TrimSpace(value[open+1 : len(value)-1])
	if body == "" {
		return &routeAnnotation{name: name, operands: nil}, nil
	}
	parts := strings.Split(body, ",")
	operands := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		operand := strings.TrimSpace(part)
		if !validRouteIdentifier(operand) {
			return nil, fmt.Errorf("query %s has invalid shard operand %q", queryName, operand)
		}
		if _, ok := seen[operand]; ok {
			return nil, fmt.Errorf("query %s repeats shard operand %q", queryName, operand)
		}
		seen[operand] = struct{}{}
		operands = append(operands, operand)
	}
	return &routeAnnotation{name: name, operands: operands}, nil
}

func parseStoreAnnotation(queryName, comment string) (string, error) {
	normalized := normalizeComment(comment)
	value := strings.TrimSpace(normalized[len(storeAnnotationPrefix):])
	first, _ := utf8.DecodeRuneInString(value)
	if !validRouteIdentifier(value) || goKeywords[value] || !unicode.IsUpper(first) {
		return "", fmt.Errorf(
			"query %s has invalid store annotation %q; expected an exported Go identifier",
			queryName,
			value,
		)
	}
	return value, nil
}

func validRouteIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for index, r := range value {
		if unicode.IsLetter(r) || r == '_' || index > 0 && unicode.IsDigit(r) {
			continue
		}
		return false
	}
	return true
}

func queryKindFromComment(queryName, comment string) (queryKind, error) {
	comment = normalizeComment(comment)
	lower := strings.ToLower(comment)
	if !strings.HasPrefix(lower, "kind:") {
		return "", fmt.Errorf(
			"query %s first comment must be kind annotation; add -- kind: read or -- kind: write before other comments",
			queryName,
		)
	}

	value := strings.TrimSpace(comment[len("kind:"):])
	switch strings.ToLower(value) {
	case string(queryKindRead):
		return queryKindRead, nil
	case string(queryKindWrite):
		return queryKindWrite, nil
	default:
		return "", fmt.Errorf("query %s has invalid kind annotation %q; expected read or write", queryName, value)
	}
}

func normalizeComment(comment string) string {
	comment = strings.TrimSpace(comment)
	comment = strings.TrimPrefix(comment, "--")
	comment = strings.TrimPrefix(comment, "//")
	comment = strings.TrimSpace(comment)
	if strings.HasPrefix(comment, "/*") && strings.HasSuffix(comment, "*/") {
		comment = strings.TrimPrefix(comment, "/*")
		comment = strings.TrimSuffix(comment, "*/")
	}
	return strings.TrimSpace(comment)
}

func buildQueryArg(query *plugin.Query, opts *options, resolver *typeResolver) (queryValue, error) {
	params := query.GetParams()
	limit := int(*opts.QueryParameterLimit)
	if len(params) == 1 && limit != 0 {
		param := params[0]
		return queryValue{
			name:   escape(paramName(param)),
			dbName: param.GetColumn().GetName(),
			typ:    resolver.goType(param.GetColumn()),
			column: param.GetColumn(),
		}, nil
	}
	if len(params) == 0 {
		return queryValue{}, nil
	}

	columns := make([]goColumn, 0, len(params))
	for _, param := range params {
		columns = append(columns, goColumn{
			id:     int(param.GetNumber()),
			column: param.GetColumn(),
		})
	}
	strct, err := columnsToStruct(query.GetName()+"Params", columns, opts, resolver, false)
	if err != nil {
		return queryValue{}, err
	}
	emit := len(params) > limit || query.GetCmd() == cmdCopyFrom

	return queryValue{
		emit:        emit,
		emitPointer: opts.EmitParamsStructPointers,
		name:        defaultArgumentName,
		structType:  strct,
	}, nil
}

func resolveShardRoute(
	query *plugin.Query,
	arg queryValue,
	annotation *routeAnnotation,
	groupedMany *groupedManySpec,
	opts *options,
	structs []goStruct,
	defaultSchema string,
) (*shardRoute, error) {
	route := &shardRoute{
		name:       annotation.name,
		methodName: routeMethodName(annotation.name, opts),
		operands:   make([]routeOperand, 0, len(annotation.operands)),
	}
	if route.methodName == "" {
		return nil, fmt.Errorf("query %s shard route %q does not produce a Go method name", query.GetName(), annotation.name)
	}
	first, _ := utf8.DecodeRuneInString(route.methodName)
	if !validRouteIdentifier(route.methodName) || !unicode.IsUpper(first) {
		return nil, fmt.Errorf(
			"query %s shard route %q generates non-exported or invalid resolver method %q",
			query.GetName(), annotation.name, route.methodName,
		)
	}
	seenNames := make(map[string]struct{}, len(annotation.operands))
	for _, dbName := range annotation.operands {
		operand, ok := routeOperandForArg(arg, dbName, opts)
		if !ok && groupedMany != nil && dbName == groupedMany.lookupDBName {
			operand, ok = routeOperandForArg(arg, groupedMany.parameterDBName, opts)
			if ok {
				operand.name = escape(lowerTitle(groupedMany.lookupField))
				operand.fieldName = groupedMany.lookupField
			}
		}
		if !ok {
			field, err := routeOperandFieldFromModels(query, dbName, structs, defaultSchema)
			if err != nil {
				return nil, err
			}
			operand = routeOperand{
				name:       escape(lowerTitle(field.name)),
				typ:        field.typ,
				expression: "",
				dbName:     dbName,
				fieldName:  field.name,
				external:   true,
			}
		}
		if _, ok := seenNames[operand.name]; ok {
			return nil, fmt.Errorf(
				"query %s shard operands generate duplicate Go parameter %q",
				query.GetName(), operand.name,
			)
		}
		seenNames[operand.name] = struct{}{}
		route.operands = append(route.operands, operand)
	}
	return route, nil
}

func routeMethodName(name string, opts *options) string {
	if replacement := opts.Rename[name]; replacement != "" {
		return replacement
	}
	parts := splitIdentifier(name)
	for index, part := range parts {
		if strings.EqualFold(part, "p2p") {
			parts[index] = "P2P"
			continue
		}
		if strings.IndexFunc(part, unicode.IsUpper) >= 0 {
			parts[index] = title(part)
			continue
		}
		parts[index] = structName(part, opts)
	}
	return strings.Join(parts, "")
}

func routeOperandForArg(arg queryValue, dbName string, opts *options) (routeOperand, bool) {
	if arg.structType != nil {
		for _, field := range arg.structType.fields {
			if field.dbName != dbName {
				continue
			}
			expression := escape(lowerTitle(field.name))
			if arg.emit {
				expression = escape(arg.name) + "." + field.name
			}
			return routeOperand{
				name:       escape(lowerTitle(field.name)),
				typ:        field.typ,
				expression: expression,
				dbName:     dbName,
				fieldName:  field.name,
				external:   false,
			}, true
		}
		return routeOperand{
			name:       "",
			typ:        "",
			expression: "",
			dbName:     "",
			fieldName:  "",
			external:   false,
		}, false
	}
	if arg.dbName != dbName {
		return routeOperand{
			name:       "",
			typ:        "",
			expression: "",
			dbName:     "",
			fieldName:  "",
			external:   false,
		}, false
	}
	fieldName := structName(dbName, opts)
	return routeOperand{
		name:       escape(lowerTitle(fieldName)),
		typ:        arg.typ,
		expression: escape(arg.name),
		dbName:     dbName,
		fieldName:  fieldName,
		external:   false,
	}, true
}

func routeOperandFieldFromModels(
	query *plugin.Query,
	dbName string,
	structs []goStruct,
	defaultSchema string,
) (goField, error) {
	groups := relatedQueryModelGroups(query, structs, defaultSchema)
	groups = append(groups, structs)
	for _, models := range groups {
		var matched *goField
		var matchedModel string
		for index := range models {
			model := &models[index]
			for fieldIndex := range model.fields {
				field := &model.fields[fieldIndex]
				if field.dbName != dbName {
					continue
				}
				if matched != nil && (matched.name != field.name || matched.typ != field.typ) {
					return goField{}, fmt.Errorf(
						"query %s shard operand %q matches incompatible fields on generated models %s and %s",
						query.GetName(),
						dbName,
						matchedModel,
						model.name,
					)
				}
				fieldCopy := *field
				matched = &fieldCopy
				matchedModel = model.name
			}
		}
		if matched != nil {
			return *matched, nil
		}
	}
	return goField{}, fmt.Errorf(
		"query %s shard operand %q does not match a SQL parameter or a field on its generated table model",
		query.GetName(),
		dbName,
	)
}

func relatedQueryModelGroups(
	query *plugin.Query,
	structs []goStruct,
	defaultSchema string,
) [][]goStruct {
	groups := make([][]goStruct, 0, 3)
	if query.GetInsertIntoTable() != nil {
		groups = append(groups, modelsForTables(
			[]*plugin.Identifier{query.GetInsertIntoTable()},
			structs,
			defaultSchema,
		))
	}

	resultTables := make([]*plugin.Identifier, 0, len(query.GetColumns()))
	for _, column := range query.GetColumns() {
		if column.GetTable() != nil {
			resultTables = append(resultTables, column.GetTable())
		}
	}
	if models := modelsForTables(resultTables, structs, defaultSchema); len(models) > 0 {
		groups = append(groups, models)
	}

	if models := modelsForTables(
		queryRelationIdentifiers(query.GetText()),
		structs,
		defaultSchema,
	); len(models) > 0 {
		groups = append(groups, models)
	}

	parameterTables := make([]*plugin.Identifier, 0, len(query.GetParams()))
	for _, param := range query.GetParams() {
		if param.GetColumn().GetTable() != nil {
			parameterTables = append(parameterTables, param.GetColumn().GetTable())
		}
	}
	if models := modelsForTables(parameterTables, structs, defaultSchema); len(models) > 0 {
		groups = append(groups, models)
	}
	return groups
}

func modelsForTables(
	tables []*plugin.Identifier,
	structs []goStruct,
	defaultSchema string,
) []goStruct {
	related := make([]goStruct, 0, len(tables))
	for _, model := range structs {
		for _, table := range tables {
			if !sameTableName(table, model.table, defaultSchema) {
				continue
			}
			related = append(related, model)
			break
		}
	}
	return related
}

func wrapQueriesWithExternalShardOperands(queries []generatedQuery, opts *options) error {
	for index := range queries {
		query := &queries[index]
		if query.route == nil {
			continue
		}
		hasExternal := false
		for _, operand := range query.route.operands {
			if operand.external {
				hasExternal = true
				break
			}
		}
		if query.shardMode == shardModeGroupedMany {
			if hasExternal || query.arg.structType != nil {
				if err := wrapGroupedManyShardOperands(query, opts); err != nil {
					return err
				}
			}
			continue
		}
		if hasExternal {
			if err := wrapExternalShardOperands(query, opts); err != nil {
				return err
			}
		}
	}
	return nil
}

func aliasSQLCParamsForStore(queries []generatedQuery, opts *options) {
	for index := range queries {
		query := &queries[index]
		if query.store == "" ||
			query.shardArgs != nil ||
			!query.arg.emit ||
			query.arg.structType == nil {
			continue
		}

		sqlcType := targetName(opts, query.arg.structType.name)
		aliasName := query.methodName + "T"
		replaced := false
		for paramIndex := range query.storeParams {
			param := &query.storeParams[paramIndex]
			typ := strings.ReplaceAll(param.typ, sqlcType, aliasName)
			if typ == param.typ {
				continue
			}
			param.typ = typ
			replaced = true
		}
		if !replaced {
			continue
		}
		query.storeParamAlias = &storeParamAlias{
			name:   aliasName,
			target: exportedSQLCType(opts, sqlcType),
		}
	}
}

func wrapGroupedManyShardOperands(query *generatedQuery, opts *options) error {
	spec := query.groupedMany
	wrapper := &shardArgWrapper{
		name:    query.methodName + "T",
		fields:  nil,
		sqlcArg: queryValue{},
	}
	usedFields := make(map[string]string)
	appendField := func(name, typ, source string) error {
		if previous, exists := usedFields[name]; exists {
			return fmt.Errorf(
				"query %s shard parameter wrapper field %s conflicts between %s and %s",
				query.methodName,
				name,
				previous,
				source,
			)
		}
		usedFields[name] = source
		wrapper.fields = append(wrapper.fields, argument{name: name, typ: typ})
		return nil
	}

	if err := appendField(
		spec.lookupField,
		spec.elementType,
		"SQL list parameter "+spec.parameterDBName,
	); err != nil {
		return err
	}
	for operandIndex := range query.route.operands {
		operand := &query.route.operands[operandIndex]
		if operand.dbName == spec.parameterDBName && !operand.external {
			operand.expression = "arg." + spec.lookupField
			continue
		}
		if !operand.external {
			return fmt.Errorf(
				"query %s grouped :many shard operand %q is not its sole SQL list parameter",
				query.methodName,
				operand.dbName,
			)
		}
		if err := appendField(
			operand.fieldName,
			operand.typ,
			"shard operand "+operand.dbName,
		); err != nil {
			return err
		}
		operand.expression = "arg." + operand.fieldName
	}

	wrapperType := wrapper.name
	if opts.EmitParamsStructPointers {
		wrapperType = "*" + wrapperType
		spec.itemIsPointer = true
	}
	query.storeParams = []argument{
		query.params[0],
		{name: defaultArgumentName, typ: "[]" + wrapperType},
	}
	query.shardArgs = wrapper
	return nil
}

func wrapExternalShardOperands(query *generatedQuery, opts *options) error {
	wrapper := &shardArgWrapper{
		name:    query.methodName + "T",
		fields:  nil,
		sqlcArg: queryValue{},
	}
	usedFields := make(map[string]string)
	appendField := func(name, typ, source string) error {
		if previous, exists := usedFields[name]; exists {
			return fmt.Errorf(
				"query %s shard parameter wrapper field %s conflicts between %s and %s",
				query.methodName,
				name,
				previous,
				source,
			)
		}
		usedFields[name] = source
		wrapper.fields = append(wrapper.fields, argument{name: name, typ: typ})
		return nil
	}

	callArgs := []string{query.params[0].name}
	arg := query.arg
	switch {
	case arg.isEmpty():
	case arg.emit:
		for _, field := range arg.structType.fields {
			if err := appendField(field.name, field.typ, "SQL parameter "+field.dbName); err != nil {
				return err
			}
		}
		wrapper.sqlcArg = arg
		callArgs = append(callArgs, "arg.sqlcParams()")
	case arg.structType != nil:
		for _, field := range arg.structType.fields {
			if err := appendField(field.name, field.typ, "SQL parameter "+field.dbName); err != nil {
				return err
			}
			callArgs = append(callArgs, "arg."+field.name)
		}
	default:
		fieldName := structName(arg.dbName, opts)
		if fieldName == "" {
			fieldName = structName(arg.name, opts)
		}
		if err := appendField(fieldName, arg.defineType(opts), "SQL parameter "+arg.dbName); err != nil {
			return err
		}
		callArgs = append(callArgs, "arg."+fieldName)
	}

	for operandIndex := range query.route.operands {
		operand := &query.route.operands[operandIndex]
		if operand.external {
			fieldName := operand.fieldName
			if err := appendField(fieldName, operand.typ, "shard operand "+operand.dbName); err != nil {
				return err
			}
			operand.expression = "arg." + fieldName
			continue
		}
		expression, ok := wrappedRouteOperandExpression(arg, operand.dbName, opts)
		if !ok {
			return fmt.Errorf(
				"query %s cannot wrap SQL parameter %q for shard routing",
				query.methodName,
				operand.dbName,
			)
		}
		operand.expression = expression
	}

	wrapperType := wrapper.name
	if opts.EmitParamsStructPointers {
		wrapperType = "*" + wrapperType
	}
	if query.shardMode == shardModeGroupedCopy {
		wrapperType = "[]" + wrapperType
	}
	query.storeParams = []argument{
		query.params[0],
		{name: defaultArgumentName, typ: wrapperType},
	}
	query.callArgs = callArgs
	query.shardArgs = wrapper
	return nil
}

func wrappedRouteOperandExpression(arg queryValue, dbName string, opts *options) (string, bool) {
	if arg.structType != nil {
		for _, field := range arg.structType.fields {
			if field.dbName != dbName {
				continue
			}
			return "arg." + field.name, true
		}
		return "", false
	}
	if arg.dbName != dbName {
		return "", false
	}
	fieldName := structName(arg.dbName, opts)
	if fieldName == "" {
		fieldName = structName(arg.name, opts)
	}
	return "arg." + fieldName, true
}

func buildQueryReturn(
	req *plugin.GenerateRequest,
	query *plugin.Query,
	opts *options,
	resolver *typeResolver,
	structs []goStruct,
) (queryValue, error) {
	if !queryReturnsData(query.GetCmd()) {
		return queryValue{}, nil
	}
	columns := query.GetColumns()
	if len(columns) == 1 && columns[0].GetEmbedTable() == nil {
		column := columns[0]
		name := strings.ReplaceAll(columnName(column, 0), "$", "_")
		return queryValue{
			name:   escape(name),
			dbName: name,
			typ:    resolver.goType(column),
			column: column,
		}, nil
	}
	if len(columns) == 0 {
		return queryValue{}, nil
	}

	var model *goStruct
	for idx := range structs {
		strct := &structs[idx]
		if queryColumnsMatchStruct(req, opts, resolver, columns, strct) {
			model = strct
			break
		}
	}

	emit := false
	if model == nil {
		goColumns := make([]goColumn, 0, len(columns))
		for idx, column := range columns {
			goColumns = append(goColumns, goColumn{
				id:     idx,
				column: column,
			})
		}
		var err error
		model, err = columnsToStruct(query.GetName()+"Row", goColumns, opts, resolver, true)
		if err != nil {
			return queryValue{}, err
		}
		emit = true
	}
	return queryValue{
		emit:        emit,
		emitPointer: opts.EmitResultStructPointers,
		name:        "i",
		structType:  model,
	}, nil
}

func queryReturnsData(cmd string) bool {
	switch cmd {
	case ":one", cmdMany, ":batchone", ":batchmany":
		return true
	default:
		return false
	}
}

func resultTypes(query *plugin.Query, ret queryValue, opts *options) ([]string, error) {
	switch query.GetCmd() {
	case ":one":
		return []string{ret.defineType(opts), resultErrorName}, nil
	case cmdMany:
		return []string{"[]" + ret.defineType(opts), resultErrorName}, nil
	case cmdExec:
		return []string{resultErrorName}, nil
	case cmdExecRows, cmdCopyFrom:
		return []string{goTypeInt64, resultErrorName}, nil
	case ":execresult":
		return []string{"pgconn.CommandTag", resultErrorName}, nil
	case ":batchexec", ":batchone", ":batchmany":
		return []string{"*" + targetName(opts, query.GetName()+"BatchResults")}, nil
	default:
		return nil, fmt.Errorf("unsupported query command %q for %s", query.GetCmd(), query.GetName())
	}
}

func queryColumnsMatchStruct(
	req *plugin.GenerateRequest,
	opts *options,
	resolver *typeResolver,
	columns []*plugin.Column,
	strct *goStruct,
) bool {
	if len(strct.fields) != len(columns) {
		return false
	}
	for idx, field := range strct.fields {
		column := columns[idx]
		if field.name != structName(columnName(column, idx), opts) {
			return false
		}
		if field.typ != resolver.goType(column) {
			return false
		}
		if !sameTableName(column.GetTable(), strct.table, req.GetCatalog().GetDefaultSchema()) {
			return false
		}
	}
	return true
}

type goColumn struct {
	id     int
	column *plugin.Column
}

func columnsToStruct(
	name string,
	columns []goColumn,
	opts *options,
	resolver *typeResolver,
	useID bool,
) (*goStruct, error) {
	strct := &goStruct{
		table:  nil,
		name:   name,
		fields: nil,
	}
	seen := map[string][]int{}
	suffixes := map[int]int{}
	for idx, column := range columns {
		columnName := columnName(column.column, idx)
		fieldName := structName(columnName, opts)
		baseFieldName := fieldName

		suffix := 0
		if existing, ok := suffixes[column.id]; ok && useID {
			suffix = existing
		} else if count := len(seen[fieldName]); count > 0 && !column.column.GetIsNamedParam() {
			suffix = count + 1
		}
		suffixes[column.id] = suffix
		if suffix > 0 {
			fieldName = fmt.Sprintf("%s_%d", fieldName, suffix)
		}

		strct.fields = append(strct.fields, goField{
			name:   fieldName,
			dbName: columnName,
			typ:    resolver.goType(column.column),
			column: column.column,
		})
		seen[baseFieldName] = append(seen[baseFieldName], idx)
	}
	return strct, checkIncompatibleFieldTypes(strct.fields)
}

func checkIncompatibleFieldTypes(fields []goField) error {
	fieldTypes := map[string]string{}
	for _, field := range fields {
		if existing, ok := fieldTypes[field.name]; ok && existing != field.typ {
			return fmt.Errorf("named param %s has incompatible types: %s, %s", field.name, existing, field.typ)
		}
		fieldTypes[field.name] = field.typ
	}
	return nil
}
