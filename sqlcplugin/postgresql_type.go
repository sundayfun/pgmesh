package sqlcplugin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/types"
	"regexp"
	"strings"

	"github.com/sqlc-dev/plugin-sdk-go/plugin"
)

func sameTableName(left, right *plugin.Identifier, defaultSchema string) bool {
	if left == nil || right == nil {
		return false
	}
	leftSchema := left.GetSchema()
	if leftSchema == "" {
		leftSchema = defaultSchema
	}
	rightSchema := right.GetSchema()
	if rightSchema == "" {
		rightSchema = defaultSchema
	}
	return left.GetCatalog() == right.GetCatalog() &&
		leftSchema == rightSchema &&
		left.GetName() == right.GetName()
}

func (r *typeResolver) goType(column *plugin.Column) string {
	for idx := range r.opts.Overrides {
		override := &r.opts.Overrides[idx]
		if override.Column == "" || !override.matchesColumn(column, r.req.GetCatalog().GetDefaultSchema()) {
			continue
		}
		typ := r.overrideType(override)
		if column.GetIsSqlcSlice() {
			return "[]" + typ
		}
		return typ
	}
	for idx := range r.opts.Overrides {
		override := &r.opts.Overrides[idx]
		if !override.matchesColumn(column, r.req.GetCatalog().GetDefaultSchema()) {
			continue
		}
		typ := r.overrideType(override)
		if column.GetIsSqlcSlice() {
			typ = "[]" + typ
		} else if column.GetIsArray() {
			typ = strings.Repeat("[]", int(column.GetArrayDims())) + typ
		}
		return typ
	}

	typ := r.goInnerType(column)
	if column.GetIsSqlcSlice() {
		typ = "[]" + typ
	} else if column.GetIsArray() {
		typ = strings.Repeat("[]", int(column.GetArrayDims())) + typ
	}
	return typ
}

func (r *typeResolver) overrideType(override *override) string {
	typ := override.typeName
	if r.opts.InternalImportPath == "" || override.importPath != "" || typeQualifier(typ) != "" {
		return typ
	}
	prefixLength := 0
	for strings.HasPrefix(typ[prefixLength:], "[]") || strings.HasPrefix(typ[prefixLength:], "*") {
		if strings.HasPrefix(typ[prefixLength:], "[]") {
			prefixLength += 2
			continue
		}
		prefixLength++
	}
	base := typ[prefixLength:]
	if base == "interface{}" || types.Universe.Lookup(base) != nil {
		return typ
	}
	return typ[:prefixLength] + targetName(r.opts, base)
}

func (r *typeResolver) addImportsForType(typ string) {
	r.imports.addForType(typ)
	for idx := range r.opts.Overrides {
		override := &r.opts.Overrides[idx]
		if override.importPath == "" {
			continue
		}
		qualifier := typeQualifier(override.typeName)
		if qualifier == "" || !typeUsesQualifier(typ, qualifier) {
			continue
		}
		r.imports.addNamed(override.importAlias, override.importPath)
	}
}

func typeQualifier(typ string) string {
	for strings.HasPrefix(typ, "[]") || strings.HasPrefix(typ, "*") {
		typ = strings.TrimPrefix(typ, "[]")
		typ = strings.TrimPrefix(typ, "*")
	}
	before, _, ok := strings.Cut(typ, ".")
	if !ok {
		return ""
	}
	return before
}

func (r *typeResolver) goInnerType(column *plugin.Column) string {
	return r.postgresType(column)
}

func (r *typeResolver) postgresType(column *plugin.Column) string {
	columnType := dataType(column.GetType())
	notNull := column.GetNotNull() || column.GetIsArray()
	emitPointersForNull := r.opts.EmitPointersForNullTypes
	emitPointersForNullEnums := emitPointersForNull
	if r.opts.EmitPointersForNullEnumType != nil {
		emitPointersForNullEnums = *r.opts.EmitPointersForNullEnumType
	}

	switch columnType {
	case "serial", "serial4", "pg_catalog.serial4", "integer", "int", "int4", "pg_catalog.int4":
		return nullableType("int32", "*int32", "pgtype.Int4", notNull, emitPointersForNull)
	case "bigserial", "serial8", "pg_catalog.serial8", "bigint", "int8", "pg_catalog.int8":
		return nullableType(goTypeInt64, "*int64", "pgtype.Int8", notNull, emitPointersForNull)
	case "smallserial", "serial2", "pg_catalog.serial2", "smallint", "int2", "pg_catalog.int2":
		return nullableType("int16", "*int16", "pgtype.Int2", notNull, emitPointersForNull)
	case "float", "double precision", "float8", "pg_catalog.float8":
		return nullableType("float64", "*float64", "pgtype.Float8", notNull, emitPointersForNull)
	case "real", "float4", "pg_catalog.float4":
		return nullableType("float32", "*float32", "pgtype.Float4", notNull, emitPointersForNull)
	case "boolean", goTypeBool, "pg_catalog.bool":
		return nullableType(goTypeBool, "*bool", "pgtype.Bool", notNull, emitPointersForNull)
	case "text", "pg_catalog.varchar", "pg_catalog.bpchar", "varchar", goTypeString, "citext", "name":
		return nullableType(goTypeString, "*string", "pgtype.Text", notNull, emitPointersForNull)
	case "bytea", "blob", "pg_catalog.bytea":
		return "[]byte"
	case "json", "pg_catalog.json", "jsonb", "pg_catalog.jsonb":
		return "[]byte"
	case "date":
		return "pgtype.Date"
	case "pg_catalog.time":
		return "pgtype.Time"
	case "pg_catalog.timetz":
		return nullableStdlibType("time.Time", "*time.Time", "sql.NullTime", notNull, emitPointersForNull)
	case "pg_catalog.timestamp", "timestamp":
		return "pgtype.Timestamp"
	case "pg_catalog.timestamptz", "timestamptz":
		return "pgtype.Timestamptz"
	case "uuid":
		return "pgtype.UUID"
	case "numeric", "pg_catalog.numeric", "money":
		return "pgtype.Numeric"
	case "inet":
		if notNull {
			return "netip.Addr"
		}
		return "*netip.Addr"
	case "cidr":
		if notNull {
			return "netip.Prefix"
		}
		return "*netip.Prefix"
	case "macaddr", "macaddr8":
		return "net.HardwareAddr"
	case "ltree", "lquery", "ltxtquery":
		return nullableType(goTypeString, "*string", "pgtype.Text", notNull, emitPointersForNull)
	case "interval", "pg_catalog.interval":
		return "pgtype.Interval"
	case "daterange":
		return "pgtype.Range[pgtype.Date]"
	case "datemultirange":
		return "pgtype.Multirange[pgtype.Range[pgtype.Date]]"
	case "tsrange":
		return "pgtype.Range[pgtype.Timestamp]"
	case "tsmultirange":
		return "pgtype.Multirange[pgtype.Range[pgtype.Timestamp]]"
	case "tstzrange":
		return "pgtype.Range[pgtype.Timestamptz]"
	case "tstzmultirange":
		return "pgtype.Multirange[pgtype.Range[pgtype.Timestamptz]]"
	case "numrange":
		return "pgtype.Range[pgtype.Numeric]"
	case "nummultirange":
		return "pgtype.Multirange[pgtype.Range[pgtype.Numeric]]"
	case "int4range":
		return "pgtype.Range[pgtype.Int4]"
	case "int4multirange":
		return "pgtype.Multirange[pgtype.Range[pgtype.Int4]]"
	case "int8range":
		return "pgtype.Range[pgtype.Int8]"
	case "int8multirange":
		return "pgtype.Multirange[pgtype.Range[pgtype.Int8]]"
	case "hstore":
		return "pgtype.Hstore"
	case "bit", "varbit", "pg_catalog.bit", "pg_catalog.varbit":
		return "pgtype.Bits"
	case "cid", "oid":
		return "pgtype.Uint32"
	case "tid":
		return "pgtype.TID"
	case "xid":
		return "pgtype.Uint32"
	case "xid8":
		return "pgtype.Uint64"
	case "box":
		return "pgtype.Box"
	case "circle":
		return "pgtype.Circle"
	case "line":
		return "pgtype.Line"
	case "lseg":
		return "pgtype.Lseg"
	case "path":
		return "pgtype.Path"
	case "point":
		return "pgtype.Point"
	case "polygon":
		return "pgtype.Polygon"
	case "vector":
		if emitPointersForNull {
			return "*pgvector.Vector"
		}
		return "pgvector.Vector"
	case "void", "any":
		return emptyInterfaceType
	default:
		return r.customPostgresType(columnType, notNull, emitPointersForNull, emitPointersForNullEnums)
	}
}

func (r *typeResolver) customPostgresType(
	columnType string,
	notNull bool,
	emitPointersForNull bool,
	emitPointersForNullEnums bool,
) string {
	parts := strings.Split(columnType, ".")
	var schemaName, typeName string
	switch len(parts) {
	case 1:
		schemaName = r.req.GetCatalog().GetDefaultSchema()
		typeName = parts[0]
	case 2:
		schemaName = parts[0]
		typeName = parts[1]
	case 3:
		schemaName = parts[1]
		typeName = parts[2]
	default:
		return emptyInterfaceType
	}

	for _, schema := range r.req.GetCatalog().GetSchemas() {
		if schema.GetName() != schemaName || skippedSchema(schema.GetName()) {
			continue
		}
		for _, enum := range schema.GetEnums() {
			if enum.GetName() != typeName {
				continue
			}
			name := enum.GetName()
			if schemaName != r.req.GetCatalog().GetDefaultSchema() {
				name = schemaName + "_" + name
			}
			name = structName(name, r.opts)
			if notNull {
				return targetName(r.opts, name)
			}
			if emitPointersForNullEnums {
				return "*" + targetName(r.opts, name)
			}
			return targetName(r.opts, "Null"+name)
		}
		for _, composite := range schema.GetCompositeTypes() {
			if composite.GetName() != typeName {
				continue
			}
			return nullableStdlibType(goTypeString, "*string", "sql.NullString", notNull, emitPointersForNull)
		}
	}
	return emptyInterfaceType
}

func nullableType(
	notNullType string,
	pointerType string,
	pgxType string,
	notNull bool,
	emitPointersForNull bool,
) string {
	if notNull {
		return notNullType
	}
	if emitPointersForNull {
		return pointerType
	}
	return pgxType
}

func nullableStdlibType(notNullType, pointerType, nullType string, notNull, emitPointersForNull bool) string {
	if notNull {
		return notNullType
	}
	if emitPointersForNull {
		return pointerType
	}
	return nullType
}

type override struct {
	GoType   goType `json:"go_type"`
	DBType   string `json:"db_type"`
	Nullable bool   `json:"nullable"`
	Unsigned bool   `json:"unsigned"`
	Column   string `json:"column"`

	typeName     string
	importPath   string
	importAlias  string
	columnName   string
	tableCatalog string
	tableSchema  string
	tableName    string
}

func (o *override) parse(req *plugin.GenerateRequest) error {
	o.DBType = strings.TrimSpace(o.DBType)
	o.Column = strings.TrimSpace(o.Column)
	if o.DBType == "" && o.Column == "" {
		return errors.New("override must specify db_type or column")
	}
	if o.DBType != "" && o.Column != "" {
		return errors.New("override cannot specify both db_type and column")
	}
	parsed, err := o.GoType.parse()
	if err != nil {
		return err
	}
	if err := validateParsedGoType(parsed); err != nil {
		return err
	}
	o.typeName = parsed.typeName
	o.importPath = parsed.importPath
	o.importAlias = parsed.importAlias
	if o.Column != "" {
		if err := o.parseColumn(req); err != nil {
			return err
		}
	}
	return nil
}

func validateParsedGoType(parsed parsedGoType) error {
	if parsed.typeName == "" {
		return errors.New("override go_type must not be empty")
	}
	if parsed.importAlias != "" {
		if err := validateIdentifier("go_type package", parsed.importAlias, false); err != nil {
			return err
		}
	}
	if parsed.importPath != "" {
		if err := validateImportPath("go_type import", parsed.importPath); err != nil {
			return err
		}
	}
	expression, err := parser.ParseExpr(parsed.typeName)
	if err != nil || !validTypeExpression(expression) {
		return fmt.Errorf("go_type %q is not a valid Go type", parsed.typeName)
	}
	if parsed.importPath != "" {
		qualifier := typeQualifier(parsed.typeName)
		if qualifier != "" {
			if err := validateIdentifier("go_type package", qualifier, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func validTypeExpression(expression ast.Expr) bool {
	switch typed := expression.(type) {
	case *ast.Ident:
		return true
	case *ast.InterfaceType:
		return validTypeFields(typed.Methods)
	case *ast.MapType:
		return validTypeExpression(typed.Key) && validTypeExpression(typed.Value)
	case *ast.ChanType:
		return validTypeExpression(typed.Value)
	case *ast.FuncType:
		return validTypeFields(typed.TypeParams) &&
			validTypeFields(typed.Params) &&
			validTypeFields(typed.Results)
	case *ast.StructType:
		return validTypeFields(typed.Fields)
	case *ast.SelectorExpr:
		_, ok := typed.X.(*ast.Ident)
		return ok
	case *ast.StarExpr:
		return validTypeExpression(typed.X)
	case *ast.ArrayType:
		return validTypeExpression(typed.Elt)
	case *ast.IndexExpr:
		return validTypeExpression(typed.X) && validTypeExpression(typed.Index)
	case *ast.IndexListExpr:
		if !validTypeExpression(typed.X) {
			return false
		}
		for _, index := range typed.Indices {
			if !validTypeExpression(index) {
				return false
			}
		}
		return true
	case *ast.ParenExpr:
		return validTypeExpression(typed.X)
	default:
		return false
	}
}

func validTypeFields(fields *ast.FieldList) bool {
	if fields == nil {
		return true
	}
	for _, field := range fields.List {
		if !validTypeExpression(field.Type) {
			return false
		}
	}
	return true
}

func (o *override) parseColumn(req *plugin.GenerateRequest) error {
	parts := strings.Split(o.Column, ".")
	defaultSchema := req.GetCatalog().GetDefaultSchema()
	if defaultSchema == "" {
		defaultSchema = "public"
	}
	switch len(parts) {
	case 2:
		o.tableSchema = defaultSchema
		o.tableName = parts[0]
		o.columnName = parts[1]
	case 3:
		o.tableSchema = parts[0]
		o.tableName = parts[1]
		o.columnName = parts[2]
	case 4:
		o.tableCatalog = parts[0]
		o.tableSchema = parts[1]
		o.tableName = parts[2]
		o.columnName = parts[3]
	default:
		return fmt.Errorf("invalid column override %q", o.Column)
	}
	for _, pattern := range []string{o.tableCatalog, o.tableSchema, o.tableName, o.columnName} {
		if pattern == "" {
			continue
		}
		if _, err := compileMatchPattern(pattern); err != nil {
			return fmt.Errorf("invalid column override %q: %w", o.Column, err)
		}
	}
	return nil
}

func (o *override) matchesColumn(column *plugin.Column, defaultSchema string) bool {
	if o.DBType != "" {
		columnType := dataType(column.GetType())
		notNull := column.GetNotNull() || column.GetIsArray()
		return dbTypesEqual(o.DBType, columnType, defaultSchema) &&
			o.Nullable != notNull &&
			o.Unsigned == column.GetUnsigned()
	}
	if o.Column == "" {
		return false
	}
	table := column.GetTable()
	if table == nil {
		return false
	}
	schema := table.GetSchema()
	if schema == "" {
		schema = defaultSchema
	}
	name := column.GetName()
	if column.GetOriginalName() != "" {
		name = column.GetOriginalName()
	}
	if o.tableCatalog != "" && !matchPattern(o.tableCatalog, table.GetCatalog()) {
		return false
	}
	return matchPattern(o.tableSchema, schema) &&
		matchPattern(o.tableName, table.GetName()) &&
		matchPattern(o.columnName, name)
}

func matchPattern(pattern, value string) bool {
	compiled, err := compileMatchPattern(pattern)
	return err == nil && compiled.MatchString(value)
}

func compileMatchPattern(pattern string) (*regexp.Regexp, error) {
	var expression strings.Builder
	escaped := false
	for _, char := range pattern {
		if escaped {
			escaped = false
			switch char {
			case '*', '?', '\\':
				expression.WriteRune('\\')
				expression.WriteRune(char)
			default:
				return nil, fmt.Errorf("invalid escaped character %q", char)
			}
			continue
		}
		switch char {
		case '\\':
			escaped = true
		case '*':
			expression.WriteString(".*")
		case '?':
			expression.WriteRune('.')
		default:
			expression.WriteString(regexp.QuoteMeta(string(char)))
		}
	}
	if escaped {
		return nil, errors.New("unterminated escape at end of pattern")
	}
	return regexp.Compile("^" + expression.String() + "$")
}

func dbTypesEqual(configured, actual, defaultSchema string) bool {
	if configured == actual {
		return true
	}
	if !strings.Contains(actual, ".") && defaultSchema != "" && configured == defaultSchema+"."+actual {
		return true
	}
	return false
}

type goType struct {
	path    string
	pkg     string
	name    string
	pointer bool
	slice   bool
	spec    string
}

func (g *goType) UnmarshalJSON(data []byte) error {
	var spec string
	if err := json.Unmarshal(data, &spec); err == nil {
		g.spec = spec
		return nil
	}
	var raw struct {
		Path    string `json:"import"`
		Package string `json:"package"`
		Name    string `json:"type"`
		Pointer bool   `json:"pointer"`
		Slice   bool   `json:"slice"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	g.path = raw.Path
	g.pkg = raw.Package
	g.name = raw.Name
	g.pointer = raw.Pointer
	g.slice = raw.Slice
	return nil
}

type parsedGoType struct {
	typeName    string
	importPath  string
	importAlias string
}

func (g goType) parse() (parsedGoType, error) {
	if g.spec != "" {
		return parseGoTypeSpec(g.spec)
	}
	if g.name == "" {
		return parsedGoType{}, errors.New("go_type must specify a nonempty type")
	}
	if g.path == "" && g.pkg != "" {
		return parsedGoType{}, errors.New("go_type package requires an import path")
	}

	typeName := g.name
	pkg := g.pkg
	alias := ""
	if pkg == "" && g.path != "" {
		pkg = packageNameForImport(g.path)
	}
	if pkg != "" {
		typeName = pkg + "." + typeName
		if g.pkg != "" && g.pkg != packageNameForImport(g.path) {
			alias = g.pkg
		}
	}
	if g.pointer {
		typeName = "*" + typeName
	}
	if g.slice {
		typeName = "[]" + typeName
	}
	return parsedGoType{typeName: typeName, importPath: g.path, importAlias: alias}, nil
}

func parseGoTypeSpec(spec string) (parsedGoType, error) {
	lastDot := strings.LastIndex(spec, ".")
	lastSlash := strings.LastIndex(spec, "/")
	if lastDot == -1 && lastSlash == -1 {
		for _, basic := range types.Typ {
			if basic != nil && basic.Info() != 0 && basic.Info()&types.IsUntyped == 0 && basic.Name() == spec {
				return parsedGoType{typeName: spec, importPath: "", importAlias: ""}, nil
			}
		}
		return parsedGoType{}, fmt.Errorf("go_type specifier %q is not a Go basic type", spec)
	}
	if lastDot == -1 {
		return parsedGoType{}, fmt.Errorf("go_type specifier %q must use package.type format", spec)
	}
	typeName := spec[lastSlash+1:]
	typeName = strings.TrimPrefix(typeName, "go-")
	typeName = strings.TrimSuffix(typeName, "-go")
	importPath := spec[:lastDot]
	if strings.HasPrefix(spec, "*") {
		importPath = strings.TrimPrefix(importPath, "*")
		typeName = "*" + strings.TrimPrefix(typeName, "*")
	}
	return parsedGoType{
		typeName:    typeName,
		importPath:  importPath,
		importAlias: "",
	}, nil
}

func dataType(identifier *plugin.Identifier) string {
	if identifier == nil {
		return ""
	}
	if identifier.GetSchema() != "" {
		return identifier.GetSchema() + "." + identifier.GetName()
	}
	return identifier.GetName()
}
