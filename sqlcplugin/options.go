package sqlcplugin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/sqlc-dev/plugin-sdk-go/plugin"
)

// options are the supported sqlc plugin options for the store wrapper generator.
type options struct {
	// OutputFileName is the generated store file name and the base for related files.
	OutputFileName string `json:"output_file_name"`
	// PackageName is the package clause used in generated code.
	PackageName string `json:"package"`
	// InternalImportPath imports sqlc output when wrappers use a separate package.
	InternalImportPath string `json:"internal_import_path"`
	// InternalImportAlias overrides the imported sqlc package qualifier.
	InternalImportAlias string `json:"internal_import_alias"`
	// ExportSQLCTypes aliases sqlc-owned types used by the generated store API.
	ExportSQLCTypes bool `json:"export_sqlc_types"`
	// ConstructorName is the public config-driven store constructor name.
	ConstructorName string `json:"constructor"`
	// IgnoreMirrorError makes generated writes discard non-NoRows mirror errors.
	IgnoreMirrorError bool `json:"ignore_mirror_error"`
	// StoreInterfaceName is the generated public query interface name.
	StoreInterfaceName string `json:"store_interface"`
	// RuntimeImportPath is the import path for the pgmesh runtime package.
	RuntimeImportPath string `json:"runtime_import_path"`
	// ResolverInterfaceName is the generated shard resolver interface name.
	ResolverInterfaceName string `json:"resolver_interface"`
	// ShardedConstructor creates an opaque sharded Topology.
	ShardedConstructor string `json:"sharded_constructor"`

	// SQLPackage selects the sqlc database driver; only pgx/v5 is supported.
	SQLPackage string `json:"sql_package"`
	// QueryParameterLimit matches sqlc's threshold for generating parameter structs.
	QueryParameterLimit *int32 `json:"query_parameter_limit"`
	// EmitParamsStructPointers matches sqlc's parameter-struct pointer setting.
	EmitParamsStructPointers bool `json:"emit_params_struct_pointers"`
	// EmitResultStructPointers matches sqlc's result-struct pointer setting.
	EmitResultStructPointers bool `json:"emit_result_struct_pointers"`
	// EmitPointersForNullTypes matches sqlc's nullable-type pointer setting.
	EmitPointersForNullTypes bool `json:"emit_pointers_for_null_types"`
	// EmitPointersForNullEnumType overrides pointer emission for nullable enums.
	EmitPointersForNullEnumType *bool `json:"emit_pointers_for_null_enum_types"`
	// EmitExactTableNames preserves table names when deriving generated Go types.
	EmitExactTableNames bool `json:"emit_exact_table_names"`
	// EmitJSONTags matches sqlc's JSON struct-tag emission.
	EmitJSONTags bool `json:"emit_json_tags"`
	// Rename maps SQL identifiers to generated Go identifiers.
	Rename map[string]string `json:"rename"`
	// Overrides applies sqlc-compatible database and column type overrides.
	Overrides []override `json:"overrides"`
}

func parseOptions(req *plugin.GenerateRequest) (*options, error) {
	if req.GetSettings().GetEngine() != "postgresql" {
		return nil, fmt.Errorf(
			"engine %q is unsupported; pgmesh v1 requires postgresql",
			req.GetSettings().GetEngine(),
		)
	}
	opts := &options{} //nolint:exhaustruct_v5 // Populated from JSON and normalized with defaults below.
	if len(req.GetPluginOptions()) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(req.GetPluginOptions()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(opts); err != nil {
			return nil, fmt.Errorf("unmarshal plugin options: %w", err)
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return nil, errors.New("unmarshal plugin options: expected one JSON object")
		}
	}

	if opts.OutputFileName == "" {
		opts.OutputFileName = defaultOutputFileName
	}
	if opts.PackageName == "" {
		opts.PackageName = outputPackageName(req)
	}
	if opts.SQLPackage == "" {
		opts.SQLPackage = defaultSQLPackage
	}
	if opts.QueryParameterLimit == nil {
		limit := int32(1)
		opts.QueryParameterLimit = &limit
	}
	if opts.ConstructorName == "" {
		opts.ConstructorName = defaultStoreNew
	}
	if opts.StoreInterfaceName == "" {
		opts.StoreInterfaceName = defaultStoreInterface
	}
	if opts.RuntimeImportPath == "" {
		opts.RuntimeImportPath = defaultRuntimePackage
	}
	if opts.ResolverInterfaceName == "" {
		opts.ResolverInterfaceName = defaultResolver
	}
	if opts.ShardedConstructor == "" {
		opts.ShardedConstructor = defaultShardedNew
	}
	if *opts.QueryParameterLimit < 0 {
		return nil, errors.New("query_parameter_limit must not be negative")
	}
	if opts.SQLPackage != defaultSQLPackage {
		return nil, fmt.Errorf("sql_package %q is unsupported; pgmesh v1 requires pgx/v5", opts.SQLPackage)
	}
	if err := validateOptions(opts); err != nil {
		return nil, err
	}

	for idx := range opts.Overrides {
		if err := opts.Overrides[idx].parse(req); err != nil {
			return nil, fmt.Errorf("parse override %d: %w", idx, err)
		}
	}
	if err := validateOverrideImports(opts); err != nil {
		return nil, err
	}

	return opts, nil
}

func validateOverrideImports(opts *options) error {
	reserved := map[string]string{
		defaultContext:       defaultContext,
		defaultErrorsPackage: defaultErrorsPackage,
		defaultFMT:           defaultFMT,
		defaultMetricAlias:   defaultMetric,
		defaultRuntimeAlias:  opts.RuntimeImportPath,
		defaultPGXAlias:      defaultPGXPackage,
		defaultSlogAlias:     defaultSlog,
		defaultSQLQualifier:  defaultDatabaseSQL,
		defaultTraceAlias:    defaultTrace,
	}
	if opts.InternalImportPath != "" {
		reserved[internalQualifier(opts)] = opts.InternalImportPath
	}
	seen := make(map[string]string)
	seenPaths := make(map[string]string)
	for index := range opts.Overrides {
		override := &opts.Overrides[index]
		if override.importPath == "" {
			continue
		}
		qualifier := override.importAlias
		if qualifier == "" {
			qualifier = typeQualifier(override.typeName)
		}
		if qualifier == "" {
			qualifier = packageNameForImport(override.importPath)
		}
		if path, exists := reserved[qualifier]; exists && path != override.importPath {
			return fmt.Errorf(
				"override %d import alias %q conflicts with generated import %q",
				index,
				qualifier,
				path,
			)
		}
		for generatedQualifier, path := range reserved {
			if path == override.importPath && generatedQualifier != qualifier {
				return fmt.Errorf(
					"override %d import %q conflicts with generated alias %q",
					index,
					override.importPath,
					generatedQualifier,
				)
			}
		}
		if path, exists := seen[qualifier]; exists && path != override.importPath {
			return fmt.Errorf(
				"override %d import alias %q conflicts with override import %q",
				index,
				qualifier,
				path,
			)
		}
		if previousQualifier, exists := seenPaths[override.importPath]; exists &&
			previousQualifier != qualifier {
			return fmt.Errorf(
				"override %d import %q conflicts with override alias %q",
				index,
				override.importPath,
				previousQualifier,
			)
		}
		seen[qualifier] = override.importPath
		seenPaths[override.importPath] = qualifier
	}
	return nil
}

func validateOptions(opts *options) error {
	if filepath.Base(opts.OutputFileName) != opts.OutputFileName ||
		strings.ContainsAny(opts.OutputFileName, `/\`) ||
		opts.OutputFileName == "." ||
		opts.OutputFileName == ".." ||
		filepath.Ext(opts.OutputFileName) != ".go" ||
		strings.HasPrefix(opts.OutputFileName, ".") ||
		strings.HasPrefix(opts.OutputFileName, "_") ||
		strings.HasSuffix(opts.OutputFileName, "_test.go") {
		return fmt.Errorf(
			"output_file_name %q must be a visible base filename ending in .go and not _test.go",
			opts.OutputFileName,
		)
	}
	if err := validateIdentifier("package", opts.PackageName, false); err != nil {
		return err
	}
	if err := validateImportPath("runtime_import_path", opts.RuntimeImportPath); err != nil {
		return err
	}
	if opts.InternalImportPath != "" {
		if err := validateImportPath("internal_import_path", opts.InternalImportPath); err != nil {
			return err
		}
	}
	if opts.InternalImportAlias != "" {
		if opts.InternalImportPath == "" {
			return errors.New("internal_import_alias requires internal_import_path")
		}
		if err := validateIdentifier("internal_import_alias", opts.InternalImportAlias, false); err != nil {
			return err
		}
		if slices.Contains([]string{
			defaultContext,
			defaultErrorsPackage,
			defaultFMT,
			defaultMetricAlias,
			defaultRuntimeAlias,
			defaultPGXAlias,
			defaultSlogAlias,
			defaultSQLQualifier,
			defaultTraceAlias,
		}, opts.InternalImportAlias) {
			return fmt.Errorf("internal_import_alias %q conflicts with a generated import", opts.InternalImportAlias)
		}
	}
	if opts.ExportSQLCTypes && opts.InternalImportPath == "" {
		return errors.New("export_sqlc_types requires internal_import_path")
	}
	type generatedImport struct {
		qualifier string
		path      string
	}
	generatedImports := []generatedImport{
		{qualifier: defaultContext, path: defaultContext},
		{qualifier: defaultErrorsPackage, path: defaultErrorsPackage},
		{qualifier: defaultFMT, path: defaultFMT},
		{qualifier: defaultMetricAlias, path: defaultMetric},
		{qualifier: defaultRuntimeAlias, path: opts.RuntimeImportPath},
		{qualifier: defaultPGXAlias, path: defaultPGXPackage},
		{qualifier: defaultSlogAlias, path: defaultSlog},
		{qualifier: defaultSQLQualifier, path: defaultDatabaseSQL},
		{qualifier: defaultTraceAlias, path: defaultTrace},
	}
	if opts.InternalImportPath != "" {
		generatedImports = append(generatedImports, generatedImport{
			qualifier: internalQualifier(opts),
			path:      opts.InternalImportPath,
		})
	}
	importPaths := make(map[string]string, len(generatedImports))
	for _, generatedImport := range generatedImports {
		if previous, exists := importPaths[generatedImport.path]; exists &&
			previous != generatedImport.qualifier {
			return fmt.Errorf(
				"generated import alias %q conflicts with %q for path %q",
				generatedImport.qualifier,
				previous,
				generatedImport.path,
			)
		}
		importPaths[generatedImport.path] = generatedImport.qualifier
	}
	publicNames := []struct {
		option string
		value  string
	}{
		{option: constructorOption, value: opts.ConstructorName},
		{option: "store_interface", value: opts.StoreInterfaceName},
		{option: "resolver_interface", value: opts.ResolverInterfaceName},
		{option: "sharded_constructor", value: opts.ShardedConstructor},
	}
	seenNames := make(map[string]string, len(publicNames))
	fixedDeclarations := fixedGeneratedDeclarations(opts.InternalImportPath == "")
	reservedNames := make(map[string]struct{}, len(fixedDeclarations))
	for declaration := range fixedDeclarations {
		reservedNames[declaration] = struct{}{}
	}
	for _, publicName := range publicNames {
		if err := validateIdentifier(publicName.option, publicName.value, true); err != nil {
			return err
		}
		if _, reserved := reservedNames[publicName.value]; reserved {
			return fmt.Errorf("%s %q conflicts with a generated declaration", publicName.option, publicName.value)
		}
		if previous, exists := seenNames[publicName.value]; exists {
			return fmt.Errorf(
				"%s %q conflicts with %s",
				publicName.option,
				publicName.value,
				previous,
			)
		}
		seenNames[publicName.value] = publicName.option
	}
	for source, renamed := range opts.Rename {
		if source == "" {
			return errors.New("rename contains an empty SQL identifier")
		}
		if err := validateIdentifier(fmt.Sprintf("rename[%q]", source), renamed, false); err != nil {
			return err
		}
	}
	return nil
}

func validateImportPath(option, path string) error {
	if path == "" ||
		strings.TrimSpace(path) != path ||
		strings.ContainsAny(path, "\"\\") ||
		strings.IndexFunc(path, func(r rune) bool {
			return unicode.IsSpace(r) || unicode.IsControl(r)
		}) >= 0 {
		return fmt.Errorf("%s %q is invalid", option, path)
	}
	return nil
}

func validateIdentifier(option, value string, exported bool) error {
	if !validRouteIdentifier(value) || goKeywords[value] {
		return fmt.Errorf("%s %q must be a valid Go identifier", option, value)
	}
	first, _ := utf8.DecodeRuneInString(value)
	if exported && !unicode.IsUpper(first) {
		return fmt.Errorf("%s %q must be an exported Go identifier", option, value)
	}
	return nil
}

func outputPackageName(req *plugin.GenerateRequest) string {
	if codegen := req.GetSettings().GetCodegen(); codegen != nil && codegen.GetOut() != "" {
		name := filepath.Base(codegen.GetOut())
		if validRouteIdentifier(name) && !goKeywords[name] {
			return name
		}
	}
	return defaultPackageName
}
