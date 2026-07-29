package sqlcplugin

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"sort"
	"strings"

	"github.com/sqlc-dev/plugin-sdk-go/plugin"
)

func collectSQLCTypeAliases(
	opts *options,
	queries []generatedQuery,
	catalog *plugin.Catalog,
) ([]string, error) {
	if !opts.ExportSQLCTypes {
		return nil, nil
	}

	qualifier := internalQualifier(opts)
	aliases := make(map[string]struct{})
	addType := func(typ string) error {
		if typ == "" {
			return nil
		}
		expression, err := parser.ParseExpr(typ)
		if err != nil {
			return fmt.Errorf("parse generated Go type %q for sqlc aliases: %w", typ, err)
		}
		ast.Inspect(expression, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			ident, ok := selector.X.(*ast.Ident)
			if ok && ident.Name == qualifier && ast.IsExported(selector.Sel.Name) {
				aliases[selector.Sel.Name] = struct{}{}
			}
			return true
		})
		return nil
	}
	addFields := func(value queryValue) error {
		if value.structType == nil {
			return nil
		}
		for _, field := range value.structType.fields {
			if err := addType(field.typ); err != nil {
				return err
			}
		}
		return nil
	}

	for _, query := range queries {
		for _, param := range query.params {
			if err := addType(param.typ); err != nil {
				return nil, err
			}
		}
		for _, result := range query.results {
			if err := addType(result); err != nil {
				return nil, err
			}
		}
		if query.route != nil {
			for _, operand := range query.route.operands {
				if err := addType(operand.typ); err != nil {
					return nil, err
				}
			}
		}
		if query.shardArgs != nil {
			for _, field := range query.shardArgs.fields {
				if err := addType(field.typ); err != nil {
					return nil, err
				}
			}
		}
		if query.arg.emit {
			if err := addType(query.arg.defineType(opts)); err != nil {
				return nil, err
			}
		}
		if err := addFields(query.arg); err != nil {
			return nil, err
		}
		if err := addType(query.ret.defineType(opts)); err != nil {
			return nil, err
		}
		if err := addFields(query.ret); err != nil {
			return nil, err
		}
	}
	for _, schema := range catalog.GetSchemas() {
		if skippedSchema(schema.GetName()) {
			continue
		}
		for _, enum := range schema.GetEnums() {
			name := enum.GetName()
			if schema.GetName() != catalog.GetDefaultSchema() {
				name = schema.GetName() + "_" + name
			}
			name = structName(name, opts)
			if _, usesNullableEnum := aliases["Null"+name]; usesNullableEnum {
				aliases[name] = struct{}{}
			}
		}
	}

	names := make([]string, 0, len(aliases))
	for name := range aliases {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func validateSQLCTypeAliases(
	opts *options,
	queries []generatedQuery,
	groups []storeGroup,
	aliases []string,
) error {
	if len(aliases) == 0 {
		return nil
	}

	declarations := generatedDeclarations(opts)
	for _, group := range groups {
		declarations[group.name] = "store group " + group.name
		declarations[storeReaderInterfaceName(group.name)] = "store group " + group.name
		declarations[storeWriterInterfaceName(group.name)] = "store group " + group.name
		declarations[storeFactoryOptionName(group.name)] = "store group " + group.name
	}
	for _, query := range queries {
		if query.storeParamAlias != nil {
			declarations[query.storeParamAlias.name] = "store parameter alias for " + query.methodName
		}
		if query.shardArgs != nil {
			declarations[query.shardArgs.name] = "shard parameter wrapper for " + query.methodName
		}
	}

	for _, alias := range aliases {
		if declaration, exists := declarations[alias]; exists {
			return fmt.Errorf(
				"export_sqlc_types cannot alias sqlc type %s because it conflicts with %s",
				alias,
				declaration,
			)
		}
	}
	return nil
}

func writeSQLCTypeAliases(out *bytes.Buffer, opts *options, aliases []string) {
	for _, alias := range aliases {
		fmt.Fprintf(out, "// %s re-exports %s from the sqlc package.\n", alias, alias)
		fmt.Fprintf(out, "type %s = %s.%s\n\n", alias, internalQualifier(opts), alias)
	}
}

func exportedSQLCType(opts *options, typ string) string {
	if !opts.ExportSQLCTypes {
		return typ
	}
	return strings.ReplaceAll(typ, internalQualifier(opts)+".", "")
}

func exportedSQLCTypes(opts *options, types []string) []string {
	exported := make([]string, len(types))
	for index, typ := range types {
		exported[index] = exportedSQLCType(opts, typ)
	}
	return exported
}

func exportedSQLCArguments(opts *options, arguments []argument) []argument {
	exported := make([]argument, len(arguments))
	for index, arg := range arguments {
		exported[index] = argument{name: arg.name, typ: exportedSQLCType(opts, arg.typ)}
	}
	return exported
}
