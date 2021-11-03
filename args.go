package main

import (
	"fmt"
	"github.com/graphql-go/graphql"
	"strings"
)

func getSizeArgument(args map[string]interface{}) (int, error) {
	rawSize, ok := args["size"]
	if !ok {
		return 0, fmt.Errorf("argument size not found")
	}
	size, ok := rawSize.(int)
	if !ok {
		return 0, fmt.Errorf("argument size was not an int")
	}
	return size, nil
}

type BooleanQueryType string

const (
	BooleanQueryTypeMust    BooleanQueryType = "must"
	BooleanQueryTypeMustNot BooleanQueryType = "must_not"
	BooleanQueryTypeFilter  BooleanQueryType = "filter"
	BooleanQueryTypeShould  BooleanQueryType = "should"
)

var booleanQueryTypes = []BooleanQueryType{
	BooleanQueryTypeMust,
	BooleanQueryTypeMustNot,
	BooleanQueryTypeFilter,
	BooleanQueryTypeShould,
}

func buildBooleanQueryTypes(index string, mapping map[string]interface{}) *graphql.ArgumentConfig {
	fields := make(graphql.InputObjectConfigFieldMap)
	for _, booleanQueryType := range booleanQueryTypes {
		subFields := make(graphql.InputObjectConfigFieldMap)
		for name, field := range mapping {
			// TODO: consolidate shared logic with getFields method
			if Config.FieldIgnorePrefix != "" && strings.HasPrefix(name, Config.FieldIgnorePrefix) {
				continue
			}
			addName(name)
			graphQLName := getGraphQLName(name)
			fieldInfo := field.(map[string]interface{})
			fieldType, ok := fieldInfo["type"].(string)
			if !ok {
				continue
			}
			scalarType, err := mapToGraphQLScalarType(fieldType)
			if err == nil {
				subFields[graphQLName] = &graphql.InputObjectFieldConfig{
					Type:         buildTermLevelQueryArgumentTypes(index, graphQLName, scalarType, booleanQueryType),
					DefaultValue: nil,
				}
			}
		}
		st := graphql.NewInputObject(graphql.InputObjectConfig{
			Name:   index + "_boolean_args" + string(booleanQueryType),
			Fields: subFields,
		})
		fields[string(booleanQueryType)] = &graphql.InputObjectFieldConfig{
			Type:         st,
			DefaultValue: nil,
		}
	}

	t := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   index + "_boolean_args",
		Fields: fields,
	})
	return &graphql.ArgumentConfig{
		Type: t,
	}

}

func buildTermLevelQueryArgumentTypes(index, field string, scalarType *graphql.Scalar, queryType BooleanQueryType) *graphql.InputObject {
	fields := make(graphql.InputObjectConfigFieldMap)

	fields["range"] = &graphql.InputObjectFieldConfig{
		Type:         buildRangeQueryArgumentType(index, field, scalarType, queryType),
		DefaultValue: nil,
	}

	return graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   "term_level_" + index + "_" + field + string(queryType),
		Fields: fields,
	})
}

func buildRangeQueryArgumentType(index, field string, scalarType *graphql.Scalar, queryType BooleanQueryType) *graphql.InputObject {
	fields := make(graphql.InputObjectConfigFieldMap)
	t := &graphql.InputObjectFieldConfig{
		Type: scalarType,
	}
	for _, rangeQueryType := range []string{"lt", "lte", "gt", "gte"} {
		fields[rangeQueryType] = t
	}
	return graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   field + "range_args_" + index + string(queryType),
		Fields: fields,
	})
}
