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

var (
	booleanQueryTypes = []BooleanQueryType{
		BooleanQueryTypeMust,
		BooleanQueryTypeMustNot,
		BooleanQueryTypeFilter,
		BooleanQueryTypeShould,
	}
)

func buildBooleanQueryTypes(index string, mapping map[string]interface{}) *graphql.ArgumentConfig {
	fields := make(graphql.InputObjectConfigFieldMap)
	for _, booleanQueryType := range booleanQueryTypes {
		subFields := make(graphql.InputObjectConfigFieldMap)

		for name, field := range mapping {
			if strings.HasPrefix(name, "raw") {
				continue
			}
			addName(name)
			graphQLName := getGraphQLName(name)
			fieldInfo := field.(map[string]interface{})
			fieldType, ok := fieldInfo["type"].(string)
			if !ok {
				continue
			}
			scalarType := mapToGraphQLScalarType(fieldType)
			subFields[graphQLName] = &graphql.InputObjectFieldConfig{
				Type:         buildTermLevelQueryArgumentTypes(index, graphQLName, scalarType, booleanQueryType),
				DefaultValue: nil,
				Description:  "",
			}
		}
		st := graphql.NewInputObject(graphql.InputObjectConfig{
			Name:        index + "_boolean_args+" + string(booleanQueryType),
			Fields:      subFields,
			Description: "",
		})
		fields[string(booleanQueryType)] = &graphql.InputObjectFieldConfig{
			Type:         st,
			DefaultValue: nil,
			Description:  "",
		}
	}

	t := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:        index + "_boolean_args",
		Fields:      fields,
		Description: "",
	})
	return &graphql.ArgumentConfig{
		Type:        t,
		Description: "",
	}

}

func buildTermLevelQueryArgumentTypes(index, field string, scalarType *graphql.Scalar, queryType BooleanQueryType) *graphql.InputObject {
	fields := make(graphql.InputObjectConfigFieldMap)

	fields["range"] = &graphql.InputObjectFieldConfig{
		Type:         buildRangeQueryArgumentType(index, field, scalarType, queryType),
		DefaultValue: nil,
		Description:  "",
	}

	return graphql.NewInputObject(graphql.InputObjectConfig{
		Name:        "term_level_" + index + "_" + field + string(queryType),
		Fields:      fields,
		Description: "",
	})
}

func buildRangeQueryArgumentType(index, field string, scalarType *graphql.Scalar, queryType BooleanQueryType) *graphql.InputObject {
	fields := make(graphql.InputObjectConfigFieldMap)

	t := &graphql.InputObjectFieldConfig{
		Type:         scalarType,
		DefaultValue: nil,
		Description:  "",
	}
	for _, rangeQueryType := range []string{"lt", "lte", "gt", "gte"} {
		fields[rangeQueryType] = t
	}

	return graphql.NewInputObject(graphql.InputObjectConfig{
		Name:        field + "range_args_" + index + string(queryType),
		Fields:      fields,
		Description: "",
	})
}
