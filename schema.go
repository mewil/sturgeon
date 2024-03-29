package main

import (
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/graphql-go/graphql"
	"log"
	"strings"
	"time"
)

func buildRootSchemaFromMappings(mappings map[string]interface{}, es *elasticsearch.Client) (graphql.Schema, error) {
	fields := buildSchemaFromMappings(mappings, es)
	if len(fields) == 0 {
		return graphql.Schema{}, errors.New("no valid mappings provided, no schemas were built")
	}
	rootQuery := graphql.ObjectConfig{Name: "RootQuery", Fields: fields}
	schemaConfig := graphql.SchemaConfig{Query: graphql.NewObject(rootQuery)}
	return graphql.NewSchema(schemaConfig)
}

func buildSchemaFromMappings(mappings map[string]interface{}, es *elasticsearch.Client) graphql.Fields {
	fields := graphql.Fields{}
	for index, mapping := range mappings {
		schemas := buildSchemasFromMapping(
			index,
			mapping.(map[string]interface{})["mappings"].(map[string]interface{}),
			es,
		)
		for _, schema := range schemas {
			schema := schema // pin https://github.com/golang/go/wiki/CommonMistakes#using-reference-to-loop-iterator-variable
			fields[schema.Name] = &schema
		}
	}
	return fields
}

func buildSchemasFromMapping(index string, mapping map[string]interface{}, es *elasticsearch.Client) []graphql.Field {
	schemas := make([]graphql.Field, 0, 3)
	properties, ok := mapping["properties"].(map[string]interface{})
	if !ok {
		log.Print("mapping for index ", index, " did not contain properties, ignoring")
		return schemas
	}
	addName(index)
	index = getGraphQLName(index)
	start := time.Now()
	documentType := buildDocumentTypeFromMapping(index, properties)
	log.Print("built document type for ", index, " in ", time.Since(start))
	start = time.Now()
	booleanArgs := buildBooleanQueryTypes(index, properties)
	log.Print("built boolean argument type for ", index, " in ", time.Since(start))

	schemas = append(schemas, graphql.Field{
		Name: index,
		Type: graphql.NewList(documentType),
		Args: graphql.FieldConfigArgument{
			"size": &graphql.ArgumentConfig{
				Type: graphql.NewNonNull(graphql.Int),
			},
			"boolean_query": booleanArgs,
		},
		Resolve: NewDocumentListResolver(index, es),
	})
	schemas = append(schemas, graphql.Field{
		Name: index + "_by_id",
		Type: documentType,
		Args: graphql.FieldConfigArgument{
			"id": &graphql.ArgumentConfig{
				Type: graphql.NewNonNull(graphql.ID),
			},
		},
		Resolve: NewDocumentResolver(index, es),
	})
	if Config.EnableAggregations {
		start = time.Now()
		documentAggregationType := buildDocumentAggregationTypeFromMapping(index, properties)
		log.Print("built document aggregation type for ", index, " in ", time.Since(start))
		schemas = append(schemas, graphql.Field{
			Name: index + "_aggregations",
			Type: documentAggregationType,
			Args: graphql.FieldConfigArgument{
				"boolean_query": booleanArgs,
			},
			Resolve: NewAggregationResolver(index, es),
		})
	}
	return schemas
}

func buildDocumentTypeFromMapping(index string, mapping map[string]interface{}) *graphql.Object {
	fields := getFields(mapping, nil)
	fields["id"] = &graphql.Field{
		Name:    "id",
		Type:    graphql.ID,
		Args:    nil,
		Resolve: nil,
	}
	return graphql.NewObject(graphql.ObjectConfig{
		Name:   index + "_document",
		Fields: fields,
	})
}

type AggregationType string

const (
	AggregationTypeMin         AggregationType = "min"
	AggregationTypeMax         AggregationType = "max"
	AggregationTypeAvg         AggregationType = "avg"
	AggregationTypeSum         AggregationType = "sum"
	AggregationTypeCardinality AggregationType = "cardinality"
	AggregationTypePercentiles AggregationType = "percentiles"
)

var aggregationTypes = []AggregationType{
	AggregationTypeMin,
	AggregationTypeMax,
	AggregationTypeAvg,
	AggregationTypeSum,
	AggregationTypeCardinality,
	AggregationTypePercentiles,
}

func buildDocumentAggregationTypeFromMapping(index string, mapping map[string]interface{}) *graphql.Object {
	fields := make(graphql.Fields)
	for _, aggregationType := range aggregationTypes {
		subFields := getFields(mapping, &aggregationType)

		name := index + "_" + string(aggregationType) + "_aggregation_document"
		fields[string(aggregationType)] = &graphql.Field{
			Name: name,
			Type: graphql.NewObject(graphql.ObjectConfig{
				Name:   name,
				Fields: subFields,
			}),
			//Args: nil,
			//Resolve: NewAggregationResolver(index, es),
		}
	}
	return graphql.NewObject(graphql.ObjectConfig{
		Name:   index + "_aggregation_document",
		Fields: fields,
	})
}

func buildKeyValueFloatType() graphql.Type {
	fields := make(graphql.Fields)
	fields["key"] = &graphql.Field{
		Name: "key",
		Type: graphql.Float,
	}
	fields["value"] = &graphql.Field{
		Name: "value",
		Type: graphql.Float,
	}
	return graphql.NewObject(graphql.ObjectConfig{
		Name:   "key_value",
		Fields: fields,
	})
}

var keyValueType = buildKeyValueFloatType()

func getFieldType(scalarType string, aggregationType *AggregationType) (graphql.Output, error) {
	if aggregationType == nil {
		return mapToGraphQLScalarType(scalarType)
	}
	switch *aggregationType {
	case AggregationTypeCardinality:
		return graphql.Int, nil
	case AggregationTypePercentiles:
		return graphql.NewList(keyValueType), nil
	default:
		return mapToGraphQLScalarType(scalarType)
	}
}

func getFields(mapping map[string]interface{}, aggregationType *AggregationType) graphql.Fields {
	fields := make(graphql.Fields)
	for name, field := range mapping {
		if Config.FieldIgnorePrefix != "" && strings.HasPrefix(name, Config.FieldIgnorePrefix) {
			continue
		}
		addName(name)
		graphQLName := getGraphQLName(name)
		// TODO: don't ignore complex types
		fieldInfo := field.(map[string]interface{})
		fieldTypeString, ok := fieldInfo["type"].(string)
		if !ok {
			continue
		}
		fieldType, err := getFieldType(fieldTypeString, aggregationType)
		if err == nil {
			fields[graphQLName] = &graphql.Field{
				Name:    graphQLName,
				Type:    fieldType,
				Args:    nil,
				Resolve: nil,
			}
		}
	}
	return fields
}

func mapToGraphQLScalarType(scalarType string) (*graphql.Scalar, error) {
	switch scalarType {
	case "float":
		return graphql.Float, nil
	case "long":
		return graphql.Int, nil
	case "string":
		return graphql.String, nil
	case "date":
		return graphql.DateTime, nil
	case "boolean":
		return graphql.Boolean, nil
	default:
		return nil, fmt.Errorf("no scalar type %s", scalarType)
	}
}
