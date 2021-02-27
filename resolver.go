package main

import (
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"log"
	"time"
)

func NewDocumentListResolver(index string, es *elasticsearch.Client) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		selectedFields, includeIdField := getSelectedFieldsFromQuery(p.Info.Operation.GetSelectionSet().Selections[0].(*ast.Field).GetSelectionSet())
		size, _ := getSizeArgument(p.Args)
		res, err := query(es, p.Context, index, size, selectedFields, p.Args)
		if err != nil {
			log.Print(err)
			return nil, err
		}
		docs := convertSourceDocumentsToQueryResult(res, includeIdField)
		return docs, nil
	}
}

func NewDocumentResolver(index string, es *elasticsearch.Client) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		selectedFields, includeIdField := getSelectedFieldsFromQuery(p.Info.Operation.GetSelectionSet().Selections[0].(*ast.Field).GetSelectionSet())
		// TODO: move parse id argument to helper
		res, err := queryById(es, p.Context, index, p.Args["id"].(string), selectedFields)
		if err != nil {
			log.Print(err)
			return nil, err
		}
		doc := convertSourceDocumentsToQueryResult([]interface{}{res}, includeIdField)[0]
		return doc, nil
	}
}

func NewAggregationResolver(index string, es *elasticsearch.Client) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		for _, topLevelSelection := range p.Info.Operation.GetSelectionSet().Selections {
			if p.Info.FieldName == topLevelSelection.(*ast.Field).Name.Value {
				results := map[string]interface{}{}
				for _, selection := range topLevelSelection.(*ast.Field).GetSelectionSet().Selections {
					aggregationName := selection.(*ast.Field).Name.Value
					selectedFields, _ := getSelectedFieldsFromQuery(selection.(*ast.Field).GetSelectionSet())
					res, err := queryAggregation(es, p.Context, index, selectedFields, p.Args, AggregationType(aggregationName))
					if err != nil {
						log.Print(err)
						return nil, err
					}
					results[aggregationName] = convertSourceToQueryResult(res)
				}
				return results, nil
			}
		}
		return nil, fmt.Errorf("operation for field %s did not match any found in the query selection set", p.Info.FieldName)
	}
}

func getSelectedFieldsFromQuery(selectionSet *ast.SelectionSet) ([]string, bool) {
	selectedFields := make([]string, 0, len(selectionSet.Selections)+1)
	includesIdField := false
	for _, selection := range selectionSet.Selections {
		field, ok := selection.(*ast.Field)
		if !ok {
			continue
		}
		name := field.Name.Value
		if name == "id" {
			includesIdField = true
		}
		selectedFields = append(selectedFields, getOriginalName(name))
	}
	return selectedFields, includesIdField
}

func convertSourceDocumentsToQueryResult(documents []interface{}, includeIdField bool) []interface{} {
	results := make([]interface{}, len(documents))
	for i, document := range documents {
		doc := document.(map[string]interface{})
		source := doc["_source"].(map[string]interface{})
		fields := convertSourceToQueryResult(source)
		if includeIdField {
			fields["id"] = doc["_id"]
		}
		results[i] = fields
	}
	return results
}

func convertSourceToQueryResult(source map[string]interface{}) map[string]interface{} {
	fields := make(map[string]interface{}, len(source))
	for key, value := range source {
		if key == "@timestamp" {
			t, err := time.Parse(time.RFC3339, value.(string))
			if err != nil {
				continue
			}
			value = t
		}
		fields[getGraphQLName(key)] = value
	}
	return fields
}
