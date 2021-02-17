package main

import (
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/handler"
	"github.com/kelseyhightower/envconfig"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type appConfig struct {
	ElasticsearchHosts               []string `split_words:"true"`
	ElasticsearchIndexIncludePattern string   `split_words:"true"`
	FieldIgnorePrefix                string   `split_words:"true"`
	EnableAggregationSchema          bool     `split_words:"true" default:"true"`
}

var Config = appConfig{}

func init() {
	if err := envconfig.Process("sturgeon", &Config); err != nil {
		log.Fatal(err)
	}
}

func allowCorsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		next.ServeHTTP(w, r)
	})
}

func main() {
	fmt.Println(Config)
	cfg := elasticsearch.Config{
		Addresses: Config.ElasticsearchHosts,
		//Username: "foo",
		//Password: "bar",
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
			//TLSClientConfig: &tls.Config{
			//	MinVersion: tls.VersionTLS11,
			//},
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	res, err := es.Indices.GetMapping(
		es.Indices.GetMapping.WithIndex("*"),
	)
	if err != nil {
		panic(err)
	}
	//ctx := context.Background()
	//res, _ := req.Do(ctx, es)
	if res.IsError() {
		log.Fatal("error", res.String())
	}
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %s", err)
	}

	start := time.Now()
	schema, err := buildSchemaFromMappings(r, es)
	if err != nil {
		panic(err)
	}
	log.Print("building schemas took ", time.Since(start))

	h := handler.New(&handler.Config{
		Schema:     &schema,
		Pretty:     true,
		GraphiQL:   false,
		Playground: true,
	})

	http.Handle("/graphql", allowCorsMiddleware(h))
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func buildSchemaFromMappings(mappings map[string]interface{}, es *elasticsearch.Client) (graphql.Schema, error) {
	wg := sync.WaitGroup{}
	schemas := make(chan *graphql.Field)
	for index, mapping := range mappings {
		wg.Add(1)
		go buildSchemasFromMapping(
			index,
			mapping.(map[string]interface{})["mappings"].(map[string]interface{}),
			es,
			schemas,
			&wg,
		)
	}
	fields := graphql.Fields{}
	go func() {
		wg.Wait()
		close(schemas)
	}()
	for schema := range schemas {
		schema := schema // pin https://github.com/golang/go/wiki/CommonMistakes#using-reference-to-loop-iterator-variable
		fields[schema.Name] = schema
	}

	rootQuery := graphql.ObjectConfig{Name: "RootQuery", Fields: fields}
	schemaConfig := graphql.SchemaConfig{Query: graphql.NewObject(rootQuery)}
	return graphql.NewSchema(schemaConfig)
}

func buildSchemasFromMapping(index string, mapping map[string]interface{}, es *elasticsearch.Client, schemas chan<- *graphql.Field, wg *sync.WaitGroup) {
	properties := mapping["properties"].(map[string]interface{})
	start := time.Now()
	documentType := buildDocumentTypeFromMapping(index, properties)
	log.Print("built document type for ", index, " took ", time.Since(start))
	start = time.Now()
	booleanArgs := buildBooleanQueryTypes(index, properties)
	log.Print("built boolean argument type for ", index, " took ", time.Since(start))

	schemas <- &graphql.Field{
		Name: index,
		Type: graphql.NewList(documentType),
		Args: graphql.FieldConfigArgument{
			"size": &graphql.ArgumentConfig{
				Type:        graphql.NewNonNull(graphql.Int),
				Description: "",
			},
			"boolean_query": booleanArgs,
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			selectedFields, includeIdField := getSelectedFieldsFromQuery(p.Info.Operation.GetSelectionSet().Selections[0].(*ast.Field).GetSelectionSet())
			size, _ := getSizeArgument(p.Args)
			res := query(es, p.Context, index, size, selectedFields, p.Args)
			docs := convertSourceDocumentsToQueryResult(res, includeIdField)
			return docs, nil
		},
		Description: "",
	}
	schemas <- &graphql.Field{
		Name: index + "_by_id",
		Type: documentType,
		Args: graphql.FieldConfigArgument{
			"id": &graphql.ArgumentConfig{
				Type:        graphql.NewNonNull(graphql.ID),
				Description: "",
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			selectedFields, includeIdField := getSelectedFieldsFromQuery(p.Info.Operation.GetSelectionSet().Selections[0].(*ast.Field).GetSelectionSet())
			res := queryById(es, p.Context, index, p.Args["id"].(string), selectedFields)
			doc := convertSourceDocumentsToQueryResult([]interface{}{res}, includeIdField)[0]
			return doc, nil
		},
		Description: "",
	}
	if Config.EnableAggregationSchema {
		start = time.Now()
		documentAggregationType := buildDocumentAggregationTypeFromMapping(index, properties, es)
		log.Print("built document aggregation type for ", index, " took ", time.Since(start))
		schemas <- &graphql.Field{
			Name: index + "_aggregations",
			Type: documentAggregationType,
			Args: graphql.FieldConfigArgument{
				"boolean_query": booleanArgs,
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return p.Info.Operation.GetSelectionSet().Selections, nil
			},
			Description: "",
		}
	}
	wg.Done()
}

func buildDocumentTypeFromMapping(index string, mapping map[string]interface{}) *graphql.Object {
	fields := getFields(mapping)
	fields["id"] = &graphql.Field{
		Name:    "id",
		Type:    graphql.ID,
		Args:    nil,
		Resolve: nil,
		//DeprecationReason: "",
		Description: "",
	}
	return graphql.NewObject(graphql.ObjectConfig{
		Name:        index + "document",
		Fields:      fields,
		Description: "",
		//Interfaces:  nil,
		//IsTypeOf:    nil,
	})
}

type AggregationType string

const (
	AggregationTypeMin AggregationType = "min"
	AggregationTypeMax AggregationType = "max"
	AggregationTypeAvg AggregationType = "avg"
)

var (
	aggregationTypes = []AggregationType{
		AggregationTypeMin,
		AggregationTypeMax,
		AggregationTypeAvg,
	}
)

func buildDocumentAggregationTypeFromMapping(index string, mapping map[string]interface{}, es *elasticsearch.Client) *graphql.Object {
	fields := make(graphql.Fields)
	subFields := getFields(mapping)
	for _, aggregationType := range aggregationTypes {
		fields[string(aggregationType)] = &graphql.Field{
			Name: index + "agg" + string(aggregationType),
			Type: graphql.NewObject(graphql.ObjectConfig{
				Name:        index + "agg" + string(aggregationType),
				Fields:      subFields,
				Description: "",
				//Interfaces:  nil,
				//IsTypeOf:    nil,
			}),
			Args: nil,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				selectionSet := p.Info.Operation.GetSelectionSet().Selections[0].(*ast.Field).GetSelectionSet().Selections[0].(*ast.Field).GetSelectionSet()
				selectedFields, _ := getSelectedFieldsFromQuery(selectionSet)
				size, _ := getSizeArgument(p.Args)
				res := queryAggregation(es, p.Context, index, size, selectedFields, AggregationType(p.Info.FieldName))
				doc := convertSourceToQueryResult(res)
				return doc, nil
			},
			Description: "",
		}
	}
	return graphql.NewObject(graphql.ObjectConfig{
		Name:        index + "agg",
		Fields:      fields,
		Description: "",
		//Interfaces:  nil,
		//IsTypeOf:    nil,
	})
}

func getFields(mapping map[string]interface{}) graphql.Fields {
	fields := make(graphql.Fields)
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
		fields[graphQLName] = &graphql.Field{
			Name:        graphQLName,
			Type:        mapToGraphQLScalarType(fieldType),
			Args:        nil,
			Resolve:     nil,
			Description: "",
		}
	}
	return fields
}

func mapToGraphQLScalarType(scalarType string) *graphql.Scalar {
	switch scalarType {
	case "float":
		return graphql.Float
	case "long":
		return graphql.Int
	case "string":
		return graphql.String
	case "date":
		return graphql.DateTime
	case "boolean":
		return graphql.Boolean
	default:
		return nil
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
