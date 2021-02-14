package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/handler"
	"log"
	"net"
	"net/http"
	"time"
)

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
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:55002",
		},
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
	req := esapi.IndicesGetMappingRequest{
		//Index:             nil,
		//DocumentType:      nil,
		//AllowNoIndices:    nil,
		//ExpandWildcards:   "",
		//IgnoreUnavailable: nil,
		//IncludeTypeName:   nil,
		//Local:             nil,
		//MasterTimeout:     0,
		//Pretty:            false,
		//Human:             false,
		//ErrorTrace:        false,
		//FilterPath:        nil,
		//Header:            nil,
	}
	ctx := context.Background()
	res, _ := req.Do(ctx, es)
	if res.IsError() {
		log.Fatal("error")
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
	_ = http.ListenAndServe(":8080", nil)
}

func buildSchemaFromMappings(mappings map[string]interface{}, es *elasticsearch.Client) (graphql.Schema, error) {
	schemas := graphql.Fields{}
	for index, mapping := range mappings {
		for _, schema := range buildSchemasFromMapping(index, mapping.(map[string]interface{})["mappings"].(map[string]interface{}), es) {
			schema := schema //pin
			schemas[schema.Name] = &schema
		}
	}
	rootQuery := graphql.ObjectConfig{Name: "RootQuery", Fields: schemas}
	schemaConfig := graphql.SchemaConfig{Query: graphql.NewObject(rootQuery)}
	return graphql.NewSchema(schemaConfig)
}

func buildSchemasFromMapping(index string, mapping map[string]interface{}, es *elasticsearch.Client) []graphql.Field {
	documentType := buildDocumentTypeFromMapping(index, mapping["properties"].(map[string]interface{}))
	documentAggregationType := buildDocumentAggregationTypeFromMapping(index, mapping["properties"].(map[string]interface{}), es)

	documentListSchema := graphql.Field{
		Name: index,
		Type: graphql.NewList(documentType),
		Args: graphql.FieldConfigArgument{
			"size": &graphql.ArgumentConfig{
				Type:        graphql.NewNonNull(graphql.Int),
				Description: "",
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			selectedFields, includeIdField := getSelectedFieldsFromQuery(p.Info.Operation.GetSelectionSet().Selections[0].(*ast.Field).GetSelectionSet())
			size, _ := getSizeArgument(p.Args)
			res := query(es, p.Context, index, size, selectedFields)
			docs := convertSourceDocumentsToQueryResult(res, includeIdField)
			return docs, nil
		},
		Description: "",
	}
	documentByIdSchema := graphql.Field{
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
	documentAggregationSchema := graphql.Field{
		Name: index + "_aggregations",
		Type: documentAggregationType,
		Args: nil,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return p.Info.Operation.GetSelectionSet().Selections, nil
		},
		Description: "",
	}

	return []graphql.Field{
		documentListSchema,
		documentByIdSchema,
		documentAggregationSchema,
	}
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
		fields[getGraphQLName(key)] = value
	}
	return fields
}

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

func buildDocumentTypeFromMapping(index string, mapping map[string]interface{}) *graphql.Object {
	fields := getFields(mapping)
	fields["id"] = &graphql.Field{
		Name:              "id",
		Type:              graphql.ID,
		Args:              nil,
		Resolve:           nil,
		DeprecationReason: "",
		Description:       "",
	}
	return graphql.NewObject(graphql.ObjectConfig{
		Name:        index + "document",
		Fields:      fields,
		Description: "",
		//Name:        "",
		//Interfaces:  nil,
		//Fields:      nil,
		//IsTypeOf:    nil,
		//Description: "",
	})
}

type AggregationType string

const (
	AggregationTypeMin AggregationType = "min"
	AggregationTypeMax AggregationType = "max"
	AggregationTypeAvg AggregationType = "avg"
)

func buildDocumentAggregationTypeFromMapping(index string, mapping map[string]interface{}, es *elasticsearch.Client) *graphql.Object {
	aggregationTypes := []AggregationType{
		AggregationTypeMin,
		AggregationTypeMax,
		AggregationTypeAvg,
	}
	fields := make(graphql.Fields)
	for _, aggregationType := range aggregationTypes {
		fields[string(aggregationType)] = &graphql.Field{
			Name: index + "agg" + string(aggregationType),
			Type: graphql.NewObject(graphql.ObjectConfig{
				Name:        index + "agg" + string(aggregationType),
				Fields:      getFields(mapping),
				Description: "",
				//Name:        "",
				//Interfaces:  nil,
				//Fields:      nil,
				//IsTypeOf:    nil,
				//Description: "",
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
		//Name:        "",
		//Interfaces:  nil,
		//Fields:      nil,
		//IsTypeOf:    nil,
		//Description: "",
	})
}

// TODO: Add support for nested types. Type will be null if we have a nested type, ignore for now.

//
//private static Optional<GraphQLScalarType> getScalarTypeFromField(
//final Map.Entry<String, Object> field) {
//// TODO: Refactor unchecked cast
//final Object type = ((HashMap<String, Object>) field.getValue()).get(SchemaConstants.TYPE);
//if (type == null) {
//return Optional.empty();
//}
//return Optional.ofNullable(ElasticsearchDecoder.mapToGraphQLScalarType(type.toString()));
//}

func getFields(mapping map[string]interface{}) graphql.Fields {
	fields := make(graphql.Fields)
	for name, field := range mapping {
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

//
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

//final DataFetchingEnvironment dataFetchingEnvironment) {
//final List<String> selectedGraphQLFields =
//dataFetchingEnvironment.getSelectionSet().getFields().stream()
//.filter(f -> List.of("/key", "/value").stream().noneMatch(s -> f.getQualifiedName().endsWith(s)))
//.map(SelectedField::getName)
//.collect(Collectors.toList());
//final boolean includeId = selectedGraphQLFields.remove(SchemaConstants.ID);
//final List<String> selectedIndexFields =
//selectedGraphQLFields.stream()
//.map(f -> NameNormalizer.getInstance().getOriginalName(f))
//.collect(Collectors.toList());
//return QueryFieldSelectorResult.builder()
//.fields(selectedIndexFields)
//.includeId(includeId)
//.build();
//}

//func fieldDefinitionForDocumentField(fieldName string) graphql.Field {
//	scalar := mapToGraphQLScalarType()
//	graphql.Fie
//}

//public static Optional<GraphQLFieldDefinition> fieldDefinitionForDocumentField(
//final Map.Entry<String, Object> field) {
//final Optional<GraphQLScalarType> scalarType = getScalarTypeFromField(field);
//if (scalarType.isEmpty()) {
//return Optional.empty();
//}
//NameNormalizer.getInstance().addName(field.getKey());
//return Optional.of(
//GraphQLFieldDefinition.newFieldDefinition()
//.name(NameNormalizer.getInstance().getGraphQLName(field.getKey()))
//.type(scalarType.get())
//.build());
//}
//
//private static final ImmutableSet<GraphQLScalarType> SUPPORTED_AGGREGATION_SCALARS =
//ImmutableSet.of(
//Scalars.GraphQLFloat, Scalars.GraphQLShort, Scalars.GraphQLInt, Scalars.GraphQLLong);
//
//public static Optional<GraphQLFieldDefinition> aggregateFieldDefinitionForDocumentField(
//final Map.Entry<String, Object> field, final AggregationType aggregationType) {
//
//final Optional<GraphQLScalarType> scalarType = getScalarTypeFromField(field);
//if (scalarType.isEmpty() || !SUPPORTED_AGGREGATION_SCALARS.contains(scalarType.get())) {
//return Optional.empty();
//}
//
//final GraphQLFieldDefinition.Builder builder = GraphQLFieldDefinition.newFieldDefinition()
//.name(NameNormalizer.getInstance().getGraphQLName(field.getKey()));
//
//switch (aggregationType) {
//case PERCENTILES:
//return Optional.of(builder
//.type(GraphQLList.list(getKeyedResponseType(scalarType.get())))
//.build());
//default:
//return Optional.of(builder
//.type(scalarType.get())
//.build());
//}
//}
//
//private static GraphQLObjectType getKeyedResponseType(final GraphQLScalarType scalarType) {
//return keyedResponseTypes.computeIfAbsent(scalarType, type -> GraphQLObjectType.newObject()
//.name(String.format("keyed_%s", scalarType.getName().toLowerCase()))
//.field(GraphQLFieldDefinition.newFieldDefinition()
//.name("key")
//.type(Scalars.GraphQLString)
//.build())
//.field(GraphQLFieldDefinition.newFieldDefinition()
//.name("value")
//.type(type)
//.build())
//.build());
//}
