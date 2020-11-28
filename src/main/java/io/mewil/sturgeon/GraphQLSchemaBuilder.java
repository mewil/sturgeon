package io.mewil.sturgeon;

import graphql.Scalars;
import graphql.scalars.ExtendedScalars;
import graphql.schema.*;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GraphQLSchemaBuilder {

    private static final String TYPE = "type";
    private static final String BOOLEAN_QUERY = "boolean_query";
    private static final String ID = "id";
    private static final String SIZE = "size";
    public static GraphQLScalarType UUID = new UUIDScalar();

    private final ElasticsearchClient elasticsearchClient;
    private final Map<String, String> normalizedNameLookupTable = new HashMap<>();
    private final Map<String, String> nameLookupTable = new HashMap<>();

    public GraphQLSchemaBuilder(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    public GraphQLSchema getSchema() throws IOException {
        return buildSchemaFromIndexMappings(elasticsearchClient.getMappings());
    }

    public GraphQLSchema buildSchemaFromIndexMappings(Map<String, Map<String, Object>> mappings) {
        GraphQLObjectType query = new GraphQLObjectType.Builder()
                .name("Query")
                .fields(buildSchemasFromIndexMappings(mappings))
                .build();
        return new GraphQLSchema.Builder()
                .query(query)
                .build();
    }

    private List<GraphQLFieldDefinition> buildSchemasFromIndexMappings(Map<String, Map<String, Object>> mappings) {
        return mappings.entrySet().stream()
                .flatMap(this::buildSchemasFromIndexMapping)
                // .sorted() TODO: implement comparator
                .collect(Collectors.toList());
    }

    private Stream<GraphQLFieldDefinition> buildSchemasFromIndexMapping(Map.Entry<String, Map<String, Object>> mapping) {
        final GraphQLObjectType doc = new GraphQLObjectType.Builder()
                .name(String.format("doc_%s", mapping.getKey()))
                .fields(buildFieldDefinitionsFromIndexMapping(mapping.getKey(), mapping.getValue()))
                .build();
        final GraphQLArgument queryArguments = GraphQLArgument.newArgument()
                .name(BOOLEAN_QUERY)
                .type(buildBooleanQueryArguments(mapping.getKey(), mapping.getValue()))
                .build();

        final GraphQLObjectType aggregateDoc = new GraphQLObjectType.Builder()
                .name(String.format("agg_%s", mapping.getKey()))
                .fields(buildAggregateFieldDefinitionsFromIndexMapping(mapping.getKey(), mapping.getValue()))
                .build();
        return Stream.of(
                new GraphQLFieldDefinition.Builder()
                        .name(normalizeGraphQLName(mapping.getKey()))
                        .type(GraphQLList.list(doc))
                        .dataFetcher(getDataFetcherForIndex(mapping.getKey()))
                        .argument(GraphQLArgument.newArgument()
                                .name(SIZE)
                                .type(GraphQLNonNull.nonNull(Scalars.GraphQLInt))
                                .build())
                        .argument(queryArguments)
                        .build(),
                new GraphQLFieldDefinition.Builder()
                        .name(String.format("%s_by_id", normalizeGraphQLName(mapping.getKey())))
                        .type(doc)
                        .dataFetcher(getDataFetcherForIndexById(mapping.getKey()))
                        .argument(GraphQLArgument.newArgument()
                                .name(ID)
                                .type(GraphQLNonNull.nonNull(UUID))
                                .build())
                        .build(),
                new GraphQLFieldDefinition.Builder()
                        .name(String.format("%s_aggregations", normalizeGraphQLName(mapping.getKey())))
                        .type(aggregateDoc)
                        .dataFetcher(environment -> environment.getSelectionSet().getFields())
                        .build()
                );
    }

    private GraphQLInputType buildBooleanQueryArguments(String index, Map<String, Object> mapping) {
        return GraphQLInputObjectType.newInputObject()
                .fields(Arrays.stream(BooleanQueryType.values())
                        .map(booleanQueryType -> GraphQLInputObjectField.newInputObjectField()
                                .type(GraphQLInputObjectType.newInputObject()
                                        .fields(mapping.entrySet().stream()
                                                     .flatMap(entry -> {
                                                        // TODO: Refactor unchecked cast
                                                        final Map<String, Object> properties = (HashMap<String, Object>) entry.getValue();
                                                        return properties.entrySet().stream();
                                                    })
                                                    .map(e -> buildBooleanQueryArgument(index, e, booleanQueryType))
                                                    .filter(Optional::isPresent)
                                                    .map(Optional::get)
                                                    // .sorted() TODO: implement comparator
                                                    .collect(Collectors.toList())
                                        )
                                        .name(String.format("boolean_query_%s_%s", booleanQueryType.getName(), index))
                                )
                                .name(booleanQueryType.getName())
                                .build())
                        .collect(Collectors.toList())
                )
                .name(String.format("boolean_query_%s", index))
                .build();
    }

    private GraphQLInputObjectType getComparisonArguments(final String name, final GraphQLScalarType type) {
        final List<GraphQLInputObjectField> fields = new ArrayList<>();
        if (Scalars.GraphQLFloat.equals(type) || ExtendedScalars.DateTime.equals(type)) {
            fields.addAll(List.of(
                    GraphQLInputObjectField.newInputObjectField()
                            .name("gt")
                            .type(Scalars.GraphQLFloat)
                            .build(),
                    GraphQLInputObjectField.newInputObjectField()
                            .name("gte")
                            .type(Scalars.GraphQLFloat)
                            .build(),
                    GraphQLInputObjectField.newInputObjectField()
                            .name("lt")
                            .type(Scalars.GraphQLFloat)
                            .build(),
                    GraphQLInputObjectField.newInputObjectField()
                            .name("lte")
                            .type(Scalars.GraphQLFloat)
                            .build()
            ));
        }
        return GraphQLInputObjectType.newInputObject()
                .fields(fields)
                .name(String.format("boolean_query_%s", normalizeGraphQLName(name)))
                .build();
    }

    private Optional<GraphQLInputObjectField> buildBooleanQueryArgument(final String index,
                                                                        final Map.Entry<String, Object> field,
                                                                        final BooleanQueryType booleanQueryType) {
        // TODO: Refactor unchecked cast
        // TODO: Add support for nested types. Type will be null if we have a nested type, ignore for now.
        final Object type = ((HashMap<String, Object>) field.getValue()).get(TYPE);
        if (type == null) {
            return Optional.empty();
        }
        final GraphQLScalarType scalarType = getGraphQLType(type.toString());
        if (Scalars.GraphQLFloat.equals(scalarType) || ExtendedScalars.DateTime.equals(scalarType)) {
            return Optional.of(GraphQLInputObjectField
                    .newInputObjectField()
                    .type(getComparisonArguments(index+booleanQueryType.getName()+normalizeGraphQLName(field.getKey()), scalarType))
                    .name(normalizeGraphQLName(field.getKey()))
                    .build());
        }
        return Optional.empty();
    }

    private List<GraphQLFieldDefinition> buildAggregateFieldDefinitionsFromIndexMapping(
            final String index, final Map<String, Object> mapping) {
        return Arrays.stream(AggregationType.values())
                .map(agg -> buildFieldDefinitionsForAggregateType(index, mapping, agg))
                .collect(Collectors.toList());
    }

    private GraphQLFieldDefinition buildFieldDefinitionsForAggregateType(final String index,
                                                                         final Map<String, Object> mapping,
                                                                         final AggregationType aggregationType) {
        final GraphQLObjectType doc = new GraphQLObjectType.Builder()
                .name(String.format("%s_%s", aggregationType.getName(), index))
                .fields(mapping.entrySet().stream()
                    .flatMap(entry -> {
                        // TODO: Refactor unchecked cast
                        final Map<String, Object> properties = (HashMap<String, Object>) entry.getValue();
                        return properties.entrySet().stream();
                    })
                    .map(field -> buildFieldDefinitionFromIndexMappingField(Scalars.GraphQLFloat, field))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    // .sorted() TODO: implement comparator
                    .collect(Collectors.toList()))
                .build();

        return new GraphQLFieldDefinition.Builder()
                .name(aggregationType.getName())
                .type(doc)
                .dataFetcher(getDataFetcherForIndexAggregation(index, aggregationType))
                .build();
    }

    public DataFetcher<Map<String, Object>> getDataFetcherForIndexAggregation(final String index,
                                                                              final AggregationType aggregationType) {
        return dataFetchingEnvironment -> {
            final Tuple<List<String>, Boolean> queryData = getSelectFieldsFromQuery(dataFetchingEnvironment);
            final SearchResponse response = elasticsearchClient.queryWithAggregation(index, queryData.v1(), aggregationType);
            return response.getAggregations().asMap().entrySet().stream()
                    .map(this::normalizeAggregationTypes)
                    .collect(Collectors.toMap(entry -> nameLookupTable.get(entry.getKey()), Map.Entry::getValue));
        };
    }


    private Map.Entry<String, Object> normalizeAggregationTypes(final Map.Entry<String, Aggregation> field) {
        switch (field.getValue().getType()) {
            case "avg":
                return new HashMap.SimpleEntry<>(field.getKey(), ((ParsedAvg) field.getValue()).getValue());
            case "max":
                return new HashMap.SimpleEntry<>(field.getKey(), ((ParsedMax) field.getValue()).getValue());
            case "min":
            default:
                return new HashMap.SimpleEntry<>(field.getKey(), ((ParsedMin) field.getValue()).getValue());
        }
    }


    private List<GraphQLFieldDefinition> buildFieldDefinitionsFromIndexMapping(String index, Map<String, Object> mapping) {
        final List<GraphQLFieldDefinition> fields = mapping.entrySet().stream()
                .flatMap(entry -> {
                    // TODO: Refactor unchecked cast
                    final Map<String, Object> properties = (HashMap<String, Object>) entry.getValue();
                    return properties.entrySet().stream();
                })
                .map(field -> buildFieldDefinitionFromIndexMappingField(null, field))
                .filter(Optional::isPresent)
                .map(Optional::get)
                // .sorted() TODO: implement comparator
                .collect(Collectors.toList());
        fields.add(GraphQLFieldDefinition.newFieldDefinition()
                        .name(ID)
                        .type(UUID)
                        .build());
        return fields;
    }

    private Optional<GraphQLFieldDefinition> buildFieldDefinitionFromIndexMappingField(final GraphQLScalarType typeFilter,
                                                                                       final Map.Entry<String, Object> field) {
        // TODO: Refactor unchecked cast
        // TODO: Add support for nested types. Type will be null if we have a nested type, ignore for now.
        final Object type = ((HashMap<String, Object>) field.getValue()).get(TYPE);
        if (type == null) {
            return Optional.empty();
        }
        GraphQLScalarType scalarType = getGraphQLType(type.toString());
        if (typeFilter != null && scalarType != typeFilter)  {
            return Optional.empty();
        }
        final String normalizedName = normalizeGraphQLName(field.getKey());
        normalizedNameLookupTable.put(normalizedName, field.getKey());
        nameLookupTable.put(field.getKey(), normalizedName);
        return Optional.of(GraphQLFieldDefinition.newFieldDefinition()
                .name(normalizedName)
                .type(scalarType)
                .build());
    }

    public DataFetcher<List<Map<String, Object>>> getDataFetcherForIndex(final String index) {
        return dataFetchingEnvironment -> {
            final Tuple<List<String>, Boolean> queryData = getSelectFieldsFromQuery(dataFetchingEnvironment);
            final int size = dataFetchingEnvironment.getArgument(SIZE);
            final SearchHits hits = elasticsearchClient.query(index, size, queryData.v1(), buildQueryFromArguments(dataFetchingEnvironment)).getHits();

            return Arrays.stream(hits.getHits())
                    .map(hit -> decodeElasticsearchDoc(queryData.v2(), hit.getSourceAsMap(), hit.getId()))
                    .collect(Collectors.toList());
        };
    }

    public List<QueryBuilder> buildQueryFromArguments(final DataFetchingEnvironment dataFetchingEnvironment) {
        final List<QueryBuilder> queryBuilders = new ArrayList<>();
        dataFetchingEnvironment.getArguments().forEach((key, value) -> {
            switch (key) {
                case BOOLEAN_QUERY:
                    // TODO: Refactor unchecked cast
                    queryBuilders.add(buildBooleanQuery((Map<String, Object>) value));
                    break;
            }
        });
        return queryBuilders;
    }

    private BoolQueryBuilder buildBooleanQuery(final Map<String, Object> booleanOccurrenceTypes) {
        final BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        booleanOccurrenceTypes.forEach((key, value) -> {
            // TODO: Refactor unchecked cast
            Map<String, Object> map = ((Map<String, Object>) value);
            switch (BooleanQueryType.valueOf(key.toUpperCase())) {
                case FILTER:
                    map.entrySet().forEach(e -> boolQueryBuilder.filter(buildRangeQuery(e)));
            }
        });
        return boolQueryBuilder;
    }

    private RangeQueryBuilder buildRangeQuery(final Map.Entry<String, Object> field) {
        final RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(normalizedNameLookupTable.get(field.getKey()));
        ((Map<String, Object>) field.getValue()).forEach((key, value) -> {
            switch (key) {
                case "gt"  -> rangeQueryBuilder.gt(value);
                case "gte" -> rangeQueryBuilder.gte(value);
                case "lt" -> rangeQueryBuilder.lt(value);
                case "lte" -> rangeQueryBuilder.lte(value);
            }
        });
        return rangeQueryBuilder;
    }

    public DataFetcher<Map<String, Object>> getDataFetcherForIndexById(final String index) {
        return dataFetchingEnvironment -> {
            final Tuple<List<String>, Boolean> queryData = getSelectFieldsFromQuery(dataFetchingEnvironment);
            final String id = dataFetchingEnvironment.getArgument(ID).toString();
            final GetResponse response = elasticsearchClient.queryById(index, id, queryData.v1());

            if (response.isSourceEmpty()) {
                return null;
            }
            return decodeElasticsearchDoc(queryData.v2(), response.getSourceAsMap(), response.getId());
        };
    }

    private Map<String, Object> decodeElasticsearchDoc(boolean includeId, Map<String, Object> sourceAsMap, String id) {
        final Map<String, Object> result = sourceAsMap.entrySet().stream()
                .collect(Collectors.toMap(entry -> nameLookupTable.get(entry.getKey()), Map.Entry::getValue));
        if (includeId) {
            result.put(ID, id);
        }
        return result;
    }

    private Tuple<List<String>, Boolean> getSelectFieldsFromQuery(final DataFetchingEnvironment dataFetchingEnvironment) {
        final List<String> selectedGraphQLFields = dataFetchingEnvironment.getSelectionSet().getFields().stream()
                .map(SelectedField::getName)
                .collect(Collectors.toList());
        final boolean includeId = selectedGraphQLFields.remove(ID);
        final List<String> selectedIndexFields = selectedGraphQLFields.stream()
                .map(normalizedNameLookupTable::get)
                .collect(Collectors.toList());
        return new Tuple<>(selectedIndexFields, includeId);
    }


    public static GraphQLScalarType getGraphQLType(final String type) {
        return switch (type) {
            case "float" -> Scalars.GraphQLFloat;
            case "long" -> Scalars.GraphQLLong;
            case "string" -> Scalars.GraphQLString;
            case "date" -> ExtendedScalars.DateTime;
            case "boolean" -> Scalars.GraphQLBoolean;
            default -> null;
        };
    }

    private static String normalizeGraphQLName(final String name) {
        return name
                .replaceAll("(^[0-9])", "_$1")
                .replace(" ", "_")
                .replace("+", "plus_")
                .replace("-", "minus_")
                .replace("@", "")
                .replace("#", "");
    }
}
