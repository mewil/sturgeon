package io.mewil.elasticgraphql;

import graphql.Scalars;
import graphql.schema.*;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GraphQLSchemaBuilder {

    private static final String TYPE = "type";
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
        return Stream.of(
                new GraphQLFieldDefinition.Builder()
                        .name(normalizeGraphQLName(mapping.getKey()))
                        .type(GraphQLList.list(doc))
                        .dataFetcher(getDataFetcherForIndex(mapping.getKey()))
                        .argument(GraphQLArgument.newArgument()
                                .name(SIZE)
                                .type(GraphQLNonNull.nonNull(Scalars.GraphQLInt))
                                .build())
                        .build(),
                new GraphQLFieldDefinition.Builder()
                        .name(String.format("%s_by_id", normalizeGraphQLName(mapping.getKey())))
                        .type(doc)
                        .dataFetcher(getDataFetcherForIndexById(mapping.getKey()))
                        .argument(GraphQLArgument.newArgument()
                                .name(ID)
                                .type(GraphQLNonNull.nonNull(UUID))
                                .build())
                        .build()
                );
    }

    private List<GraphQLFieldDefinition> buildFieldDefinitionsFromIndexMapping(String index, Map<String, Object> mapping) {
        final List<GraphQLFieldDefinition> fields = mapping.entrySet().stream()
                .flatMap(entry -> {
                    // TODO: Refactor unchecked cast
                    final Map<String, Object> properties = (HashMap<String, Object>) entry.getValue();
                    return properties.entrySet().stream();
                })
                .map(this::buildFieldDefinitionFromIndexMappingField)
                .filter(Objects::nonNull)
                // .sorted() TODO: implement comparator
                .collect(Collectors.toList());
        fields.add(GraphQLFieldDefinition.newFieldDefinition()
                        .name(ID)
                        .type(UUID)
                        .build());
        return fields;
    }

    private GraphQLFieldDefinition buildFieldDefinitionFromIndexMappingField(final Map.Entry<String, Object> field) {
        // TODO: Refactor unchecked cast
        // TODO: Add support for nested types. Type will be null if we have a nested type, ignore for now.
        final Object type = ((HashMap<String, Object>) field.getValue()).get(TYPE);
        if (type == null) {
            return null;
        }
        final String normalizedName = normalizeGraphQLName(field.getKey());
        normalizedNameLookupTable.put(normalizedName, field.getKey());
        nameLookupTable.put(field.getKey(), normalizedName);
        return GraphQLFieldDefinition.newFieldDefinition()
                .name(normalizedName)
                .type(getGraphQLType(type.toString()))
                .build();
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

    public DataFetcher<List<Map<String, Object>>> getDataFetcherForIndex(final String index) {
        return dataFetchingEnvironment -> {
            final Tuple<List<String>, Boolean> queryData = getSelectFieldsFromQuery(dataFetchingEnvironment);
            final int size = dataFetchingEnvironment.getArgument(SIZE);
            final SearchHits hits = elasticsearchClient.query(index, size, queryData.v1()).getHits();

            return Arrays.stream(hits.getHits())
                    .map(hit -> decodeElasticsearchDoc(queryData.v2(), hit.getSourceAsMap(), hit.getId()))
                    .collect(Collectors.toList());
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
            case "string", "date" -> Scalars.GraphQLString;
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
