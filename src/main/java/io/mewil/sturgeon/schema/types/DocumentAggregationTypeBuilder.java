package io.mewil.sturgeon.schema.types;

import graphql.Scalars;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import io.mewil.sturgeon.ElasticsearchClient;
import io.mewil.sturgeon.schema.util.NameNormalizer;
import io.mewil.sturgeon.schema.util.QueryFieldSelector;
import io.mewil.sturgeon.schema.util.QueryFieldSelectorResult;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;

import java.util.*;
import java.util.stream.Collectors;

public class DocumentAggregationTypeBuilder extends TypeBuilder {

    private final Map.Entry<String, Map<String, Object>> indexMapping;

    public DocumentAggregationTypeBuilder(Map.Entry<String, Map<String, Object>> indexMapping) {
        this.indexMapping = indexMapping;
    }

    @Override
    public GraphQLObjectType build() {
        return new GraphQLObjectType.Builder()
                .name(String.format("agg_%s", indexMapping.getKey()))
                .fields(buildAggregateFieldDefinitionsFromIndexMapping(indexMapping.getKey(), indexMapping.getValue()))
                .build();
    }


    private List<GraphQLFieldDefinition> buildAggregateFieldDefinitionsFromIndexMapping(
            final String index, final Map<String, Object> mapping) {
        return Arrays.stream(AggregationType.values())
                .map(agg -> buildFieldDefinitionsForAggregateType(index, mapping, agg))
                .collect(Collectors.toList());
    }

    private static GraphQLFieldDefinition buildFieldDefinitionsForAggregateType(final String index,
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
                        .map(field -> FieldDefinitionBuilder.fromIndexMappingField(Scalars.GraphQLFloat, field))
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


    private static DataFetcher<Map<String, Object>> getDataFetcherForIndexAggregation(final String index,
                                                                              final AggregationType aggregationType) {
        return dataFetchingEnvironment -> {
            final QueryFieldSelectorResult selectorResult =
                    QueryFieldSelector.getSelectedFieldsFromQuery(dataFetchingEnvironment);
            final SearchResponse response = ElasticsearchClient.getInstance().queryWithAggregation(index, selectorResult.getFields(), aggregationType);
            return response.getAggregations().asMap().entrySet().stream()
                    .map(DocumentAggregationTypeBuilder::normalizeAggregationTypes)
                    .collect(Collectors.toMap(entry -> NameNormalizer.getInstance().getGraphQLName(entry.getKey()), Map.Entry::getValue));
        };
    }

    private static Map.Entry<String, Object> normalizeAggregationTypes(final Map.Entry<String, Aggregation> field) {
        return switch (field.getValue().getType()) {
            case "avg" -> new HashMap.SimpleEntry<>(field.getKey(), ((ParsedAvg) field.getValue()).getValue());
            case "max" -> new HashMap.SimpleEntry<>(field.getKey(), ((ParsedMax) field.getValue()).getValue());
            default -> new HashMap.SimpleEntry<>(field.getKey(), ((ParsedMin) field.getValue()).getValue());
        };
    }
}
