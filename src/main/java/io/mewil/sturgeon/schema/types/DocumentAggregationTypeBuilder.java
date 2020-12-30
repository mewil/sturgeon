package io.mewil.sturgeon.schema.types;

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import io.mewil.sturgeon.schema.resolver.DocumentAggregationDataFetcherBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
                        .map(field -> FieldDefinitionBuilder.aggregateFieldDefinitionForDocumentField(field, aggregationType))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        // .sorted() TODO: implement comparator
                        .collect(Collectors.toList()))
                .build();

        return new GraphQLFieldDefinition.Builder()
                .name(aggregationType.getName())
                .type(doc)
                .dataFetcher(new DocumentAggregationDataFetcherBuilder(index, aggregationType).build())
                .build();
    }
}
