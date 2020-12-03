package io.mewil.sturgeon.schema;

import graphql.schema.*;
import io.mewil.sturgeon.ElasticsearchClient;
import io.mewil.sturgeon.schema.argument.BooleanQueryArgumentBuilder;
import io.mewil.sturgeon.schema.argument.IdArgumentBuilder;
import io.mewil.sturgeon.schema.argument.SizeArgumentBuilder;
import io.mewil.sturgeon.schema.resolver.IndexByIdDataFetcherBuilder;
import io.mewil.sturgeon.schema.resolver.IndexDataFetcherBuilder;
import io.mewil.sturgeon.schema.types.DocumentAggregationTypeBuilder;
import io.mewil.sturgeon.schema.types.DocumentTypeBuilder;
import io.mewil.sturgeon.schema.util.NameNormalizer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class GraphQLSchemaBuilder {

    private GraphQLSchemaBuilder() {
    }

    public static GraphQLSchema getSchema() throws IOException {
        return buildSchemaFromIndexMappings(ElasticsearchClient.getInstance().getMappings());
    }

    private static GraphQLSchema buildSchemaFromIndexMappings(Map<String, Map<String, Object>> mappings) {
        GraphQLObjectType query = new GraphQLObjectType.Builder()
                .name("Query")
                .fields(buildSchemasFromIndexMappings(mappings))
                .build();
        return new GraphQLSchema.Builder()
                .query(query)
                .build();
    }

    private static List<GraphQLFieldDefinition> buildSchemasFromIndexMappings(Map<String, Map<String, Object>> mappings) {
        return mappings.entrySet().stream()
                .flatMap(GraphQLSchemaBuilder::buildSchemasFromIndexMapping)
                // .sorted() TODO: implement comparator
                .collect(Collectors.toList());
    }

    private static Stream<GraphQLFieldDefinition> buildSchemasFromIndexMapping(Map.Entry<String, Map<String, Object>> indexMapping) {
        NameNormalizer.getInstance().addName((indexMapping.getKey()));
        final String normalizedIndexName = NameNormalizer.getInstance().getGraphQLName(indexMapping.getKey());
        final GraphQLObjectType documentType = new DocumentTypeBuilder(indexMapping).build();
        final GraphQLObjectType aggregateDocumentType = new DocumentAggregationTypeBuilder(indexMapping).build();
        final GraphQLArgument booleanQueryArguments = new BooleanQueryArgumentBuilder(indexMapping).build();
        return Stream.of(
                new GraphQLFieldDefinition.Builder()
                        .name(NameNormalizer.getInstance().getGraphQLName((indexMapping.getKey())))
                        .type(GraphQLList.list(documentType))
                        .argument(new SizeArgumentBuilder().build())
                        .argument(booleanQueryArguments)
                        .dataFetcher(new IndexDataFetcherBuilder(normalizedIndexName).build())
                        .build(),
                new GraphQLFieldDefinition.Builder()
                        .name(String.format("%s_by_id", normalizedIndexName))
                        .type(documentType)
                        .argument(new IdArgumentBuilder().build())
                        .dataFetcher(new IndexByIdDataFetcherBuilder(normalizedIndexName).build())
                        .build(),
                new GraphQLFieldDefinition.Builder()
                        .name(String.format("%s_aggregations", normalizedIndexName))
                        .type(aggregateDocumentType)
                        .dataFetcher(environment -> environment.getSelectionSet().getFields())
                        .build()
                );
    }
}
