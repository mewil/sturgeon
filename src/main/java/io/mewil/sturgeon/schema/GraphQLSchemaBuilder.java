package io.mewil.sturgeon.schema;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import io.mewil.sturgeon.Configuration;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class GraphQLSchemaBuilder {

  private GraphQLSchemaBuilder() {}

  public static GraphQLSchema getSchema() throws IOException {
    return buildSchemaFromIndexMappings(ElasticsearchClient.getInstance().getMappings());
  }

  private static GraphQLSchema buildSchemaFromIndexMappings(final Map<String, Map<String, Object>> mappings) {
    return new GraphQLSchema.Builder().query(new GraphQLObjectType.Builder()
            .name("Query")
            .fields(buildSchemasFromIndexMappings(mappings))
            .build()).build();
  }

  private static List<GraphQLFieldDefinition> buildSchemasFromIndexMappings(
      final Map<String, Map<String, Object>> mappings) {
    return mappings.entrySet().stream()
        .flatMap(GraphQLSchemaBuilder::buildSchemasFromIndexMapping)
        // .sorted() TODO: implement comparator
        .collect(Collectors.toList());
  }

  private static Stream<GraphQLFieldDefinition> buildSchemasFromIndexMapping(
      final Map.Entry<String, Map<String, Object>> indexMapping) {
    NameNormalizer.getInstance().addName((indexMapping.getKey()));
    final String normalizedIndexName =
        NameNormalizer.getInstance().getGraphQLName(indexMapping.getKey());
    final GraphQLObjectType documentType = new DocumentTypeBuilder(indexMapping).build();
    final GraphQLArgument booleanQueryArguments = new BooleanQueryArgumentBuilder(indexMapping).build();

    final Stream<GraphQLFieldDefinition> schemas =
        Stream.of(
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
                .build());

    return Configuration.getInstance().getEnableAggregations()
        ? Stream.concat(
            schemas,
            Stream.of(
                new GraphQLFieldDefinition.Builder()
                    .name(String.format("%s_aggregations", normalizedIndexName))
                    .type(new DocumentAggregationTypeBuilder(indexMapping).build())
                    .dataFetcher(environment -> environment.getSelectionSet().getFields())
                    .build()))
        : schemas;
  }
}
