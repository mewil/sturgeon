package io.mewil.sturgeon.schema.types;

import com.google.common.collect.ImmutableSet;
import graphql.Scalars;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import io.mewil.sturgeon.elasticsearch.ElasticsearchDecoder;
import io.mewil.sturgeon.schema.SchemaConstants;
import io.mewil.sturgeon.schema.util.NameNormalizer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class FieldDefinitionBuilder {

  // TODO: Add support for nested types. Type will be null if we have a nested type, ignore for now.
  private static Optional<GraphQLScalarType> getScalarTypeFromField(
      final Map.Entry<String, Object> field) {
    // TODO: Refactor unchecked cast
    final Object type = ((HashMap<String, Object>) field.getValue()).get(SchemaConstants.TYPE);
    if (type == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(ElasticsearchDecoder.mapToGraphQLScalarType(type.toString()));
  }

  public static Optional<GraphQLFieldDefinition> fieldDefinitionForDocumentField(
      final Map.Entry<String, Object> field) {
    final Optional<GraphQLScalarType> scalarType = getScalarTypeFromField(field);
    if (scalarType.isEmpty()) {
      return Optional.empty();
    }
    NameNormalizer.getInstance().addName(field.getKey());
    return Optional.of(
        GraphQLFieldDefinition.newFieldDefinition()
            .name(NameNormalizer.getInstance().getGraphQLName(field.getKey()))
            .type(scalarType.get())
            .build());
  }

  private static final ImmutableSet<GraphQLScalarType> SUPPORTED_AGGREGATION_SCALARS =
      ImmutableSet.of(
          Scalars.GraphQLFloat, Scalars.GraphQLShort, Scalars.GraphQLInt, Scalars.GraphQLLong);

  public static Optional<GraphQLFieldDefinition> aggregateFieldDefinitionForDocumentField(
      final Map.Entry<String, Object> field, final AggregationType aggregationType) {

    final Optional<GraphQLScalarType> scalarType = getScalarTypeFromField(field);
    if (scalarType.isEmpty() || !SUPPORTED_AGGREGATION_SCALARS.contains(scalarType.get())) {
      return Optional.empty();
    }

    final GraphQLFieldDefinition.Builder builder = GraphQLFieldDefinition.newFieldDefinition()
            .name(NameNormalizer.getInstance().getGraphQLName(field.getKey()));

    switch (aggregationType) {
      case PERCENTILES:
        return Optional.of(builder
                .type(GraphQLList.list(getKeyedResponseType(scalarType.get())))
                .build());
      default:
        return Optional.of(builder
                .type(scalarType.get())
                .build());
    }
  }

  private static GraphQLObjectType getKeyedResponseType(final GraphQLScalarType scalarType) {
    return keyedResponseTypes.computeIfAbsent(scalarType, type -> GraphQLObjectType.newObject()
          .name(String.format("keyed_%s", scalarType.getName().toLowerCase()))
          .field(GraphQLFieldDefinition.newFieldDefinition()
                  .name("key")
                  .type(Scalars.GraphQLString)
                  .build())
          .field(GraphQLFieldDefinition.newFieldDefinition()
                  .name("value")
                  .type(type)
                  .build())
          .build());
  }

  private static final Map<GraphQLScalarType, GraphQLObjectType> keyedResponseTypes = new HashMap<>();

}
