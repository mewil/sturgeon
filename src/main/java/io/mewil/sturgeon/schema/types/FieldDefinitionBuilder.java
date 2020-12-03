package io.mewil.sturgeon.schema.types;

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLScalarType;
import io.mewil.sturgeon.schema.SchemaConstants;
import io.mewil.sturgeon.schema.util.ElasticsearchDecoder;
import io.mewil.sturgeon.schema.util.NameNormalizer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class FieldDefinitionBuilder {

    public static Optional<GraphQLFieldDefinition> fromIndexMappingField(final GraphQLScalarType typeFilter,
                                                                         final Map.Entry<String, Object> field) {
        // TODO: Refactor unchecked cast
        // TODO: Add support for nested types. Type will be null if we have a nested type, ignore for now.
        final Object type = ((HashMap<String, Object>) field.getValue()).get(SchemaConstants.TYPE);
        if (type == null) {
            return Optional.empty();
        }
        GraphQLScalarType scalarType = ElasticsearchDecoder.mapToGraphQLScalarType(type.toString());
        if (typeFilter != null && scalarType != typeFilter)  {
            return Optional.empty();
        }
        NameNormalizer.getInstance().addName(field.getKey());
        final String normalizedName = NameNormalizer.getInstance().getGraphQLName(field.getKey());
        return Optional.of(GraphQLFieldDefinition.newFieldDefinition()
                .name(normalizedName)
                .type(scalarType)
                .build());
    }
}
