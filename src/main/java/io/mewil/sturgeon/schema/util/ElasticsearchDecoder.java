package io.mewil.sturgeon.schema.util;

import graphql.Scalars;
import graphql.scalars.ExtendedScalars;
import graphql.schema.GraphQLScalarType;
import io.mewil.sturgeon.schema.SchemaConstants;

import java.util.Map;
import java.util.stream.Collectors;

public final class ElasticsearchDecoder {

    public static Map<String, Object> decodeElasticsearchDoc(Map<String, Object> sourceAsMap, boolean includeId, String id) {
        final Map<String, Object> result = sourceAsMap.entrySet().stream()
                .collect(Collectors.toMap(entry -> NameNormalizer.getInstance().getGraphQLName(entry.getKey()), Map.Entry::getValue));
        if (includeId) {
            result.put(SchemaConstants.ID, id);
        }
        return result;
    }

    public static GraphQLScalarType mapToGraphQLScalarType(final String type) {
        return switch (type) {
            case "float" -> Scalars.GraphQLFloat;
            case "long" -> Scalars.GraphQLLong;
            case "string" -> Scalars.GraphQLString;
            case "date" -> ExtendedScalars.DateTime;
            case "boolean" -> Scalars.GraphQLBoolean;
            default -> null;
        };
    }
}
