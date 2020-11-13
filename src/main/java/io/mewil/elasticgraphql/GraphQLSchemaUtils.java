package io.mewil.elasticgraphql;

import graphql.Scalars;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class GraphQLSchemaUtils {
    public static GraphQLSchema buildSchemaFromIndexMappings(Map<String, Map<String, Object>> mappings) {
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
                .map(GraphQLSchemaUtils::buildSchemaFromIndexMapping)
                // .sorted() TODO: implement comparator
                .collect(Collectors.toList());
    }

    private static GraphQLFieldDefinition buildSchemaFromIndexMapping(Map.Entry<String, Map<String, Object>> mapping) {
        GraphQLObjectType doc = new GraphQLObjectType.Builder()
                .name(String.format("doc_%s", mapping.getKey()))
                .fields(buildFieldDefinitionsFromIndexMapping(mapping.getValue()))
                .build();
        return new GraphQLFieldDefinition.Builder()
                .name(normalizeGraphQLName(mapping.getKey()))
                .type(doc)
                .build();
    }

    private static final String TYPE = "type";

    private static List<GraphQLFieldDefinition> buildFieldDefinitionsFromIndexMapping(Map<String, Object> mapping) {
        return mapping.entrySet().stream()
                .flatMap(entry -> {
                    final Map<String, Object> properties = (HashMap<String, Object>) entry.getValue();
                    return properties.entrySet().stream();
                })
                .map(GraphQLSchemaUtils::buildFieldDefinitionFromIndexMappingField)
                .filter(Objects::nonNull)
                // .sorted() TODO: implement comparator
                .collect(Collectors.toList());
    }

    private static GraphQLFieldDefinition buildFieldDefinitionFromIndexMappingField(Map.Entry<String, Object> field) {
        final Object type = ((HashMap<String, Object>) field.getValue()).get(TYPE);
        // TODO: Add support for nested types. Type will be null if we have a nested type, ignore for now.
        if (type == null) {
            return null;
        }
        return GraphQLFieldDefinition.newFieldDefinition()
                .name(normalizeGraphQLName(field.getKey()))
                .type(getGraphQLType(type.toString()))
                .build();
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
