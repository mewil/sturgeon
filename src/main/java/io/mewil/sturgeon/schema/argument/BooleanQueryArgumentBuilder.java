package io.mewil.sturgeon.schema.argument;

import graphql.Scalars;
import graphql.scalars.ExtendedScalars;
import graphql.schema.*;
import io.mewil.sturgeon.schema.types.BooleanQueryType;
import io.mewil.sturgeon.schema.SchemaConstants;
import io.mewil.sturgeon.schema.util.ElasticsearchDecoder;
import io.mewil.sturgeon.schema.util.NameNormalizer;

import java.util.*;
import java.util.stream.Collectors;

public class BooleanQueryArgumentBuilder extends ArgumentBuilder {

    private final String index;
    private final Map<String, Object> mapping;

    public BooleanQueryArgumentBuilder(final Map.Entry<String, Map<String, Object>> indexMapping) {
        this.index = indexMapping.getKey();
        this.mapping = indexMapping.getValue();
    }

    @Override
    public GraphQLArgument build() {
        return GraphQLArgument.newArgument()
                .name(SchemaConstants.BOOLEAN_QUERY)
                .type(buildBooleanQueryArguments())
                .build();
    }

    private GraphQLInputType buildBooleanQueryArguments() {
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
                                                .map(e -> buildBooleanQueryArgument(e, booleanQueryType))
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

    private Optional<GraphQLInputObjectField> buildBooleanQueryArgument(final Map.Entry<String, Object> field,
                                                                        final BooleanQueryType booleanQueryType) {
        // TODO: Refactor unchecked cast
        // TODO: Add support for nested types. Type will be null if we have a nested type, ignore for now.
        final Object type = ((HashMap<String, Object>) field.getValue()).get(SchemaConstants.TYPE);
        if (type == null) {
            return Optional.empty();
        }
        final GraphQLScalarType scalarType = ElasticsearchDecoder.mapToGraphQLScalarType(type.toString());
        if (Scalars.GraphQLFloat.equals(scalarType) || ExtendedScalars.DateTime.equals(scalarType)) {
            return Optional.of(GraphQLInputObjectField
                    .newInputObjectField()
                    .type(buildBooleanQueryArgumentType(field.getKey(), scalarType, booleanQueryType))
                    .name(NameNormalizer.getInstance().getGraphQLName(field.getKey()))
                    .build());
        }
        return Optional.empty();
    }

    private GraphQLInputObjectType buildBooleanQueryArgumentType(final String fieldName, final GraphQLScalarType type,
                                                                 final BooleanQueryType booleanQueryType) {
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
        final String typeName = String.format("%s_boolean_query_%s_%s", index,
                NameNormalizer.getInstance().getGraphQLName(fieldName), booleanQueryType.getName());
        return GraphQLInputObjectType.newInputObject()
                .fields(fields)
                .name(typeName)
                .build();
    }
}
