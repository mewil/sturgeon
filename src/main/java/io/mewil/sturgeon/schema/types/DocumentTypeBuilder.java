package io.mewil.sturgeon.schema.types;

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import io.mewil.sturgeon.schema.SchemaConstants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DocumentTypeBuilder extends TypeBuilder {

    private final Map.Entry<String, Map<String, Object>> indexMapping;

    public DocumentTypeBuilder(Map.Entry<String, Map<String, Object>> indexMapping) {
        this.indexMapping = indexMapping;
    }

    @Override
    public GraphQLObjectType build() {
        return new GraphQLObjectType.Builder()
                .name(String.format("doc_%s", indexMapping.getKey()))
                .fields(buildFieldDefinitionsFromIndexMapping(indexMapping.getKey(), indexMapping.getValue()))
                .build();
    }

    private List<GraphQLFieldDefinition> buildFieldDefinitionsFromIndexMapping(String index, Map<String, Object> mapping) {
        final List<GraphQLFieldDefinition> fields = mapping.entrySet().stream()
                .flatMap(entry -> {
                    // TODO: Refactor unchecked cast
                    final Map<String, Object> properties = (HashMap<String, Object>) entry.getValue();
                    return properties.entrySet().stream();
                })
                .map(field -> FieldDefinitionBuilder.fromIndexMappingField(null, field))
                .filter(Optional::isPresent)
                .map(Optional::get)
                // .sorted() TODO: implement comparator
                .collect(Collectors.toList());
        fields.add(GraphQLFieldDefinition.newFieldDefinition()
                .name(SchemaConstants.ID)
                .type(SchemaConstants.UUID)
                .build());
        return fields;
    }

}
