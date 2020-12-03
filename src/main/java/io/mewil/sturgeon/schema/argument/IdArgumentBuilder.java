package io.mewil.sturgeon.schema.argument;

import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLNonNull;
import io.mewil.sturgeon.schema.SchemaConstants;

import static io.mewil.sturgeon.schema.SchemaConstants.UUID;

public class IdArgumentBuilder extends ArgumentBuilder {

    @Override
    public GraphQLArgument build() {
        return GraphQLArgument.newArgument()
                .name(SchemaConstants.ID)
                .type(GraphQLNonNull.nonNull(UUID))
                .build();
    }
}
