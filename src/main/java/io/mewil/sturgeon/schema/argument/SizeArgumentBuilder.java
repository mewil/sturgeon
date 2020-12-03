package io.mewil.sturgeon.schema.argument;

import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLNonNull;
import io.mewil.sturgeon.schema.SchemaConstants;

public class SizeArgumentBuilder extends ArgumentBuilder {

  @Override
  public GraphQLArgument build() {
    return GraphQLArgument.newArgument()
        .name(SchemaConstants.SIZE)
        .type(GraphQLNonNull.nonNull(Scalars.GraphQLInt))
        .build();
  }
}
