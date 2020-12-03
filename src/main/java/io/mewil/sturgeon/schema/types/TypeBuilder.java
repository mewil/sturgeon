package io.mewil.sturgeon.schema.types;

import graphql.schema.GraphQLObjectType;

public abstract class TypeBuilder {
    public abstract GraphQLObjectType build();
}
