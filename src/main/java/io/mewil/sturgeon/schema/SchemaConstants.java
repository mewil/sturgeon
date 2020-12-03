package io.mewil.sturgeon.schema;


import graphql.schema.GraphQLScalarType;
import io.mewil.sturgeon.schema.types.UUIDScalar;

public final class SchemaConstants {
    public static final String TYPE = "type";
    public static final String BOOLEAN_QUERY = "boolean_query";
    public static final String ID = "id";
    public static final String SIZE = "size";
    public static GraphQLScalarType UUID = new UUIDScalar();
}
