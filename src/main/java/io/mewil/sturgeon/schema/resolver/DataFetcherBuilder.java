package io.mewil.sturgeon.schema.resolver;

import graphql.schema.DataFetcher;

public abstract class DataFetcherBuilder {
    public abstract DataFetcher<?> build();
}
