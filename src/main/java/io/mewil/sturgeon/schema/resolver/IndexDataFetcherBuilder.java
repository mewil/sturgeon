package io.mewil.sturgeon.schema.resolver;

import graphql.schema.DataFetcher;
import io.mewil.sturgeon.elasticsearch.ElasticsearchClient;
import io.mewil.sturgeon.schema.SchemaConstants;
import io.mewil.sturgeon.schema.util.QueryFieldSelector;
import io.mewil.sturgeon.schema.util.QueryFieldSelectorResult;
import org.elasticsearch.search.SearchHits;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.mewil.sturgeon.elasticsearch.QueryAdapter.buildQueryFromArguments;
import static io.mewil.sturgeon.elasticsearch.ElasticsearchDecoder.decodeElasticsearchDoc;

public class IndexDataFetcherBuilder extends DataFetcherBuilder {
  public IndexDataFetcherBuilder(String index) {
    this.index = index;
  }

  private final String index;

  @Override
  public DataFetcher<List<Map<String, Object>>> build() {
    return dataFetchingEnvironment -> {
      final QueryFieldSelectorResult selectorResult =
          QueryFieldSelector.getSelectedFieldsFromQuery(dataFetchingEnvironment);
      final int size = dataFetchingEnvironment.getArgument(SchemaConstants.SIZE);
      final SearchHits hits =
          ElasticsearchClient.getInstance()
              .query(
                  index,
                  size,
                  selectorResult.getFields(),
                  buildQueryFromArguments(dataFetchingEnvironment.getArguments()))
              .getHits();

      return Arrays.stream(hits.getHits())
          .map(
              hit ->
                  decodeElasticsearchDoc(
                      hit.getSourceAsMap(), selectorResult.getIncludeId(), hit.getId()))
          .collect(Collectors.toList());
    };
  }
}
