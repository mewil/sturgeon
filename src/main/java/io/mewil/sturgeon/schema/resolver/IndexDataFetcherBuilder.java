package io.mewil.sturgeon.schema.resolver;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.mewil.sturgeon.ElasticsearchClient;
import io.mewil.sturgeon.schema.SchemaConstants;
import io.mewil.sturgeon.schema.types.BooleanQueryType;
import io.mewil.sturgeon.schema.util.ElasticsearchDecoder;
import io.mewil.sturgeon.schema.util.NameNormalizer;
import io.mewil.sturgeon.schema.util.QueryFieldSelector;
import io.mewil.sturgeon.schema.util.QueryFieldSelectorResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHits;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
      final SearchHits hits = ElasticsearchClient.getInstance().query(index, size, selectorResult.getFields(), buildQueryFromArguments(dataFetchingEnvironment)).getHits();

      return Arrays.stream(hits.getHits())
              .map(hit -> ElasticsearchDecoder.decodeElasticsearchDoc(hit.getSourceAsMap(), selectorResult.getIncludeId(), hit.getId()))
              .collect(Collectors.toList());
    };
  }

  public List<QueryBuilder> buildQueryFromArguments(final DataFetchingEnvironment dataFetchingEnvironment) {
    final List<QueryBuilder> queryBuilders = new ArrayList<>();
    dataFetchingEnvironment.getArguments().forEach((key, value) -> {
      switch (key) {
        case SchemaConstants.BOOLEAN_QUERY:
          // TODO: Refactor unchecked cast
          queryBuilders.add(buildBooleanQuery((Map<String, Object>) value));
          break;
      }
    });
    return queryBuilders;
  }

  private BoolQueryBuilder buildBooleanQuery(final Map<String, Object> booleanOccurrenceTypes) {
    final BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
    booleanOccurrenceTypes.forEach((key, value) -> {
      // TODO: Refactor unchecked cast
      Map<String, Object> map = ((Map<String, Object>) value);
      switch (BooleanQueryType.valueOf(key.toUpperCase())) {
        case FILTER:
          map.entrySet().forEach(e -> boolQueryBuilder.filter(buildRangeQuery(e)));
      }
    });
    return boolQueryBuilder;
  }

  private RangeQueryBuilder buildRangeQuery(final Map.Entry<String, Object> field) {
    final RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(NameNormalizer.getInstance().getOriginalName(field.getKey()));
    ((Map<String, Object>) field.getValue()).forEach((key, value) -> {
      switch (key) {
        case "gt"  -> rangeQueryBuilder.gt(value);
        case "gte" -> rangeQueryBuilder.gte(value);
        case "lt" -> rangeQueryBuilder.lt(value);
        case "lte" -> rangeQueryBuilder.lte(value);
      }
    });
    return rangeQueryBuilder;
  }
}
