package io.mewil.sturgeon.schema.resolver;

import com.google.common.collect.Streams;
import graphql.schema.DataFetcher;
import io.mewil.sturgeon.elasticsearch.ElasticsearchClient;
import io.mewil.sturgeon.elasticsearch.QueryAdapter;
import io.mewil.sturgeon.schema.types.AggregationType;
import io.mewil.sturgeon.schema.types.KeyedResponse;
import io.mewil.sturgeon.schema.util.NameNormalizer;
import io.mewil.sturgeon.schema.util.QueryFieldSelector;
import io.mewil.sturgeon.schema.util.QueryFieldSelectorResult;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.ParsedPercentiles;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class DocumentAggregationDataFetcherBuilder extends DataFetcherBuilder {

    private final String index;
    private final AggregationType aggregationType;

    public DocumentAggregationDataFetcherBuilder(String index, AggregationType aggregationType) {
        this.index = index;
        this.aggregationType = aggregationType;
    }

    @Override
    public DataFetcher<Map<String, Object>> build() {
        return dataFetchingEnvironment -> {
            final QueryFieldSelectorResult selectorResult =
                    QueryFieldSelector.getSelectedFieldsFromQuery(dataFetchingEnvironment);
            final SearchResponse response = ElasticsearchClient.getInstance().queryWithAggregation(
                    index,
                    selectorResult.getFields(),
                            QueryAdapter.buildQueryFromArguments(dataFetchingEnvironment.getExecutionStepInfo().getParent().getArguments()),
                            aggregationType);
            return response.getAggregations().asMap().entrySet().stream()
                    .map(DocumentAggregationDataFetcherBuilder::normalizeAggregationTypes)
                    .map(DocumentAggregationDataFetcherBuilder::changeInfinityToNull)
                    // Fix to avoid null values here causing exceptions
                    // https://stackoverflow.com/questions/24630963/java-8-nullpointerexception-in-collectors-tomap
                    .collect(HashMap::new, (m, e) -> m.put(NameNormalizer.getInstance().getGraphQLName(e.getKey()), e.getValue()), HashMap::putAll);
        };
    }

    private static Map.Entry<String, Object> normalizeAggregationTypes(final Map.Entry<String, Aggregation> field) {
        return switch (AggregationType.fromAggregationType(field.getValue().getType())) {
            case AVG -> new HashMap.SimpleEntry<>(field.getKey(), ((ParsedAvg) field.getValue()).getValue());
            case MAX -> new HashMap.SimpleEntry<>(field.getKey(), ((ParsedMax) field.getValue()).getValue());
            case MIN -> new HashMap.SimpleEntry<>(field.getKey(), ((ParsedMin) field.getValue()).getValue());
            case CARDINALITY -> new HashMap.SimpleEntry<>(field.getKey(), ((ParsedCardinality) field.getValue()).getValue());
            case PERCENTILES -> new HashMap.SimpleEntry<>(field.getKey(), Streams.stream((ParsedPercentiles) field.getValue())
                    .map(KeyedResponse::new)
                    .collect(Collectors.toList()));
        };
    }

    // TODO: make this more elegant
    private static Map.Entry<String, Object> changeInfinityToNull(final Map.Entry<String, Object> entry) {
        if (entry.getValue() instanceof Double && ((Double) entry.getValue()).isInfinite()) {
            return new HashMap.SimpleEntry<>(entry.getKey(), null);
        }
        return entry;
    }
}
