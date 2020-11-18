package io.mewil.sturgeon;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElasticsearchClient {
    private final RestHighLevelClient client;

    public ElasticsearchClient() {
        HttpHost[] hosts = Arrays.stream(System.getenv("ELASTICSEARCH_HOSTS").split(","))
                .map(s -> {
                    String[] parts = s.split(":");
                    if (parts.length != 2) {
                        throw new IllegalArgumentException(String.format("invalid host %s", s));
                    }
                    return new HttpHost(parts[0], Integer.parseInt(parts[1]), "http");
                })
                .toArray(HttpHost[]::new);
        client = new RestHighLevelClient(RestClient.builder(hosts));
    }

    public Map<String, Map<String, Object>> getMappings() throws IOException {
        GetMappingsRequest request = new GetMappingsRequest();
        GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
        return response.mappings().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSourceAsMap()));
    }

    public GetResponse queryById(final String index, final String id, final List<String> selectedFields) throws IOException {
        GetRequest getRequest = new GetRequest(index, id);
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, selectedFields.toArray(String[]::new), Strings.EMPTY_ARRAY);
        getRequest.fetchSourceContext(fetchSourceContext);
        return client.get(getRequest, RequestOptions.DEFAULT);
    }

    private SearchResponse doQueryFromSearchSourceBuilder(final String index, final SearchSourceBuilder sourceBuilder) throws IOException {
        final SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(sourceBuilder);
        return client.search(searchRequest, RequestOptions.DEFAULT);
    }

    public SearchResponse query(final String index, final int size, final List<String> selectedFields) throws IOException {
        final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .fetchSource(selectedFields.toArray(String[]::new), Strings.EMPTY_ARRAY)
                .size(size);
        return doQueryFromSearchSourceBuilder(index, sourceBuilder);
    }

    public SearchResponse queryWithAggregation(final String index, final List<String> selectedFields,
                                               final AggregationType aggregationType) throws IOException {
        final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0);
        selectedFields.forEach(field -> {
            switch (aggregationType) {
                case AVG -> sourceBuilder.aggregation(AggregationBuilders.avg(field).field(field));
                case MAX -> sourceBuilder.aggregation(AggregationBuilders.max(field).field(field));
                case MIN -> sourceBuilder.aggregation(AggregationBuilders.min(field).field(field));
            }
        });
        return doQueryFromSearchSourceBuilder(index, sourceBuilder);
    }
}
