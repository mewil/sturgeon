package io.mewil.elasticgraphql;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;

import java.io.IOException;
import java.util.Arrays;
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
}
