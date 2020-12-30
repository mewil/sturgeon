package io.mewil.sturgeon.schema.types;

import lombok.Data;
import org.elasticsearch.search.aggregations.metrics.Percentile;

@Data
public class KeyedResponse {
    private final String key;
    private final Double value;

    public KeyedResponse(final Percentile percentile) {
        key = String.valueOf(percentile.getPercent());
        value = percentile.getValue();
    }
}
