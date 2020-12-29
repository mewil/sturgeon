package io.mewil.sturgeon.schema.resolver;

import io.mewil.sturgeon.schema.SchemaConstants;
import io.mewil.sturgeon.schema.types.BooleanQueryType;
import io.mewil.sturgeon.schema.util.NameNormalizer;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class QueryAdapter {

    public static List<QueryBuilder> buildQueryFromArguments(final Map<String, Object> arguments) {
        final List<QueryBuilder> queryBuilders = new ArrayList<>();
        arguments.forEach((key, value) -> {
            switch (key) {
                case SchemaConstants.BOOLEAN_QUERY:
                    // TODO: Refactor unchecked cast
                    queryBuilders.add(buildBooleanQuery((Map<String, Object>) value));
                    break;
            }
        });
        return queryBuilders;
    }

    private  static BoolQueryBuilder buildBooleanQuery(final Map<String, Object> booleanOccurrenceTypes) {
        final BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        booleanOccurrenceTypes.forEach((key, value) -> {
            // TODO: Refactor unchecked cast
            Set<Map.Entry<String, Object>> entrySet = ((Map<String, Object>) value).entrySet();
            switch (BooleanQueryType.valueOf(key.toUpperCase())) {
                case FILTER:
                    entrySet.forEach(e -> buildTermLevelQueries(e).forEach(boolQueryBuilder::filter));
                    break;
                case SHOULD:
                    entrySet.forEach(e -> buildTermLevelQueries(e).forEach(boolQueryBuilder::should));
                    break;
                case MUST:
                    entrySet.forEach(e -> buildTermLevelQueries(e).forEach(boolQueryBuilder::must));
                    break;
                case MUST_NOT:
                    entrySet.forEach(e -> buildTermLevelQueries(e).forEach(boolQueryBuilder::mustNot));
                    break;
            }
        });
        return boolQueryBuilder;
    }

    private static List<QueryBuilder> buildTermLevelQueries(final Map.Entry<String, Object> field) {
        final String fieldName = field.getKey();
        final Set<Map.Entry<String, Object>> termLevelQueries = ((Map<String, Object>) field.getValue()).entrySet();
        final List<QueryBuilder> queryBuilders =  new ArrayList<>();

        termLevelQueries.forEach(entry -> {
            switch (entry.getKey()) {
                case "range" -> queryBuilders.add(buildRangeQuery(fieldName, entry));
            }
        });

        return queryBuilders;
    }

    private static RangeQueryBuilder buildRangeQuery(final String fieldName, final Map.Entry<String, Object> field) {
        final RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(NameNormalizer.getInstance().getOriginalName(fieldName));
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
