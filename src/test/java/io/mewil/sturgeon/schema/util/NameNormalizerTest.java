package io.mewil.sturgeon.schema.util;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NameNormalizerTest {
    private final Map<String, String> cases = new ImmutableMap.Builder<String, String>()
            .put("foo bar", "foo_bar")
            .put("@timestamp", "timestamp")
            .put("+foo", "plus_foo")
            .put("-foo", "minus_foo")
            .put("#count", "count")
            .put("1foo", "_1foo")
            .build();

    @Test
    void test() {
        final NameNormalizer normalizer = NameNormalizer.getInstance();
        cases.forEach((key, value) -> normalizer.addName(key));
        cases.forEach((key, value) -> assertEquals(value, normalizer.getGraphQLName(key)));
        cases.forEach((key, value) -> assertEquals(key, normalizer.getOriginalName(value)));
    }
}