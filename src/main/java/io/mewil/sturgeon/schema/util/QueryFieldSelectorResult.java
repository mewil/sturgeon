package io.mewil.sturgeon.schema.util;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class QueryFieldSelectorResult {
    List<String> fields;
    Boolean includeId;
}
