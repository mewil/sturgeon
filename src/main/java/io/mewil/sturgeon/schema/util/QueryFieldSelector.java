package io.mewil.sturgeon.schema.util;

import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import io.mewil.sturgeon.schema.SchemaConstants;

import java.util.List;
import java.util.stream.Collectors;

public class QueryFieldSelector {

  public static QueryFieldSelectorResult getSelectedFieldsFromQuery(
      final DataFetchingEnvironment dataFetchingEnvironment) {
    final List<String> selectedGraphQLFields =
        dataFetchingEnvironment.getSelectionSet().getFields().stream()
            .filter(f -> List.of("/key", "/value").stream().noneMatch(s -> f.getQualifiedName().endsWith(s)))
            .map(SelectedField::getName)
            .collect(Collectors.toList());
    final boolean includeId = selectedGraphQLFields.remove(SchemaConstants.ID);
    final List<String> selectedIndexFields =
        selectedGraphQLFields.stream()
            .map(f -> NameNormalizer.getInstance().getOriginalName(f))
            .collect(Collectors.toList());
    return QueryFieldSelectorResult.builder()
        .fields(selectedIndexFields)
        .includeId(includeId)
        .build();
  }
}
