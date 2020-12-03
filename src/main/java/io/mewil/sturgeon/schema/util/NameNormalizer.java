package io.mewil.sturgeon.schema.util;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public final class NameNormalizer {

  private static NameNormalizer INSTANCE;

  private final BiMap<String, String> normalizedNameLookupTable = HashBiMap.create();

  private NameNormalizer() {}

  public static NameNormalizer getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new NameNormalizer();
    }
    return INSTANCE;
  }

  public void addName(final String originalName) {
    normalizedNameLookupTable.put(originalName, normalizeName(originalName));
  }

  public String getGraphQLName(final String originalName) {
    return normalizedNameLookupTable.get(originalName);
  }

  public String getOriginalName(final String graphQLName) {
    return normalizedNameLookupTable.inverse().get(graphQLName);
  }

  private static String normalizeName(final String name) {
    return name.replaceAll("(^[0-9])", "_$1")
        .replace(" ", "_")
        .replace("+", "plus_")
        .replace("-", "minus_")
        .replace("@", "")
        .replace("#", "");
  }
}
