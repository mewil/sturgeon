package io.mewil.sturgeon;

import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Getter
public class Configuration {

  private static class LazyHolder {
    private static final Configuration INSTANCE = new Configuration();
  }

  public static Configuration getInstance() {
    return LazyHolder.INSTANCE;
  }

  private static String getEnv(final String name, final String defaultValue) {
    return System.getenv().getOrDefault(name, defaultValue);
  }

  private static String getEnv(final String name) {
    return getEnv(name, "");
  }

  private final List<String> elasticsearchHosts =
      Arrays.stream(getEnv("ELASTICSEARCH_HOSTS").split(",")).collect(Collectors.toList());

  private final Pattern elasticsearchIndexIncludePattern =
      Pattern.compile(getEnv("ELASTICSEARCH_INDEX_INCLUDE_PATTERN", ".*"));

  private final Boolean enableAggregations = Boolean.valueOf(getEnv("ENABLE_AGGREGATIONS", "true"));
}
