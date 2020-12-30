package io.mewil.sturgeon.schema.types;

public enum AggregationType {
  AVG,
  MAX,
  MIN,
  CARDINALITY,
  PERCENTILES;

  private static final String PERCENTILE_TYPE = "tdigest_percentiles";

  public String getName() {
    return name().toLowerCase();
  }

  public static AggregationType fromAggregationType(final String name) {
    if (PERCENTILE_TYPE.equals(name)) {
      return PERCENTILES;
    }
    return AggregationType.valueOf(name.toUpperCase());
  }
}
