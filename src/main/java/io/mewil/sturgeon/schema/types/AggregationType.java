package io.mewil.sturgeon.schema.types;

public enum AggregationType {
  AVG,
  MAX,
  MIN;

  public String getName() {
    return name().toLowerCase();
  }
}
