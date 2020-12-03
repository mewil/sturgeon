package io.mewil.sturgeon.schema.types;

public enum BooleanQueryType {
  MUST,
  MUST_NOT,
  FILTER,
  SHOULD;

  public String getName() {
    return name().toLowerCase();
  }
}
