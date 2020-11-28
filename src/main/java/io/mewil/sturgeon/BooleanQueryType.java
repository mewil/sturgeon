package io.mewil.sturgeon;

public enum BooleanQueryType {
    MUST,
    MUST_NOT,
    FILTER,
    SHOULD;

    public String getName() {
        return name().toLowerCase();
    }
}

