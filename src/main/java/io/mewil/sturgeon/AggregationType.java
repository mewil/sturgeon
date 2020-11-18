package io.mewil.sturgeon;

public enum AggregationType {
    AVG("avg"),
    MAX("max"),
    MIN("min");

    public String getName() {
        return name;
    }

    private final String name;

    private AggregationType(final String name) {
        this.name = name;
    }
}

