package io.mewil.sturgeon;

public enum AggregationType {
    AVG,
    MAX,
    MIN;

    public String getName() {
        return name().toLowerCase();
    }

}
