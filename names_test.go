package main

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNameConversions(t *testing.T) {
	pairs := []struct {
		originalName   string
		normalizedName string
	}{
		{"foo bar", "foo_bar"},
		{"@timestamp", "timestamp"},
		{"+foo", "plus_foo"},
		{"-foo", "minus_foo"},
		{"#count", "count"},
		{"1foo", "_1foo"},
	}
	for _, pair := range pairs {
		addName(pair.originalName)
		require.Equal(t, pair.normalizedName, getGraphQLName(pair.originalName))
		require.Equal(t, pair.originalName, getOriginalName(pair.normalizedName))
	}
}
