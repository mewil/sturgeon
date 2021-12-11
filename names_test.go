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
		{"@timestamp", "_timestamp"},
		{"+foo", "plus_foo"},
		{"-foo", "minus_foo"},
		{"test+foo", "test_foo"},
		{"test-foo", "test_foo"},
		{"#count", "_count"},
		{"1foo", "_1foo"},
		{"filebeat-2021.11.24.0001", "filebeat_2021_11_24_0001"},
	}
	for _, pair := range pairs {
		addName(pair.originalName)
		require.Equal(t, pair.normalizedName, getGraphQLName(pair.originalName))
		require.Equal(t, pair.originalName, getOriginalName(pair.normalizedName))
	}
}
