package main

import (
	"strings"
)

var (
	graphQLNameToOriginal = make(map[string]string)
	originalNameToGraphQL = make(map[string]string)
)

func addName(originalName string) {
	graphQLName := normalizeName(originalName)
	graphQLNameToOriginal[graphQLName] = originalName
	originalNameToGraphQL[originalName] = graphQLName
}

func getGraphQLName(originalName string) string {
	return originalNameToGraphQL[originalName]
}

func getOriginalName(graphQLName string) string {
	return graphQLNameToOriginal[graphQLName]
}

func normalizeName(name string) string {
	for _, s := range []string{"[", "(", ")", "?", "@", "#", "]"} {
		name = strings.ReplaceAll(name, s, "")
	}
	for _, s := range []string{" "} {
		name = strings.ReplaceAll(name, s, "_")
	}
	for _, s := range []string{"+"} {
		name = strings.ReplaceAll(name, s, "plus_")
	}
	for _, s := range []string{"-"} {
		name = strings.ReplaceAll(name, s, "minus_")
	}
	if len(name) > 0 && name[0] >= '0' && name[0] <= '9' {
		name = "_" + name
	}
	return name
}
