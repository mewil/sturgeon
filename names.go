package main

import (
	"strings"
	"sync"
	"unicode"
)

var (
	nameMux               = sync.RWMutex{}
	graphQLNameToOriginal = make(map[string]string)
	originalNameToGraphQL = make(map[string]string)
)

func addName(originalName string) {
	graphQLName := normalizeName(originalName)
	nameMux.Lock()
	defer nameMux.Unlock()
	graphQLNameToOriginal[graphQLName] = originalName
	originalNameToGraphQL[originalName] = graphQLName
}

func getGraphQLName(originalName string) string {
	nameMux.RLock()
	defer nameMux.RUnlock()
	return originalNameToGraphQL[originalName]
}

func getOriginalName(graphQLName string) string {
	nameMux.RLock()
	defer nameMux.RUnlock()
	return graphQLNameToOriginal[graphQLName]
}

func replaceAtIndex(s, r string, i int) string {
	return s[:i] + r + s[i+1:]
}

func normalizeName(name string) string {
	if strings.HasPrefix(name, "+") {
		name = replaceAtIndex(name, "plus_", 0)
	}
	if strings.HasPrefix(name, "-") {
		name = replaceAtIndex(name, "minus_", 0)
	}
	for i := range name {
		if !(unicode.IsLetter(rune(name[i])) || unicode.IsNumber(rune(name[i]))) {
			name = replaceAtIndex(name, "_", i)
		}
	}
	if len(name) > 0 && name[0] >= '0' && name[0] <= '9' {
		name = "_" + name
	}
	return name
}
