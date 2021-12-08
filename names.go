package main

import (
	"strings"
	"sync"
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

type nameNormalization struct {
	old        []string
	new        string
	prefixOnly bool
}

var nameNormalizations = []nameNormalization{
	{
		old: []string{"[", "(", ")", "?", "@", "#", "]"},
		new: "",
	},
	{
		old:        []string{"+"},
		new:        "plus_",
		prefixOnly: true,
	},
	{
		old:        []string{"-"},
		new:        "minus_",
		prefixOnly: true,
	},
	{
		old: []string{" ", "/", "+", "-"},
		new: "_",
	},
}

func normalizeName(name string) string {
	for _, normalization := range nameNormalizations {
		for _, s := range normalization.old {
			if normalization.prefixOnly {
				if strings.HasPrefix(name, s) {
					name = strings.Replace(name, s, normalization.new, 1)
				}
			} else {
				name = strings.ReplaceAll(name, s, normalization.new)
			}
		}
	}
	if len(name) > 0 && name[0] >= '0' && name[0] <= '9' {
		name = "_" + name
	}
	return name
}
