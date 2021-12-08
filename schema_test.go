package main

import (
	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestBuildSchemaFromMappings(t *testing.T) {
	inputMapping := map[string]interface{}{
		"properties": map[string]interface{}{
			"+X Temperature": map[string]interface{}{
				"type": "float",
			},
			"-X Temperature": map[string]interface{}{
				"type": "float",
			},
			"3": map[string]interface{}{
				"properties": map[string]interface{}{
					"3V Bus Current": map[string]interface{}{
						"type": "float",
					},
					"3V Bus Voltage": map[string]interface{}{
						"type": "float",
					},
					"3V Input Current": map[string]interface{}{
						"type": "float",
					},
				},
			},
			"5V Current": map[string]interface{}{
				"type": "float",
			},
			"@timestamp": map[string]interface{}{
				"type": "date",
			},
			"Battery Temperature": map[string]interface{}{
				"type": "float",
			},
			"Unix Time": map[string]interface{}{
				"type": "long",
			},
			"Archive": map[string]interface{}{
				"type": "boolean",
			},
		},
	}

	wg := sync.WaitGroup{}
	schemas := make(chan *graphql.Field, 3)
	wg.Add(1)
	buildSchemasFromMapping("es-index", inputMapping, nil, schemas, &wg)
	documentListSchema := <-schemas
	documentByIDSchema := <-schemas
	documentAggregation := <-schemas

	require.Equal(t, "es_index", documentListSchema.Name)
	require.Equal(t, "[es_index_document]", documentListSchema.Type.String())

	require.Equal(t, "es_index_by_id", documentByIDSchema.Name)
	require.Equal(t, "es_index_document", documentByIDSchema.Type.String())

	require.Equal(t, "es_index_aggregations", documentAggregation.Name)
	require.Equal(t, "es_index_aggregation_document", documentAggregation.Type.String())
}
