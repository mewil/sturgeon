package main

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBuildSchemaFromMappings(t *testing.T) {
	mappings := map[string]interface{}{
		"es-index": map[string]interface{}{
			"mappings": map[string]interface{}{
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
			},
		},
	}

	schemas := buildSchemaFromMappings(mappings, nil)
	require.Len(t, schemas, 3)
	require.Contains(t, schemas, "es_index")
	require.Contains(t, schemas, "es_index_by_id")
	require.Contains(t, schemas, "es_index_aggregations")

	documentListSchema := schemas["es_index"]
	documentByIDSchema := schemas["es_index_by_id"]
	documentAggregation := schemas["es_index_aggregations"]

	require.Equal(t, "es_index", documentListSchema.Name)
	require.Equal(t, "[es_index_document]", documentListSchema.Type.String())

	require.Equal(t, "es_index_by_id", documentByIDSchema.Name)
	require.Equal(t, "es_index_document", documentByIDSchema.Type.String())

	require.Equal(t, "es_index_aggregations", documentAggregation.Name)
	require.Equal(t, "es_index_aggregation_document", documentAggregation.Type.String())
}

func TestBuildSchemaFromEmptyMappings(t *testing.T) {
	mappings := map[string]interface{}{
		"es-index": map[string]interface{}{
			"mappings": map[string]interface{}{},
		},
	}
	schemas := buildSchemaFromMappings(mappings, nil)
	require.Len(t, schemas, 0)
}
