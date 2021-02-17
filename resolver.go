package main

func convertSourceDocumentsToQueryResult(documents []interface{}, includeIdField bool) []interface{} {
	results := make([]interface{}, len(documents))
	for i, document := range documents {
		doc := document.(map[string]interface{})
		source := doc["_source"].(map[string]interface{})
		fields := convertSourceToQueryResult(source)
		if includeIdField {
			fields["id"] = doc["_id"]
		}
		results[i] = fields
	}
	return results
}

func convertSourceToQueryResult(source map[string]interface{}) map[string]interface{} {
	fields := make(map[string]interface{}, len(source))
	for key, value := range source {
		fields[getGraphQLName(key)] = value
	}
	return fields
}
