package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"log"
)

func query(es *elasticsearch.Client, ctx context.Context, index string, size int, selectedFields []string, args map[string]interface{}) []interface{} {
	//Build the request body.
	q := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{},
		},
	}

	if boolQuery, exists := args["boolean_query"]; exists {
		for clause := range boolQuery.(map[string]interface{}) {
			for field := range boolQuery.(map[string]interface{})[clause].(map[string]interface{}) {
				for termLevel := range boolQuery.(map[string]interface{})[clause].(map[string]interface{})[field].(map[string]interface{}) {
					sq := q["query"].(map[string]interface{})
					sq["bool"] = map[string]interface{}{
						clause: map[string]interface{}{
							termLevel: map[string]interface{}{
								getOriginalName(field): boolQuery.(map[string]interface{})[clause].(map[string]interface{})[field].(map[string]interface{})[termLevel],
							},
						},
					}
				}
			}
		}

	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(q); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}

	// Perform the search request.
	res, err := es.Search(
		es.Search.WithSource(selectedFields...),
		es.Search.WithContext(ctx),
		es.Search.WithIndex(index),
		es.Search.WithSize(size),
		es.Search.WithBody(&buf),
		//es.Search.WithTrackTotalHits(true),
		//es.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			log.Fatalf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}

	r := make(map[string]interface{})
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	log.Println(r)
	return r["hits"].(map[string]interface{})["hits"].([]interface{})
}

func queryById(es *elasticsearch.Client, ctx context.Context, index, id string, selectedFields []string) map[string]interface{} {
	// Build the request body.

	// Perform the search request.
	res, err := es.Get(index, id,
		es.Get.WithSource(selectedFields...),
		es.Get.WithContext(ctx),
		//es.Search.WithBody(&buf),
		//es.Search.WithTrackTotalHits(true),
		//es.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			log.Fatalf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}

	r := make(map[string]interface{})
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	log.Println(r)
	return r
}

func queryAggregation(es *elasticsearch.Client, ctx context.Context, index string, size int, selectedFields []string, aggregationType AggregationType) map[string]interface{} {
	// Build the request body.
	var buf bytes.Buffer
	q := map[string]map[string]interface{}{
		"aggs": make(map[string]interface{}),
	}
	//"aggs": {
	//	"max_price": { "max": { "field": "price" } }
	//}
	for _, field := range selectedFields {
		q["aggs"][fmt.Sprintf("%v_%s", aggregationType, field)] = map[string]map[string]interface{}{
			string(aggregationType): {
				"field": field,
			},
		}
	}

	if err := json.NewEncoder(&buf).Encode(q); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}

	// Perform the search request.
	res, err := es.Search(
		es.Search.WithSource(selectedFields...),
		es.Search.WithContext(ctx),
		es.Search.WithIndex(index),
		es.Search.WithSize(0),
		es.Search.WithBody(&buf),
		//es.Search.WithTrackTotalHits(true),
		//es.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			log.Fatalf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}

	r := make(map[string]interface{})
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	log.Println(r)

	results := make(map[string]interface{})
	for _, field := range selectedFields {
		results[field] = r["aggregations"].(map[string]interface{})[fmt.Sprintf("%v_%s", aggregationType, field)].(map[string]interface{})["value"]
	}
	return results
}
