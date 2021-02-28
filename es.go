package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

func NewElasticsearchClient() (*elasticsearch.Client, error) {
	cfg := elasticsearch.Config{
		Addresses: Config.ElasticsearchHosts,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
		},
	}
	return elasticsearch.NewClient(cfg)
}

func getMappings(es *elasticsearch.Client) (map[string]interface{}, error) {
	ctx := context.Background()
	res, err := es.Indices.GetMapping(
		es.Indices.GetMapping.WithIndex(Config.ElasticsearchIndexIncludePattern),
		es.Indices.GetMapping.WithContext(ctx),
	)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, fmt.Errorf(res.String())
	}
	data := map[string]interface{}{}
	err = json.NewDecoder(res.Body).Decode(&data)
	return data, err
}

func buildQueryFromArgs(args map[string]interface{}) map[string]interface{} {
	q := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{},
		},
	}
	// TODO: refactor boolean query parsing and add validation
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
	return q
}

func query(es *elasticsearch.Client, ctx context.Context, index string, size int, selectedFields []string, args map[string]interface{}) ([]interface{}, error) {
	q := buildQueryFromArgs(args)
	buf := bytes.Buffer{}
	if err := json.NewEncoder(&buf).Encode(q); err != nil {
		return nil, err
	}
	if Config.EnableQueryLogging {
		log.Print(buf.String())
	}
	// TODO: consolidate common logic between queries
	res, err := es.Search(
		es.Search.WithSource(selectedFields...),
		es.Search.WithContext(ctx),
		es.Search.WithIndex(index),
		es.Search.WithSize(size),
		es.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, err
	}
	data, err := parseQueryResponse(res)
	if err != nil {
		return nil, err
	}
	return data["hits"].(map[string]interface{})["hits"].([]interface{}), nil
}

func queryById(es *elasticsearch.Client, ctx context.Context, index, id string, selectedFields []string) (map[string]interface{}, error) {
	res, err := es.Get(index, id,
		es.Get.WithSource(selectedFields...),
		es.Get.WithContext(ctx),
	)
	if err != nil {
		return nil, err
	}
	return parseQueryResponse(res)
}

func queryAggregation(es *elasticsearch.Client, ctx context.Context, index string, selectedFields []string, args map[string]interface{}, aggregationType AggregationType) (map[string]interface{}, error) {
	// TODO: refactor aggregation query parsing and add validation
	q := buildQueryFromArgs(args)
	q["aggs"] = map[string]interface{}{}
	for _, field := range selectedFields {
		sq := map[string]map[string]interface{}{
			string(aggregationType): {
				"field": field,
			},
		}

		if aggregationType == AggregationTypePercentiles {
			sq[string(aggregationType)]["keyed"] = false
		}
		q["aggs"].(map[string]interface{})[fmt.Sprintf("%v_%s", aggregationType, field)] = sq
	}
	buf := bytes.Buffer{}
	if err := json.NewEncoder(&buf).Encode(q); err != nil {
		return nil, err
	}
	if Config.EnableQueryLogging {
		log.Print(buf.String())
	}
	res, err := es.Search(
		es.Search.WithSource(selectedFields...),
		es.Search.WithContext(ctx),
		es.Search.WithIndex(index),
		es.Search.WithSize(0),
		es.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, err
	}
	data, err := parseQueryResponse(res)
	if err != nil {
		return nil, err
	}
	// TODO: refactor aggregation parsing and add error handling
	results := make(map[string]interface{})
	for _, field := range selectedFields {
		v := data["aggregations"].(map[string]interface{})[fmt.Sprintf("%v_%s", aggregationType, field)].(map[string]interface{})
		if value, ok := v["value"]; ok {
			results[field] = value
			continue
		}
		if values, ok := v["values"]; ok {
			results[field] = values
			continue
		}
	}
	return results, nil
}

func parseQueryResponse(res *esapi.Response) (map[string]interface{}, error) {
	defer res.Body.Close()
	if res.IsError() {
		return nil, parseQueryError(res)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if Config.EnableQueryResultLogging {
		log.Print(string(body))
	}
	data := map[string]interface{}{}
	if err = json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func parseQueryError(res *esapi.Response) error {
	data := map[string]interface{}{}
	if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
		return fmt.Errorf("failed to parse response body: %v", err)
	} else {
		errData := data["error"].(map[string]interface{})
		return fmt.Errorf("query failed: %s %s: %s", res.Status(), errData["type"], errData["reason"])
	}
}
