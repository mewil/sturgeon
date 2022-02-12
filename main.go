package main

import (
	"fmt"
	"github.com/graphql-go/handler"
	"log"
	"net/http"
	"time"
)

func main() {
	es, err := NewElasticsearchClient()
	if err != nil {
		log.Fatal(err)
	}
	mappings, err := getMappings(es)
	if err != nil {
		log.Fatal(err)
	}
	start := time.Now()
	schema, err := buildRootSchemaFromMappings(mappings, es)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("built schemas for ", len(mappings), " indices in ", time.Since(start))
	h := handler.New(&handler.Config{
		Schema:   &schema,
		Pretty:   true,
		GraphiQL: Config.EnableGraphiql,
	})
	http.Handle(Config.Path, h)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", Config.Port), nil))
}
