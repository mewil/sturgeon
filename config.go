package main

import (
	"fmt"
	"github.com/kelseyhightower/envconfig"
	"log"
)

type appConfig struct {
	ElasticsearchHosts               []string `split_words:"true"`
	ElasticsearchIndexIncludePattern string   `split_words:"true" default:"_all"`
	FieldIgnorePrefix                string   `split_words:"true"`
	EnableAggregations               bool     `split_words:"true" default:"true"`
	EnableGraphiql                   bool     `split_words:"true"`
	EnableQueryLogging               bool     `split_words:"true" default:"true"`
	EnableQueryResultLogging         bool     `split_words:"true" default:"true"`
}

var Config appConfig

func init() {
	if err := envconfig.Process("sturgeon", &Config); err != nil {
		log.Fatal(err)
	}
	fmt.Println(Config)
}
