package main

import (
	"github.com/kelseyhightower/envconfig"
	"log"
)

type appConfig struct {
	ElasticsearchHosts               []string `split_words:"true"`
	ElasticsearchIndexIncludePattern string   `split_words:"true" default:"_all"`
	FieldIgnorePrefix                string   `split_words:"true"`
	EnableAggregations               bool     `split_words:"true" default:"true"`
	EnableGraphiql                   bool     `split_words:"true" default:"true"`
	Port                             int      `split_words:"true" default:"8080"`
	Path                             string   `split_words:"true" default:"/"`
	EnableQueryLogging               bool     `split_words:"true" default:"true"`
	EnableQueryResultLogging         bool     `split_words:"true" default:"true"`
}

var Config appConfig

func init() {
	if err := envconfig.Process("sturgeon", &Config); err != nil {
		log.Fatal(err)
	}
	log.Printf("%+v", Config)
}
