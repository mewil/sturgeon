package io.mewil.elasticgraphql;

import graphql.GraphQL;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Component
public class GraphQLProvider {

    private GraphQLSchemaBuilder graphQLSchemaBuilder;

    private GraphQL graphQL;

    @PostConstruct
    public void init() throws IOException {
        graphQLSchemaBuilder = new GraphQLSchemaBuilder(new ElasticsearchClient());
        updateGraphQLSchema();
    }

    private void updateGraphQLSchema() throws IOException {
        this.graphQL = GraphQL.newGraphQL(graphQLSchemaBuilder.getSchema()).build();
    }

    @Bean
    public GraphQL graphQL() {
        return graphQL;
    }
}
