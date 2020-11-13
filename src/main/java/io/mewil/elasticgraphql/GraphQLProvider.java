package io.mewil.elasticgraphql;

import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Component
public class GraphQLProvider {

    private ElasticsearchClient elasticsearchClient;

    private GraphQL graphQL;

    @PostConstruct
    public void init() throws IOException {
        elasticsearchClient = new ElasticsearchClient();
        updateGraphQLSchema();
    }

    private void updateGraphQLSchema() throws IOException {
        GraphQLSchema graphQLSchema = GraphQLSchemaUtils.buildSchemaFromIndexMappings(elasticsearchClient.getMappings());
        this.graphQL = GraphQL.newGraphQL(graphQLSchema).build();
    }

    private GraphQLSchema buildSchema(String sdl) {
        TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(sdl);
        RuntimeWiring runtimeWiring = buildWiring();
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        return schemaGenerator.makeExecutableSchema(typeRegistry, runtimeWiring);
    }

    private RuntimeWiring buildWiring() {
        return RuntimeWiring.newRuntimeWiring()
                .build();
    }

    @Bean
    public GraphQL graphQL() {
        return graphQL;
    }
}
