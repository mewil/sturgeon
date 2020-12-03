package io.mewil.sturgeon;

import graphql.GraphQL;
import io.mewil.sturgeon.schema.GraphQLSchemaBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Component
public class GraphQLProvider {

  private GraphQL graphQL;

  @PostConstruct
  public void init() throws IOException {
    updateGraphQLSchema();
  }

  private void updateGraphQLSchema() throws IOException {
    this.graphQL = GraphQL.newGraphQL(GraphQLSchemaBuilder.getSchema()).build();
  }

  @Bean
  public GraphQL graphQL() {
    return graphQL;
  }
}
