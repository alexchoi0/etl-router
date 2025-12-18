# conveyor-graphql

GraphQL API for the Conveyor dashboard.

## Overview

This crate provides a GraphQL API for querying and managing Conveyor resources. It powers the web dashboard and can be used by external tools.

## Schema

### Queries

```graphql
type Query {
  # Cluster status
  clusterStatus: ClusterStatus!

  # List all services
  services: [Service!]!

  # List all pipelines
  pipelines: [Pipeline!]!

  # Get pipeline by ID
  pipeline(id: String!): Pipeline

  # Get recent errors
  errors(limit: Int, offset: Int): ErrorConnection!
}
```

### Mutations

```graphql
type Mutation {
  # Create a new pipeline
  createPipeline(input: CreatePipelineInput!): PipelineResult!

  # Update pipeline
  updatePipeline(id: String!, input: UpdatePipelineInput!): PipelineResult!

  # Delete pipeline
  deletePipeline(id: String!): DeleteResult!

  # Retry failed records
  retryErrors(ids: [String!]!): RetryResult!

  # Clear errors
  clearErrors(ids: [String!]!): ClearResult!
}
```

### Types

```graphql
type ClusterStatus {
  nodeId: Int!
  role: String!
  currentTerm: Int!
  leaderId: Int
  nodes: [NodeInfo!]!
}

type Service {
  serviceId: String!
  serviceName: String!
  serviceType: ServiceType!
  endpoint: String!
  health: HealthStatus!
  labels: JSON
}

type Pipeline {
  id: String!
  name: String!
  source: String!
  stages: [Stage!]!
  sink: String!
  enabled: Boolean!
  status: PipelineStatus!
}

type ErrorInfo {
  id: String!
  errorType: ErrorType!
  message: String!
  pipelineId: String
  stageId: String
  recordId: String
  timestamp: DateTime!
  retryCount: Int!
}
```

## Usage

### Server Setup

```rust
use conveyor_graphql::{GraphQLServer, create_schema};

let schema = create_schema(raft_node, registry);
let server = GraphQLServer::new(schema, "0.0.0.0:8080".parse()?);
server.run().await?;
```

### Example Queries

```graphql
# Get cluster status
query {
  clusterStatus {
    nodeId
    role
    currentTerm
    leaderId
    nodes {
      id
      role
      healthy
    }
  }
}

# List pipelines
query {
  pipelines {
    id
    name
    source
    stages {
      id
      serviceType
      serviceName
    }
    sink
    enabled
  }
}

# Get errors with pagination
query {
  errors(limit: 20, offset: 0) {
    totalCount
    edges {
      node {
        id
        errorType
        message
        timestamp
        retryCount
      }
    }
  }
}
```

### Example Mutations

```graphql
# Create pipeline
mutation {
  createPipeline(input: {
    name: "user-analytics"
    source: "kafka-users"
    steps: ["filter-active", "enrich-geo"]
    sink: "clickhouse-analytics"
  }) {
    success
    pipelineId
    message
  }
}

# Retry failed records
mutation {
  retryErrors(ids: ["err-123", "err-456"]) {
    success
    retriedCount
  }
}
```

## Components

### RouterSchema

The GraphQL schema combining queries, mutations, and subscriptions:

```rust
pub type RouterSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

pub fn create_schema(
    raft_node: Arc<RwLock<RaftNode>>,
    registry: Arc<RwLock<ServiceRegistry>>,
) -> RouterSchema;
```

### ErrorBuffer

In-memory buffer for recent errors, queryable via GraphQL:

```rust
use conveyor_graphql::ErrorBuffer;

let buffer = ErrorBuffer::new(1000);  // Keep last 1000 errors

buffer.push(ErrorInfo { ... });

let recent = buffer.get_recent(20, 0);
let count = buffer.count();
```

## Endpoints

| Path | Description |
|------|-------------|
| `/graphql` | GraphQL endpoint (POST) |
| `/graphql/playground` | GraphQL Playground UI |
| `/health` | Health check |

## Exports

```rust
pub use schema::{create_schema, RouterSchema};
pub use server::GraphQLServer;
pub use error_buffer::ErrorBuffer;
pub use types::ErrorType;
```
