# ETL Router

A distributed ETL (Extract, Transform, Load) routing system built in Rust with Raft consensus for high availability.

## Overview

ETL Router provides a scalable, fault-tolerant platform for routing data between sources, transforms, and sinks. It uses Raft consensus to ensure consistent state across cluster nodes and supports dynamic service registration, pipeline management, and consumer group coordination.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Sources   │────▶│ ETL Router  │────▶│    Sinks    │
│ (Kafka, S3) │     │   Cluster   │     │ (S3, gRPC)  │
└─────────────┘     └─────────────┘     └─────────────┘
                           │
                    ┌──────┴──────┐
                    │ Transforms  │
                    │(Filter, Map)│
                    └─────────────┘
```

## Features

- **Raft Consensus** - Distributed state machine for high availability
- **Service Registry** - Dynamic registration of sources, transforms, and sinks
- **Pipeline Management** - DAG-based routing with fan-out support
- **Consumer Groups** - Coordinated consumption with partition assignment
- **Offset Tracking** - Exactly-once semantics with checkpoint support
- **Watermarks** - Event-time processing with watermark propagation
- **GraphQL API** - Query and mutate cluster state
- **gRPC API** - High-performance service communication
- **Web Dashboard** - Real-time monitoring and management UI

## Components

| Crate | Description |
|-------|-------------|
| `etl-router` | Main application binary |
| `etl-raft` | Raft consensus implementation |
| `etl-registry` | Service registry and group coordination |
| `etl-routing` | DAG-based routing engine |
| `etl-graphql` | GraphQL API server |
| `etl-grpc` | gRPC service handlers |
| `etl-proto` | Protocol buffer definitions |
| `etl-dsl` | Pipeline DSL and manifest parsing |
| `etl-buffer` | Backpressure and buffer management |
| `etl-dlq` | Dead letter queue handling |
| `etl-metrics` | Prometheus metrics |
| `etlctl` | CLI tool for cluster management |

## Quick Start

### Prerequisites

- Rust 1.75+
- Node.js 20+ (for web dashboard)

### Running the Router

```bash
# Build and run
cargo run -p etl-router

# The router starts:
# - gRPC server on :50051
# - GraphQL API on :8080/graphql
```

### Running the Web Dashboard

```bash
cd web
npm install
npm run db:generate
npm run db:push
npm run dev

# Open http://localhost:5173
```

### Using etlctl

```bash
# Build the CLI
cargo build -p etlctl

# Apply a pipeline manifest
./target/debug/etlctl apply -f examples/manifests/pipelines/user-analytics.yaml

# List pipelines
./target/debug/etlctl get pipelines

# Describe a service
./target/debug/etlctl describe service my-source
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GRPC_PORT` | gRPC server port | `50051` |
| `GRAPHQL_PORT` | GraphQL server port | `8080` |
| `RAFT_NODE_ID` | Raft node identifier | `1` |
| `RAFT_PEERS` | Comma-separated peer addresses | - |

### Manifest Example

```yaml
apiVersion: etl.router/v1
kind: Pipeline
metadata:
  name: user-analytics
spec:
  source:
    ref: kafka-user-events
  transforms:
    - ref: filter-active-users
    - ref: mask-pii
  sink:
    ref: grpc-analytics
  enabled: true
```

## Web Dashboard

The web dashboard provides:

- **Dashboard** - Cluster overview with status cards
- **Services** - View registered sources, transforms, and sinks
- **Pipelines** - DAG visualization and pipeline management
- **Cluster** - Raft cluster state and node information
- **Metrics** - Real-time throughput and consumer lag monitoring
- **Admin** - User management (when auth is enabled)

See [web/README.md](web/README.md) for detailed setup instructions.

## API

### GraphQL

```graphql
# Query cluster status
query {
  clusterStatus {
    nodeId
    role
    currentTerm
    leaderId
  }
}

# List services
query {
  services {
    serviceId
    serviceType
    endpoint
    health
  }
}

# Create a pipeline
mutation {
  createPipeline(input: {
    pipelineId: "my-pipeline"
    name: "My Pipeline"
    config: "..."
  }) {
    success
    message
  }
}
```

### gRPC

Protocol buffer definitions are in `crates/etl-proto/proto/`:

- `registry.proto` - Service registration
- `source.proto` - Source operations
- `sink.proto` - Sink operations
- `transform.proto` - Transform operations
- `checkpoint.proto` - Offset management
- `raft.proto` - Raft consensus

## Testing

```bash
# Run Rust tests
cargo nextest run

# Run web E2E tests
cd web
npm run test:e2e
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

Copyright (c) 2025 Alex Choi
