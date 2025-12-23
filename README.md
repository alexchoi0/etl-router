# Conveyor ETL

A distributed, high-availability Conveyor ETL platform built in Rust. Route data through pipelines of sources, transforms, and sinks with automatic service discovery, backpressure handling, and Kubernetes-native deployment.

## What is Conveyor ETL?

Conveyor ETL is a **control plane** for data pipelines. Instead of hardcoding connections between your data services, you define pipelines declaratively and Conveyor handles:

- **Service Discovery** - Automatically finds and registers your gRPC services
- **Routing** - Routes records through pipeline stages across pods
- **Backpressure** - Prevents overload by propagating flow control upstream
- **High Availability** - Raft consensus ensures no single point of failure
- **Kubernetes Native** - Deploy with CRDs and manage with `kubectl`

```
┌───────────────────────────────────────────────────────────────┐
│                        CONTROL PLANE                          │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │         Conveyor ETL Cluster (Raft Consensus)           │  │
│  │         ┌──────────┐ ┌──────────┐ ┌──────────┐          │  │
│  │         │  Leader  │ │ Follower │ │ Follower │          │  │
│  │         └────┬─────┘ └────┬─────┘ └────┬─────┘          │  │
│  └──────────────┼────────────┼────────────┼────────────────┘  │
└─────────────────┼────────────┼────────────┼───────────────────┘
                  │            │            │
                  │     Pipeline Assignments
                  ▼            ▼            ▼
┌────────────────────────────────────────────────────────────────┐
│                         DATA PLANE                             │
│                                                                │
│   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐    │
│   │ Source Pod  │      │Transform Pod│      │  Sink Pod   │    │
│   │ ┌─────────┐ │      │ ┌─────────┐ │      │ ┌─────────┐ │    │
│   │ │ Sidecar │─┼─────▶│ │ Sidecar │─┼─────▶│ │ Sidecar │ │    │
│   │ └────┬────┘ │      │ └────┬────┘ │      │ └────┬────┘ │    │
│   │      │      │      │      │      │      │      │      │    │
│   │ ┌────▼────┐ │      │ ┌────▼────┐ │      │ ┌────▼────┐ │    │
│   │ │  Kafka  │ │      │ │ Filter  │ │      │ │ ClickHs │ │    │
│   │ └─────────┘ │      │ └─────────┘ │      │ └─────────┘ │    │
│   └─────────────┘      └─────────────┘      └─────────────┘    │
│                                                                │
│                    Records flow pod-to-pod                     │
└────────────────────────────────────────────────────────────────┘
```

## Key Features

| Feature | Description |
|---------|-------------|
| **Sidecar Architecture** | Deploy a sidecar in each pod - it discovers local services and handles routing |
| **Raft Consensus** | 3+ node cluster with automatic leader election and failover |
| **Pipeline DSL** | Define pipelines in YAML with fan-in, fan-out, and shared stages |
| **Backpressure** | Credit-based flow control prevents unbounded memory growth |
| **Dead Letter Queue** | Failed records are captured with full error context for replay |
| **Kubernetes Operator** | CRDs for `Pipeline`, `Source`, `Transform`, `Sink` |
| **CLI Tool** | `conveyor-etl-cli` for pipeline management, backup/restore, debugging |

## How It Works

```
1. Sidecars register with router     2. Router assigns pipeline stages
   ┌─────────┐                          ┌─────────┐
   │ Sidecar │ ───"I have Kafka"───▶    │ Router  │
   └─────────┘                          │ Cluster │
   ┌─────────┐                          │         │ ───"You handle stage 1"──▶ Sidecar
   │ Sidecar │ ───"I have Filter"──▶    │  (Raft) │
   └─────────┘                          └─────────┘

3. Records flow directly between sidecars (not through router)
   ┌─────────┐         ┌─────────┐         ┌─────────┐
   │ Sidecar │ ──────▶ │ Sidecar │ ──────▶ │ Sidecar │
   │ (Kafka) │ records │ (Filter)│ records │(ClickHs)│
   └─────────┘         └─────────┘         └─────────┘
```

1. **Sidecars register** with the router cluster, reporting their local services
2. **Router assigns pipeline stages** to sidecars based on what services they have
3. **Records flow** directly between sidecars (data plane), not through the router
4. **Backpressure propagates** upstream when any stage is slow

## Quick Start

### Prerequisites

- Rust 1.75+
- Docker (for Kubernetes deployment)

### Run Locally

```bash
# Start the router
cargo run -p conveyor-etl-router

# In another terminal, start a sidecar
cargo run -p conveyor-etl-sidecar

# Use the CLI
cargo run -p conveyor-etl-cli -- get pipelines
```

### Deploy to Kubernetes

```bash
# Install CRDs and operator
kubectl apply -f crates/conveyor-etl-operator/deploy/crds/crds.yaml
kubectl apply -k crates/conveyor-etl-operator/deploy/operator/

# Create a cluster
kubectl apply -f - <<EOF
apiVersion: conveyor.etl/v1
kind: ConveyorCluster
metadata:
  name: my-cluster
spec:
  replicas: 3
  image: conveyor-etl/router:latest
EOF

# Create a pipeline
kubectl apply -f - <<EOF
apiVersion: conveyor.etl/v1
kind: Pipeline
metadata:
  name: user-analytics
spec:
  source: kafka-users
  steps:
    - filter-active
    - mask-pii
  sink: clickhouse-analytics
EOF
```

### Use the CLI

```bash
# Apply manifests
conveyor-etl-cli apply -f pipelines/

# List resources
conveyor-etl-cli get pipelines
conveyor-etl-cli get sources --all-namespaces

# Visualize pipeline DAG
conveyor-etl-cli graph -f pipelines/ --format dot | dot -Tpng > pipeline.png

# Backup cluster state
conveyor-etl-cli backup create --dest s3://my-bucket/backups/

# Restore from backup
conveyor-etl-cli backup restore abc123 --source s3://my-bucket/backups/
```

## Pipeline Definition

Define pipelines using simple YAML manifests:

```yaml
# source.yaml
apiVersion: conveyor.etl/v1
kind: Source
metadata:
  name: kafka-users
  namespace: default
spec:
  grpc:
    endpoint: kafka-source-svc:50051

# transform.yaml
apiVersion: conveyor.etl/v1
kind: Transform
metadata:
  name: filter-active
spec:
  grpc:
    endpoint: filter-svc:50051

# pipeline.yaml
apiVersion: conveyor.etl/v1
kind: Pipeline
metadata:
  name: user-analytics
spec:
  source: kafka-users
  steps:
    - filter-active
    - enrich-geo
  sink: clickhouse-analytics
  dlq:
    sink: error-handler
    maxRetries: 3
```

## Architecture Overview

```
                         ┌───────────────┐
                         │  conveyor-etl-cli  │
                         └────┬──────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  CONTROL PLANE                                                   │
│  ┌───────────────────────────────────────────────────────────┐   │
│  │  Conveyor ETL Cluster: Leader ◄──► Follower ◄──► Follower │   │
│  └───────────────────────────────────────────────────────────┘   │
│                          ▲                                       │
│                          │                                       │
│                 ┌────────┴────────┐                              │
│                 │   K8s Operator  │                              │
│                 └─────────────────┘                              │
└──────────────────────────────────────────────────────────────────┘
                           │
                    Assign Stages
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  DATA PLANE                                                 │
│       Source  ──────▶  Transform  ──────▶  Sink             │
└─────────────────────────────────────────────────────────────┘
```

For detailed architecture documentation, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Project Structure

```
conveyor-etl/
├── crates/
│   ├── conveyor-etl-router/      # Main router binary
│   ├── conveyor-etl-sidecar/     # Pod sidecar binary
│   ├── conveyor-etl-operator/    # Kubernetes operator
│   ├── conveyor-etl-cli/         # CLI tool
│   ├── conveyor-etl-raft/        # Raft consensus
│   ├── conveyor-etl-grpc/        # gRPC handlers
│   ├── conveyor-etl-proto/       # Protocol buffers
│   ├── conveyor-etl-registry/    # Service registry
│   ├── conveyor-etl-routing/     # Routing engine
│   ├── conveyor-etl-dsl/         # Pipeline DSL
│   ├── conveyor-etl-buffer/      # Backpressure buffers
│   ├── conveyor-etl-dlq/         # Dead letter queue
│   ├── conveyor-etl-config/      # Configuration
│   └── conveyor-etl-metrics/     # Prometheus metrics
```

## Configuration

### Router Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GRPC_PORT` | gRPC server port | `50051` |
| `RAFT_NODE_ID` | Unique node identifier | `1` |
| `RAFT_PEERS` | Comma-separated peer addresses | - |
| `DATA_DIR` | Persistent storage path | `./data` |

### Sidecar Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SIDECAR_ID` | Unique sidecar identifier | auto-generated |
| `CLUSTER_ENDPOINT` | Router cluster address | `conveyor-etl-router:50051` |
| `GRPC_PORT` | Sidecar gRPC port | `9091` |
| `DISCOVERY_PORTS` | Ports to scan for services | `50051-50060` |

## API Reference

### gRPC

Protocol definitions in `crates/conveyor-etl-proto/proto/`:

| Proto | Services |
|-------|----------|
| `source.proto` | `SourceService` - Pull records from sources |
| `transform.proto` | `TransformService` - Process record batches |
| `sink.proto` | `SinkService` - Write records to destinations |
| `registry.proto` | `ServiceRegistry` - Register/discover services |
| `sidecar.proto` | `SidecarCoordinator`, `SidecarDataPlane` |
| `raft.proto` | `RaftService` - Consensus protocol |

## Testing

```bash
# Run all tests
cargo nextest run

# Run specific crate tests
cargo nextest run -p conveyor-etl-raft

# Run with logging
RUST_LOG=debug cargo nextest run
```

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit PRs to the `main` branch.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Copyright (c) 2025 Alex Choi
