# conveyor-etl-operator

Kubernetes operator for managing Conveyor ETL resources.

## Overview

The operator watches Custom Resource Definitions (CRDs) and reconciles them with the actual cluster state. It manages the lifecycle of router clusters, pipelines, and data pipeline services.

## Custom Resources

### EtlRouterCluster

Deploys and manages a Raft cluster of router nodes.

```yaml
apiVersion: conveyor.etl/v1
kind: EtlRouterCluster
metadata:
  name: my-cluster
spec:
  replicas: 3
  image: conveyor-etl-router:latest
  raft:
    electionTimeoutMs: 300
    heartbeatIntervalMs: 100
  resources:
    requests:
      memory: "256Mi"
      cpu: "100m"
    limits:
      memory: "512Mi"
      cpu: "500m"
  storage:
    size: 10Gi
    storageClassName: standard
```

### EtlPipeline

Defines a data pipeline from source through transforms to sink.

```yaml
apiVersion: conveyor.etl/v1
kind: EtlPipeline
metadata:
  name: user-analytics
spec:
  source: kafka-users
  steps:
    - filter-active
    - enrich-geo
  sink: clickhouse-analytics
  dlq:
    enabled: true
    maxRetries: 3
```

### EtlSource / EtlTransform / EtlSink

Define individual data pipeline services.

```yaml
apiVersion: conveyor.etl/v1
kind: EtlSource
metadata:
  name: kafka-users
spec:
  grpc:
    endpoint: kafka-source-svc:50051
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  conveyor-etl-operator              │
├─────────────────────────────────────────────────┤
│  ┌───────────────────────────────────────────┐  │
│  │              Controller Manager           │  │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────────┐  │  │
│  │  │ Cluster │ │Pipeline │ │  Resource   │  │  │
│  │  │  Ctrl   │ │  Ctrl   │ │   Ctrls     │  │  │
│  │  └────┬────┘ └────┬────┘ └──────┬──────┘  │  │
│  └───────┼───────────┼─────────────┼─────────┘  │
│          │           │             │            │
│          ▼           ▼             ▼            │
│  ┌───────────────────────────────────────────┐  │
│  │              Kubernetes API               │  │
│  │   (StatefulSets, Services, ConfigMaps)    │  │
│  └───────────────────────────────────────────┘  │
│                      │                          │
│                      ▼                          │
│  ┌───────────────────────────────────────────┐  │
│  │         Conveyor ETL Cluster (gRPC)       │  │
│  └───────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

## Installation

### Install CRDs

```bash
kubectl apply -f crates/conveyor-etl-operator/deploy/crds/crds.yaml
```

### Deploy Operator

```bash
# Using kustomize
kubectl apply -k crates/conveyor-etl-operator/deploy/operator/

# Using Helm
helm install conveyor-etl-operator crates/conveyor-etl-operator/deploy/helm/conveyor-etl-operator/
```

## Controllers

### ClusterController
- Creates StatefulSet for router nodes
- Manages headless Service for Raft communication
- Creates ConfigMap with cluster configuration
- Polls cluster health and updates status

### PipelineController
- Validates pipeline references (source, transforms, sink exist)
- Submits pipeline to router cluster via gRPC
- Updates pipeline status with assignment info

### ResourceControllers
- SourceController, TransformController, SinkController
- Register/deregister services with router cluster

## Development

```bash
# Run locally against a cluster
cargo run -p conveyor-etl-operator

# Build Docker image
docker build -f crates/conveyor-etl-operator/Dockerfile -t conveyor-etl-operator:dev .
```

## Helm Values

```yaml
replicaCount: 1
image:
  repository: conveyor-etl-operator
  tag: latest
rbac:
  create: true
serviceAccount:
  create: true
resources:
  limits:
    cpu: 200m
    memory: 256Mi
```
