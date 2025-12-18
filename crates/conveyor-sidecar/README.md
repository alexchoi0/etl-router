# conveyor-sidecar

Sidecar binary for pod-level data routing.

## Overview

The sidecar runs alongside application containers in each pod. It discovers local gRPC services, registers with the router cluster, receives pipeline assignments, and routes records between stages.

## Usage

```bash
cargo run -p conveyor-sidecar
```

Or with explicit configuration:

```bash
CLUSTER_ENDPOINT=conveyor-router:50051 \
SIDECAR_ID=sidecar-pod-abc \
POD_NAME=my-pod \
NAMESPACE=default \
GRPC_PORT=9091 \
cargo run -p conveyor-sidecar
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SIDECAR_ID` | Unique sidecar identifier | Auto-generated UUID |
| `POD_NAME` | Kubernetes pod name | `unknown` |
| `NAMESPACE` | Kubernetes namespace | `default` |
| `CLUSTER_ENDPOINT` | Router cluster address | `localhost:50051` |
| `GRPC_PORT` | Sidecar gRPC server port | `9091` |
| `DISCOVERY_START_PORT` | Start of port scan range | `50051` |
| `DISCOVERY_END_PORT` | End of port scan range | `50060` |
| `HEARTBEAT_INTERVAL_SECS` | Heartbeat frequency | `5` |

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                      Pod                            │
├─────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────┐   │
│  │              conveyor-sidecar                │   │
│  │  ┌───────────┐  ┌───────────┐  ┌─────────┐   │   │
│  │  │ Discovery │  │ Cluster   │  │ Data    │   │   │
│  │  │ Module    │  │ Client    │  │ Plane   │   │   │
│  │  └─────┬─────┘  └─────┬─────┘  └────┬────┘   │   │
│  │        │              │             │        │   │
│  │        │      ┌───────▼───────┐     │        │   │
│  │        │      │ Routing Table │     │        │   │
│  │        │      └───────┬───────┘     │        │   │
│  │        │              │             │        │   │
│  │  ┌─────▼──────────────▼──────────────▼─────┐ │   │
│  │  │         Local / Remote Router           │ │   │
│  │  └─────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────┘   │
│                        │                            │
│  ┌─────────────────────▼───────────────────────┐    │
│  │          Local gRPC Services                │    │
│  │   (Source, Transform, Sink on localhost)    │    │
│  └─────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

## Modules

### `discovery`
Scans localhost ports and uses gRPC reflection to identify services (Source, Transform, Sink).

### `cluster_client`
- **Registration**: Connects to router, reports local services
- **Heartbeat**: Periodic health updates, receives commands (assign/revoke pipelines)

### `routing`
- **RoutingTable**: Maps pipeline stages to endpoints
- **LocalRouter**: Calls local transform/sink services
- **RemoteRouter**: Forwards records to other sidecars
- **ClientPool**: Reusable gRPC connection pool

### `data_plane`
gRPC server that receives records from sources and other sidecars.

## Lifecycle

1. **Startup**: Scan ports, discover local services via gRPC reflection
2. **Register**: Connect to router cluster, report services
3. **Receive Assignments**: Router sends pipeline stage assignments
4. **Route Records**: Process incoming records through assigned stages
5. **Heartbeat**: Every 5s, report health and receive commands
6. **Shutdown**: Deregister from cluster

## Kubernetes Deployment

```yaml
containers:
  - name: my-transform
    image: my-transform:latest
    ports:
      - containerPort: 50051
  - name: sidecar
    image: conveyor-sidecar:latest
    env:
      - name: POD_NAME
        valueFrom:
          fieldRef:
            fieldPath: metadata.name
      - name: NAMESPACE
        valueFrom:
          fieldRef:
            fieldPath: metadata.namespace
      - name: CLUSTER_ENDPOINT
        value: "conveyor-router:50051"
```
