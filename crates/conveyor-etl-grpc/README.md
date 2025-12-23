# conveyor-grpc

gRPC server implementations and client wrappers.

## Overview

This crate provides all gRPC service implementations for the router, plus client wrappers for calling external services (transforms, sinks, lookups).

## Server Components

### RouterServer

Main server that hosts all gRPC services:

```rust
use conveyor_grpc::RouterServer;

let server = RouterServer::new(
    node_id,
    grpc_addr,
    raft_addr,
    peers,
    data_dir,
    settings,
).await?;

server.run().await?;
```

### Service Handlers

| Handler | Proto Service | Description |
|---------|---------------|-------------|
| `SourceHandler` | `SourceService` | Handle source push/pull |
| `RegistryHandler` | `ServiceRegistry` | Service registration |
| `CheckpointHandler` | `CheckpointService` | Offset management |
| `SidecarHandler` | `SidecarCoordinator` | Sidecar coordination |

## Client Wrappers

### TransformClient

```rust
use conveyor_grpc::transform_client::TransformClient;

let mut client = TransformClient::connect(
    "http://transform-svc:50051".to_string(),
    "my-transform".to_string(),
).await?;

let response = client.process_batch(batch, config).await?;
```

### SinkClient

```rust
use conveyor_grpc::sink_client::SinkClient;

let mut client = SinkClient::connect(
    "http://sink-svc:50051".to_string(),
    "my-sink".to_string(),
).await?;

client.write_batch(batch, options).await?;
client.flush(true).await?;
```

### LookupClient

```rust
use conveyor_grpc::lookup_client::LookupClient;

let mut client = LookupClient::connect(
    "http://lookup-svc:50051".to_string(),
    "my-lookup".to_string(),
).await?;

let result = client.lookup(record, key_fields).await?;
let results = client.batch_lookup(records, key_fields).await?;
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   RouterServer                      │
├─────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│  │   Source    │  │  Registry   │  │ Checkpoint  │  │
│  │   Handler   │  │   Handler   │  │   Handler   │  │
│  └─────────────┘  └─────────────┘  └─────────────┘  │
│  ┌─────────────┐  ┌─────────────┐                   │
│  │   Sidecar   │  │    Raft     │                   │
│  │   Handler   │  │  Transport  │                   │
│  └─────────────┘  └─────────────┘                   │
├─────────────────────────────────────────────────────┤
│                  Client Wrappers                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│  │ Transform   │  │    Sink     │  │   Lookup    │  │
│  │   Client    │  │   Client    │  │   Client    │  │
│  └─────────────┘  └─────────────┘  └─────────────┘  │
└─────────────────────────────────────────────────────┘
```

## SidecarCoordinator

Handles sidecar lifecycle:

```rust
use conveyor_grpc::SidecarCoordinatorImpl;

// Sidecar registration
// - Receives sidecar's local services
// - Proposes RegisterSidecar to Raft
// - Returns initial pipeline assignments

// Heartbeat
// - Receives health updates
// - Returns commands (Assign, Revoke, Drain)
```

## Exports

```rust
pub use server::RouterServer;
pub use sidecar_handler::SidecarCoordinatorImpl;
```

## Dependencies

- `conveyor-proto` - Protocol buffer definitions
- `conveyor-raft` - Raft consensus
- `conveyor-registry` - Service registry
- `tonic` - gRPC framework
