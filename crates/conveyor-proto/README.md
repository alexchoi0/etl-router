# conveyor-proto

Protocol buffer definitions and generated Rust code.

## Overview

This crate contains all `.proto` files defining the gRPC services and message types used throughout Conveyor. The Rust code is generated at build time using `tonic-build`.

## Proto Files

| File | Description |
|------|-------------|
| `common.proto` | Shared types: Record, RecordBatch, Endpoint, etc. |
| `source.proto` | SourceService for pulling records |
| `transform.proto` | TransformService for processing batches |
| `sink.proto` | SinkService for writing records |
| `lookup.proto` | LookupService for enrichment |
| `registry.proto` | ServiceRegistry for service discovery |
| `checkpoint.proto` | CheckpointService for offset management |
| `sidecar.proto` | SidecarCoordinator and SidecarDataPlane |
| `raft.proto` | Raft consensus protocol |
| `backup.proto` | Backup and restore operations |
| `router.proto` | Router-specific types |

## Services

### SourceService

```protobuf
service SourceService {
  rpc PullRecords(PullRequest) returns (stream RecordBatch);
  rpc Acknowledge(AckRequest) returns (AckResponse);
  rpc GetWatermark(Empty) returns (WatermarkResponse);
}
```

### TransformService

```protobuf
service TransformService {
  rpc ProcessBatch(ProcessBatchRequest) returns (ProcessBatchResponse);
  rpc GetCapabilities(Empty) returns (Capabilities);
}
```

### SinkService

```protobuf
service SinkService {
  rpc WriteBatch(WriteBatchRequest) returns (WriteBatchResponse);
  rpc GetCapacity(Empty) returns (CapacityResponse);
  rpc Flush(FlushRequest) returns (FlushResponse);
}
```

### ServiceRegistry

```protobuf
service ServiceRegistry {
  rpc Register(RegisterRequest) returns (RegisterResponse);
  rpc Deregister(DeregisterRequest) returns (DeregisterResponse);
  rpc Heartbeat(stream HeartbeatRequest) returns (stream HeartbeatResponse);
  rpc WatchServices(WatchServicesRequest) returns (stream ServiceEvent);
}
```

### SidecarCoordinator

```protobuf
service SidecarCoordinator {
  rpc RegisterSidecar(RegisterSidecarRequest) returns (RegisterSidecarResponse);
  rpc Heartbeat(SidecarHeartbeatRequest) returns (SidecarHeartbeatResponse);
}
```

### SidecarDataPlane

```protobuf
service SidecarDataPlane {
  rpc ReceiveRecords(ReceiveRecordsRequest) returns (ReceiveRecordsResponse);
  rpc PushRecords(stream RecordBatch) returns (PushResponse);
}
```

## Key Types

### Record

```protobuf
message Record {
  string record_id = 1;
  string record_type = 2;
  bytes payload = 3;
  map<string, string> metadata = 4;
  google.protobuf.Timestamp timestamp = 5;
  string source_id = 6;
  uint64 sequence_number = 7;
}
```

### RecordBatch

```protobuf
message RecordBatch {
  string batch_id = 1;
  repeated Record records = 2;
  Watermark watermark = 3;
}
```

## Usage

```rust
use conveyor_proto::common::{Record, RecordBatch};
use conveyor_proto::transform::transform_service_client::TransformServiceClient;
use conveyor_proto::sink::sink_service_server::SinkServiceServer;

// Client usage
let mut client = TransformServiceClient::connect("http://localhost:50051").await?;

// Server usage
let service = MySinkService::new();
Server::builder()
    .add_service(SinkServiceServer::new(service))
    .serve(addr)
    .await?;
```

## Build

Proto files are compiled during `cargo build` via `build.rs`:

```rust
fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(
            &["proto/common.proto", "proto/source.proto", ...],
            &["proto/"],
        )
        .unwrap();
}
```

## Generated Modules

```rust
pub mod common { /* Record, RecordBatch, Endpoint, ... */ }
pub mod source { /* SourceService client/server */ }
pub mod transform { /* TransformService client/server */ }
pub mod sink { /* SinkService client/server */ }
pub mod lookup { /* LookupService client/server */ }
pub mod registry { /* ServiceRegistry client/server */ }
pub mod checkpoint { /* CheckpointService client/server */ }
pub mod sidecar { /* SidecarCoordinator, SidecarDataPlane */ }
pub mod raft { /* RaftService client/server */ }
pub mod backup { /* BackupService client/server */ }
```
