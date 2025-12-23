# conveyor-etl-router

Main binary for the Conveyor ETL control plane.

## Overview

This crate provides the entry point for running a Conveyor ETL node. It orchestrates all the components needed for a router cluster member: gRPC services, Raft consensus, and service registry.

## Usage

```bash
cargo run -p conveyor-etl-router -- \
  --node-id 1 \
  --listen-addr 127.0.0.1:50051 \
  --raft-addr 127.0.0.1:50052 \
  --peers 127.0.0.1:50053,127.0.0.1:50054 \
  --data-dir ./data
```

## Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--config`, `-c` | Path to configuration file | `config/router.yaml` |
| `--node-id`, `-n` | Unique Raft node identifier | Required |
| `--listen-addr`, `-l` | gRPC server bind address | `127.0.0.1:50051` |
| `--raft-addr` | Raft RPC bind address | `127.0.0.1:50052` |
| `--peers` | Comma-separated Raft peer addresses | None |
| `--data-dir` | Persistent storage directory | `./data` |

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    conveyor-etl-router                  │
├─────────────────────────────────────────────────────┤
│  ┌─────────────────────────┐  ┌─────────────┐       │
│  │      gRPC Server        │  │ Raft Node   │       │
│  │        :50051           │  │  :50052     │       │
│  └───────────┬─────────────┘  └──────┬──────┘       │
│              │                       │              │
│              └───────────┬───────────┘              │
│                          │                          │
│                 ┌────────▼─────────┐                │
│                 │ Service Registry │                │
│                 │   (Raft State)   │                │
│                 └──────────────────┘                │
└─────────────────────────────────────────────────────┘
```

## Services Exposed

- **gRPC** (`--listen-addr`): Service registry, sidecar coordination, checkpoint management
- **Raft** (`--raft-addr`): Inter-node consensus protocol

## Configuration File

```yaml
cluster:
  election_timeout_ms: 300
  heartbeat_interval_ms: 100
  snapshot_interval: 10000

buffer:
  max_total_records: 100000
  max_per_stage: 10000
  backpressure_threshold: 0.8

grpc:
  max_message_size: 16777216
  keepalive_interval_secs: 30
```

## Dependencies

- `conveyor-etl-grpc` - gRPC service implementations
- `conveyor-etl-raft` - Raft consensus
- `conveyor-etl-config` - Configuration loading
