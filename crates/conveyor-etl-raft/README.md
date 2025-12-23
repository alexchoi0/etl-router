# conveyor-raft

Raft consensus implementation for distributed state management.

## Overview

This crate provides a complete Raft consensus implementation tailored for Conveyor. It handles leader election, log replication, and state machine application to ensure consistent state across all cluster nodes.

## Features

- Leader election with randomized timeouts
- Log replication with consistency guarantees
- Snapshot support for state compaction
- Pluggable state machine via `RouterStateMachine`
- gRPC transport between nodes

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     RaftNode                        │
├─────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│  │  RaftCore   │  │ LogStorage  │  │  Transport  │  │
│  │  (state,    │  │ (persistent │  │  (gRPC to   │  │
│  │   voting)   │  │   log)      │  │   peers)    │  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  │
│         │                │                │         │
│         └────────────────┼────────────────┘         │
│                          │                          │
│                 ┌────────▼────────┐                 │
│                 │ StateMachine    │                 │
│                 │ (apply cmds)    │                 │
│                 └─────────────────┘                 │
└─────────────────────────────────────────────────────┘
```

## Usage

```rust
use conveyor_raft::{RaftNode, RaftConfig, RouterStateMachine};

let config = RaftConfig {
    node_id: 1,
    election_timeout_min_ms: 150,
    election_timeout_max_ms: 300,
    heartbeat_interval_ms: 50,
};

let state_machine = RouterStateMachine::new();
let node = RaftNode::new(config, state_machine, peers).await?;

// Propose a command (only succeeds on leader)
node.propose(RouterCommand::RegisterService { ... }).await?;

// Read current state (can be done on any node)
let state = node.state().await;
```

## Components

### RaftCore

The core consensus algorithm implementation:

- **Roles**: Follower, Candidate, Leader
- **Election**: Randomized timeout triggers candidacy
- **Heartbeat**: Leader sends periodic AppendEntries
- **Replication**: Log entries replicated to majority before commit

### LogStorage

Persistent log storage with:

- Append entries
- Get entries by index range
- Truncate after index (for conflict resolution)
- Persist to disk for durability

### RouterStateMachine

Domain-specific state machine that processes commands:

```rust
pub enum RouterCommand {
    RegisterService { service_id, service_type, endpoint, ... },
    DeregisterService { service_id },
    RegisterSidecar { sidecar_id, endpoint, services },
    DeregisterSidecar { sidecar_id },
    AssignPipeline { pipeline_id, sidecar_id, stages },
    RevokePipeline { pipeline_id, sidecar_id },
}
```

### RaftTransport

gRPC-based transport implementing:

- `AppendEntries` - Log replication and heartbeat
- `RequestVote` - Leader election

## Configuration

```rust
pub struct RaftConfig {
    pub node_id: NodeId,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
}
```

## State Machine Flow

```
1. Client proposes command to leader
2. Leader appends to local log
3. Leader replicates to followers (AppendEntries)
4. Followers append and acknowledge
5. Leader commits once majority acknowledges
6. Leader applies to state machine
7. Leader responds to client
8. Followers apply on next heartbeat
```

## Exports

```rust
pub use state_machine::{RouterStateMachine, RouterState};
pub use storage::LogStorage;
pub use network::RaftNetwork;
pub use commands::{RouterCommand, ...};
pub use core::{NodeId, Term, LogIndex, RaftCore, RaftConfig, RaftRole};
pub use transport::{RaftTransport, RaftTransportService};
pub use node::RaftNode;
pub use backup_service::BackupServiceImpl;
```

## Testing

```bash
cargo nextest run -p conveyor-raft
```

Tests cover:
- Single node quorum
- Leader election
- Term advancement
- Log replication
- State machine application
