# Conveyor Architecture

This document describes the architecture of the Conveyor system, a distributed data routing platform for building Conveyor pipelines.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           KUBERNETES CLUSTER                                │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                         CONTROL PLANE                                 │  │
│  │                                                                       │  │
│  │   ┌─────────────┐    ┌─────────────────────────────────────────────┐  │  │
│  │   │  Operator   │    │         Router Cluster (Raft)               │  │  │
│  │   └──────┬──────┘    │  ┌────────┐  ┌────────┐  ┌────────┐         │  │  │
│  │          │           │  │ Leader │◄─┤Follower│◄─┤Follower│         │  │  │
│  │          └──────────▶│  └────────┘  └────────┘  └────────┘         │  │  │
│  │                      └─────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                      │                                      │
│                               Assign Stages                                 │
│                                      ▼                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                          DATA PLANE                                   │  │
│  │                                                                       │  │
│  │   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐            │  │
│  │   │   Pod A      │    │   Pod B      │    │   Pod C      │            │  │
│  │   │  ┌────────┐  │    │  ┌────────┐  │    │  ┌────────┐  │            │  │
│  │   │  │Sidecar │──┼───▶│  │Sidecar │──┼───▶│  │Sidecar │  │            │  │
│  │   │  └───┬────┘  │    │  └───┬────┘  │    │  └───┬────┘  │            │  │
│  │   │      │       │    │      │       │    │      │       │            │  │
│  │   │  ┌───▼────┐  │    │  ┌───▼─────┐ │    │  ┌───▼────┐  │            │  │
│  │   │  │ Source │  │    │  │Transform│ │    │  │  Sink  │  │            │  │
│  │   │  └────────┘  │    │  └─────────┘ │    │  └────────┘  │            │  │
│  │   └──────────────┘    └──────────────┘    └──────────────┘            │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
         ▲                               ▲
         │                               │
    ┌────┴────────┐                ┌─────┴─────┐
    │ conveyorctl │                │ Dashboard │
    └─────────────┘                └───────────┘
```

## Core Components

### Component Hierarchy

```
     Binaries                                Libraries
     ─────────                               ─────────
┌──────────────────┐                   ┌────────────────────┐
│ conveyor-router  │──────────────────▶│  conveyor-raft     │
│      (main)      │──────────────────▶│  conveyor-grpc     │
└──────────────────┘──────────────────▶│  conveyor-registry │
                                       │  conveyor-graphql  │
                                       └────────────────────┘
┌──────────────────┐                   ┌───────────────────┐
│ conveyor-sidecar │──────────────────▶│  conveyor-grpc    │
│      (pod)       │──────────────────▶│  conveyor-buffer  │
└──────────────────┘──────────────────▶│  conveyor-routing │
                                       └───────────────────┘
┌───────────────────┐                  ┌─────────────────┐
│ conveyor-operator │─────────────────▶│  conveyor-grpc  │
│       (k8s)       │                  └─────────────────┘
└───────────────────┘

┌──────────────────┐                   ┌────────────────┐
│   conveyorctl    │──────────────────▶│  conveyor-dsl  │
│      (CLI)       │──────────────────▶│  conveyor-grpc │
└──────────────────┘                   └────────────────┘

Shared: conveyor-proto, conveyor-config, conveyor-metrics, conveyor-dlq
```

## Router Cluster

The router cluster is the central control plane, providing service registry, pipeline management, and sidecar coordination. It uses Raft consensus for high availability.

### Raft State Machine

```
                    Election timeout
             ┌──────────────────────────┐
             │                          ▼
         ┌───┴────┐               ┌───────────┐
         │Follower│◄──────────────│ Candidate │◄─┐
         └───┬────┘  Discovers    └─────┬─────┘  │
             │       leader             │        │ Election
             │                          │        │ timeout
   ──────────┼──────────────────────────┼────────┘
   Start     │                          │ Wins
             │   Higher term seen       ▼
             │◄─────────────────────┌───────┐
             │                      │Leader │
             └──────────────────────└───────┘
```

### Raft Log Replication

```
  Client        Leader         Follower 1      Follower 2
    │             │                │                │
    │ Propose     │                │                │
    ├────────────▶│                │                │
    │             │ Append to log  │                │
    │             ├───────────────▶│ AppendEntries  │
    │             ├────────────────┼───────────────▶│
    │             │                │                │
    │             │◄───────────────┤ Success        │
    │             │◄───────────────┼────────────────┤
    │             │                │                │
    │             │ Commit (majority)               │
    │             │ Apply to state machine          │
    │◄────────────┤                │                │
    │  Response   │                │                │
```

### State Machine Commands

The Raft state machine processes these commands:

| Command | Description |
|---------|-------------|
| `RegisterService` | Register a source/transform/sink service |
| `DeregisterService` | Remove a service from registry |
| `RegisterSidecar` | Register a sidecar with its local services |
| `DeregisterSidecar` | Remove a sidecar (pod terminated) |
| `AssignPipeline` | Assign pipeline stages to a sidecar |
| `RevokePipeline` | Revoke pipeline assignment from sidecar |

## Sidecar Architecture

Each application pod runs a sidecar that handles service discovery, routing, and data flow.

### Sidecar Components

```
┌──────────────────────────────────────────────────────┐
│                   Sidecar Process                    │
│                                                      │
│  ┌───────────┐    ┌──────────────┐    ┌───────────┐  │
│  │ Discovery │───▶│Cluster Client│───▶│  Routing  │  │
│  │  Module   │    │              │    │  Engine   │  │
│  └─────┬─────┘    └──────────────┘    └─────┬─────┘  │
│        │                                    │        │
│        │         ┌─────────────┐            │        │
│        │         │ Data Plane  │◀───────────┘        │
│        │         │   Server    │                     │
│        │         └──────┬──────┘                     │
│        │                │                            │
│        │         ┌──────▼──────┐                     │
│        │         │   Buffer    │                     │
│        │         │  Manager    │                     │
│        │         └─────────────┘                     │
└────────┼────────────────┼────────────────────────────┘
         │                │
         ▼                ▼
┌──────────────┐  ┌──────────────────┐
│Local Services│  │  Remote          │
│(gRPC on pod) │  │  - Router Cluster│
└──────────────┘  │  - Other Sidecars│
                  └──────────────────┘
```

### Sidecar Lifecycle

```
  Sidecar            Local Services       Router (Leader)
    │                     │                     │
    │ Startup             │                     │
    ├────────────────────▶│ Scan ports          │
    │                     │ (gRPC reflection)   │
    │◀────────────────────┤                     │
    │                     │                     │
    │ RegisterSidecar(id, services)             │
    ├───────────────────────────────────────────▶
    │                                           │
    │                           Raft commit ────┤
    │                                           │
    │◀──────────────────────────────────────────┤
    │              InitialAssignments           │
    │                                           │
    │ Populate routing table                    │
    │                                           │
    │         ┌─── Every 5s ───┐                │
    │         │                │                │
    │ Heartbeat(health, load)  │                │
    ├──────────────────────────┼───────────────▶│
    │◀─────────────────────────┼────────────────┤
    │  Commands (Assign/Revoke)│                │
    │         └────────────────┘                │
    │                                           │
    │ Shutdown                                  │
    ├───────────────────────────────────────────▶
    │        DeregisterSidecar                  │
```

### Routing Table Structure

```
┌───────────────────────────────────────────────────────────┐
│                      Routing Table                        │
├───────────────────────────────────────────────────────────┤
│                                                           │
│  Pipeline: user-analytics                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    │
│  │ stage-1     │───▶│ stage-2     │───▶│ stage-3     │    │
│  │ filter      │    │ enrich      │    │ sink        │    │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘    │
│         │                  │                  │           │
│         ▼                  ▼                  ▼           │
│    localhost:50051   sidecar-b:9091   sidecar-c:9091      │
│      (Local)            (Remote)         (Remote)         │
│                                                           │
│  Pipeline: order-processing                               │
│  ┌─────────────┐    ┌─────────────┐                       │
│  │ stage-1     │───▶│ stage-2     │                       │
│  │ validate    │    │ sink        │                       │
│  └─────────────┘    └─────────────┘                       │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

## Data Flow

### Record Processing Pipeline

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│   Source Pod     │    │  Transform Pod   │    │    Sink Pod      │
│                  │    │                  │    │                  │
│  ┌────────────┐  │    │  ┌────────────┐  │    │  ┌────────────┐  │
│  │Kafka Source│  │    │  │  Filter    │  │    │  │  S3 Sink   │  │
│  └─────┬──────┘  │    │  └─────▲──────┘  │    │  └─────▲──────┘  │
│        │         │    │        │         │    │        │         │
│  PullRecords     │    │  ProcessBatch    │    │  WriteBatch      │
│        ▼         │    │        │         │    │        │         │
│  ┌────────────┐  │    │  ┌─────┴──────┐  │    │  ┌─────┴──────┐  │
│  │  Sidecar   │──┼───▶│  │  Sidecar   │──┼───▶│  │  Sidecar   │  │
│  └────────────┘  │    │  └────────────┘  │    │  └────────────┘  │
│                  │    │                  │    │                  │
└──────────────────┘    └──────────────────┘    └──────────────────┘
         ▲                      │                      │
         │        Ack           │         Ack          │
         └──────────────────────┴──────────────────────┘
```

### Backpressure Flow

```
  Source      Sidecar A     Sidecar B        Sink
    │             │             │             │
    │ PushRecords │             │             │
    ├────────────▶│             │             │
    │             │ReceiveRecords             │
    │             ├────────────▶│             │
    │             │             │ WriteBatch  │
    │             │             ├────────────▶│
    │             │             │             │
    │             │             │  Slow...    │ ◀── Sink overloaded
    │             │             │◀────────────┤
    │             │             │             │
    │             │ Buffer fills│             │
    │             │◀────────────┤             │
    │             │  Backpressure             │
    │ ReduceCredits             │             │
    │◀────────────┤             │             │
    │             │             │             │
    │ Slow down   │             │             │
    │ pulling     │             │             │
```

## Pipeline Optimization

The DSL optimizer merges shared pipeline prefixes to avoid redundant processing.

```
BEFORE (Unoptimized)                 AFTER (Optimized)
────────────────────                 ─────────────────

┌───────┐    ┌────────┐    ┌────┐   ┌───────┐    ┌────────┐    ┌────┐
│ Kafka │───▶│ Filter │───▶│ S3 │   │       │    │        │───▶│ S3 │
└───────┘    └────────┘    └────┘   │ Kafka │───▶│ Filter │    └────┘
                                    │       │    │(shared)│
┌───────┐    ┌────────┐    ┌────┐   └───────┘    └────────┘    ┌────┐
│ Kafka │───▶│ Filter │───▶│ CH │                          ───▶│ CH │
└───────┘    └────────┘    └────┘                              └────┘

2 sources, 2 filters                 1 source, 1 filter (fan-out)
```

## Kubernetes Integration

### Custom Resource Definitions

```
┌─────────────────────────────────────────────────────────────┐
│                           CRDs                              │
│  ┌────────────────┐  ┌────────────┐  ┌──────────────────┐   │
│  │ RouterCluster  │  │  Pipeline  │  │Source, Transform,│   │
│  │                │  │            │  │Sink              │   │
│  └────────────────┘  └────────────┘  └──────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Conveyor Operator                        │
│  ┌──────────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │Cluster Controller│  │Pipeline Ctrl │  │Resource Ctrl │   │
│  └──────────────────┘  └──────────────┘  └──────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
      ┌───────────────┐               ┌─────────────────┐
      │ StatefulSet   │               │Router Cluster   │
      │ Services      │               │(Submit pipeline)│
      │ ConfigMaps    │               └─────────────────┘
      └───────────────┘
```

### Operator Reconciliation

```
Kubernetes API        Operator         Router Cluster
      │                  │                   │
      │ Pipeline         │                   │
      │ created          │                   │
      ├─────────────────▶│                   │
      │                  │ Validate spec     │
      │                  │                   │
      │                  │ CreatePipeline    │
      │                  ├──────────────────▶│
      │                  │                   │
      │                  │◀──────────────────┤
      │                  │      Success      │
      │◀─────────────────┤                   │
      │ Update status:   │                   │
      │ Ready            │                   │
      │                  │                   │
      │ Pipeline         │                   │
      │ deleted          │                   │
      ├─────────────────▶│                   │
      │                  │ DeletePipeline    │
      │                  ├──────────────────▶│
      │                  │◀──────────────────┤
      │◀─────────────────┤                   │
      │ Remove finalizer │                   │
```

## Protocol Overview

### gRPC Services

```
Application Services                Infrastructure Services
────────────────────                ───────────────────────

SourceService                       ServiceRegistry
  - PullRecords                       - Register
  - Acknowledge                       - Heartbeat
                                      - Watch
TransformService
  - ProcessBatch                    SidecarCoordinator
  - GetCapabilities                   - RegisterSidecar
                                      - Heartbeat
SinkService
  - WriteBatch                      SidecarDataPlane
  - Flush                             - ReceiveRecords
                                      - PushRecords
LookupService
  - Lookup                          RaftService
  - BatchLookup                       - AppendEntries
                                      - RequestVote

                                    BackupService
                                      - CreateSnapshot
                                      - RestoreSnapshot
```

## Deployment Architecture

### Production Deployment

```
                        ┌────────────────┐
                        │ Load Balancer  │
                        └───────┬────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
        ▼                       ▼                       ▼
┌───────────────┐       ┌───────────────┐       ┌───────────────┐
│     AZ 1      │       │     AZ 2      │       │     AZ 3      │
│               │       │               │       │               │
│ ┌───────────┐ │       │ ┌───────────┐ │       │ ┌───────────┐ │
│ │  Router   │◀┼──────▶│ │  Router   │◀┼──────▶│ │  Router   │ │
│ │  Node 1   │ │       │ │  Node 2   │ │       │ │  Node 3   │ │
│ └───────────┘ │       │ └───────────┘ │       │ └───────────┘ │
│       │       │       │       │       │       │       │       │
│       ▼       │       │       ▼       │       │       ▼       │
│ ┌───────────┐ │       │ ┌───────────┐ │       │ ┌───────────┐ │
│ │ App Pods  │◀┼──────▶│ │ App Pods  │◀┼──────▶│ │ App Pods  │ │
│ │+ Sidecars │ │       │ │+ Sidecars │ │       │ │+ Sidecars │ │
│ └───────────┘ │       │ └───────────┘ │       │ └───────────┘ │
└───────────────┘       └───────────────┘       └───────────────┘
```

## Crate Descriptions

| Crate | Purpose |
|-------|---------|
| `conveyor-router` | Main router binary, orchestrates all components |
| `conveyor-sidecar` | Sidecar binary for pod deployment |
| `conveyor-operator` | Kubernetes operator for CRD management |
| `conveyorctl` | CLI tool for pipeline management |
| `conveyor-raft` | Raft consensus implementation |
| `conveyor-grpc` | gRPC server implementations |
| `conveyor-proto` | Protocol buffer definitions |
| `conveyor-registry` | Service registry logic |
| `conveyor-routing` | Record routing and watermarks |
| `conveyor-dsl` | Pipeline DSL parsing and optimization |
| `conveyor-buffer` | Record buffering with backpressure |
| `conveyor-dlq` | Dead letter queue handling |
| `conveyor-config` | Configuration management |
| `conveyor-metrics` | Prometheus metrics |
| `conveyor-graphql` | GraphQL API for dashboard |

## Design Principles

1. **Sidecar Pattern**: Routing logic is decoupled from application code. Services implement simple gRPC interfaces; the sidecar handles discovery and routing.

2. **Raft for Consistency**: Pipeline assignments must be consistent across the cluster. Raft ensures all nodes agree on state.

3. **Pull-Based with Backpressure**: Sources pull at a rate the pipeline can handle. Backpressure propagates upstream to prevent unbounded queuing.

4. **Shared Stage Optimization**: Pipelines from the same source with common prefixes share processing, reducing redundant work.

5. **Kubernetes Native**: Full integration with Kubernetes via CRDs and operators. Familiar patterns for k8s users.

6. **Observability Built-in**: Prometheus metrics, distributed tracing hooks, and a real-time dashboard.
