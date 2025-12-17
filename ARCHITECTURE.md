# ETL Router Architecture

This document describes the architecture of the ETL Router system, a distributed data routing platform for building ETL pipelines.

## System Overview

```mermaid
flowchart TB
    subgraph k8s["Kubernetes Cluster"]
        subgraph control["Control Plane"]
            op["ETL Operator"]
            subgraph raft["Router Cluster (Raft)"]
                r1["Router (Leader)"]
                r2["Router (Follower)"]
                r3["Router (Follower)"]
                r1 <--> r2
                r2 <--> r3
                r1 <--> r3
            end
        end

        subgraph data["Data Plane"]
            subgraph pod1["Pod A"]
                s1["Sidecar"]
                src["Source Service"]
                s1 --- src
            end

            subgraph pod2["Pod B"]
                s2["Sidecar"]
                tx["Transform Service"]
                s2 --- tx
            end

            subgraph pod3["Pod C"]
                s3["Sidecar"]
                sink["Sink Service"]
                s3 --- sink
            end
        end

        op --> raft
        r1 --> s1
        r1 --> s2
        r1 --> s3
        s1 <--> s2
        s2 <--> s3
    end

    cli["etlctl CLI"] --> raft
    web["Web Dashboard"] --> raft
```

## Core Components

### Component Hierarchy

```mermaid
flowchart LR
    subgraph crates["Rust Crates"]
        router["etl-router<br/>(main binary)"]
        sidecar["etl-sidecar<br/>(pod sidecar)"]
        operator["etl-operator<br/>(k8s controller)"]
        ctl["etlctl<br/>(CLI)"]

        subgraph libs["Libraries"]
            raft["etl-raft"]
            grpc["etl-grpc"]
            proto["etl-proto"]
            registry["etl-registry"]
            routing["etl-routing"]
            dsl["etl-dsl"]
            buffer["etl-buffer"]
            dlq["etl-dlq"]
            config["etl-config"]
            metrics["etl-metrics"]
            graphql["etl-graphql"]
        end
    end

    router --> raft
    router --> grpc
    router --> registry
    router --> graphql

    sidecar --> grpc
    sidecar --> buffer
    sidecar --> routing

    operator --> grpc

    ctl --> dsl
    ctl --> grpc

    grpc --> proto
```

## Router Cluster

The router cluster is the central control plane, providing service registry, pipeline management, and sidecar coordination. It uses Raft consensus for high availability.

### Raft State Machine

```mermaid
stateDiagram-v2
    [*] --> Follower
    Follower --> Candidate: Election timeout
    Candidate --> Leader: Wins election
    Candidate --> Follower: Discovers leader
    Leader --> Follower: Higher term seen
    Candidate --> Candidate: Election timeout
```

### Raft Log Replication

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant F1 as Follower 1
    participant F2 as Follower 2

    C->>L: Propose(Command)
    L->>L: Append to log
    par Replicate
        L->>F1: AppendEntries
        L->>F2: AppendEntries
    end
    F1-->>L: Success
    F2-->>L: Success
    L->>L: Commit (majority)
    L->>L: Apply to state machine
    L-->>C: Response
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

```mermaid
flowchart TB
    subgraph sidecar["Sidecar Process"]
        discovery["Discovery Module"]
        cluster["Cluster Client"]
        dataplane["Data Plane Server"]
        routing["Routing Engine"]
        buffer["Buffer Manager"]

        discovery --> cluster
        cluster --> routing
        dataplane --> routing
        routing --> buffer
    end

    subgraph local["Local Services (same pod)"]
        svc1["gRPC Service A"]
        svc2["gRPC Service B"]
    end

    subgraph remote["Remote"]
        router["Router Cluster"]
        other["Other Sidecars"]
    end

    discovery -.->|gRPC reflection| local
    cluster <-->|Heartbeat| router
    dataplane <-->|Records| other
    routing -->|ProcessBatch| local
```

### Sidecar Lifecycle

```mermaid
sequenceDiagram
    participant S as Sidecar
    participant L as Local Services
    participant R as Router (Leader)

    Note over S: Startup
    S->>L: Scan ports (gRPC reflection)
    S->>S: Build local service registry

    S->>R: RegisterSidecar(id, services)
    R->>R: Raft commit
    R-->>S: InitialAssignments

    S->>S: Populate routing table

    loop Every 5s
        S->>R: Heartbeat(health, load)
        R-->>S: Commands (Assign/Revoke)
        S->>S: Update routing table
    end

    Note over S: Shutdown
    S->>R: DeregisterSidecar
```

### Routing Table Structure

```mermaid
flowchart LR
    subgraph rt["Routing Table"]
        subgraph p1["Pipeline: user-analytics"]
            s1["stage-1: filter"]
            s2["stage-2: enrich"]
            s3["stage-3: sink"]
            s1 --> s2 --> s3
        end

        subgraph p2["Pipeline: order-processing"]
            s4["stage-1: validate"]
            s5["stage-2: sink"]
            s4 --> s5
        end
    end

    s1 -.->|Local| local1["localhost:50051"]
    s2 -.->|Remote| remote1["sidecar-b:9091"]
    s3 -.->|Remote| remote2["sidecar-c:9091"]
```

## Data Flow

### Record Processing Pipeline

```mermaid
flowchart LR
    subgraph source["Source Pod"]
        src["Kafka Source"]
        sc1["Sidecar"]
        src -->|PullRecords| sc1
    end

    subgraph transform["Transform Pod"]
        sc2["Sidecar"]
        tx["Filter Transform"]
        sc2 -->|ProcessBatch| tx
        tx -->|Results| sc2
    end

    subgraph sink["Sink Pod"]
        sc3["Sidecar"]
        sk["S3 Sink"]
        sc3 -->|WriteBatch| sk
    end

    sc1 -->|ReceiveRecords| sc2
    sc2 -->|ReceiveRecords| sc3
    sc3 -.->|Ack| sc2
    sc2 -.->|Ack| sc1
    sc1 -.->|Acknowledge| src
```

### Backpressure Flow

```mermaid
sequenceDiagram
    participant Src as Source
    participant S1 as Sidecar A
    participant S2 as Sidecar B
    participant Sink as Sink

    Src->>S1: PushRecords(batch)
    S1->>S2: ReceiveRecords(batch)
    S2->>Sink: WriteBatch(batch)

    Note over Sink: Slow/overloaded
    Sink-->>S2: SlowResponse

    S2->>S2: Buffer fills up
    S2-->>S1: Backpressure signal
    S1-->>Src: ReduceCredits

    Note over Src: Slows down pulling
```

## Pipeline Optimization

The DSL optimizer merges shared pipeline prefixes to avoid redundant processing.

### Before Optimization

```mermaid
flowchart LR
    subgraph before["Unoptimized"]
        k1["Kafka"] --> f1["Filter"] --> s1["S3"]
        k2["Kafka"] --> f2["Filter"] --> ch["ClickHouse"]
    end
```

### After Optimization

```mermaid
flowchart LR
    subgraph after["Optimized (Shared Prefix)"]
        k["Kafka"] --> f["Filter (shared)"]
        f --> s3["S3"]
        f --> ch["ClickHouse"]
    end
```

## Kubernetes Integration

### Custom Resource Definitions

```mermaid
flowchart TB
    subgraph crds["CRDs"]
        cluster["EtlRouterCluster"]
        pipeline["EtlPipeline"]
        source["EtlSource"]
        transform["EtlTransform"]
        sink["EtlSink"]
    end

    subgraph operator["ETL Operator"]
        cc["Cluster Controller"]
        pc["Pipeline Controller"]
        rc["Resource Controllers"]
    end

    subgraph managed["Managed Resources"]
        sts["StatefulSet"]
        svc["Services"]
        cm["ConfigMaps"]
    end

    cluster --> cc
    pipeline --> pc
    source --> rc
    transform --> rc
    sink --> rc

    cc --> sts
    cc --> svc
    cc --> cm

    pc -->|Submit| router["Router Cluster"]
```

### Operator Reconciliation

```mermaid
sequenceDiagram
    participant K8s as Kubernetes API
    participant Op as Operator
    participant R as Router Cluster

    K8s->>Op: EtlPipeline created
    Op->>Op: Validate spec
    Op->>R: CreatePipeline(spec)
    R-->>Op: Success
    Op->>K8s: Update status: Ready

    K8s->>Op: EtlPipeline deleted
    Op->>R: DeletePipeline(id)
    R-->>Op: Success
    Op->>K8s: Remove finalizer
```

## Protocol Overview

### gRPC Services

```mermaid
flowchart TB
    subgraph services["gRPC Services"]
        subgraph app["Application Services"]
            source["SourceService<br/>- PullRecords<br/>- Acknowledge"]
            transform["TransformService<br/>- ProcessBatch<br/>- GetCapabilities"]
            sink["SinkService<br/>- WriteBatch<br/>- Flush"]
            lookup["LookupService<br/>- Lookup<br/>- BatchLookup"]
        end

        subgraph infra["Infrastructure Services"]
            registry["ServiceRegistry<br/>- Register<br/>- Heartbeat<br/>- Watch"]
            coordinator["SidecarCoordinator<br/>- RegisterSidecar<br/>- Heartbeat"]
            dataplane["SidecarDataPlane<br/>- ReceiveRecords<br/>- PushRecords"]
            raft_svc["RaftService<br/>- AppendEntries<br/>- RequestVote"]
            backup["BackupService<br/>- CreateSnapshot<br/>- RestoreSnapshot"]
        end
    end
```

## Deployment Architecture

### Production Deployment

```mermaid
flowchart TB
    subgraph az1["Availability Zone 1"]
        r1["Router Node 1"]
        pods1["App Pods + Sidecars"]
    end

    subgraph az2["Availability Zone 2"]
        r2["Router Node 2"]
        pods2["App Pods + Sidecars"]
    end

    subgraph az3["Availability Zone 3"]
        r3["Router Node 3"]
        pods3["App Pods + Sidecars"]
    end

    lb["Load Balancer"]

    lb --> r1
    lb --> r2
    lb --> r3

    r1 <--> r2
    r2 <--> r3
    r1 <--> r3

    r1 --> pods1
    r2 --> pods2
    r3 --> pods3

    pods1 <--> pods2
    pods2 <--> pods3
```

## Crate Descriptions

| Crate | Purpose |
|-------|---------|
| `etl-router` | Main router binary, orchestrates all components |
| `etl-sidecar` | Sidecar binary for pod deployment |
| `etl-operator` | Kubernetes operator for CRD management |
| `etlctl` | CLI tool for pipeline management |
| `etl-raft` | Raft consensus implementation |
| `etl-grpc` | gRPC server implementations |
| `etl-proto` | Protocol buffer definitions |
| `etl-registry` | Service registry logic |
| `etl-routing` | Record routing and watermarks |
| `etl-dsl` | Pipeline DSL parsing and optimization |
| `etl-buffer` | Record buffering with backpressure |
| `etl-dlq` | Dead letter queue handling |
| `etl-config` | Configuration management |
| `etl-metrics` | Prometheus metrics |
| `etl-graphql` | GraphQL API for dashboard |

## Design Principles

1. **Sidecar Pattern**: Routing logic is decoupled from application code. Services implement simple gRPC interfaces; the sidecar handles discovery and routing.

2. **Raft for Consistency**: Pipeline assignments must be consistent across the cluster. Raft ensures all nodes agree on state.

3. **Pull-Based with Backpressure**: Sources pull at a rate the pipeline can handle. Backpressure propagates upstream to prevent unbounded queuing.

4. **Shared Stage Optimization**: Pipelines from the same source with common prefixes share processing, reducing redundant work.

5. **Kubernetes Native**: Full integration with Kubernetes via CRDs and operators. Familiar patterns for k8s users.

6. **Observability Built-in**: Prometheus metrics, distributed tracing hooks, and a real-time dashboard.
