# conveyor-registry

Service registry and group coordination.

## Overview

This crate manages the registration, discovery, and coordination of data pipeline services. It tracks service health, handles consumer groups, and provides load balancing across service instances.

## Components

### ServiceRegistry

Tracks all registered services with their endpoints and health status.

```rust
use conveyor_registry::{ServiceRegistry, ServiceType, ServiceHealth};

let registry = ServiceRegistry::new(raft_node);

// Register a service
let lease = registry.register(
    "service-123".to_string(),
    "kafka-source".to_string(),
    ServiceType::Source,
    "10.0.0.1:50051".to_string(),
    labels,
    Some("group-1".to_string()),
).await?;

// Query services
let sources = registry.get_services_by_name("kafka-source").await;
let healthy = registry.get_healthy_services_by_labels(&labels).await;

// Heartbeat to maintain lease
registry.heartbeat("service-123").await?;

// Cleanup expired leases
let expired = registry.cleanup_expired().await;
```

### GroupCoordinator

Manages consumer groups for coordinated consumption:

```rust
use conveyor_registry::{GroupCoordinator, ServiceGroup};

let coordinator = GroupCoordinator::new(raft_node);

// Join a group
let assignment = coordinator.join_group(
    "group-1",
    "member-1",
    "kafka-source",
    vec!["topic-1"],
).await?;

// Get partition assignments
let partitions = assignment.assigned_partitions;

// Leave group (triggers rebalance)
coordinator.leave_group("group-1", "member-1").await?;
```

### LoadBalancer

Distributes requests across service instances:

```rust
use conveyor_registry::LoadBalancer;

let balancer = LoadBalancer::new();

// Round-robin selection
let endpoint = balancer.select(&services);

// Weighted selection
let endpoint = balancer.select_weighted(&services);
```

## Data Structures

### RegisteredService

```rust
pub struct RegisteredService {
    pub service_id: String,
    pub service_name: String,
    pub service_type: ServiceType,
    pub endpoint: String,
    pub labels: HashMap<String, String>,
    pub health: ServiceHealth,
    pub group_id: Option<String>,
    pub registered_at: Option<Instant>,
    pub last_heartbeat: Option<Instant>,
    pub lease_duration: Duration,
}
```

### ServiceType

```rust
pub enum ServiceType {
    Source,
    Transform,
    Sink,
}
```

### ServiceHealth

```rust
pub enum ServiceHealth {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}
```

### PartitionAssignment

```rust
pub struct PartitionAssignment {
    pub group_id: String,
    pub member_id: String,
    pub generation: u64,
    pub assigned_partitions: Vec<i32>,
}
```

## Lease Management

Services must send periodic heartbeats to maintain their registration:

```
1. Service registers → receives lease duration (e.g., 30s)
2. Service sends heartbeat every 10s
3. If no heartbeat for 30s → service marked unhealthy
4. Cleanup task removes expired registrations
```

## Group Rebalancing

When group membership changes:

```
1. Member joins/leaves group
2. Coordinator increments generation
3. All members receive new assignments
4. Partitions redistributed evenly
```

## Exports

```rust
pub use service_registry::{ServiceRegistry, RegisteredService, ServiceHealth, ServiceType};
pub use group_coordinator::{GroupCoordinator, ServiceGroup, GroupMember, PartitionAssignment};
pub use load_balancer::LoadBalancer;
```
