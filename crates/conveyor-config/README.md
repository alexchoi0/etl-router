# conveyor-config

Configuration management for Conveyor.

## Overview

This crate provides typed configuration loading from YAML files. It defines all configuration structures used by the router and related components.

## Usage

```rust
use conveyor_config::Settings;

let settings = Settings::load("config/router.yaml")?;

println!("Election timeout: {}ms", settings.cluster.election_timeout_ms);
println!("Max buffer size: {}", settings.buffer.max_total_records);
```

## Configuration Structure

### Settings

Top-level configuration:

```rust
pub struct Settings {
    pub cluster: ClusterSettings,
    pub buffer: BufferSettings,
    pub grpc: GrpcSettings,
    pub metrics: MetricsSettings,
}
```

### ClusterSettings

Raft cluster configuration:

```rust
pub struct ClusterSettings {
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub snapshot_interval: u64,
}
```

### BufferSettings

Buffer and backpressure configuration:

```rust
pub struct BufferSettings {
    pub max_total_records: usize,
    pub max_per_stage: usize,
    pub max_per_source: usize,
    pub backpressure_threshold: f64,
}
```

### GrpcSettings

gRPC server configuration:

```rust
pub struct GrpcSettings {
    pub max_message_size: usize,
    pub keepalive_interval_secs: u64,
    pub keepalive_timeout_secs: u64,
}
```

### MetricsSettings

Prometheus metrics configuration:

```rust
pub struct MetricsSettings {
    pub enabled: bool,
    pub port: u16,
    pub path: String,
}
```

## Configuration File

```yaml
# config/router.yaml

cluster:
  election_timeout_ms: 300
  heartbeat_interval_ms: 100
  snapshot_interval: 10000

buffer:
  max_total_records: 100000
  max_per_stage: 10000
  max_per_source: 5000
  backpressure_threshold: 0.8

grpc:
  max_message_size: 16777216  # 16MB
  keepalive_interval_secs: 30
  keepalive_timeout_secs: 10

metrics:
  enabled: true
  port: 9090
  path: /metrics
```

## Defaults

If configuration file is not found or values are missing, sensible defaults are used:

| Setting | Default |
|---------|---------|
| `election_timeout_ms` | 300 |
| `heartbeat_interval_ms` | 100 |
| `snapshot_interval` | 10000 |
| `max_total_records` | 100000 |
| `max_per_stage` | 10000 |
| `max_per_source` | 5000 |
| `backpressure_threshold` | 0.8 |
| `max_message_size` | 16MB |
| `metrics.enabled` | true |
| `metrics.port` | 9090 |

## Environment Variable Override

Configuration values can be overridden via environment variables:

```bash
CONVEYOR_CLUSTER_ELECTION_TIMEOUT_MS=500 \
CONVEYOR_BUFFER_MAX_TOTAL_RECORDS=200000 \
cargo run -p conveyor-router
```

## Exports

```rust
pub use settings::{Settings, ClusterSettings, BufferSettings, GrpcSettings, MetricsSettings};
```
