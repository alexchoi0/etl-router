# conveyor-metrics

Prometheus metrics for observability.

## Overview

This crate provides metrics collection and export for monitoring Conveyor clusters. It uses the `metrics` crate for recording and exports to Prometheus format.

## Metrics

### Record Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `conveyor_router_records_received_total` | Counter | `source_id` | Records received from sources |
| `conveyor_router_records_routed_total` | Counter | `pipeline_id`, `stage_id` | Records routed through stages |
| `conveyor_router_records_delivered_total` | Counter | `sink_id` | Records delivered to sinks |

### Latency Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `conveyor_router_routing_latency_ms` | Histogram | `pipeline_id` | End-to-end routing latency |

### Buffer Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `conveyor_router_buffer_utilization` | Gauge | `stage_id` | Buffer fill percentage (0-1) |
| `conveyor_router_backpressure_events_total` | Counter | `source_id` | Backpressure signals sent |

### Service Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `conveyor_router_active_services` | Gauge | `service_type` | Count of registered services |

### Raft Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `conveyor_router_raft_is_leader` | Gauge | - | 1 if leader, 0 otherwise |
| `conveyor_router_raft_term` | Gauge | - | Current Raft term |

### Operational Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `conveyor_router_retry_events_total` | Counter | `stage_id` | Record retry attempts |
| `conveyor_router_checkpoints_saved_total` | Counter | `service_id` | Checkpoints persisted |
| `conveyor_router_group_rebalances_total` | Counter | `group_id` | Consumer group rebalances |

## Usage

### Recording Metrics

```rust
use conveyor_metrics::{
    record_records_received,
    record_records_routed,
    record_routing_latency,
    record_buffer_utilization,
    record_raft_state,
};

// Record incoming records
record_records_received("kafka-source-1", 100);

// Record routing through pipeline
record_records_routed("user-analytics", "filter-stage", 95);

// Record latency
record_routing_latency("user-analytics", 15.5);

// Record buffer state
record_buffer_utilization("filter-stage", 0.45);

// Record Raft state
record_raft_state(true, 42);  // is_leader, term
```

### Exporting Metrics

```rust
use conveyor_metrics::MetricsExporter;

let exporter = MetricsExporter::new(9090);
exporter.start().await?;

// Metrics available at http://localhost:9090/metrics
```

## Prometheus Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'conveyor-router'
    static_configs:
      - targets: ['conveyor-router-0:9090', 'conveyor-router-1:9090', 'conveyor-router-2:9090']
    scrape_interval: 15s
```

## Grafana Dashboard

Example queries:

```promql
# Records per second by source
rate(conveyor_router_records_received_total[1m])

# Routing latency p99
histogram_quantile(0.99, rate(conveyor_router_routing_latency_ms_bucket[5m]))

# Buffer utilization
conveyor_router_buffer_utilization

# Leader election events
changes(conveyor_router_raft_is_leader[1h])
```

## Exports

```rust
pub use prometheus::MetricsExporter;

pub fn record_records_received(source_id: &str, count: u64);
pub fn record_records_routed(pipeline_id: &str, stage_id: &str, count: u64);
pub fn record_records_delivered(sink_id: &str, count: u64);
pub fn record_routing_latency(pipeline_id: &str, latency_ms: f64);
pub fn record_buffer_utilization(stage_id: &str, utilization: f64);
pub fn record_active_services(service_type: &str, count: f64);
pub fn record_raft_state(is_leader: bool, term: u64);
pub fn record_backpressure_events(source_id: &str);
pub fn record_retry_events(stage_id: &str);
pub fn record_checkpoint_saved(service_id: &str);
pub fn record_group_rebalance(group_id: &str);
```
