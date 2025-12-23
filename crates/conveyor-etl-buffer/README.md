# conveyor-buffer

Record buffering and backpressure management.

## Overview

This crate provides buffering capabilities for handling temporary record storage between pipeline stages, along with backpressure signaling to prevent memory exhaustion.

## Components

### BufferManager

Manages per-stage and per-source buffers:

```rust
use conveyor_buffer::{BufferManager, BufferedRecord};
use conveyor_config::BufferSettings;

let settings = BufferSettings {
    max_total_records: 100_000,
    max_per_stage: 10_000,
    max_per_source: 5_000,
    backpressure_threshold: 0.8,
};

let manager = BufferManager::new(settings);

// Buffer a record for a stage
let record = BufferedRecord {
    record: proto_record,
    source_id: "kafka-1".to_string(),
    pipeline_id: "analytics".to_string(),
    target_stage_id: "filter".to_string(),
    buffered_at: Instant::now(),
    retry_count: 0,
};

manager.buffer_for_stage("filter", record).await?;

// Get a batch for processing
let batch = manager.get_batch("filter", 100).await;

// Return failed records to buffer
manager.return_to_buffer("filter", failed_records).await;
```

### BackpressureController

Coordinates flow control across the system:

```rust
use conveyor_buffer::{BackpressureController, BackpressureSignal};

let controller = BackpressureController::new(threshold);

// Check if backpressure should be applied
if manager.should_backpressure("source-1").await {
    controller.signal(BackpressureSignal::SlowDown);
}

// Get available credits for a source
let credits = manager.available_credits("source-1").await;
```

## Buffer Hierarchy

```
┌────────────────────────────────────────────────┐
│                  BufferManager                 │
├────────────────────────────────────────────────┤
│  Global Limit: 100,000 records                 │
│  ┌─────────────────────────────────────────┐   │
│  │            Stage Buffers                │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  │   │
│  │  │ filter  │  │ enrich  │  │  sink   │  │   │
│  │  │ 10,000  │  │ 10,000  │  │ 10,000  │  │   │
│  │  └─────────┘  └─────────┘  └─────────┘  │   │
│  └─────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────┐   │
│  │            Source Buffers               │   │
│  │  ┌─────────┐  ┌─────────┐               │   │
│  │  │ kafka-1 │  │ kafka-2 │               │   │
│  │  │  5,000  │  │  5,000  │               │   │
│  │  └─────────┘  └─────────┘               │   │
│  └─────────────────────────────────────────┘   │
└────────────────────────────────────────────────┘
```

## BufferedRecord

```rust
pub struct BufferedRecord {
    pub record: Record,
    pub source_id: String,
    pub pipeline_id: String,
    pub target_stage_id: String,
    pub buffered_at: Instant,
    pub retry_count: u32,
}
```

## Backpressure Flow

```
1. Source pushes records
2. BufferManager accepts if under limits
3. If utilization > 80%:
   - Signal backpressure to source
   - Source reduces pull rate
4. As buffer drains, credits restored
5. Source resumes normal rate
```

## Methods

### BufferManager

| Method | Description |
|--------|-------------|
| `buffer_for_stage()` | Add record to stage buffer |
| `buffer_batch_for_stage()` | Add multiple records |
| `get_batch()` | Retrieve batch for processing |
| `return_to_buffer()` | Return failed records (increments retry) |
| `should_backpressure()` | Check if source should slow down |
| `available_credits()` | Get remaining capacity for source |
| `get_stage_utilization()` | Get buffer fill percentage |
| `get_global_utilization()` | Get total fill percentage |

## Exports

```rust
pub use manager::{BufferManager, BufferedRecord};
pub use backpressure::{BackpressureController, BackpressureSignal};
```
