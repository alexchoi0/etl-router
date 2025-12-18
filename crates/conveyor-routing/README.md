# conveyor-routing

Pipeline DAG routing engine and watermark tracking.

## Overview

This crate defines the pipeline data structures and provides the routing engine that determines how records flow through pipeline stages. It also handles watermark tracking for event-time processing.

## Components

### Pipeline

Defines the structure of a data pipeline:

```rust
use conveyor_routing::{Pipeline, Stage, StageType};

let pipeline = Pipeline {
    id: "user-analytics".to_string(),
    name: "User Analytics Pipeline".to_string(),
    source_name: "kafka-users".to_string(),
    stages: vec![
        Stage {
            id: "filter".to_string(),
            stage_type: StageType::Transform,
            service_name: "filter-active".to_string(),
            // ...
        },
        Stage {
            id: "sink".to_string(),
            stage_type: StageType::Sink,
            service_name: "clickhouse".to_string(),
            // ...
        },
    ],
    edges: vec![
        Edge { from: "source".to_string(), to: "filter".to_string() },
        Edge { from: "filter".to_string(), to: "sink".to_string() },
    ],
    // ...
};
```

### RoutingEngine

Determines next stage for records:

```rust
use conveyor_routing::RoutingEngine;

let engine = RoutingEngine::new();

// Get next stage for a record
let next = engine.route(&pipeline, &current_stage, &record)?;

// Lookup enrichment
let enriched = engine.lookup(&pipeline, &stage, &record).await?;
```

### WatermarkTracker

Tracks event-time progress across sources:

```rust
use conveyor_routing::{WatermarkTracker, Watermark};

let mut tracker = WatermarkTracker::new(
    allowed_lateness,
    idle_timeout,
);

// Update watermark for a source
tracker.update("source-1", Watermark::new(timestamp));

// Get combined watermark (minimum across all sources)
let watermark = tracker.combined_watermark();

// Check if a record is late
let is_late = tracker.is_late(&record);
```

## Pipeline Features

### Fan-Out

Route records to multiple sinks:

```rust
let fan_out = FanOutConfig {
    sinks: vec![
        FanOutSink { sink_name: "s3".to_string(), condition: None },
        FanOutSink { sink_name: "kafka".to_string(), condition: Some(condition) },
    ],
};
```

### Fan-In

Merge records from multiple sources:

```rust
let fan_in = FanInConfig {
    sources: vec![
        FanInSource { source_name: "kafka-1".to_string(), ... },
        FanInSource { source_name: "kafka-2".to_string(), ... },
    ],
    merge_strategy: MergeStrategy::EventTime,
    watermark: FanInWatermark::Min,
};
```

### Lookup Enrichment

Enrich records with external data:

```rust
let lookup = LookupConfig {
    lookup_service: "user-db".to_string(),
    key_mappings: vec![
        LookupKeyMapping {
            record_field: "user_id".to_string(),
            lookup_field: "id".to_string(),
        },
    ],
    result_field: "user_data".to_string(),
    miss_strategy: LookupMissStrategy::Skip,
};
```

### Conditional Routing

Route based on record content:

```rust
use conveyor_routing::Condition;

let condition = Condition::And(vec![
    Condition::MetadataEquals {
        key: "type".to_string(),
        value: "user_event".to_string(),
    },
    Condition::MetadataExists("user_id".to_string()),
]);

if condition.evaluate(&record) {
    // Route to this stage
}
```

## Watermark Semantics

```
┌─────────────────────────────────────────────────────┐
│                  Time Progress                      │
│                                                     │
│  Source 1:  ─────●─────●─────●───────────►          │
│                  t1    t2    t3                     │
│                                                     │
│  Source 2:  ─────────●───────●─────●────►           │
│                      t1      t2    t3               │
│                                                     │
│  Combined:  ─────●───●───────●─────●────►           │
│             (min of all sources)                    │
│                                                     │
│  Late records: timestamp < watermark - lateness     │
└─────────────────────────────────────────────────────┘
```

## Exports

```rust
pub use engine::{LookupResult, RoutingEngine};
pub use dag::{
    Edge, FanInConfig, FanOutConfig, LoadBalanceStrategy,
    LookupConfig, Pipeline, Stage, StageType, ...
};
pub use matcher::Condition;
pub use watermark::{Watermark, WatermarkTracker};
```
