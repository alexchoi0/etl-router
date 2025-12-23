# conveyor-dsl

Pipeline DSL parsing, validation, and optimization.

## Overview

This crate handles the YAML manifest format for defining pipeline resources. It parses manifests, validates references, converts to internal types, and optimizes pipeline DAGs.

## Features

- Parse YAML manifests for Sources, Transforms, Sinks, Pipelines
- Validate pipeline references exist
- Convert manifests to `conveyor-routing` types
- Optimize DAGs by merging shared prefixes

## Usage

### Parse and Load Pipeline

```rust
use conveyor_dsl::{load_pipeline, parse_pipeline};

// From file
let pipeline = load_pipeline("pipeline.yaml")?;

// From string
let yaml = r#"
apiVersion: conveyor.etl/v1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  source: kafka-events
  steps:
    - filter
    - enrich
  sink: s3-archive
"#;
let pipeline = parse_pipeline(yaml)?;
```

### Registry

Store and query resources:

```rust
use conveyor_dsl::Registry;

let mut registry = Registry::new();

// Apply manifests
registry.apply(source_manifest)?;
registry.apply(transform_manifest)?;
registry.apply(pipeline_manifest)?;

// Query
let source = registry.source_spec("default", "kafka-events");
let pipelines = registry.list_pipelines("default");

// Validate pipeline references
registry.validate_pipeline("default", "my-pipeline")?;
```

### Optimizer

Merge shared pipeline prefixes:

```rust
use conveyor_dsl::{Optimizer, Registry};

let registry = Registry::new();
// ... apply manifests ...

let optimizer = Optimizer::new(&registry, "default");
let dag = optimizer.optimize()?;

// dag.stages contains shared stages
// dag.edges contains optimized routing
```

## Manifest Format

### Source

```yaml
apiVersion: conveyor.etl/v1
kind: Source
metadata:
  name: kafka-users
  namespace: default
  labels:
    team: data
spec:
  grpc:
    endpoint: kafka-source-svc:50051
    tls:
      enabled: true
      ca_cert: /certs/ca.pem
```

### Transform

```yaml
apiVersion: conveyor.etl/v1
kind: Transform
metadata:
  name: filter-active
spec:
  grpc:
    endpoint: filter-svc:50051
  config:
    field: status
    value: active
```

### Sink

```yaml
apiVersion: conveyor.etl/v1
kind: Sink
metadata:
  name: s3-archive
spec:
  grpc:
    endpoint: s3-sink-svc:50051
  config:
    bucket: my-bucket
    prefix: events/
```

### Pipeline

```yaml
apiVersion: conveyor.etl/v1
kind: Pipeline
metadata:
  name: user-analytics
spec:
  source: kafka-users
  steps:
    - filter-active
    - enrich-geo
    - mask-pii
  sink: clickhouse-analytics
  dlq:
    enabled: true
    maxRetries: 3
    backoffMs: 1000
```

## DAG Optimization

Before optimization:
```
Pipeline A: Kafka → Filter → Enrich → S3
Pipeline B: Kafka → Filter → Enrich → ClickHouse
```

After optimization:
```
                              ┌→ S3
Kafka → Filter → Enrich (shared)
                              └→ ClickHouse
```

## Validation

The validator checks:

- Source exists for pipeline
- All transform steps exist
- Sink exists for pipeline
- No circular references
- DLQ configuration valid

```rust
use conveyor_dsl::validate;

validate(&manifest)?;  // Returns DslError on failure
```

## Exports

```rust
pub use types::*;
pub use error::{DslError, Result};
pub use parser::{parse_yaml, parse_file};
pub use validation::{validate, validate_backup, validate_restore};
pub use convert::convert;
pub use manifest::{Manifest, Metadata, ResourceKind, ...};
pub use registry::Registry;
pub use optimizer::{Optimizer, OptimizedDag, ...};
```
