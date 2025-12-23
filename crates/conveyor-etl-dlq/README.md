# conveyor-dlq

Dead Letter Queue handling for failed records.

## Overview

This crate manages records that fail processing. It captures error context, applies retry policies, and routes permanently failed records to configured error sinks.

## Components

### DlqManager

Central manager for dead letter handling:

```rust
use conveyor_dlq::{DlqManager, DlqPolicy, RetryPolicy};

let policy = DlqPolicy {
    enabled: true,
    max_retries: 3,
    retry_policy: RetryPolicy::ExponentialBackoff {
        initial_delay_ms: 100,
        max_delay_ms: 10000,
        multiplier: 2.0,
    },
    error_sink: Some("s3://errors/".to_string()),
};

let manager = DlqManager::new(policy);

// Register a record failure
manager.record_failure(
    record,
    ErrorContext {
        error_code: ErrorCode::TransformFailed,
        message: "Schema validation failed".to_string(),
        stage_id: "validator".to_string(),
        timestamp: Utc::now(),
        stack_trace: None,
    },
).await?;

// Get records ready for retry
let batch = manager.get_retry_batch("validator", 100).await;

// Get stats
let stats = manager.stats().await;
```

### DeadLetterRecord

Full context for a failed record:

```rust
pub struct DeadLetterRecord {
    pub original_record: Record,
    pub error_context: ErrorContext,
    pub pipeline_id: String,
    pub stage_id: String,
    pub first_failure: DateTime<Utc>,
    pub last_failure: DateTime<Utc>,
    pub retry_count: u32,
    pub next_retry: Option<DateTime<Utc>>,
}
```

### ErrorContext

```rust
pub struct ErrorContext {
    pub error_code: ErrorCode,
    pub message: String,
    pub stage_id: String,
    pub timestamp: DateTime<Utc>,
    pub stack_trace: Option<String>,
}

pub enum ErrorCode {
    TransformFailed,
    SinkFailed,
    ValidationFailed,
    Timeout,
    ResourceExhausted,
    Internal,
}
```

### RetryPolicy

```rust
pub enum RetryPolicy {
    // Fixed delay between retries
    Fixed { delay_ms: u64 },

    // Exponential backoff
    ExponentialBackoff {
        initial_delay_ms: u64,
        max_delay_ms: u64,
        multiplier: f64,
    },

    // No retries
    None,
}
```

## Retry Flow

```
┌─────────────────────────────────────────────────┐
│                   Record Flow                   │
├─────────────────────────────────────────────────┤
│                                                 │
│  Record ──► Stage ──► Success ──► Next Stage    │
│               │                                 │
│               ▼                                 │
│            Failure                              │
│               │                                 │
│               ▼                                 │
│         DlqManager                              │
│               │                                 │
│      ┌────────┴────────┐                        │
│      ▼                 ▼                        │
│  retry_count      retry_count                   │
│    < max            >= max                      │
│      │                 │                        │
│      ▼                 ▼                        │
│  Schedule          Send to                      │
│   Retry           Error Sink                    │
│      │                                          │
│      ▼                                          │
│  Back to Stage                                  │
│                                                 │
└─────────────────────────────────────────────────┘
```

## Backoff Calculation

```rust
// ExponentialBackoff example:
// initial_delay_ms = 100
// multiplier = 2.0
// max_delay_ms = 10000

// Retry 1: 100ms
// Retry 2: 200ms
// Retry 3: 400ms
// Retry 4: 800ms
// Retry 5: 1600ms
// ...capped at 10000ms
```

## DlqPolicy Configuration

```yaml
dlq:
  enabled: true
  maxRetries: 3
  retryPolicy:
    type: exponentialBackoff
    initialDelayMs: 100
    maxDelayMs: 10000
    multiplier: 2.0
  errorSink: s3://my-bucket/dlq/
```

## Exports

```rust
pub use record::{DeadLetterRecord, ErrorContext, ErrorCode};
pub use manager::DlqManager;
pub use policy::{DlqPolicy, RetryPolicy};
pub use error::{DlqError, Result};
```
