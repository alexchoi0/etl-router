use std::path::Path;
use std::fs;

use crate::error::{DslError, Result};
use crate::types::{PipelineManifest, API_VERSION, KIND_PIPELINE};

pub fn parse_yaml(yaml: &str) -> Result<PipelineManifest> {
    let manifest: PipelineManifest = serde_yaml::from_str(yaml)?;
    validate_manifest(&manifest)?;
    Ok(manifest)
}

pub fn parse_file<P: AsRef<Path>>(path: P) -> Result<PipelineManifest> {
    let content = fs::read_to_string(path)?;
    parse_yaml(&content)
}

fn validate_manifest(manifest: &PipelineManifest) -> Result<()> {
    if manifest.api_version != API_VERSION {
        return Err(DslError::UnsupportedVersion(manifest.api_version.clone()));
    }
    if manifest.kind != KIND_PIPELINE {
        return Err(DslError::ValidationError(format!(
            "Expected kind '{}', got '{}'",
            KIND_PIPELINE, manifest.kind
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_pipeline() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: simple-pipeline
  labels:
    env: production
spec:
  description: A basic source to sink pipeline
  enabled: true
  stages:
    - id: kafka-source
      name: Kafka Source
      type: source
      service:
        name: kafka-connector
    - id: json-sink
      name: JSON Sink
      type: sink
      service:
        name: s3-writer
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

        let manifest = result.unwrap();
        assert_eq!(manifest.api_version, "etl.dev/v1");
        assert_eq!(manifest.kind, "Pipeline");
        assert_eq!(manifest.metadata.name, "simple-pipeline");
        assert_eq!(manifest.spec.stages.len(), 2);
        assert!(manifest.spec.enabled);
    }

    #[test]
    fn test_parse_pipeline_with_transform() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: etl-pipeline
spec:
  stages:
    - id: source
      name: Data Source
      type: source
      service:
        name: data-source
    - id: transform
      name: Data Transform
      type: transform
      service:
        group: transform-workers
        load_balance: least_connections
      parallelism: 4
    - id: sink
      name: Data Sink
      type: sink
      service:
        name: data-sink
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

        let manifest = result.unwrap();
        let transform = manifest.spec.stages.iter().find(|s| s.id == "transform").unwrap();
        assert_eq!(transform.parallelism, 4);
    }

    #[test]
    fn test_unsupported_version() {
        let yaml = r#"
apiVersion: etl.dev/v2
kind: Pipeline
metadata:
  name: test
spec:
  stages: []
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DslError::UnsupportedVersion(_)));
    }

    #[test]
    fn test_invalid_kind() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Deployment
metadata:
  name: test
spec:
  stages: []
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DslError::ValidationError(_)));
    }

    #[test]
    fn test_parse_kafka_source_config() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: kafka-pipeline
spec:
  stages:
    - id: kafka-source
      name: Kafka Source
      type: source
      service:
        name: kafka-connector
      config:
        source_type: kafka
        brokers:
          - localhost:9092
          - localhost:9093
        topic: events
        consumer_group: my-group
        auto_offset_reset: earliest
        security:
          protocol: SASL_SSL
          sasl_mechanism: PLAIN
          sasl_username: user
          sasl_password_env: KAFKA_PASSWORD
    - id: sink
      name: Sink
      type: sink
      service:
        name: sink
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

        let manifest = result.unwrap();
        let source = &manifest.spec.stages[0];
        assert!(source.config.is_some());
    }

    #[test]
    fn test_parse_filter_transform_config() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: filter-pipeline
spec:
  stages:
    - id: source
      name: Source
      type: source
      service:
        name: src
    - id: filter
      name: Filter Transform
      type: transform
      config:
        transform_type: filter
        condition:
          metadata_equals:
            key: status
            value: active
        negate: false
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    }

    #[test]
    fn test_parse_aggregate_transform_config() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: agg-pipeline
spec:
  stages:
    - id: source
      name: Source
      type: source
      service:
        name: src
    - id: aggregate
      name: Aggregate
      type: transform
      config:
        transform_type: aggregate
        group_by:
          - user_id
          - region
        window:
          tumbling:
            minutes: 5
        aggregations:
          - field: amount
            function: sum
            output_field: total_amount
          - field: count
            function: count
        emit: on_window_close
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    }

    #[test]
    fn test_parse_dedupe_transform_config() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: dedupe-pipeline
spec:
  stages:
    - id: source
      name: Source
      type: source
      service:
        name: src
    - id: dedupe
      name: Dedupe
      type: transform
      config:
        transform_type: dedupe
        key_fields:
          - event_id
        window:
          tumbling:
            hours: 1
        keep: first
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    }

    #[test]
    fn test_parse_grpc_sink_config() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: grpc-pipeline
spec:
  stages:
    - id: source
      name: Source
      type: source
      service:
        name: src
    - id: sink
      name: gRPC Sink
      type: sink
      config:
        sink_type: grpc
        endpoint: http://localhost:50051
        timeout_ms: 5000
        retry:
          max_retries: 5
          initial_backoff_ms: 100
          max_backoff_ms: 5000
          backoff_multiplier: 2.0
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    }

    #[test]
    fn test_parse_mask_transform_config() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: mask-pipeline
spec:
  stages:
    - id: source
      name: Source
      type: source
      service:
        name: src
    - id: mask
      name: Mask PII
      type: transform
      config:
        transform_type: mask
        fields:
          - field: ssn
            strategy: redact
          - field: email
            strategy: partial
            preserve_length: true
          - field: credit_card
            strategy: hash
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    }

    #[test]
    fn test_parse_join_transform_config() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: join-pipeline
spec:
  stages:
    - id: orders
      name: Orders Source
      type: source
      service:
        name: orders-src
    - id: join
      name: Join Orders with Users
      type: transform
      config:
        transform_type: join
        join_type: left
        right_stream: users-stream
        on:
          left_key: user_id
          right_key: id
        window:
          tumbling:
            minutes: 10
        output_fields:
          left_prefix: order_
          right_prefix: user_
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    }

    #[test]
    fn test_parse_with_namespace() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: my-pipeline
  namespace: production
  labels:
    team: data-engineering
  annotations:
    description: Main data pipeline
spec:
  stages:
    - id: source
      name: Source
      type: source
      service:
        name: src
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

        let manifest = result.unwrap();
        assert_eq!(manifest.metadata.namespace, Some("production".to_string()));
        assert_eq!(manifest.metadata.labels.get("team"), Some(&"data-engineering".to_string()));
    }
}
