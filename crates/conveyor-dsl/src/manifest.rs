use std::collections::HashMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Manifest<T: JsonSchema> {
    pub api_version: String,
    pub kind: ResourceKind,
    pub metadata: Metadata,
    pub spec: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ResourceKind {
    Source,
    Transform,
    Sink,
    Pipeline,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Metadata {
    pub name: String,
    #[serde(default = "default_namespace")]
    pub namespace: String,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default)]
    pub annotations: HashMap<String, String>,
}

fn default_namespace() -> String {
    "default".to_string()
}

impl Metadata {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace: default_namespace(),
            labels: HashMap::new(),
            annotations: HashMap::new(),
        }
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GrpcEndpoint {
    pub endpoint: String,
    #[serde(default)]
    pub proto: Option<String>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    #[serde(default)]
    pub ca_cert: Option<String>,
    #[serde(default)]
    pub client_cert: Option<String>,
    #[serde(default)]
    pub client_key: Option<String>,
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SourceSpec {
    pub grpc: GrpcEndpoint,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TransformSpec {
    pub grpc: GrpcEndpoint,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SinkSpec {
    pub grpc: GrpcEndpoint,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineSpec {
    pub source: String,
    #[serde(default)]
    pub steps: Vec<String>,
    pub sink: String,
    #[serde(default)]
    pub dlq: Option<DlqConfig>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DlqConfig {
    pub sink: String,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default = "default_max_retry_backoff_ms")]
    pub max_retry_backoff_ms: u64,
}

fn default_max_retries() -> u32 {
    3
}

fn default_retry_backoff_ms() -> u64 {
    100
}

fn default_max_retry_backoff_ms() -> u64 {
    30_000
}

pub type SourceManifest = Manifest<SourceSpec>;
pub type TransformManifest = Manifest<TransformSpec>;
pub type SinkManifest = Manifest<SinkSpec>;
pub type PipelineManifest = Manifest<PipelineSpec>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AnyManifest {
    Source(SourceManifest),
    Transform(TransformManifest),
    Sink(SinkManifest),
    Pipeline(PipelineManifest),
}

impl AnyManifest {
    pub fn kind(&self) -> ResourceKind {
        match self {
            AnyManifest::Source(m) => m.kind,
            AnyManifest::Transform(m) => m.kind,
            AnyManifest::Sink(m) => m.kind,
            AnyManifest::Pipeline(m) => m.kind,
        }
    }

    pub fn metadata(&self) -> &Metadata {
        match self {
            AnyManifest::Source(m) => &m.metadata,
            AnyManifest::Transform(m) => &m.metadata,
            AnyManifest::Sink(m) => &m.metadata,
            AnyManifest::Pipeline(m) => &m.metadata,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_source_manifest() {
        let yaml = r#"
apiVersion: etl.router/v1
kind: Source
metadata:
  name: kafka-user-events
  namespace: default
  labels:
    team: data-platform
spec:
  grpc:
    endpoint: kafka-source-svc:50051
    proto: etl.source.SourceService
  config:
    brokers:
      - kafka:9092
    topic: user-events
    consumerGroup: etl-router
"#;

        let manifest: SourceManifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(manifest.api_version, "etl.router/v1");
        assert_eq!(manifest.kind, ResourceKind::Source);
        assert_eq!(manifest.metadata.name, "kafka-user-events");
        assert_eq!(manifest.metadata.namespace, "default");
        assert_eq!(manifest.metadata.labels.get("team"), Some(&"data-platform".to_string()));
        assert_eq!(manifest.spec.grpc.endpoint, "kafka-source-svc:50051");
    }

    #[test]
    fn test_parse_transform_manifest() {
        let yaml = r#"
apiVersion: etl.router/v1
kind: Transform
metadata:
  name: filter-active-users
  namespace: default
spec:
  grpc:
    endpoint: filter-svc:50051
    proto: etl.transform.TransformService
  config:
    type: filter
    condition:
      metadataEquals:
        key: status
        value: active
"#;

        let manifest: TransformManifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(manifest.kind, ResourceKind::Transform);
        assert_eq!(manifest.metadata.name, "filter-active-users");
        assert_eq!(manifest.spec.grpc.endpoint, "filter-svc:50051");
    }

    #[test]
    fn test_parse_sink_manifest() {
        let yaml = r#"
apiVersion: etl.router/v1
kind: Sink
metadata:
  name: s3-archive
  namespace: production
spec:
  grpc:
    endpoint: s3-sink-svc:50051
    proto: etl.sink.SinkService
  config:
    bucket: user-events-archive
    prefix: v1/
    format: parquet
"#;

        let manifest: SinkManifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(manifest.kind, ResourceKind::Sink);
        assert_eq!(manifest.metadata.name, "s3-archive");
        assert_eq!(manifest.metadata.namespace, "production");
    }

    #[test]
    fn test_parse_pipeline_manifest() {
        let yaml = r#"
apiVersion: etl.router/v1
kind: Pipeline
metadata:
  name: user-analytics
  namespace: default
  labels:
    team: analytics
spec:
  source: kafka-user-events
  steps:
    - filter-active-users
    - validate-user-schema
    - mask-pii
  sink: s3-archive
  dlq:
    sink: error-handler
    maxRetries: 5
"#;

        let manifest: PipelineManifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(manifest.kind, ResourceKind::Pipeline);
        assert_eq!(manifest.metadata.name, "user-analytics");
        assert_eq!(manifest.spec.source, "kafka-user-events");
        assert_eq!(manifest.spec.steps, vec!["filter-active-users", "validate-user-schema", "mask-pii"]);
        assert_eq!(manifest.spec.sink, "s3-archive");

        let dlq = manifest.spec.dlq.unwrap();
        assert_eq!(dlq.sink, "error-handler");
        assert_eq!(dlq.max_retries, 5);
    }

    #[test]
    fn test_parse_pipeline_without_steps() {
        let yaml = r#"
apiVersion: etl.router/v1
kind: Pipeline
metadata:
  name: simple-passthrough
spec:
  source: input
  sink: output
"#;

        let manifest: PipelineManifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(manifest.spec.steps.len(), 0);
        assert!(manifest.spec.dlq.is_none());
    }

    #[test]
    fn test_parse_pipeline_dlq_defaults() {
        let yaml = r#"
apiVersion: etl.router/v1
kind: Pipeline
metadata:
  name: with-dlq
spec:
  source: input
  sink: output
  dlq:
    sink: error-sink
"#;

        let manifest: PipelineManifest = serde_yaml::from_str(yaml).unwrap();
        let dlq = manifest.spec.dlq.unwrap();
        assert_eq!(dlq.sink, "error-sink");
        assert_eq!(dlq.max_retries, 3);
        assert_eq!(dlq.retry_backoff_ms, 100);
        assert_eq!(dlq.max_retry_backoff_ms, 30_000);
    }
}
