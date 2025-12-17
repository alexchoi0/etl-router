use std::collections::HashMap;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::Condition;

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "etl.router",
    version = "v1",
    kind = "Pipeline",
    plural = "pipelines",
    namespaced,
    status = "PipelineStatus",
    printcolumn = r#"{"name":"Source", "type":"string", "jsonPath":".spec.source"}"#,
    printcolumn = r#"{"name":"Sink", "type":"string", "jsonPath":".spec.sink"}"#,
    printcolumn = r#"{"name":"Enabled", "type":"boolean", "jsonPath":".spec.enabled"}"#,
    printcolumn = r#"{"name":"Ready", "type":"string", "jsonPath":".status.conditions[?(@.type=='Ready')].status"}"#,
    printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".metadata.creationTimestamp"}"#
)]
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PipelineStatus {
    #[serde(default)]
    pub observed_generation: Option<i64>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(default)]
    pub pipeline_id: Option<String>,
    #[serde(default)]
    pub version: Option<u64>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub records_processed: Option<u64>,
    #[serde(default)]
    pub stage_statuses: HashMap<String, StageStatusInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StageStatusInfo {
    pub records_processed: u64,
    pub records_buffered: u64,
    pub errors: u64,
    pub avg_latency_ms: f64,
}
