use std::collections::HashMap;
use std::ops::Deref;
use conveyor_dsl::PipelineSpec;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::Condition;

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "conveyor.dev",
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
pub struct PipelineCrdSpec {
    #[serde(flatten)]
    pub inner: PipelineSpec,
}

impl Deref for PipelineCrdSpec {
    type Target = PipelineSpec;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
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
