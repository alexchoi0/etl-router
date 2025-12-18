use std::ops::Deref;
use conveyor_dsl::SourceSpec;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::Condition;

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "conveyor.dev",
    version = "v1",
    kind = "Source",
    plural = "sources",
    namespaced,
    status = "SourceStatus",
    printcolumn = r#"{"name":"Endpoint", "type":"string", "jsonPath":".spec.grpc.endpoint"}"#,
    printcolumn = r#"{"name":"Health", "type":"string", "jsonPath":".status.health"}"#,
    printcolumn = r#"{"name":"Ready", "type":"string", "jsonPath":".status.conditions[?(@.type=='Ready')].status"}"#,
    printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".metadata.creationTimestamp"}"#
)]
pub struct SourceCrdSpec {
    #[serde(flatten)]
    pub inner: SourceSpec,
}

impl Deref for SourceCrdSpec {
    type Target = SourceSpec;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct SourceStatus {
    #[serde(default)]
    pub observed_generation: Option<i64>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(default)]
    pub service_id: Option<String>,
    #[serde(default)]
    pub registered_at: Option<String>,
    #[serde(default)]
    pub health: Option<String>,
    #[serde(default)]
    pub last_heartbeat: Option<String>,
}
