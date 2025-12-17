use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Condition, GrpcEndpoint};

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "etl.router",
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
#[serde(rename_all = "camelCase")]
pub struct SourceSpec {
    pub grpc: GrpcEndpoint,
    #[serde(default)]
    pub config: serde_json::Value,
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
