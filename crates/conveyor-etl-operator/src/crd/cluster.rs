use std::collections::HashMap;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::Condition;

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "conveyor.etl",
    version = "v1",
    kind = "ConveyorCluster",
    plural = "conveyorclusters",
    shortname = "cc",
    namespaced,
    status = "ConveyorClusterStatus",
    printcolumn = r#"{"name":"Replicas", "type":"integer", "jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Ready", "type":"integer", "jsonPath":".status.readyReplicas"}"#,
    printcolumn = r#"{"name":"Leader", "type":"integer", "jsonPath":".status.leaderId"}"#,
    printcolumn = r#"{"name":"Health", "type":"string", "jsonPath":".status.clusterHealth"}"#,
    printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ConveyorClusterSpec {
    #[schemars(range(min = 3))]
    #[serde(default = "default_replicas")]
    pub replicas: i32,
    pub image: String,
    #[serde(default)]
    pub image_pull_policy: Option<String>,
    #[serde(default)]
    pub image_pull_secrets: Vec<String>,
    #[serde(default)]
    pub resources: Option<ResourceConfig>,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub service: ServiceConfig,
    #[serde(default)]
    pub raft: RaftConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub node_selector: HashMap<String, String>,
    #[serde(default)]
    pub tolerations: Vec<Toleration>,
    #[serde(default)]
    pub affinity: Option<Affinity>,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceConfig {
    #[serde(default)]
    pub requests: Option<ResourceValues>,
    #[serde(default)]
    pub limits: Option<ResourceValues>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceValues {
    #[serde(default)]
    pub cpu: Option<String>,
    #[serde(default)]
    pub memory: Option<String>,
}

fn default_replicas() -> i32 {
    3
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct StorageConfig {
    #[serde(default = "default_storage_class")]
    pub storage_class: String,
    #[serde(default = "default_storage_size")]
    pub size: String,
}

fn default_storage_class() -> String {
    "standard".to_string()
}

fn default_storage_size() -> String {
    "10Gi".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceConfig {
    #[serde(default = "default_grpc_port")]
    pub grpc_port: i32,
    #[serde(default = "default_raft_port")]
    pub raft_port: i32,
    #[serde(default = "default_graphql_port")]
    pub graphql_port: i32,
    #[serde(default)]
    pub annotations: HashMap<String, String>,
    #[serde(default)]
    pub service_type: Option<String>,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            grpc_port: default_grpc_port(),
            raft_port: default_raft_port(),
            graphql_port: default_graphql_port(),
            annotations: HashMap::new(),
            service_type: None,
        }
    }
}

fn default_grpc_port() -> i32 {
    50051
}

fn default_raft_port() -> i32 {
    50052
}

fn default_graphql_port() -> i32 {
    8080
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RaftConfig {
    #[serde(default = "default_election_timeout_ms")]
    pub election_timeout_ms: u64,
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_ms: default_election_timeout_ms(),
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            snapshot_interval: default_snapshot_interval(),
        }
    }
}

fn default_election_timeout_ms() -> u64 {
    1000
}

fn default_heartbeat_interval_ms() -> u64 {
    100
}

fn default_snapshot_interval() -> u64 {
    10000
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetricsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_metrics_port")]
    pub port: i32,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_metrics_port(),
        }
    }
}

fn default_metrics_port() -> i32 {
    9090
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct Toleration {
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub operator: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub effect: Option<String>,
    #[serde(default)]
    pub toleration_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct Affinity {
    #[serde(default)]
    pub node_affinity: Option<NodeAffinity>,
    #[serde(default)]
    pub pod_affinity: Option<PodAffinity>,
    #[serde(default)]
    pub pod_anti_affinity: Option<PodAntiAffinity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct NodeAffinity {
    #[serde(default)]
    pub required_during_scheduling_ignored_during_execution: Option<NodeSelector>,
    #[serde(default)]
    pub preferred_during_scheduling_ignored_during_execution: Vec<PreferredSchedulingTerm>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelector {
    pub node_selector_terms: Vec<NodeSelectorTerm>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelectorTerm {
    #[serde(default)]
    pub match_expressions: Vec<NodeSelectorRequirement>,
    #[serde(default)]
    pub match_fields: Vec<NodeSelectorRequirement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelectorRequirement {
    pub key: String,
    pub operator: String,
    #[serde(default)]
    pub values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PreferredSchedulingTerm {
    pub weight: i32,
    pub preference: NodeSelectorTerm,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodAffinity {
    #[serde(default)]
    pub required_during_scheduling_ignored_during_execution: Vec<PodAffinityTerm>,
    #[serde(default)]
    pub preferred_during_scheduling_ignored_during_execution: Vec<WeightedPodAffinityTerm>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodAntiAffinity {
    #[serde(default)]
    pub required_during_scheduling_ignored_during_execution: Vec<PodAffinityTerm>,
    #[serde(default)]
    pub preferred_during_scheduling_ignored_during_execution: Vec<WeightedPodAffinityTerm>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodAffinityTerm {
    #[serde(default)]
    pub label_selector: Option<LabelSelector>,
    #[serde(default)]
    pub namespace_selector: Option<LabelSelector>,
    #[serde(default)]
    pub namespaces: Vec<String>,
    pub topology_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct WeightedPodAffinityTerm {
    pub weight: i32,
    pub pod_affinity_term: PodAffinityTerm,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelector {
    #[serde(default)]
    pub match_labels: HashMap<String, String>,
    #[serde(default)]
    pub match_expressions: Vec<LabelSelectorRequirement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelectorRequirement {
    pub key: String,
    pub operator: String,
    #[serde(default)]
    pub values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConveyorClusterStatus {
    #[serde(default)]
    pub observed_generation: Option<i64>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(default)]
    pub replicas: i32,
    #[serde(default)]
    pub ready_replicas: i32,
    #[serde(default)]
    pub leader_id: Option<u64>,
    #[serde(default)]
    pub term: Option<u64>,
    #[serde(default)]
    pub cluster_health: String,
    #[serde(default)]
    pub nodes: Vec<NodeStatusInfo>,
    #[serde(default)]
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatusInfo {
    pub node_id: u64,
    pub pod_name: String,
    pub address: String,
    pub role: String,
    pub healthy: bool,
}
