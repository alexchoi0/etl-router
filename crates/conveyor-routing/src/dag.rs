use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use super::matcher::Condition;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub id: String,
    pub name: String,
    pub description: String,
    pub stages: HashMap<String, Stage>,
    pub edges: Vec<Edge>,
    pub enabled: bool,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stage {
    pub id: String,
    pub name: String,
    pub stage_type: StageType,
    pub service_selector: ServiceSelector,
    pub parallelism: u32,
    pub lookup_config: Option<LookupConfig>,
    pub fan_in_config: Option<FanInConfig>,
    pub fan_out_config: Option<FanOutConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupConfig {
    pub key_fields: Vec<LookupKeyMapping>,
    pub output_prefix: Option<String>,
    pub merge_strategy: MergeStrategy,
    pub on_miss: LookupMissStrategy,
    pub timeout_ms: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupKeyMapping {
    pub record_field: String,
    pub lookup_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum MergeStrategy {
    #[default]
    Merge,
    Nest,
    Replace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum LookupMissStrategy {
    #[default]
    PassThrough,
    Drop,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanInConfig {
    pub sources: Vec<FanInSource>,
    pub watermark: Option<FanInWatermark>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanInSource {
    pub id: String,
    pub name: String,
    pub service_selector: ServiceSelector,
    pub watermark: Option<SourceWatermark>,
    pub field_mappings: Vec<FieldMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceWatermark {
    pub event_time_field: String,
    pub idle_timeout: Option<std::time::Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanInWatermark {
    pub allowed_lateness: Option<std::time::Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanOutConfig {
    pub sinks: Vec<FanOutSink>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanOutSink {
    pub id: String,
    pub name: String,
    pub service_selector: ServiceSelector,
    pub field_mappings: Vec<FieldMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    pub source_field: Option<String>,
    pub target_field: String,
    pub cast_type: Option<FieldCastType>,
    pub default_value: Option<Vec<u8>>,
    pub literal_value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldCastType {
    String,
    Int,
    Int64,
    Float,
    Float64,
    Bool,
    Timestamp,
    Date,
    Json,
    Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageType {
    Source,
    Transform,
    Lookup,
    FanIn,
    FanOut,
    Sink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceSelector {
    pub service_name: Option<String>,
    pub group_id: Option<String>,
    pub labels: HashMap<String, String>,
    pub load_balance: LoadBalanceStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoadBalanceStrategy {
    RoundRobin,
    LeastConnections,
    WeightedRandom,
    ConsistentHash,
}

impl Default for LoadBalanceStrategy {
    fn default() -> Self {
        Self::RoundRobin
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub from_stage: String,
    pub to_stage: String,
    pub condition: Option<Condition>,
}

impl Pipeline {
    pub fn new(id: String, name: String) -> Self {
        Self {
            id,
            name,
            description: String::new(),
            stages: HashMap::new(),
            edges: Vec::new(),
            enabled: false,
            metadata: HashMap::new(),
        }
    }

    pub fn add_stage(&mut self, stage: Stage) {
        self.stages.insert(stage.id.clone(), stage);
    }

    pub fn add_edge(&mut self, from: &str, to: &str, condition: Option<Condition>) {
        self.edges.push(Edge {
            from_stage: from.to_string(),
            to_stage: to.to_string(),
            condition,
        });
    }

    pub fn get_source_stages(&self) -> Vec<&Stage> {
        self.stages
            .values()
            .filter(|s| s.stage_type == StageType::Source)
            .collect()
    }

    pub fn get_sink_stages(&self) -> Vec<&Stage> {
        self.stages
            .values()
            .filter(|s| s.stage_type == StageType::Sink)
            .collect()
    }

    pub fn get_lookup_stages(&self) -> Vec<&Stage> {
        self.stages
            .values()
            .filter(|s| s.stage_type == StageType::Lookup)
            .collect()
    }

    pub fn get_downstream_stages(&self, stage_id: &str) -> Vec<&Stage> {
        self.edges
            .iter()
            .filter(|e| e.from_stage == stage_id)
            .filter_map(|e| self.stages.get(&e.to_stage))
            .collect()
    }

    pub fn get_upstream_stages(&self, stage_id: &str) -> Vec<&Stage> {
        self.edges
            .iter()
            .filter(|e| e.to_stage == stage_id)
            .filter_map(|e| self.stages.get(&e.from_stage))
            .collect()
    }
}
