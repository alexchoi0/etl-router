use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

use super::matcher::Condition;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineValidationError {
    CycleDetected { path: Vec<String> },
    DisconnectedStage { stage_id: String },
    UnreachableFromSource { stage_id: String },
    CannotReachSink { stage_id: String },
    MissingStage { stage_id: String },
    NoSourceStages,
    NoSinkStages,
}

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

    pub fn validate(&self) -> Result<(), Vec<PipelineValidationError>> {
        let mut errors = Vec::new();

        for edge in &self.edges {
            if !self.stages.contains_key(&edge.from_stage) {
                errors.push(PipelineValidationError::MissingStage {
                    stage_id: edge.from_stage.clone(),
                });
            }
            if !self.stages.contains_key(&edge.to_stage) {
                errors.push(PipelineValidationError::MissingStage {
                    stage_id: edge.to_stage.clone(),
                });
            }
        }

        if let Some(cycle_path) = self.detect_cycle() {
            errors.push(PipelineValidationError::CycleDetected { path: cycle_path });
        }

        let source_stages = self.get_source_stages();
        if source_stages.is_empty() {
            errors.push(PipelineValidationError::NoSourceStages);
        }

        let sink_stages = self.get_sink_stages();
        if sink_stages.is_empty() {
            errors.push(PipelineValidationError::NoSinkStages);
        }

        let reachable_from_sources = self.get_reachable_from_sources();
        let can_reach_sinks = self.get_stages_that_can_reach_sinks();

        for stage_id in self.stages.keys() {
            if !reachable_from_sources.contains(stage_id) {
                let stage = self.stages.get(stage_id).unwrap();
                if stage.stage_type != StageType::Source {
                    errors.push(PipelineValidationError::UnreachableFromSource {
                        stage_id: stage_id.clone(),
                    });
                }
            }

            if !can_reach_sinks.contains(stage_id) {
                let stage = self.stages.get(stage_id).unwrap();
                if stage.stage_type != StageType::Sink {
                    errors.push(PipelineValidationError::CannotReachSink {
                        stage_id: stage_id.clone(),
                    });
                }
            }

            let has_incoming = self.edges.iter().any(|e| e.to_stage == *stage_id);
            let has_outgoing = self.edges.iter().any(|e| e.from_stage == *stage_id);
            let stage = self.stages.get(stage_id).unwrap();

            let is_disconnected = match stage.stage_type {
                StageType::Source => !has_outgoing,
                StageType::Sink => !has_incoming,
                _ => !has_incoming && !has_outgoing,
            };

            if is_disconnected {
                errors.push(PipelineValidationError::DisconnectedStage {
                    stage_id: stage_id.clone(),
                });
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn detect_cycle(&self) -> Option<Vec<String>> {
        let adjacency: HashMap<&str, Vec<&str>> = {
            let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
            for edge in &self.edges {
                adj.entry(edge.from_stage.as_str())
                    .or_default()
                    .push(edge.to_stage.as_str());
            }
            adj
        };

        let mut visited: HashSet<&str> = HashSet::new();
        let mut rec_stack: HashSet<&str> = HashSet::new();
        let mut path: Vec<&str> = Vec::new();

        for stage_id in self.stages.keys() {
            if !visited.contains(stage_id.as_str()) {
                if let Some(cycle) = self.dfs_detect_cycle(
                    stage_id.as_str(),
                    &adjacency,
                    &mut visited,
                    &mut rec_stack,
                    &mut path,
                ) {
                    return Some(cycle);
                }
            }
        }

        None
    }

    fn dfs_detect_cycle<'a>(
        &self,
        node: &'a str,
        adjacency: &HashMap<&str, Vec<&'a str>>,
        visited: &mut HashSet<&'a str>,
        rec_stack: &mut HashSet<&'a str>,
        path: &mut Vec<&'a str>,
    ) -> Option<Vec<String>> {
        visited.insert(node);
        rec_stack.insert(node);
        path.push(node);

        if let Some(neighbors) = adjacency.get(node) {
            for &neighbor in neighbors {
                if !visited.contains(neighbor) {
                    if let Some(cycle) =
                        self.dfs_detect_cycle(neighbor, adjacency, visited, rec_stack, path)
                    {
                        return Some(cycle);
                    }
                } else if rec_stack.contains(neighbor) {
                    let cycle_start = path.iter().position(|&n| n == neighbor).unwrap();
                    let mut cycle: Vec<String> =
                        path[cycle_start..].iter().map(|s| s.to_string()).collect();
                    cycle.push(neighbor.to_string());
                    return Some(cycle);
                }
            }
        }

        path.pop();
        rec_stack.remove(node);
        None
    }

    fn get_reachable_from_sources(&self) -> HashSet<String> {
        let mut reachable = HashSet::new();
        let adjacency: HashMap<&str, Vec<&str>> = {
            let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
            for edge in &self.edges {
                adj.entry(edge.from_stage.as_str())
                    .or_default()
                    .push(edge.to_stage.as_str());
            }
            adj
        };

        for stage in self.get_source_stages() {
            self.dfs_reachable(&stage.id, &adjacency, &mut reachable);
        }

        reachable
    }

    fn dfs_reachable(
        &self,
        node: &str,
        adjacency: &HashMap<&str, Vec<&str>>,
        reachable: &mut HashSet<String>,
    ) {
        if reachable.contains(node) {
            return;
        }
        reachable.insert(node.to_string());

        if let Some(neighbors) = adjacency.get(node) {
            for neighbor in neighbors {
                self.dfs_reachable(neighbor, adjacency, reachable);
            }
        }
    }

    fn get_stages_that_can_reach_sinks(&self) -> HashSet<String> {
        let mut can_reach = HashSet::new();
        let reverse_adjacency: HashMap<&str, Vec<&str>> = {
            let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
            for edge in &self.edges {
                adj.entry(edge.to_stage.as_str())
                    .or_default()
                    .push(edge.from_stage.as_str());
            }
            adj
        };

        for stage in self.get_sink_stages() {
            self.dfs_reachable(&stage.id, &reverse_adjacency, &mut can_reach);
        }

        can_reach
    }

    pub fn has_cycle(&self) -> bool {
        self.detect_cycle().is_some()
    }

    pub fn get_disconnected_stages(&self) -> Vec<String> {
        match self.validate() {
            Ok(()) => Vec::new(),
            Err(errors) => errors
                .into_iter()
                .filter_map(|e| match e {
                    PipelineValidationError::DisconnectedStage { stage_id } => Some(stage_id),
                    _ => None,
                })
                .collect(),
        }
    }
}
