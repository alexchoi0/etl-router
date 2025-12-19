use std::collections::HashMap;
use dashmap::DashMap;
use anyhow::Result;

use super::dag::{
    FanInConfig, FanOutConfig, FieldMapping, LookupConfig, LookupMissStrategy, MergeStrategy,
    Pipeline, Stage, StageType,
};
use super::watermark::WatermarkTracker;
use conveyor_proto::common::{Record, RecordBatch};

pub struct RoutingDecision {
    pub target_stage_id: String,
    pub records: Vec<Record>,
}

pub enum LookupResult {
    Found {
        data: HashMap<String, Vec<u8>>,
    },
    NotFound,
    Error {
        message: String,
    },
}

pub struct RoutingEngine {
    pipelines: DashMap<String, Pipeline>,
}

impl RoutingEngine {
    pub fn new() -> Self {
        Self {
            pipelines: DashMap::new(),
        }
    }

    pub async fn add_pipeline(&self, pipeline: Pipeline) {
        let pipeline_id = pipeline.id.clone();
        self.pipelines.insert(pipeline_id, pipeline);
    }

    pub async fn remove_pipeline(&self, pipeline_id: &str) {
        self.pipelines.remove(pipeline_id);
    }

    pub async fn get_pipeline(&self, pipeline_id: &str) -> Option<Pipeline> {
        self.pipelines.get(pipeline_id).map(|p| p.clone())
    }

    pub async fn list_pipelines(&self) -> Vec<Pipeline> {
        self.pipelines.iter().map(|r| r.clone()).collect()
    }

    pub async fn route_batch(
        &self,
        pipeline_id: &str,
        source_stage_id: &str,
        batch: RecordBatch,
    ) -> Result<Vec<RoutingDecision>> {
        let pipeline = self.pipelines
            .get(pipeline_id)
            .ok_or_else(|| anyhow::anyhow!("Pipeline not found: {}", pipeline_id))?;

        if !pipeline.enabled {
            return Err(anyhow::anyhow!("Pipeline is disabled: {}", pipeline_id));
        }

        let next_stage = pipeline
            .edges
            .iter()
            .find(|e| e.from_stage == source_stage_id)
            .map(|e| e.to_stage.clone());

        match next_stage {
            Some(target_stage_id) => Ok(vec![RoutingDecision {
                target_stage_id,
                records: batch.records,
            }]),
            None => Ok(vec![]),
        }
    }

    pub async fn find_pipelines_for_source(&self, source_service_name: &str) -> Vec<String> {
        self.pipelines
            .iter()
            .filter(|r| {
                let p = r.value();
                p.enabled && p.stages.values().any(|s| {
                    s.stage_type == StageType::Source
                        && s.service_selector.service_name.as_deref() == Some(source_service_name)
                })
            })
            .map(|r| r.key().clone())
            .collect()
    }

    pub async fn get_source_stages(&self, pipeline_id: &str) -> Vec<Stage> {
        self.pipelines
            .get(pipeline_id)
            .map(|p| p.get_source_stages().into_iter().cloned().collect())
            .unwrap_or_default()
    }

    pub async fn get_sink_stages(&self, pipeline_id: &str) -> Vec<Stage> {
        self.pipelines
            .get(pipeline_id)
            .map(|p| p.get_sink_stages().into_iter().cloned().collect())
            .unwrap_or_default()
    }

    pub async fn get_lookup_stages(&self, pipeline_id: &str) -> Vec<Stage> {
        self.pipelines
            .get(pipeline_id)
            .map(|p| p.get_lookup_stages().into_iter().cloned().collect())
            .unwrap_or_default()
    }

    pub async fn get_lookup_config(
        &self,
        pipeline_id: &str,
        stage_id: &str,
    ) -> Option<LookupConfig> {
        self.pipelines
            .get(pipeline_id)
            .and_then(|p| p.stages.get(stage_id).and_then(|s| s.lookup_config.clone()))
    }

    pub fn merge_lookup_result(
        &self,
        mut record: Record,
        lookup_result: LookupResult,
        config: &LookupConfig,
    ) -> Result<Option<Record>> {
        match lookup_result {
            LookupResult::Found { data } => {
                match config.merge_strategy {
                    MergeStrategy::Merge => {
                        for (key, value) in data {
                            let prefixed_key = match &config.output_prefix {
                                Some(prefix) => format!("{}{}", prefix, key),
                                None => key,
                            };
                            let value_str = String::from_utf8_lossy(&value).to_string();
                            record.metadata.insert(prefixed_key, value_str);
                        }
                        Ok(Some(record))
                    }
                    MergeStrategy::Nest => {
                        let field_name = config
                            .output_prefix
                            .clone()
                            .unwrap_or_else(|| "lookup".to_string());
                        let json_data: HashMap<String, String> = data
                            .into_iter()
                            .map(|(k, v)| (k, String::from_utf8_lossy(&v).to_string()))
                            .collect();
                        let json = serde_json::to_string(&json_data)
                            .map_err(|e| anyhow::anyhow!("Failed to serialize lookup data: {}", e))?;
                        record.metadata.insert(field_name, json);
                        Ok(Some(record))
                    }
                    MergeStrategy::Replace => {
                        let json_data: HashMap<String, String> = data
                            .into_iter()
                            .map(|(k, v)| (k, String::from_utf8_lossy(&v).to_string()))
                            .collect();
                        record.payload = serde_json::to_vec(&json_data)
                            .map_err(|e| anyhow::anyhow!("Failed to serialize lookup data: {}", e))?;
                        Ok(Some(record))
                    }
                }
            }
            LookupResult::NotFound => {
                match config.on_miss {
                    LookupMissStrategy::PassThrough => Ok(Some(record)),
                    LookupMissStrategy::Drop => Ok(None),
                    LookupMissStrategy::Error => {
                        Err(anyhow::anyhow!("Lookup miss: no matching record found"))
                    }
                }
            }
            LookupResult::Error { message } => {
                Err(anyhow::anyhow!("Lookup error: {}", message))
            }
        }
    }

    pub fn get_lookup_key_fields(&self, config: &LookupConfig) -> Vec<String> {
        config
            .key_fields
            .iter()
            .map(|k| k.lookup_key.clone())
            .collect()
    }

    pub async fn get_fan_in_stages(&self, pipeline_id: &str) -> Vec<Stage> {
        self.pipelines
            .get(pipeline_id)
            .map(|p| {
                p.stages
                    .values()
                    .filter(|s| s.stage_type == StageType::FanIn)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub async fn get_fan_out_stages(&self, pipeline_id: &str) -> Vec<Stage> {
        self.pipelines
            .get(pipeline_id)
            .map(|p| {
                p.stages
                    .values()
                    .filter(|s| s.stage_type == StageType::FanOut)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub async fn get_fan_in_config(
        &self,
        pipeline_id: &str,
        stage_id: &str,
    ) -> Option<FanInConfig> {
        self.pipelines
            .get(pipeline_id)
            .and_then(|p| p.stages.get(stage_id).and_then(|s| s.fan_in_config.clone()))
    }

    pub async fn get_fan_out_config(
        &self,
        pipeline_id: &str,
        stage_id: &str,
    ) -> Option<FanOutConfig> {
        self.pipelines
            .get(pipeline_id)
            .and_then(|p| p.stages.get(stage_id).and_then(|s| s.fan_out_config.clone()))
    }

    pub fn create_watermark_tracker_for_fan_in(
        &self,
        config: &FanInConfig,
    ) -> WatermarkTracker {
        let source_ids: Vec<String> = config.sources.iter().map(|s| s.id.clone()).collect();
        let allowed_lateness = config
            .watermark
            .as_ref()
            .and_then(|w| w.allowed_lateness)
            .unwrap_or(std::time::Duration::from_secs(0));

        let mut tracker = WatermarkTracker::new(source_ids, allowed_lateness);

        for source in &config.sources {
            if let Some(wm) = &source.watermark {
                if let Some(timeout) = wm.idle_timeout {
                    tracker.set_idle_timeout(&source.id, timeout);
                }
            }
        }

        tracker
    }

    pub fn apply_field_mappings(&self, record: Record, mappings: &[FieldMapping]) -> Record {
        if mappings.is_empty() {
            return record;
        }

        let mut new_record = record.clone();
        let original_payload: HashMap<String, serde_json::Value> =
            serde_json::from_slice(&record.payload).unwrap_or_default();

        let mut new_payload: HashMap<String, serde_json::Value> = HashMap::new();

        for mapping in mappings {
            let value = if let Some(literal) = &mapping.literal_value {
                serde_json::from_slice(literal).ok()
            } else if let Some(source_field) = &mapping.source_field {
                original_payload.get(source_field).cloned()
            } else if let Some(default) = &mapping.default_value {
                serde_json::from_slice(default).ok()
            } else {
                original_payload.get(&mapping.target_field).cloned()
            };

            if let Some(v) = value {
                new_payload.insert(mapping.target_field.clone(), v);
            } else if let Some(default) = &mapping.default_value {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(default) {
                    new_payload.insert(mapping.target_field.clone(), v);
                }
            }
        }

        new_record.payload = serde_json::to_vec(&new_payload).unwrap_or(record.payload);
        new_record
    }

    pub async fn route_fan_out(
        &self,
        pipeline_id: &str,
        fan_out_stage_id: &str,
        batch: RecordBatch,
    ) -> Result<Vec<RoutingDecision>> {
        let config = self
            .get_fan_out_config(pipeline_id, fan_out_stage_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Fan-out config not found for stage: {}", fan_out_stage_id))?;

        let mut decisions = Vec::with_capacity(config.sinks.len());

        for sink in &config.sinks {
            let target_id = format!("{}:{}", fan_out_stage_id, sink.id);
            let mapped_records: Vec<Record> = batch
                .records
                .iter()
                .map(|r| self.apply_field_mappings(r.clone(), &sink.field_mappings))
                .collect();

            decisions.push(RoutingDecision {
                target_stage_id: target_id,
                records: mapped_records,
            });
        }

        Ok(decisions)
    }

    pub fn get_fan_in_source_id<'a>(&self, full_stage_id: &'a str) -> Option<(&'a str, &'a str)> {
        full_stage_id.split_once(':')
    }
}

impl Default for RoutingEngine {
    fn default() -> Self {
        Self::new()
    }
}
