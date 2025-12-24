use std::collections::HashMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::commands::{RouterCommand, SerializableTimestamp, SidecarLocalService, SidecarStageAssignment};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RouterState {
    pub services: HashMap<String, ServiceState>,
    pub pipelines: HashMap<String, PipelineState>,
    pub checkpoints: CheckpointState,
    pub groups: HashMap<String, GroupState>,
    pub sidecars: HashMap<String, SidecarState>,
    pub service_locations: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceState {
    pub service_id: String,
    pub service_name: String,
    pub service_type: String,
    pub endpoint: String,
    pub labels: HashMap<String, String>,
    pub health: String,
    pub group_id: Option<String>,
    pub registered_at: u64,
    pub last_heartbeat: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineState {
    pub pipeline_id: String,
    pub name: String,
    pub config: Vec<u8>,
    pub enabled: bool,
    pub version: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckpointState {
    pub source_offsets: HashMap<String, HashMap<u32, u64>>,
    pub watermarks: HashMap<String, WatermarkState>,
    pub service_checkpoints: HashMap<String, ServiceCheckpointState>,
    pub group_offsets: HashMap<String, HashMap<String, HashMap<u32, u64>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkState {
    pub source_id: String,
    pub partition: u32,
    pub position: u64,
    pub event_time_secs: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceCheckpointState {
    pub service_id: String,
    pub checkpoint_id: String,
    pub data: Vec<u8>,
    pub source_offsets: HashMap<String, u64>,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupState {
    pub group_id: String,
    pub stage_id: String,
    pub members: Vec<String>,
    pub partition_assignments: HashMap<String, Vec<u32>>,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarState {
    pub sidecar_id: String,
    pub pod_name: String,
    pub namespace: String,
    pub endpoint: String,
    pub local_services: Vec<SidecarLocalService>,
    pub assigned_pipelines: HashMap<String, Vec<SidecarStageAssignment>>,
    pub registered_at: u64,
    pub last_heartbeat: u64,
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

impl RouterState {
    pub fn get_source_offsets(&self, source_id: &str) -> HashMap<u32, u64> {
        self.checkpoints
            .source_offsets
            .get(source_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn apply_command(&mut self, command: RouterCommand) -> Result<()> {
        match command {
            RouterCommand::Noop => {}

            RouterCommand::RegisterService { service_id, service_name, service_type, endpoint, labels, group_id } => {
                self.apply_register_service(service_id, service_name, service_type, endpoint, labels, group_id);
            }
            RouterCommand::DeregisterService { service_id } => {
                self.services.remove(&service_id);
            }
            RouterCommand::RenewLease { service_id } => {
                if let Some(service) = self.services.get_mut(&service_id) {
                    service.last_heartbeat = current_timestamp();
                }
            }
            RouterCommand::UpdateServiceHealth { service_id, health } => {
                if let Some(service) = self.services.get_mut(&service_id) {
                    service.health = health;
                }
            }

            RouterCommand::CreatePipeline { pipeline_id, name, config } => {
                self.apply_create_pipeline(pipeline_id, name, config);
            }
            RouterCommand::UpdatePipeline { pipeline_id, config } => {
                if let Some(pipeline) = self.pipelines.get_mut(&pipeline_id) {
                    pipeline.config = config;
                    pipeline.version += 1;
                }
            }
            RouterCommand::DeletePipeline { pipeline_id } => {
                self.pipelines.remove(&pipeline_id);
            }
            RouterCommand::EnablePipeline { pipeline_id } => {
                if let Some(pipeline) = self.pipelines.get_mut(&pipeline_id) {
                    pipeline.enabled = true;
                }
            }
            RouterCommand::DisablePipeline { pipeline_id } => {
                if let Some(pipeline) = self.pipelines.get_mut(&pipeline_id) {
                    pipeline.enabled = false;
                }
            }

            RouterCommand::CommitSourceOffset { source_id, partition, offset } => {
                self.apply_commit_source_offset(source_id, partition, offset);
            }
            RouterCommand::AdvanceWatermark { source_id, partition, position, event_time } => {
                self.apply_advance_watermark(source_id, partition, position, event_time);
            }
            RouterCommand::SaveServiceCheckpoint { service_id, checkpoint_id, data, source_offsets } => {
                self.apply_save_service_checkpoint(service_id, checkpoint_id, data, source_offsets);
            }
            RouterCommand::CommitGroupOffset { group_id, source_id, partition, offset } => {
                self.apply_commit_group_offset(group_id, source_id, partition, offset);
            }

            RouterCommand::JoinGroup { service_id, group_id, stage_id } => {
                self.apply_join_group(service_id, group_id, stage_id);
            }
            RouterCommand::LeaveGroup { service_id, group_id } => {
                self.apply_leave_group(service_id, group_id);
            }
            RouterCommand::AssignPartitions { group_id, assignments, generation } => {
                if let Some(group) = self.groups.get_mut(&group_id) {
                    group.partition_assignments = assignments;
                    group.generation = generation;
                }
            }

            RouterCommand::RegisterSidecar { sidecar_id, pod_name, namespace, endpoint, local_services } => {
                self.apply_register_sidecar(sidecar_id, pod_name, namespace, endpoint, local_services);
            }
            RouterCommand::DeregisterSidecar { sidecar_id } => {
                self.apply_deregister_sidecar(sidecar_id);
            }
            RouterCommand::UpdateSidecarHeartbeat { sidecar_id, timestamp } => {
                if let Some(sidecar) = self.sidecars.get_mut(&sidecar_id) {
                    sidecar.last_heartbeat = timestamp;
                }
            }
            RouterCommand::AssignPipelineToSidecar { pipeline_id, sidecar_id, stage_assignments } => {
                if let Some(sidecar) = self.sidecars.get_mut(&sidecar_id) {
                    sidecar.assigned_pipelines.insert(pipeline_id, stage_assignments);
                }
            }
            RouterCommand::RevokePipelineFromSidecar { pipeline_id, sidecar_id } => {
                if let Some(sidecar) = self.sidecars.get_mut(&sidecar_id) {
                    sidecar.assigned_pipelines.remove(&pipeline_id);
                }
            }
        }

        Ok(())
    }

    fn apply_register_service(
        &mut self,
        service_id: String,
        service_name: String,
        service_type: String,
        endpoint: String,
        labels: HashMap<String, String>,
        group_id: Option<String>,
    ) {
        let now = current_timestamp();
        self.services.insert(
            service_id.clone(),
            ServiceState {
                service_id,
                service_name,
                service_type,
                endpoint,
                labels,
                health: "healthy".to_string(),
                group_id,
                registered_at: now,
                last_heartbeat: now,
            },
        );
    }

    fn apply_create_pipeline(&mut self, pipeline_id: String, name: String, config: Vec<u8>) {
        self.pipelines.insert(
            pipeline_id.clone(),
            PipelineState {
                pipeline_id,
                name,
                config,
                enabled: false,
                version: 1,
            },
        );
    }

    fn apply_commit_source_offset(&mut self, source_id: String, partition: u32, offset: u64) {
        self.checkpoints
            .source_offsets
            .entry(source_id)
            .or_default()
            .insert(partition, offset);
    }

    fn apply_advance_watermark(
        &mut self,
        source_id: String,
        partition: u32,
        position: u64,
        event_time: Option<SerializableTimestamp>,
    ) {
        self.checkpoints.watermarks.insert(
            format!("{}:{}", source_id, partition),
            WatermarkState {
                source_id,
                partition,
                position,
                event_time_secs: event_time.as_ref().map(|t| t.seconds).unwrap_or(0),
            },
        );
    }

    fn apply_save_service_checkpoint(
        &mut self,
        service_id: String,
        checkpoint_id: String,
        data: Vec<u8>,
        source_offsets: HashMap<String, u64>,
    ) {
        self.checkpoints.service_checkpoints.insert(
            service_id.clone(),
            ServiceCheckpointState {
                service_id,
                checkpoint_id,
                data,
                source_offsets,
                created_at: current_timestamp(),
            },
        );
    }

    fn apply_commit_group_offset(
        &mut self,
        group_id: String,
        source_id: String,
        partition: u32,
        offset: u64,
    ) {
        self.checkpoints
            .group_offsets
            .entry(group_id)
            .or_default()
            .entry(source_id)
            .or_default()
            .insert(partition, offset);
    }

    fn apply_join_group(&mut self, service_id: String, group_id: String, stage_id: String) {
        let group = self.groups.entry(group_id.clone()).or_insert_with(|| {
            GroupState {
                group_id,
                stage_id,
                members: Vec::new(),
                partition_assignments: HashMap::new(),
                generation: 0,
            }
        });

        if !group.members.contains(&service_id) {
            group.members.push(service_id);
            group.generation += 1;
        }
    }

    fn apply_leave_group(&mut self, service_id: String, group_id: String) {
        if let Some(group) = self.groups.get_mut(&group_id) {
            group.members.retain(|m| m != &service_id);
            group.partition_assignments.remove(&service_id);
            group.generation += 1;
        }
    }

    fn apply_register_sidecar(
        &mut self,
        sidecar_id: String,
        pod_name: String,
        namespace: String,
        endpoint: String,
        local_services: Vec<SidecarLocalService>,
    ) {
        let now = current_timestamp();

        for svc in &local_services {
            self.service_locations
                .insert(svc.service_name.clone(), sidecar_id.clone());
        }

        self.sidecars.insert(
            sidecar_id.clone(),
            SidecarState {
                sidecar_id,
                pod_name,
                namespace,
                endpoint,
                local_services,
                assigned_pipelines: HashMap::new(),
                registered_at: now,
                last_heartbeat: now,
            },
        );
    }

    fn apply_deregister_sidecar(&mut self, sidecar_id: String) {
        if let Some(sidecar) = self.sidecars.remove(&sidecar_id) {
            for svc in &sidecar.local_services {
                self.service_locations.remove(&svc.service_name);
            }
        }
    }
}
