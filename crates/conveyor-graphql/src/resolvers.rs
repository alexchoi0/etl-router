use std::sync::Arc;
use async_graphql::{Context, Object, Result, Error};
use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

use conveyor_raft::{RaftNode, RouterCommand, SerializableTimestamp};

use super::types::*;
use super::error_buffer::ErrorBuffer;

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn cluster_status(&self, ctx: &Context<'_>) -> Result<ClusterStatus> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        let is_leader = node.is_leader().await;
        let leader_id = node.leader_id().await;
        let term = node.current_term().await;

        let role = if is_leader {
            NodeRole::Leader
        } else if leader_id.is_some() {
            NodeRole::Follower
        } else {
            NodeRole::Candidate
        };

        Ok(ClusterStatus {
            node_id: node.id,
            role,
            current_term: term,
            leader_id,
            commit_index: 0,
            last_applied: 0,
            log_length: 0,
        })
    }

    async fn services(&self, ctx: &Context<'_>) -> Result<Vec<Service>> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        let state = node.get_state();
        let services: Vec<Service> = state.services.values().map(|s| {
            Service {
                service_id: s.service_id.clone(),
                service_name: s.service_name.clone(),
                service_type: match s.service_type.as_str() {
                    "source" => ServiceType::Source,
                    "transform" => ServiceType::Transform,
                    "sink" => ServiceType::Sink,
                    _ => ServiceType::Source,
                },
                endpoint: s.endpoint.clone(),
                labels: s.labels.iter().map(|(k, v)| Label {
                    key: k.clone(),
                    value: v.clone(),
                }).collect(),
                health: match s.health.as_str() {
                    "healthy" => ServiceHealth::Healthy,
                    "unhealthy" => ServiceHealth::Unhealthy,
                    _ => ServiceHealth::Unknown,
                },
                group_id: s.group_id.clone(),
                registered_at: timestamp_to_datetime(s.registered_at),
                last_heartbeat: timestamp_to_datetime(s.last_heartbeat),
            }
        }).collect();

        Ok(services)
    }

    async fn service(&self, ctx: &Context<'_>, service_id: String) -> Result<Option<Service>> {
        let services = self.services(ctx).await?;
        Ok(services.into_iter().find(|s| s.service_id == service_id))
    }

    async fn pipelines(&self, ctx: &Context<'_>) -> Result<Vec<Pipeline>> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        let state = node.get_state();
        let pipelines: Vec<Pipeline> = state.pipelines.values().map(|p| {
            Pipeline {
                pipeline_id: p.pipeline_id.clone(),
                name: p.name.clone(),
                enabled: p.enabled,
                version: p.version,
            }
        }).collect();

        Ok(pipelines)
    }

    async fn pipeline(&self, ctx: &Context<'_>, pipeline_id: String) -> Result<Option<Pipeline>> {
        let pipelines = self.pipelines(ctx).await?;
        Ok(pipelines.into_iter().find(|p| p.pipeline_id == pipeline_id))
    }

    async fn source_offsets(&self, ctx: &Context<'_>, source_id: String) -> Result<Vec<SourceOffset>> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        let offsets = node.get_source_offsets(&source_id, 0);
        let result: Vec<SourceOffset> = offsets.iter().map(|(&partition, &offset)| {
            SourceOffset {
                source_id: source_id.clone(),
                partition,
                offset,
            }
        }).collect();

        Ok(result)
    }

    async fn group_offsets(&self, ctx: &Context<'_>, group_id: String, source_id: Option<String>) -> Result<Vec<GroupOffsets>> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        let offsets = node.get_group_offsets(&group_id, source_id.as_deref());
        let result: Vec<GroupOffsets> = offsets.iter().map(|(sid, part_offsets)| {
            GroupOffsets {
                group_id: group_id.clone(),
                source_id: sid.clone(),
                partitions: part_offsets.offsets.iter().map(|(&p, &o)| {
                    PartitionOffset { partition: p, offset: o }
                }).collect(),
            }
        }).collect();

        Ok(result)
    }

    async fn consumer_groups(&self, ctx: &Context<'_>) -> Result<Vec<ConsumerGroup>> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        let state = node.get_state();
        let groups: Vec<ConsumerGroup> = state.groups.values().map(|g| {
            ConsumerGroup {
                group_id: g.group_id.clone(),
                stage_id: g.stage_id.clone(),
                members: g.members.clone(),
                generation: g.generation,
                partition_assignments: g.partition_assignments.iter().map(|(member, parts)| {
                    MemberAssignment {
                        member_id: member.clone(),
                        partitions: parts.clone(),
                    }
                }).collect(),
            }
        }).collect();

        Ok(groups)
    }

    async fn consumer_group(&self, ctx: &Context<'_>, group_id: String) -> Result<Option<ConsumerGroup>> {
        let groups = self.consumer_groups(ctx).await?;
        Ok(groups.into_iter().find(|g| g.group_id == group_id))
    }

    async fn service_checkpoint(&self, ctx: &Context<'_>, service_id: String) -> Result<Option<ServiceCheckpoint>> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        let checkpoint = node.get_service_checkpoint(&service_id);
        Ok(checkpoint.map(|cp| {
            ServiceCheckpoint {
                service_id: service_id.clone(),
                checkpoint_id: cp.checkpoint_id,
                source_offsets: cp.source_offsets.iter().map(|(sid, &offset)| {
                    SourceOffsetEntry {
                        source_id: sid.clone(),
                        offset,
                    }
                }).collect(),
                created_at: cp.created_at.map(|ts| {
                    DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                        .unwrap_or_else(|| Utc::now())
                }).unwrap_or_else(|| Utc::now()),
            }
        }))
    }

    async fn watermarks(&self, ctx: &Context<'_>, source_id: String) -> Result<Vec<Watermark>> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        let state = node.get_state();
        let watermarks: Vec<Watermark> = state.checkpoints.watermarks.values()
            .filter(|w| w.source_id == source_id)
            .map(|w| {
                Watermark {
                    source_id: w.source_id.clone(),
                    partition: w.partition,
                    position: w.position,
                    event_time: if w.event_time_secs > 0 {
                        DateTime::from_timestamp(w.event_time_secs, 0)
                    } else {
                        None
                    },
                }
            })
            .collect();

        Ok(watermarks)
    }

    async fn operational_errors(&self, ctx: &Context<'_>, filter: Option<ErrorFilter>) -> Result<Vec<OperationalError>> {
        let error_buffer = ctx.data::<ErrorBuffer>()?;
        Ok(error_buffer.get_errors(filter).await)
    }

    async fn operational_error(&self, ctx: &Context<'_>, id: String) -> Result<Option<OperationalError>> {
        let error_buffer = ctx.data::<ErrorBuffer>()?;
        Ok(error_buffer.get_error(&id).await)
    }

    async fn error_stats(&self, ctx: &Context<'_>) -> Result<ErrorStats> {
        let error_buffer = ctx.data::<ErrorBuffer>()?;
        Ok(error_buffer.get_stats().await)
    }
}

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn register_service(&self, ctx: &Context<'_>, input: RegisterServiceInput) -> Result<MutationResult> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        if !node.is_leader().await {
            return Err(Error::new("Not the leader. Redirect to leader node."));
        }

        let labels = input.labels.unwrap_or_default()
            .into_iter()
            .map(|l| (l.key, l.value))
            .collect();

        let cmd = RouterCommand::RegisterService {
            service_id: input.service_id,
            service_name: input.service_name,
            service_type: match input.service_type {
                ServiceType::Source => "source".to_string(),
                ServiceType::Transform => "transform".to_string(),
                ServiceType::Sink => "sink".to_string(),
            },
            endpoint: input.endpoint,
            labels,
            group_id: input.group_id,
        };

        match node.propose_and_wait(cmd).await {
            Ok(()) => Ok(MutationResult {
                success: true,
                message: Some("Service registered successfully".to_string()),
            }),
            Err(e) => Ok(MutationResult {
                success: false,
                message: Some(e.to_string()),
            }),
        }
    }

    async fn deregister_service(&self, ctx: &Context<'_>, service_id: String) -> Result<MutationResult> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        if !node.is_leader().await {
            return Err(Error::new("Not the leader. Redirect to leader node."));
        }

        let cmd = RouterCommand::DeregisterService { service_id };

        match node.propose_and_wait(cmd).await {
            Ok(()) => Ok(MutationResult {
                success: true,
                message: Some("Service deregistered successfully".to_string()),
            }),
            Err(e) => Ok(MutationResult {
                success: false,
                message: Some(e.to_string()),
            }),
        }
    }

    async fn commit_offset(&self, ctx: &Context<'_>, input: CommitOffsetInput) -> Result<MutationResult> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        if !node.is_leader().await {
            return Err(Error::new("Not the leader. Redirect to leader node."));
        }

        match node.commit_source_offset(&input.source_id, input.partition, input.offset).await {
            Ok(()) => Ok(MutationResult {
                success: true,
                message: Some("Offset committed successfully".to_string()),
            }),
            Err(e) => Ok(MutationResult {
                success: false,
                message: Some(e.to_string()),
            }),
        }
    }

    async fn commit_group_offset(&self, ctx: &Context<'_>, input: CommitGroupOffsetInput) -> Result<MutationResult> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        if !node.is_leader().await {
            return Err(Error::new("Not the leader. Redirect to leader node."));
        }

        match node.commit_group_offset(&input.group_id, &input.source_id, input.partition, input.offset).await {
            Ok(()) => Ok(MutationResult {
                success: true,
                message: Some("Group offset committed successfully".to_string()),
            }),
            Err(e) => Ok(MutationResult {
                success: false,
                message: Some(e.to_string()),
            }),
        }
    }

    async fn create_pipeline(&self, ctx: &Context<'_>, input: CreatePipelineInput) -> Result<MutationResult> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        if !node.is_leader().await {
            return Err(Error::new("Not the leader. Redirect to leader node."));
        }

        let cmd = RouterCommand::CreatePipeline {
            pipeline_id: input.pipeline_id,
            name: input.name,
            config: input.config.into_bytes(),
        };

        match node.propose_and_wait(cmd).await {
            Ok(()) => Ok(MutationResult {
                success: true,
                message: Some("Pipeline created successfully".to_string()),
            }),
            Err(e) => Ok(MutationResult {
                success: false,
                message: Some(e.to_string()),
            }),
        }
    }

    async fn enable_pipeline(&self, ctx: &Context<'_>, pipeline_id: String) -> Result<MutationResult> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        if !node.is_leader().await {
            return Err(Error::new("Not the leader. Redirect to leader node."));
        }

        let cmd = RouterCommand::EnablePipeline { pipeline_id };

        match node.propose_and_wait(cmd).await {
            Ok(()) => Ok(MutationResult {
                success: true,
                message: Some("Pipeline enabled successfully".to_string()),
            }),
            Err(e) => Ok(MutationResult {
                success: false,
                message: Some(e.to_string()),
            }),
        }
    }

    async fn disable_pipeline(&self, ctx: &Context<'_>, pipeline_id: String) -> Result<MutationResult> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        if !node.is_leader().await {
            return Err(Error::new("Not the leader. Redirect to leader node."));
        }

        let cmd = RouterCommand::DisablePipeline { pipeline_id };

        match node.propose_and_wait(cmd).await {
            Ok(()) => Ok(MutationResult {
                success: true,
                message: Some("Pipeline disabled successfully".to_string()),
            }),
            Err(e) => Ok(MutationResult {
                success: false,
                message: Some(e.to_string()),
            }),
        }
    }

    async fn delete_pipeline(&self, ctx: &Context<'_>, pipeline_id: String) -> Result<MutationResult> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        if !node.is_leader().await {
            return Err(Error::new("Not the leader. Redirect to leader node."));
        }

        let cmd = RouterCommand::DeletePipeline { pipeline_id };

        match node.propose_and_wait(cmd).await {
            Ok(()) => Ok(MutationResult {
                success: true,
                message: Some("Pipeline deleted successfully".to_string()),
            }),
            Err(e) => Ok(MutationResult {
                success: false,
                message: Some(e.to_string()),
            }),
        }
    }

    async fn advance_watermark(&self, ctx: &Context<'_>, input: AdvanceWatermarkInput) -> Result<MutationResult> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        if !node.is_leader().await {
            return Err(Error::new("Not the leader. Redirect to leader node."));
        }

        let cmd = RouterCommand::AdvanceWatermark {
            source_id: input.source_id,
            partition: input.partition,
            position: input.position,
            event_time: input.event_time.map(|dt| SerializableTimestamp {
                seconds: dt.timestamp(),
                nanos: dt.timestamp_subsec_nanos() as i32,
            }),
        };

        match node.propose_and_wait(cmd).await {
            Ok(()) => Ok(MutationResult {
                success: true,
                message: Some("Watermark advanced successfully".to_string()),
            }),
            Err(e) => Ok(MutationResult {
                success: false,
                message: Some(e.to_string()),
            }),
        }
    }

    async fn renew_service_lease(&self, ctx: &Context<'_>, service_id: String) -> Result<MutationResult> {
        let raft_node = ctx.data::<Arc<RwLock<RaftNode>>>()?;
        let node = raft_node.read().await;

        if !node.is_leader().await {
            return Err(Error::new("Not the leader. Redirect to leader node."));
        }

        let cmd = RouterCommand::RenewLease { service_id };

        match node.propose_and_wait(cmd).await {
            Ok(()) => Ok(MutationResult {
                success: true,
                message: Some("Lease renewed successfully".to_string()),
            }),
            Err(e) => Ok(MutationResult {
                success: false,
                message: Some(e.to_string()),
            }),
        }
    }

    async fn resolve_error(&self, ctx: &Context<'_>, id: String) -> Result<MutationResult> {
        let error_buffer = ctx.data::<ErrorBuffer>()?;
        if error_buffer.resolve_error(&id).await {
            Ok(MutationResult {
                success: true,
                message: Some("Error resolved successfully".to_string()),
            })
        } else {
            Ok(MutationResult {
                success: false,
                message: Some("Error not found".to_string()),
            })
        }
    }

    async fn clear_resolved_errors(&self, ctx: &Context<'_>) -> Result<MutationResult> {
        let error_buffer = ctx.data::<ErrorBuffer>()?;
        error_buffer.clear_resolved().await;
        Ok(MutationResult {
            success: true,
            message: Some("Resolved errors cleared".to_string()),
        })
    }
}

fn timestamp_to_datetime(secs: u64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs as i64, 0).unwrap_or_else(|| Utc::now())
}
