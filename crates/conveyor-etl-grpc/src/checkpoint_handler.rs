use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{info, debug};

use crate::error::{GrpcError, ResultExt};

use conveyor_etl_proto::checkpoint::{
    checkpoint_service_server::CheckpointService,
    CommitOffsetRequest, CommitOffsetResponse,
    GetOffsetRequest, GetOffsetResponse,
    WatermarkRequest, WatermarkResponse,
    GetWatermarkRequest, GetWatermarkResponse,
    SaveCheckpointRequest, SaveCheckpointResponse,
    GetCheckpointRequest, GetCheckpointResponse,
    GetGroupOffsetsRequest, GetGroupOffsetsResponse,
    CommitGroupOffsetRequest, CommitGroupOffsetResponse,
    GetPipelineCheckpointsRequest, GetPipelineCheckpointsResponse,
    PartitionOffsets,
};
use conveyor_etl_raft::RaftNode;

pub struct CheckpointServiceImpl {
    raft_node: Arc<RwLock<RaftNode>>,
}

impl CheckpointServiceImpl {
    pub fn new(raft_node: Arc<RwLock<RaftNode>>) -> Self {
        Self { raft_node }
    }
}

#[tonic::async_trait]
impl CheckpointService for CheckpointServiceImpl {
    async fn commit_offset(
        &self,
        request: Request<CommitOffsetRequest>,
    ) -> Result<Response<CommitOffsetResponse>, Status> {
        let req = request.into_inner();

        debug!(
            source_id = %req.source_id,
            partition = req.partition,
            offset = req.offset,
            "Committing offset"
        );

        let node = self.raft_node.write().await;
        node.commit_source_offset(&req.source_id, req.partition, req.offset)
            .await
            .map_to_status()?;
        Ok(Response::new(CommitOffsetResponse { success: true }))
    }

    async fn get_source_offset(
        &self,
        request: Request<GetOffsetRequest>,
    ) -> Result<Response<GetOffsetResponse>, Status> {
        let req = request.into_inner();

        let node = self.raft_node.read().await;
        let offsets = node.get_source_offsets(&req.source_id, req.partition.unwrap_or(0));

        Ok(Response::new(GetOffsetResponse {
            offsets,
            timestamps: Default::default(),
        }))
    }

    async fn report_watermark(
        &self,
        request: Request<WatermarkRequest>,
    ) -> Result<Response<WatermarkResponse>, Status> {
        let req = request.into_inner();
        let watermark = req.watermark.ok_or_else(|| GrpcError::missing_field("watermark"))?;

        debug!(
            source_id = %watermark.source_id,
            partition = watermark.partition,
            position = watermark.position,
            "Reporting watermark"
        );

        let node = self.raft_node.write().await;
        let global = node.advance_watermark(&watermark).await.map_to_status()?;
        Ok(Response::new(WatermarkResponse {
            accepted: true,
            current_global_watermark: Some(global),
        }))
    }

    async fn get_watermark(
        &self,
        request: Request<GetWatermarkRequest>,
    ) -> Result<Response<GetWatermarkResponse>, Status> {
        let req = request.into_inner();

        let node = self.raft_node.read().await;
        let (watermarks, global) = node.get_watermarks(&req.source_id, req.partition.unwrap_or(0));

        Ok(Response::new(GetWatermarkResponse {
            watermarks,
            global_watermark: Some(global),
        }))
    }

    async fn save_checkpoint(
        &self,
        request: Request<SaveCheckpointRequest>,
    ) -> Result<Response<SaveCheckpointResponse>, Status> {
        let req = request.into_inner();

        info!(
            service_id = %req.service_id,
            checkpoint_id = %req.checkpoint_id,
            "Saving checkpoint"
        );

        let source_offsets: HashMap<String, u64> = req.source_offsets
            .iter()
            .flat_map(|(source_id, partition_offsets)| {
                partition_offsets.offsets.iter().map(move |(partition, offset)| {
                    (format!("{}:{}", source_id, partition), *offset)
                })
            })
            .collect();

        let node = self.raft_node.write().await;
        let created_at = node.save_service_checkpoint(&req.service_id, &req.checkpoint_id, &req.data, &source_offsets)
            .await
            .map_to_status()?;
        Ok(Response::new(SaveCheckpointResponse {
            success: true,
            checkpoint_id: req.checkpoint_id,
            created_at: Some(created_at),
        }))
    }

    async fn get_checkpoint(
        &self,
        request: Request<GetCheckpointRequest>,
    ) -> Result<Response<GetCheckpointResponse>, Status> {
        let req = request.into_inner();

        debug!(service_id = %req.service_id, "Getting checkpoint");

        let node = self.raft_node.read().await;
        match node.get_service_checkpoint(&req.service_id) {
            Some(checkpoint) => {
                let source_offsets: HashMap<String, PartitionOffsets> = checkpoint.source_offsets
                    .iter()
                    .map(|(k, v)| {
                        let parts: Vec<&str> = k.split(':').collect();
                        let source_id = parts.get(0).unwrap_or(&"").to_string();
                        let partition: u32 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
                        (source_id, PartitionOffsets {
                            offsets: [(partition, *v)].into_iter().collect(),
                        })
                    })
                    .collect();

                Ok(Response::new(GetCheckpointResponse {
                    found: true,
                    checkpoint_id: checkpoint.checkpoint_id,
                    data: checkpoint.data,
                    source_offsets,
                    created_at: checkpoint.created_at,
                }))
            }
            None => Ok(Response::new(GetCheckpointResponse {
                found: false,
                ..Default::default()
            })),
        }
    }

    async fn get_group_offsets(
        &self,
        request: Request<GetGroupOffsetsRequest>,
    ) -> Result<Response<GetGroupOffsetsResponse>, Status> {
        let req = request.into_inner();

        debug!(group_id = %req.group_id, "Getting group offsets");

        let node = self.raft_node.read().await;
        let offsets = node.get_group_offsets(&req.group_id, req.source_id.as_deref());

        Ok(Response::new(GetGroupOffsetsResponse {
            source_offsets: offsets,
        }))
    }

    async fn commit_group_offset(
        &self,
        request: Request<CommitGroupOffsetRequest>,
    ) -> Result<Response<CommitGroupOffsetResponse>, Status> {
        let req = request.into_inner();

        debug!(
            group_id = %req.group_id,
            source_id = %req.source_id,
            partition = req.partition,
            offset = req.offset,
            "Committing group offset"
        );

        let node = self.raft_node.write().await;
        node.commit_group_offset(&req.group_id, &req.source_id, req.partition, req.offset)
            .await
            .map_to_status()?;
        Ok(Response::new(CommitGroupOffsetResponse { success: true }))
    }

    async fn get_pipeline_checkpoints(
        &self,
        request: Request<GetPipelineCheckpointsRequest>,
    ) -> Result<Response<GetPipelineCheckpointsResponse>, Status> {
        let req = request.into_inner();

        debug!(pipeline_id = %req.pipeline_id, "Getting pipeline checkpoints");

        let node = self.raft_node.read().await;
        let (_service_checkpoints, source_offsets, watermarks) = node.get_pipeline_checkpoints(&req.pipeline_id);

        Ok(Response::new(GetPipelineCheckpointsResponse {
            service_checkpoints: Vec::new(),
            source_offsets,
            watermarks,
        }))
    }
}
