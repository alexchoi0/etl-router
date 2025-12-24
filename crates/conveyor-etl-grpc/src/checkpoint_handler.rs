use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use crate::error::ResultExt;

use conveyor_etl_proto::checkpoint::{
    checkpoint_service_server::CheckpointService, CommitGroupOffsetRequest,
    CommitGroupOffsetResponse, CommitOffsetRequest, CommitOffsetResponse, GetCheckpointRequest,
    GetCheckpointResponse, GetGroupOffsetsRequest, GetGroupOffsetsResponse, GetOffsetRequest,
    GetOffsetResponse, GetPipelineCheckpointsRequest, GetPipelineCheckpointsResponse,
    GetWatermarkRequest, GetWatermarkResponse, PartitionOffsets, SaveCheckpointRequest,
    SaveCheckpointResponse, WatermarkRequest, WatermarkResponse,
};
use conveyor_etl_proto::common::Watermark;
use conveyor_etl_raft::{
    ConveyorRaft, RouterCommand, RouterRequest, RouterState, SerializableTimestamp,
};

pub struct CheckpointServiceImpl {
    raft: Arc<ConveyorRaft>,
    state: Arc<RwLock<RouterState>>,
}

impl CheckpointServiceImpl {
    pub fn new(raft: Arc<ConveyorRaft>, state: Arc<RwLock<RouterState>>) -> Self {
        Self { raft, state }
    }

    async fn propose(&self, command: RouterCommand) -> Result<(), Status> {
        let request = RouterRequest { command };
        self.raft
            .client_write(request)
            .await
            .map_err(|e| Status::internal(format!("Raft error: {}", e)))?;
        Ok(())
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

        let command = RouterCommand::CommitSourceOffset {
            source_id: req.source_id,
            partition: req.partition,
            offset: req.offset,
        };

        self.propose(command).await?;
        Ok(Response::new(CommitOffsetResponse { success: true }))
    }

    async fn get_source_offset(
        &self,
        request: Request<GetOffsetRequest>,
    ) -> Result<Response<GetOffsetResponse>, Status> {
        let req = request.into_inner();

        let state = self.state.read().await;
        let offsets = state.get_source_offsets(&req.source_id);

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
        let watermark = req
            .watermark
            .ok_or_else(|| Status::invalid_argument("Missing watermark"))?;

        debug!(
            source_id = %watermark.source_id,
            partition = watermark.partition,
            position = watermark.position,
            "Reporting watermark"
        );

        let command = RouterCommand::AdvanceWatermark {
            source_id: watermark.source_id.clone(),
            partition: watermark.partition,
            position: watermark.position,
            event_time: watermark.event_time.map(|t| SerializableTimestamp {
                seconds: t.seconds,
                nanos: t.nanos,
            }),
        };

        self.propose(command).await?;

        let state = self.state.read().await;
        let key = format!("{}:{}", watermark.source_id, watermark.partition);
        let global = state
            .checkpoints
            .watermarks
            .get(&key)
            .map(|w| Watermark {
                source_id: w.source_id.clone(),
                partition: w.partition,
                position: w.position,
                event_time: Some(prost_types::Timestamp {
                    seconds: w.event_time_secs,
                    nanos: 0,
                }),
            })
            .unwrap_or_else(|| watermark.clone());

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

        let state = self.state.read().await;
        let key = format!("{}:{}", req.source_id, req.partition.unwrap_or(0));

        let watermarks: Vec<Watermark> = state
            .checkpoints
            .watermarks
            .iter()
            .filter(|(k, _)| k.starts_with(&req.source_id))
            .map(|(_, w)| Watermark {
                source_id: w.source_id.clone(),
                partition: w.partition,
                position: w.position,
                event_time: Some(prost_types::Timestamp {
                    seconds: w.event_time_secs,
                    nanos: 0,
                }),
            })
            .collect();

        let global = state
            .checkpoints
            .watermarks
            .get(&key)
            .map(|w| Watermark {
                source_id: w.source_id.clone(),
                partition: w.partition,
                position: w.position,
                event_time: Some(prost_types::Timestamp {
                    seconds: w.event_time_secs,
                    nanos: 0,
                }),
            })
            .unwrap_or_default();

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

        let source_offsets: HashMap<String, u64> = req
            .source_offsets
            .iter()
            .flat_map(|(source_id, partition_offsets)| {
                partition_offsets
                    .offsets
                    .iter()
                    .map(move |(partition, offset)| (format!("{}:{}", source_id, partition), *offset))
            })
            .collect();

        let command = RouterCommand::SaveServiceCheckpoint {
            service_id: req.service_id.clone(),
            checkpoint_id: req.checkpoint_id.clone(),
            data: req.data,
            source_offsets,
        };

        self.propose(command).await?;

        let state = self.state.read().await;
        let created_at = state
            .checkpoints
            .service_checkpoints
            .get(&req.service_id)
            .map(|c| prost_types::Timestamp {
                seconds: c.created_at as i64,
                nanos: 0,
            });

        Ok(Response::new(SaveCheckpointResponse {
            success: true,
            checkpoint_id: req.checkpoint_id,
            created_at,
        }))
    }

    async fn get_checkpoint(
        &self,
        request: Request<GetCheckpointRequest>,
    ) -> Result<Response<GetCheckpointResponse>, Status> {
        let req = request.into_inner();

        debug!(service_id = %req.service_id, "Getting checkpoint");

        let state = self.state.read().await;
        match state.checkpoints.service_checkpoints.get(&req.service_id) {
            Some(checkpoint) => {
                let source_offsets: HashMap<String, PartitionOffsets> = checkpoint
                    .source_offsets
                    .iter()
                    .map(|(k, v)| {
                        let parts: Vec<&str> = k.split(':').collect();
                        let source_id = parts.first().unwrap_or(&"").to_string();
                        let partition: u32 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
                        (
                            source_id,
                            PartitionOffsets {
                                offsets: [(partition, *v)].into_iter().collect(),
                            },
                        )
                    })
                    .collect();

                Ok(Response::new(GetCheckpointResponse {
                    found: true,
                    checkpoint_id: checkpoint.checkpoint_id.clone(),
                    data: checkpoint.data.clone(),
                    source_offsets,
                    created_at: Some(prost_types::Timestamp {
                        seconds: checkpoint.created_at as i64,
                        nanos: 0,
                    }),
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

        let state = self.state.read().await;
        let offsets = state
            .checkpoints
            .group_offsets
            .get(&req.group_id)
            .map(|group_offsets| {
                group_offsets
                    .iter()
                    .filter(|(source_id, _)| {
                        req.source_id.is_none() || req.source_id.as_ref() == Some(source_id)
                    })
                    .map(|(source_id, partitions)| {
                        (
                            source_id.clone(),
                            PartitionOffsets {
                                offsets: partitions.clone(),
                            },
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

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

        let command = RouterCommand::CommitGroupOffset {
            group_id: req.group_id,
            source_id: req.source_id,
            partition: req.partition,
            offset: req.offset,
        };

        self.propose(command).await?;
        Ok(Response::new(CommitGroupOffsetResponse { success: true }))
    }

    async fn get_pipeline_checkpoints(
        &self,
        request: Request<GetPipelineCheckpointsRequest>,
    ) -> Result<Response<GetPipelineCheckpointsResponse>, Status> {
        let req = request.into_inner();

        debug!(pipeline_id = %req.pipeline_id, "Getting pipeline checkpoints");

        let state = self.state.read().await;

        let source_offsets: HashMap<String, PartitionOffsets> = state
            .checkpoints
            .source_offsets
            .iter()
            .map(|(source_id, partitions)| {
                (
                    source_id.clone(),
                    PartitionOffsets {
                        offsets: partitions.clone(),
                    },
                )
            })
            .collect();

        let watermarks: Vec<Watermark> = state
            .checkpoints
            .watermarks
            .values()
            .map(|w| Watermark {
                source_id: w.source_id.clone(),
                partition: w.partition,
                position: w.position,
                event_time: Some(prost_types::Timestamp {
                    seconds: w.event_time_secs,
                    nanos: 0,
                }),
            })
            .collect();

        Ok(Response::new(GetPipelineCheckpointsResponse {
            service_checkpoints: Vec::new(),
            source_offsets,
            watermarks,
        }))
    }
}
