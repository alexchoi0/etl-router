use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn, debug, instrument};

use conveyor_etl_proto::sidecar::{
    sidecar_data_plane_server::SidecarDataPlane,
    PushRecordsRequest, PushRecordsResponse,
    ReceiveRecordsRequest, ReceiveRecordsResponse,
    push_records_request, push_records_response,
    PushAck, PushBackpressure, PushCredits,
    RecordAck, AckStatus,
};
use conveyor_etl_proto::common::RecordBatch;

use crate::routing::{SharedRoutingTable, RouteDecision, LocalRouter, RemoteRouter};

type PushStream = Pin<Box<dyn Stream<Item = Result<PushRecordsResponse, Status>> + Send>>;

pub struct SidecarDataPlaneImpl {
    routing_table: SharedRoutingTable,
    local_router: Arc<LocalRouter>,
    remote_router: Arc<RemoteRouter>,
    sidecar_id: String,
}

impl SidecarDataPlaneImpl {
    pub fn new(
        routing_table: SharedRoutingTable,
        local_router: Arc<LocalRouter>,
        remote_router: Arc<RemoteRouter>,
        sidecar_id: String,
    ) -> Self {
        Self {
            routing_table,
            local_router,
            remote_router,
            sidecar_id,
        }
    }

    async fn process_batch(
        &self,
        pipeline_id: &str,
        batch: RecordBatch,
    ) -> Result<Vec<RecordAck>, Status> {
        let table = self.routing_table.read().await;

        let routes = table
            .get_pipeline_routes(pipeline_id)
            .ok_or_else(|| Status::not_found(format!("Pipeline {} not found", pipeline_id)))?;

        let mut acks = Vec::new();

        for stage in routes.stages.values() {
            match &stage.decision {
                RouteDecision::Local { endpoint } => {
                    debug!(
                        pipeline = pipeline_id,
                        stage = %stage.stage_id,
                        endpoint = %endpoint,
                        "Routing batch to local service"
                    );

                    let result = self
                        .local_router
                        .route_to_transform(&endpoint, &stage.stage_id, batch.clone(), HashMap::new())
                        .await;

                    match result {
                        Ok(_output_batches) => {
                            for record in &batch.records {
                                acks.push(RecordAck {
                                    record_id: record.id.clone(),
                                    status: AckStatus::Success as i32,
                                    error: String::new(),
                                });
                            }
                        }
                        Err(e) => {
                            warn!(
                                pipeline = pipeline_id,
                                stage = %stage.stage_id,
                                error = %e,
                                "Local routing failed"
                            );
                            for record in &batch.records {
                                acks.push(RecordAck {
                                    record_id: record.id.clone(),
                                    status: AckStatus::Retry as i32,
                                    error: e.to_string(),
                                });
                            }
                        }
                    }
                }
                RouteDecision::Remote {
                    sidecar_id: remote_sidecar_id,
                    endpoint,
                } => {
                    debug!(
                        pipeline = pipeline_id,
                        stage = %stage.stage_id,
                        remote_sidecar = %remote_sidecar_id,
                        endpoint = %endpoint,
                        "Forwarding batch to remote sidecar"
                    );

                    let result = self
                        .remote_router
                        .forward_to_sidecar(&endpoint, pipeline_id, &stage.stage_id, batch.clone())
                        .await;

                    match result {
                        Ok(success) => {
                            for record in &batch.records {
                                acks.push(RecordAck {
                                    record_id: record.id.clone(),
                                    status: if success {
                                        AckStatus::Success as i32
                                    } else {
                                        AckStatus::Failed as i32
                                    },
                                    error: String::new(),
                                });
                            }
                        }
                        Err(e) => {
                            warn!(
                                pipeline = pipeline_id,
                                stage = %stage.stage_id,
                                error = %e,
                                "Remote routing failed"
                            );
                            for record in &batch.records {
                                acks.push(RecordAck {
                                    record_id: record.id.clone(),
                                    status: AckStatus::Retry as i32,
                                    error: e.to_string(),
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(acks)
    }
}

#[tonic::async_trait]
impl SidecarDataPlane for SidecarDataPlaneImpl {
    type PushRecordsStream = PushStream;

    async fn push_records(
        &self,
        request: Request<Streaming<PushRecordsRequest>>,
    ) -> Result<Response<Self::PushRecordsStream>, Status> {
        let mut stream = request.into_inner();

        let init_msg = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("Expected init message"))?
            .map_err(|e| Status::internal(format!("Stream error: {}", e)))?;

        let init = match init_msg.msg {
            Some(push_records_request::Msg::Init(init)) => init,
            _ => return Err(Status::invalid_argument("First message must be init")),
        };

        info!(
            source = %init.source_id,
            pipeline = %init.pipeline_id,
            "New push stream started"
        );

        let pipeline_id = init.pipeline_id.clone();
        let source_id = init.source_id.clone();

        let routing_table = self.routing_table.clone();
        let local_router = self.local_router.clone();
        let remote_router = self.remote_router.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(16);

        let _ = tx
            .send(Ok(PushRecordsResponse {
                msg: Some(push_records_response::Msg::Credits(PushCredits {
                    granted: 100,
                })),
            }))
            .await;

        let sidecar_id = self.sidecar_id.clone();
        tokio::spawn(async move {
            let handler = SidecarDataPlaneImpl {
                routing_table,
                local_router,
                remote_router,
                sidecar_id,
            };

            while let Some(result) = stream.next().await {
                match result {
                    Ok(msg) => match msg.msg {
                        Some(push_records_request::Msg::Batch(batch)) => {
                            let batch_id = batch.batch_id.clone();

                            match handler.process_batch(&pipeline_id, batch).await {
                                Ok(record_acks) => {
                                    let ack = PushRecordsResponse {
                                        msg: Some(push_records_response::Msg::Ack(PushAck {
                                            batch_id,
                                            success: record_acks
                                                .iter()
                                                .all(|a| a.status == AckStatus::Success as i32),
                                            record_acks,
                                        })),
                                    };
                                    if tx.send(Ok(ack)).await.is_err() {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        pipeline = %pipeline_id,
                                        batch = %batch_id,
                                        error = %e,
                                        "Batch processing failed"
                                    );
                                    let backpressure = PushRecordsResponse {
                                        msg: Some(push_records_response::Msg::Backpressure(
                                            PushBackpressure {
                                                pause: true,
                                                wait_ms: 1000,
                                                reason: e.to_string(),
                                            },
                                        )),
                                    };
                                    if tx.send(Ok(backpressure)).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                        Some(push_records_request::Msg::Heartbeat(_)) => {
                            debug!(source = %source_id, "Heartbeat received");
                        }
                        Some(push_records_request::Msg::Init(_)) => {
                            warn!("Unexpected init message in stream");
                        }
                        None => {}
                    },
                    Err(e) => {
                        warn!(error = %e, "Stream error");
                        break;
                    }
                }
            }

            info!(
                source = %source_id,
                pipeline = %pipeline_id,
                "Push stream ended"
            );
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::PushRecordsStream))
    }

    #[instrument(skip(self, request), fields(pipeline = %request.get_ref().pipeline_id))]
    async fn receive_records(
        &self,
        request: Request<ReceiveRecordsRequest>,
    ) -> Result<Response<ReceiveRecordsResponse>, Status> {
        let req = request.into_inner();

        debug!(
            pipeline = %req.pipeline_id,
            stage = %req.stage_id,
            source_sidecar = %req.source_sidecar_id,
            "Received records from remote sidecar"
        );

        let batch = req
            .batch
            .ok_or_else(|| Status::invalid_argument("Missing batch"))?;

        let record_acks = self.process_batch(&req.pipeline_id, batch).await?;

        let success = record_acks
            .iter()
            .all(|a| a.status == AckStatus::Success as i32);

        Ok(Response::new(ReceiveRecordsResponse {
            success,
            error: if success {
                String::new()
            } else {
                "Some records failed".to_string()
            },
            record_acks,
        }))
    }
}
