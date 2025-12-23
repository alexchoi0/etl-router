use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use conveyor_etl_proto::source::{
    source_router_server::SourceRouter,
    PushRequest, PushResponse, CreditRequest, CreditResponse,
    BatchAck, RecordAck, AckStatus, BackpressureSignal, CreditGrant,
    BackpressureLevel,
};
use conveyor_etl_raft::RaftNode;
use conveyor_etl_buffer::BufferManager;
use conveyor_etl_routing::RoutingEngine;

pub struct SourceRouterImpl {
    raft_node: Arc<RwLock<RaftNode>>,
    buffer_manager: Arc<RwLock<BufferManager>>,
    routing_engine: Arc<RwLock<RoutingEngine>>,
}

impl SourceRouterImpl {
    pub fn new(
        raft_node: Arc<RwLock<RaftNode>>,
        buffer_manager: Arc<RwLock<BufferManager>>,
        routing_engine: Arc<RwLock<RoutingEngine>>,
    ) -> Self {
        Self {
            raft_node,
            buffer_manager,
            routing_engine,
        }
    }
}

type PushRecordsStream = Pin<Box<dyn Stream<Item = Result<PushResponse, Status>> + Send>>;

#[tonic::async_trait]
impl SourceRouter for SourceRouterImpl {
    type PushRecordsStream = PushRecordsStream;

    async fn push_records(
        &self,
        request: Request<Streaming<PushRequest>>,
    ) -> Result<Response<Self::PushRecordsStream>, Status> {
        let mut stream = request.into_inner();
        let _raft_node = self.raft_node.clone();
        let buffer_manager = self.buffer_manager.clone();
        let _routing_engine = self.routing_engine.clone();

        let output = async_stream::try_stream! {
            while let Some(req) = stream.message().await? {
                match req.msg {
                    Some(conveyor_etl_proto::source::push_request::Msg::Batch(batch)) => {
                        let source_id = batch.records.first()
                            .and_then(|r| r.id.as_ref())
                            .map(|id| id.source_id.clone())
                            .unwrap_or_else(|| "unknown".to_string());

                        let buffer = buffer_manager.read().await;
                        if buffer.should_backpressure(&source_id).await {
                            yield PushResponse {
                                msg: Some(conveyor_etl_proto::source::push_response::Msg::Backpressure(
                                    BackpressureSignal {
                                        pause: true,
                                        recommended_batch_size: 100,
                                        resume_after_ms: 1000,
                                        level: BackpressureLevel::High as i32,
                                    }
                                )),
                            };
                            continue;
                        }
                        drop(buffer);

                        let mut record_acks = Vec::new();
                        for record in &batch.records {
                            let ack = RecordAck {
                                record_id: record.id.clone(),
                                status: AckStatus::Accepted as i32,
                                error_message: String::new(),
                            };
                            record_acks.push(ack);
                        }

                        yield PushResponse {
                            msg: Some(conveyor_etl_proto::source::push_response::Msg::Ack(BatchAck {
                                batch_id: batch.batch_id.clone(),
                                record_acks,
                            })),
                        };
                    }
                    Some(conveyor_etl_proto::source::push_request::Msg::Heartbeat(_hb)) => {
                        let buffer = buffer_manager.read().await;
                        let source_id = "default";
                        let credits = buffer.available_credits(source_id).await;

                        yield PushResponse {
                            msg: Some(conveyor_etl_proto::source::push_response::Msg::Credits(CreditGrant {
                                credits,
                                expires_at_ms: chrono::Utc::now().timestamp_millis() as u64 + 30000,
                            })),
                        };
                    }
                    None => {}
                }
            }
        };

        Ok(Response::new(Box::pin(output) as Self::PushRecordsStream))
    }

    async fn request_credits(
        &self,
        request: Request<CreditRequest>,
    ) -> Result<Response<CreditResponse>, Status> {
        let req = request.into_inner();
        let buffer = self.buffer_manager.read().await;

        let source_id = &req.source_id;
        let available = buffer.available_credits(source_id).await;
        let granted = std::cmp::min(req.requested_credits, available);

        let current_pressure = if buffer.should_backpressure(source_id).await {
            BackpressureLevel::High
        } else {
            BackpressureLevel::None
        };

        Ok(Response::new(CreditResponse {
            granted_credits: granted,
            total_available: available,
            expires_in_ms: 30000,
            current_pressure: current_pressure as i32,
        }))
    }
}
