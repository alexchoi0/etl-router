use std::pin::Pin;
use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::RwLock;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::{info, warn, debug};

use conveyor_etl_proto::sidecar::{
    sidecar_coordinator_server::SidecarCoordinator,
    RegisterSidecarRequest, RegisterSidecarResponse,
    SidecarHeartbeatRequest, HeartbeatResponse,
    WatchAssignmentsRequest, PipelineAssignmentEvent,
    PipelineAssignment, SidecarCommand, LocalService,
    sidecar_command, ServiceType,
};
use conveyor_etl_raft::{RaftNode, RouterCommand, SidecarLocalService, SidecarStageTarget};

type ResponseStream = Pin<Box<dyn Stream<Item = Result<PipelineAssignmentEvent, Status>> + Send>>;

pub struct SidecarCoordinatorImpl {
    raft_node: Arc<RwLock<RaftNode>>,
    pending_assignments: DashMap<String, Vec<PipelineAssignment>>,
}

impl SidecarCoordinatorImpl {
    pub fn new(raft_node: Arc<RwLock<RaftNode>>) -> Self {
        Self {
            raft_node,
            pending_assignments: DashMap::new(),
        }
    }

    fn convert_local_services(proto_services: Vec<LocalService>) -> Vec<SidecarLocalService> {
        proto_services
            .into_iter()
            .map(|svc| SidecarLocalService {
                service_name: svc.service_name,
                service_type: match ServiceType::try_from(svc.service_type) {
                    Ok(ServiceType::Source) => "source".to_string(),
                    Ok(ServiceType::Transform) => "transform".to_string(),
                    Ok(ServiceType::Sink) => "sink".to_string(),
                    _ => "unknown".to_string(),
                },
                local_endpoint: svc.local_endpoint,
            })
            .collect()
    }

    async fn compute_pipeline_assignments(
        &self,
        sidecar_id: &str,
        local_services: &[SidecarLocalService],
    ) -> Vec<PipelineAssignment> {
        let raft = self.raft_node.read().await;
        let state = raft.get_state();

        let mut assignments = Vec::new();

        for (pipeline_id, pipeline) in &state.pipelines {
            if !pipeline.enabled {
                continue;
            }

            let mut stage_assignments = Vec::new();
            let mut all_local = true;

            for svc in local_services {
                stage_assignments.push(conveyor_etl_proto::sidecar::StageAssignment {
                    stage_id: svc.service_name.clone(),
                    target: Some(conveyor_etl_proto::sidecar::stage_assignment::Target::LocalEndpoint(
                        svc.local_endpoint.clone(),
                    )),
                });
            }

            for (other_sidecar_id, other_sidecar) in &state.sidecars {
                if other_sidecar_id == sidecar_id {
                    continue;
                }
                for svc in &other_sidecar.local_services {
                    if !local_services.iter().any(|ls| ls.service_name == svc.service_name) {
                        all_local = false;
                        stage_assignments.push(conveyor_etl_proto::sidecar::StageAssignment {
                            stage_id: svc.service_name.clone(),
                            target: Some(
                                conveyor_etl_proto::sidecar::stage_assignment::Target::RemoteSidecar(
                                    conveyor_etl_proto::sidecar::RemoteSidecar {
                                        sidecar_id: other_sidecar_id.clone(),
                                        endpoint: other_sidecar.endpoint.clone(),
                                    },
                                ),
                            ),
                        });
                    }
                }
            }

            assignments.push(PipelineAssignment {
                pipeline_id: pipeline_id.clone(),
                is_local_complete: all_local,
                stages: stage_assignments,
            });
        }

        assignments
    }
}

#[tonic::async_trait]
impl SidecarCoordinator for SidecarCoordinatorImpl {
    async fn register_sidecar(
        &self,
        request: Request<RegisterSidecarRequest>,
    ) -> Result<Response<RegisterSidecarResponse>, Status> {
        let req = request.into_inner();
        info!(
            "Registering sidecar {} from pod {}/{} with {} local services",
            req.sidecar_id,
            req.namespace,
            req.pod_name,
            req.local_services.len()
        );

        let local_services = Self::convert_local_services(req.local_services.clone());

        let command = RouterCommand::RegisterSidecar {
            sidecar_id: req.sidecar_id.clone(),
            pod_name: req.pod_name.clone(),
            namespace: req.namespace.clone(),
            endpoint: req.sidecar_endpoint.clone(),
            local_services: local_services.clone(),
        };

        {
            let raft = self.raft_node.write().await;
            if let Err(e) = raft.propose(command).await {
                warn!("Failed to propose sidecar registration: {}", e);
                return Ok(Response::new(RegisterSidecarResponse {
                    success: false,
                    error: e.to_string(),
                    registration_id: String::new(),
                    initial_assignments: Vec::new(),
                }));
            }
        }

        let initial_assignments = self
            .compute_pipeline_assignments(&req.sidecar_id, &local_services)
            .await;

        let registration_id = uuid::Uuid::new_v4().to_string();

        info!(
            "Sidecar {} registered with {} initial pipeline assignments",
            req.sidecar_id,
            initial_assignments.len()
        );

        Ok(Response::new(RegisterSidecarResponse {
            success: true,
            error: String::new(),
            registration_id,
            initial_assignments,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<SidecarHeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        debug!("Heartbeat from sidecar {}", req.sidecar_id);

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let command = RouterCommand::UpdateSidecarHeartbeat {
            sidecar_id: req.sidecar_id.clone(),
            timestamp,
        };

        {
            let raft = self.raft_node.write().await;
            if let Err(e) = raft.propose(command).await {
                warn!("Failed to update sidecar heartbeat: {}", e);
            }
        }

        let commands = self.pending_assignments
            .remove(&req.sidecar_id)
            .map(|(_, assignments)| {
                assignments
                    .into_iter()
                    .map(|assignment| SidecarCommand {
                        command: Some(sidecar_command::Command::Assign(assignment)),
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(Response::new(HeartbeatResponse {
            acknowledged: true,
            commands,
        }))
    }

    type WatchPipelineAssignmentsStream = ResponseStream;

    async fn watch_pipeline_assignments(
        &self,
        request: Request<WatchAssignmentsRequest>,
    ) -> Result<Response<Self::WatchPipelineAssignmentsStream>, Status> {
        let req = request.into_inner();
        info!("Sidecar {} watching for pipeline assignments", req.sidecar_id);

        let (tx, rx) = tokio::sync::mpsc::channel(16);

        let raft_node = self.raft_node.clone();
        let sidecar_id = req.sidecar_id.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

            loop {
                interval.tick().await;

                let state = {
                    let raft = raft_node.read().await;
                    raft.get_state()
                };

                if let Some(sidecar) = state.sidecars.get(&sidecar_id) {
                    for (pipeline_id, stages) in &sidecar.assigned_pipelines {
                        let assignment = PipelineAssignment {
                            pipeline_id: pipeline_id.clone(),
                            is_local_complete: false,
                            stages: stages
                                .iter()
                                .map(|s| conveyor_etl_proto::sidecar::StageAssignment {
                                    stage_id: s.stage_id.clone(),
                                    target: match &s.target {
                                        SidecarStageTarget::Local { endpoint } => Some(
                                            conveyor_etl_proto::sidecar::stage_assignment::Target::LocalEndpoint(
                                                endpoint.clone(),
                                            ),
                                        ),
                                        SidecarStageTarget::Remote {
                                            sidecar_id,
                                            endpoint,
                                        } => Some(
                                            conveyor_etl_proto::sidecar::stage_assignment::Target::RemoteSidecar(
                                                conveyor_etl_proto::sidecar::RemoteSidecar {
                                                    sidecar_id: sidecar_id.clone(),
                                                    endpoint: endpoint.clone(),
                                                },
                                            ),
                                        ),
                                    },
                                })
                                .collect(),
                        };

                        let event = PipelineAssignmentEvent {
                            event_type: conveyor_etl_proto::sidecar::EventType::Assigned as i32,
                            assignment: Some(assignment),
                        };

                        if tx.send(Ok(event)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::WatchPipelineAssignmentsStream))
    }
}
