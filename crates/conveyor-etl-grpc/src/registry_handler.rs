use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

use crate::error::{GrpcError, IntoStatus, ResultExt};

use conveyor_etl_proto::common::{Endpoint, HealthStatus, ServiceIdentity};
use conveyor_etl_proto::registry::{
    service_registry_server::ServiceRegistry as ServiceRegistryTrait, DeregisterRequest,
    DeregisterResponse, EndpointInfo, EventType, GetServiceEndpointsRequest,
    GetServiceEndpointsResponse, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest,
    JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, ListServicesRequest,
    ListServicesResponse, RegisterRequest, RegisterResponse,
    RegisteredService as ProtoRegisteredService, ServiceEvent, ServiceHealth as ProtoServiceHealth,
    ServiceMetadata, WatchServicesRequest,
};
use conveyor_etl_raft::{ConveyorRaft, RouterState};
use conveyor_etl_registry::{ServiceRegistry, ServiceType};

pub struct ServiceRegistryImpl {
    #[allow(dead_code)]
    raft: Arc<ConveyorRaft>,
    #[allow(dead_code)]
    state: Arc<RwLock<RouterState>>,
    registry: Arc<RwLock<ServiceRegistry>>,
}

impl ServiceRegistryImpl {
    pub fn new(
        raft: Arc<ConveyorRaft>,
        state: Arc<RwLock<RouterState>>,
        registry: Arc<RwLock<ServiceRegistry>>,
    ) -> Self {
        Self {
            raft,
            state,
            registry,
        }
    }
}

type HeartbeatStream = Pin<Box<dyn Stream<Item = Result<HeartbeatResponse, Status>> + Send>>;
type WatchServicesStream = Pin<Box<dyn Stream<Item = Result<ServiceEvent, Status>> + Send>>;

#[tonic::async_trait]
impl ServiceRegistryTrait for ServiceRegistryImpl {
    type HeartbeatStream = HeartbeatStream;
    type WatchServicesStream = WatchServicesStream;

    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();
        let identity = req
            .identity
            .ok_or_else(|| GrpcError::missing_field("identity"))?;
        let endpoint = req
            .endpoint
            .ok_or_else(|| GrpcError::missing_field("endpoint"))?;

        info!(
            service_id = %identity.service_id,
            service_type = ?identity.service_type,
            "Registering service"
        );

        let endpoint_str = format!("{}:{}", endpoint.host, endpoint.port);
        let service_type = ServiceType::from_proto(identity.service_type);

        let labels: HashMap<String, String> = req.metadata.map(|m| m.labels).unwrap_or_default();

        let group_id = if identity.group_id.is_empty() {
            None
        } else {
            Some(identity.group_id)
        };

        let registry = self.registry.write().await;
        match registry
            .register(
                identity.service_id.clone(),
                identity.name,
                service_type,
                endpoint_str,
                labels,
                group_id,
            )
            .await
        {
            Ok(lease_duration) => Ok(Response::new(RegisterResponse {
                success: true,
                registration_id: identity.service_id,
                lease_id: uuid::Uuid::new_v4().to_string(),
                lease_ttl: Some(prost_types::Duration {
                    seconds: lease_duration.as_secs() as i64,
                    nanos: 0,
                }),
                initial_credits: 10000,
            })),
            Err(e) => Err(e.into_status()),
        }
    }

    async fn deregister(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<DeregisterResponse>, Status> {
        let req = request.into_inner();

        info!(service_id = %req.service_id, "Deregistering service");

        let registry = self.registry.write().await;
        registry.deregister(&req.service_id).await.map_to_status()?;
        Ok(Response::new(DeregisterResponse {
            success: true,
            pending_records: 0,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<Streaming<HeartbeatRequest>>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        let mut stream = request.into_inner();
        let registry = self.registry.clone();

        let output = async_stream::try_stream! {
            while let Some(req) = stream.message().await? {
                let registry = registry.write().await;
                match registry.heartbeat(&req.service_id).await {
                    Ok(lease_duration) => {
                        yield HeartbeatResponse {
                            acknowledged: true,
                            next_heartbeat_deadline: Some(prost_types::Duration {
                                seconds: lease_duration.as_secs() as i64,
                                nanos: 0,
                            }),
                            commands: Vec::new(),
                        };
                    }
                    Err(_) => {
                        yield HeartbeatResponse {
                            acknowledged: false,
                            next_heartbeat_deadline: None,
                            commands: Vec::new(),
                        };
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(output) as Self::HeartbeatStream))
    }

    async fn list_services(
        &self,
        _request: Request<ListServicesRequest>,
    ) -> Result<Response<ListServicesResponse>, Status> {
        let registry = self.registry.read().await;
        let services = registry.list_all().await;

        let service_infos: Vec<ProtoRegisteredService> = services
            .iter()
            .map(|s| ProtoRegisteredService {
                identity: Some(ServiceIdentity {
                    service_id: s.service_id.clone(),
                    service_type: s.service_type.to_proto(),
                    name: s.service_name.clone(),
                    version: String::new(),
                    capabilities: Vec::new(),
                    group_id: s.group_id.clone().unwrap_or_default(),
                }),
                endpoint: Some(Endpoint {
                    host: s.endpoint.split(':').next().unwrap_or("").to_string(),
                    port: s
                        .endpoint
                        .split(':')
                        .nth(1)
                        .and_then(|p| p.parse().ok())
                        .unwrap_or(0),
                    use_tls: false,
                }),
                metadata: Some(ServiceMetadata {
                    labels: s.labels.clone(),
                    record_types_handled: Vec::new(),
                    max_concurrent_requests: 100,
                    weight: 100,
                }),
                health: Some(ProtoServiceHealth {
                    status: HealthStatus::Healthy as i32,
                    message: String::new(),
                    components: HashMap::new(),
                }),
                registered_at: None,
                last_heartbeat: None,
                assigned_partitions: Vec::new(),
            })
            .collect();

        Ok(Response::new(ListServicesResponse {
            services: service_infos,
        }))
    }

    async fn watch_services(
        &self,
        _request: Request<WatchServicesRequest>,
    ) -> Result<Response<Self::WatchServicesStream>, Status> {
        let output = async_stream::try_stream! {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                yield ServiceEvent {
                    event_type: EventType::Added as i32,
                    service: None,
                };
            }
        };

        Ok(Response::new(Box::pin(output) as Self::WatchServicesStream))
    }

    async fn get_service_endpoints(
        &self,
        request: Request<GetServiceEndpointsRequest>,
    ) -> Result<Response<GetServiceEndpointsResponse>, Status> {
        let req = request.into_inner();
        let registry = self.registry.read().await;

        let services = registry.get_services_by_name(&req.service_name).await;

        let endpoints: Vec<EndpointInfo> = services
            .iter()
            .map(|s| EndpointInfo {
                service_id: s.service_id.clone(),
                endpoint: Some(Endpoint {
                    host: s.endpoint.split(':').next().unwrap_or("").to_string(),
                    port: s
                        .endpoint
                        .split(':')
                        .nth(1)
                        .and_then(|p| p.parse().ok())
                        .unwrap_or(0),
                    use_tls: false,
                }),
                weight: 100,
                health: HealthStatus::Healthy as i32,
                assigned_partitions: Vec::new(),
            })
            .collect();

        Ok(Response::new(GetServiceEndpointsResponse { endpoints }))
    }

    async fn join_group(
        &self,
        request: Request<JoinGroupRequest>,
    ) -> Result<Response<JoinGroupResponse>, Status> {
        let req = request.into_inner();

        info!(
            service_id = %req.service_id,
            group_id = %req.group_id,
            stage_id = %req.stage_id,
            "Service joining group"
        );

        Ok(Response::new(JoinGroupResponse {
            success: true,
            generation: 1,
            assigned_partitions: vec![0],
        }))
    }

    async fn leave_group(
        &self,
        request: Request<LeaveGroupRequest>,
    ) -> Result<Response<LeaveGroupResponse>, Status> {
        let req = request.into_inner();

        info!(
            service_id = %req.service_id,
            group_id = %req.group_id,
            "Service leaving group"
        );

        Ok(Response::new(LeaveGroupResponse { success: true }))
    }
}
