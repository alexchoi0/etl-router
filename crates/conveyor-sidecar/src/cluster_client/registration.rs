use anyhow::{Result, Context};
use tonic::transport::Channel;
use tracing::info;

use conveyor_proto::sidecar::{
    sidecar_coordinator_client::SidecarCoordinatorClient,
    RegisterSidecarRequest, LocalService as ProtoLocalService,
    ServiceType as ProtoServiceType,
};

use crate::config::SidecarConfig;
use crate::discovery::{LocalServiceRegistry, ServiceType};
use crate::routing::PipelineRoutes;
use super::conversions::convert_assignment_to_routes;

pub struct ClusterRegistration {
    client: SidecarCoordinatorClient<Channel>,
    config: SidecarConfig,
}

impl ClusterRegistration {
    pub async fn connect(config: SidecarConfig) -> Result<Self> {
        let endpoint = format!("http://{}", config.cluster_endpoint);
        info!("Connecting to cluster at {}", endpoint);

        let channel = Channel::from_shared(endpoint)
            .context("Invalid cluster endpoint")?
            .connect()
            .await
            .context("Failed to connect to cluster")?;

        let client = SidecarCoordinatorClient::new(channel);

        Ok(Self { client, config })
    }

    pub async fn register(
        &mut self,
        registry: &LocalServiceRegistry,
    ) -> Result<Vec<PipelineRoutes>> {
        let local_services: Vec<ProtoLocalService> = registry
            .all_services()
            .map(|svc| ProtoLocalService {
                service_name: svc.name.clone(),
                service_type: match svc.service_type {
                    ServiceType::Source => ProtoServiceType::Source as i32,
                    ServiceType::Transform => ProtoServiceType::Transform as i32,
                    ServiceType::Sink => ProtoServiceType::Sink as i32,
                    ServiceType::Unknown => ProtoServiceType::Unspecified as i32,
                },
                local_endpoint: svc.endpoint.clone(),
            })
            .collect();

        info!(
            "Registering sidecar {} with {} local services",
            self.config.sidecar_id,
            local_services.len()
        );

        let response = self
            .client
            .register_sidecar(RegisterSidecarRequest {
                sidecar_id: self.config.sidecar_id.clone(),
                pod_name: self.config.pod_name.clone(),
                namespace: self.config.namespace.clone(),
                sidecar_endpoint: self.config.sidecar_endpoint(),
                local_services,
            })
            .await
            .context("Failed to register with cluster")?
            .into_inner();

        if !response.success {
            return Err(anyhow::anyhow!(
                "Registration failed: {}",
                response.error
            ));
        }

        info!(
            "Registered successfully, received {} pipeline assignments",
            response.initial_assignments.len()
        );

        let routes = response
            .initial_assignments
            .into_iter()
            .map(convert_assignment_to_routes)
            .collect();

        Ok(routes)
    }

    pub fn client(&mut self) -> &mut SidecarCoordinatorClient<Channel> {
        &mut self.client
    }

    pub fn config(&self) -> &SidecarConfig {
        &self.config
    }
}
