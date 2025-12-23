use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use anyhow::Result;
use tracing::{info, warn, debug, error};

use conveyor_etl_proto::sidecar::{
    sidecar_coordinator_client::SidecarCoordinatorClient,
    SidecarHeartbeatRequest, LocalServiceHealth,
    sidecar_command,
};
use tonic::transport::Channel;

use crate::discovery::LocalServiceRegistry;
use crate::routing::SharedRoutingTable;
use super::conversions::convert_assignment_to_routes;

pub struct HeartbeatLoop {
    client: SidecarCoordinatorClient<Channel>,
    sidecar_id: String,
    routing_table: SharedRoutingTable,
    interval: Duration,
}

impl HeartbeatLoop {
    pub fn new(
        client: SidecarCoordinatorClient<Channel>,
        sidecar_id: String,
        routing_table: SharedRoutingTable,
        interval: Duration,
    ) -> Self {
        Self {
            client,
            sidecar_id,
            routing_table,
            interval,
        }
    }

    pub async fn run(
        mut self,
        registry: Arc<RwLock<LocalServiceRegistry>>,
    ) -> Result<()> {
        info!(
            "Starting heartbeat loop for sidecar {}, interval: {:?}",
            self.sidecar_id, self.interval
        );

        let mut interval = interval(self.interval);

        loop {
            interval.tick().await;

            let service_health = {
                let reg = registry.read().await;
                reg.all_services()
                    .map(|svc| LocalServiceHealth {
                        service_name: svc.name.clone(),
                        healthy: true,
                        active_requests: 0,
                    })
                    .collect()
            };

            let request = SidecarHeartbeatRequest {
                sidecar_id: self.sidecar_id.clone(),
                service_health,
                load: None,
            };

            match self.client.heartbeat(request).await {
                Ok(response) => {
                    let resp = response.into_inner();

                    if !resp.acknowledged {
                        warn!("Heartbeat not acknowledged");
                        continue;
                    }

                    for command in resp.commands {
                        self.handle_command(command).await;
                    }

                    debug!("Heartbeat successful");
                }
                Err(e) => {
                    error!("Heartbeat failed: {}", e);
                }
            }
        }
    }

    async fn handle_command(&self, command: conveyor_etl_proto::sidecar::SidecarCommand) {
        let cmd = match command.command {
            Some(c) => c,
            None => return,
        };

        match cmd {
            sidecar_command::Command::Assign(assignment) => {
                info!("Received pipeline assignment: {}", assignment.pipeline_id);
                let routes = convert_assignment_to_routes(assignment);
                let mut table = self.routing_table.write().await;
                table.set_pipeline_routes(routes);
            }
            sidecar_command::Command::Revoke(revocation) => {
                info!("Received pipeline revocation: {}", revocation.pipeline_id);
                let mut table = self.routing_table.write().await;
                table.remove_pipeline(&revocation.pipeline_id);
            }
            sidecar_command::Command::Drain(_drain) => {
                info!("Received drain command");
            }
        }
    }
}
