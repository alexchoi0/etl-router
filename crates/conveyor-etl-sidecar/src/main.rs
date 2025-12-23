use std::sync::Arc;
use std::time::Duration;
use anyhow::{Result, Context};
use tokio::sync::RwLock;
use tonic::transport::Server;
use tracing::{info, error, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use conveyor_etl_proto::sidecar::sidecar_data_plane_server::SidecarDataPlaneServer;

use conveyor_etl_sidecar::config::SidecarConfig;
use conveyor_etl_sidecar::discovery::GrpcReflectionDiscovery;
use conveyor_etl_sidecar::routing::{RoutingTable, LocalRouter, RemoteRouter};
use conveyor_etl_sidecar::cluster_client::{ClusterRegistration, HeartbeatLoop};
use conveyor_etl_sidecar::SidecarDataPlaneImpl;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "conveyor_etl_sidecar=info,tower_http=debug".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting ETL Sidecar...");

    let config = SidecarConfig::from_env()
        .context("Failed to load configuration from environment")?;

    info!(
        sidecar_id = %config.sidecar_id,
        pod = %config.pod_name,
        namespace = %config.namespace,
        "Configuration loaded"
    );

    info!("Discovering local services...");
    let discovery = GrpcReflectionDiscovery::new(config.local_ports.clone());

    let registry = discovery
        .discover()
        .await
        .context("Failed to discover local services")?;

    info!(
        "Discovered {} local services",
        registry.all_services().count()
    );

    for service in registry.all_services() {
        info!(
            name = %service.name,
            service_type = ?service.service_type,
            endpoint = %service.endpoint,
            "Found local service"
        );
    }

    let registry = Arc::new(RwLock::new(registry));

    info!("Connecting to cluster at {}...", config.cluster_endpoint);
    let mut cluster_registration = ClusterRegistration::connect(config.clone())
        .await
        .context("Failed to connect to cluster")?;

    let initial_routes = {
        let reg = registry.read().await;
        cluster_registration
            .register(&reg)
            .await
            .context("Failed to register with cluster")?
    };

    info!(
        "Registered with cluster, received {} initial pipeline routes",
        initial_routes.len()
    );

    let routing_table = Arc::new(RwLock::new(RoutingTable::new()));

    {
        let mut table = routing_table.write().await;
        for routes in initial_routes {
            info!(
                pipeline = %routes.pipeline_id,
                stages = routes.stages.len(),
                local_complete = routes.is_local_complete,
                "Loaded pipeline routes"
            );
            table.set_pipeline_routes(routes);
        }
    }

    let local_router = Arc::new(LocalRouter::new());
    let remote_router = Arc::new(RemoteRouter::new());

    let heartbeat_loop = HeartbeatLoop::new(
        cluster_registration.client().clone(),
        config.sidecar_id.clone(),
        routing_table.clone(),
        Duration::from_secs(10),
    );

    let heartbeat_registry = registry.clone();
    let heartbeat_handle = tokio::spawn(async move {
        if let Err(e) = heartbeat_loop.run(heartbeat_registry).await {
            error!("Heartbeat loop failed: {}", e);
        }
    });

    let data_plane = SidecarDataPlaneImpl::new(
        routing_table.clone(),
        local_router.clone(),
        remote_router.clone(),
        config.sidecar_id.clone(),
    );

    info!("Starting SidecarDataPlane gRPC server on {}...", config.listen_addr);

    let server = Server::builder()
        .add_service(SidecarDataPlaneServer::new(data_plane))
        .serve(config.listen_addr);

    tokio::select! {
        result = server => {
            if let Err(e) = result {
                error!("gRPC server error: {}", e);
            }
        }
        _ = heartbeat_handle => {
            warn!("Heartbeat loop stopped");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal");
        }
    }

    info!("ETL Sidecar shutting down");
    Ok(())
}
