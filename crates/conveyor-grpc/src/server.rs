use std::net::SocketAddr;
use std::sync::Arc;
use anyhow::Result;
use tokio::sync::RwLock;
use tonic::transport::Server;
use tracing::{info, error};

use conveyor_config::Settings;
use conveyor_raft::{RaftNode, NodeId};
use conveyor_registry::ServiceRegistry;
use conveyor_buffer::BufferManager;
use conveyor_routing::RoutingEngine;
use conveyor_graphql::GraphQLServer;

use conveyor_proto::source::source_router_server::SourceRouterServer;
use conveyor_proto::registry::service_registry_server::ServiceRegistryServer;
use conveyor_proto::checkpoint::checkpoint_service_server::CheckpointServiceServer;
use conveyor_proto::sidecar::sidecar_coordinator_server::SidecarCoordinatorServer;

use super::source_handler::SourceRouterImpl;
use super::registry_handler::ServiceRegistryImpl;
use super::checkpoint_handler::CheckpointServiceImpl;
use super::sidecar_handler::SidecarCoordinatorImpl;

pub struct RouterServer {
    node_id: u64,
    listen_addr: SocketAddr,
    raft_addr: SocketAddr,
    graphql_addr: SocketAddr,
    peers: Vec<String>,
    data_dir: String,
    settings: Settings,
}

impl RouterServer {
    pub async fn new(
        node_id: u64,
        listen_addr: SocketAddr,
        raft_addr: SocketAddr,
        graphql_addr: SocketAddr,
        peers: Vec<String>,
        data_dir: String,
        settings: Settings,
    ) -> Result<Self> {
        Ok(Self {
            node_id,
            listen_addr,
            raft_addr,
            graphql_addr,
            peers,
            data_dir,
            settings,
        })
    }

    fn parse_peers(peers: &[String]) -> Vec<(NodeId, String)> {
        peers.iter()
            .filter_map(|peer| {
                let parts: Vec<&str> = peer.splitn(2, ':').collect();
                if parts.len() == 2 {
                    if let Ok(id) = parts[0].parse::<NodeId>() {
                        Some((id, parts[1].to_string()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    pub async fn run(self) -> Result<()> {
        info!("Initializing Raft node {}...", self.node_id);

        let parsed_peers = Self::parse_peers(&self.peers);

        let mut raft_node = RaftNode::new(
            self.node_id,
            &self.data_dir,
            self.raft_addr,
            parsed_peers,
            self.settings.cluster.clone(),
        ).await?;

        let raft_transport = raft_node.transport();

        raft_node.start().await?;

        let raft_node = Arc::new(RwLock::new(raft_node));

        let service_registry = Arc::new(RwLock::new(
            ServiceRegistry::new(raft_node.clone())
        ));

        let buffer_manager = Arc::new(RwLock::new(
            BufferManager::new(self.settings.buffer.clone())
        ));

        let routing_engine = Arc::new(RwLock::new(
            RoutingEngine::new()
        ));

        let source_router = SourceRouterImpl::new(
            raft_node.clone(),
            buffer_manager.clone(),
            routing_engine.clone(),
        );

        let registry_service = ServiceRegistryImpl::new(
            raft_node.clone(),
            service_registry.clone(),
        );

        let checkpoint_service = CheckpointServiceImpl::new(
            raft_node.clone(),
        );

        let sidecar_coordinator = SidecarCoordinatorImpl::new(
            raft_node.clone(),
        );

        info!("Starting Raft gRPC server on {}...", self.raft_addr);
        let raft_addr = self.raft_addr;
        let raft_server = tokio::spawn(async move {
            if let Err(e) = Server::builder()
                .add_service(raft_transport.into_server())
                .serve(raft_addr)
                .await
            {
                error!("Raft gRPC server error: {}", e);
            }
        });

        let graphql_server = GraphQLServer::new(self.graphql_addr, raft_node.clone());
        let graphql_handle = tokio::spawn(async move {
            if let Err(e) = graphql_server.run().await {
                error!("GraphQL server error: {}", e);
            }
        });

        info!("Starting main gRPC server on {}...", self.listen_addr);

        let main_server = Server::builder()
            .add_service(SourceRouterServer::new(source_router))
            .add_service(ServiceRegistryServer::new(registry_service))
            .add_service(CheckpointServiceServer::new(checkpoint_service))
            .add_service(SidecarCoordinatorServer::new(sidecar_coordinator))
            .serve(self.listen_addr);

        tokio::select! {
            result = main_server => {
                if let Err(e) = result {
                    error!("Main gRPC server error: {}", e);
                }
            }
            _ = raft_server => {
                info!("Raft server stopped");
            }
            _ = graphql_handle => {
                info!("GraphQL server stopped");
            }
        }

        Ok(())
    }
}
