use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use openraft::{BasicNode, Config, Raft};
use tokio::sync::RwLock;
use tonic::transport::Server;
use tracing::{error, info};

use conveyor_etl_buffer::BufferManager;
use conveyor_etl_config::Settings;
use conveyor_etl_raft::{
    ConveyorRaft, LogStorage, NetworkFactory, NodeId, RaftServer, RouterState, StateMachine,
    TypeConfig,
};
use conveyor_etl_registry::ServiceRegistry;
use conveyor_etl_routing::RoutingEngine;

use conveyor_etl_proto::checkpoint::checkpoint_service_server::CheckpointServiceServer;
use conveyor_etl_proto::registry::service_registry_server::ServiceRegistryServer;
use conveyor_etl_proto::sidecar::sidecar_coordinator_server::SidecarCoordinatorServer;
use conveyor_etl_proto::source::source_router_server::SourceRouterServer;

use super::checkpoint_handler::CheckpointServiceImpl;
use super::registry_handler::ServiceRegistryImpl;
use super::sidecar_handler::SidecarCoordinatorImpl;
use super::source_handler::SourceRouterImpl;

pub struct RouterServer {
    node_id: u64,
    listen_addr: SocketAddr,
    raft_addr: SocketAddr,
    peers: Vec<String>,
    data_dir: String,
    settings: Settings,
}

impl RouterServer {
    pub async fn new(
        node_id: u64,
        listen_addr: SocketAddr,
        raft_addr: SocketAddr,
        peers: Vec<String>,
        data_dir: String,
        settings: Settings,
    ) -> Result<Self> {
        Ok(Self {
            node_id,
            listen_addr,
            raft_addr,
            peers,
            data_dir,
            settings,
        })
    }

    fn parse_peers(peers: &[String]) -> BTreeMap<NodeId, BasicNode> {
        peers
            .iter()
            .filter_map(|peer| {
                let parts: Vec<&str> = peer.splitn(2, ':').collect();
                if parts.len() == 2 {
                    if let Ok(id) = parts[0].parse::<NodeId>() {
                        Some((id, BasicNode::new(parts[1].to_string())))
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

        let config = Arc::new(
            Config {
                cluster_name: "conveyor-etl".into(),
                heartbeat_interval: self.settings.cluster.heartbeat_interval_ms as u64,
                election_timeout_min: self.settings.cluster.election_timeout_min_ms as u64,
                election_timeout_max: self.settings.cluster.election_timeout_max_ms as u64,
                ..Default::default()
            }
            .validate()?,
        );

        let log_storage = LogStorage::new(&self.data_dir)?;

        let router_state = Arc::new(RwLock::new(RouterState::default()));
        let state_machine = StateMachine::new(router_state.clone());

        let network = NetworkFactory::new();

        let raft: ConveyorRaft =
            Raft::new(self.node_id, config, network, log_storage, state_machine).await?;

        let raft = Arc::new(raft);

        let parsed_peers = Self::parse_peers(&self.peers);
        let is_bootstrap = parsed_peers.is_empty()
            || (parsed_peers.len() == 1 && parsed_peers.contains_key(&self.node_id));

        if is_bootstrap {
            info!("Bootstrapping new cluster as node {}", self.node_id);
            let mut members = BTreeMap::new();
            members.insert(
                self.node_id,
                BasicNode::new(self.raft_addr.to_string()),
            );
            raft.initialize(members).await?;
        }

        let service_registry = Arc::new(RwLock::new(ServiceRegistry::new(
            raft.clone(),
            router_state.clone(),
        )));

        let buffer_manager = Arc::new(RwLock::new(BufferManager::new(
            self.settings.buffer.clone(),
        )));

        let routing_engine = Arc::new(RwLock::new(RoutingEngine::new()));

        let source_router = SourceRouterImpl::new(
            raft.clone(),
            router_state.clone(),
            buffer_manager.clone(),
            routing_engine.clone(),
        );

        let registry_service =
            ServiceRegistryImpl::new(raft.clone(), router_state.clone(), service_registry.clone());

        let checkpoint_service = CheckpointServiceImpl::new(raft.clone(), router_state.clone());

        let sidecar_coordinator = SidecarCoordinatorImpl::new(raft.clone(), router_state.clone());

        info!("Starting Raft gRPC server on {}...", self.raft_addr);
        let raft_addr = self.raft_addr;
        let raft_for_server = raft.clone();
        let raft_server = tokio::spawn(async move {
            let raft_service = RaftServer::new(raft_for_server);
            if let Err(e) = Server::builder()
                .add_service(raft_service.into_service())
                .serve(raft_addr)
                .await
            {
                error!("Raft gRPC server error: {}", e);
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
        }

        Ok(())
    }
}
