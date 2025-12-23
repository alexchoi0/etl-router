use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, timeout};
use rand::Rng;
use anyhow::{Result, anyhow};
use tracing::{debug, info};

use super::core::{NodeId, Term, LogIndex, RaftCore, RaftConfig, RaftRole};
use super::storage::LogStorage;
use super::transport::RaftTransport;
use super::commands::RouterCommand;
use super::ServiceCheckpoint;
use conveyor_etl_config::ClusterSettings;
use conveyor_etl_proto::common::Watermark;
use conveyor_etl_proto::checkpoint::{PartitionOffsets, GetCheckpointResponse};

pub struct RaftNode {
    pub id: NodeId,
    core: Arc<RaftCore>,
    transport: Arc<RaftTransport>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl RaftNode {
    pub async fn new(
        id: NodeId,
        data_dir: &str,
        _raft_addr: SocketAddr,
        peers: Vec<(NodeId, String)>,
        config: ClusterSettings,
    ) -> Result<Self> {
        info!(node_id = id, "Initializing Raft node");

        let log_path = format!("{}/raft-log-{}", data_dir, id);
        let log = Arc::new(LogStorage::new(&log_path)?);

        let peer_ids: Vec<NodeId> = peers.iter().map(|(id, _)| *id).collect();
        let peer_addrs: HashMap<NodeId, String> = peers.into_iter().collect();

        let raft_config = RaftConfig {
            election_timeout_min: Duration::from_millis(config.election_timeout_min_ms),
            election_timeout_max: Duration::from_millis(config.election_timeout_max_ms),
            heartbeat_interval: Duration::from_millis(config.heartbeat_interval_ms),
            max_entries_per_append: config.max_entries_per_append as usize,
        };

        let core = Arc::new(RaftCore::new(id, peer_ids, log, raft_config).await?);
        let transport = Arc::new(RaftTransport::new(core.clone(), peer_addrs));

        Ok(Self {
            id,
            core,
            transport,
            shutdown_tx: None,
        })
    }

    #[doc(hidden)]
    pub fn from_core(id: NodeId, core: Arc<RaftCore>) -> Self {
        let transport = Arc::new(RaftTransport::new(core.clone(), HashMap::new()));
        Self {
            id,
            core,
            transport,
            shutdown_tx: None,
        }
    }

    pub fn transport(&self) -> Arc<RaftTransport> {
        self.transport.clone()
    }

    pub async fn start(&mut self) -> Result<()> {
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        let core = self.core.clone();
        let transport = self.transport.clone();

        tokio::spawn(async move {
            Self::run_loop(core, transport, shutdown_rx).await;
        });

        info!(node = self.id, "Raft node started");
        Ok(())
    }

    async fn run_loop(
        core: Arc<RaftCore>,
        transport: Arc<RaftTransport>,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) {
        let mut heartbeat_interval = interval(core.config.heartbeat_interval);
        let mut election_check_interval = interval(Duration::from_millis(10));

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!(node = core.id, "Raft node shutting down");
                    break;
                }
                _ = heartbeat_interval.tick() => {
                    if core.is_leader().await {
                        Self::send_heartbeats(&core, &transport).await;
                    }
                }
                _ = election_check_interval.tick() => {
                    Self::check_election_timeout(&core, &transport).await;
                }
            }
        }
    }

    async fn check_election_timeout(core: &Arc<RaftCore>, transport: &Arc<RaftTransport>) {
        let role = core.role().await;

        if role == RaftRole::Leader {
            return;
        }

        let election_timeout = Self::random_election_timeout(&core.config);

        if core.should_start_election(election_timeout) {
            info!(node = core.id, "Election timeout, starting election");
            Self::start_election(core, transport).await;
        }
    }

    fn random_election_timeout(config: &RaftConfig) -> Duration {
        let mut rng = rand::thread_rng();
        let min = config.election_timeout_min.as_millis() as u64;
        let max = config.election_timeout_max.as_millis() as u64;
        Duration::from_millis(rng.gen_range(min..=max))
    }

    async fn start_election(core: &Arc<RaftCore>, transport: &Arc<RaftTransport>) {
        core.become_candidate().await;

        let req = core.build_request_vote_request().await;
        let peers = core.peers.clone();

        for peer in peers {
            let transport = transport.clone();
            let core = core.clone();
            let req = req.clone();

            tokio::spawn(async move {
                match timeout(
                    Duration::from_millis(100),
                    transport.send_request_vote(peer, req),
                ).await {
                    Ok(Ok(resp)) => {
                        core.handle_request_vote_response(peer, resp).await;
                    }
                    Ok(Err(e)) => {
                        debug!(node = core.id, peer, error = ?e, "RequestVote failed");
                        transport.clear_client(peer).await;
                    }
                    Err(_) => {
                        debug!(node = core.id, peer, "RequestVote timed out");
                    }
                }
            });
        }
    }

    async fn send_heartbeats(core: &Arc<RaftCore>, transport: &Arc<RaftTransport>) {
        let peers = core.peers.clone();

        for peer in peers {
            let transport = transport.clone();
            let core = core.clone();

            tokio::spawn(async move {
                if let Some(req) = core.build_append_entries_request(peer).await {
                    match timeout(
                        Duration::from_millis(100),
                        transport.send_append_entries(peer, req),
                    ).await {
                        Ok(Ok(resp)) => {
                            core.handle_append_entries_response(peer, resp).await;
                        }
                        Ok(Err(e)) => {
                            debug!(node = core.id, peer, error = ?e, "AppendEntries failed");
                            transport.clear_client(peer).await;
                        }
                        Err(_) => {
                            debug!(node = core.id, peer, "AppendEntries timed out");
                        }
                    }
                }
            });
        }
    }

    pub async fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }
        self.core.shutdown().await;
    }

    pub async fn is_leader(&self) -> bool {
        self.core.is_leader().await
    }

    pub async fn leader_id(&self) -> Option<NodeId> {
        self.core.leader_id().await
    }

    pub async fn current_term(&self) -> Term {
        self.core.current_term().await
    }

    pub async fn propose(&self, command: RouterCommand) -> Result<LogIndex> {
        if !self.is_leader().await {
            return Err(anyhow!("Not the leader"));
        }
        self.core.propose(command).await
    }

    pub async fn propose_and_wait(&self, command: RouterCommand) -> Result<()> {
        let index = self.propose(command).await?;

        let timeout_duration = Duration::from_secs(5);
        let start = std::time::Instant::now();

        loop {
            let commit_index = self.core.volatile.read().await.commit_index;
            if commit_index >= index {
                return Ok(());
            }

            if start.elapsed() > timeout_duration {
                return Err(anyhow!("Timed out waiting for commit"));
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn commit_source_offset(
        &self,
        source_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        self.propose_and_wait(RouterCommand::CommitSourceOffset {
            source_id: source_id.to_string(),
            partition,
            offset,
        }).await
    }

    pub fn get_source_offsets(&self, source_id: &str, _partition: u32) -> HashMap<u32, u64> {
        if let Ok(state) = self.core.state_machine.try_read() {
            state.get_source_offsets(source_id)
        } else {
            HashMap::new()
        }
    }

    pub fn get_state(&self) -> super::state_machine::RouterState {
        if let Ok(state) = self.core.state_machine.try_read() {
            state.clone()
        } else {
            super::state_machine::RouterState::default()
        }
    }

    pub async fn advance_watermark(&self, watermark: &Watermark) -> Result<Watermark> {
        self.propose_and_wait(RouterCommand::AdvanceWatermark {
            source_id: watermark.source_id.clone(),
            partition: watermark.partition,
            position: watermark.position,
            event_time: watermark.timestamp.clone().map(|ts| ts.into()),
        }).await?;

        Ok(watermark.clone())
    }

    pub fn get_watermarks(&self, _source_id: &str, _partition: u32) -> (Vec<Watermark>, Watermark) {
        (Vec::new(), Watermark::default())
    }

    pub async fn save_service_checkpoint(
        &self,
        service_id: &str,
        checkpoint_id: &str,
        data: &[u8],
        source_offsets: &HashMap<String, u64>,
    ) -> Result<prost_types::Timestamp> {
        self.propose_and_wait(RouterCommand::SaveServiceCheckpoint {
            service_id: service_id.to_string(),
            checkpoint_id: checkpoint_id.to_string(),
            data: data.to_vec(),
            source_offsets: source_offsets.clone(),
        }).await?;

        Ok(prost_types::Timestamp {
            seconds: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            nanos: 0,
        })
    }

    pub fn get_service_checkpoint(&self, service_id: &str) -> Option<ServiceCheckpoint> {
        if let Ok(state) = self.core.state_machine.try_read() {
            state.checkpoints.service_checkpoints.get(service_id).map(|cp| {
                ServiceCheckpoint {
                    checkpoint_id: cp.checkpoint_id.clone(),
                    data: cp.data.clone(),
                    source_offsets: cp.source_offsets.clone(),
                    created_at: Some(prost_types::Timestamp {
                        seconds: cp.created_at as i64,
                        nanos: 0,
                    }),
                }
            })
        } else {
            None
        }
    }

    pub fn get_group_offsets(
        &self,
        group_id: &str,
        source_id: Option<&str>,
    ) -> HashMap<String, PartitionOffsets> {
        let mut result = HashMap::new();

        if let Ok(state) = self.core.state_machine.try_read() {
            if let Some(group_offsets) = state.checkpoints.group_offsets.get(group_id) {
                for (sid, partitions) in group_offsets {
                    if source_id.is_none() || source_id == Some(sid.as_str()) {
                        let offsets: HashMap<u32, u64> = partitions.iter()
                            .map(|(&partition, &offset)| (partition, offset))
                            .collect();
                        result.insert(sid.clone(), PartitionOffsets { offsets });
                    }
                }
            }
        }

        result
    }

    pub async fn commit_group_offset(
        &self,
        group_id: &str,
        source_id: &str,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        self.propose_and_wait(RouterCommand::CommitGroupOffset {
            group_id: group_id.to_string(),
            source_id: source_id.to_string(),
            partition,
            offset,
        }).await
    }

    pub fn get_pipeline_checkpoints(
        &self,
        _pipeline_id: &str,
    ) -> (
        HashMap<String, GetCheckpointResponse>,
        HashMap<String, PartitionOffsets>,
        Vec<Watermark>,
    ) {
        (
            HashMap::new(),
            HashMap::new(),
            Vec::new(),
        )
    }
}
