use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tempfile::TempDir;
use anyhow::{Result, anyhow};
use tracing::{info, warn};

use crate::core::{
    NodeId, RaftCore, RaftConfig, RaftRole,
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
};
use crate::storage::LogStorage;

#[derive(Clone)]
pub struct MockTransport {
    nodes: Arc<RwLock<HashMap<NodeId, Arc<RaftCore>>>>,
    partitions: Arc<RwLock<HashSet<(NodeId, NodeId)>>>,
    message_delay: Duration,
    drop_probability: Arc<RwLock<f64>>,
}

impl MockTransport {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            partitions: Arc::new(RwLock::new(HashSet::new())),
            message_delay: Duration::from_millis(1),
            drop_probability: Arc::new(RwLock::new(0.0)),
        }
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.message_delay = delay;
        self
    }

    pub async fn register_node(&self, id: NodeId, core: Arc<RaftCore>) {
        self.nodes.write().await.insert(id, core);
    }

    pub async fn unregister_node(&self, id: NodeId) {
        self.nodes.write().await.remove(&id);
    }

    pub async fn is_partitioned(&self, from: NodeId, to: NodeId) -> bool {
        let partitions = self.partitions.read().await;
        partitions.contains(&(from, to)) || partitions.contains(&(to, from))
    }

    pub async fn partition(&self, node_a: NodeId, node_b: NodeId) {
        let mut partitions = self.partitions.write().await;
        partitions.insert((node_a, node_b));
        info!(node_a, node_b, "Created network partition");
    }

    pub async fn partition_node(&self, isolated: NodeId, others: &[NodeId]) {
        let mut partitions = self.partitions.write().await;
        for &other in others {
            partitions.insert((isolated, other));
        }
        info!(isolated, "Isolated node from cluster");
    }

    pub async fn heal_partition(&self, node_a: NodeId, node_b: NodeId) {
        let mut partitions = self.partitions.write().await;
        partitions.remove(&(node_a, node_b));
        partitions.remove(&(node_b, node_a));
        info!(node_a, node_b, "Healed network partition");
    }

    pub async fn heal_all_partitions(&self) {
        self.partitions.write().await.clear();
        info!("Healed all network partitions");
    }

    pub async fn set_drop_probability(&self, prob: f64) {
        *self.drop_probability.write().await = prob;
    }

    fn should_drop(&self) -> bool {
        if let Ok(prob) = self.drop_probability.try_read() {
            if *prob > 0.0 {
                return rand::random::<f64>() < *prob;
            }
        }
        false
    }

    pub async fn send_request_vote(
        &self,
        from: NodeId,
        to: NodeId,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse> {
        if self.is_partitioned(from, to).await {
            return Err(anyhow!("Network partition between {} and {}", from, to));
        }

        if self.should_drop() {
            return Err(anyhow!("Message dropped"));
        }

        if !self.message_delay.is_zero() {
            tokio::time::sleep(self.message_delay).await;
        }

        let nodes = self.nodes.read().await;
        let target = nodes.get(&to)
            .ok_or_else(|| anyhow!("Node {} not found", to))?;

        Ok(target.handle_request_vote(req).await)
    }

    pub async fn send_append_entries(
        &self,
        from: NodeId,
        to: NodeId,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        if self.is_partitioned(from, to).await {
            return Err(anyhow!("Network partition between {} and {}", from, to));
        }

        if self.should_drop() {
            return Err(anyhow!("Message dropped"));
        }

        if !self.message_delay.is_zero() {
            tokio::time::sleep(self.message_delay).await;
        }

        let nodes = self.nodes.read().await;
        let target = nodes.get(&to)
            .ok_or_else(|| anyhow!("Node {} not found", to))?;

        Ok(target.handle_append_entries(req).await)
    }

    pub async fn broadcast_request_vote(
        &self,
        from: NodeId,
        peers: &[NodeId],
        req: RequestVoteRequest,
    ) -> Vec<(NodeId, Result<RequestVoteResponse>)> {
        let mut results = Vec::new();
        for &peer in peers {
            let result = self.send_request_vote(from, peer, req.clone()).await;
            results.push((peer, result));
        }
        results
    }

    pub async fn broadcast_append_entries(
        &self,
        from: NodeId,
        peers: &[NodeId],
        build_req: impl Fn(NodeId) -> Option<AppendEntriesRequest>,
    ) -> Vec<(NodeId, Result<AppendEntriesResponse>)> {
        let mut results = Vec::new();
        for &peer in peers {
            if let Some(req) = build_req(peer) {
                let result = self.send_append_entries(from, peer, req).await;
                results.push((peer, result));
            }
        }
        results
    }
}

impl Default for MockTransport {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TestCluster {
    pub nodes: Vec<Arc<RaftCore>>,
    pub transport: MockTransport,
    pub config: RaftConfig,
    _tmp_dirs: Vec<TempDir>,
    shutdown: Arc<RwLock<bool>>,
}

impl TestCluster {
    pub async fn new(size: usize) -> Result<Self> {
        Self::with_config(size, RaftConfig {
            election_timeout_min: Duration::from_millis(50),
            election_timeout_max: Duration::from_millis(100),
            heartbeat_interval: Duration::from_millis(20),
            max_entries_per_append: 100,
        }).await
    }

    pub async fn with_config(size: usize, config: RaftConfig) -> Result<Self> {
        let mut nodes = Vec::with_capacity(size);
        let mut tmp_dirs = Vec::with_capacity(size);
        let transport = MockTransport::new();

        let peer_ids: Vec<NodeId> = (1..=size as NodeId).collect();

        for id in 1..=size as NodeId {
            let tmp_dir = TempDir::new()?;
            let log = Arc::new(LogStorage::new(tmp_dir.path())?);

            let peers: Vec<NodeId> = peer_ids.iter()
                .filter(|&&p| p != id)
                .copied()
                .collect();

            let core = Arc::new(RaftCore::new(id, peers, log, config.clone()).await?);
            transport.register_node(id, core.clone()).await;
            nodes.push(core);
            tmp_dirs.push(tmp_dir);
        }

        Ok(Self {
            nodes,
            transport,
            config,
            _tmp_dirs: tmp_dirs,
            shutdown: Arc::new(RwLock::new(false)),
        })
    }

    pub fn node(&self, id: NodeId) -> Option<&Arc<RaftCore>> {
        self.nodes.iter().find(|n| n.id == id)
    }

    pub async fn leader(&self) -> Option<&Arc<RaftCore>> {
        for node in &self.nodes {
            if node.is_leader().await {
                return Some(node);
            }
        }
        None
    }

    pub async fn leader_id(&self) -> Option<NodeId> {
        self.leader().await.map(|n| n.id)
    }

    pub async fn followers(&self) -> Vec<&Arc<RaftCore>> {
        let mut followers = Vec::new();
        for node in &self.nodes {
            if node.role().await == RaftRole::Follower {
                followers.push(node);
            }
        }
        followers
    }

    pub async fn trigger_election(&self, node_id: NodeId) -> Result<()> {
        let node = self.node(node_id)
            .ok_or_else(|| anyhow!("Node {} not found", node_id))?;

        node.become_candidate().await;

        if node.is_leader().await {
            return Ok(());
        }

        let req = node.build_request_vote_request().await;
        let results = self.transport.broadcast_request_vote(node_id, &node.peers, req).await;

        for (peer, result) in results {
            if let Ok(resp) = result {
                node.handle_request_vote_response(peer, resp).await;
            }
        }

        Ok(())
    }

    pub async fn run_election_round(&self) -> Result<Option<NodeId>> {
        for node in &self.nodes {
            if node.role().await == RaftRole::Follower {
                self.trigger_election(node.id).await?;
                if node.is_leader().await {
                    return Ok(Some(node.id));
                }
            }
        }
        Ok(None)
    }

    pub async fn wait_for_leader(&self, timeout: Duration) -> Option<NodeId> {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            if let Some(leader) = self.leader_id().await {
                return Some(leader);
            }

            if let Ok(Some(leader_id)) = self.run_election_round().await {
                return Some(leader_id);
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        None
    }

    pub async fn replicate(&self) -> Result<()> {
        let leader = self.leader().await
            .ok_or_else(|| anyhow!("No leader"))?;

        let leader_id = leader.id;
        let peers = leader.peers.clone();

        for peer in &peers {
            if let Some(req) = leader.build_append_entries_request(*peer).await {
                if let Ok(resp) = self.transport.send_append_entries(leader_id, *peer, req).await {
                    leader.handle_append_entries_response(*peer, resp).await;
                }
            }
        }

        Ok(())
    }

    pub async fn replicate_until_committed(&self, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        let leader = self.leader().await
            .ok_or_else(|| anyhow!("No leader"))?;

        let target_index = leader.last_log_index().await;

        while start.elapsed() < timeout {
            self.replicate().await?;

            let commit_index = leader.volatile.read().await.commit_index;
            if commit_index >= target_index {
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        Err(anyhow!("Timeout waiting for commit"))
    }

    pub async fn send_heartbeats(&self) -> Result<()> {
        let leader = self.leader().await
            .ok_or_else(|| anyhow!("No leader"))?;

        let leader_id = leader.id;
        let peers = leader.peers.clone();

        for peer in &peers {
            let req = AppendEntriesRequest {
                term: leader.current_term().await,
                leader_id,
                prev_log_index: leader.last_log_index().await,
                prev_log_term: leader.last_log_term().await,
                entries: vec![],
                leader_commit: leader.volatile.read().await.commit_index,
            };

            let _ = self.transport.send_append_entries(leader_id, *peer, req).await;
        }

        Ok(())
    }

    pub async fn partition_leader(&self) -> Result<NodeId> {
        let leader_id = self.leader_id().await
            .ok_or_else(|| anyhow!("No leader"))?;

        let others: Vec<NodeId> = self.nodes.iter()
            .map(|n| n.id)
            .filter(|&id| id != leader_id)
            .collect();

        self.transport.partition_node(leader_id, &others).await;

        Ok(leader_id)
    }

    pub async fn count_leaders(&self) -> usize {
        let mut count = 0;
        for node in &self.nodes {
            if node.is_leader().await {
                count += 1;
            }
        }
        count
    }

    pub async fn all_nodes_agree_on_leader(&self) -> Option<NodeId> {
        let mut seen_leader: Option<NodeId> = None;

        for node in &self.nodes {
            let leader_id = node.leader_id().await;
            match (seen_leader, leader_id) {
                (None, Some(id)) => seen_leader = Some(id),
                (Some(prev), Some(id)) if prev != id => return None,
                _ => {}
            }
        }

        seen_leader
    }

    pub async fn verify_log_consistency(&self) -> bool {
        if self.nodes.is_empty() {
            return true;
        }

        let reference = &self.nodes[0];
        let ref_commit = reference.volatile.read().await.commit_index;

        for node in &self.nodes[1..] {
            let commit = node.volatile.read().await.commit_index;
            let check_up_to = std::cmp::min(ref_commit, commit);

            for i in 1..=check_up_to {
                let ref_entry = reference.log.get(i).await;
                let node_entry = node.log.get(i).await;

                match (ref_entry, node_entry) {
                    (Some(r), Some(n)) => {
                        if r.term != n.term {
                            warn!(
                                index = i,
                                ref_term = r.term,
                                node_term = n.term,
                                node_id = node.id,
                                "Log inconsistency detected"
                            );
                            return false;
                        }
                    }
                    (Some(_), None) | (None, Some(_)) => {
                        warn!(index = i, node_id = node.id, "Missing log entry");
                        return false;
                    }
                    (None, None) => {}
                }
            }
        }

        true
    }

    pub async fn shutdown(&self) {
        *self.shutdown.write().await = true;
        for node in &self.nodes {
            node.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_transport_basic() {
        let transport = MockTransport::new();

        let tmp1 = TempDir::new().unwrap();
        let tmp2 = TempDir::new().unwrap();

        let log1 = Arc::new(LogStorage::new(tmp1.path()).unwrap());
        let log2 = Arc::new(LogStorage::new(tmp2.path()).unwrap());

        let config = RaftConfig::default();

        let node1 = Arc::new(RaftCore::new(1, vec![2], log1, config.clone()).await.unwrap());
        let node2 = Arc::new(RaftCore::new(2, vec![1], log2, config).await.unwrap());

        transport.register_node(1, node1.clone()).await;
        transport.register_node(2, node2.clone()).await;

        node1.become_candidate().await;
        let req = node1.build_request_vote_request().await;

        let resp = transport.send_request_vote(1, 2, req).await.unwrap();
        assert!(resp.vote_granted);
    }

    #[tokio::test]
    async fn test_mock_transport_partition() {
        let transport = MockTransport::new();

        let tmp1 = TempDir::new().unwrap();
        let tmp2 = TempDir::new().unwrap();

        let log1 = Arc::new(LogStorage::new(tmp1.path()).unwrap());
        let log2 = Arc::new(LogStorage::new(tmp2.path()).unwrap());

        let config = RaftConfig::default();

        let node1 = Arc::new(RaftCore::new(1, vec![2], log1, config.clone()).await.unwrap());
        let node2 = Arc::new(RaftCore::new(2, vec![1], log2, config).await.unwrap());

        transport.register_node(1, node1.clone()).await;
        transport.register_node(2, node2.clone()).await;

        transport.partition(1, 2).await;

        node1.become_candidate().await;
        let req = node1.build_request_vote_request().await;

        let result = transport.send_request_vote(1, 2, req).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("partition"));
    }

    #[tokio::test]
    async fn test_cluster_creation() {
        let cluster = TestCluster::new(3).await.unwrap();
        assert_eq!(cluster.nodes.len(), 3);

        for node in &cluster.nodes {
            assert_eq!(node.role().await, RaftRole::Follower);
            assert_eq!(node.peers.len(), 2);
        }
    }

    #[tokio::test]
    async fn test_cluster_leader_election() {
        let cluster = TestCluster::new(3).await.unwrap();

        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await;
        assert!(leader_id.is_some(), "Should elect a leader");

        let leader_count = cluster.count_leaders().await;
        assert_eq!(leader_count, 1, "Should have exactly one leader");
    }
}
