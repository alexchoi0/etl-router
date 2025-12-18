use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::NodeId;

#[derive(Debug, Clone)]
pub struct PeerNode {
    pub id: NodeId,
    pub addr: String,
}

pub struct RaftNetwork {
    peers: Arc<RwLock<HashMap<NodeId, PeerNode>>>,
    #[allow(dead_code)]
    local_id: NodeId,
}

impl RaftNetwork {
    pub fn new(local_id: NodeId) -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
            local_id,
        }
    }

    pub async fn add_peer(&self, id: NodeId, addr: String) {
        let mut peers = self.peers.write().await;
        peers.insert(id, PeerNode { id, addr });
    }

    pub async fn remove_peer(&self, id: NodeId) {
        let mut peers = self.peers.write().await;
        peers.remove(&id);
    }

    pub async fn get_peer(&self, id: NodeId) -> Option<PeerNode> {
        let peers = self.peers.read().await;
        peers.get(&id).cloned()
    }

    pub async fn list_peers(&self) -> Vec<PeerNode> {
        let peers = self.peers.read().await;
        peers.values().cloned().collect()
    }
}

impl Default for RaftNetwork {
    fn default() -> Self {
        Self::new(0)
    }
}
