use dashmap::DashMap;

use super::NodeId;

#[derive(Debug, Clone)]
pub struct PeerNode {
    pub id: NodeId,
    pub addr: String,
}

pub struct RaftNetwork {
    peers: DashMap<NodeId, PeerNode>,
    #[allow(dead_code)]
    local_id: NodeId,
}

impl RaftNetwork {
    pub fn new(local_id: NodeId) -> Self {
        Self {
            peers: DashMap::new(),
            local_id,
        }
    }

    pub async fn add_peer(&self, id: NodeId, addr: String) {
        self.peers.insert(id, PeerNode { id, addr });
    }

    pub async fn remove_peer(&self, id: NodeId) {
        self.peers.remove(&id);
    }

    pub async fn get_peer(&self, id: NodeId) -> Option<PeerNode> {
        self.peers.get(&id).map(|r| r.clone())
    }

    pub async fn list_peers(&self) -> Vec<PeerNode> {
        self.peers.iter().map(|r| r.clone()).collect()
    }
}

impl Default for RaftNetwork {
    fn default() -> Self {
        Self::new(0)
    }
}
