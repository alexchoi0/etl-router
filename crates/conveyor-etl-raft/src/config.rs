use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

use crate::commands::RouterCommand;

pub type NodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RouterRequest {
    pub command: RouterCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RouterResponse {
    pub success: bool,
    pub error: Option<String>,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = RouterRequest,
        R = RouterResponse,
        Node = BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

pub type ConveyorRaft = openraft::Raft<TypeConfig>;
