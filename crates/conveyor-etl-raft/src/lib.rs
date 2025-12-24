mod commands;
mod config;
mod log_storage;
mod network;
mod router_state;
mod state_machine;

pub use commands::{
    RouterCommand, SerializableTimestamp, SidecarLocalService, SidecarStageAssignment,
    SidecarStageTarget,
};
pub use config::{ConveyorRaft, NodeId, RouterRequest, RouterResponse, TypeConfig};
pub use log_storage::LogStorage;
pub use network::{Network, NetworkFactory, RaftServer};
pub use router_state::{
    CheckpointState, GroupState, PipelineState, RouterState, ServiceCheckpointState, ServiceState,
    SidecarState, WatermarkState,
};
pub use state_machine::{StateMachine, StoredSnapshot};

pub use openraft::{BasicNode, Config, Raft};

use std::collections::HashMap;

pub struct ServiceCheckpoint {
    pub checkpoint_id: String,
    pub data: Vec<u8>,
    pub source_offsets: HashMap<String, u64>,
    pub created_at: Option<prost_types::Timestamp>,
}
