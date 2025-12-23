mod state_machine;
mod storage;
mod network;
mod commands;
mod core;
mod transport;
mod node;
mod backup_service;
#[cfg(test)]
mod tests;
#[cfg(test)]
pub mod testing;

pub use state_machine::{RouterStateMachine, RouterState};
pub use storage::LogStorage;
pub use network::RaftNetwork;
pub use commands::{RouterCommand, SerializableTimestamp, SidecarLocalService, SidecarStageAssignment, SidecarStageTarget};
pub use core::{NodeId, Term, LogIndex, RaftCore, RaftConfig, RaftRole};
pub use transport::{RaftTransport, RaftTransportService};
pub use node::RaftNode;
pub use backup_service::BackupServiceImpl;

use std::collections::HashMap;

pub struct ServiceCheckpoint {
    pub checkpoint_id: String,
    pub data: Vec<u8>,
    pub source_offsets: HashMap<String, u64>,
    pub created_at: Option<prost_types::Timestamp>,
}
