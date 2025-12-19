mod types;
mod messages;
mod election;
mod replication;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, watch};
use anyhow::{Result, anyhow};
use tracing::{info, debug, error};

use crate::storage::LogStorage;
use crate::commands::RouterCommand;
use crate::state_machine::RouterState;

pub use types::{NodeId, Term, RaftRole, PersistentState, VolatileState, LeaderState, RaftConfig};
pub use messages::{RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse, LogEntry};

pub type LogIndex = u64;

pub struct RaftCore {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub config: RaftConfig,

    pub role: RwLock<RaftRole>,
    pub persistent: RwLock<PersistentState>,
    pub volatile: RwLock<VolatileState>,
    pub leader_state: RwLock<Option<LeaderState>>,
    pub current_leader: RwLock<Option<NodeId>>,

    pub log: Arc<LogStorage>,
    pub state_machine: RwLock<RouterState>,

    pub last_heartbeat: RwLock<Instant>,
    pub votes_received: RwLock<HashSet<NodeId>>,

    pub shutdown_tx: watch::Sender<bool>,
    pub shutdown_rx: watch::Receiver<bool>,
}

impl RaftCore {
    pub async fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        log: Arc<LogStorage>,
        config: RaftConfig,
    ) -> Result<Self> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let persistent = Self::load_persistent_state(&log).await?;
        let commit_index = log.commit_index();

        info!(
            node = id,
            term = persistent.current_term,
            voted_for = ?persistent.voted_for,
            commit_index,
            last_index = log.last_index(),
            "Loading Raft state from storage"
        );

        let core = Self {
            id,
            peers,
            config,
            role: RwLock::new(RaftRole::Follower),
            persistent: RwLock::new(persistent),
            volatile: RwLock::new(VolatileState {
                commit_index,
                last_applied: 0,
            }),
            leader_state: RwLock::new(None),
            current_leader: RwLock::new(None),
            log,
            state_machine: RwLock::new(RouterState::default()),
            last_heartbeat: RwLock::new(Instant::now()),
            votes_received: RwLock::new(HashSet::new()),
            shutdown_tx,
            shutdown_rx,
        };

        core.replay_committed_entries().await?;

        Ok(core)
    }

    async fn replay_committed_entries(&self) -> Result<()> {
        let commit_index = self.volatile.read().await.commit_index;

        if commit_index == 0 {
            return Ok(());
        }

        info!(node = self.id, commit_index, "Replaying committed log entries");

        let entries = self.log.get_range(1, commit_index).await;
        let mut state_machine = self.state_machine.write().await;

        for entry in entries {
            if let Err(e) = state_machine.apply_command(entry.command) {
                error!(
                    node = self.id,
                    index = entry.index,
                    error = ?e,
                    "Failed to apply log entry during replay"
                );
            }
        }

        self.volatile.write().await.last_applied = commit_index;

        info!(
            node = self.id,
            last_applied = commit_index,
            "Log replay complete"
        );

        Ok(())
    }

    async fn load_persistent_state(log: &LogStorage) -> Result<PersistentState> {
        let (current_term, voted_for) = log.load_raft_state();
        Ok(PersistentState {
            current_term,
            voted_for,
        })
    }

    pub async fn save_persistent_state(&self) -> Result<()> {
        let persistent = self.persistent.read().await;
        self.log.save_raft_state(persistent.current_term, persistent.voted_for)?;
        Ok(())
    }

    pub async fn current_term(&self) -> Term {
        self.persistent.read().await.current_term
    }

    pub async fn role(&self) -> RaftRole {
        *self.role.read().await
    }

    pub async fn is_leader(&self) -> bool {
        *self.role.read().await == RaftRole::Leader
    }

    pub async fn leader_id(&self) -> Option<NodeId> {
        *self.current_leader.read().await
    }

    pub async fn last_log_index(&self) -> LogIndex {
        self.log.last_index()
    }

    pub async fn last_log_term(&self) -> Term {
        self.log.last_term()
    }

    pub fn quorum_size(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }

    pub async fn become_follower(&self, term: Term, leader: Option<NodeId>) {
        info!(node = self.id, term, ?leader, "Becoming follower");

        *self.role.write().await = RaftRole::Follower;
        *self.current_leader.write().await = leader;
        *self.leader_state.write().await = None;

        let mut persistent = self.persistent.write().await;
        let need_save = term > persistent.current_term;
        if need_save {
            persistent.current_term = term;
            persistent.voted_for = None;
        }
        drop(persistent);

        if need_save {
            if let Err(e) = self.save_persistent_state().await {
                error!(node = self.id, error = ?e, "Failed to save persistent state");
            }
        }

        *self.last_heartbeat.write().await = Instant::now();
    }

    pub(crate) async fn become_follower_no_persist(&self, term: Term, leader: Option<NodeId>) {
        info!(node = self.id, term, ?leader, "Becoming follower");

        *self.role.write().await = RaftRole::Follower;
        *self.current_leader.write().await = leader;
        *self.leader_state.write().await = None;
        *self.last_heartbeat.write().await = Instant::now();
    }

    pub async fn become_leader(&self) {
        let term = self.current_term().await;
        info!(node = self.id, term, "Becoming leader");

        *self.role.write().await = RaftRole::Leader;
        *self.current_leader.write().await = Some(self.id);

        let last_log_index = self.last_log_index().await;
        *self.leader_state.write().await = Some(LeaderState::new(&self.peers, last_log_index));

        *self.last_heartbeat.write().await = Instant::now();
    }

    pub(crate) async fn apply_committed_entries(&self) {
        let volatile = self.volatile.read().await;
        let commit_index = volatile.commit_index;
        let last_applied = volatile.last_applied;
        drop(volatile);

        if commit_index <= last_applied {
            return;
        }

        for i in (last_applied + 1)..=commit_index {
            if let Some(entry) = self.log.get(i).await {
                let mut state_machine = self.state_machine.write().await;
                if let Err(e) = state_machine.apply_command(entry.command) {
                    error!(node = self.id, index = i, error = ?e, "Failed to apply command");
                }
                drop(state_machine);

                self.volatile.write().await.last_applied = i;
                debug!(node = self.id, index = i, "Applied command to state machine");
            }
        }
    }

    #[cfg(test)]
    pub async fn apply_committed_entries_public(&self) {
        self.apply_committed_entries().await;
    }

    pub async fn propose(&self, command: RouterCommand) -> Result<LogIndex> {
        if !self.is_leader().await {
            return Err(anyhow!("Not the leader"));
        }

        let term = self.current_term().await;
        let index = self.log.append(term, command).await?;

        debug!(node = self.id, term, index, "Proposed new entry");

        Ok(index)
    }

    pub fn should_start_election(&self, election_timeout: Duration) -> bool {
        if let Ok(last_heartbeat) = self.last_heartbeat.try_read() {
            last_heartbeat.elapsed() >= election_timeout
        } else {
            false
        }
    }

    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}
