use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, watch};
use serde::{Serialize, Deserialize};
use anyhow::{Result, anyhow};
use tracing::{info, debug, error};

use super::storage::LogStorage;
use super::commands::RouterCommand;
use super::state_machine::RouterState;

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

impl Default for PersistentState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VolatileState {
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
}

impl Default for VolatileState {
    fn default() -> Self {
        Self {
            commit_index: 0,
            last_applied: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LeaderState {
    pub next_index: HashMap<NodeId, LogIndex>,
    pub match_index: HashMap<NodeId, LogIndex>,
}

impl LeaderState {
    pub fn new(peers: &[NodeId], last_log_index: LogIndex) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();

        for &peer in peers {
            next_index.insert(peer, last_log_index + 1);
            match_index.insert(peer, 0);
        }

        Self {
            next_index,
            match_index,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,
    pub conflict_index: Option<LogIndex>,
    pub conflict_term: Option<Term>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: LogIndex,
    pub term: Term,
    pub command: RouterCommand,
}

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    pub max_entries_per_append: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            max_entries_per_append: 100,
        }
    }
}

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

    async fn become_follower_no_persist(&self, term: Term, leader: Option<NodeId>) {
        info!(node = self.id, term, ?leader, "Becoming follower");

        *self.role.write().await = RaftRole::Follower;
        *self.current_leader.write().await = leader;
        *self.leader_state.write().await = None;
        *self.last_heartbeat.write().await = Instant::now();
    }

    pub async fn become_candidate(&self) {
        let mut persistent = self.persistent.write().await;
        persistent.current_term += 1;
        persistent.voted_for = Some(self.id);
        let term = persistent.current_term;
        drop(persistent);

        if let Err(e) = self.save_persistent_state().await {
            error!(node = self.id, error = ?e, "Failed to save persistent state");
        }

        info!(node = self.id, term, "Becoming candidate");

        *self.role.write().await = RaftRole::Candidate;
        *self.current_leader.write().await = None;
        *self.leader_state.write().await = None;

        let mut votes = self.votes_received.write().await;
        votes.clear();
        votes.insert(self.id); // Vote for self

        let vote_count = votes.len();
        drop(votes);

        *self.last_heartbeat.write().await = Instant::now();

        if vote_count >= self.quorum_size() {
            self.become_leader().await;
        }
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

    pub async fn handle_request_vote(&self, req: RequestVoteRequest) -> RequestVoteResponse {
        let mut persistent = self.persistent.write().await;

        // If term is stale, reject
        if req.term < persistent.current_term {
            return RequestVoteResponse {
                term: persistent.current_term,
                vote_granted: false,
            };
        }

        // If we see a higher term, become follower
        if req.term > persistent.current_term {
            persistent.current_term = req.term;
            persistent.voted_for = None;
            drop(persistent);

            if let Err(e) = self.save_persistent_state().await {
                error!(node = self.id, error = ?e, "Failed to save persistent state");
            }

            self.become_follower_no_persist(req.term, None).await;
            persistent = self.persistent.write().await;
        }

        // Check if we can grant vote
        let can_vote = persistent.voted_for.is_none()
            || persistent.voted_for == Some(req.candidate_id);

        // Check if candidate's log is at least as up-to-date as ours
        let last_log_term = self.log.last_term();
        let last_log_index = self.log.last_index();

        let log_ok = req.last_log_term > last_log_term
            || (req.last_log_term == last_log_term && req.last_log_index >= last_log_index);

        let vote_granted = can_vote && log_ok;
        let term_to_return = persistent.current_term;

        if vote_granted {
            persistent.voted_for = Some(req.candidate_id);
            drop(persistent);

            if let Err(e) = self.save_persistent_state().await {
                error!(node = self.id, error = ?e, "Failed to save persistent state after voting");
            }

            *self.last_heartbeat.write().await = Instant::now();
            debug!(
                node = self.id,
                candidate = req.candidate_id,
                term = req.term,
                "Granted vote"
            );
        } else {
            drop(persistent);
            debug!(
                node = self.id,
                candidate = req.candidate_id,
                term = req.term,
                can_vote,
                log_ok,
                "Denied vote"
            );
        }

        RequestVoteResponse {
            term: term_to_return,
            vote_granted,
        }
    }

    pub async fn handle_request_vote_response(&self, from: NodeId, resp: RequestVoteResponse) {
        let role = self.role().await;
        let current_term = self.current_term().await;

        // Ignore if not candidate or term changed
        if role != RaftRole::Candidate || resp.term != current_term {
            if resp.term > current_term {
                self.become_follower(resp.term, None).await;
            }
            return;
        }

        if resp.vote_granted {
            let mut votes = self.votes_received.write().await;
            votes.insert(from);

            let vote_count = votes.len();
            debug!(node = self.id, from, vote_count, "Received vote");

            if vote_count >= self.quorum_size() {
                drop(votes);
                self.become_leader().await;
            }
        }
    }

    pub async fn handle_append_entries(&self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        let mut persistent = self.persistent.write().await;

        // If term is stale, reject
        if req.term < persistent.current_term {
            return AppendEntriesResponse {
                term: persistent.current_term,
                success: false,
                match_index: 0,
                conflict_index: None,
                conflict_term: None,
            };
        }

        // If we see a higher or equal term from a leader, become follower
        if req.term >= persistent.current_term {
            let need_save = req.term > persistent.current_term;
            if need_save {
                persistent.current_term = req.term;
                persistent.voted_for = None;
                drop(persistent);

                if let Err(e) = self.save_persistent_state().await {
                    error!(node = self.id, error = ?e, "Failed to save persistent state");
                }
            } else {
                drop(persistent);
            }
            self.become_follower_no_persist(req.term, Some(req.leader_id)).await;
        } else {
            drop(persistent);
        }

        *self.last_heartbeat.write().await = Instant::now();

        // Check if log contains entry at prev_log_index with prev_log_term
        if req.prev_log_index > 0 {
            if let Some(entry) = self.log.get(req.prev_log_index).await {
                if entry.term != req.prev_log_term {
                    // Log inconsistency - find conflict point
                    return AppendEntriesResponse {
                        term: self.current_term().await,
                        success: false,
                        match_index: 0,
                        conflict_index: Some(req.prev_log_index),
                        conflict_term: Some(entry.term),
                    };
                }
            } else if self.log.last_index() < req.prev_log_index {
                // We don't have this entry
                return AppendEntriesResponse {
                    term: self.current_term().await,
                    success: false,
                    match_index: 0,
                    conflict_index: Some(self.log.last_index() + 1),
                    conflict_term: None,
                };
            }
        }

        // Append new entries
        if !req.entries.is_empty() {
            // Find point of divergence and truncate if needed
            for entry in &req.entries {
                if let Some(existing) = self.log.get(entry.index).await {
                    if existing.term != entry.term {
                        // Conflict - truncate from here
                        if let Err(e) = self.log.truncate_after(entry.index - 1).await {
                            error!(node = self.id, error = ?e, "Failed to truncate log");
                        }
                        break;
                    }
                } else {
                    break;
                }
            }

            // Append entries that we don't have
            let entries_to_append: Vec<_> = req.entries.iter()
                .filter(|e| e.index > self.log.last_index())
                .map(|e| (e.term, e.command.clone()))
                .collect();

            if !entries_to_append.is_empty() {
                if let Err(e) = self.log.append_entries(entries_to_append).await {
                    error!(node = self.id, error = ?e, "Failed to append entries");
                    return AppendEntriesResponse {
                        term: self.current_term().await,
                        success: false,
                        match_index: 0,
                        conflict_index: None,
                        conflict_term: None,
                    };
                }
            }
        }

        // Update commit index
        if req.leader_commit > self.volatile.read().await.commit_index {
            let new_commit = std::cmp::min(req.leader_commit, self.log.last_index());
            self.volatile.write().await.commit_index = new_commit;

            // Apply committed entries
            self.apply_committed_entries().await;
        }

        AppendEntriesResponse {
            term: self.current_term().await,
            success: true,
            match_index: self.log.last_index(),
            conflict_index: None,
            conflict_term: None,
        }
    }

    pub async fn handle_append_entries_response(
        &self,
        from: NodeId,
        resp: AppendEntriesResponse,
    ) {
        if !self.is_leader().await {
            return;
        }

        if resp.term > self.current_term().await {
            self.become_follower(resp.term, None).await;
            return;
        }

        let mut leader_state = self.leader_state.write().await;
        let leader_state = match leader_state.as_mut() {
            Some(s) => s,
            None => return,
        };

        if resp.success {
            // Update match_index and next_index
            leader_state.match_index.insert(from, resp.match_index);
            leader_state.next_index.insert(from, resp.match_index + 1);

            // Check if we can advance commit_index
            let _ = leader_state;
            self.try_advance_commit_index().await;
        } else {
            // Decrement next_index and retry
            if let Some(conflict_index) = resp.conflict_index {
                leader_state.next_index.insert(from, conflict_index);
            } else {
                let next = leader_state.next_index.get(&from).copied().unwrap_or(1);
                if next > 1 {
                    leader_state.next_index.insert(from, next - 1);
                }
            }
        }
    }

    async fn try_advance_commit_index(&self) {
        let leader_state = self.leader_state.read().await;
        let leader_state = match leader_state.as_ref() {
            Some(s) => s,
            None => return,
        };

        let current_term = self.current_term().await;
        let volatile = self.volatile.read().await;
        let current_commit = volatile.commit_index;
        drop(volatile);

        // Find the highest index that a majority has
        let last_index = self.log.last_index();

        for n in (current_commit + 1)..=last_index {
            // Check if entry at n has current term
            if let Some(entry) = self.log.get(n).await {
                if entry.term != current_term {
                    continue;
                }
            } else {
                continue;
            }

            // Count replicas (including self)
            let mut count = 1; // self
            for &peer in &self.peers {
                if leader_state.match_index.get(&peer).copied().unwrap_or(0) >= n {
                    count += 1;
                }
            }

            if count >= self.quorum_size() {
                self.volatile.write().await.commit_index = n;
                debug!(node = self.id, commit_index = n, "Advanced commit index");
            }
        }

        let _ = leader_state;
        self.apply_committed_entries().await;
    }

    async fn apply_committed_entries(&self) {
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

    pub async fn build_append_entries_request(&self, peer: NodeId) -> Option<AppendEntriesRequest> {
        let leader_state = self.leader_state.read().await;
        let leader_state = leader_state.as_ref()?;

        let next_index = leader_state.next_index.get(&peer).copied().unwrap_or(1);
        let prev_log_index = if next_index > 0 { next_index - 1 } else { 0 };

        let prev_log_term = if prev_log_index > 0 {
            self.log.get(prev_log_index).await.map(|e| e.term).unwrap_or(0)
        } else {
            0
        };

        // Get entries to send
        let last_index = self.log.last_index();
        let entries = if next_index <= last_index {
            let end_index = std::cmp::min(
                last_index,
                next_index + self.config.max_entries_per_append as u64 - 1,
            );
            self.log.get_range(next_index, end_index).await
                .into_iter()
                .map(|e| LogEntry {
                    index: e.index,
                    term: e.term,
                    command: e.command,
                })
                .collect()
        } else {
            vec![]
        };

        let commit_index = self.volatile.read().await.commit_index;

        Some(AppendEntriesRequest {
            term: self.current_term().await,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: commit_index,
        })
    }

    pub async fn build_request_vote_request(&self) -> RequestVoteRequest {
        RequestVoteRequest {
            term: self.current_term().await,
            candidate_id: self.id,
            last_log_index: self.last_log_index().await,
            last_log_term: self.last_log_term().await,
        }
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
