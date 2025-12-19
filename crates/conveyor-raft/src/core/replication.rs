use tracing::{debug, error};

use super::RaftCore;
use super::types::NodeId;
use super::messages::{AppendEntriesRequest, AppendEntriesResponse, LogEntry};
use super::LogIndex;

impl RaftCore {
    pub async fn handle_append_entries(&self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        let mut persistent = self.persistent.write().await;

        if req.term < persistent.current_term {
            return AppendEntriesResponse {
                term: persistent.current_term,
                success: false,
                match_index: 0,
                conflict_index: None,
                conflict_term: None,
            };
        }

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

        *self.last_heartbeat.write().await = std::time::Instant::now();

        if req.prev_log_index > 0 {
            if let Some(entry) = self.log.get(req.prev_log_index).await {
                if entry.term != req.prev_log_term {
                    return AppendEntriesResponse {
                        term: self.current_term().await,
                        success: false,
                        match_index: 0,
                        conflict_index: Some(req.prev_log_index),
                        conflict_term: Some(entry.term),
                    };
                }
            } else if self.log.last_index() < req.prev_log_index {
                return AppendEntriesResponse {
                    term: self.current_term().await,
                    success: false,
                    match_index: 0,
                    conflict_index: Some(self.log.last_index() + 1),
                    conflict_term: None,
                };
            }
        }

        if !req.entries.is_empty() {
            for entry in &req.entries {
                if let Some(existing) = self.log.get(entry.index).await {
                    if existing.term != entry.term {
                        if let Err(e) = self.log.truncate_after(entry.index - 1).await {
                            error!(node = self.id, error = ?e, "Failed to truncate log");
                        }
                        break;
                    }
                } else {
                    break;
                }
            }

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

        if req.leader_commit > self.volatile.read().await.commit_index {
            let new_commit = std::cmp::min(req.leader_commit, self.log.last_index());
            self.volatile.write().await.commit_index = new_commit;
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
            leader_state.match_index.insert(from, resp.match_index);
            leader_state.next_index.insert(from, resp.match_index + 1);

            let _ = leader_state;
            self.try_advance_commit_index().await;
        } else {
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

    pub(super) async fn try_advance_commit_index(&self) {
        let leader_state = self.leader_state.read().await;
        let leader_state = match leader_state.as_ref() {
            Some(s) => s,
            None => return,
        };

        let current_term = self.current_term().await;
        let volatile = self.volatile.read().await;
        let current_commit = volatile.commit_index;
        drop(volatile);

        let last_index = self.log.last_index();

        for n in (current_commit + 1)..=last_index {
            if let Some(entry) = self.log.get(n).await {
                if entry.term != current_term {
                    continue;
                }
            } else {
                continue;
            }

            let mut count = 1;
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

        let last_index = self.log.last_index();
        let entries = if next_index <= last_index {
            let end_index = std::cmp::min(
                last_index,
                next_index + self.config.max_entries_per_append as LogIndex - 1,
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
}
