use tracing::{info, debug, error};

use super::RaftCore;
use super::types::{NodeId, RaftRole};
use super::messages::{RequestVoteRequest, RequestVoteResponse};

impl RaftCore {
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
        votes.insert(self.id);

        let vote_count = votes.len();
        drop(votes);

        *self.last_heartbeat.write().await = std::time::Instant::now();

        if vote_count >= self.quorum_size() {
            self.become_leader().await;
        }
    }

    pub async fn handle_request_vote(&self, req: RequestVoteRequest) -> RequestVoteResponse {
        let mut persistent = self.persistent.write().await;

        if req.term < persistent.current_term {
            return RequestVoteResponse {
                term: persistent.current_term,
                vote_granted: false,
            };
        }

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

        let can_vote = persistent.voted_for.is_none()
            || persistent.voted_for == Some(req.candidate_id);

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

            *self.last_heartbeat.write().await = std::time::Instant::now();
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

    pub async fn build_request_vote_request(&self) -> RequestVoteRequest {
        RequestVoteRequest {
            term: self.current_term().await,
            candidate_id: self.id,
            last_log_index: self.last_log_index().await,
            last_log_term: self.last_log_term().await,
        }
    }
}
