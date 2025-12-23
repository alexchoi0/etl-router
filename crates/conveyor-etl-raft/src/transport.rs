use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, transport::Channel};
use anyhow::Result;

use conveyor_etl_proto::raft::{
    raft_service_server::{RaftService, RaftServiceServer},
    raft_service_client::RaftServiceClient,
    RequestVoteRequest as ProtoRequestVoteRequest,
    RequestVoteResponse as ProtoRequestVoteResponse,
    AppendEntriesRequest as ProtoAppendEntriesRequest,
    AppendEntriesResponse as ProtoAppendEntriesResponse,
    LogEntry as ProtoLogEntry,
    InstallSnapshotRequest, InstallSnapshotResponse,
};

use super::core::{
    NodeId, RaftCore, RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse, LogEntry,
};
use super::commands::RouterCommand;

pub struct RaftTransport {
    core: Arc<RaftCore>,
    clients: RwLock<HashMap<NodeId, RaftServiceClient<Channel>>>,
    peer_addrs: HashMap<NodeId, String>,
}

pub struct RaftTransportService {
    transport: Arc<RaftTransport>,
}

impl RaftTransport {
    pub fn new(core: Arc<RaftCore>, peer_addrs: HashMap<NodeId, String>) -> Self {
        Self {
            core,
            clients: RwLock::new(HashMap::new()),
            peer_addrs,
        }
    }

    pub fn into_service(self: Arc<Self>) -> RaftTransportService {
        RaftTransportService { transport: self }
    }

    pub fn into_server(self: Arc<Self>) -> RaftServiceServer<RaftTransportService> {
        RaftServiceServer::new(self.into_service())
    }

    async fn get_client(&self, peer: NodeId) -> Result<RaftServiceClient<Channel>> {
        {
            let clients = self.clients.read().await;
            if let Some(client) = clients.get(&peer) {
                return Ok(client.clone());
            }
        }

        let addr = self.peer_addrs.get(&peer)
            .ok_or_else(|| anyhow::anyhow!("Unknown peer: {}", peer))?;

        let endpoint = format!("http://{}", addr);
        let channel = Channel::from_shared(endpoint)?
            .connect_timeout(std::time::Duration::from_secs(5))
            .connect()
            .await?;

        let client = RaftServiceClient::new(channel);

        {
            let mut clients = self.clients.write().await;
            clients.insert(peer, client.clone());
        }

        Ok(client)
    }

    pub async fn send_request_vote(
        &self,
        peer: NodeId,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse> {
        let mut client = self.get_client(peer).await?;

        let proto_req = ProtoRequestVoteRequest {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        };

        let response = client.request_vote(Request::new(proto_req)).await?;
        let resp = response.into_inner();

        Ok(RequestVoteResponse {
            term: resp.term,
            vote_granted: resp.vote_granted,
        })
    }

    pub async fn send_append_entries(
        &self,
        peer: NodeId,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        let mut client = self.get_client(peer).await?;

        let proto_entries: Vec<ProtoLogEntry> = req.entries.iter().map(|e| {
            let command_bytes = bincode::serialize(&e.command).unwrap_or_default();
            ProtoLogEntry {
                index: e.index,
                term: e.term,
                command: command_bytes,
            }
        }).collect();

        let proto_req = ProtoAppendEntriesRequest {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries: proto_entries,
            leader_commit: req.leader_commit,
        };

        let response = client.append_entries(Request::new(proto_req)).await?;
        let resp = response.into_inner();

        Ok(AppendEntriesResponse {
            term: resp.term,
            success: resp.success,
            match_index: resp.match_index,
            conflict_index: resp.conflict_index,
            conflict_term: resp.conflict_term,
        })
    }

    pub async fn clear_client(&self, peer: NodeId) {
        let mut clients = self.clients.write().await;
        clients.remove(&peer);
    }
}

#[tonic::async_trait]
impl RaftService for RaftTransportService {
    async fn request_vote(
        &self,
        request: Request<ProtoRequestVoteRequest>,
    ) -> Result<Response<ProtoRequestVoteResponse>, Status> {
        let req = request.into_inner();

        let vote_req = RequestVoteRequest {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        };

        let resp = self.transport.core.handle_request_vote(vote_req).await;

        Ok(Response::new(ProtoRequestVoteResponse {
            term: resp.term,
            vote_granted: resp.vote_granted,
        }))
    }

    async fn append_entries(
        &self,
        request: Request<ProtoAppendEntriesRequest>,
    ) -> Result<Response<ProtoAppendEntriesResponse>, Status> {
        let req = request.into_inner();

        let entries: Vec<LogEntry> = req.entries.into_iter().map(|e| {
            let command: RouterCommand = bincode::deserialize(&e.command)
                .unwrap_or(RouterCommand::Noop);
            LogEntry {
                index: e.index,
                term: e.term,
                command,
            }
        }).collect();

        let append_req = AppendEntriesRequest {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries,
            leader_commit: req.leader_commit,
        };

        let resp = self.transport.core.handle_append_entries(append_req).await;

        Ok(Response::new(ProtoAppendEntriesResponse {
            term: resp.term,
            success: resp.success,
            match_index: resp.match_index,
            conflict_index: resp.conflict_index,
            conflict_term: resp.conflict_term,
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let _req = request.into_inner();

        Ok(Response::new(InstallSnapshotResponse {
            term: self.transport.core.current_term().await,
        }))
    }
}
