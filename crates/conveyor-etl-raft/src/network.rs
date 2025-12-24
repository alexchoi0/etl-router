use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use conveyor_etl_proto::raft::{
    raft_service_client::RaftServiceClient,
    raft_service_server::{RaftService, RaftServiceServer},
    AppendEntriesRequest as ProtoAppendEntriesRequest,
    AppendEntriesResponse as ProtoAppendEntriesResponse,
    InstallSnapshotRequest as ProtoInstallSnapshotRequest,
    InstallSnapshotResponse as ProtoInstallSnapshotResponse, VoteRequest as ProtoVoteRequest,
    VoteResponse as ProtoVoteResponse,
};

use crate::config::{ConveyorRaft, NodeId, TypeConfig};

type RpcError = openraft::error::RPCError<TypeConfig>;

fn to_rpc_err<E: std::error::Error + 'static>(e: E) -> RpcError {
    openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
}

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, RpcError> {
    bincode::serialize(value).map_err(to_rpc_err)
}

fn serialize_opt<T: Serialize>(value: Option<T>) -> Option<Vec<u8>> {
    value.and_then(|v| bincode::serialize(&v).ok())
}

fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, RpcError> {
    bincode::deserialize(bytes).map_err(to_rpc_err)
}

fn deserialize_opt<T: DeserializeOwned>(bytes: Option<Vec<u8>>) -> Option<T> {
    bytes.and_then(|b| bincode::deserialize(&b).ok())
}

pub struct NetworkFactory {
    clients: Arc<RwLock<BTreeMap<NodeId, RaftServiceClient<Channel>>>>,
}

impl NetworkFactory {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Default for NetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Network;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        Network {
            target,
            endpoint: node.addr.clone(),
            clients: self.clients.clone(),
        }
    }
}

pub struct Network {
    target: NodeId,
    endpoint: String,
    clients: Arc<RwLock<BTreeMap<NodeId, RaftServiceClient<Channel>>>>,
}

impl Network {
    async fn get_client(&self) -> Result<RaftServiceClient<Channel>, RpcError> {
        {
            let clients = self.clients.read().await;
            if let Some(client) = clients.get(&self.target) {
                return Ok(client.clone());
            }
        }

        let endpoint = if self.endpoint.starts_with("http") {
            self.endpoint.clone()
        } else {
            format!("http://{}", self.endpoint)
        };

        let channel = Channel::from_shared(endpoint)
            .map_err(to_rpc_err)?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .connect()
            .await
            .map_err(to_rpc_err)?;

        let client = RaftServiceClient::new(channel);

        {
            let mut clients = self.clients.write().await;
            clients.insert(self.target, client.clone());
        }

        Ok(client)
    }
}

impl RaftNetwork<TypeConfig> for Network {
    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RpcError> {
        let mut client = self.get_client().await?;

        let proto_req = ProtoVoteRequest {
            vote: serialize(&rpc.vote)?,
            last_log_id: serialize_opt(rpc.last_log_id),
        };

        let resp = client
            .vote(Request::new(proto_req))
            .await
            .map_err(to_rpc_err)?
            .into_inner();

        Ok(VoteResponse {
            vote: deserialize(&resp.vote)?,
            vote_granted: resp.vote_granted,
            last_log_id: deserialize_opt(resp.last_log_id),
        })
    }

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RpcError> {
        let mut client = self.get_client().await?;

        let entries_bytes: Vec<Vec<u8>> = rpc
            .entries
            .iter()
            .filter_map(|e| bincode::serialize(e).ok())
            .collect();

        let proto_req = ProtoAppendEntriesRequest {
            vote: serialize(&rpc.vote)?,
            prev_log_id: serialize_opt(rpc.prev_log_id),
            entries: entries_bytes,
            leader_commit: serialize_opt(rpc.leader_commit),
        };

        let resp = client
            .append_entries(Request::new(proto_req))
            .await
            .map_err(to_rpc_err)?
            .into_inner();

        Ok(AppendEntriesResponse {
            vote: deserialize(&resp.vote)?,
            success: resp.success,
            conflict: deserialize_opt(resp.conflict),
        })
    }

    async fn full_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<TypeConfig>, RpcError> {
        let mut client = self.get_client().await?;

        let proto_req = ProtoInstallSnapshotRequest {
            vote: serialize(&rpc.vote)?,
            meta: serialize(&rpc.meta)?,
            offset: 0,
            data: rpc.snapshot.into_inner(),
            done: true,
        };

        let resp = client
            .install_snapshot(Request::new(proto_req))
            .await
            .map_err(to_rpc_err)?
            .into_inner();

        Ok(InstallSnapshotResponse {
            vote: deserialize(&resp.vote)?,
        })
    }
}

pub struct RaftServer {
    raft: Arc<ConveyorRaft>,
}

impl RaftServer {
    pub fn new(raft: Arc<ConveyorRaft>) -> Self {
        Self { raft }
    }

    pub fn into_service(self) -> RaftServiceServer<Self> {
        RaftServiceServer::new(self)
    }
}

fn deser<T: DeserializeOwned>(bytes: &[u8], field: &str) -> Result<T, Status> {
    bincode::deserialize(bytes).map_err(|e| Status::invalid_argument(format!("Invalid {}: {}", field, e)))
}

fn ser<T: Serialize>(value: &T) -> Result<Vec<u8>, Status> {
    bincode::serialize(value).map_err(|e| Status::internal(format!("Serialize error: {}", e)))
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    async fn vote(
        &self,
        request: Request<ProtoVoteRequest>,
    ) -> Result<Response<ProtoVoteResponse>, Status> {
        let req = request.into_inner();

        let rpc = VoteRequest {
            vote: deser(&req.vote, "vote")?,
            last_log_id: deserialize_opt(req.last_log_id),
        };

        let resp = self
            .raft
            .vote(rpc)
            .await
            .map_err(|e| Status::internal(format!("Vote error: {}", e)))?;

        Ok(Response::new(ProtoVoteResponse {
            vote: ser(&resp.vote)?,
            vote_granted: resp.vote_granted,
            last_log_id: serialize_opt(resp.last_log_id),
        }))
    }

    async fn append_entries(
        &self,
        request: Request<ProtoAppendEntriesRequest>,
    ) -> Result<Response<ProtoAppendEntriesResponse>, Status> {
        let req = request.into_inner();

        let rpc = AppendEntriesRequest {
            vote: deser(&req.vote, "vote")?,
            prev_log_id: deserialize_opt(req.prev_log_id),
            entries: req
                .entries
                .into_iter()
                .filter_map(|bytes| bincode::deserialize(&bytes).ok())
                .collect(),
            leader_commit: deserialize_opt(req.leader_commit),
        };

        let resp = self
            .raft
            .append_entries(rpc)
            .await
            .map_err(|e| Status::internal(format!("AppendEntries error: {}", e)))?;

        Ok(Response::new(ProtoAppendEntriesResponse {
            vote: ser(&resp.vote)?,
            success: resp.success,
            conflict: serialize_opt(resp.conflict),
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<ProtoInstallSnapshotRequest>,
    ) -> Result<Response<ProtoInstallSnapshotResponse>, Status> {
        let req = request.into_inner();

        let rpc = InstallSnapshotRequest {
            vote: deser(&req.vote, "vote")?,
            meta: deser(&req.meta, "meta")?,
            snapshot: Box::new(std::io::Cursor::new(req.data)),
        };

        let resp = self
            .raft
            .install_full_snapshot(rpc)
            .await
            .map_err(|e| Status::internal(format!("InstallSnapshot error: {}", e)))?;

        Ok(Response::new(ProtoInstallSnapshotResponse {
            vote: ser(&resp.vote)?,
        }))
    }
}
