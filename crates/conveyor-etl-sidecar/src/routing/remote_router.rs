use anyhow::{Result, Context};
use tonic::transport::Channel;
use tracing::instrument;

use conveyor_etl_proto::common::RecordBatch;
use conveyor_etl_proto::sidecar::{
    sidecar_data_plane_client::SidecarDataPlaneClient,
    ReceiveRecordsRequest,
};

use super::ClientPool;

pub struct RemoteRouter {
    sidecar_clients: ClientPool<SidecarDataPlaneClient<Channel>>,
}

impl RemoteRouter {
    pub fn new() -> Self {
        Self {
            sidecar_clients: ClientPool::new(),
        }
    }

    #[instrument(skip(self, batch), fields(target = %sidecar_endpoint))]
    pub async fn forward_to_sidecar(
        &self,
        sidecar_endpoint: &str,
        pipeline_id: &str,
        stage_id: &str,
        batch: RecordBatch,
    ) -> Result<bool> {
        let mut client = self.sidecar_clients
            .get_or_create(sidecar_endpoint, SidecarDataPlaneClient::new)
            .await?;

        let response = client
            .receive_records(ReceiveRecordsRequest {
                pipeline_id: pipeline_id.to_string(),
                stage_id: stage_id.to_string(),
                batch: Some(batch),
                source_sidecar_id: String::new(),
            })
            .await
            .context("Forward to sidecar failed")?
            .into_inner();

        Ok(response.success)
    }

    pub fn remove_client(&self, endpoint: &str) {
        self.sidecar_clients.remove(endpoint);
    }

    pub fn clear_clients(&self) {
        self.sidecar_clients.clear();
    }
}

impl Default for RemoteRouter {
    fn default() -> Self {
        Self::new()
    }
}
