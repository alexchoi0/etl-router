use anyhow::Result;
use tonic::transport::Channel;

use conveyor_etl_proto::transform::{
    transform_service_client::TransformServiceClient,
    ProcessBatchRequest, ProcessBatchResponse,
    Capabilities,
};
use conveyor_etl_proto::common::{RecordBatch, Empty};

pub struct TransformClient {
    client: TransformServiceClient<Channel>,
    transform_id: String,
}

impl TransformClient {
    pub async fn connect(addr: String, transform_id: String) -> Result<Self> {
        let client = TransformServiceClient::connect(addr).await?;
        Ok(Self { client, transform_id })
    }

    pub async fn process_batch(
        &mut self,
        batch: RecordBatch,
        config: std::collections::HashMap<String, String>,
    ) -> Result<ProcessBatchResponse> {
        let request = ProcessBatchRequest {
            transform_id: self.transform_id.clone(),
            input_batch: Some(batch),
            transform_config: config,
        };

        let response = self.client.process_batch(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_capabilities(&mut self) -> Result<Capabilities> {
        let response = self.client.get_capabilities(Empty {}).await?;
        Ok(response.into_inner())
    }
}
