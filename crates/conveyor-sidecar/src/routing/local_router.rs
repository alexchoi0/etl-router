use std::collections::HashMap;
use anyhow::{Result, Context};
use tonic::transport::Channel;
use tracing::instrument;

use conveyor_proto::transform::{
    transform_service_client::TransformServiceClient,
    ProcessBatchRequest, TransformStatus,
};
use conveyor_proto::sink::{
    sink_service_client::SinkServiceClient,
    WriteBatchRequest,
};
use conveyor_proto::common::RecordBatch;

use super::ClientPool;

pub struct LocalRouter {
    transform_clients: ClientPool<TransformServiceClient<Channel>>,
    sink_clients: ClientPool<SinkServiceClient<Channel>>,
}

impl LocalRouter {
    pub fn new() -> Self {
        Self {
            transform_clients: ClientPool::new(),
            sink_clients: ClientPool::new(),
        }
    }

    #[instrument(skip(self, batch))]
    pub async fn route_to_transform(
        &self,
        endpoint: &str,
        transform_id: &str,
        batch: RecordBatch,
        config: HashMap<String, String>,
    ) -> Result<Vec<RecordBatch>> {
        let mut client = self.transform_clients
            .get_or_create(endpoint, TransformServiceClient::new)
            .await?;

        let response = client
            .process_batch(ProcessBatchRequest {
                transform_id: transform_id.to_string(),
                input_batch: Some(batch),
                transform_config: config,
            })
            .await
            .context("Transform call failed")?
            .into_inner();

        let output_records: Vec<_> = response
            .results
            .into_iter()
            .filter(|r| r.status == TransformStatus::Success as i32)
            .flat_map(|r| r.output_records)
            .collect();

        let output_batch = RecordBatch {
            batch_id: format!("out-{}", chrono::Utc::now().timestamp_millis()),
            records: output_records,
            watermark: None,
        };

        Ok(vec![output_batch])
    }

    #[instrument(skip(self, batch))]
    pub async fn route_to_sink(
        &self,
        endpoint: &str,
        sink_id: &str,
        batch: RecordBatch,
    ) -> Result<bool> {
        let mut client = self.sink_clients
            .get_or_create(endpoint, SinkServiceClient::new)
            .await?;

        let response = client
            .write_batch(WriteBatchRequest {
                sink_id: sink_id.to_string(),
                batch: Some(batch),
                options: None,
            })
            .await
            .context("Sink call failed")?
            .into_inner();

        let all_delivered = response
            .results
            .iter()
            .all(|r| r.success);

        Ok(all_delivered)
    }
}

impl Default for LocalRouter {
    fn default() -> Self {
        Self::new()
    }
}
