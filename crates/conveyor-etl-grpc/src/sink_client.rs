use anyhow::Result;
use tonic::transport::Channel;

use conveyor_etl_proto::sink::{
    sink_service_client::SinkServiceClient,
    WriteBatchRequest, WriteBatchResponse, WriteOptions,
    CapacityResponse, FlushRequest, FlushResponse,
};
use conveyor_etl_proto::common::{RecordBatch, Empty};

pub struct SinkClient {
    client: SinkServiceClient<Channel>,
    sink_id: String,
}

impl SinkClient {
    pub async fn connect(addr: String, sink_id: String) -> Result<Self> {
        let client = SinkServiceClient::connect(addr).await?;
        Ok(Self { client, sink_id })
    }

    pub async fn write_batch(
        &mut self,
        batch: RecordBatch,
        options: WriteOptions,
    ) -> Result<WriteBatchResponse> {
        let request = WriteBatchRequest {
            sink_id: self.sink_id.clone(),
            batch: Some(batch),
            options: Some(options),
        };

        let response = self.client.write_batch(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_capacity(&mut self) -> Result<CapacityResponse> {
        let response = self.client.get_capacity(Empty {}).await?;
        Ok(response.into_inner())
    }

    pub async fn flush(&mut self, wait: bool) -> Result<FlushResponse> {
        let request = FlushRequest {
            sink_id: self.sink_id.clone(),
            wait_for_completion: wait,
        };

        let response = self.client.flush(request).await?;
        Ok(response.into_inner())
    }
}

