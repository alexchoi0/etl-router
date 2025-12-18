use anyhow::Result;
use tonic::transport::Channel;

use conveyor_proto::lookup::{
    lookup_service_client::LookupServiceClient,
    BatchLookupRequest, BatchLookupResponse, Capabilities, LookupRequest, LookupResponse,
};
use conveyor_proto::common::{Empty, Record};

pub struct LookupClient {
    client: LookupServiceClient<Channel>,
    lookup_id: String,
}

impl LookupClient {
    pub async fn connect(addr: String, lookup_id: String) -> Result<Self> {
        let client = LookupServiceClient::connect(addr).await?;
        Ok(Self { client, lookup_id })
    }

    pub async fn lookup(
        &mut self,
        record: Record,
        key_fields: Vec<String>,
    ) -> Result<LookupResponse> {
        let request = LookupRequest {
            lookup_id: self.lookup_id.clone(),
            input_record: Some(record),
            key_fields,
        };

        let response = self.client.lookup(request).await?;
        Ok(response.into_inner())
    }

    pub async fn batch_lookup(
        &mut self,
        records: Vec<Record>,
        key_fields: Vec<String>,
    ) -> Result<BatchLookupResponse> {
        let request = BatchLookupRequest {
            lookup_id: self.lookup_id.clone(),
            records,
            key_fields,
        };

        let response = self.client.batch_lookup(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_capabilities(&mut self) -> Result<Capabilities> {
        let response = self.client.get_capabilities(Empty {}).await?;
        Ok(response.into_inner())
    }

    pub fn lookup_id(&self) -> &str {
        &self.lookup_id
    }
}
