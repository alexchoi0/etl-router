use std::collections::{HashMap, VecDeque};

use dashmap::DashMap;
use tracing::{debug, warn};

use crate::error::{DlqError, Result};
use crate::policy::DlqPolicy;
use crate::record::DeadLetterRecord;

struct SinkDlqBuffer {
    records: VecDeque<DeadLetterRecord>,
    policy: DlqPolicy,
}

impl SinkDlqBuffer {
    fn new(policy: DlqPolicy) -> Self {
        Self {
            records: VecDeque::new(),
            policy,
        }
    }

    fn push(&mut self, record: DeadLetterRecord) -> Result<()> {
        if self.records.len() >= self.policy.max_buffer_size {
            return Err(DlqError::BufferFull {
                sink_id: "unknown".to_string(),
            });
        }
        self.records.push_back(record);
        Ok(())
    }

    fn drain_batch(&mut self) -> Vec<DeadLetterRecord> {
        let count = std::cmp::min(self.policy.batch_size, self.records.len());
        self.records.drain(..count).collect()
    }

    fn drain_expired(&mut self) -> Vec<DeadLetterRecord> {
        let max_age = self.policy.max_age_ms;
        let mut expired = Vec::new();
        let mut i = 0;
        while i < self.records.len() {
            if self.records[i].age_ms() > max_age {
                if let Some(record) = self.records.remove(i) {
                    expired.push(record);
                }
            } else {
                i += 1;
            }
        }
        expired
    }

    fn len(&self) -> usize {
        self.records.len()
    }
}

pub struct DlqManager {
    buffers: DashMap<String, SinkDlqBuffer>,
    pipeline_to_error_sink: DashMap<String, String>,
    default_policy: DlqPolicy,
}

impl DlqManager {
    pub fn new(default_policy: DlqPolicy) -> Self {
        Self {
            buffers: DashMap::new(),
            pipeline_to_error_sink: DashMap::new(),
            default_policy,
        }
    }

    pub async fn register_pipeline(&self, pipeline_id: &str, error_sink_id: &str) {
        self.pipeline_to_error_sink
            .insert(pipeline_id.to_string(), error_sink_id.to_string());
        debug!(pipeline_id, error_sink_id, "Registered pipeline error sink");
    }

    pub async fn register_sink(&self, sink_id: &str, policy: Option<DlqPolicy>) {
        let policy = policy.unwrap_or_else(|| self.default_policy.clone());
        self.buffers
            .insert(sink_id.to_string(), SinkDlqBuffer::new(policy));
        debug!(sink_id, "Registered DLQ sink");
    }

    pub async fn send_to_dlq(&self, record: DeadLetterRecord) -> Result<()> {
        let pipeline_id = &record.pipeline_id;

        let sink_id = self
            .pipeline_to_error_sink
            .get(pipeline_id)
            .map(|r| r.value().clone())
            .ok_or_else(|| DlqError::NoErrorSink {
                pipeline_id: pipeline_id.clone(),
            })?;

        let mut buffer = self
            .buffers
            .entry(sink_id.clone())
            .or_insert_with(|| SinkDlqBuffer::new(self.default_policy.clone()));

        buffer.push(record).map_err(|_| DlqError::BufferFull { sink_id })?;
        Ok(())
    }

    pub async fn send_batch_to_dlq(&self, records: Vec<DeadLetterRecord>) -> Result<usize> {
        let mut sent = 0;
        for record in records {
            match self.send_to_dlq(record).await {
                Ok(()) => sent += 1,
                Err(e) => {
                    warn!(error = %e, "Failed to send record to DLQ");
                    break;
                }
            }
        }
        Ok(sent)
    }

    pub async fn get_batch(&self, sink_id: &str) -> Vec<DeadLetterRecord> {
        if let Some(mut buffer) = self.buffers.get_mut(sink_id) {
            buffer.drain_batch()
        } else {
            Vec::new()
        }
    }

    pub async fn drain_expired(&self, sink_id: &str) -> Vec<DeadLetterRecord> {
        if let Some(mut buffer) = self.buffers.get_mut(sink_id) {
            buffer.drain_expired()
        } else {
            Vec::new()
        }
    }

    pub async fn get_buffer_size(&self, sink_id: &str) -> usize {
        self.buffers.get(sink_id).map(|b| b.len()).unwrap_or(0)
    }

    pub async fn get_all_sink_ids(&self) -> Vec<String> {
        self.buffers.iter().map(|e| e.key().clone()).collect()
    }

    pub async fn get_stats(&self) -> DlqStats {
        let mut total_records = 0;
        let mut per_sink = HashMap::new();

        for entry in self.buffers.iter() {
            let len = entry.value().len();
            total_records += len;
            per_sink.insert(entry.key().clone(), len);
        }

        DlqStats {
            total_records,
            per_sink,
            sink_count: self.buffers.len(),
        }
    }

    pub async fn should_retry(&self, record: &DeadLetterRecord) -> bool {
        let Some(sink_id) = self
            .pipeline_to_error_sink
            .get(&record.pipeline_id)
            .map(|r| r.value().clone())
        else {
            return false;
        };

        let Some(buffer) = self.buffers.get(&sink_id) else {
            return self.default_policy.retry.should_retry(record.error.retry_count);
        };

        buffer.policy.retry.should_retry(record.error.retry_count)
    }
}

impl Default for DlqManager {
    fn default() -> Self {
        Self::new(DlqPolicy::default())
    }
}

#[derive(Debug, Clone)]
pub struct DlqStats {
    pub total_records: usize,
    pub per_sink: HashMap<String, usize>,
    pub sink_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{ErrorCode, ErrorContext};
    use conveyor_etl_proto::common::{Record, RecordId};

    fn make_record(id: &str) -> Record {
        Record {
            id: Some(RecordId {
                source_id: "test-source".to_string(),
                partition: 0,
                sequence_number: 1,
                idempotency_key: vec![],
            }),
            record_type: "test.event".to_string(),
            key: id.as_bytes().to_vec(),
            payload: b"{}".to_vec(),
            metadata: Default::default(),
            event_time: None,
            ingestion_time: None,
        }
    }

    fn make_dlq_record(id: &str, pipeline: &str) -> DeadLetterRecord {
        let error = ErrorContext::new(
            ErrorCode::TransformFailed,
            "Test error",
            "test-stage",
            "transform",
        );
        DeadLetterRecord::new(make_record(id), pipeline, "source-1", error)
    }

    #[tokio::test]
    async fn test_register_and_send() {
        let manager = DlqManager::default();

        manager.register_pipeline("pipeline-1", "error-sink").await;
        manager.register_sink("error-sink", None).await;

        let record = make_dlq_record("rec-1", "pipeline-1");
        manager.send_to_dlq(record).await.unwrap();

        assert_eq!(manager.get_buffer_size("error-sink").await, 1);
    }

    #[tokio::test]
    async fn test_get_batch() {
        let manager = DlqManager::default();

        manager.register_pipeline("pipeline-1", "error-sink").await;
        manager.register_sink("error-sink", None).await;

        for i in 0..5 {
            let record = make_dlq_record(&format!("rec-{}", i), "pipeline-1");
            manager.send_to_dlq(record).await.unwrap();
        }

        let batch = manager.get_batch("error-sink").await;
        assert_eq!(batch.len(), 5);
        assert_eq!(manager.get_buffer_size("error-sink").await, 0);
    }

    #[tokio::test]
    async fn test_no_error_sink_configured() {
        let manager = DlqManager::default();

        let record = make_dlq_record("rec-1", "unknown-pipeline");
        let result = manager.send_to_dlq(record).await;

        assert!(matches!(result, Err(DlqError::NoErrorSink { .. })));
    }

    #[tokio::test]
    async fn test_buffer_full() {
        let policy = DlqPolicy {
            max_buffer_size: 2,
            ..Default::default()
        };
        let manager = DlqManager::new(policy);

        manager.register_pipeline("pipeline-1", "error-sink").await;
        manager.register_sink("error-sink", Some(DlqPolicy {
            max_buffer_size: 2,
            ..Default::default()
        })).await;

        manager.send_to_dlq(make_dlq_record("rec-1", "pipeline-1")).await.unwrap();
        manager.send_to_dlq(make_dlq_record("rec-2", "pipeline-1")).await.unwrap();

        let result = manager.send_to_dlq(make_dlq_record("rec-3", "pipeline-1")).await;
        assert!(matches!(result, Err(DlqError::BufferFull { .. })));
    }

    #[tokio::test]
    async fn test_stats() {
        let manager = DlqManager::default();

        manager.register_pipeline("pipeline-1", "sink-1").await;
        manager.register_pipeline("pipeline-2", "sink-2").await;
        manager.register_sink("sink-1", None).await;
        manager.register_sink("sink-2", None).await;

        for i in 0..3 {
            manager.send_to_dlq(make_dlq_record(&format!("a-{}", i), "pipeline-1")).await.unwrap();
        }
        for i in 0..2 {
            manager.send_to_dlq(make_dlq_record(&format!("b-{}", i), "pipeline-2")).await.unwrap();
        }

        let stats = manager.get_stats().await;
        assert_eq!(stats.total_records, 5);
        assert_eq!(stats.per_sink.get("sink-1"), Some(&3));
        assert_eq!(stats.per_sink.get("sink-2"), Some(&2));
    }
}
