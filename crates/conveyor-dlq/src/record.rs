use conveyor_proto::common::Record;
use prost_types::Timestamp;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    Unknown,
    TransformFailed,
    SinkFailed,
    ValidationFailed,
    Timeout,
    RateLimited,
    ServiceUnavailable,
    MaxRetriesExceeded,
    RecordTooLarge,
    MalformedRecord,
}

impl ErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::Unknown => "UNKNOWN",
            ErrorCode::TransformFailed => "TRANSFORM_FAILED",
            ErrorCode::SinkFailed => "SINK_FAILED",
            ErrorCode::ValidationFailed => "VALIDATION_FAILED",
            ErrorCode::Timeout => "TIMEOUT",
            ErrorCode::RateLimited => "RATE_LIMITED",
            ErrorCode::ServiceUnavailable => "SERVICE_UNAVAILABLE",
            ErrorCode::MaxRetriesExceeded => "MAX_RETRIES_EXCEEDED",
            ErrorCode::RecordTooLarge => "RECORD_TOO_LARGE",
            ErrorCode::MalformedRecord => "MALFORMED_RECORD",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorContext {
    pub code: ErrorCode,
    pub message: String,
    pub failed_stage_id: String,
    pub failed_stage_type: String,
    pub retry_count: u32,
    pub first_failure_time: u64,
    pub last_failure_time: u64,
    pub stack_trace: Option<String>,
}

impl ErrorContext {
    pub fn new(
        code: ErrorCode,
        message: impl Into<String>,
        stage_id: impl Into<String>,
        stage_type: impl Into<String>,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            code,
            message: message.into(),
            failed_stage_id: stage_id.into(),
            failed_stage_type: stage_type.into(),
            retry_count: 0,
            first_failure_time: now,
            last_failure_time: now,
            stack_trace: None,
        }
    }

    pub fn with_stack_trace(mut self, trace: impl Into<String>) -> Self {
        self.stack_trace = Some(trace.into());
        self
    }

    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
        self.last_failure_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
    }
}

#[derive(Debug, Clone)]
pub struct DeadLetterRecord {
    pub original_record: Record,
    pub pipeline_id: String,
    pub source_id: String,
    pub error: ErrorContext,
    pub dlq_timestamp: Timestamp,
}

impl DeadLetterRecord {
    pub fn new(
        record: Record,
        pipeline_id: impl Into<String>,
        source_id: impl Into<String>,
        error: ErrorContext,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();

        Self {
            original_record: record,
            pipeline_id: pipeline_id.into(),
            source_id: source_id.into(),
            error,
            dlq_timestamp: Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            },
        }
    }

    pub fn to_dlq_record(&self) -> Record {
        let mut record = self.original_record.clone();

        record.metadata.insert(
            "_dlq_error_code".to_string(),
            self.error.code.as_str().to_string(),
        );
        record.metadata.insert(
            "_dlq_error_message".to_string(),
            self.error.message.clone(),
        );
        record.metadata.insert(
            "_dlq_failed_stage".to_string(),
            self.error.failed_stage_id.clone(),
        );
        record.metadata.insert(
            "_dlq_failed_stage_type".to_string(),
            self.error.failed_stage_type.clone(),
        );
        record.metadata.insert(
            "_dlq_retry_count".to_string(),
            self.error.retry_count.to_string(),
        );
        record.metadata.insert(
            "_dlq_first_failure".to_string(),
            self.error.first_failure_time.to_string(),
        );
        record.metadata.insert(
            "_dlq_last_failure".to_string(),
            self.error.last_failure_time.to_string(),
        );
        record.metadata.insert(
            "_dlq_pipeline".to_string(),
            self.pipeline_id.clone(),
        );
        record.metadata.insert(
            "_dlq_source".to_string(),
            self.source_id.clone(),
        );
        record.metadata.insert(
            "_dlq_timestamp".to_string(),
            format!("{}.{}", self.dlq_timestamp.seconds, self.dlq_timestamp.nanos),
        );

        if let Some(ref trace) = self.error.stack_trace {
            record.metadata.insert("_dlq_stack_trace".to_string(), trace.clone());
        }

        record.record_type = format!("dlq.{}", record.record_type);

        record
    }

    pub fn age_ms(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let dlq_time = (self.dlq_timestamp.seconds as u64 * 1000)
            + (self.dlq_timestamp.nanos as u64 / 1_000_000);
        now.saturating_sub(dlq_time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use conveyor_proto::common::RecordId;

    fn make_record(id: &str) -> Record {
        Record {
            id: Some(RecordId {
                source_id: "test-source".to_string(),
                partition: 0,
                sequence_number: 1,
                idempotency_key: vec![],
            }),
            record_type: "user.event".to_string(),
            key: id.as_bytes().to_vec(),
            payload: br#"{"user_id": 123}"#.to_vec(),
            metadata: Default::default(),
            event_time: None,
            ingestion_time: None,
        }
    }

    #[test]
    fn test_dead_letter_record_creation() {
        let record = make_record("test-1");
        let error = ErrorContext::new(
            ErrorCode::TransformFailed,
            "Schema validation error",
            "validate-stage",
            "transform",
        );

        let dlq_record = DeadLetterRecord::new(
            record,
            "user-analytics",
            "kafka-source",
            error,
        );

        assert_eq!(dlq_record.pipeline_id, "user-analytics");
        assert_eq!(dlq_record.error.code, ErrorCode::TransformFailed);
    }

    #[test]
    fn test_to_dlq_record_adds_metadata() {
        let record = make_record("test-2");
        let error = ErrorContext::new(
            ErrorCode::SinkFailed,
            "Connection refused",
            "s3-sink",
            "sink",
        );

        let dlq_record = DeadLetterRecord::new(record, "pipeline-1", "source-1", error);
        let output = dlq_record.to_dlq_record();

        assert_eq!(output.record_type, "dlq.user.event");
        assert_eq!(
            output.metadata.get("_dlq_error_code"),
            Some(&"SINK_FAILED".to_string())
        );
        assert_eq!(
            output.metadata.get("_dlq_failed_stage"),
            Some(&"s3-sink".to_string())
        );
    }

    #[test]
    fn test_error_context_retry_increment() {
        let mut error = ErrorContext::new(
            ErrorCode::Timeout,
            "Request timed out",
            "transform-1",
            "transform",
        );

        assert_eq!(error.retry_count, 0);
        error.increment_retry();
        assert_eq!(error.retry_count, 1);
        error.increment_retry();
        assert_eq!(error.retry_count, 2);
    }
}
