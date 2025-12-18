use thiserror::Error;

#[derive(Debug, Error)]
pub enum DlqError {
    #[error("DLQ buffer full for sink: {sink_id}")]
    BufferFull { sink_id: String },

    #[error("No error sink configured for pipeline: {pipeline_id}")]
    NoErrorSink { pipeline_id: String },

    #[error("Max retries exceeded: {retries} >= {max_retries}")]
    MaxRetriesExceeded { retries: u32, max_retries: u32 },

    #[error("Record expired after {age_ms}ms (max: {max_age_ms}ms)")]
    RecordExpired { age_ms: u64, max_age_ms: u64 },

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, DlqError>;
