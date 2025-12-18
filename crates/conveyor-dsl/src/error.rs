use thiserror::Error;

#[derive(Debug, Error)]
pub enum DslError {
    #[error("YAML parse error: {0}")]
    YamlParseError(#[from] serde_yaml::Error),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Invalid pipeline '{pipeline_id}': {message}")]
    InvalidPipeline {
        pipeline_id: String,
        message: String,
    },

    #[error("Invalid stage '{stage_id}' in pipeline '{pipeline_id}': {message}")]
    InvalidStage {
        pipeline_id: String,
        stage_id: String,
        message: String,
    },

    #[error("Invalid edge in pipeline '{pipeline_id}': {message}")]
    InvalidEdge {
        pipeline_id: String,
        message: String,
    },

    #[error("Cycle detected in pipeline '{pipeline_id}': {cycle}")]
    CycleDetected {
        pipeline_id: String,
        cycle: String,
    },

    #[error("Unknown stage '{stage_id}' referenced in pipeline '{pipeline_id}'")]
    UnknownStage {
        pipeline_id: String,
        stage_id: String,
    },

    #[error("Invalid condition: {0}")]
    InvalidCondition(String),

    #[error("Invalid config: {0}")]
    InvalidConfig(String),

    #[error("Unsupported DSL version: {0}")]
    UnsupportedVersion(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, DslError>;
