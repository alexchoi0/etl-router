use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("Finalizer error: {0}")]
    FinalizerError(String),

    #[error("Dependency not found: {0}")]
    DependencyNotFound(String),

    #[error("Router not found in namespace: {0}")]
    RouterNotFound(String),

    #[error("gRPC error: {0}")]
    GrpcError(#[from] tonic::Status),

    #[error("gRPC transport error: {0}")]
    GrpcTransportError(#[from] tonic::transport::Error),

    #[error("Invalid URI: {0}")]
    InvalidUri(String),

    #[error("Router returned error: {0}")]
    RouterError(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Missing field: {0}")]
    MissingField(String),

    #[error("Resource not ready: {0}")]
    NotReady(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::KubeError(_) => true,
            Error::GrpcError(_) => true,
            Error::GrpcTransportError(_) => true,
            Error::RouterNotFound(_) => true,
            Error::NotReady(_) => true,
            _ => false,
        }
    }
}
