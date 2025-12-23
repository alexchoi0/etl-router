use tonic::{Code, Status};

#[derive(Debug, Clone)]
pub enum GrpcError {
    NotFound { resource: &'static str, id: String },
    AlreadyExists { resource: &'static str, id: String },
    InvalidArgument { field: &'static str, reason: String },
    FailedPrecondition { reason: String },
    ResourceExhausted { reason: String },
    Unavailable { reason: String },
    NotLeader,
    Internal { reason: String },
}

impl GrpcError {
    pub fn service_not_found(id: impl Into<String>) -> Self {
        Self::NotFound { resource: "service", id: id.into() }
    }

    pub fn pipeline_not_found(id: impl Into<String>) -> Self {
        Self::NotFound { resource: "pipeline", id: id.into() }
    }

    pub fn group_not_found(id: impl Into<String>) -> Self {
        Self::NotFound { resource: "group", id: id.into() }
    }

    pub fn checkpoint_not_found(id: impl Into<String>) -> Self {
        Self::NotFound { resource: "checkpoint", id: id.into() }
    }

    pub fn sidecar_not_found(id: impl Into<String>) -> Self {
        Self::NotFound { resource: "sidecar", id: id.into() }
    }

    pub fn group_already_exists(id: impl Into<String>) -> Self {
        Self::AlreadyExists { resource: "group", id: id.into() }
    }

    pub fn pipeline_already_exists(id: impl Into<String>) -> Self {
        Self::AlreadyExists { resource: "pipeline", id: id.into() }
    }

    pub fn missing_field(field: &'static str) -> Self {
        Self::InvalidArgument { field, reason: "required field is missing".to_string() }
    }

    pub fn invalid_field(field: &'static str, reason: impl Into<String>) -> Self {
        Self::InvalidArgument { field, reason: reason.into() }
    }

    pub fn buffer_full(reason: impl Into<String>) -> Self {
        Self::ResourceExhausted { reason: reason.into() }
    }

    pub fn pipeline_disabled(id: impl Into<String>) -> Self {
        Self::FailedPrecondition { reason: format!("pipeline is disabled: {}", id.into()) }
    }

    pub fn not_leader() -> Self {
        Self::NotLeader
    }

    pub fn unavailable(reason: impl Into<String>) -> Self {
        Self::Unavailable { reason: reason.into() }
    }

    pub fn internal(reason: impl Into<String>) -> Self {
        Self::Internal { reason: reason.into() }
    }
}

impl From<GrpcError> for Status {
    fn from(err: GrpcError) -> Self {
        match err {
            GrpcError::NotFound { resource, id } => {
                Status::new(Code::NotFound, format!("{} not found: {}", resource, id))
            }
            GrpcError::AlreadyExists { resource, id } => {
                Status::new(Code::AlreadyExists, format!("{} already exists: {}", resource, id))
            }
            GrpcError::InvalidArgument { field, reason } => {
                Status::new(Code::InvalidArgument, format!("{}: {}", field, reason))
            }
            GrpcError::FailedPrecondition { reason } => {
                Status::new(Code::FailedPrecondition, reason)
            }
            GrpcError::ResourceExhausted { reason } => {
                Status::new(Code::ResourceExhausted, reason)
            }
            GrpcError::Unavailable { reason } => {
                Status::new(Code::Unavailable, reason)
            }
            GrpcError::NotLeader => {
                Status::new(Code::Unavailable, "not the leader, retry on another node")
            }
            GrpcError::Internal { reason } => {
                Status::new(Code::Internal, reason)
            }
        }
    }
}

pub trait IntoGrpcError {
    fn into_grpc_error(self) -> GrpcError;
}

pub trait ResultExt<T> {
    fn map_to_status(self) -> Result<T, Status>;
}

impl<T, E: std::fmt::Display> ResultExt<T> for Result<T, E> {
    fn map_to_status(self) -> Result<T, Status> {
        self.map_err(|e| classify_error(&e.to_string()))
    }
}

fn classify_error(msg: &str) -> Status {
    let msg_lower = msg.to_lowercase();

    if msg_lower.contains("not found") {
        if msg_lower.contains("service") {
            return GrpcError::service_not_found(extract_id(msg)).into();
        }
        if msg_lower.contains("pipeline") {
            return GrpcError::pipeline_not_found(extract_id(msg)).into();
        }
        if msg_lower.contains("group") {
            return GrpcError::group_not_found(extract_id(msg)).into();
        }
        if msg_lower.contains("member") || msg_lower.contains("sidecar") {
            return GrpcError::sidecar_not_found(extract_id(msg)).into();
        }
        return Status::new(Code::NotFound, msg);
    }

    if msg_lower.contains("already exists") {
        if msg_lower.contains("group") {
            return GrpcError::group_already_exists(extract_id(msg)).into();
        }
        return Status::new(Code::AlreadyExists, msg);
    }

    if msg_lower.contains("buffer full") || msg_lower.contains("exhausted") {
        return GrpcError::buffer_full(msg).into();
    }

    if msg_lower.contains("not the leader") {
        return GrpcError::not_leader().into();
    }

    if msg_lower.contains("disabled") {
        return Status::new(Code::FailedPrecondition, msg);
    }

    if msg_lower.contains("timed out") || msg_lower.contains("timeout") {
        return Status::new(Code::DeadlineExceeded, msg);
    }

    if msg_lower.contains("invalid") || msg_lower.contains("required") {
        return Status::new(Code::InvalidArgument, msg);
    }

    Status::new(Code::Internal, msg)
}

fn extract_id(msg: &str) -> String {
    if let Some(start) = msg.find(':') {
        msg[start + 1..].trim().to_string()
    } else {
        "unknown".to_string()
    }
}

pub trait IntoStatus {
    fn into_status(self) -> Status;
}

impl IntoStatus for anyhow::Error {
    fn into_status(self) -> Status {
        classify_error(&self.to_string())
    }
}

impl IntoStatus for GrpcError {
    fn into_status(self) -> Status {
        self.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_found_errors() {
        let status: Status = GrpcError::service_not_found("svc-123").into();
        assert_eq!(status.code(), Code::NotFound);
        assert!(status.message().contains("service not found"));
        assert!(status.message().contains("svc-123"));
    }

    #[test]
    fn test_already_exists_errors() {
        let status: Status = GrpcError::group_already_exists("group-1").into();
        assert_eq!(status.code(), Code::AlreadyExists);
        assert!(status.message().contains("group already exists"));
    }

    #[test]
    fn test_invalid_argument_errors() {
        let status: Status = GrpcError::missing_field("identity").into();
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("identity"));
    }

    #[test]
    fn test_resource_exhausted_errors() {
        let status: Status = GrpcError::buffer_full("stage buffer full").into();
        assert_eq!(status.code(), Code::ResourceExhausted);
    }

    #[test]
    fn test_not_leader_error() {
        let status: Status = GrpcError::not_leader().into();
        assert_eq!(status.code(), Code::Unavailable);
        assert!(status.message().contains("not the leader"));
    }

    #[test]
    fn test_classify_error() {
        assert_eq!(classify_error("Service not found: svc-1").code(), Code::NotFound);
        assert_eq!(classify_error("Group already exists: g1").code(), Code::AlreadyExists);
        assert_eq!(classify_error("Global buffer full").code(), Code::ResourceExhausted);
        assert_eq!(classify_error("Not the leader").code(), Code::Unavailable);
        assert_eq!(classify_error("Pipeline is disabled: p1").code(), Code::FailedPrecondition);
        assert_eq!(classify_error("Timed out waiting").code(), Code::DeadlineExceeded);
        assert_eq!(classify_error("Something else broke").code(), Code::Internal);
    }
}
