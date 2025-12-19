use tonic::Status;

pub trait IntoStatus {
    fn into_status(self) -> Status;
}

impl IntoStatus for anyhow::Error {
    fn into_status(self) -> Status {
        Status::internal(self.to_string())
    }
}

impl<T, E: Into<anyhow::Error>> IntoStatus for Result<T, E> {
    fn into_status(self) -> Status {
        match self {
            Ok(_) => Status::ok(""),
            Err(e) => Status::internal(e.into().to_string()),
        }
    }
}

pub trait ResultExt<T> {
    fn map_to_status(self) -> Result<T, Status>;
}

impl<T, E: std::fmt::Display> ResultExt<T> for Result<T, E> {
    fn map_to_status(self) -> Result<T, Status> {
        self.map_err(|e| Status::internal(e.to_string()))
    }
}
