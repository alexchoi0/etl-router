use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use conveyor_proto::backup::{
    backup_service_server::BackupService,
    CompressionType, CreateSnapshotRequest, CreateSnapshotResponse, DataChunk,
    DeleteSnapshotRequest, DeleteSnapshotResponse, GetStateMetadataRequest,
    ListSnapshotsRequest, ListSnapshotsResponse, RestoreSnapshotRequest,
    RestoreSnapshotResponse, SnapshotInfo, StateMetadata, StreamSnapshotRequest,
    UploadSnapshotResponse,
};

use super::state_machine::RouterState;
use super::storage::LogStorage;

pub struct BackupServiceImpl {
    storage: Arc<LogStorage>,
    state: Arc<RwLock<RouterState>>,
    snapshot_dir: PathBuf,
}

impl BackupServiceImpl {
    pub fn new(
        storage: Arc<LogStorage>,
        state: Arc<RwLock<RouterState>>,
        snapshot_dir: PathBuf,
    ) -> Self {
        std::fs::create_dir_all(&snapshot_dir).ok();
        Self {
            storage,
            state,
            snapshot_dir,
        }
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn snapshot_path(&self, id: &str) -> PathBuf {
        self.snapshot_dir.join(id)
    }

    async fn get_state_counts(&self) -> (u32, u32) {
        let state = self.state.read().await;
        (state.services.len() as u32, state.pipelines.len() as u32)
    }
}

#[tonic::async_trait]
impl BackupService for BackupServiceImpl {
    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let req = request.into_inner();
        let id = if req.snapshot_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            req.snapshot_id
        };

        let path = self.snapshot_path(&id);
        if path.exists() {
            return Err(Status::already_exists(format!(
                "Snapshot already exists: {}",
                id
            )));
        }

        let size_bytes = self
            .storage
            .create_db_checkpoint(&path)
            .map_err(|e| Status::internal(format!("Failed to create snapshot: {}", e)))?;

        Ok(Response::new(CreateSnapshotResponse {
            snapshot_id: id,
            size_bytes,
            commit_index: self.storage.commit_index(),
            term: self.storage.last_term(),
            timestamp: Self::now_millis(),
        }))
    }

    type StreamSnapshotStream = ReceiverStream<Result<DataChunk, Status>>;

    async fn stream_snapshot(
        &self,
        request: Request<StreamSnapshotRequest>,
    ) -> Result<Response<Self::StreamSnapshotStream>, Status> {
        let req = request.into_inner();
        let path = self.snapshot_path(&req.snapshot_id);

        if !path.exists() {
            return Err(Status::not_found(format!(
                "Snapshot not found: {}",
                req.snapshot_id
            )));
        }

        let (tx, rx) = mpsc::channel(32);
        let compression = req.compression();

        tokio::spawn(async move {
            if let Err(e) = stream_directory_as_tar(&path, compression, tx.clone()).await {
                tx.send(Err(Status::internal(e.to_string()))).await.ok();
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn delete_snapshot(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let req = request.into_inner();
        let path = self.snapshot_path(&req.snapshot_id);

        if !path.exists() {
            return Ok(Response::new(DeleteSnapshotResponse { success: true }));
        }

        std::fs::remove_dir_all(&path)
            .map_err(|e| Status::internal(format!("Failed to delete snapshot: {}", e)))?;

        Ok(Response::new(DeleteSnapshotResponse { success: true }))
    }

    async fn upload_snapshot(
        &self,
        request: Request<tonic::Streaming<DataChunk>>,
    ) -> Result<Response<UploadSnapshotResponse>, Status> {
        let mut stream = request.into_inner();
        let id = uuid::Uuid::new_v4().to_string();
        let archive_path = self.snapshot_dir.join(format!("{}.tar.zst", id));
        let extract_path = self.snapshot_path(&id);

        let mut file = tokio::fs::File::create(&archive_path)
            .await
            .map_err(|e| Status::internal(format!("Failed to create archive file: {}", e)))?;

        use tokio::io::AsyncWriteExt;
        while let Some(chunk) = stream.message().await? {
            file.write_all(&chunk.data)
                .await
                .map_err(|e| Status::internal(format!("Failed to write chunk: {}", e)))?;
        }
        file.flush().await.ok();
        drop(file);

        extract_tar_zstd(&archive_path, &extract_path)
            .map_err(|e| Status::internal(format!("Failed to extract archive: {}", e)))?;

        std::fs::remove_file(&archive_path).ok();

        Ok(Response::new(UploadSnapshotResponse {
            success: true,
            snapshot_id: id,
        }))
    }

    async fn restore_snapshot(
        &self,
        request: Request<RestoreSnapshotRequest>,
    ) -> Result<Response<RestoreSnapshotResponse>, Status> {
        let req = request.into_inner();
        let path = self.snapshot_path(&req.snapshot_id);

        if !path.exists() {
            return Err(Status::not_found(format!(
                "Snapshot not found: {}",
                req.snapshot_id
            )));
        }

        if req.validate_only {
            let valid = validate_snapshot(&path);
            return Ok(Response::new(RestoreSnapshotResponse {
                success: valid,
                message: if valid {
                    "Snapshot is valid".to_string()
                } else {
                    "Snapshot validation failed".to_string()
                },
                state: None,
            }));
        }

        Err(Status::unimplemented(
            "Full restore requires server restart. Use validate_only=true to check snapshot validity.",
        ))
    }

    async fn get_state_metadata(
        &self,
        _request: Request<GetStateMetadataRequest>,
    ) -> Result<Response<StateMetadata>, Status> {
        let (service_count, pipeline_count) = self.get_state_counts().await;

        Ok(Response::new(StateMetadata {
            commit_index: self.storage.commit_index(),
            term: self.storage.last_term(),
            service_count,
            pipeline_count,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }

    async fn list_snapshots(
        &self,
        _request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let mut snapshots = Vec::new();

        if let Ok(entries) = std::fs::read_dir(&self.snapshot_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    let id = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("")
                        .to_string();

                    if id.is_empty() || id.ends_with(".tar.zst") {
                        continue;
                    }

                    let metadata = entry.metadata().ok();
                    let created_at = metadata
                        .and_then(|m| m.created().ok())
                        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);

                    let size_bytes = dir_size(&path).unwrap_or(0);

                    snapshots.push(SnapshotInfo {
                        snapshot_id: id,
                        size_bytes,
                        commit_index: 0,
                        term: 0,
                        created_at,
                    });
                }
            }
        }

        snapshots.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        Ok(Response::new(ListSnapshotsResponse { snapshots }))
    }
}

async fn stream_directory_as_tar(
    path: &std::path::Path,
    compression: CompressionType,
    tx: mpsc::Sender<Result<DataChunk, Status>>,
) -> Result<()> {
    use std::io::Write;

    let mut buffer = Vec::new();

    {
        let encoder: Box<dyn Write> = match compression {
            CompressionType::CompressionZstd => Box::new(
                zstd::Encoder::new(&mut buffer, 3)
                    .map_err(|e| anyhow::anyhow!("Failed to create zstd encoder: {}", e))?
                    .auto_finish(),
            ),
            CompressionType::CompressionGzip => Box::new(flate2::write::GzEncoder::new(
                &mut buffer,
                flate2::Compression::default(),
            )),
            CompressionType::CompressionNone => Box::new(&mut buffer as &mut dyn Write),
        };

        let mut tar = tar::Builder::new(encoder);
        tar.append_dir_all(".", path)
            .map_err(|e| anyhow::anyhow!("Failed to create tar archive: {}", e))?;
        tar.finish()
            .map_err(|e| anyhow::anyhow!("Failed to finish tar archive: {}", e))?;
    }

    let mut offset = 0u64;
    for chunk in buffer.chunks(64 * 1024) {
        let is_last = offset + chunk.len() as u64 >= buffer.len() as u64;
        tx.send(Ok(DataChunk {
            data: chunk.to_vec(),
            offset,
            is_last,
        }))
        .await
        .map_err(|_| anyhow::anyhow!("Failed to send chunk"))?;
        offset += chunk.len() as u64;
    }

    Ok(())
}

fn extract_tar_zstd(archive_path: &std::path::Path, extract_path: &std::path::Path) -> Result<()> {
    

    let file = std::fs::File::open(archive_path)?;
    let decoder = zstd::Decoder::new(file)?;
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(extract_path)?;
    Ok(())
}

fn validate_snapshot(path: &std::path::Path) -> bool {
    if !path.exists() || !path.is_dir() {
        return false;
    }

    let current_file = path.join("CURRENT");
    current_file.exists()
}

fn dir_size(path: &std::path::Path) -> Result<u64> {
    let mut size = 0u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            size += metadata.len();
        } else if metadata.is_dir() {
            size += dir_size(&entry.path())?;
        }
    }
    Ok(size)
}
