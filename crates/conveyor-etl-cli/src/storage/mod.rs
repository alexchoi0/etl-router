pub mod file;
pub mod gcs;
pub mod s3;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMetadata {
    pub backup_id: String,
    pub router_endpoint: String,
    pub commit_index: u64,
    pub term: u64,
    pub size_bytes: u64,
    pub created_at: String,
    pub compression: String,
    pub version: String,
}

#[async_trait]
pub trait BackupStorage: Send + Sync {
    async fn upload(&self, path: &str, data: &[u8]) -> Result<()>;

    async fn download(&self, path: &str) -> Result<Vec<u8>>;

    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    #[allow(dead_code)]
    async fn delete(&self, path: &str) -> Result<()>;

    async fn delete_recursive(&self, prefix: &str) -> Result<()>;

    #[allow(dead_code)]
    async fn exists(&self, path: &str) -> Result<bool>;
}

pub async fn read_metadata(storage: &dyn BackupStorage, backup_id: &str) -> Result<BackupMetadata> {
    let path = format!("{}/metadata.json", backup_id);
    let data = storage.download(&path).await?;
    let metadata: BackupMetadata = serde_json::from_slice(&data)?;
    Ok(metadata)
}

pub async fn write_metadata(
    storage: &dyn BackupStorage,
    backup_id: &str,
    metadata: &BackupMetadata,
) -> Result<()> {
    let path = format!("{}/metadata.json", backup_id);
    let data = serde_json::to_vec_pretty(metadata)?;
    storage.upload(&path, &data).await
}

pub fn parse_storage_url(url: &str) -> Result<Box<dyn BackupStorage>> {
    if url.starts_with("s3://") {
        let url = url.strip_prefix("s3://").unwrap();
        let (bucket, prefix) = url.split_once('/').unwrap_or((url, ""));
        Ok(Box::new(s3::S3Storage::new(bucket.to_string(), prefix.to_string())))
    } else if url.starts_with("gs://") {
        let url = url.strip_prefix("gs://").unwrap();
        let (bucket, prefix) = url.split_once('/').unwrap_or((url, ""));
        Ok(Box::new(gcs::GcsStorage::new(bucket.to_string(), prefix.to_string())))
    } else if url.starts_with("file://") {
        let path = url.strip_prefix("file://").unwrap();
        Ok(Box::new(file::FileStorage::new(path.to_string())))
    } else {
        Ok(Box::new(file::FileStorage::new(url.to_string())))
    }
}

pub fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
