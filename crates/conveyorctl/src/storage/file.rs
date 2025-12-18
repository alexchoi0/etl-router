use anyhow::{Context, Result};
use async_trait::async_trait;
use std::path::PathBuf;
use tokio::fs;

use super::BackupStorage;

pub struct FileStorage {
    base_path: PathBuf,
}

impl FileStorage {
    pub fn new(path: String) -> Self {
        Self {
            base_path: PathBuf::from(path),
        }
    }

    fn full_path(&self, path: &str) -> PathBuf {
        self.base_path.join(path)
    }
}

#[async_trait]
impl BackupStorage for FileStorage {
    async fn upload(&self, path: &str, data: &[u8]) -> Result<()> {
        let full_path = self.full_path(path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)
                .await
                .context("Failed to create parent directories")?;
        }
        fs::write(&full_path, data)
            .await
            .context("Failed to write file")?;
        Ok(())
    }

    async fn download(&self, path: &str) -> Result<Vec<u8>> {
        let full_path = self.full_path(path);
        let data = fs::read(&full_path)
            .await
            .context("Failed to read file")?;
        Ok(data)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let full_path = self.full_path(prefix);
        if !full_path.exists() {
            return Ok(Vec::new());
        }

        let mut entries = Vec::new();
        let mut read_dir = fs::read_dir(&full_path)
            .await
            .context("Failed to read directory")?;

        while let Some(entry) = read_dir.next_entry().await? {
            if entry.path().is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    entries.push(format!("{}/", name));
                }
            }
        }

        entries.sort();
        Ok(entries)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let full_path = self.full_path(path);
        if full_path.exists() {
            if full_path.is_dir() {
                fs::remove_dir_all(&full_path)
                    .await
                    .context("Failed to remove directory")?;
            } else {
                fs::remove_file(&full_path)
                    .await
                    .context("Failed to remove file")?;
            }
        }
        Ok(())
    }

    async fn delete_recursive(&self, prefix: &str) -> Result<()> {
        let full_path = self.full_path(prefix);
        if full_path.exists() && full_path.is_dir() {
            fs::remove_dir_all(&full_path)
                .await
                .context("Failed to remove directory recursively")?;
        }
        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        let full_path = self.full_path(path);
        Ok(full_path.exists())
    }
}
