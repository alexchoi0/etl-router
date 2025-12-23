use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::delete::DeleteObjectRequest;
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::list::ListObjectsRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};

use super::BackupStorage;

pub struct GcsStorage {
    client: Client,
    bucket: String,
    prefix: String,
}

impl GcsStorage {
    pub fn new(bucket: String, prefix: String) -> Self {
        let client = tokio::runtime::Handle::current().block_on(async {
            let config = ClientConfig::default().with_auth().await.unwrap_or_default();
            Client::new(config)
        });

        Self {
            client,
            bucket,
            prefix,
        }
    }

    fn full_key(&self, path: &str) -> String {
        if self.prefix.is_empty() {
            path.to_string()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), path)
        }
    }
}

#[async_trait]
impl BackupStorage for GcsStorage {
    async fn upload(&self, path: &str, data: &[u8]) -> Result<()> {
        let key = self.full_key(path);
        self.client
            .upload_object(
                &UploadObjectRequest {
                    bucket: self.bucket.clone(),
                    ..Default::default()
                },
                data.to_vec(),
                &UploadType::Simple(Media::new(key)),
            )
            .await
            .context("Failed to upload to GCS")?;
        Ok(())
    }

    async fn download(&self, path: &str) -> Result<Vec<u8>> {
        let key = self.full_key(path);
        let data = self
            .client
            .download_object(
                &GetObjectRequest {
                    bucket: self.bucket.clone(),
                    object: key,
                    ..Default::default()
                },
                &Range::default(),
            )
            .await
            .context("Failed to download from GCS")?;
        Ok(data)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let full_prefix = self.full_key(prefix);
        let resp = self
            .client
            .list_objects(&ListObjectsRequest {
                bucket: self.bucket.clone(),
                prefix: Some(full_prefix.clone()),
                delimiter: Some("/".to_string()),
                ..Default::default()
            })
            .await
            .context("Failed to list GCS objects")?;

        let mut entries = Vec::new();

        if let Some(prefixes) = resp.prefixes {
            for p in prefixes {
                let relative = p.strip_prefix(&full_prefix).unwrap_or(&p).to_string();
                if !relative.is_empty() {
                    entries.push(relative);
                }
            }
        }

        Ok(entries)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let key = self.full_key(path);
        self.client
            .delete_object(&DeleteObjectRequest {
                bucket: self.bucket.clone(),
                object: key,
                ..Default::default()
            })
            .await
            .context("Failed to delete GCS object")?;
        Ok(())
    }

    async fn delete_recursive(&self, prefix: &str) -> Result<()> {
        let full_prefix = self.full_key(prefix);

        let mut page_token: Option<String> = None;
        loop {
            let resp = self
                .client
                .list_objects(&ListObjectsRequest {
                    bucket: self.bucket.clone(),
                    prefix: Some(full_prefix.clone()),
                    page_token: page_token.clone(),
                    ..Default::default()
                })
                .await
                .context("Failed to list objects")?;

            if let Some(items) = resp.items {
                for obj in items {
                    self.client
                        .delete_object(&DeleteObjectRequest {
                            bucket: self.bucket.clone(),
                            object: obj.name,
                            ..Default::default()
                        })
                        .await
                        .context("Failed to delete object")?;
                }
            }

            if resp.next_page_token.is_some() {
                page_token = resp.next_page_token;
            } else {
                break;
            }
        }

        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        let key = self.full_key(path);
        match self
            .client
            .get_object(&GetObjectRequest {
                bucket: self.bucket.clone(),
                object: key,
                ..Default::default()
            })
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("NotFound") || err_str.contains("404") {
                    Ok(false)
                } else {
                    Err(anyhow!("Failed to check if object exists: {}", e))
                }
            }
        }
    }
}
