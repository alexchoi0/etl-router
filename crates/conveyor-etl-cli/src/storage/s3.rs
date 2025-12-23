use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use super::BackupStorage;

pub struct S3Storage {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3Storage {
    pub fn new(bucket: String, prefix: String) -> Self {
        let config = tokio::runtime::Handle::current().block_on(async {
            aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await
        });
        let client = Client::new(&config);

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
impl BackupStorage for S3Storage {
    async fn upload(&self, path: &str, data: &[u8]) -> Result<()> {
        let key = self.full_key(path);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
            .context("Failed to upload to S3")?;
        Ok(())
    }

    async fn download(&self, path: &str) -> Result<Vec<u8>> {
        let key = self.full_key(path);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .context("Failed to download from S3")?;

        let data = resp.body.collect().await?.into_bytes().to_vec();
        Ok(data)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let full_prefix = self.full_key(prefix);
        let resp = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&full_prefix)
            .delimiter("/")
            .send()
            .await
            .context("Failed to list S3 objects")?;

        let mut entries = Vec::new();

        if let Some(common_prefixes) = resp.common_prefixes {
            for cp in common_prefixes {
                if let Some(p) = cp.prefix {
                    let relative = p
                        .strip_prefix(&full_prefix)
                        .unwrap_or(&p)
                        .to_string();
                    if !relative.is_empty() {
                        entries.push(relative);
                    }
                }
            }
        }

        Ok(entries)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let key = self.full_key(path);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .context("Failed to delete S3 object")?;
        Ok(())
    }

    async fn delete_recursive(&self, prefix: &str) -> Result<()> {
        let full_prefix = self.full_key(prefix);

        let mut continuation_token: Option<String> = None;
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);

            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let resp = request.send().await.context("Failed to list objects")?;

            if let Some(contents) = resp.contents {
                for obj in contents {
                    if let Some(key) = obj.key {
                        self.client
                            .delete_object()
                            .bucket(&self.bucket)
                            .key(&key)
                            .send()
                            .await
                            .context("Failed to delete object")?;
                    }
                }
            }

            if resp.is_truncated == Some(true) {
                continuation_token = resp.next_continuation_token;
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
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.to_string().contains("NotFound") || e.to_string().contains("404") {
                    Ok(false)
                } else {
                    Err(anyhow!("Failed to check if object exists: {}", e))
                }
            }
        }
    }
}
