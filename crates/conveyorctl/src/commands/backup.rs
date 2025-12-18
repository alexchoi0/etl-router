use anyhow::{anyhow, Context, Result};
use clap::{Args, Subcommand};
use std::io::Write;
use tabled::{Table, Tabled};
use tonic::transport::Channel;

use conveyor_proto::backup::{
    backup_service_client::BackupServiceClient, CompressionType, CreateSnapshotRequest,
    DeleteSnapshotRequest, RestoreSnapshotRequest,
    StreamSnapshotRequest,
};

use crate::storage::{format_size, parse_storage_url, read_metadata, write_metadata, BackupMetadata};

use super::Context as AppContext;

#[derive(Tabled)]
struct BackupRow {
    #[tabled(rename = "ID")]
    id: String,
    #[tabled(rename = "CREATED")]
    created: String,
    #[tabled(rename = "SIZE")]
    size: String,
    #[tabled(rename = "ROUTER")]
    router: String,
}

#[derive(Args)]
pub struct BackupArgs {
    #[command(subcommand)]
    pub command: BackupCommand,
}

#[derive(Subcommand)]
pub enum BackupCommand {
    Create(CreateBackupArgs),
    List(ListBackupsArgs),
    Describe(DescribeBackupArgs),
    Delete(DeleteBackupArgs),
    Restore(RestoreArgs),
    Cleanup(CleanupArgs),
}

#[derive(Args)]
pub struct CreateBackupArgs {
    #[arg(short, long, default_value = "localhost:9090")]
    pub router: String,

    #[arg(short, long)]
    pub dest: String,

    #[arg(short, long, default_value = "zstd")]
    pub compression: String,
}

#[derive(Args)]
pub struct ListBackupsArgs {
    #[arg(short, long)]
    pub dest: String,

    #[arg(short, long, default_value = "20")]
    pub limit: usize,

    #[arg(short, long, default_value = "table")]
    pub output: String,
}

#[derive(Args)]
pub struct DescribeBackupArgs {
    pub backup_id: String,

    #[arg(short, long)]
    pub source: String,

    #[arg(short, long, default_value = "yaml")]
    pub output: String,
}

#[derive(Args)]
pub struct DeleteBackupArgs {
    pub backup_id: String,

    #[arg(short, long)]
    pub dest: String,

    #[arg(short, long)]
    pub force: bool,
}

#[derive(Args)]
pub struct RestoreArgs {
    pub backup_id: String,

    #[arg(short, long)]
    pub source: String,

    #[arg(short, long)]
    pub target: Option<String>,

    #[arg(long)]
    pub validate_only: bool,

    #[arg(short, long)]
    pub yes: bool,
}

#[derive(Args)]
pub struct CleanupArgs {
    #[arg(short, long)]
    pub dest: String,

    #[arg(short, long, default_value = "10")]
    pub keep: usize,

    #[arg(short, long)]
    pub yes: bool,
}

pub async fn run(_ctx: &AppContext, args: BackupArgs) -> Result<()> {
    match args.command {
        BackupCommand::Create(args) => create_backup(args).await,
        BackupCommand::List(args) => list_backups(args).await,
        BackupCommand::Describe(args) => describe_backup(args).await,
        BackupCommand::Delete(args) => delete_backup(args).await,
        BackupCommand::Restore(args) => restore(args).await,
        BackupCommand::Cleanup(args) => cleanup(args).await,
    }
}

fn parse_compression(s: &str) -> CompressionType {
    match s.to_lowercase().as_str() {
        "zstd" => CompressionType::CompressionZstd,
        "gzip" | "gz" => CompressionType::CompressionGzip,
        "none" => CompressionType::CompressionNone,
        _ => CompressionType::CompressionZstd,
    }
}

async fn connect_to_router(endpoint: &str) -> Result<BackupServiceClient<Channel>> {
    let endpoint = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{}", endpoint)
    };

    BackupServiceClient::connect(endpoint)
        .await
        .context("Failed to connect to router")
}

async fn create_backup(args: CreateBackupArgs) -> Result<()> {
    let backup_id = uuid::Uuid::new_v4().to_string();
    let storage = parse_storage_url(&args.dest)?;
    let compression = parse_compression(&args.compression);

    let mut client = connect_to_router(&args.router).await?;

    println!("Creating snapshot on router...");
    let snapshot = client
        .create_snapshot(CreateSnapshotRequest {
            snapshot_id: backup_id.clone(),
        })
        .await
        .context("Failed to create snapshot")?
        .into_inner();

    println!("Streaming snapshot to {}...", args.dest);
    let mut stream = client
        .stream_snapshot(StreamSnapshotRequest {
            snapshot_id: backup_id.clone(),
            compression: compression as i32,
        })
        .await
        .context("Failed to stream snapshot")?
        .into_inner();

    let checkpoint_path = format!("{}/checkpoint.tar.zst", backup_id);
    let mut data = Vec::new();

    while let Some(chunk) = stream.message().await? {
        data.extend_from_slice(&chunk.data);
        if chunk.is_last {
            break;
        }
    }

    storage.upload(&checkpoint_path, &data).await?;

    let metadata = BackupMetadata {
        backup_id: backup_id.clone(),
        router_endpoint: args.router.clone(),
        commit_index: snapshot.commit_index,
        term: snapshot.term,
        size_bytes: data.len() as u64,
        created_at: chrono::Utc::now().to_rfc3339(),
        compression: args.compression.clone(),
        version: "1.0".to_string(),
    };
    write_metadata(storage.as_ref(), &backup_id, &metadata).await?;

    client
        .delete_snapshot(DeleteSnapshotRequest {
            snapshot_id: backup_id.clone(),
        })
        .await
        .ok();

    println!("Backup created: {}", backup_id);
    println!("  Commit index: {}", snapshot.commit_index);
    println!("  Size: {}", format_size(data.len() as u64));

    Ok(())
}

async fn list_backups(args: ListBackupsArgs) -> Result<()> {
    let storage = parse_storage_url(&args.dest)?;
    let backups = storage.list("").await?;

    let mut metadatas = Vec::new();
    for backup_dir in backups.iter().take(args.limit * 2) {
        let backup_id = backup_dir.trim_end_matches('/');
        if let Ok(meta) = read_metadata(storage.as_ref(), backup_id).await {
            metadatas.push(meta);
        }
    }

    metadatas.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    metadatas.truncate(args.limit);

    if args.output == "json" {
        println!("{}", serde_json::to_string_pretty(&metadatas)?);
        return Ok(());
    }

    if metadatas.is_empty() {
        println!("No backups found");
        return Ok(());
    }

    let rows: Vec<BackupRow> = metadatas
        .iter()
        .map(|meta| BackupRow {
            id: meta.backup_id[..12.min(meta.backup_id.len())].to_string(),
            created: meta.created_at.clone(),
            size: format_size(meta.size_bytes),
            router: meta.router_endpoint.clone(),
        })
        .collect();

    println!("{}", Table::new(rows));

    Ok(())
}

async fn describe_backup(args: DescribeBackupArgs) -> Result<()> {
    let storage = parse_storage_url(&args.source)?;
    let metadata = read_metadata(storage.as_ref(), &args.backup_id).await?;

    if args.output == "json" {
        println!("{}", serde_json::to_string_pretty(&metadata)?);
    } else {
        println!("{}", serde_yaml::to_string(&metadata)?);
    }

    Ok(())
}

async fn delete_backup(args: DeleteBackupArgs) -> Result<()> {
    let storage = parse_storage_url(&args.dest)?;

    if !args.force {
        print!("Delete backup {}? [y/N] ", args.backup_id);
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted");
            return Ok(());
        }
    }

    storage.delete_recursive(&args.backup_id).await?;
    println!("Deleted backup: {}", args.backup_id);

    Ok(())
}

async fn restore(args: RestoreArgs) -> Result<()> {
    let storage = parse_storage_url(&args.source)?;

    let metadata = read_metadata(storage.as_ref(), &args.backup_id).await?;

    let target = args.target.unwrap_or(metadata.router_endpoint.clone());

    println!("Restoring backup {} to {}", args.backup_id, target);
    println!("  Commit index: {}", metadata.commit_index);
    println!("  Created at: {}", metadata.created_at);

    if !args.yes && !args.validate_only {
        print!("Continue? [y/N] ");
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted");
            return Ok(());
        }
    }

    let mut client = connect_to_router(&target).await?;

    println!("Downloading checkpoint...");
    let checkpoint_path = format!("{}/checkpoint.tar.zst", args.backup_id);
    let data = storage.download(&checkpoint_path).await?;

    println!("Uploading to router...");
    let _upload_id = uuid::Uuid::new_v4().to_string();

    use conveyor_proto::backup::DataChunk;
    use tokio_stream::iter;

    let chunks: Vec<_> = data
        .chunks(64 * 1024)
        .enumerate()
        .map(|(i, chunk)| {
            let is_last = (i + 1) * 64 * 1024 >= data.len();
            DataChunk {
                data: chunk.to_vec(),
                offset: (i * 64 * 1024) as u64,
                is_last,
            }
        })
        .collect();

    let upload_resp = client
        .upload_snapshot(iter(chunks))
        .await
        .context("Failed to upload checkpoint")?
        .into_inner();

    if !upload_resp.success {
        return Err(anyhow!("Failed to upload checkpoint to router"));
    }

    println!("Restoring state...");
    let restore_resp = client
        .restore_snapshot(RestoreSnapshotRequest {
            snapshot_id: upload_resp.snapshot_id,
            validate_only: args.validate_only,
        })
        .await
        .context("Failed to restore snapshot")?
        .into_inner();

    if restore_resp.success {
        if args.validate_only {
            println!("Validation passed: {}", restore_resp.message);
        } else {
            println!("Restore completed successfully");
        }
        if let Some(state) = restore_resp.state {
            println!("  Services: {}", state.service_count);
            println!("  Pipelines: {}", state.pipeline_count);
        }
    } else {
        println!("Restore failed: {}", restore_resp.message);
    }

    Ok(())
}

async fn cleanup(args: CleanupArgs) -> Result<()> {
    let storage = parse_storage_url(&args.dest)?;
    let backups = storage.list("").await?;

    let mut metadatas: Vec<(String, BackupMetadata)> = Vec::new();
    for backup_dir in &backups {
        let backup_id = backup_dir.trim_end_matches('/');
        if let Ok(meta) = read_metadata(storage.as_ref(), backup_id).await {
            metadatas.push((backup_id.to_string(), meta));
        }
    }

    metadatas.sort_by(|a, b| b.1.created_at.cmp(&a.1.created_at));

    let to_delete: Vec<_> = metadatas.iter().skip(args.keep).collect();

    if to_delete.is_empty() {
        println!("No backups to delete (keeping {})", args.keep);
        return Ok(());
    }

    println!("Deleting {} old backups...", to_delete.len());
    for (id, meta) in &to_delete {
        println!("  Will delete {} ({})", &id, meta.created_at);
    }

    if !args.yes {
        print!("Continue? [y/N] ");
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted");
            return Ok(());
        }
    }

    for (id, meta) in to_delete {
        let short_id = if id.len() > 12 { &id[..12] } else { id };
        println!("  Deleting {} ({})", short_id, meta.created_at);
        storage.delete_recursive(id).await?;
    }

    println!("Cleanup complete");
    Ok(())
}
