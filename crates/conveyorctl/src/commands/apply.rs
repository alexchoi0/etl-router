use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::Args;
use conveyor_dsl::manifest::{AnyManifest, ResourceKind};

use super::Context;

#[derive(Args)]
pub struct ApplyArgs {
    #[arg(short, long)]
    file: PathBuf,

    #[arg(long, default_value = "false")]
    dry_run: bool,

    #[arg(long, default_value = "false")]
    recursive: bool,
}

pub async fn run(ctx: &Context, args: ApplyArgs) -> Result<()> {
    let manifests = load_manifests(&args.file, args.recursive)?;

    if manifests.is_empty() {
        println!("No manifests found");
        return Ok(());
    }

    println!(
        "Found {} manifest(s) to apply{}",
        manifests.len(),
        if args.dry_run { " (dry-run)" } else { "" }
    );

    for manifest in &manifests {
        let kind = manifest.kind();
        let metadata = manifest.metadata();
        let resource_name = format!("{}/{}", metadata.namespace, metadata.name);

        if args.dry_run {
            println!("  {:?} {} would be applied", kind, resource_name);
        } else {
            match apply_manifest(ctx, manifest).await {
                Ok(_) => println!("  {:?} {} applied", kind, resource_name),
                Err(e) => eprintln!("  {:?} {} failed: {}", kind, resource_name, e),
            }
        }
    }

    Ok(())
}

fn load_manifests(path: &PathBuf, recursive: bool) -> Result<Vec<AnyManifest>> {
    let mut manifests = Vec::new();

    if path.is_file() {
        let manifest = load_manifest_file(path)?;
        manifests.push(manifest);
    } else if path.is_dir() {
        load_manifests_from_dir(path, recursive, &mut manifests)?;
    } else {
        anyhow::bail!("Path does not exist: {}", path.display());
    }

    Ok(manifests)
}

fn load_manifests_from_dir(dir: &PathBuf, recursive: bool, manifests: &mut Vec<AnyManifest>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "yaml" || ext == "yml" {
                    match load_manifest_file(&path) {
                        Ok(m) => manifests.push(m),
                        Err(e) => eprintln!("Warning: failed to load {}: {}", path.display(), e),
                    }
                }
            }
        } else if path.is_dir() && recursive {
            load_manifests_from_dir(&path, recursive, manifests)?;
        }
    }

    Ok(())
}

fn load_manifest_file(path: &PathBuf) -> Result<AnyManifest> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read file: {}", path.display()))?;

    let kind_probe: KindProbe = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse YAML: {}", path.display()))?;

    let manifest = match kind_probe.kind {
        ResourceKind::Source => {
            let m: conveyor_dsl::SourceManifest = serde_yaml::from_str(&content)?;
            AnyManifest::Source(m)
        }
        ResourceKind::Transform => {
            let m: conveyor_dsl::TransformManifest = serde_yaml::from_str(&content)?;
            AnyManifest::Transform(m)
        }
        ResourceKind::Sink => {
            let m: conveyor_dsl::SinkManifest = serde_yaml::from_str(&content)?;
            AnyManifest::Sink(m)
        }
        ResourceKind::Pipeline => {
            let m: conveyor_dsl::PipelineManifest = serde_yaml::from_str(&content)?;
            AnyManifest::Pipeline(m)
        }
    };

    Ok(manifest)
}

#[derive(serde::Deserialize)]
struct KindProbe {
    kind: ResourceKind,
}

async fn apply_manifest(ctx: &Context, manifest: &AnyManifest) -> Result<()> {
    let metadata = manifest.metadata();
    let endpoint = get_endpoint(ctx, manifest.kind(), &metadata.namespace);

    let client = reqwest::Client::new();
    let response = client
        .post(&endpoint)
        .json(&manifest)
        .send()
        .await
        .with_context(|| format!("Failed to send request to {}", endpoint))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Server returned {}: {}", status, body);
    }

    Ok(())
}

fn get_endpoint(ctx: &Context, kind: ResourceKind, namespace: &str) -> String {
    let resource_type = match kind {
        ResourceKind::Source => "sources",
        ResourceKind::Transform => "transforms",
        ResourceKind::Sink => "sinks",
        ResourceKind::Pipeline => "pipelines",
    };

    format!(
        "{}/apis/etl.router/v1/namespaces/{}/{}",
        ctx.server, namespace, resource_type
    )
}
