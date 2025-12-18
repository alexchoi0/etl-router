use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::Args;
use conveyor_dsl::ResourceKind;

use super::Context;
use super::get::ResourceType;

#[derive(Args)]
pub struct DeleteArgs {
    #[arg(required_unless_present = "file")]
    resource: Option<ResourceType>,

    #[arg(required_unless_present = "file")]
    name: Option<String>,

    #[arg(short, long)]
    file: Option<PathBuf>,

    #[arg(long)]
    force: bool,
}

pub async fn run(ctx: &Context, args: DeleteArgs) -> Result<()> {
    if let Some(file) = &args.file {
        return delete_from_file(ctx, file, args.force).await;
    }

    let resource = args.resource.expect("resource type required");
    let name = args.name.expect("resource name required");

    delete_resource(ctx, &resource, &name, args.force).await
}

async fn delete_from_file(ctx: &Context, file: &PathBuf, force: bool) -> Result<()> {
    let content = std::fs::read_to_string(file)
        .with_context(|| format!("Failed to read file: {}", file.display()))?;

    #[derive(serde::Deserialize)]
    struct ManifestProbe {
        kind: ResourceKind,
        metadata: MetadataProbe,
    }

    #[derive(serde::Deserialize)]
    struct MetadataProbe {
        name: String,
        #[serde(default = "default_namespace")]
        namespace: String,
    }

    fn default_namespace() -> String {
        "default".to_string()
    }

    let manifest: ManifestProbe = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse manifest: {}", file.display()))?;

    let resource_type = match manifest.kind {
        ResourceKind::Source => ResourceType::Sources,
        ResourceKind::Transform => ResourceType::Transforms,
        ResourceKind::Sink => ResourceType::Sinks,
        ResourceKind::Pipeline => ResourceType::Pipelines,
    };

    delete_resource_impl(
        ctx,
        &resource_type,
        &manifest.metadata.name,
        &manifest.metadata.namespace,
        force,
    )
    .await
}

async fn delete_resource(ctx: &Context, resource: &ResourceType, name: &str, force: bool) -> Result<()> {
    delete_resource_impl(ctx, resource, name, &ctx.namespace, force).await
}

async fn delete_resource_impl(
    ctx: &Context,
    resource: &ResourceType,
    name: &str,
    namespace: &str,
    force: bool,
) -> Result<()> {
    if !force {
        println!(
            "Delete {} '{}/{}' ? (use --force to confirm)",
            resource.api_path(),
            namespace,
            name
        );
        return Ok(());
    }

    let endpoint = format!(
        "{}/apis/etl.router/v1/namespaces/{}/{}/{}",
        ctx.server,
        namespace,
        resource.api_path(),
        name
    );

    let client = reqwest::Client::new();
    let response = client
        .delete(&endpoint)
        .send()
        .await
        .with_context(|| format!("Failed to connect to {}", endpoint));

    match response {
        Ok(resp) if resp.status().is_success() => {
            println!("{} '{}/{}' deleted", resource.api_path(), namespace, name);
        }
        Ok(resp) => {
            let status = resp.status();
            if status.as_u16() == 404 {
                println!(
                    "Error: {} '{}' not found in namespace '{}'",
                    resource.api_path(),
                    name,
                    namespace
                );
            } else {
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Server returned {}: {}", status, body);
            }
        }
        Err(_) => {
            println!("Note: API server not available");
            println!(
                "{} '{}/{}' would be deleted (mock)",
                resource.api_path(),
                namespace,
                name
            );
        }
    }

    Ok(())
}
