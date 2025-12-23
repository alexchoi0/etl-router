use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::Args;
use conveyor_etl_dsl::{
    manifest::AnyManifest, registry::Registry, ResourceKind,
    PipelineManifest, SinkManifest,
    SourceManifest, TransformManifest,
};

use super::Context;

#[derive(Args)]
pub struct ValidateArgs {
    #[arg(short, long)]
    file: PathBuf,

    #[arg(long, default_value = "false")]
    recursive: bool,
}

pub async fn run(_ctx: &Context, args: ValidateArgs) -> Result<()> {
    let mut registry = Registry::new();
    let mut manifest_count = 0;
    let mut error_count = 0;

    println!("Validating manifests from: {}", args.file.display());
    println!();

    if args.file.is_file() {
        match load_and_register(&args.file, &mut registry) {
            Ok(_) => {
                manifest_count += 1;
                println!("  ✓ {}", args.file.display());
            }
            Err(e) => {
                error_count += 1;
                println!("  ✗ {}: {}", args.file.display(), e);
            }
        }
    } else if args.file.is_dir() {
        validate_dir(&args.file, args.recursive, &mut registry, &mut manifest_count, &mut error_count)?;
    } else {
        anyhow::bail!("Path does not exist: {}", args.file.display());
    }

    println!();
    println!("Manifest validation: {} loaded, {} errors", manifest_count, error_count);

    if error_count > 0 {
        println!();
        anyhow::bail!("Validation failed with {} error(s)", error_count);
    }

    println!();
    println!("Cross-reference validation:");

    match registry.validate_all_pipelines() {
        Ok(_) => {
            let pipeline_count = registry.list_pipelines(None).len();
            println!("  ✓ All {} pipeline(s) are valid", pipeline_count);
        }
        Err(e) => {
            println!("  ✗ {}", e);
            anyhow::bail!("Pipeline validation failed");
        }
    }

    println!();
    println!("Validation passed!");

    Ok(())
}

fn validate_dir(
    dir: &PathBuf,
    recursive: bool,
    registry: &mut Registry,
    manifest_count: &mut usize,
    error_count: &mut usize,
) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "yaml" || ext == "yml" {
                    match load_and_register(&path, registry) {
                        Ok(_) => {
                            *manifest_count += 1;
                            println!("  ✓ {}", path.display());
                        }
                        Err(e) => {
                            *error_count += 1;
                            println!("  ✗ {}: {}", path.display(), e);
                        }
                    }
                }
            }
        } else if path.is_dir() && recursive {
            validate_dir(&path, recursive, registry, manifest_count, error_count)?;
        }
    }

    Ok(())
}

fn load_and_register(path: &PathBuf, registry: &mut Registry) -> Result<()> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read file: {}", path.display()))?;

    #[derive(serde::Deserialize)]
    struct KindProbe {
        kind: ResourceKind,
    }

    let kind_probe: KindProbe = serde_yaml::from_str(&content)
        .with_context(|| format!("Invalid YAML structure"))?;

    let manifest = match kind_probe.kind {
        ResourceKind::Source => {
            let m: SourceManifest = serde_yaml::from_str(&content)?;
            AnyManifest::Source(m)
        }
        ResourceKind::Transform => {
            let m: TransformManifest = serde_yaml::from_str(&content)?;
            AnyManifest::Transform(m)
        }
        ResourceKind::Sink => {
            let m: SinkManifest = serde_yaml::from_str(&content)?;
            AnyManifest::Sink(m)
        }
        ResourceKind::Pipeline => {
            let m: PipelineManifest = serde_yaml::from_str(&content)?;
            AnyManifest::Pipeline(m)
        }
    };

    registry.apply(manifest)?;
    Ok(())
}
