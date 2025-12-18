use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::Args;
use conveyor_dsl::{
    manifest::AnyManifest, optimizer::Optimizer, registry::Registry, ResourceKind,
    PipelineManifest, SinkManifest, SourceManifest, TransformManifest,
};

use super::Context;

#[derive(Args)]
pub struct GraphArgs {
    #[arg(short, long)]
    file: Option<PathBuf>,

    #[arg(long)]
    output_dot: bool,
}

pub async fn run(ctx: &Context, args: GraphArgs) -> Result<()> {
    let registry = if let Some(file) = &args.file {
        load_registry_from_files(file)?
    } else {
        fetch_registry_from_server(ctx).await?
    };

    let optimizer = Optimizer::new(&registry, &ctx.namespace);
    let dag = optimizer.optimize();

    if args.output_dot {
        print_dot(&dag);
    } else {
        print_ascii(&dag);
    }

    Ok(())
}

fn load_registry_from_files(path: &PathBuf) -> Result<Registry> {
    let mut registry = Registry::new();

    if path.is_file() {
        load_manifest_into_registry(path, &mut registry)?;
    } else if path.is_dir() {
        load_dir_into_registry(path, &mut registry)?;
    } else {
        anyhow::bail!("Path does not exist: {}", path.display());
    }

    Ok(registry)
}

fn load_dir_into_registry(dir: &PathBuf, registry: &mut Registry) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "yaml" || ext == "yml" {
                    if let Err(e) = load_manifest_into_registry(&path, registry) {
                        eprintln!("Warning: failed to load {}: {}", path.display(), e);
                    }
                }
            }
        } else if path.is_dir() {
            load_dir_into_registry(&path, registry)?;
        }
    }

    Ok(())
}

fn load_manifest_into_registry(path: &PathBuf, registry: &mut Registry) -> Result<()> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read file: {}", path.display()))?;

    #[derive(serde::Deserialize)]
    struct KindProbe {
        kind: ResourceKind,
    }

    let kind_probe: KindProbe = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse YAML: {}", path.display()))?;

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

async fn fetch_registry_from_server(_ctx: &Context) -> Result<Registry> {
    println!("Note: API server not available, using empty registry");
    Ok(Registry::new())
}

fn print_ascii(dag: &conveyor_dsl::optimizer::OptimizedDag) {
    println!("Optimized Pipeline DAG: {}", dag.id);
    println!();

    if dag.sources.is_empty() && dag.stages.is_empty() && dag.sinks.is_empty() {
        println!("  (empty - no pipelines defined)");
        return;
    }

    println!("Sources ({}):", dag.sources.len());
    for (id, source) in &dag.sources {
        println!("  {} -> {}", id, source.grpc.endpoint);
        if !source.consumers.is_empty() {
            println!("    consumers: {:?}", source.consumers);
        }
    }

    println!();
    println!("Stages ({}):", dag.stages.len());
    for (id, stage) in &dag.stages {
        let shared_marker = if stage.is_shared { " [SHARED]" } else { "" };
        println!("  {}{} -> {}", id, shared_marker, stage.grpc.endpoint);
    }

    println!();
    println!("Sinks ({}):", dag.sinks.len());
    for (id, sink) in &dag.sinks {
        println!("  {} -> {}", id, sink.grpc.endpoint);
        if !sink.source_pipelines.is_empty() {
            println!("    from pipelines: {:?}", sink.source_pipelines);
        }
    }

    println!();
    println!("Edges ({}):", dag.edges.len());
    for edge in &dag.edges {
        println!("  {} --> {}", edge.from, edge.to);
        if !edge.pipelines.is_empty() {
            println!("    pipelines: {:?}", edge.pipelines);
        }
    }
}

fn print_dot(dag: &conveyor_dsl::optimizer::OptimizedDag) {
    println!("digraph optimized_pipeline {{");
    println!("  rankdir=LR;");
    println!();

    println!("  // Sources");
    for (id, _source) in &dag.sources {
        println!(
            "  \"source:{}\" [shape=cylinder, label=\"{}\"];",
            id, id
        );
    }

    println!();
    println!("  // Stages");
    for (id, stage) in &dag.stages {
        let shape = if stage.is_shared { "box" } else { "ellipse" };
        let style = if stage.is_shared {
            ", style=bold"
        } else {
            ""
        };
        println!("  \"{}\" [shape={}{}];", id, shape, style);
    }

    println!();
    println!("  // Sinks");
    for (id, _) in &dag.sinks {
        println!("  \"{}\" [shape=house];", id);
    }

    println!();
    println!("  // Edges");
    for edge in &dag.edges {
        println!("  \"{}\" -> \"{}\";", edge.from, edge.to);
    }

    println!("}}");
}
