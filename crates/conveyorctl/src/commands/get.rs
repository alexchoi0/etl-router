use anyhow::{Context as _, Result};
use clap::{Args, ValueEnum};
use tabled::{Table, Tabled};

use super::Context;
use crate::output::OutputFormat;

#[derive(Args)]
pub struct GetArgs {
    resource: ResourceType,

    #[arg(short, long)]
    name: Option<String>,

    #[arg(short, long, default_value = "table")]
    output: OutputFormat,

    #[arg(short = 'A', long)]
    all_namespaces: bool,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum ResourceType {
    Sources,
    Transforms,
    Sinks,
    Pipelines,
}

impl ResourceType {
    pub fn api_path(&self) -> &'static str {
        match self {
            ResourceType::Sources => "sources",
            ResourceType::Transforms => "transforms",
            ResourceType::Sinks => "sinks",
            ResourceType::Pipelines => "pipelines",
        }
    }
}

pub async fn run(ctx: &Context, args: GetArgs) -> Result<()> {
    let namespace = if args.all_namespaces {
        None
    } else {
        Some(ctx.namespace.as_str())
    };

    let resources = fetch_resources(ctx, &args.resource, namespace, args.name.as_deref()).await?;

    match args.output {
        OutputFormat::Table => print_table(&args.resource, &resources),
        OutputFormat::Yaml => print_yaml(&resources)?,
        OutputFormat::Json => print_json(&resources)?,
    }

    Ok(())
}

async fn fetch_resources(
    ctx: &Context,
    resource_type: &ResourceType,
    namespace: Option<&str>,
    name: Option<&str>,
) -> Result<Vec<ResourceInfo>> {
    let ns = namespace.unwrap_or("_all");
    let endpoint = match name {
        Some(n) => format!(
            "{}/apis/etl.router/v1/namespaces/{}/{}/{}",
            ctx.server,
            ns,
            resource_type.api_path(),
            n
        ),
        None => format!(
            "{}/apis/etl.router/v1/namespaces/{}/{}",
            ctx.server,
            ns,
            resource_type.api_path()
        ),
    };

    let client = reqwest::Client::new();
    let response = client
        .get(&endpoint)
        .send()
        .await
        .with_context(|| format!("Failed to connect to {}", endpoint));

    match response {
        Ok(resp) if resp.status().is_success() => {
            let body = resp.text().await?;
            let resources: Vec<ResourceInfo> = serde_json::from_str(&body)?;
            Ok(resources)
        }
        Ok(resp) => {
            let status = resp.status();
            if status.as_u16() == 404 {
                Ok(Vec::new())
            } else {
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Server returned {}: {}", status, body);
            }
        }
        Err(_) => {
            println!("Note: API server not available, showing mock data");
            Ok(get_mock_resources(resource_type, namespace, name))
        }
    }
}

fn get_mock_resources(
    resource_type: &ResourceType,
    _namespace: Option<&str>,
    name: Option<&str>,
) -> Vec<ResourceInfo> {
    let mock = match resource_type {
        ResourceType::Sources => vec![
            ResourceInfo {
                name: "kafka-user-events".to_string(),
                namespace: "default".to_string(),
                endpoint: "kafka-source-svc:50051".to_string(),
                labels: "team=data-platform".to_string(),
            },
            ResourceInfo {
                name: "kafka-orders".to_string(),
                namespace: "default".to_string(),
                endpoint: "kafka-source-svc:50051".to_string(),
                labels: "team=commerce".to_string(),
            },
        ],
        ResourceType::Transforms => vec![
            ResourceInfo {
                name: "filter-active-users".to_string(),
                namespace: "default".to_string(),
                endpoint: "filter-svc:50051".to_string(),
                labels: "type=filter".to_string(),
            },
            ResourceInfo {
                name: "validate-user-schema".to_string(),
                namespace: "default".to_string(),
                endpoint: "validator-svc:50051".to_string(),
                labels: "type=validate".to_string(),
            },
        ],
        ResourceType::Sinks => vec![ResourceInfo {
            name: "s3-archive".to_string(),
            namespace: "default".to_string(),
            endpoint: "s3-sink-svc:50051".to_string(),
            labels: "team=data-platform".to_string(),
        }],
        ResourceType::Pipelines => vec![ResourceInfo {
            name: "user-analytics".to_string(),
            namespace: "default".to_string(),
            endpoint: "kafka-user-events → s3-archive".to_string(),
            labels: "team=analytics".to_string(),
        }],
    };

    if let Some(n) = name {
        mock.into_iter().filter(|r| r.name == n).collect()
    } else {
        mock
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Tabled)]
struct ResourceInfo {
    #[tabled(rename = "NAME")]
    name: String,
    #[tabled(rename = "NAMESPACE")]
    namespace: String,
    #[tabled(rename = "ENDPOINT")]
    endpoint: String,
    #[tabled(rename = "LABELS")]
    labels: String,
}

fn print_table(resource_type: &ResourceType, resources: &[ResourceInfo]) {
    if resources.is_empty() {
        println!("No {} found", resource_type.api_path());
        return;
    }

    let table = Table::new(resources);
    println!("{}", table);
}

fn print_yaml(resources: &[ResourceInfo]) -> Result<()> {
    let yaml = serde_yaml::to_string(resources)?;
    println!("{}", yaml);
    Ok(())
}

fn print_json(resources: &[ResourceInfo]) -> Result<()> {
    let json = serde_json::to_string_pretty(resources)?;
    println!("{}", json);
    Ok(())
}
