use anyhow::{Context as _, Result};
use clap::Args;

use super::Context;
use super::get::ResourceType;

#[derive(Args)]
pub struct DescribeArgs {
    resource: ResourceType,
    name: String,
}

pub async fn run(ctx: &Context, args: DescribeArgs) -> Result<()> {
    let endpoint = format!(
        "{}/apis/etl.router/v1/namespaces/{}/{}/{}",
        ctx.server,
        ctx.namespace,
        args.resource.api_path(),
        args.name
    );

    let client = reqwest::Client::new();
    let response = client
        .get(&endpoint)
        .send()
        .await
        .with_context(|| format!("Failed to connect to {}", endpoint));

    match response {
        Ok(resp) if resp.status().is_success() => {
            let body = resp.text().await?;
            let resource: serde_json::Value = serde_json::from_str(&body)?;
            print_describe(&args.resource, &resource);
        }
        Ok(resp) => {
            let status = resp.status();
            if status.as_u16() == 404 {
                println!(
                    "Error: {} '{}' not found in namespace '{}'",
                    args.resource.api_path(),
                    args.name,
                    ctx.namespace
                );
            } else {
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Server returned {}: {}", status, body);
            }
        }
        Err(_) => {
            println!("Note: API server not available, showing mock data");
            print_mock_describe(&args.resource, &args.name, &ctx.namespace);
        }
    }

    Ok(())
}

fn print_describe(_resource_type: &ResourceType, resource: &serde_json::Value) {
    let yaml = serde_yaml::to_string(resource).unwrap_or_default();
    println!("{}", yaml);
}

fn print_mock_describe(resource_type: &ResourceType, name: &str, namespace: &str) {
    println!("Name:         {}", name);
    println!("Namespace:    {}", namespace);
    println!("Kind:         {:?}", resource_type);
    println!();

    match resource_type {
        ResourceType::Sources => {
            println!("Spec:");
            println!("  gRPC:");
            println!("    Endpoint:  kafka-source-svc:50051");
            println!("    Proto:     etl.source.SourceService");
            println!("  Config:");
            println!("    Brokers:   [kafka:9092]");
            println!("    Topic:     user-events");
        }
        ResourceType::Transforms => {
            println!("Spec:");
            println!("  gRPC:");
            println!("    Endpoint:  filter-svc:50051");
            println!("    Proto:     etl.transform.TransformService");
            println!("  Config:");
            println!("    Type:      filter");
            if name.contains("validate") {
                println!("  Invariant:");
                println!("    Checks:");
                println!("      - fieldsRequired: [user_id, email]");
                println!("    OnFailure: route");
            }
        }
        ResourceType::Sinks => {
            println!("Spec:");
            println!("  gRPC:");
            println!("    Endpoint:  s3-sink-svc:50051");
            println!("    Proto:     etl.sink.SinkService");
            println!("  Config:");
            println!("    Bucket:    user-events-archive");
        }
        ResourceType::Pipelines => {
            println!("Spec:");
            println!("  Source:     kafka-user-events");
            println!("  Steps:");
            println!("    - filter-active-users");
            println!("    - validate-user-schema");
            println!("  Sink:       s3-archive");
            println!("  OnError:    error-handler");
        }
    }
}

