use std::sync::Arc;

use clap::Parser;
use kube::Client;
use tracing::{error, info};

use etl_operator::{
    crd,
    controller::{
        cluster_controller, pipeline_controller, sink_controller, source_controller,
        transform_controller,
    },
    grpc::RouterClient,
};

#[derive(Parser, Debug)]
#[command(name = "etl-operator")]
#[command(about = "Kubernetes operator for ETL Router")]
struct Args {
    #[arg(long, help = "Print CRD definitions and exit")]
    crd: bool,

    #[arg(long, default_value = "info", help = "Log level (trace, debug, info, warn, error)")]
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if args.crd {
        crd::print_crds();
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&args.log_level)),
        )
        .init();

    info!("Starting ETL Router Operator");

    let client = Client::try_default().await?;
    let router_client = Arc::new(RouterClient::new());

    info!("Connected to Kubernetes API");

    let cluster_ctrl = cluster_controller::run(client.clone(), router_client.clone());
    let source_ctrl = source_controller::run(client.clone(), router_client.clone());
    let transform_ctrl = transform_controller::run(client.clone(), router_client.clone());
    let sink_ctrl = sink_controller::run(client.clone(), router_client.clone());
    let pipeline_ctrl = pipeline_controller::run(client.clone(), router_client.clone());

    info!("All controllers started");

    tokio::select! {
        _ = cluster_ctrl => error!("Cluster controller exited"),
        _ = source_ctrl => error!("Source controller exited"),
        _ = transform_ctrl => error!("Transform controller exited"),
        _ = sink_ctrl => error!("Sink controller exited"),
        _ = pipeline_ctrl => error!("Pipeline controller exited"),
    }

    Ok(())
}
