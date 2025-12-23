use anyhow::Result;
use clap::Parser;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(name = "etl-router")]
#[command(about = "High-availability ETL routing layer with Raft consensus")]
struct Args {
    #[arg(short, long, default_value = "config/router.yaml")]
    config: String,

    #[arg(short, long)]
    node_id: u64,

    #[arg(short, long, default_value = "127.0.0.1:50051")]
    listen_addr: String,

    #[arg(long, default_value = "127.0.0.1:50052")]
    raft_addr: String,

    #[arg(long)]
    peers: Vec<String>,

    #[arg(long, default_value = "./data")]
    data_dir: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(true)
        .with_thread_ids(true)
        .init();

    let args = Args::parse();

    info!(
        node_id = args.node_id,
        listen_addr = %args.listen_addr,
        raft_addr = %args.raft_addr,
        "Starting ETL Router"
    );

    let settings = conveyor_etl_config::Settings::load(&args.config)?;

    let server = conveyor_etl_grpc::RouterServer::new(
        args.node_id,
        args.listen_addr.parse()?,
        args.raft_addr.parse()?,
        args.peers,
        args.data_dir,
        settings,
    ).await?;

    server.run().await?;

    Ok(())
}
