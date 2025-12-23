mod commands;
mod output;
mod storage;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "etlctl")]
#[command(about = "CLI for managing ETL Router resources", long_about = None)]
#[command(version)]
struct Cli {
    #[arg(short, long, default_value = "http://localhost:8080")]
    server: String,

    #[arg(short, long, default_value = "default")]
    namespace: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Apply(commands::ApplyArgs),
    Get(commands::GetArgs),
    Describe(commands::DescribeArgs),
    Delete(commands::DeleteArgs),
    Graph(commands::GraphArgs),
    Validate(commands::ValidateArgs),
    Backup(commands::BackupArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let ctx = commands::Context {
        server: cli.server,
        namespace: cli.namespace,
    };

    match cli.command {
        Commands::Apply(args) => commands::apply::run(&ctx, args).await,
        Commands::Get(args) => commands::get::run(&ctx, args).await,
        Commands::Describe(args) => commands::describe::run(&ctx, args).await,
        Commands::Delete(args) => commands::delete::run(&ctx, args).await,
        Commands::Graph(args) => commands::graph::run(&ctx, args).await,
        Commands::Validate(args) => commands::validate::run(&ctx, args).await,
        Commands::Backup(args) => commands::backup::run(&ctx, args).await,
    }
}
