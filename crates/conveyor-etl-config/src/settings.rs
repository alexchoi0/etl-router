use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    pub cluster: ClusterSettings,
    pub buffer: BufferSettings,
    pub grpc: GrpcSettings,
    pub metrics: MetricsSettings,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterSettings {
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub snapshot_interval: u64,
    pub max_entries_per_append: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BufferSettings {
    pub max_total_records: usize,
    pub max_per_source: usize,
    pub max_per_stage: usize,
    pub backpressure_threshold: f64,
    pub backpressure_resume_threshold: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GrpcSettings {
    pub max_message_size: usize,
    pub keepalive_interval_secs: u64,
    pub keepalive_timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsSettings {
    pub enabled: bool,
    pub listen_addr: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            cluster: ClusterSettings {
                election_timeout_min_ms: 150,
                election_timeout_max_ms: 300,
                heartbeat_interval_ms: 50,
                snapshot_interval: 10000,
                max_entries_per_append: 100,
            },
            buffer: BufferSettings {
                max_total_records: 1_000_000,
                max_per_source: 100_000,
                max_per_stage: 100_000,
                backpressure_threshold: 0.8,
                backpressure_resume_threshold: 0.6,
            },
            grpc: GrpcSettings {
                max_message_size: 64 * 1024 * 1024,
                keepalive_interval_secs: 10,
                keepalive_timeout_secs: 5,
            },
            metrics: MetricsSettings {
                enabled: true,
                listen_addr: "0.0.0.0:9090".to_string(),
            },
        }
    }
}

impl Settings {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config = config::Config::builder()
            .add_source(config::File::from(path.as_ref()).required(false))
            .add_source(config::Environment::with_prefix("ETL_ROUTER").separator("__"))
            .build()?;

        match config.try_deserialize() {
            Ok(settings) => Ok(settings),
            Err(_) => {
                tracing::warn!("Config file not found or invalid, using defaults");
                Ok(Self::default())
            }
        }
    }
}
