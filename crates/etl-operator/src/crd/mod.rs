mod source;
mod transform;
mod sink;
mod pipeline;
mod cluster;

pub use source::{Source, SourceSpec, SourceStatus};
pub use transform::{Transform, TransformSpec, TransformStatus};
pub use sink::{Sink, SinkSpec, SinkStatus};
pub use pipeline::{Pipeline, PipelineSpec, PipelineStatus, DlqConfig};
pub use cluster::{EtlRouterCluster, EtlRouterClusterSpec, EtlRouterClusterStatus, StorageConfig, ServiceConfig, RaftConfig, NodeStatusInfo};

use kube::CustomResourceExt;

pub fn print_crds() {
    println!("---");
    println!("{}", serde_yaml::to_string(&Source::crd()).unwrap());
    println!("---");
    println!("{}", serde_yaml::to_string(&Transform::crd()).unwrap());
    println!("---");
    println!("{}", serde_yaml::to_string(&Sink::crd()).unwrap());
    println!("---");
    println!("{}", serde_yaml::to_string(&Pipeline::crd()).unwrap());
    println!("---");
    println!("{}", serde_yaml::to_string(&EtlRouterCluster::crd()).unwrap());
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GrpcEndpoint {
    pub endpoint: String,
    #[serde(default)]
    pub proto: Option<String>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    #[serde(default)]
    pub ca_cert: Option<String>,
    #[serde(default)]
    pub client_cert: Option<String>,
    #[serde(default)]
    pub client_key: Option<String>,
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    pub r#type: String,
    pub status: String,
    pub last_transition_time: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

impl Condition {
    pub fn ready(status: bool, reason: &str, message: &str) -> Self {
        Self {
            r#type: "Ready".to_string(),
            status: if status { "True" } else { "False" }.to_string(),
            last_transition_time: chrono::Utc::now().to_rfc3339(),
            reason: Some(reason.to_string()),
            message: Some(message.to_string()),
        }
    }
}
