mod source;
mod transform;
mod sink;
mod pipeline;
mod cluster;

pub use source::{Source, SourceStatus};
pub use transform::{Transform, TransformStatus};
pub use sink::{Sink, SinkStatus};
pub use pipeline::{Pipeline, PipelineStatus};
pub use cluster::{
    ConveyorCluster, ConveyorClusterSpec, ConveyorClusterStatus, StorageConfig, ServiceConfig,
    RaftConfig, NodeStatusInfo, Toleration, Affinity, NodeAffinity, PodAffinity, PodAntiAffinity,
    NodeSelector, NodeSelectorTerm, NodeSelectorRequirement, PreferredSchedulingTerm,
    PodAffinityTerm, WeightedPodAffinityTerm, LabelSelector, LabelSelectorRequirement,
};

pub use conveyor_etl_dsl::{
    DlqConfig, GrpcEndpoint, PipelineSpec, SinkSpec, SourceSpec, TlsConfig, TransformSpec,
};

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
    println!("{}", serde_yaml::to_string(&ConveyorCluster::crd()).unwrap());
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
