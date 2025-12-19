use std::collections::HashMap;
use dashmap::DashMap;
use tonic::transport::Channel;

use conveyor_proto::router::{
    router_admin_client::RouterAdminClient,
    CreatePipelineRequest, UpdatePipelineRequest, DeletePipelineRequest,
    EnablePipelineRequest, DisablePipelineRequest, GetPipelineRequest,
    GetClusterStatusRequest, PipelineConfig, Stage, Edge, ServiceSelector,
    StageType, LoadBalanceStrategy,
};

use crate::error::{Error, Result};
use crate::crd::Pipeline;

#[derive(Clone)]
pub struct RouterClient {
    connections: DashMap<String, RouterAdminClient<Channel>>,
}

impl RouterClient {
    pub fn new() -> Self {
        Self {
            connections: DashMap::new(),
        }
    }

    pub async fn connect(&self, endpoint: &str) -> Result<RouterConnection> {
        if let Some(client) = self.connections.get(endpoint) {
            return Ok(RouterConnection {
                client: client.clone(),
            });
        }

        let channel = Channel::from_shared(format!("http://{}", endpoint))
            .map_err(|e| Error::InvalidUri(e.to_string()))?
            .connect()
            .await?;

        let client = RouterAdminClient::new(channel);
        self.connections.insert(endpoint.to_string(), client.clone());

        Ok(RouterConnection { client })
    }

    pub async fn disconnect(&self, endpoint: &str) {
        self.connections.remove(endpoint);
    }
}

impl Default for RouterClient {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RouterConnection {
    client: RouterAdminClient<Channel>,
}

#[derive(Debug)]
pub struct PipelineResult {
    pub pipeline_id: String,
    pub version: Option<u64>,
}

#[derive(Debug)]
pub struct ClusterStatusResult {
    pub node_id: u64,
    pub leader_id: u64,
    pub term: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub health: String,
    pub nodes: Vec<NodeStatusResult>,
}

#[derive(Debug)]
pub struct NodeStatusResult {
    pub node_id: u64,
    pub address: String,
    pub role: String,
    pub healthy: bool,
}

impl RouterConnection {
    pub async fn create_pipeline(&mut self, pipeline: &Pipeline) -> Result<PipelineResult> {
        let config = self.pipeline_to_config(pipeline)?;

        let response = self
            .client
            .create_pipeline(CreatePipelineRequest {
                config: Some(config),
            })
            .await?
            .into_inner();

        if response.success {
            Ok(PipelineResult {
                pipeline_id: response.pipeline_id,
                version: None,
            })
        } else {
            Err(Error::RouterError(response.error))
        }
    }

    pub async fn update_pipeline(
        &mut self,
        pipeline_id: &str,
        pipeline: &Pipeline,
    ) -> Result<PipelineResult> {
        let config = self.pipeline_to_config(pipeline)?;

        let response = self
            .client
            .update_pipeline(UpdatePipelineRequest {
                pipeline_id: pipeline_id.to_string(),
                config: Some(config),
            })
            .await?
            .into_inner();

        if response.success {
            Ok(PipelineResult {
                pipeline_id: pipeline_id.to_string(),
                version: Some(response.version),
            })
        } else {
            Err(Error::RouterError(response.error))
        }
    }

    pub async fn delete_pipeline(&mut self, pipeline_id: &str, force: bool) -> Result<()> {
        let response = self
            .client
            .delete_pipeline(DeletePipelineRequest {
                pipeline_id: pipeline_id.to_string(),
                force,
            })
            .await?
            .into_inner();

        if response.success {
            Ok(())
        } else {
            Err(Error::RouterError(response.error))
        }
    }

    pub async fn enable_pipeline(&mut self, pipeline_id: &str) -> Result<()> {
        let response = self
            .client
            .enable_pipeline(EnablePipelineRequest {
                pipeline_id: pipeline_id.to_string(),
            })
            .await?
            .into_inner();

        if response.success {
            Ok(())
        } else {
            Err(Error::RouterError(response.error))
        }
    }

    pub async fn disable_pipeline(&mut self, pipeline_id: &str, drain: bool) -> Result<()> {
        let response = self
            .client
            .disable_pipeline(DisablePipelineRequest {
                pipeline_id: pipeline_id.to_string(),
                drain,
            })
            .await?
            .into_inner();

        if response.success {
            Ok(())
        } else {
            Err(Error::RouterError(response.error))
        }
    }

    pub async fn get_pipeline(&mut self, pipeline_id: &str) -> Result<Option<PipelineConfig>> {
        let response = self
            .client
            .get_pipeline(GetPipelineRequest {
                pipeline_id: pipeline_id.to_string(),
            })
            .await?
            .into_inner();

        if response.found {
            Ok(response.config)
        } else {
            Ok(None)
        }
    }

    pub async fn get_cluster_status(&mut self) -> Result<ClusterStatusResult> {
        let response = self
            .client
            .get_cluster_status(GetClusterStatusRequest {})
            .await?
            .into_inner();

        let health = match response.health {
            1 => "Healthy",
            2 => "Degraded",
            3 => "Unhealthy",
            _ => "Unknown",
        };

        let nodes = response
            .nodes
            .into_iter()
            .map(|n| {
                let role = match n.role {
                    1 => "Leader",
                    2 => "Follower",
                    3 => "Candidate",
                    4 => "Learner",
                    _ => "Unknown",
                };
                NodeStatusResult {
                    node_id: n.node_id,
                    address: n.address,
                    role: role.to_string(),
                    healthy: n.healthy,
                }
            })
            .collect();

        Ok(ClusterStatusResult {
            node_id: response.node_id,
            leader_id: response.leader_id,
            term: response.term,
            commit_index: response.commit_index,
            applied_index: response.applied_index,
            health: health.to_string(),
            nodes,
        })
    }

    fn pipeline_to_config(&self, pipeline: &Pipeline) -> Result<PipelineConfig> {
        let name = pipeline
            .metadata
            .name
            .as_ref()
            .ok_or_else(|| Error::MissingField("metadata.name".to_string()))?;

        let namespace = pipeline
            .metadata
            .namespace
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("default");

        let pipeline_id = format!("{}/{}", namespace, name);
        let spec = &pipeline.spec;

        let mut stages = Vec::new();
        let mut edges = Vec::new();

        let source_stage_id = format!("{}-source", name);
        stages.push(Stage {
            id: source_stage_id.clone(),
            name: spec.source.clone(),
            stage_type: StageType::Source as i32,
            service_selector: Some(ServiceSelector {
                service_name: spec.source.clone(),
                group_id: String::new(),
                labels: HashMap::new(),
                load_balance: LoadBalanceStrategy::LoadBalanceRoundRobin as i32,
            }),
            routing_rules: vec![],
            parallelism: 1,
        });

        let mut prev_stage_id = source_stage_id;

        for (i, step) in spec.steps.iter().enumerate() {
            let stage_id = format!("{}-transform-{}", name, i);
            stages.push(Stage {
                id: stage_id.clone(),
                name: step.clone(),
                stage_type: StageType::Transform as i32,
                service_selector: Some(ServiceSelector {
                    service_name: step.clone(),
                    group_id: String::new(),
                    labels: HashMap::new(),
                    load_balance: LoadBalanceStrategy::LoadBalanceRoundRobin as i32,
                }),
                routing_rules: vec![],
                parallelism: 1,
            });

            edges.push(Edge {
                from_stage: prev_stage_id.clone(),
                to_stage: stage_id.clone(),
                condition: None,
            });

            prev_stage_id = stage_id;
        }

        let sink_stage_id = format!("{}-sink", name);
        stages.push(Stage {
            id: sink_stage_id.clone(),
            name: spec.sink.clone(),
            stage_type: StageType::Sink as i32,
            service_selector: Some(ServiceSelector {
                service_name: spec.sink.clone(),
                group_id: String::new(),
                labels: HashMap::new(),
                load_balance: LoadBalanceStrategy::LoadBalanceRoundRobin as i32,
            }),
            routing_rules: vec![],
            parallelism: 1,
        });

        edges.push(Edge {
            from_stage: prev_stage_id,
            to_stage: sink_stage_id,
            condition: None,
        });

        Ok(PipelineConfig {
            id: pipeline_id,
            name: name.clone(),
            description: String::new(),
            stages,
            edges,
            enabled: spec.enabled,
            metadata: pipeline
                .metadata
                .labels
                .clone()
                .unwrap_or_default()
                .into_iter()
                .collect(),
        })
    }
}
