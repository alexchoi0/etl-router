use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub enum RouteDecision {
    Local {
        endpoint: String,
    },
    Remote {
        sidecar_id: String,
        endpoint: String,
    },
}

#[derive(Debug, Clone)]
pub struct StageRoute {
    pub stage_id: String,
    pub decision: RouteDecision,
}

#[derive(Debug, Clone)]
pub struct PipelineRoutes {
    pub pipeline_id: String,
    pub is_local_complete: bool,
    pub stages: HashMap<String, StageRoute>,
}

#[derive(Debug, Default)]
pub struct RoutingTable {
    pipelines: HashMap<String, PipelineRoutes>,
    local_services: HashMap<String, String>,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_local_service(&mut self, service_name: String, endpoint: String) {
        self.local_services.insert(service_name, endpoint);
    }

    pub fn set_pipeline_routes(&mut self, routes: PipelineRoutes) {
        self.pipelines.insert(routes.pipeline_id.clone(), routes);
    }

    pub fn remove_pipeline(&mut self, pipeline_id: &str) {
        self.pipelines.remove(pipeline_id);
    }

    pub fn get_route(&self, pipeline_id: &str, stage_id: &str) -> Option<&RouteDecision> {
        self.pipelines
            .get(pipeline_id)
            .and_then(|p| p.stages.get(stage_id))
            .map(|s| &s.decision)
    }

    pub fn get_next_stage_route(
        &self,
        pipeline_id: &str,
        current_stage_id: &str,
        edges: &[(String, String)],
    ) -> Option<&RouteDecision> {
        let next_stage = edges
            .iter()
            .find(|(from, _)| from == current_stage_id)
            .map(|(_, to)| to)?;

        self.get_route(pipeline_id, next_stage)
    }

    pub fn is_local_complete(&self, pipeline_id: &str) -> bool {
        self.pipelines
            .get(pipeline_id)
            .map(|p| p.is_local_complete)
            .unwrap_or(false)
    }

    pub fn has_pipeline(&self, pipeline_id: &str) -> bool {
        self.pipelines.contains_key(pipeline_id)
    }

    pub fn pipeline_ids(&self) -> impl Iterator<Item = &str> {
        self.pipelines.keys().map(|s| s.as_str())
    }

    pub fn get_pipeline_routes(&self, pipeline_id: &str) -> Option<&PipelineRoutes> {
        self.pipelines.get(pipeline_id)
    }

    pub fn get_local_endpoint(&self, service_name: &str) -> Option<&String> {
        self.local_services.get(service_name)
    }
}

pub type SharedRoutingTable = Arc<RwLock<RoutingTable>>;
