pub mod cluster_controller;
pub mod pipeline_controller;
pub mod source_controller;
pub mod transform_controller;
pub mod sink_controller;

use std::sync::Arc;
use kube::Client;
use crate::grpc::RouterClient;

pub struct Context {
    pub client: Client,
    pub router_client: Arc<RouterClient>,
}

impl Context {
    pub fn new(client: Client, router_client: Arc<RouterClient>) -> Arc<Self> {
        Arc::new(Self {
            client,
            router_client,
        })
    }
}

pub const FINALIZER_NAME: &str = "etl.router/finalizer";
