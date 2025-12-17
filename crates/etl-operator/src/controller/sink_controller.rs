use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        finalizer::{finalizer, Event as FinalizerEvent},
        watcher::Config,
    },
    Client, ResourceExt,
};
use tracing::{error, info, instrument, warn};

use crate::crd::{Condition, Sink, SinkStatus, EtlRouterCluster};
use crate::error::{Error, Result};
use crate::grpc::RouterClient;
use super::{Context, FINALIZER_NAME};

pub async fn run(client: Client, router_client: Arc<RouterClient>) {
    let sinks: Api<Sink> = Api::all(client.clone());

    let ctx = Context::new(client.clone(), router_client);

    Controller::new(sinks, Config::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("Reconciled Sink {:?}", o),
                Err(e) => error!("Reconcile failed: {:?}", e),
            }
        })
        .await;
}

#[instrument(skip(ctx, sink), fields(name = %sink.name_any(), namespace = ?sink.namespace()))]
async fn reconcile(sink: Arc<Sink>, ctx: Arc<Context>) -> Result<Action> {
    let ns = sink.namespace().unwrap_or_else(|| "default".to_string());
    let api: Api<Sink> = Api::namespaced(ctx.client.clone(), &ns);

    finalizer(&api, FINALIZER_NAME, sink, |event| async {
        match event {
            FinalizerEvent::Apply(sink) => apply(sink, ctx.clone()).await,
            FinalizerEvent::Cleanup(sink) => cleanup(sink, ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| Error::FinalizerError(e.to_string()))
}

async fn apply(sink: Arc<Sink>, ctx: Arc<Context>) -> Result<Action> {
    let ns = sink.namespace().unwrap_or_else(|| "default".to_string());
    let name = sink.name_any();

    info!("Reconciling Sink {}/{}", ns, name);

    let router_endpoint = find_router_endpoint(&ctx.client, &ns).await?;

    let status = match ctx.router_client.connect(&router_endpoint).await {
        Ok(_conn) => {
            SinkStatus {
                observed_generation: sink.metadata.generation,
                conditions: vec![Condition::ready(true, "Registered", "Sink registered with router")],
                service_id: Some(format!("{}/{}", ns, name)),
                registered_at: Some(chrono::Utc::now().to_rfc3339()),
                health: Some("Healthy".to_string()),
                last_heartbeat: Some(chrono::Utc::now().to_rfc3339()),
            }
        }
        Err(e) => {
            SinkStatus {
                observed_generation: sink.metadata.generation,
                conditions: vec![Condition::ready(false, "RegistrationFailed", &e.to_string())],
                service_id: None,
                registered_at: None,
                health: Some("Unknown".to_string()),
                last_heartbeat: None,
            }
        }
    };

    update_sink_status(&ctx.client, &ns, &name, status).await?;

    Ok(Action::requeue(Duration::from_secs(60)))
}

async fn cleanup(sink: Arc<Sink>, ctx: Arc<Context>) -> Result<Action> {
    let ns = sink.namespace().unwrap_or_else(|| "default".to_string());
    let name = sink.name_any();

    info!("Cleaning up Sink {}/{}", ns, name);

    if let Ok(router_endpoint) = find_router_endpoint(&ctx.client, &ns).await {
        if let Ok(_conn) = ctx.router_client.connect(&router_endpoint).await {
            info!("Deregistered Sink {}/{} from router", ns, name);
        }
    }

    Ok(Action::await_change())
}

async fn find_router_endpoint(client: &Client, ns: &str) -> Result<String> {
    let api: Api<EtlRouterCluster> = Api::namespaced(client.clone(), ns);

    let clusters = api.list(&Default::default()).await?;

    if let Some(cluster) = clusters.items.first() {
        if let Some(status) = &cluster.status {
            if let Some(endpoint) = &status.endpoint {
                return Ok(endpoint.clone());
            }
        }
        let name = cluster.metadata.name.as_ref().ok_or_else(|| {
            Error::RouterNotFound(format!("Cluster in namespace {} has no name", ns))
        })?;
        let port = cluster.spec.service.grpc_port;
        return Ok(format!("{}:{}", name, port));
    }

    Err(Error::RouterNotFound(format!(
        "No EtlRouterCluster found in namespace {}",
        ns
    )))
}

async fn update_sink_status(
    client: &Client,
    ns: &str,
    name: &str,
    status: SinkStatus,
) -> Result<()> {
    let api: Api<Sink> = Api::namespaced(client.clone(), ns);

    let patch = serde_json::json!({
        "status": status
    });

    api.patch_status(name, &PatchParams::apply("etl-operator"), &Patch::Merge(&patch))
        .await?;

    Ok(())
}

fn error_policy(sink: Arc<Sink>, error: &Error, _ctx: Arc<Context>) -> Action {
    warn!("Sink {} reconciliation error: {:?}", sink.name_any(), error);

    if error.is_retryable() {
        Action::requeue(Duration::from_secs(30))
    } else {
        Action::requeue(Duration::from_secs(300))
    }
}
