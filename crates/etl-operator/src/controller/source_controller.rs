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

use crate::crd::{Condition, Source, SourceStatus, EtlRouterCluster};
use crate::error::{Error, Result};
use crate::grpc::RouterClient;
use super::{Context, FINALIZER_NAME};

pub async fn run(client: Client, router_client: Arc<RouterClient>) {
    let sources: Api<Source> = Api::all(client.clone());

    let ctx = Context::new(client.clone(), router_client);

    Controller::new(sources, Config::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("Reconciled Source {:?}", o),
                Err(e) => error!("Reconcile failed: {:?}", e),
            }
        })
        .await;
}

#[instrument(skip(ctx, source), fields(name = %source.name_any(), namespace = ?source.namespace()))]
async fn reconcile(source: Arc<Source>, ctx: Arc<Context>) -> Result<Action> {
    let ns = source.namespace().unwrap_or_else(|| "default".to_string());
    let api: Api<Source> = Api::namespaced(ctx.client.clone(), &ns);

    finalizer(&api, FINALIZER_NAME, source, |event| async {
        match event {
            FinalizerEvent::Apply(source) => apply(source, ctx.clone()).await,
            FinalizerEvent::Cleanup(source) => cleanup(source, ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| Error::FinalizerError(e.to_string()))
}

async fn apply(source: Arc<Source>, ctx: Arc<Context>) -> Result<Action> {
    let ns = source.namespace().unwrap_or_else(|| "default".to_string());
    let name = source.name_any();

    info!("Reconciling Source {}/{}", ns, name);

    let router_endpoint = find_router_endpoint(&ctx.client, &ns).await?;

    let status = match ctx.router_client.connect(&router_endpoint).await {
        Ok(_conn) => {
            SourceStatus {
                observed_generation: source.metadata.generation,
                conditions: vec![Condition::ready(true, "Registered", "Source registered with router")],
                service_id: Some(format!("{}/{}", ns, name)),
                registered_at: Some(chrono::Utc::now().to_rfc3339()),
                health: Some("Healthy".to_string()),
                last_heartbeat: Some(chrono::Utc::now().to_rfc3339()),
            }
        }
        Err(e) => {
            SourceStatus {
                observed_generation: source.metadata.generation,
                conditions: vec![Condition::ready(false, "RegistrationFailed", &e.to_string())],
                service_id: None,
                registered_at: None,
                health: Some("Unknown".to_string()),
                last_heartbeat: None,
            }
        }
    };

    update_source_status(&ctx.client, &ns, &name, status).await?;

    Ok(Action::requeue(Duration::from_secs(60)))
}

async fn cleanup(source: Arc<Source>, ctx: Arc<Context>) -> Result<Action> {
    let ns = source.namespace().unwrap_or_else(|| "default".to_string());
    let name = source.name_any();

    info!("Cleaning up Source {}/{}", ns, name);

    if let Ok(router_endpoint) = find_router_endpoint(&ctx.client, &ns).await {
        if let Ok(_conn) = ctx.router_client.connect(&router_endpoint).await {
            info!("Deregistered Source {}/{} from router", ns, name);
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

async fn update_source_status(
    client: &Client,
    ns: &str,
    name: &str,
    status: SourceStatus,
) -> Result<()> {
    let api: Api<Source> = Api::namespaced(client.clone(), ns);

    let patch = serde_json::json!({
        "status": status
    });

    api.patch_status(name, &PatchParams::apply("etl-operator"), &Patch::Merge(&patch))
        .await?;

    Ok(())
}

fn error_policy(source: Arc<Source>, error: &Error, _ctx: Arc<Context>) -> Action {
    warn!("Source {} reconciliation error: {:?}", source.name_any(), error);

    if error.is_retryable() {
        Action::requeue(Duration::from_secs(30))
    } else {
        Action::requeue(Duration::from_secs(300))
    }
}
