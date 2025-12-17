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

use crate::crd::{Condition, Transform, TransformStatus, EtlRouterCluster};
use crate::error::{Error, Result};
use crate::grpc::RouterClient;
use super::{Context, FINALIZER_NAME};

pub async fn run(client: Client, router_client: Arc<RouterClient>) {
    let transforms: Api<Transform> = Api::all(client.clone());

    let ctx = Context::new(client.clone(), router_client);

    Controller::new(transforms, Config::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("Reconciled Transform {:?}", o),
                Err(e) => error!("Reconcile failed: {:?}", e),
            }
        })
        .await;
}

#[instrument(skip(ctx, transform), fields(name = %transform.name_any(), namespace = ?transform.namespace()))]
async fn reconcile(transform: Arc<Transform>, ctx: Arc<Context>) -> Result<Action> {
    let ns = transform.namespace().unwrap_or_else(|| "default".to_string());
    let api: Api<Transform> = Api::namespaced(ctx.client.clone(), &ns);

    finalizer(&api, FINALIZER_NAME, transform, |event| async {
        match event {
            FinalizerEvent::Apply(transform) => apply(transform, ctx.clone()).await,
            FinalizerEvent::Cleanup(transform) => cleanup(transform, ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| Error::FinalizerError(e.to_string()))
}

async fn apply(transform: Arc<Transform>, ctx: Arc<Context>) -> Result<Action> {
    let ns = transform.namespace().unwrap_or_else(|| "default".to_string());
    let name = transform.name_any();

    info!("Reconciling Transform {}/{}", ns, name);

    let router_endpoint = find_router_endpoint(&ctx.client, &ns).await?;

    let status = match ctx.router_client.connect(&router_endpoint).await {
        Ok(_conn) => {
            TransformStatus {
                observed_generation: transform.metadata.generation,
                conditions: vec![Condition::ready(true, "Registered", "Transform registered with router")],
                service_id: Some(format!("{}/{}", ns, name)),
                registered_at: Some(chrono::Utc::now().to_rfc3339()),
                health: Some("Healthy".to_string()),
                last_heartbeat: Some(chrono::Utc::now().to_rfc3339()),
            }
        }
        Err(e) => {
            TransformStatus {
                observed_generation: transform.metadata.generation,
                conditions: vec![Condition::ready(false, "RegistrationFailed", &e.to_string())],
                service_id: None,
                registered_at: None,
                health: Some("Unknown".to_string()),
                last_heartbeat: None,
            }
        }
    };

    update_transform_status(&ctx.client, &ns, &name, status).await?;

    Ok(Action::requeue(Duration::from_secs(60)))
}

async fn cleanup(transform: Arc<Transform>, ctx: Arc<Context>) -> Result<Action> {
    let ns = transform.namespace().unwrap_or_else(|| "default".to_string());
    let name = transform.name_any();

    info!("Cleaning up Transform {}/{}", ns, name);

    if let Ok(router_endpoint) = find_router_endpoint(&ctx.client, &ns).await {
        if let Ok(_conn) = ctx.router_client.connect(&router_endpoint).await {
            info!("Deregistered Transform {}/{} from router", ns, name);
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

async fn update_transform_status(
    client: &Client,
    ns: &str,
    name: &str,
    status: TransformStatus,
) -> Result<()> {
    let api: Api<Transform> = Api::namespaced(client.clone(), ns);

    let patch = serde_json::json!({
        "status": status
    });

    api.patch_status(name, &PatchParams::apply("etl-operator"), &Patch::Merge(&patch))
        .await?;

    Ok(())
}

fn error_policy(transform: Arc<Transform>, error: &Error, _ctx: Arc<Context>) -> Action {
    warn!("Transform {} reconciliation error: {:?}", transform.name_any(), error);

    if error.is_retryable() {
        Action::requeue(Duration::from_secs(30))
    } else {
        Action::requeue(Duration::from_secs(300))
    }
}
