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

use crate::crd::{
    Condition, Pipeline, PipelineStatus, Source, Transform, Sink, ConveyorCluster,
};
use crate::error::{Error, Result};
use crate::grpc::RouterClient;
use super::{Context, FINALIZER_NAME};

pub async fn run(client: Client, router_client: Arc<RouterClient>) {
    let pipelines: Api<Pipeline> = Api::all(client.clone());
    let sources: Api<Source> = Api::all(client.clone());
    let transforms: Api<Transform> = Api::all(client.clone());
    let sinks: Api<Sink> = Api::all(client.clone());

    let ctx = Context::new(client.clone(), router_client);

    Controller::new(pipelines, Config::default())
        .owns(sources, Config::default())
        .owns(transforms, Config::default())
        .owns(sinks, Config::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("Reconciled Pipeline {:?}", o),
                Err(e) => error!("Reconcile failed: {:?}", e),
            }
        })
        .await;
}

#[instrument(skip(ctx, pipeline), fields(name = %pipeline.name_any(), namespace = ?pipeline.namespace()))]
async fn reconcile(pipeline: Arc<Pipeline>, ctx: Arc<Context>) -> Result<Action> {
    let ns = pipeline.namespace().unwrap_or_else(|| "default".to_string());
    let api: Api<Pipeline> = Api::namespaced(ctx.client.clone(), &ns);

    finalizer(&api, FINALIZER_NAME, pipeline, |event| async {
        match event {
            FinalizerEvent::Apply(pipeline) => apply(pipeline, ctx.clone()).await,
            FinalizerEvent::Cleanup(pipeline) => cleanup(pipeline, ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| Error::FinalizerError(e.to_string()))
}

async fn apply(pipeline: Arc<Pipeline>, ctx: Arc<Context>) -> Result<Action> {
    let ns = pipeline.namespace().unwrap_or_else(|| "default".to_string());
    let name = pipeline.name_any();

    info!("Reconciling Pipeline {}/{}", ns, name);

    if let Err(e) = validate_dependencies(&pipeline, &ctx.client, &ns).await {
        let status = PipelineStatus {
            observed_generation: pipeline.metadata.generation,
            conditions: vec![Condition::ready(false, "DependencyMissing", &e.to_string())],
            pipeline_id: None,
            version: None,
            enabled: false,
            records_processed: None,
            stage_statuses: Default::default(),
        };
        update_pipeline_status(&ctx.client, &ns, &name, status).await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    let router_endpoint = find_router_endpoint(&ctx.client, &ns).await?;

    let status = match ctx.router_client.connect(&router_endpoint).await {
        Ok(mut conn) => {
            let existing_id = pipeline
                .status
                .as_ref()
                .and_then(|s| s.pipeline_id.clone());

            let result = if let Some(ref pipeline_id) = existing_id {
                conn.update_pipeline(pipeline_id, &pipeline).await
            } else {
                conn.create_pipeline(&pipeline).await
            };

            match result {
                Ok(res) => {
                    if pipeline.spec.enabled {
                        if let Err(e) = conn.enable_pipeline(&res.pipeline_id).await {
                            warn!("Failed to enable pipeline: {:?}", e);
                        }
                    } else if let Err(e) = conn.disable_pipeline(&res.pipeline_id, true).await {
                        warn!("Failed to disable pipeline: {:?}", e);
                    }

                    PipelineStatus {
                        observed_generation: pipeline.metadata.generation,
                        conditions: vec![Condition::ready(true, "Synced", "Pipeline synced with router")],
                        pipeline_id: Some(res.pipeline_id),
                        version: res.version,
                        enabled: pipeline.spec.enabled,
                        records_processed: None,
                        stage_statuses: Default::default(),
                    }
                }
                Err(e) => PipelineStatus {
                    observed_generation: pipeline.metadata.generation,
                    conditions: vec![Condition::ready(false, "SyncFailed", &e.to_string())],
                    pipeline_id: existing_id,
                    version: None,
                    enabled: false,
                    records_processed: None,
                    stage_statuses: Default::default(),
                },
            }
        }
        Err(e) => PipelineStatus {
            observed_generation: pipeline.metadata.generation,
            conditions: vec![Condition::ready(false, "ConnectionFailed", &e.to_string())],
            pipeline_id: None,
            version: None,
            enabled: false,
            records_processed: None,
            stage_statuses: Default::default(),
        },
    };

    update_pipeline_status(&ctx.client, &ns, &name, status).await?;

    Ok(Action::requeue(Duration::from_secs(60)))
}

async fn cleanup(pipeline: Arc<Pipeline>, ctx: Arc<Context>) -> Result<Action> {
    let ns = pipeline.namespace().unwrap_or_else(|| "default".to_string());
    let name = pipeline.name_any();

    info!("Cleaning up Pipeline {}/{}", ns, name);

    if let Some(pipeline_id) = pipeline.status.as_ref().and_then(|s| s.pipeline_id.as_ref()) {
        if let Ok(router_endpoint) = find_router_endpoint(&ctx.client, &ns).await {
            if let Ok(mut conn) = ctx.router_client.connect(&router_endpoint).await {
                if let Err(e) = conn.disable_pipeline(pipeline_id, true).await {
                    warn!("Failed to disable pipeline during cleanup: {:?}", e);
                }
                if let Err(e) = conn.delete_pipeline(pipeline_id, false).await {
                    warn!("Failed to delete pipeline during cleanup: {:?}", e);
                } else {
                    info!("Deleted pipeline {} from router", pipeline_id);
                }
            }
        }
    }

    Ok(Action::await_change())
}

async fn validate_dependencies(
    pipeline: &Pipeline,
    client: &Client,
    ns: &str,
) -> Result<()> {
    let sources: Api<Source> = Api::namespaced(client.clone(), ns);
    let transforms: Api<Transform> = Api::namespaced(client.clone(), ns);
    let sinks: Api<Sink> = Api::namespaced(client.clone(), ns);

    sources
        .get(&pipeline.spec.source)
        .await
        .map_err(|_| Error::DependencyNotFound(format!("Source '{}' not found", pipeline.spec.source)))?;

    for step in &pipeline.spec.steps {
        transforms
            .get(step)
            .await
            .map_err(|_| Error::DependencyNotFound(format!("Transform '{}' not found", step)))?;
    }

    sinks
        .get(&pipeline.spec.sink)
        .await
        .map_err(|_| Error::DependencyNotFound(format!("Sink '{}' not found", pipeline.spec.sink)))?;

    if let Some(dlq) = &pipeline.spec.dlq {
        sinks
            .get(&dlq.sink)
            .await
            .map_err(|_| Error::DependencyNotFound(format!("DLQ Sink '{}' not found", dlq.sink)))?;
    }

    Ok(())
}

async fn find_router_endpoint(client: &Client, ns: &str) -> Result<String> {
    let api: Api<ConveyorCluster> = Api::namespaced(client.clone(), ns);

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
        "No ConveyorCluster found in namespace {}",
        ns
    )))
}

async fn update_pipeline_status(
    client: &Client,
    ns: &str,
    name: &str,
    status: PipelineStatus,
) -> Result<()> {
    let api: Api<Pipeline> = Api::namespaced(client.clone(), ns);

    let patch = serde_json::json!({
        "status": status
    });

    api.patch_status(name, &PatchParams::apply("etl-operator"), &Patch::Merge(&patch))
        .await?;

    Ok(())
}

fn error_policy(pipeline: Arc<Pipeline>, error: &Error, _ctx: Arc<Context>) -> Action {
    warn!("Pipeline {} reconciliation error: {:?}", pipeline.name_any(), error);

    if error.is_retryable() {
        Action::requeue(Duration::from_secs(30))
    } else {
        Action::requeue(Duration::from_secs(300))
    }
}
