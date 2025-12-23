use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    Affinity as K8sAffinity, ConfigMap, Container, ContainerPort, EnvVar, EnvVarSource,
    NodeAffinity as K8sNodeAffinity, NodeSelector as K8sNodeSelector,
    NodeSelectorRequirement as K8sNodeSelectorRequirement, NodeSelectorTerm as K8sNodeSelectorTerm,
    ObjectFieldSelector, PersistentVolumeClaim, PersistentVolumeClaimSpec,
    PodAffinity as K8sPodAffinity, PodAffinityTerm as K8sPodAffinityTerm,
    PodAntiAffinity as K8sPodAntiAffinity, PodSpec, PodTemplateSpec,
    PreferredSchedulingTerm as K8sPreferredSchedulingTerm, Probe, Service, ServicePort, ServiceSpec,
    Toleration as K8sToleration, Volume, VolumeMount, ConfigMapVolumeSource, GRPCAction,
    WeightedPodAffinityTerm as K8sWeightedPodAffinityTerm,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector as K8sLabelSelector;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement as K8sLabelSelectorRequirement;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        finalizer::{finalizer, Event as FinalizerEvent},
        watcher::Config,
    },
    Client, Resource, ResourceExt,
};
use tracing::{error, info, instrument, warn};

use crate::crd::{
    Condition, ConveyorCluster, ConveyorClusterStatus, NodeStatusInfo,
};
use crate::error::{Error, Result};
use crate::grpc::RouterClient;
use super::{Context, FINALIZER_NAME};

pub async fn run(client: Client, router_client: Arc<RouterClient>) {
    let clusters: Api<ConveyorCluster> = Api::all(client.clone());
    let statefulsets: Api<StatefulSet> = Api::all(client.clone());
    let services: Api<Service> = Api::all(client.clone());
    let configmaps: Api<ConfigMap> = Api::all(client.clone());

    let ctx = Context::new(client.clone(), router_client);

    Controller::new(clusters, Config::default())
        .owns(statefulsets, Config::default())
        .owns(services, Config::default())
        .owns(configmaps, Config::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("Reconciled ConveyorCluster {:?}", o),
                Err(e) => error!("Reconcile failed: {:?}", e),
            }
        })
        .await;
}

#[instrument(skip(ctx, cluster), fields(name = %cluster.name_any(), namespace = ?cluster.namespace()))]
async fn reconcile(cluster: Arc<ConveyorCluster>, ctx: Arc<Context>) -> Result<Action> {
    let ns = cluster.namespace().unwrap_or_else(|| "default".to_string());
    let api: Api<ConveyorCluster> = Api::namespaced(ctx.client.clone(), &ns);

    finalizer(&api, FINALIZER_NAME, cluster, |event| async {
        match event {
            FinalizerEvent::Apply(cluster) => apply(cluster, ctx.clone()).await,
            FinalizerEvent::Cleanup(cluster) => cleanup(cluster, ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| Error::FinalizerError(e.to_string()))
}

async fn apply(cluster: Arc<ConveyorCluster>, ctx: Arc<Context>) -> Result<Action> {
    let ns = cluster.namespace().unwrap_or_else(|| "default".to_string());
    let name = cluster.name_any();

    info!("Reconciling ConveyorCluster {}/{}", ns, name);

    reconcile_configmap(&ctx.client, &ns, &name, &cluster.spec).await?;
    reconcile_headless_service(&ctx.client, &ns, &name, &cluster.spec).await?;
    reconcile_client_service(&ctx.client, &ns, &name, &cluster.spec).await?;
    reconcile_statefulset(&ctx.client, &ns, &name, &cluster.spec).await?;

    let status = poll_cluster_status(&ctx, &ns, &name, &cluster.spec).await;
    update_cluster_status(&ctx.client, &ns, &name, status).await?;

    Ok(Action::requeue(Duration::from_secs(30)))
}

async fn cleanup(cluster: Arc<ConveyorCluster>, _ctx: Arc<Context>) -> Result<Action> {
    info!(
        "Cleaning up ConveyorCluster {}/{}",
        cluster.namespace().unwrap_or_default(),
        cluster.name_any()
    );
    Ok(Action::await_change())
}

fn labels(name: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), "etl-router".to_string());
    labels.insert("app.kubernetes.io/instance".to_string(), name.to_string());
    labels.insert("app.kubernetes.io/managed-by".to_string(), "etl-operator".to_string());
    labels
}

#[allow(dead_code)]
fn owner_reference(cluster: &ConveyorCluster) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
    k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
        api_version: ConveyorCluster::api_version(&()).to_string(),
        kind: ConveyorCluster::kind(&()).to_string(),
        name: cluster.metadata.name.clone().unwrap_or_default(),
        uid: cluster.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

async fn reconcile_configmap(
    client: &Client,
    ns: &str,
    name: &str,
    spec: &crate::crd::ConveyorClusterSpec,
) -> Result<()> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), ns);

    let config_content = format!(
        r#"
election_timeout_ms: {}
heartbeat_interval_ms: {}
snapshot_interval: {}
"#,
        spec.raft.election_timeout_ms,
        spec.raft.heartbeat_interval_ms,
        spec.raft.snapshot_interval
    );

    let cm = ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!("{}-config", name)),
            namespace: Some(ns.to_string()),
            labels: Some(labels(name)),
            ..Default::default()
        },
        data: Some({
            let mut data = BTreeMap::new();
            data.insert("config.yaml".to_string(), config_content);
            data
        }),
        ..Default::default()
    };

    api.patch(
        &format!("{}-config", name),
        &PatchParams::apply("etl-operator"),
        &Patch::Apply(&cm),
    )
    .await?;

    Ok(())
}

async fn reconcile_headless_service(
    client: &Client,
    ns: &str,
    name: &str,
    spec: &crate::crd::ConveyorClusterSpec,
) -> Result<()> {
    let api: Api<Service> = Api::namespaced(client.clone(), ns);

    let svc = Service {
        metadata: ObjectMeta {
            name: Some(format!("{}-headless", name)),
            namespace: Some(ns.to_string()),
            labels: Some(labels(name)),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            selector: Some(labels(name)),
            ports: Some(vec![
                ServicePort {
                    name: Some("grpc".to_string()),
                    port: spec.service.grpc_port,
                    ..Default::default()
                },
                ServicePort {
                    name: Some("raft".to_string()),
                    port: spec.service.raft_port,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    };

    api.patch(
        &format!("{}-headless", name),
        &PatchParams::apply("etl-operator"),
        &Patch::Apply(&svc),
    )
    .await?;

    Ok(())
}

async fn reconcile_client_service(
    client: &Client,
    ns: &str,
    name: &str,
    spec: &crate::crd::ConveyorClusterSpec,
) -> Result<()> {
    let api: Api<Service> = Api::namespaced(client.clone(), ns);

    let mut svc_labels = labels(name);
    svc_labels.extend(spec.service.annotations.clone());

    let svc = Service {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(ns.to_string()),
            labels: Some(labels(name)),
            annotations: if spec.service.annotations.is_empty() {
                None
            } else {
                Some(spec.service.annotations.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            },
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(labels(name)),
            type_: spec.service.service_type.clone(),
            ports: Some(vec![
                ServicePort {
                    name: Some("grpc".to_string()),
                    port: spec.service.grpc_port,
                    ..Default::default()
                },
                ServicePort {
                    name: Some("graphql".to_string()),
                    port: spec.service.graphql_port,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    };

    api.patch(
        name,
        &PatchParams::apply("etl-operator"),
        &Patch::Apply(&svc),
    )
    .await?;

    Ok(())
}

async fn reconcile_statefulset(
    client: &Client,
    ns: &str,
    name: &str,
    spec: &crate::crd::ConveyorClusterSpec,
) -> Result<()> {
    let api: Api<StatefulSet> = Api::namespaced(client.clone(), ns);

    let mut env_vars = vec![
        EnvVar {
            name: "POD_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: "metadata.name".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "NAMESPACE".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: "metadata.namespace".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "CLUSTER_NAME".to_string(),
            value: Some(name.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "REPLICAS".to_string(),
            value: Some(spec.replicas.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "HEADLESS_SERVICE".to_string(),
            value: Some(format!("{}-headless", name)),
            ..Default::default()
        },
    ];

    for (k, v) in &spec.env {
        env_vars.push(EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..Default::default()
        });
    }

    let sts = StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(ns.to_string()),
            labels: Some(labels(name)),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(spec.replicas),
            service_name: format!("{}-headless", name),
            selector: K8sLabelSelector {
                match_labels: Some(labels(name)),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels(name)),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "etl-router".to_string(),
                        image: Some(spec.image.clone()),
                        image_pull_policy: spec.image_pull_policy.clone(),
                        ports: Some(vec![
                            ContainerPort {
                                container_port: spec.service.grpc_port,
                                name: Some("grpc".to_string()),
                                ..Default::default()
                            },
                            ContainerPort {
                                container_port: spec.service.raft_port,
                                name: Some("raft".to_string()),
                                ..Default::default()
                            },
                            ContainerPort {
                                container_port: spec.service.graphql_port,
                                name: Some("graphql".to_string()),
                                ..Default::default()
                            },
                        ]),
                        env: Some(env_vars),
                        volume_mounts: Some(vec![
                            VolumeMount {
                                name: "data".to_string(),
                                mount_path: "/data".to_string(),
                                ..Default::default()
                            },
                            VolumeMount {
                                name: "config".to_string(),
                                mount_path: "/etc/etl-router".to_string(),
                                ..Default::default()
                            },
                        ]),
                        resources: spec.resources.as_ref().map(|r| {
                            k8s_openapi::api::core::v1::ResourceRequirements {
                                requests: r.requests.as_ref().map(|req| {
                                    let mut m = BTreeMap::new();
                                    if let Some(cpu) = &req.cpu {
                                        m.insert("cpu".to_string(), Quantity(cpu.clone()));
                                    }
                                    if let Some(memory) = &req.memory {
                                        m.insert("memory".to_string(), Quantity(memory.clone()));
                                    }
                                    m
                                }),
                                limits: r.limits.as_ref().map(|lim| {
                                    let mut m = BTreeMap::new();
                                    if let Some(cpu) = &lim.cpu {
                                        m.insert("cpu".to_string(), Quantity(cpu.clone()));
                                    }
                                    if let Some(memory) = &lim.memory {
                                        m.insert("memory".to_string(), Quantity(memory.clone()));
                                    }
                                    m
                                }),
                                ..Default::default()
                            }
                        }),
                        liveness_probe: Some(Probe {
                            grpc: Some(GRPCAction {
                                port: spec.service.grpc_port,
                                service: Some("grpc.health.v1.Health".to_string()),
                            }),
                            initial_delay_seconds: Some(30),
                            period_seconds: Some(10),
                            ..Default::default()
                        }),
                        readiness_probe: Some(Probe {
                            grpc: Some(GRPCAction {
                                port: spec.service.grpc_port,
                                service: Some("grpc.health.v1.Health".to_string()),
                            }),
                            initial_delay_seconds: Some(5),
                            period_seconds: Some(5),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    volumes: Some(vec![Volume {
                        name: "config".to_string(),
                        config_map: Some(ConfigMapVolumeSource {
                            name: format!("{}-config", name),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]),
                    node_selector: if spec.node_selector.is_empty() {
                        None
                    } else {
                        Some(spec.node_selector.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    },
                    tolerations: if spec.tolerations.is_empty() {
                        None
                    } else {
                        Some(spec.tolerations.iter().map(convert_toleration).collect())
                    },
                    affinity: spec.affinity.as_ref().map(convert_affinity),
                    ..Default::default()
                }),
            },
            volume_claim_templates: Some(vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some("data".to_string()),
                    ..Default::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    storage_class_name: Some(spec.storage.storage_class.clone()),
                    resources: Some(k8s_openapi::api::core::v1::VolumeResourceRequirements {
                        requests: Some({
                            let mut m = BTreeMap::new();
                            m.insert("storage".to_string(), Quantity(spec.storage.size.clone()));
                            m
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };

    api.patch(name, &PatchParams::apply("etl-operator"), &Patch::Apply(&sts))
        .await?;

    Ok(())
}

async fn poll_cluster_status(
    ctx: &Context,
    _ns: &str,
    name: &str,
    spec: &crate::crd::ConveyorClusterSpec,
) -> ConveyorClusterStatus {
    let endpoint = format!("{}:{}", name, spec.service.grpc_port);

    match ctx.router_client.connect(&endpoint).await {
        Ok(mut conn) => match conn.get_cluster_status().await {
            Ok(status) => ConveyorClusterStatus {
                observed_generation: None,
                conditions: vec![Condition::ready(true, "ClusterHealthy", "Cluster is healthy")],
                replicas: spec.replicas,
                ready_replicas: status.nodes.iter().filter(|n| n.healthy).count() as i32,
                leader_id: Some(status.leader_id),
                term: Some(status.term),
                cluster_health: status.health,
                nodes: status
                    .nodes
                    .into_iter()
                    .map(|n| NodeStatusInfo {
                        node_id: n.node_id,
                        pod_name: format!("{}-{}", name, n.node_id),
                        address: n.address,
                        role: n.role,
                        healthy: n.healthy,
                    })
                    .collect(),
                endpoint: Some(endpoint),
            },
            Err(e) => ConveyorClusterStatus {
                observed_generation: None,
                conditions: vec![Condition::ready(false, "StatusFetchFailed", &e.to_string())],
                replicas: spec.replicas,
                ready_replicas: 0,
                leader_id: None,
                term: None,
                cluster_health: "Unknown".to_string(),
                nodes: vec![],
                endpoint: Some(endpoint),
            },
        },
        Err(e) => ConveyorClusterStatus {
            observed_generation: None,
            conditions: vec![Condition::ready(false, "ConnectionFailed", &e.to_string())],
            replicas: spec.replicas,
            ready_replicas: 0,
            leader_id: None,
            term: None,
            cluster_health: "Unknown".to_string(),
            nodes: vec![],
            endpoint: None,
        },
    }
}

async fn update_cluster_status(
    client: &Client,
    ns: &str,
    name: &str,
    status: ConveyorClusterStatus,
) -> Result<()> {
    let api: Api<ConveyorCluster> = Api::namespaced(client.clone(), ns);

    let patch = serde_json::json!({
        "status": status
    });

    api.patch_status(name, &PatchParams::apply("etl-operator"), &Patch::Merge(&patch))
        .await?;

    Ok(())
}

fn error_policy(cluster: Arc<ConveyorCluster>, error: &Error, _ctx: Arc<Context>) -> Action {
    warn!(
        "ConveyorCluster {} reconciliation error: {:?}",
        cluster.name_any(),
        error
    );

    if error.is_retryable() {
        Action::requeue(Duration::from_secs(30))
    } else {
        Action::requeue(Duration::from_secs(300))
    }
}

fn convert_toleration(t: &crate::crd::Toleration) -> K8sToleration {
    K8sToleration {
        key: t.key.clone(),
        operator: t.operator.clone(),
        value: t.value.clone(),
        effect: t.effect.clone(),
        toleration_seconds: t.toleration_seconds,
    }
}

fn convert_affinity(a: &crate::crd::Affinity) -> K8sAffinity {
    K8sAffinity {
        node_affinity: a.node_affinity.as_ref().map(convert_node_affinity),
        pod_affinity: a.pod_affinity.as_ref().map(convert_pod_affinity),
        pod_anti_affinity: a.pod_anti_affinity.as_ref().map(convert_pod_anti_affinity),
    }
}

fn convert_node_affinity(na: &crate::crd::NodeAffinity) -> K8sNodeAffinity {
    K8sNodeAffinity {
        required_during_scheduling_ignored_during_execution: na
            .required_during_scheduling_ignored_during_execution
            .as_ref()
            .map(|ns| K8sNodeSelector {
                node_selector_terms: ns
                    .node_selector_terms
                    .iter()
                    .map(convert_node_selector_term)
                    .collect(),
            }),
        preferred_during_scheduling_ignored_during_execution: if na
            .preferred_during_scheduling_ignored_during_execution
            .is_empty()
        {
            None
        } else {
            Some(
                na.preferred_during_scheduling_ignored_during_execution
                    .iter()
                    .map(|p| K8sPreferredSchedulingTerm {
                        weight: p.weight,
                        preference: convert_node_selector_term(&p.preference),
                    })
                    .collect(),
            )
        },
    }
}

fn convert_node_selector_term(nst: &crate::crd::NodeSelectorTerm) -> K8sNodeSelectorTerm {
    K8sNodeSelectorTerm {
        match_expressions: if nst.match_expressions.is_empty() {
            None
        } else {
            Some(
                nst.match_expressions
                    .iter()
                    .map(|e| K8sNodeSelectorRequirement {
                        key: e.key.clone(),
                        operator: e.operator.clone(),
                        values: if e.values.is_empty() {
                            None
                        } else {
                            Some(e.values.clone())
                        },
                    })
                    .collect(),
            )
        },
        match_fields: if nst.match_fields.is_empty() {
            None
        } else {
            Some(
                nst.match_fields
                    .iter()
                    .map(|f| K8sNodeSelectorRequirement {
                        key: f.key.clone(),
                        operator: f.operator.clone(),
                        values: if f.values.is_empty() {
                            None
                        } else {
                            Some(f.values.clone())
                        },
                    })
                    .collect(),
            )
        },
    }
}

fn convert_pod_affinity(pa: &crate::crd::PodAffinity) -> K8sPodAffinity {
    K8sPodAffinity {
        required_during_scheduling_ignored_during_execution: if pa
            .required_during_scheduling_ignored_during_execution
            .is_empty()
        {
            None
        } else {
            Some(
                pa.required_during_scheduling_ignored_during_execution
                    .iter()
                    .map(convert_pod_affinity_term)
                    .collect(),
            )
        },
        preferred_during_scheduling_ignored_during_execution: if pa
            .preferred_during_scheduling_ignored_during_execution
            .is_empty()
        {
            None
        } else {
            Some(
                pa.preferred_during_scheduling_ignored_during_execution
                    .iter()
                    .map(|w| K8sWeightedPodAffinityTerm {
                        weight: w.weight,
                        pod_affinity_term: convert_pod_affinity_term(&w.pod_affinity_term),
                    })
                    .collect(),
            )
        },
    }
}

fn convert_pod_anti_affinity(paa: &crate::crd::PodAntiAffinity) -> K8sPodAntiAffinity {
    K8sPodAntiAffinity {
        required_during_scheduling_ignored_during_execution: if paa
            .required_during_scheduling_ignored_during_execution
            .is_empty()
        {
            None
        } else {
            Some(
                paa.required_during_scheduling_ignored_during_execution
                    .iter()
                    .map(convert_pod_affinity_term)
                    .collect(),
            )
        },
        preferred_during_scheduling_ignored_during_execution: if paa
            .preferred_during_scheduling_ignored_during_execution
            .is_empty()
        {
            None
        } else {
            Some(
                paa.preferred_during_scheduling_ignored_during_execution
                    .iter()
                    .map(|w| K8sWeightedPodAffinityTerm {
                        weight: w.weight,
                        pod_affinity_term: convert_pod_affinity_term(&w.pod_affinity_term),
                    })
                    .collect(),
            )
        },
    }
}

fn convert_pod_affinity_term(pat: &crate::crd::PodAffinityTerm) -> K8sPodAffinityTerm {
    K8sPodAffinityTerm {
        label_selector: pat.label_selector.as_ref().map(convert_label_selector),
        namespace_selector: pat.namespace_selector.as_ref().map(convert_label_selector),
        namespaces: if pat.namespaces.is_empty() {
            None
        } else {
            Some(pat.namespaces.clone())
        },
        topology_key: pat.topology_key.clone(),
        ..Default::default()
    }
}

fn convert_label_selector(ls: &crate::crd::LabelSelector) -> K8sLabelSelector {
    K8sLabelSelector {
        match_labels: if ls.match_labels.is_empty() {
            None
        } else {
            Some(ls.match_labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        },
        match_expressions: if ls.match_expressions.is_empty() {
            None
        } else {
            Some(
                ls.match_expressions
                    .iter()
                    .map(|e| K8sLabelSelectorRequirement {
                        key: e.key.clone(),
                        operator: e.operator.clone(),
                        values: if e.values.is_empty() {
                            None
                        } else {
                            Some(e.values.clone())
                        },
                    })
                    .collect(),
            )
        },
    }
}
