use std::collections::HashMap;

use crate::manifest::{GrpcEndpoint, PipelineManifest};
use crate::registry::Registry;

#[derive(Debug, Clone)]
pub struct OptimizedDag {
    pub id: String,
    pub sources: HashMap<String, SourceNode>,
    pub stages: HashMap<String, StageNode>,
    pub sinks: HashMap<String, SinkNode>,
    pub edges: Vec<DagEdge>,
}

#[derive(Debug, Clone)]
pub struct SourceNode {
    pub id: String,
    pub grpc: GrpcEndpoint,
    pub consumers: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct StageNode {
    pub id: String,
    pub grpc: GrpcEndpoint,
    pub source_pipelines: Vec<String>,
    pub is_shared: bool,
}

#[derive(Debug, Clone)]
pub struct SinkNode {
    pub id: String,
    pub grpc: GrpcEndpoint,
    pub source_pipelines: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DagEdge {
    pub from: String,
    pub to: String,
    pub pipelines: Vec<String>,
}

pub struct Optimizer<'a> {
    registry: &'a Registry,
    namespace: String,
}

impl<'a> Optimizer<'a> {
    pub fn new(registry: &'a Registry, namespace: impl Into<String>) -> Self {
        Self {
            registry,
            namespace: namespace.into(),
        }
    }

    pub fn optimize(&self) -> OptimizedDag {
        let pipelines = self.registry.list_pipelines(Some(&self.namespace));

        let mut sources: HashMap<String, SourceNode> = HashMap::new();
        let mut stages: HashMap<String, StageNode> = HashMap::new();
        let mut sinks: HashMap<String, SinkNode> = HashMap::new();
        let mut edges: Vec<DagEdge> = Vec::new();

        let source_groups = self.group_by_source(&pipelines);

        for (source_name, pipeline_names) in &source_groups {
            if let Some(source_spec) = self.registry.source_spec(&self.namespace, source_name) {
                sources.insert(
                    source_name.clone(),
                    SourceNode {
                        id: source_name.clone(),
                        grpc: source_spec.grpc.clone(),
                        consumers: pipeline_names.clone(),
                    },
                );
            }
        }

        for (source_name, pipeline_names) in &source_groups {
            let pipelines_for_source: Vec<_> = pipeline_names
                .iter()
                .filter_map(|name| self.registry.get_pipeline(&self.namespace, name))
                .collect();

            let shared_prefix = self.find_shared_prefix(&pipelines_for_source);

            let mut prev_node = format!("source:{}", source_name);

            for step_name in shared_prefix.iter() {
                let stage_id = format!("shared:{}:{}", source_name, step_name);

                if let Some(transform_spec) = self.registry.transform_spec(&self.namespace, step_name) {
                    stages.entry(stage_id.clone()).or_insert_with(|| StageNode {
                        id: stage_id.clone(),
                        grpc: transform_spec.grpc.clone(),
                        source_pipelines: pipeline_names.clone(),
                        is_shared: true,
                    });
                }

                edges.push(DagEdge {
                    from: prev_node.clone(),
                    to: stage_id.clone(),
                    pipelines: pipeline_names.clone(),
                });

                prev_node = stage_id;
            }

            for pipeline in &pipelines_for_source {
                let pipeline_name = &pipeline.metadata.name;
                let remaining_steps: Vec<_> = pipeline
                    .spec
                    .steps
                    .iter()
                    .skip(shared_prefix.len())
                    .collect();

                let mut pipeline_prev = prev_node.clone();

                for step_name in remaining_steps {
                    let stage_id = format!("{}:{}", pipeline_name, step_name);

                    if let Some(transform_spec) =
                        self.registry.transform_spec(&self.namespace, step_name)
                    {
                        stages.entry(stage_id.clone()).or_insert_with(|| StageNode {
                            id: stage_id.clone(),
                            grpc: transform_spec.grpc.clone(),
                            source_pipelines: vec![pipeline_name.clone()],
                            is_shared: false,
                        });
                    }

                    edges.push(DagEdge {
                        from: pipeline_prev.clone(),
                        to: stage_id.clone(),
                        pipelines: vec![pipeline_name.clone()],
                    });

                    pipeline_prev = stage_id;
                }

                let sink_id = format!("sink:{}", pipeline.spec.sink);
                if let Some(sink_spec) = self.registry.sink_spec(&self.namespace, &pipeline.spec.sink)
                {
                    sinks
                        .entry(sink_id.clone())
                        .and_modify(|s| {
                            if !s.source_pipelines.contains(pipeline_name) {
                                s.source_pipelines.push(pipeline_name.clone());
                            }
                        })
                        .or_insert_with(|| SinkNode {
                            id: sink_id.clone(),
                            grpc: sink_spec.grpc.clone(),
                            source_pipelines: vec![pipeline_name.clone()],
                        });
                }

                edges.push(DagEdge {
                    from: pipeline_prev,
                    to: sink_id,
                    pipelines: vec![pipeline_name.clone()],
                });
            }
        }

        OptimizedDag {
            id: format!("optimized-{}", self.namespace),
            sources,
            stages,
            sinks,
            edges,
        }
    }

    fn group_by_source(&self, pipelines: &[&PipelineManifest]) -> HashMap<String, Vec<String>> {
        let mut groups: HashMap<String, Vec<String>> = HashMap::new();

        for pipeline in pipelines {
            groups
                .entry(pipeline.spec.source.clone())
                .or_default()
                .push(pipeline.metadata.name.clone());
        }

        groups
    }

    fn find_shared_prefix(&self, pipelines: &[&PipelineManifest]) -> Vec<String> {
        if pipelines.is_empty() {
            return Vec::new();
        }

        if pipelines.len() == 1 {
            return Vec::new();
        }

        let first_steps = &pipelines[0].spec.steps;
        let mut shared = Vec::new();

        for (idx, step) in first_steps.iter().enumerate() {
            let all_have_same = pipelines.iter().skip(1).all(|p| {
                p.spec.steps.get(idx).is_some_and(|s| s == step)
            });

            if all_have_same {
                if self.is_stateless_transform(step) {
                    shared.push(step.clone());
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        shared
    }

    fn is_stateless_transform(&self, name: &str) -> bool {
        if let Some(transform) = self.registry.get_transform(&self.namespace, name) {
            let config = &transform.spec.config;

            if let Some(t) = config.get("type").and_then(|v| v.as_str()) {
                return matches!(
                    t.to_lowercase().as_str(),
                    "filter" | "map" | "project" | "rename" | "cast" | "mask" | "validate"
                );
            }

        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{
        AnyManifest, GrpcEndpoint, Manifest, Metadata, PipelineSpec, ResourceKind, SinkSpec,
        SourceSpec, TransformSpec,
    };

    fn make_source(name: &str) -> AnyManifest {
        AnyManifest::Source(Manifest {
            api_version: "etl.router/v1".to_string(),
            kind: ResourceKind::Source,
            metadata: Metadata::new(name),
            spec: SourceSpec {
                grpc: GrpcEndpoint {
                    endpoint: format!("{}-svc:50051", name),
                    proto: None,
                    tls: None,
                },
                config: serde_json::Value::Null,
            },
        })
    }

    fn make_transform(name: &str, transform_type: &str) -> AnyManifest {
        AnyManifest::Transform(Manifest {
            api_version: "etl.router/v1".to_string(),
            kind: ResourceKind::Transform,
            metadata: Metadata::new(name),
            spec: TransformSpec {
                grpc: GrpcEndpoint {
                    endpoint: format!("{}-svc:50051", name),
                    proto: None,
                    tls: None,
                },
                config: serde_json::json!({ "type": transform_type }),
            },
        })
    }

    fn make_sink(name: &str) -> AnyManifest {
        AnyManifest::Sink(Manifest {
            api_version: "etl.router/v1".to_string(),
            kind: ResourceKind::Sink,
            metadata: Metadata::new(name),
            spec: SinkSpec {
                grpc: GrpcEndpoint {
                    endpoint: format!("{}-svc:50051", name),
                    proto: None,
                    tls: None,
                },
                config: serde_json::Value::Null,
            },
        })
    }

    fn make_pipeline(name: &str, source: &str, steps: &[&str], sink: &str) -> AnyManifest {
        AnyManifest::Pipeline(Manifest {
            api_version: "etl.router/v1".to_string(),
            kind: ResourceKind::Pipeline,
            metadata: Metadata::new(name),
            spec: PipelineSpec {
                source: source.to_string(),
                steps: steps.iter().map(|s| s.to_string()).collect(),
                sink: sink.to_string(),
                dlq: None,
                enabled: true,
            },
        })
    }

    #[test]
    fn test_optimize_single_pipeline() {
        let mut registry = Registry::new();

        registry.apply(make_source("kafka")).unwrap();
        registry.apply(make_transform("filter", "filter")).unwrap();
        registry.apply(make_sink("s3")).unwrap();
        registry.apply(make_pipeline("pipeline-a", "kafka", &["filter"], "s3")).unwrap();

        let optimizer = Optimizer::new(&registry, "default");
        let dag = optimizer.optimize();

        assert_eq!(dag.sources.len(), 1);
        assert!(dag.sources.contains_key("kafka"));

        assert_eq!(dag.sinks.len(), 1);
        assert!(dag.sinks.contains_key("sink:s3"));

        assert_eq!(dag.stages.len(), 1);
    }

    #[test]
    fn test_optimize_shared_source() {
        let mut registry = Registry::new();

        registry.apply(make_source("kafka")).unwrap();
        registry.apply(make_transform("filter", "filter")).unwrap();
        registry.apply(make_transform("mask", "mask")).unwrap();
        registry.apply(make_sink("s3")).unwrap();
        registry.apply(make_sink("grpc")).unwrap();

        registry.apply(make_pipeline("pipeline-a", "kafka", &["filter"], "s3")).unwrap();
        registry.apply(make_pipeline("pipeline-b", "kafka", &["mask"], "grpc")).unwrap();

        let optimizer = Optimizer::new(&registry, "default");
        let dag = optimizer.optimize();

        assert_eq!(dag.sources.len(), 1);
        assert!(dag.sources.contains_key("kafka"));

        let kafka_source = &dag.sources["kafka"];
        assert_eq!(kafka_source.consumers.len(), 2);
    }

    #[test]
    fn test_optimize_shared_prefix() {
        let mut registry = Registry::new();

        registry.apply(make_source("kafka")).unwrap();
        registry.apply(make_transform("filter", "filter")).unwrap();
        registry.apply(make_transform("mask", "mask")).unwrap();
        registry.apply(make_transform("aggregate", "aggregate")).unwrap();
        registry.apply(make_sink("s3")).unwrap();
        registry.apply(make_sink("grpc")).unwrap();

        registry.apply(make_pipeline("pipeline-a", "kafka", &["filter", "mask"], "s3")).unwrap();
        registry.apply(make_pipeline("pipeline-b", "kafka", &["filter", "aggregate"], "grpc")).unwrap();

        let optimizer = Optimizer::new(&registry, "default");
        let dag = optimizer.optimize();

        let shared_stages: Vec<_> = dag.stages.values().filter(|s| s.is_shared).collect();
        assert_eq!(shared_stages.len(), 1);
        assert!(shared_stages[0].id.contains("filter"));
    }

    #[test]
    fn test_optimize_different_sources() {
        let mut registry = Registry::new();

        registry.apply(make_source("kafka")).unwrap();
        registry.apply(make_source("kinesis")).unwrap();
        registry.apply(make_transform("filter", "filter")).unwrap();
        registry.apply(make_sink("s3")).unwrap();

        registry.apply(make_pipeline("pipeline-a", "kafka", &["filter"], "s3")).unwrap();
        registry.apply(make_pipeline("pipeline-b", "kinesis", &["filter"], "s3")).unwrap();

        let optimizer = Optimizer::new(&registry, "default");
        let dag = optimizer.optimize();

        assert_eq!(dag.sources.len(), 2);
        assert!(dag.sources.contains_key("kafka"));
        assert!(dag.sources.contains_key("kinesis"));

        assert_eq!(dag.sinks.len(), 1);
        let s3_sink = &dag.sinks["sink:s3"];
        assert_eq!(s3_sink.source_pipelines.len(), 2);
    }

    #[test]
    fn test_optimize_empty_steps() {
        let mut registry = Registry::new();

        registry.apply(make_source("kafka")).unwrap();
        registry.apply(make_sink("s3")).unwrap();
        registry.apply(make_pipeline("passthrough", "kafka", &[], "s3")).unwrap();

        let optimizer = Optimizer::new(&registry, "default");
        let dag = optimizer.optimize();

        assert_eq!(dag.sources.len(), 1);
        assert_eq!(dag.stages.len(), 0);
        assert_eq!(dag.sinks.len(), 1);

        let source_to_sink: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.from.starts_with("source:") && e.to.starts_with("sink:"))
            .collect();
        assert_eq!(source_to_sink.len(), 1);
    }
}
