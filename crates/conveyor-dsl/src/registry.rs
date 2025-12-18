use std::collections::HashMap;

use crate::error::{DslError, Result};
use crate::manifest::{
    AnyManifest, Metadata, PipelineManifest, PipelineSpec, SinkManifest,
    SinkSpec, SourceManifest, SourceSpec, TransformManifest, TransformSpec,
};

type ResourceKey = (String, String);

#[derive(Debug, Default, Clone)]
pub struct Registry {
    sources: HashMap<ResourceKey, SourceManifest>,
    transforms: HashMap<ResourceKey, TransformManifest>,
    sinks: HashMap<ResourceKey, SinkManifest>,
    pipelines: HashMap<ResourceKey, PipelineManifest>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    fn make_key(metadata: &Metadata) -> ResourceKey {
        (metadata.namespace.clone(), metadata.name.clone())
    }

    pub fn apply(&mut self, manifest: AnyManifest) -> Result<()> {
        match manifest {
            AnyManifest::Source(m) => {
                let key = Self::make_key(&m.metadata);
                self.sources.insert(key, m);
            }
            AnyManifest::Transform(m) => {
                let key = Self::make_key(&m.metadata);
                self.transforms.insert(key, m);
            }
            AnyManifest::Sink(m) => {
                let key = Self::make_key(&m.metadata);
                self.sinks.insert(key, m);
            }
            AnyManifest::Pipeline(m) => {
                let key = Self::make_key(&m.metadata);
                self.pipelines.insert(key, m);
            }
        }
        Ok(())
    }

    pub fn get_source(&self, namespace: &str, name: &str) -> Option<&SourceManifest> {
        self.sources.get(&(namespace.to_string(), name.to_string()))
    }

    pub fn get_transform(&self, namespace: &str, name: &str) -> Option<&TransformManifest> {
        self.transforms.get(&(namespace.to_string(), name.to_string()))
    }

    pub fn get_sink(&self, namespace: &str, name: &str) -> Option<&SinkManifest> {
        self.sinks.get(&(namespace.to_string(), name.to_string()))
    }

    pub fn get_pipeline(&self, namespace: &str, name: &str) -> Option<&PipelineManifest> {
        self.pipelines.get(&(namespace.to_string(), name.to_string()))
    }

    pub fn delete_source(&mut self, namespace: &str, name: &str) -> bool {
        self.sources.remove(&(namespace.to_string(), name.to_string())).is_some()
    }

    pub fn delete_transform(&mut self, namespace: &str, name: &str) -> bool {
        self.transforms.remove(&(namespace.to_string(), name.to_string())).is_some()
    }

    pub fn delete_sink(&mut self, namespace: &str, name: &str) -> bool {
        self.sinks.remove(&(namespace.to_string(), name.to_string())).is_some()
    }

    pub fn delete_pipeline(&mut self, namespace: &str, name: &str) -> bool {
        self.pipelines.remove(&(namespace.to_string(), name.to_string())).is_some()
    }

    pub fn list_sources(&self, namespace: Option<&str>) -> Vec<&SourceManifest> {
        self.sources
            .iter()
            .filter(|((ns, _), _)| namespace.is_none_or(|n| n == ns))
            .map(|(_, m)| m)
            .collect()
    }

    pub fn list_transforms(&self, namespace: Option<&str>) -> Vec<&TransformManifest> {
        self.transforms
            .iter()
            .filter(|((ns, _), _)| namespace.is_none_or(|n| n == ns))
            .map(|(_, m)| m)
            .collect()
    }

    pub fn list_sinks(&self, namespace: Option<&str>) -> Vec<&SinkManifest> {
        self.sinks
            .iter()
            .filter(|((ns, _), _)| namespace.is_none_or(|n| n == ns))
            .map(|(_, m)| m)
            .collect()
    }

    pub fn list_pipelines(&self, namespace: Option<&str>) -> Vec<&PipelineManifest> {
        self.pipelines
            .iter()
            .filter(|((ns, _), _)| namespace.is_none_or(|n| n == ns))
            .map(|(_, m)| m)
            .collect()
    }

    pub fn validate_pipeline(&self, pipeline: &PipelineManifest) -> Result<()> {
        let ns = &pipeline.metadata.namespace;
        let pipeline_name = &pipeline.metadata.name;

        if self.get_source(ns, &pipeline.spec.source).is_none() {
            return Err(DslError::ValidationError(format!(
                "Pipeline '{}': source '{}' not found in namespace '{}'",
                pipeline_name, pipeline.spec.source, ns
            )));
        }

        for step in &pipeline.spec.steps {
            if self.get_transform(ns, step).is_none() {
                return Err(DslError::ValidationError(format!(
                    "Pipeline '{}': transform '{}' not found in namespace '{}'",
                    pipeline_name, step, ns
                )));
            }
        }

        if self.get_sink(ns, &pipeline.spec.sink).is_none() {
            return Err(DslError::ValidationError(format!(
                "Pipeline '{}': sink '{}' not found in namespace '{}'",
                pipeline_name, pipeline.spec.sink, ns
            )));
        }

        if let Some(dlq) = &pipeline.spec.dlq {
            if self.get_sink(ns, &dlq.sink).is_none() {
                return Err(DslError::ValidationError(format!(
                    "Pipeline '{}': DLQ sink '{}' not found in namespace '{}'",
                    pipeline_name, dlq.sink, ns
                )));
            }
        }

        Ok(())
    }

    pub fn validate_all_pipelines(&self) -> Result<()> {
        for pipeline in self.pipelines.values() {
            self.validate_pipeline(pipeline)?;
        }
        Ok(())
    }

    pub fn source_spec(&self, namespace: &str, name: &str) -> Option<&SourceSpec> {
        self.get_source(namespace, name).map(|m| &m.spec)
    }

    pub fn transform_spec(&self, namespace: &str, name: &str) -> Option<&TransformSpec> {
        self.get_transform(namespace, name).map(|m| &m.spec)
    }

    pub fn sink_spec(&self, namespace: &str, name: &str) -> Option<&SinkSpec> {
        self.get_sink(namespace, name).map(|m| &m.spec)
    }

    pub fn pipeline_spec(&self, namespace: &str, name: &str) -> Option<&PipelineSpec> {
        self.get_pipeline(namespace, name).map(|m| &m.spec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{GrpcEndpoint, Manifest, Metadata, ResourceKind};

    fn make_source(name: &str, endpoint: &str) -> SourceManifest {
        Manifest {
            api_version: "etl.router/v1".to_string(),
            kind: ResourceKind::Source,
            metadata: Metadata::new(name),
            spec: SourceSpec {
                grpc: GrpcEndpoint {
                    endpoint: endpoint.to_string(),
                    proto: None,
                    tls: None,
                },
                config: serde_json::Value::Null,
            },
        }
    }

    fn make_transform(name: &str, endpoint: &str) -> TransformManifest {
        Manifest {
            api_version: "etl.router/v1".to_string(),
            kind: ResourceKind::Transform,
            metadata: Metadata::new(name),
            spec: TransformSpec {
                grpc: GrpcEndpoint {
                    endpoint: endpoint.to_string(),
                    proto: None,
                    tls: None,
                },
                config: serde_json::Value::Null,
            },
        }
    }

    fn make_sink(name: &str, endpoint: &str) -> SinkManifest {
        Manifest {
            api_version: "etl.router/v1".to_string(),
            kind: ResourceKind::Sink,
            metadata: Metadata::new(name),
            spec: SinkSpec {
                grpc: GrpcEndpoint {
                    endpoint: endpoint.to_string(),
                    proto: None,
                    tls: None,
                },
                config: serde_json::Value::Null,
            },
        }
    }

    fn make_pipeline(name: &str, source: &str, steps: &[&str], sink: &str) -> PipelineManifest {
        Manifest {
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
        }
    }

    #[test]
    fn test_apply_and_get_source() {
        let mut registry = Registry::new();
        let source = make_source("kafka-src", "kafka:50051");

        registry.apply(AnyManifest::Source(source.clone())).unwrap();

        let retrieved = registry.get_source("default", "kafka-src").unwrap();
        assert_eq!(retrieved.metadata.name, "kafka-src");
        assert_eq!(retrieved.spec.grpc.endpoint, "kafka:50051");
    }

    #[test]
    fn test_list_resources() {
        let mut registry = Registry::new();

        registry.apply(AnyManifest::Source(make_source("src1", "ep1"))).unwrap();
        registry.apply(AnyManifest::Source(make_source("src2", "ep2"))).unwrap();
        registry.apply(AnyManifest::Transform(make_transform("tf1", "ep3"))).unwrap();

        assert_eq!(registry.list_sources(None).len(), 2);
        assert_eq!(registry.list_transforms(None).len(), 1);
        assert_eq!(registry.list_sinks(None).len(), 0);
    }

    #[test]
    fn test_delete_resource() {
        let mut registry = Registry::new();
        registry.apply(AnyManifest::Source(make_source("src1", "ep1"))).unwrap();

        assert!(registry.get_source("default", "src1").is_some());
        assert!(registry.delete_source("default", "src1"));
        assert!(registry.get_source("default", "src1").is_none());
        assert!(!registry.delete_source("default", "src1"));
    }

    #[test]
    fn test_validate_pipeline_success() {
        let mut registry = Registry::new();

        registry.apply(AnyManifest::Source(make_source("kafka-src", "ep1"))).unwrap();
        registry.apply(AnyManifest::Transform(make_transform("filter", "ep2"))).unwrap();
        registry.apply(AnyManifest::Transform(make_transform("mask", "ep3"))).unwrap();
        registry.apply(AnyManifest::Sink(make_sink("s3-sink", "ep4"))).unwrap();

        let pipeline = make_pipeline("my-pipeline", "kafka-src", &["filter", "mask"], "s3-sink");
        registry.apply(AnyManifest::Pipeline(pipeline)).unwrap();

        assert!(registry.validate_all_pipelines().is_ok());
    }

    #[test]
    fn test_validate_pipeline_missing_source() {
        let mut registry = Registry::new();

        registry.apply(AnyManifest::Transform(make_transform("filter", "ep2"))).unwrap();
        registry.apply(AnyManifest::Sink(make_sink("s3-sink", "ep4"))).unwrap();

        let pipeline = make_pipeline("my-pipeline", "missing-src", &["filter"], "s3-sink");
        registry.apply(AnyManifest::Pipeline(pipeline)).unwrap();

        let result = registry.validate_all_pipelines();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("source 'missing-src' not found"));
    }

    #[test]
    fn test_validate_pipeline_missing_transform() {
        let mut registry = Registry::new();

        registry.apply(AnyManifest::Source(make_source("kafka-src", "ep1"))).unwrap();
        registry.apply(AnyManifest::Sink(make_sink("s3-sink", "ep4"))).unwrap();

        let pipeline = make_pipeline("my-pipeline", "kafka-src", &["missing-tf"], "s3-sink");
        registry.apply(AnyManifest::Pipeline(pipeline)).unwrap();

        let result = registry.validate_all_pipelines();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("transform 'missing-tf' not found"));
    }

    #[test]
    fn test_validate_pipeline_missing_sink() {
        let mut registry = Registry::new();

        registry.apply(AnyManifest::Source(make_source("kafka-src", "ep1"))).unwrap();

        let pipeline = make_pipeline("my-pipeline", "kafka-src", &[], "missing-sink");
        registry.apply(AnyManifest::Pipeline(pipeline)).unwrap();

        let result = registry.validate_all_pipelines();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("sink 'missing-sink' not found"));
    }

    #[test]
    fn test_namespace_isolation() {
        let mut registry = Registry::new();

        let mut src_prod = make_source("kafka-src", "kafka-prod:50051");
        src_prod.metadata.namespace = "production".to_string();

        let src_dev = make_source("kafka-src", "kafka-dev:50051");

        registry.apply(AnyManifest::Source(src_prod)).unwrap();
        registry.apply(AnyManifest::Source(src_dev)).unwrap();

        let prod = registry.get_source("production", "kafka-src").unwrap();
        let dev = registry.get_source("default", "kafka-src").unwrap();

        assert_eq!(prod.spec.grpc.endpoint, "kafka-prod:50051");
        assert_eq!(dev.spec.grpc.endpoint, "kafka-dev:50051");

        assert_eq!(registry.list_sources(Some("production")).len(), 1);
        assert_eq!(registry.list_sources(Some("default")).len(), 1);
        assert_eq!(registry.list_sources(None).len(), 2);
    }
}
