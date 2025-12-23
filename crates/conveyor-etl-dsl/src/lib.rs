mod types;
mod error;
mod parser;
mod validation;
mod convert;
pub mod manifest;
pub mod registry;
pub mod optimizer;

pub use types::*;
pub use error::{DslError, Result};
pub use parser::{parse_yaml, parse_file};
pub use validation::{validate, validate_backup, validate_restore};
pub use convert::convert;

pub use manifest::{
    AnyManifest, DlqConfig, GrpcEndpoint, Manifest, Metadata, PipelineManifest,
    PipelineSpec, ResourceKind, SinkManifest, SinkSpec, SourceManifest, SourceSpec,
    TlsConfig, TransformManifest, TransformSpec,
};
pub use registry::Registry;
pub use optimizer::{DagEdge, OptimizedDag, Optimizer, SinkNode, SourceNode, StageNode};

use std::path::Path;
use conveyor_etl_routing::Pipeline;

pub fn load_pipeline<P: AsRef<Path>>(path: P) -> Result<Pipeline> {
    let manifest = parse_file(path)?;
    validate(&manifest)?;
    convert(&manifest)
}

pub fn parse_pipeline(yaml: &str) -> Result<Pipeline> {
    let manifest = parse_yaml(yaml)?;
    validate(&manifest)?;
    convert(&manifest)
}
