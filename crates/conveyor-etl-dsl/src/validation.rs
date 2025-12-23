use std::collections::HashSet;

use crate::error::{DslError, Result};
use crate::types::{
    BackupDestination, BackupManifest, FanInSourceDsl, FanOutSinkDsl, FieldMappingDsl,
    PipelineManifest, PipelineSpec, RestoreManifest, StageDsl, StageConfigDsl, StageTypeDsl,
};

pub fn validate(manifest: &PipelineManifest) -> Result<()> {
    validate_metadata(manifest)?;
    validate_spec(&manifest.metadata.name, &manifest.spec)?;
    Ok(())
}

pub fn validate_backup(manifest: &BackupManifest) -> Result<()> {
    if manifest.metadata.name.is_empty() {
        return Err(DslError::ValidationError(
            "Backup name cannot be empty".to_string(),
        ));
    }

    if manifest.spec.include.is_empty() {
        return Err(DslError::ValidationError(
            "Backup must include at least one component (checkpoints, offsets, configuration, or state)".to_string(),
        ));
    }

    if manifest.spec.pipeline.name.is_empty() {
        return Err(DslError::ValidationError(
            "Backup must specify a pipeline name".to_string(),
        ));
    }

    if let Some(schedule) = &manifest.spec.schedule {
        validate_cron_expression(&schedule.cron)?;

        if let Some(retention) = &schedule.retention {
            if retention.count.is_none() && retention.days.is_none() {
                return Err(DslError::ValidationError(
                    "Retention must specify either count or days".to_string(),
                ));
            }
        }
    }

    validate_backup_destination(&manifest.spec.destination)?;

    Ok(())
}

pub fn validate_restore(manifest: &RestoreManifest) -> Result<()> {
    if manifest.metadata.name.is_empty() {
        return Err(DslError::ValidationError(
            "Restore name cannot be empty".to_string(),
        ));
    }

    let source = &manifest.spec.from;
    let has_backup = source.backup.is_some();
    let has_snapshot = source.snapshot.is_some();
    let has_latest = source.latest == Some(true);

    if !has_backup && !has_snapshot && !has_latest {
        return Err(DslError::ValidationError(
            "Restore must specify one of: backup, snapshot, or latest".to_string(),
        ));
    }

    let source_count = [has_backup, has_snapshot, has_latest]
        .iter()
        .filter(|&&x| x)
        .count();

    if source_count > 1 {
        return Err(DslError::ValidationError(
            "Restore must specify only one of: backup, snapshot, or latest".to_string(),
        ));
    }

    if let Some(target) = &manifest.spec.target {
        if target.name.is_empty() {
            return Err(DslError::ValidationError(
                "Target pipeline name cannot be empty".to_string(),
            ));
        }
    }

    Ok(())
}

fn validate_backup_destination(destination: &BackupDestination) -> Result<()> {
    match destination {
        BackupDestination::S3(s3) => {
            if s3.bucket.is_empty() {
                return Err(DslError::ValidationError(
                    "S3 destination must specify a bucket".to_string(),
                ));
            }
        }
        BackupDestination::Gcs(gcs) => {
            if gcs.bucket.is_empty() {
                return Err(DslError::ValidationError(
                    "GCS destination must specify a bucket".to_string(),
                ));
            }
        }
        BackupDestination::File(file) => {
            if file.path.is_empty() {
                return Err(DslError::ValidationError(
                    "File destination must specify a path".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_cron_expression(cron: &str) -> Result<()> {
    if cron.is_empty() {
        return Err(DslError::ValidationError(
            "Cron expression cannot be empty".to_string(),
        ));
    }

    let parts: Vec<&str> = cron.split_whitespace().collect();
    if parts.len() != 5 {
        return Err(DslError::ValidationError(format!(
            "Invalid cron expression '{}': expected 5 fields (minute hour day month weekday)",
            cron
        )));
    }

    Ok(())
}

fn validate_metadata(manifest: &PipelineManifest) -> Result<()> {
    if manifest.metadata.name.is_empty() {
        return Err(DslError::ValidationError("Pipeline name cannot be empty".to_string()));
    }
    Ok(())
}

fn validate_spec(name: &str, spec: &PipelineSpec) -> Result<()> {
    if spec.stages.is_empty() {
        return Err(DslError::InvalidPipeline {
            pipeline_id: name.to_string(),
            message: "Pipeline must have at least one stage".to_string(),
        });
    }

    let stage_ids: HashSet<_> = spec.stages.iter().map(|s| s.id.as_str()).collect();
    if stage_ids.len() != spec.stages.len() {
        return Err(DslError::InvalidPipeline {
            pipeline_id: name.to_string(),
            message: "Duplicate stage IDs found".to_string(),
        });
    }

    validate_stage_types(name, spec)?;

    for stage in &spec.stages {
        validate_stage(name, stage, &stage_ids)?;
    }

    validate_linear_order(name, spec)?;

    Ok(())
}

fn validate_stage(name: &str, stage: &StageDsl, all_stage_ids: &HashSet<&str>) -> Result<()> {
    if stage.id.is_empty() {
        return Err(DslError::InvalidStage {
            pipeline_id: name.to_string(),
            stage_id: stage.id.clone(),
            message: "Stage ID cannot be empty".to_string(),
        });
    }

    if stage.parallelism == 0 {
        return Err(DslError::InvalidStage {
            pipeline_id: name.to_string(),
            stage_id: stage.id.clone(),
            message: "Parallelism must be at least 1".to_string(),
        });
    }

    validate_stage_config(name, stage)?;
    validate_fan_in(name, stage, all_stage_ids)?;
    validate_fan_out(name, stage, all_stage_ids)?;

    Ok(())
}

fn validate_stage_config(name: &str, stage: &StageDsl) -> Result<()> {
    if let Some(config) = &stage.config {
        let config_matches = match (&stage.stage_type, config) {
            (StageTypeDsl::Source, StageConfigDsl::Source(_)) => true,
            (StageTypeDsl::Transform, StageConfigDsl::Transform(_)) => true,
            (StageTypeDsl::Lookup, StageConfigDsl::Lookup(_)) => true,
            (StageTypeDsl::Sink, StageConfigDsl::Sink(_)) => true,
            _ => false,
        };

        if !config_matches {
            return Err(DslError::InvalidStage {
                pipeline_id: name.to_string(),
                stage_id: stage.id.clone(),
                message: format!(
                    "Config type does not match stage type '{:?}'",
                    stage.stage_type
                ),
            });
        }
    }

    Ok(())
}

fn validate_stage_types(name: &str, spec: &PipelineSpec) -> Result<()> {
    let has_source = spec.stages.iter().any(|s| s.stage_type == StageTypeDsl::Source);
    let has_sink = spec.stages.iter().any(|s| s.stage_type == StageTypeDsl::Sink);
    let has_fan_in = spec.stages.iter().any(|s| s.stage_type == StageTypeDsl::FanIn);
    let has_fan_out = spec.stages.iter().any(|s| s.stage_type == StageTypeDsl::FanOut);

    if !has_source && !has_fan_in {
        return Err(DslError::InvalidPipeline {
            pipeline_id: name.to_string(),
            message: "Pipeline must have at least one source or fan-in stage".to_string(),
        });
    }

    if !has_sink && !has_fan_out {
        return Err(DslError::InvalidPipeline {
            pipeline_id: name.to_string(),
            message: "Pipeline must have at least one sink or fan-out stage".to_string(),
        });
    }

    Ok(())
}

fn validate_linear_order(name: &str, spec: &PipelineSpec) -> Result<()> {
    if let Some(first) = spec.stages.first() {
        if first.stage_type != StageTypeDsl::Source && first.stage_type != StageTypeDsl::FanIn {
            return Err(DslError::InvalidPipeline {
                pipeline_id: name.to_string(),
                message: "Pipeline must start with a source or fan-in stage".to_string(),
            });
        }
    }

    if let Some(last) = spec.stages.last() {
        if last.stage_type != StageTypeDsl::Sink && last.stage_type != StageTypeDsl::FanOut {
            return Err(DslError::InvalidPipeline {
                pipeline_id: name.to_string(),
                message: "Pipeline must end with a sink or fan-out stage".to_string(),
            });
        }
    }

    Ok(())
}

fn validate_fan_in(name: &str, stage: &StageDsl, all_stage_ids: &HashSet<&str>) -> Result<()> {
    if stage.sources.is_some() && stage.stage_type != StageTypeDsl::FanIn {
        return Err(DslError::InvalidStage {
            pipeline_id: name.to_string(),
            stage_id: stage.id.clone(),
            message: "Only fan-in stages can have 'sources' field".to_string(),
        });
    }

    if stage.stage_type == StageTypeDsl::FanIn {
        let sources = stage.sources.as_ref().ok_or_else(|| DslError::InvalidStage {
            pipeline_id: name.to_string(),
            stage_id: stage.id.clone(),
            message: "Fan-in stage must have 'sources' field".to_string(),
        })?;

        if sources.len() < 2 {
            return Err(DslError::InvalidStage {
                pipeline_id: name.to_string(),
                stage_id: stage.id.clone(),
                message: "Fan-in stage must have at least 2 sources".to_string(),
            });
        }

        let mut source_ids: HashSet<&str> = HashSet::new();
        for source in sources {
            if source.id.is_empty() {
                return Err(DslError::InvalidStage {
                    pipeline_id: name.to_string(),
                    stage_id: stage.id.clone(),
                    message: "Source ID cannot be empty".to_string(),
                });
            }

            if !source_ids.insert(&source.id) {
                return Err(DslError::InvalidStage {
                    pipeline_id: name.to_string(),
                    stage_id: stage.id.clone(),
                    message: format!("Duplicate source ID: {}", source.id),
                });
            }

            let full_id = format!("{}:{}", stage.id, source.id);
            if all_stage_ids.contains(full_id.as_str()) {
                return Err(DslError::InvalidStage {
                    pipeline_id: name.to_string(),
                    stage_id: stage.id.clone(),
                    message: format!("Source ID conflicts with stage ID: {}", full_id),
                });
            }

            validate_fan_in_source(name, &stage.id, source)?;
        }
    }

    Ok(())
}

fn validate_fan_in_source(name: &str, stage_id: &str, source: &FanInSourceDsl) -> Result<()> {
    if let Some(mappings) = &source.mapping {
        for mapping in mappings {
            validate_field_mapping(name, stage_id, &source.id, mapping)?;
        }
    }
    Ok(())
}

fn validate_fan_out(name: &str, stage: &StageDsl, all_stage_ids: &HashSet<&str>) -> Result<()> {
    if stage.sinks.is_some() && stage.stage_type != StageTypeDsl::FanOut {
        return Err(DslError::InvalidStage {
            pipeline_id: name.to_string(),
            stage_id: stage.id.clone(),
            message: "Only fan-out stages can have 'sinks' field".to_string(),
        });
    }

    if stage.stage_type == StageTypeDsl::FanOut {
        let sinks = stage.sinks.as_ref().ok_or_else(|| DslError::InvalidStage {
            pipeline_id: name.to_string(),
            stage_id: stage.id.clone(),
            message: "Fan-out stage must have 'sinks' field".to_string(),
        })?;

        if sinks.len() < 2 {
            return Err(DslError::InvalidStage {
                pipeline_id: name.to_string(),
                stage_id: stage.id.clone(),
                message: "Fan-out stage must have at least 2 sinks".to_string(),
            });
        }

        let mut sink_ids: HashSet<&str> = HashSet::new();
        for sink in sinks {
            if sink.id.is_empty() {
                return Err(DslError::InvalidStage {
                    pipeline_id: name.to_string(),
                    stage_id: stage.id.clone(),
                    message: "Sink ID cannot be empty".to_string(),
                });
            }

            if !sink_ids.insert(&sink.id) {
                return Err(DslError::InvalidStage {
                    pipeline_id: name.to_string(),
                    stage_id: stage.id.clone(),
                    message: format!("Duplicate sink ID: {}", sink.id),
                });
            }

            let full_id = format!("{}:{}", stage.id, sink.id);
            if all_stage_ids.contains(full_id.as_str()) {
                return Err(DslError::InvalidStage {
                    pipeline_id: name.to_string(),
                    stage_id: stage.id.clone(),
                    message: format!("Sink ID conflicts with stage ID: {}", full_id),
                });
            }

            validate_fan_out_sink(name, &stage.id, sink)?;
        }
    }

    Ok(())
}

fn validate_fan_out_sink(name: &str, stage_id: &str, sink: &FanOutSinkDsl) -> Result<()> {
    if let Some(mappings) = &sink.mapping {
        for mapping in mappings {
            validate_field_mapping(name, stage_id, &sink.id, mapping)?;
        }
    }
    Ok(())
}

fn validate_field_mapping(
    name: &str,
    stage_id: &str,
    sub_id: &str,
    mapping: &FieldMappingDsl,
) -> Result<()> {
    if mapping.target.is_empty() {
        return Err(DslError::InvalidStage {
            pipeline_id: name.to_string(),
            stage_id: stage_id.to_string(),
            message: format!("Field mapping in {} must have a 'target' field", sub_id),
        });
    }

    let source_count = [
        mapping.source.is_some(),
        mapping.literal.is_some(),
        mapping.default.is_some() && mapping.source.is_none(),
    ]
    .iter()
    .filter(|&&x| x)
    .count();

    if source_count > 1 {
        return Err(DslError::InvalidStage {
            pipeline_id: name.to_string(),
            stage_id: stage_id.to_string(),
            message: format!(
                "Field mapping for '{}' in {} can only have one of: source, literal",
                mapping.target, sub_id
            ),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_yaml;

    #[test]
    fn test_valid_pipeline() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: valid
spec:
  stages:
    - id: source
      name: Source
      type: source
      service:
        name: src
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
"#;

        let manifest = parse_yaml(yaml).unwrap();
        assert!(validate(&manifest).is_ok());
    }

    #[test]
    fn test_missing_source() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: no-source
spec:
  stages:
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
"#;

        let manifest = parse_yaml(yaml).unwrap();
        let result = validate(&manifest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("source"));
    }

    #[test]
    fn test_missing_sink() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: no-sink
spec:
  stages:
    - id: source
      name: Source
      type: source
      service:
        name: src
"#;

        let manifest = parse_yaml(yaml).unwrap();
        let result = validate(&manifest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("sink"));
    }

    #[test]
    fn test_source_not_first() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: wrong-order
spec:
  stages:
    - id: transform
      name: Transform
      type: transform
      service:
        name: trans
    - id: source
      name: Source
      type: source
      service:
        name: src
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
"#;

        let manifest = parse_yaml(yaml).unwrap();
        let result = validate(&manifest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("start with a source"));
    }

    #[test]
    fn test_sink_not_last() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: wrong-order
spec:
  stages:
    - id: source
      name: Source
      type: source
      service:
        name: src
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
    - id: transform
      name: Transform
      type: transform
      service:
        name: trans
"#;

        let manifest = parse_yaml(yaml).unwrap();
        let result = validate(&manifest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("end with a sink"));
    }

    #[test]
    fn test_valid_fan_in_pipeline() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: fan-in-test
spec:
  stages:
    - id: unified
      name: Unified Source
      type: fan-in
      sources:
        - id: source1
          name: Source 1
          service:
            name: kafka
        - id: source2
          name: Source 2
          service:
            name: kafka
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
"#;

        let manifest = parse_yaml(yaml).unwrap();
        assert!(validate(&manifest).is_ok());
    }

    #[test]
    fn test_fan_in_requires_sources() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: fan-in-no-sources
spec:
  stages:
    - id: unified
      name: Unified Source
      type: fan-in
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
"#;

        let manifest = parse_yaml(yaml).unwrap();
        let result = validate(&manifest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must have 'sources'"));
    }

    #[test]
    fn test_fan_in_requires_at_least_two_sources() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: fan-in-one-source
spec:
  stages:
    - id: unified
      name: Unified Source
      type: fan-in
      sources:
        - id: source1
          name: Source 1
          service:
            name: kafka
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
"#;

        let manifest = parse_yaml(yaml).unwrap();
        let result = validate(&manifest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("at least 2 sources"));
    }

    #[test]
    fn test_valid_fan_out_pipeline() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: fan-out-test
spec:
  stages:
    - id: source
      name: Source
      type: source
      service:
        name: kafka
    - id: distribute
      name: Distribute
      type: fan-out
      sinks:
        - id: sink1
          name: Sink 1
          service:
            name: postgres
        - id: sink2
          name: Sink 2
          service:
            name: s3
"#;

        let manifest = parse_yaml(yaml).unwrap();
        assert!(validate(&manifest).is_ok());
    }

    #[test]
    fn test_fan_out_requires_sinks() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: fan-out-no-sinks
spec:
  stages:
    - id: source
      name: Source
      type: source
      service:
        name: kafka
    - id: distribute
      name: Distribute
      type: fan-out
"#;

        let manifest = parse_yaml(yaml).unwrap();
        let result = validate(&manifest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must have 'sinks'"));
    }

    #[test]
    fn test_sources_only_on_fan_in() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: sources-on-source
spec:
  stages:
    - id: src
      name: Source
      type: source
      service:
        name: kafka
      sources:
        - id: s1
          name: S1
          service:
            name: kafka
        - id: s2
          name: S2
          service:
            name: kafka
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
"#;

        let manifest = parse_yaml(yaml).unwrap();
        let result = validate(&manifest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Only fan-in"));
    }
}
