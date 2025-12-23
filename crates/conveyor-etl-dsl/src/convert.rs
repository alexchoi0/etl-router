use std::time::Duration;

use conveyor_etl_routing::{
    FanInConfig, FanInSource, FanInWatermark, FanOutConfig, FanOutSink, FieldCastType, FieldMapping,
    LoadBalanceStrategy, LookupConfig, LookupKeyMapping, LookupMissStrategy, MergeStrategy,
    Pipeline, ServiceSelector, SourceWatermark, Stage, StageType,
};

use crate::error::{DslError, Result};
use crate::types::{
    FanInSourceDsl, FanOutSinkDsl, FieldMappingDsl, FieldType, LoadBalanceDsl, LookupConfigDsl,
    LookupOnMissDsl, MergeStrategyDsl, PipelineManifest, PipelineSpec, ServiceSelectorDsl,
    StageDsl, StageConfigDsl, StageTypeDsl,
};

pub fn convert(manifest: &PipelineManifest) -> Result<Pipeline> {
    convert_pipeline(&manifest.metadata.name, &manifest.spec, &manifest.metadata.labels)
}

fn convert_pipeline(
    name: &str,
    spec: &PipelineSpec,
    labels: &std::collections::HashMap<String, String>,
) -> Result<Pipeline> {
    let mut pipeline = Pipeline::new(name.to_string(), name.to_string());
    pipeline.description = spec.description.clone();
    pipeline.enabled = spec.enabled;
    pipeline.metadata = labels.clone();

    for stage_dsl in &spec.stages {
        let stage = convert_stage(stage_dsl)?;
        pipeline.add_stage(stage);

        if stage_dsl.stage_type == StageTypeDsl::FanIn {
            add_fan_in_internal_stages(&mut pipeline, stage_dsl)?;
        } else if stage_dsl.stage_type == StageTypeDsl::FanOut {
            add_fan_out_internal_stages(&mut pipeline, stage_dsl)?;
        }
    }

    // Linear pipeline: each stage flows to the next one in order
    for i in 0..spec.stages.len().saturating_sub(1) {
        let from_id = &spec.stages[i].id;
        let to_id = &spec.stages[i + 1].id;
        pipeline.add_edge(from_id, to_id, None);
    }

    Ok(pipeline)
}

fn add_fan_in_internal_stages(pipeline: &mut Pipeline, stage_dsl: &StageDsl) -> Result<()> {
    if let Some(sources) = &stage_dsl.sources {
        for source in sources {
            let internal_stage = Stage {
                id: format!("{}:{}", stage_dsl.id, source.id),
                name: source.name.clone(),
                stage_type: StageType::Source,
                service_selector: convert_service_selector(&source.service),
                parallelism: 1,
                lookup_config: None,
                fan_in_config: None,
                fan_out_config: None,
            };
            pipeline.add_stage(internal_stage);
            pipeline.add_edge(&format!("{}:{}", stage_dsl.id, source.id), &stage_dsl.id, None);
        }
    }
    Ok(())
}

fn add_fan_out_internal_stages(pipeline: &mut Pipeline, stage_dsl: &StageDsl) -> Result<()> {
    if let Some(sinks) = &stage_dsl.sinks {
        for sink in sinks {
            let internal_stage = Stage {
                id: format!("{}:{}", stage_dsl.id, sink.id),
                name: sink.name.clone(),
                stage_type: StageType::Sink,
                service_selector: convert_service_selector(&sink.service),
                parallelism: 1,
                lookup_config: None,
                fan_in_config: None,
                fan_out_config: None,
            };
            pipeline.add_stage(internal_stage);
            pipeline.add_edge(&stage_dsl.id, &format!("{}:{}", stage_dsl.id, sink.id), None);
        }
    }
    Ok(())
}

fn convert_stage(dsl: &StageDsl) -> Result<Stage> {
    let lookup_config = extract_lookup_config(&dsl.config, dsl.stage_type)?;
    let fan_in_config = convert_fan_in_config(dsl)?;
    let fan_out_config = convert_fan_out_config(dsl)?;

    Ok(Stage {
        id: dsl.id.clone(),
        name: dsl.name.clone(),
        stage_type: convert_stage_type(dsl.stage_type),
        service_selector: convert_service_selector(&dsl.service),
        parallelism: dsl.parallelism,
        lookup_config,
        fan_in_config,
        fan_out_config,
    })
}

fn convert_fan_in_config(dsl: &StageDsl) -> Result<Option<FanInConfig>> {
    if dsl.stage_type != StageTypeDsl::FanIn {
        return Ok(None);
    }

    let sources = dsl.sources.as_ref().ok_or_else(|| {
        DslError::InvalidConfig("Fan-in stage must have sources".to_string())
    })?;

    let converted_sources: Vec<FanInSource> = sources
        .iter()
        .map(|s| convert_fan_in_source(s))
        .collect::<Result<Vec<_>>>()?;

    let watermark = dsl.watermark.as_ref().map(|w| FanInWatermark {
        allowed_lateness: w.allowed_lateness.as_ref().and_then(|s| parse_duration(s)),
    });

    Ok(Some(FanInConfig {
        sources: converted_sources,
        watermark,
    }))
}

fn convert_fan_in_source(dsl: &FanInSourceDsl) -> Result<FanInSource> {
    let watermark = dsl.watermark.as_ref().map(|w| SourceWatermark {
        event_time_field: w.event_time_field.clone(),
        idle_timeout: w.idle_timeout.as_ref().and_then(|s| parse_duration(s)),
    });

    let field_mappings = dsl
        .mapping
        .as_ref()
        .map(|m| m.iter().map(convert_field_mapping).collect())
        .unwrap_or_default();

    Ok(FanInSource {
        id: dsl.id.clone(),
        name: dsl.name.clone(),
        service_selector: convert_service_selector(&dsl.service),
        watermark,
        field_mappings,
    })
}

fn convert_fan_out_config(dsl: &StageDsl) -> Result<Option<FanOutConfig>> {
    if dsl.stage_type != StageTypeDsl::FanOut {
        return Ok(None);
    }

    let sinks = dsl.sinks.as_ref().ok_or_else(|| {
        DslError::InvalidConfig("Fan-out stage must have sinks".to_string())
    })?;

    let converted_sinks: Vec<FanOutSink> = sinks
        .iter()
        .map(|s| convert_fan_out_sink(s))
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(FanOutConfig {
        sinks: converted_sinks,
    }))
}

fn convert_fan_out_sink(dsl: &FanOutSinkDsl) -> Result<FanOutSink> {
    let field_mappings = dsl
        .mapping
        .as_ref()
        .map(|m| m.iter().map(convert_field_mapping).collect())
        .unwrap_or_default();

    Ok(FanOutSink {
        id: dsl.id.clone(),
        name: dsl.name.clone(),
        service_selector: convert_service_selector(&dsl.service),
        field_mappings,
    })
}

fn convert_field_mapping(dsl: &FieldMappingDsl) -> FieldMapping {
    FieldMapping {
        source_field: dsl.source.clone(),
        target_field: dsl.target.clone(),
        cast_type: dsl.cast.map(convert_field_cast_type),
        default_value: dsl.default.as_ref().map(|v| serde_json::to_vec(v).unwrap_or_default()),
        literal_value: dsl.literal.as_ref().map(|v| serde_json::to_vec(v).unwrap_or_default()),
    }
}

fn convert_field_cast_type(dsl: FieldType) -> FieldCastType {
    match dsl {
        FieldType::String => FieldCastType::String,
        FieldType::Int => FieldCastType::Int,
        FieldType::Int64 => FieldCastType::Int64,
        FieldType::Float => FieldCastType::Float,
        FieldType::Float64 => FieldCastType::Float64,
        FieldType::Bool => FieldCastType::Bool,
        FieldType::Timestamp => FieldCastType::Timestamp,
        FieldType::Date => FieldCastType::Date,
        FieldType::Json => FieldCastType::Json,
        FieldType::Bytes => FieldCastType::Bytes,
    }
}

fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let (num_str, unit) = if s.ends_with("ms") {
        (&s[..s.len() - 2], "ms")
    } else if s.ends_with('s') {
        (&s[..s.len() - 1], "s")
    } else if s.ends_with('m') {
        (&s[..s.len() - 1], "m")
    } else if s.ends_with('h') {
        (&s[..s.len() - 1], "h")
    } else {
        return None;
    };

    let num: u64 = num_str.parse().ok()?;

    match unit {
        "ms" => Some(Duration::from_millis(num)),
        "s" => Some(Duration::from_secs(num)),
        "m" => Some(Duration::from_secs(num * 60)),
        "h" => Some(Duration::from_secs(num * 3600)),
        _ => None,
    }
}

fn extract_lookup_config(
    config: &Option<StageConfigDsl>,
    stage_type: StageTypeDsl,
) -> Result<Option<LookupConfig>> {
    if stage_type != StageTypeDsl::Lookup {
        return Ok(None);
    }

    match config {
        Some(StageConfigDsl::Lookup(lookup_dsl)) => {
            Ok(Some(convert_lookup_config(lookup_dsl)?))
        }
        Some(_) => Err(DslError::InvalidConfig(
            "Lookup stage requires lookup config".to_string(),
        )),
        None => Err(DslError::InvalidConfig(
            "Lookup stage requires config".to_string(),
        )),
    }
}

fn convert_lookup_config(dsl: &LookupConfigDsl) -> Result<LookupConfig> {
    let key_fields = dsl
        .keys
        .iter()
        .map(|k| LookupKeyMapping {
            record_field: k.record_field.clone(),
            lookup_key: k.lookup_key.clone().unwrap_or_else(|| k.record_field.clone()),
        })
        .collect();

    Ok(LookupConfig {
        key_fields,
        output_prefix: dsl.output_prefix.clone(),
        merge_strategy: convert_merge_strategy(dsl.merge_strategy),
        on_miss: convert_lookup_on_miss(dsl.on_miss),
        timeout_ms: dsl.timeout_ms.unwrap_or(100),
    })
}

fn convert_merge_strategy(dsl: MergeStrategyDsl) -> MergeStrategy {
    match dsl {
        MergeStrategyDsl::Merge => MergeStrategy::Merge,
        MergeStrategyDsl::Nest => MergeStrategy::Nest,
        MergeStrategyDsl::Replace => MergeStrategy::Replace,
    }
}

fn convert_lookup_on_miss(dsl: LookupOnMissDsl) -> LookupMissStrategy {
    match dsl {
        LookupOnMissDsl::PassThrough => LookupMissStrategy::PassThrough,
        LookupOnMissDsl::Drop => LookupMissStrategy::Drop,
        LookupOnMissDsl::Error => LookupMissStrategy::Error,
    }
}

fn convert_stage_type(dsl: StageTypeDsl) -> StageType {
    match dsl {
        StageTypeDsl::Source => StageType::Source,
        StageTypeDsl::Transform => StageType::Transform,
        StageTypeDsl::Lookup => StageType::Lookup,
        StageTypeDsl::FanIn => StageType::FanIn,
        StageTypeDsl::FanOut => StageType::FanOut,
        StageTypeDsl::Sink => StageType::Sink,
    }
}

fn convert_service_selector(dsl: &ServiceSelectorDsl) -> ServiceSelector {
    ServiceSelector {
        service_name: dsl.name.clone(),
        group_id: dsl.group.clone(),
        labels: dsl.labels.clone(),
        load_balance: convert_load_balance(dsl.load_balance),
    }
}

fn convert_load_balance(dsl: LoadBalanceDsl) -> LoadBalanceStrategy {
    match dsl {
        LoadBalanceDsl::RoundRobin => LoadBalanceStrategy::RoundRobin,
        LoadBalanceDsl::LeastConnections => LoadBalanceStrategy::LeastConnections,
        LoadBalanceDsl::WeightedRandom => LoadBalanceStrategy::WeightedRandom,
        LoadBalanceDsl::ConsistentHash => LoadBalanceStrategy::ConsistentHash,
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_yaml;

    #[test]
    fn test_convert_simple_pipeline() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: simple
spec:
  enabled: true
  stages:
    - id: source
      name: Source
      type: source
      service:
        name: kafka
    - id: sink
      name: Sink
      type: sink
      service:
        name: s3
"#;

        let manifest = parse_yaml(yaml).unwrap();
        let pipeline = convert(&manifest).unwrap();

        assert_eq!(pipeline.id, "simple");
        assert_eq!(pipeline.name, "simple");
        assert!(pipeline.enabled);
        assert_eq!(pipeline.stages.len(), 2);
        assert_eq!(pipeline.edges.len(), 1);

        let source = pipeline.stages.get("source").unwrap();
        assert_eq!(source.stage_type, StageType::Source);
        assert_eq!(source.service_selector.service_name, Some("kafka".to_string()));

        let sink = pipeline.stages.get("sink").unwrap();
        assert_eq!(sink.stage_type, StageType::Sink);

        assert_eq!(pipeline.edges[0].from_stage, "source");
        assert_eq!(pipeline.edges[0].to_stage, "sink");
    }

    #[test]
    fn test_convert_load_balance_strategy() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: lb
spec:
  stages:
    - id: source
      name: Source
      type: source
      service:
        name: src
        load_balance: consistent_hash
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
        load_balance: least_connections
"#;

        let manifest = parse_yaml(yaml).unwrap();
        let pipeline = convert(&manifest).unwrap();

        let source = pipeline.stages.get("source").unwrap();
        assert_eq!(source.service_selector.load_balance, LoadBalanceStrategy::ConsistentHash);

        let sink = pipeline.stages.get("sink").unwrap();
        assert_eq!(sink.service_selector.load_balance, LoadBalanceStrategy::LeastConnections);
    }

    #[test]
    fn test_convert_parallelism() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: parallel
spec:
  stages:
    - id: source
      name: Source
      type: source
      service:
        name: src
    - id: transform
      name: Transform
      type: transform
      service:
        group: workers
      parallelism: 8
    - id: sink
      name: Sink
      type: sink
      service:
        name: snk
"#;

        let manifest = parse_yaml(yaml).unwrap();
        let pipeline = convert(&manifest).unwrap();

        assert_eq!(pipeline.edges.len(), 2);

        let transform = pipeline.stages.get("transform").unwrap();
        assert_eq!(transform.parallelism, 8);
        assert_eq!(transform.service_selector.group_id, Some("workers".to_string()));
    }

    #[test]
    fn test_convert_with_labels() {
        let yaml = r#"
apiVersion: etl.dev/v1
kind: Pipeline
metadata:
  name: labeled
  labels:
    env: production
    team: data
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
        let pipeline = convert(&manifest).unwrap();

        assert_eq!(pipeline.metadata.get("env"), Some(&"production".to_string()));
        assert_eq!(pipeline.metadata.get("team"), Some(&"data".to_string()));
    }

    #[test]
    fn test_convert_fan_in_pipeline() {
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
      watermark:
        allowed_lateness: 5m
      sources:
        - id: web
          name: Web Events
          service:
            name: kafka
          watermark:
            event_time_field: timestamp
            idle_timeout: 30s
          mapping:
            - source: web_id
              target: event_id
            - target: source
              literal: "web"
        - id: mobile
          name: Mobile Events
          service:
            name: kafka
          watermark:
            event_time_field: ts
          mapping:
            - source: mobile_id
              target: event_id
            - target: source
              literal: "mobile"
    - id: sink
      name: Sink
      type: sink
      service:
        name: postgres
"#;

        let manifest = parse_yaml(yaml).unwrap();
        let pipeline = convert(&manifest).unwrap();

        assert_eq!(pipeline.id, "fan-in-test");
        assert_eq!(pipeline.stages.len(), 4);

        let fan_in = pipeline.stages.get("unified").unwrap();
        assert_eq!(fan_in.stage_type, StageType::FanIn);
        assert!(fan_in.fan_in_config.is_some());

        let config = fan_in.fan_in_config.as_ref().unwrap();
        assert_eq!(config.sources.len(), 2);
        assert!(config.watermark.is_some());
        assert_eq!(
            config.watermark.as_ref().unwrap().allowed_lateness,
            Some(Duration::from_secs(300))
        );

        let web_source = &config.sources[0];
        assert_eq!(web_source.id, "web");
        assert!(web_source.watermark.is_some());
        assert_eq!(
            web_source.watermark.as_ref().unwrap().event_time_field,
            "timestamp"
        );
        assert_eq!(
            web_source.watermark.as_ref().unwrap().idle_timeout,
            Some(Duration::from_secs(30))
        );
        assert_eq!(web_source.field_mappings.len(), 2);

        let internal_web = pipeline.stages.get("unified:web").unwrap();
        assert_eq!(internal_web.stage_type, StageType::Source);
        assert_eq!(internal_web.service_selector.service_name, Some("kafka".to_string()));

        let internal_mobile = pipeline.stages.get("unified:mobile").unwrap();
        assert_eq!(internal_mobile.stage_type, StageType::Source);
    }

    #[test]
    fn test_convert_fan_out_pipeline() {
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
        - id: archive
          name: S3 Archive
          service:
            name: s3
          mapping:
            - source: event_id
              target: id
        - id: realtime
          name: Real-time
          service:
            name: websocket
"#;

        let manifest = parse_yaml(yaml).unwrap();
        let pipeline = convert(&manifest).unwrap();

        assert_eq!(pipeline.id, "fan-out-test");
        assert_eq!(pipeline.stages.len(), 4);

        let fan_out = pipeline.stages.get("distribute").unwrap();
        assert_eq!(fan_out.stage_type, StageType::FanOut);
        assert!(fan_out.fan_out_config.is_some());

        let config = fan_out.fan_out_config.as_ref().unwrap();
        assert_eq!(config.sinks.len(), 2);

        let archive_sink = &config.sinks[0];
        assert_eq!(archive_sink.id, "archive");
        assert_eq!(archive_sink.field_mappings.len(), 1);
        assert_eq!(archive_sink.field_mappings[0].source_field, Some("event_id".to_string()));
        assert_eq!(archive_sink.field_mappings[0].target_field, "id");

        let internal_archive = pipeline.stages.get("distribute:archive").unwrap();
        assert_eq!(internal_archive.stage_type, StageType::Sink);
        assert_eq!(internal_archive.service_selector.service_name, Some("s3".to_string()));

        let internal_realtime = pipeline.stages.get("distribute:realtime").unwrap();
        assert_eq!(internal_realtime.stage_type, StageType::Sink);
        assert_eq!(internal_realtime.service_selector.service_name, Some("websocket".to_string()));
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("2h"), Some(Duration::from_secs(7200)));
        assert_eq!(parse_duration("100ms"), Some(Duration::from_millis(100)));
        assert_eq!(parse_duration("invalid"), None);
        assert_eq!(parse_duration(""), None);
    }
}
