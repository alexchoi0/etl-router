#[cfg(test)]
mod dag_tests {
    use std::collections::HashMap;
    use crate::{Pipeline, Stage, StageType, ServiceSelector, LoadBalanceStrategy};

    fn create_stage(id: &str, stage_type: StageType) -> Stage {
        Stage {
            id: id.to_string(),
            name: format!("{}-stage", id),
            stage_type,
            service_selector: ServiceSelector {
                service_name: Some(format!("{}-service", id)),
                group_id: None,
                labels: HashMap::new(),
                load_balance: LoadBalanceStrategy::RoundRobin,
            },
            parallelism: 1,
            lookup_config: None,
            fan_in_config: None,
            fan_out_config: None,
        }
    }

    #[test]
    fn test_create_simple_pipeline() {
        let mut pipeline = Pipeline::new("p1".to_string(), "Simple Pipeline".to_string());

        pipeline.add_stage(create_stage("source", StageType::Source));
        pipeline.add_stage(create_stage("transform", StageType::Transform));
        pipeline.add_stage(create_stage("sink", StageType::Sink));

        pipeline.add_edge("source", "transform", None);
        pipeline.add_edge("transform", "sink", None);

        assert_eq!(pipeline.stages.len(), 3);
        assert_eq!(pipeline.edges.len(), 2);
        assert!(pipeline.stages.contains_key("source"));
        assert!(pipeline.stages.contains_key("transform"));
        assert!(pipeline.stages.contains_key("sink"));
    }

    #[test]
    fn test_fan_out_pipeline() {
        let mut pipeline = Pipeline::new("p1".to_string(), "Fan-out Pipeline".to_string());

        pipeline.add_stage(create_stage("source", StageType::Source));
        pipeline.add_stage(create_stage("transform_a", StageType::Transform));
        pipeline.add_stage(create_stage("transform_b", StageType::Transform));

        pipeline.add_edge("source", "transform_a", None);
        pipeline.add_edge("source", "transform_b", None);

        let downstream = pipeline.get_downstream_stages("source");
        assert_eq!(downstream.len(), 2);
    }

    #[test]
    fn test_fan_in_pipeline() {
        let mut pipeline = Pipeline::new("p1".to_string(), "Fan-in Pipeline".to_string());

        pipeline.add_stage(create_stage("source_a", StageType::Source));
        pipeline.add_stage(create_stage("source_b", StageType::Source));
        pipeline.add_stage(create_stage("transform", StageType::Transform));

        pipeline.add_edge("source_a", "transform", None);
        pipeline.add_edge("source_b", "transform", None);

        let upstream = pipeline.get_upstream_stages("transform");
        assert_eq!(upstream.len(), 2);
    }

    #[test]
    fn test_diamond_pipeline() {
        let mut pipeline = Pipeline::new("p1".to_string(), "Diamond Pipeline".to_string());

        pipeline.add_stage(create_stage("source", StageType::Source));
        pipeline.add_stage(create_stage("a", StageType::Transform));
        pipeline.add_stage(create_stage("b", StageType::Transform));
        pipeline.add_stage(create_stage("sink", StageType::Sink));

        pipeline.add_edge("source", "a", None);
        pipeline.add_edge("source", "b", None);
        pipeline.add_edge("a", "sink", None);
        pipeline.add_edge("b", "sink", None);

        let downstream_source = pipeline.get_downstream_stages("source");
        assert_eq!(downstream_source.len(), 2);

        let upstream_sink = pipeline.get_upstream_stages("sink");
        assert_eq!(upstream_sink.len(), 2);
    }

    #[test]
    fn test_get_source_stages() {
        let mut pipeline = Pipeline::new("p1".to_string(), "Multi-source Pipeline".to_string());

        pipeline.add_stage(create_stage("source1", StageType::Source));
        pipeline.add_stage(create_stage("source2", StageType::Source));
        pipeline.add_stage(create_stage("transform", StageType::Transform));
        pipeline.add_stage(create_stage("sink", StageType::Sink));

        let sources = pipeline.get_source_stages();
        assert_eq!(sources.len(), 2);
        assert!(sources.iter().all(|s| s.stage_type == StageType::Source));
    }

    #[test]
    fn test_get_sink_stages() {
        let mut pipeline = Pipeline::new("p1".to_string(), "Multi-sink Pipeline".to_string());

        pipeline.add_stage(create_stage("source", StageType::Source));
        pipeline.add_stage(create_stage("sink1", StageType::Sink));
        pipeline.add_stage(create_stage("sink2", StageType::Sink));

        let sinks = pipeline.get_sink_stages();
        assert_eq!(sinks.len(), 2);
        assert!(sinks.iter().all(|s| s.stage_type == StageType::Sink));
    }

    #[test]
    fn test_get_downstream_stages() {
        let mut pipeline = Pipeline::new("p1".to_string(), "Test Pipeline".to_string());

        pipeline.add_stage(create_stage("source", StageType::Source));
        pipeline.add_stage(create_stage("a", StageType::Transform));
        pipeline.add_stage(create_stage("b", StageType::Transform));
        pipeline.add_stage(create_stage("c", StageType::Transform));

        pipeline.add_edge("source", "a", None);
        pipeline.add_edge("source", "b", None);
        pipeline.add_edge("source", "c", None);

        let downstream = pipeline.get_downstream_stages("source");
        assert_eq!(downstream.len(), 3);
    }

    #[test]
    fn test_get_upstream_stages() {
        let mut pipeline = Pipeline::new("p1".to_string(), "Test Pipeline".to_string());

        pipeline.add_stage(create_stage("a", StageType::Source));
        pipeline.add_stage(create_stage("b", StageType::Source));
        pipeline.add_stage(create_stage("c", StageType::Source));
        pipeline.add_stage(create_stage("sink", StageType::Sink));

        pipeline.add_edge("a", "sink", None);
        pipeline.add_edge("b", "sink", None);
        pipeline.add_edge("c", "sink", None);

        let upstream = pipeline.get_upstream_stages("sink");
        assert_eq!(upstream.len(), 3);
    }

    #[test]
    fn test_cycle_detection() {
        let mut pipeline = Pipeline::new("p1".to_string(), "Cyclic Pipeline".to_string());

        pipeline.add_stage(create_stage("a", StageType::Source));
        pipeline.add_stage(create_stage("b", StageType::Transform));
        pipeline.add_stage(create_stage("c", StageType::Transform));

        pipeline.add_edge("a", "b", None);
        pipeline.add_edge("b", "c", None);
        pipeline.add_edge("c", "b", None);

        assert!(pipeline.has_cycle());

        let cycle = pipeline.detect_cycle();
        assert!(cycle.is_some());
        let cycle_path = cycle.unwrap();
        assert!(cycle_path.contains(&"b".to_string()));
        assert!(cycle_path.contains(&"c".to_string()));
    }

    #[test]
    fn test_no_cycle_in_valid_pipeline() {
        let mut pipeline = Pipeline::new("p1".to_string(), "Valid Pipeline".to_string());

        pipeline.add_stage(create_stage("source", StageType::Source));
        pipeline.add_stage(create_stage("transform", StageType::Transform));
        pipeline.add_stage(create_stage("sink", StageType::Sink));

        pipeline.add_edge("source", "transform", None);
        pipeline.add_edge("transform", "sink", None);

        assert!(!pipeline.has_cycle());
        assert!(pipeline.detect_cycle().is_none());
    }

    #[test]
    fn test_disconnected_stage_warning() {
        use crate::PipelineValidationError;

        let mut pipeline = Pipeline::new("p1".to_string(), "Disconnected Pipeline".to_string());

        pipeline.add_stage(create_stage("source", StageType::Source));
        pipeline.add_stage(create_stage("transform", StageType::Transform));
        pipeline.add_stage(create_stage("sink", StageType::Sink));
        pipeline.add_stage(create_stage("orphan", StageType::Transform));

        pipeline.add_edge("source", "transform", None);
        pipeline.add_edge("transform", "sink", None);

        let result = pipeline.validate();
        assert!(result.is_err());

        let errors = result.unwrap_err();
        let has_disconnected = errors.iter().any(|e| {
            matches!(e, PipelineValidationError::DisconnectedStage { stage_id } if stage_id == "orphan")
        });
        assert!(has_disconnected);
    }

    #[test]
    fn test_valid_pipeline_passes_validation() {
        let mut pipeline = Pipeline::new("p1".to_string(), "Valid Pipeline".to_string());

        pipeline.add_stage(create_stage("source", StageType::Source));
        pipeline.add_stage(create_stage("transform", StageType::Transform));
        pipeline.add_stage(create_stage("sink", StageType::Sink));

        pipeline.add_edge("source", "transform", None);
        pipeline.add_edge("transform", "sink", None);

        let result = pipeline.validate();
        assert!(result.is_ok());
    }
}

#[cfg(test)]
mod condition_tests {
    use std::collections::HashMap;
    use crate::Condition;
    use conveyor_etl_proto::common::Record;

    fn create_test_record(record_type: &str, metadata: HashMap<String, String>) -> Record {
        Record {
            id: None,
            record_type: record_type.to_string(),
            key: vec![],
            payload: vec![],
            metadata,
            event_time: None,
            ingestion_time: None,
        }
    }

    #[test]
    fn test_field_equals_condition() {
        let mut metadata = HashMap::new();
        metadata.insert("status".to_string(), "error".to_string());

        let condition = Condition::MetadataEquals {
            key: "status".to_string(),
            value: "error".to_string(),
        };

        let record_error = create_test_record("log", metadata);
        assert!(condition.evaluate(&record_error));

        let mut metadata_ok = HashMap::new();
        metadata_ok.insert("status".to_string(), "ok".to_string());
        let record_ok = create_test_record("log", metadata_ok);
        assert!(!condition.evaluate(&record_ok));
    }

    #[test]
    fn test_field_not_equals_condition() {
        let condition = Condition::Not(Box::new(Condition::MetadataEquals {
            key: "type".to_string(),
            value: "heartbeat".to_string(),
        }));

        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), "data".to_string());
        let record_data = create_test_record("event", metadata);
        assert!(condition.evaluate(&record_data));

        let mut metadata_hb = HashMap::new();
        metadata_hb.insert("type".to_string(), "heartbeat".to_string());
        let record_hb = create_test_record("event", metadata_hb);
        assert!(!condition.evaluate(&record_hb));
    }

    #[test]
    fn test_field_contains_condition() {
        let condition = Condition::MetadataMatch {
            key: "message".to_string(),
            pattern: "error".to_string(),
        };

        let mut metadata = HashMap::new();
        metadata.insert("message".to_string(), "An error occurred".to_string());
        let record_error = create_test_record("log", metadata);
        assert!(condition.evaluate(&record_error));

        let mut metadata_ok = HashMap::new();
        metadata_ok.insert("message".to_string(), "All good".to_string());
        let record_ok = create_test_record("log", metadata_ok);
        assert!(!condition.evaluate(&record_ok));
    }

    #[test]
    fn test_field_regex_condition() {
        let condition = Condition::MetadataMatch {
            key: "email".to_string(),
            pattern: r".*@example\.com".to_string(),
        };

        let mut metadata = HashMap::new();
        metadata.insert("email".to_string(), "user@example.com".to_string());
        let record_match = create_test_record("user", metadata);
        assert!(condition.evaluate(&record_match));

        let mut metadata_other = HashMap::new();
        metadata_other.insert("email".to_string(), "user@other.com".to_string());
        let record_other = create_test_record("user", metadata_other);
        assert!(!condition.evaluate(&record_other));
    }

    #[test]
    fn test_field_greater_than_condition() {
        let mut metadata = HashMap::new();
        metadata.insert("priority".to_string(), "10".to_string());
        metadata.insert("score".to_string(), "85.5".to_string());

        let record = create_test_record("event", metadata);

        let gt_condition = Condition::MetadataGreaterThan {
            key: "priority".to_string(),
            value: 5.0,
        };
        assert!(gt_condition.evaluate(&record));

        let gt_condition_false = Condition::MetadataGreaterThan {
            key: "priority".to_string(),
            value: 15.0,
        };
        assert!(!gt_condition_false.evaluate(&record));

        let lt_condition = Condition::MetadataLessThan {
            key: "priority".to_string(),
            value: 15.0,
        };
        assert!(lt_condition.evaluate(&record));

        let gte_condition = Condition::MetadataGreaterThanOrEqual {
            key: "priority".to_string(),
            value: 10.0,
        };
        assert!(gte_condition.evaluate(&record));

        let lte_condition = Condition::MetadataLessThanOrEqual {
            key: "score".to_string(),
            value: 85.5,
        };
        assert!(lte_condition.evaluate(&record));

        let missing_key = Condition::MetadataGreaterThan {
            key: "nonexistent".to_string(),
            value: 0.0,
        };
        assert!(!missing_key.evaluate(&record));
    }

    #[test]
    fn test_and_condition() {
        let condition = Condition::And(vec![
            Condition::RecordType("log".to_string()),
            Condition::MetadataEquals {
                key: "level".to_string(),
                value: "error".to_string(),
            },
        ]);

        let mut metadata = HashMap::new();
        metadata.insert("level".to_string(), "error".to_string());
        let record_match = create_test_record("log", metadata);
        assert!(condition.evaluate(&record_match));

        let mut metadata_info = HashMap::new();
        metadata_info.insert("level".to_string(), "info".to_string());
        let record_info = create_test_record("log", metadata_info);
        assert!(!condition.evaluate(&record_info));
    }

    #[test]
    fn test_or_condition() {
        let condition = Condition::Or(vec![
            Condition::MetadataEquals {
                key: "level".to_string(),
                value: "error".to_string(),
            },
            Condition::MetadataEquals {
                key: "level".to_string(),
                value: "warning".to_string(),
            },
        ]);

        let mut metadata_error = HashMap::new();
        metadata_error.insert("level".to_string(), "error".to_string());
        let record_error = create_test_record("log", metadata_error);
        assert!(condition.evaluate(&record_error));

        let mut metadata_warning = HashMap::new();
        metadata_warning.insert("level".to_string(), "warning".to_string());
        let record_warning = create_test_record("log", metadata_warning);
        assert!(condition.evaluate(&record_warning));

        let mut metadata_info = HashMap::new();
        metadata_info.insert("level".to_string(), "info".to_string());
        let record_info = create_test_record("log", metadata_info);
        assert!(!condition.evaluate(&record_info));
    }

    #[test]
    fn test_not_condition() {
        let condition = Condition::Not(Box::new(Condition::RecordType("heartbeat".to_string())));

        let record_data = create_test_record("data", HashMap::new());
        assert!(condition.evaluate(&record_data));

        let record_hb = create_test_record("heartbeat", HashMap::new());
        assert!(!condition.evaluate(&record_hb));
    }

    #[test]
    fn test_nested_conditions() {
        let condition = Condition::Or(vec![
            Condition::And(vec![
                Condition::RecordType("log".to_string()),
                Condition::MetadataEquals {
                    key: "level".to_string(),
                    value: "error".to_string(),
                },
            ]),
            Condition::And(vec![
                Condition::RecordType("metric".to_string()),
                Condition::MetadataEquals {
                    key: "critical".to_string(),
                    value: "true".to_string(),
                },
            ]),
        ]);

        let mut metadata = HashMap::new();
        metadata.insert("level".to_string(), "error".to_string());
        let record_log_error = create_test_record("log", metadata);
        assert!(condition.evaluate(&record_log_error));

        let mut metadata_metric = HashMap::new();
        metadata_metric.insert("critical".to_string(), "true".to_string());
        let record_critical = create_test_record("metric", metadata_metric);
        assert!(condition.evaluate(&record_critical));

        let record_none = create_test_record("other", HashMap::new());
        assert!(!condition.evaluate(&record_none));
    }

    #[test]
    fn test_missing_field_handling() {
        let condition = Condition::MetadataEquals {
            key: "optional_field".to_string(),
            value: "some_value".to_string(),
        };

        let record = create_test_record("test", HashMap::new());
        assert!(!condition.evaluate(&record));
    }
}

#[cfg(test)]
mod routing_engine_tests {
    use std::collections::HashMap;
    use crate::{RoutingEngine, Pipeline, Stage, StageType, ServiceSelector, LoadBalanceStrategy};
    use conveyor_etl_proto::common::RecordBatch;

    fn create_stage(id: &str, stage_type: StageType, service_name: Option<&str>) -> Stage {
        Stage {
            id: id.to_string(),
            name: format!("{}-stage", id),
            stage_type,
            service_selector: ServiceSelector {
                service_name: service_name.map(|s| s.to_string()),
                group_id: None,
                labels: HashMap::new(),
                load_balance: LoadBalanceStrategy::RoundRobin,
            },
            parallelism: 1,
            lookup_config: None,
            fan_in_config: None,
            fan_out_config: None,
        }
    }

    #[tokio::test]
    async fn test_route_batch_simple() {
        use conveyor_etl_proto::common::Record;

        let engine = RoutingEngine::new();

        let mut pipeline = Pipeline::new("p1".to_string(), "Simple Pipeline".to_string());
        pipeline.add_stage(create_stage("source", StageType::Source, Some("my-source")));
        pipeline.add_stage(create_stage("transform", StageType::Transform, Some("my-transform")));
        pipeline.add_stage(create_stage("sink", StageType::Sink, Some("my-sink")));
        pipeline.add_edge("source", "transform", None);
        pipeline.add_edge("transform", "sink", None);
        pipeline.enabled = true;

        engine.add_pipeline(pipeline).await;

        let batch = RecordBatch {
            batch_id: "b1".to_string(),
            records: vec![Record {
                id: None,
                record_type: "test".to_string(),
                key: vec![],
                payload: vec![1, 2, 3],
                metadata: HashMap::new(),
                event_time: None,
                ingestion_time: None,
            }],
            watermark: None,
        };

        let decisions = engine.route_batch("p1", "source", batch).await.unwrap();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].target_stage_id, "transform");
        assert_eq!(decisions[0].records.len(), 1);
    }

    #[tokio::test]
    async fn test_route_batch_with_condition() {
        use crate::Condition;
        use conveyor_etl_proto::common::Record;

        let engine = RoutingEngine::new();

        let mut pipeline = Pipeline::new("p1".to_string(), "Conditional Pipeline".to_string());
        pipeline.add_stage(create_stage("source", StageType::Source, Some("my-source")));
        pipeline.add_stage(create_stage("error_handler", StageType::Transform, Some("error-handler")));
        pipeline.add_stage(create_stage("normal_handler", StageType::Transform, Some("normal-handler")));

        pipeline.add_edge("source", "error_handler", Some(Condition::MetadataEquals {
            key: "level".to_string(),
            value: "error".to_string(),
        }));
        pipeline.add_edge("source", "normal_handler", Some(Condition::Not(Box::new(
            Condition::MetadataEquals {
                key: "level".to_string(),
                value: "error".to_string(),
            }
        ))));
        pipeline.enabled = true;

        engine.add_pipeline(pipeline).await;

        let mut error_metadata = HashMap::new();
        error_metadata.insert("level".to_string(), "error".to_string());

        let mut info_metadata = HashMap::new();
        info_metadata.insert("level".to_string(), "info".to_string());

        let batch = RecordBatch {
            batch_id: "b1".to_string(),
            records: vec![
                Record {
                    id: None,
                    record_type: "log".to_string(),
                    key: vec![],
                    payload: vec![],
                    metadata: error_metadata,
                    event_time: None,
                    ingestion_time: None,
                },
                Record {
                    id: None,
                    record_type: "log".to_string(),
                    key: vec![],
                    payload: vec![],
                    metadata: info_metadata,
                    event_time: None,
                    ingestion_time: None,
                },
            ],
            watermark: None,
        };

        let decisions = engine.route_batch("p1", "source", batch).await.unwrap();

        let error_decision = decisions.iter().find(|d| d.target_stage_id == "error_handler");
        let normal_decision = decisions.iter().find(|d| d.target_stage_id == "normal_handler");

        assert!(error_decision.is_some());
        assert!(normal_decision.is_some());
        assert_eq!(error_decision.unwrap().records.len(), 1);
        assert_eq!(normal_decision.unwrap().records.len(), 1);
    }

    #[tokio::test]
    async fn test_route_batch_fan_out() {
        use conveyor_etl_proto::common::Record;

        let engine = RoutingEngine::new();

        let mut pipeline = Pipeline::new("p1".to_string(), "Fan-out Pipeline".to_string());
        pipeline.add_stage(create_stage("source", StageType::Source, Some("my-source")));
        pipeline.add_stage(create_stage("sink_a", StageType::Sink, Some("sink-a")));
        pipeline.add_stage(create_stage("sink_b", StageType::Sink, Some("sink-b")));
        pipeline.add_stage(create_stage("sink_c", StageType::Sink, Some("sink-c")));

        pipeline.add_edge("source", "sink_a", None);
        pipeline.add_edge("source", "sink_b", None);
        pipeline.add_edge("source", "sink_c", None);
        pipeline.enabled = true;

        engine.add_pipeline(pipeline).await;

        let batch = RecordBatch {
            batch_id: "b1".to_string(),
            records: vec![
                Record {
                    id: None,
                    record_type: "event".to_string(),
                    key: vec![],
                    payload: vec![1, 2, 3],
                    metadata: HashMap::new(),
                    event_time: None,
                    ingestion_time: None,
                },
            ],
            watermark: None,
        };

        let decisions = engine.route_batch("p1", "source", batch).await.unwrap();

        assert_eq!(decisions.len(), 3);

        for decision in &decisions {
            assert_eq!(decision.records.len(), 1);
        }

        let target_ids: std::collections::HashSet<_> = decisions.iter()
            .map(|d| d.target_stage_id.as_str())
            .collect();
        assert!(target_ids.contains("sink_a"));
        assert!(target_ids.contains("sink_b"));
        assert!(target_ids.contains("sink_c"));
    }

    #[tokio::test]
    async fn test_route_batch_mixed_conditions() {
        use crate::Condition;
        use conveyor_etl_proto::common::Record;

        let engine = RoutingEngine::new();

        let mut pipeline = Pipeline::new("p1".to_string(), "Mixed Pipeline".to_string());
        pipeline.add_stage(create_stage("source", StageType::Source, Some("my-source")));
        pipeline.add_stage(create_stage("all", StageType::Transform, Some("all-handler")));
        pipeline.add_stage(create_stage("errors", StageType::Transform, Some("error-handler")));
        pipeline.add_stage(create_stage("critical", StageType::Transform, Some("critical-handler")));

        pipeline.add_edge("source", "all", None);
        pipeline.add_edge("source", "errors", Some(Condition::MetadataEquals {
            key: "level".to_string(),
            value: "error".to_string(),
        }));
        pipeline.add_edge("source", "critical", Some(Condition::And(vec![
            Condition::MetadataEquals {
                key: "level".to_string(),
                value: "error".to_string(),
            },
            Condition::MetadataEquals {
                key: "critical".to_string(),
                value: "true".to_string(),
            },
        ])));
        pipeline.enabled = true;

        engine.add_pipeline(pipeline).await;

        let mut critical_error_meta = HashMap::new();
        critical_error_meta.insert("level".to_string(), "error".to_string());
        critical_error_meta.insert("critical".to_string(), "true".to_string());

        let mut normal_error_meta = HashMap::new();
        normal_error_meta.insert("level".to_string(), "error".to_string());

        let batch = RecordBatch {
            batch_id: "b1".to_string(),
            records: vec![
                Record {
                    id: None,
                    record_type: "event".to_string(),
                    key: vec![],
                    payload: vec![],
                    metadata: critical_error_meta,
                    event_time: None,
                    ingestion_time: None,
                },
                Record {
                    id: None,
                    record_type: "event".to_string(),
                    key: vec![],
                    payload: vec![],
                    metadata: normal_error_meta,
                    event_time: None,
                    ingestion_time: None,
                },
                Record {
                    id: None,
                    record_type: "event".to_string(),
                    key: vec![],
                    payload: vec![],
                    metadata: HashMap::new(),
                    event_time: None,
                    ingestion_time: None,
                },
            ],
            watermark: None,
        };

        let decisions = engine.route_batch("p1", "source", batch).await.unwrap();

        let all_decision = decisions.iter().find(|d| d.target_stage_id == "all");
        let error_decision = decisions.iter().find(|d| d.target_stage_id == "errors");
        let critical_decision = decisions.iter().find(|d| d.target_stage_id == "critical");

        assert!(all_decision.is_some());
        assert_eq!(all_decision.unwrap().records.len(), 3);

        assert!(error_decision.is_some());
        assert_eq!(error_decision.unwrap().records.len(), 2);

        assert!(critical_decision.is_some());
        assert_eq!(critical_decision.unwrap().records.len(), 1);
    }

    #[tokio::test]
    async fn test_add_pipeline_dynamically() {
        let engine = RoutingEngine::new();

        assert!(engine.list_pipelines().await.is_empty());

        let mut pipeline = Pipeline::new("p1".to_string(), "Test Pipeline".to_string());
        pipeline.add_stage(create_stage("source", StageType::Source, Some("my-source")));
        pipeline.enabled = true;

        engine.add_pipeline(pipeline).await;

        let pipelines = engine.list_pipelines().await;
        assert_eq!(pipelines.len(), 1);
        assert_eq!(pipelines[0].id, "p1");
    }

    #[tokio::test]
    async fn test_remove_pipeline_dynamically() {
        let engine = RoutingEngine::new();

        let mut pipeline = Pipeline::new("p1".to_string(), "Test Pipeline".to_string());
        pipeline.enabled = true;
        engine.add_pipeline(pipeline).await;

        assert_eq!(engine.list_pipelines().await.len(), 1);

        engine.remove_pipeline("p1").await;

        assert!(engine.list_pipelines().await.is_empty());
    }

    #[tokio::test]
    async fn test_find_pipelines_for_source() {
        let engine = RoutingEngine::new();

        let mut pipeline_a = Pipeline::new("pa".to_string(), "Pipeline A".to_string());
        pipeline_a.add_stage(create_stage("source", StageType::Source, Some("my-source")));
        pipeline_a.enabled = true;
        engine.add_pipeline(pipeline_a).await;

        let mut pipeline_b = Pipeline::new("pb".to_string(), "Pipeline B".to_string());
        pipeline_b.add_stage(create_stage("source", StageType::Source, Some("my-source")));
        pipeline_b.enabled = true;
        engine.add_pipeline(pipeline_b).await;

        let mut pipeline_c = Pipeline::new("pc".to_string(), "Pipeline C".to_string());
        pipeline_c.add_stage(create_stage("source", StageType::Source, Some("other-source")));
        pipeline_c.enabled = true;
        engine.add_pipeline(pipeline_c).await;

        let pipelines = engine.find_pipelines_for_source("my-source").await;
        assert_eq!(pipelines.len(), 2);
        assert!(pipelines.contains(&"pa".to_string()));
        assert!(pipelines.contains(&"pb".to_string()));
    }

    #[tokio::test]
    async fn test_service_selector_matching() {
        let engine = RoutingEngine::new();

        let mut labels = HashMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        labels.insert("region".to_string(), "us".to_string());

        let stage = Stage {
            id: "transform".to_string(),
            name: "Transform".to_string(),
            stage_type: StageType::Transform,
            service_selector: ServiceSelector {
                service_name: Some("my-transform".to_string()),
                group_id: None,
                labels: labels.clone(),
                load_balance: LoadBalanceStrategy::RoundRobin,
            },
            parallelism: 1,
            lookup_config: None,
            fan_in_config: None,
            fan_out_config: None,
        };

        let mut pipeline = Pipeline::new("p1".to_string(), "Test Pipeline".to_string());
        pipeline.add_stage(stage);
        pipeline.enabled = true;

        engine.add_pipeline(pipeline).await;

        let p = engine.get_pipeline("p1").await.unwrap();
        let s = p.stages.get("transform").unwrap();
        assert_eq!(s.service_selector.labels.get("env"), Some(&"prod".to_string()));
        assert_eq!(s.service_selector.labels.get("region"), Some(&"us".to_string()));
    }

    #[tokio::test]
    #[ignore = "Load balancing requires registry integration"]
    async fn test_load_balance_across_service_instances() {
        todo!("Implement load balancing test")
    }

    #[tokio::test]
    #[ignore = "Metrics not yet implemented"]
    async fn test_routing_metrics_recorded() {
        todo!("Implement routing metrics test")
    }
}

#[cfg(test)]
mod routing_persistence_tests {
    #[tokio::test]
    #[ignore = "Pipeline persistence not implemented"]
    async fn test_pipeline_persisted_in_raft() {
        todo!("Implement pipeline persistence test")
    }

    #[tokio::test]
    #[ignore = "Pipeline persistence not implemented"]
    async fn test_pipeline_survives_restart() {
        todo!("Implement pipeline restart test")
    }
}
