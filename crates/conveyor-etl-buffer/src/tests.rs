#[cfg(test)]
mod buffer_manager_tests {
    use std::time::Instant;
    use crate::{BufferManager, BufferedRecord};
    use conveyor_etl_proto::common::{Record, RecordId};

    fn create_test_record(id: &str, source_id: &str, target_stage_id: &str) -> BufferedRecord {
        BufferedRecord {
            record: Record {
                id: Some(RecordId {
                    source_id: source_id.to_string(),
                    partition: 0,
                    sequence_number: id.parse().unwrap_or(0),
                    idempotency_key: id.as_bytes().to_vec(),
                }),
                record_type: "test".to_string(),
                key: format!("key-{}", id).into_bytes(),
                payload: vec![1, 2, 3],
                metadata: Default::default(),
                event_time: None,
                ingestion_time: None,
            },
            source_id: source_id.to_string(),
            pipeline_id: "test-pipeline".to_string(),
            target_stage_id: target_stage_id.to_string(),
            buffered_at: Instant::now(),
            retry_count: 0,
        }
    }

    fn get_record_id(record: &Record) -> String {
        record.id.as_ref()
            .map(|id| String::from_utf8_lossy(&id.idempotency_key).to_string())
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn test_buffer_record_for_stage() {
        let manager = BufferManager::with_limits(100, 50, 25, 0.8);

        let record = create_test_record("1", "source-1", "transform-1");
        let result = manager.buffer_for_stage("transform-1", record).await;

        assert!(result.is_ok());
        assert_eq!(manager.get_stage_buffer_size("transform-1").await, 1);

        let batch = manager.get_batch("transform-1", 10).await;
        assert_eq!(batch.len(), 1);
        assert_eq!(get_record_id(&batch[0].record), "1");
    }

    #[tokio::test]
    async fn test_buffer_batch_for_stage() {
        let manager = BufferManager::with_limits(1000, 500, 250, 0.8);

        let records: Vec<BufferedRecord> = (0..100)
            .map(|i| create_test_record(&i.to_string(), "source-1", "transform-1"))
            .collect();

        let result = manager.buffer_batch_for_stage("transform-1", records).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100);
        assert_eq!(manager.get_stage_buffer_size("transform-1").await, 100);

        let batch = manager.get_batch("transform-1", 100).await;
        assert_eq!(batch.len(), 100);
    }

    #[tokio::test]
    async fn test_get_batch_respects_max_size() {
        let manager = BufferManager::with_limits(1000, 500, 250, 0.8);

        let records: Vec<BufferedRecord> = (0..100)
            .map(|i| create_test_record(&i.to_string(), "source-1", "transform-1"))
            .collect();

        manager.buffer_batch_for_stage("transform-1", records).await.unwrap();

        let batch = manager.get_batch("transform-1", 10).await;
        assert_eq!(batch.len(), 10);

        assert_eq!(manager.get_stage_buffer_size("transform-1").await, 90);
    }

    #[tokio::test]
    async fn test_fifo_ordering() {
        let manager = BufferManager::with_limits(100, 50, 25, 0.8);

        let record_a = create_test_record("A", "source-1", "transform-1");
        let record_b = create_test_record("B", "source-1", "transform-1");
        let record_c = create_test_record("C", "source-1", "transform-1");

        manager.buffer_for_stage("transform-1", record_a).await.unwrap();
        manager.buffer_for_stage("transform-1", record_b).await.unwrap();
        manager.buffer_for_stage("transform-1", record_c).await.unwrap();

        let batch = manager.get_batch("transform-1", 1).await;
        assert_eq!(get_record_id(&batch[0].record), "A");

        let batch = manager.get_batch("transform-1", 1).await;
        assert_eq!(get_record_id(&batch[0].record), "B");

        let batch = manager.get_batch("transform-1", 1).await;
        assert_eq!(get_record_id(&batch[0].record), "C");
    }

    #[tokio::test]
    async fn test_return_to_buffer_for_retry() {
        let manager = BufferManager::with_limits(100, 50, 25, 0.8);

        let record = create_test_record("1", "source-1", "transform-1");
        manager.buffer_for_stage("transform-1", record).await.unwrap();

        let batch = manager.get_batch("transform-1", 1).await;
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].retry_count, 0);

        manager.return_to_buffer("transform-1", batch).await;

        let batch = manager.get_batch("transform-1", 1).await;
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].retry_count, 1);
    }

    #[tokio::test]
    async fn test_stage_isolation() {
        let manager = BufferManager::with_limits(100, 50, 25, 0.8);

        let record_a1 = create_test_record("A1", "source-1", "stage-a");
        let record_a2 = create_test_record("A2", "source-1", "stage-a");
        let record_b1 = create_test_record("B1", "source-1", "stage-b");
        let record_b2 = create_test_record("B2", "source-1", "stage-b");

        manager.buffer_for_stage("stage-a", record_a1).await.unwrap();
        manager.buffer_for_stage("stage-a", record_a2).await.unwrap();
        manager.buffer_for_stage("stage-b", record_b1).await.unwrap();
        manager.buffer_for_stage("stage-b", record_b2).await.unwrap();

        let batch_a = manager.get_batch("stage-a", 10).await;
        assert_eq!(batch_a.len(), 2);
        assert!(batch_a.iter().all(|r| get_record_id(&r.record).starts_with("A")));

        let batch_b = manager.get_batch("stage-b", 10).await;
        assert_eq!(batch_b.len(), 2);
        assert!(batch_b.iter().all(|r| get_record_id(&r.record).starts_with("B")));
    }

    #[tokio::test]
    async fn test_buffer_utilization_calculation() {
        let manager = BufferManager::with_limits(200, 100, 50, 0.8);

        let records: Vec<BufferedRecord> = (0..50)
            .map(|i| create_test_record(&i.to_string(), "source-1", "transform-1"))
            .collect();

        manager.buffer_batch_for_stage("transform-1", records).await.unwrap();

        let utilization = manager.get_stage_utilization("transform-1").await;
        assert!((utilization - 0.5).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_global_utilization() {
        let manager = BufferManager::with_limits(100, 50, 25, 0.8);

        let records_a: Vec<BufferedRecord> = (0..25)
            .map(|i| create_test_record(&format!("a{}", i), "source-1", "stage-a"))
            .collect();
        let records_b: Vec<BufferedRecord> = (0..25)
            .map(|i| create_test_record(&format!("b{}", i), "source-1", "stage-b"))
            .collect();

        manager.buffer_batch_for_stage("stage-a", records_a).await.unwrap();
        manager.buffer_batch_for_stage("stage-b", records_b).await.unwrap();

        let global = manager.get_global_utilization().await;
        assert!((global - 0.5).abs() < 0.01);
        assert_eq!(manager.get_total_buffered().await, 50);
    }

    #[tokio::test]
    async fn test_buffer_overflow_rejected() {
        let manager = BufferManager::with_limits(10, 5, 5, 0.8);

        let records: Vec<BufferedRecord> = (0..10)
            .map(|i| create_test_record(&i.to_string(), "source-1", "transform-1"))
            .collect();

        let result = manager.buffer_batch_for_stage("transform-1", records).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 5);

        let extra = create_test_record("extra", "source-1", "transform-1");
        let result = manager.buffer_for_stage("transform-1", extra).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("buffer full"));
    }

    #[tokio::test]
    async fn test_get_stages_with_data() {
        let manager = BufferManager::with_limits(100, 50, 25, 0.8);

        manager.buffer_for_stage("stage-a", create_test_record("1", "source-1", "stage-a")).await.unwrap();
        manager.buffer_for_stage("stage-b", create_test_record("2", "source-1", "stage-b")).await.unwrap();

        let stages = manager.get_stages_with_data().await;
        assert_eq!(stages.len(), 2);
        assert!(stages.contains(&"stage-a".to_string()));
        assert!(stages.contains(&"stage-b".to_string()));

        manager.get_batch("stage-a", 10).await;

        let stages = manager.get_stages_with_data().await;
        assert_eq!(stages.len(), 1);
        assert!(stages.contains(&"stage-b".to_string()));
    }
}

#[cfg(test)]
mod backpressure_tests {
    use crate::{BackpressureController, BackpressureSignal};

    #[tokio::test]
    async fn test_backpressure_pause_on_high_utilization() {
        let controller = BackpressureController::new(0.8, 0.6);

        let signal = controller.compute_signal("source-1", 0.9).await;

        assert_eq!(signal, BackpressureSignal::Pause);
    }

    #[tokio::test]
    async fn test_backpressure_resume_on_low_utilization() {
        let controller = BackpressureController::new(0.8, 0.6);

        controller.compute_signal("source-1", 0.9).await;
        assert_eq!(controller.get_current_signal("source-1").await, BackpressureSignal::Pause);

        let signal = controller.compute_signal("source-1", 0.5).await;

        assert_eq!(signal, BackpressureSignal::None);
    }

    #[tokio::test]
    async fn test_backpressure_throttle_between_watermarks() {
        let controller = BackpressureController::new(0.8, 0.6);

        let signal = controller.compute_signal("source-1", 0.7).await;

        match signal {
            BackpressureSignal::SlowDown { delay_ms } => {
                assert!(delay_ms >= 10);
                assert!(delay_ms <= 100);
            }
            _ => panic!("Expected SlowDown signal, got {:?}", signal),
        }
    }

    #[tokio::test]
    async fn test_credit_based_flow_control() {
        let controller = BackpressureController::new(0.8, 0.6);

        controller.grant_credits("source-1", 100).await;

        assert_eq!(controller.get_available_credits("source-1").await, 100);

        controller.use_credits("source-1", 50).await;

        assert_eq!(controller.get_available_credits("source-1").await, 50);
    }

    #[tokio::test]
    async fn test_credits_exhausted_blocks() {
        let controller = BackpressureController::new(0.8, 0.6);

        controller.grant_credits("source-1", 10).await;
        controller.use_credits("source-1", 10).await;

        let available = controller.get_available_credits("source-1").await;
        assert_eq!(available, 0);
    }

    #[tokio::test]
    async fn test_credit_replenishment() {
        let controller = BackpressureController::new(0.8, 0.6);

        controller.grant_credits("source-1", 100).await;
        controller.use_credits("source-1", 100).await;
        assert_eq!(controller.get_available_credits("source-1").await, 0);

        controller.grant_credits("source-1", 50).await;

        assert_eq!(controller.get_available_credits("source-1").await, 50);
    }

    #[tokio::test]
    async fn test_per_source_backpressure() {
        let controller = BackpressureController::new(0.8, 0.6);

        controller.compute_signal("source-a", 0.9).await;
        controller.compute_signal("source-b", 0.5).await;

        assert_eq!(controller.get_current_signal("source-a").await, BackpressureSignal::Pause);
        assert_eq!(controller.get_current_signal("source-b").await, BackpressureSignal::None);
    }

    #[tokio::test]
    #[ignore = "Backpressure propagation requires gRPC integration"]
    async fn test_backpressure_propagation_to_source() {
        todo!("Implement backpressure propagation test")
    }

    #[tokio::test]
    async fn test_hysteresis_prevents_oscillation() {
        let controller = BackpressureController::new(0.8, 0.6);

        controller.compute_signal("source-1", 0.85).await;
        assert_eq!(controller.get_current_signal("source-1").await, BackpressureSignal::Pause);

        let signal = controller.compute_signal("source-1", 0.75).await;
        match signal {
            BackpressureSignal::SlowDown { .. } => {}
            _ => panic!("Expected SlowDown at 0.75, got {:?}", signal),
        }

        let signal = controller.compute_signal("source-1", 0.5).await;
        assert_eq!(signal, BackpressureSignal::None);
    }
}

#[cfg(test)]
mod buffer_persistence_tests {
    #[tokio::test]
    #[ignore = "Buffer persistence not implemented"]
    async fn test_buffer_survives_restart() {
        todo!("Implement buffer persistence test")
    }

    #[tokio::test]
    #[ignore = "Buffer persistence not implemented"]
    async fn test_buffer_replicated_to_followers() {
        todo!("Implement buffer replication test")
    }

    #[tokio::test]
    #[ignore = "Buffer persistence not implemented"]
    async fn test_buffer_recovered_after_leader_change() {
        todo!("Implement leader change buffer recovery test")
    }
}
