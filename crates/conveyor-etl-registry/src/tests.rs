#[cfg(test)]
mod service_registry_tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::RwLock;

    use crate::{ServiceRegistry, ServiceType, ServiceHealth};
    use conveyor_etl_raft::{RaftNode, RaftCore, RaftConfig, LogStorage};

    async fn create_test_registry() -> (ServiceRegistry, TempDir) {
        let tmp_dir = TempDir::new().unwrap();
        let log = Arc::new(LogStorage::new(tmp_dir.path()).unwrap());
        let config = RaftConfig {
            election_timeout_min: Duration::from_millis(50),
            election_timeout_max: Duration::from_millis(100),
            heartbeat_interval: Duration::from_millis(25),
            max_entries_per_append: 100,
        };
        let core = Arc::new(RaftCore::new(1, vec![], log, config).await.unwrap());
        let raft_node = RaftNode::from_core(1, core);
        let registry = ServiceRegistry::new(Arc::new(RwLock::new(raft_node)));
        (registry, tmp_dir)
    }

    #[tokio::test]
    async fn test_register_service() {
        let (registry, _tmp) = create_test_registry().await;

        let result = registry.register(
            "source-1".to_string(),
            "my-source".to_string(),
            ServiceType::Source,
            "localhost:8080".to_string(),
            HashMap::new(),
            None,
        ).await;

        assert!(result.is_ok());

        let service = registry.get_service("source-1").await;
        assert!(service.is_some());
        let svc = service.unwrap();
        assert_eq!(svc.service_id, "source-1");
        assert_eq!(svc.service_name, "my-source");
        assert_eq!(svc.service_type, ServiceType::Source);
        assert_eq!(svc.endpoint, "localhost:8080");
        assert_eq!(svc.health, ServiceHealth::Healthy);
    }

    #[tokio::test]
    async fn test_register_duplicate_service_fails() {
        let (registry, _tmp) = create_test_registry().await;

        let result1 = registry.register(
            "source-1".to_string(),
            "my-source".to_string(),
            ServiceType::Source,
            "localhost:8080".to_string(),
            HashMap::new(),
            None,
        ).await;
        assert!(result1.is_ok());

        let result2 = registry.register(
            "source-1".to_string(),
            "my-source-2".to_string(),
            ServiceType::Source,
            "localhost:8081".to_string(),
            HashMap::new(),
            None,
        ).await;

        assert!(result2.is_ok());

        let service = registry.get_service("source-1").await;
        assert!(service.is_some());
        assert_eq!(service.unwrap().service_name, "my-source-2");
    }

    #[tokio::test]
    async fn test_deregister_service() {
        let (registry, _tmp) = create_test_registry().await;

        registry.register(
            "source-1".to_string(),
            "my-source".to_string(),
            ServiceType::Source,
            "localhost:8080".to_string(),
            HashMap::new(),
            None,
        ).await.unwrap();

        assert!(registry.get_service("source-1").await.is_some());

        registry.deregister("source-1").await.unwrap();

        assert!(registry.get_service("source-1").await.is_none());
    }

    #[tokio::test]
    async fn test_heartbeat_updates_last_seen() {
        let (registry, _tmp) = create_test_registry().await;

        registry.register(
            "source-1".to_string(),
            "my-source".to_string(),
            ServiceType::Source,
            "localhost:8080".to_string(),
            HashMap::new(),
            None,
        ).await.unwrap();

        let before = registry.get_service("source-1").await.unwrap();
        let before_heartbeat = before.last_heartbeat;

        tokio::time::sleep(Duration::from_millis(10)).await;

        let result = registry.heartbeat("source-1").await;
        assert!(result.is_ok());

        let after = registry.get_service("source-1").await.unwrap();
        let after_heartbeat = after.last_heartbeat;

        assert!(after_heartbeat.unwrap() > before_heartbeat.unwrap());
    }

    #[tokio::test]
    async fn test_service_expires_without_heartbeat() {
        let (registry, _tmp) = create_test_registry().await;

        registry.register(
            "source-1".to_string(),
            "my-source".to_string(),
            ServiceType::Source,
            "localhost:8080".to_string(),
            HashMap::new(),
            None,
        ).await.unwrap();

        let service = registry.get_service("source-1").await.unwrap();
        assert!(!service.is_lease_expired());
    }

    #[tokio::test]
    async fn test_list_services_by_type() {
        let (registry, _tmp) = create_test_registry().await;

        registry.register(
            "source-1".to_string(),
            "sources".to_string(),
            ServiceType::Source,
            "localhost:8080".to_string(),
            HashMap::new(),
            None,
        ).await.unwrap();

        registry.register(
            "source-2".to_string(),
            "sources".to_string(),
            ServiceType::Source,
            "localhost:8081".to_string(),
            HashMap::new(),
            None,
        ).await.unwrap();

        registry.register(
            "transform-1".to_string(),
            "transforms".to_string(),
            ServiceType::Transform,
            "localhost:8082".to_string(),
            HashMap::new(),
            None,
        ).await.unwrap();

        registry.register(
            "sink-1".to_string(),
            "sinks".to_string(),
            ServiceType::Sink,
            "localhost:8083".to_string(),
            HashMap::new(),
            None,
        ).await.unwrap();

        let all = registry.list_all().await;
        assert_eq!(all.len(), 4);

        let sources = registry.get_services_by_name("sources").await;
        assert_eq!(sources.len(), 2);
        for s in &sources {
            assert_eq!(s.service_type, ServiceType::Source);
        }
    }

    #[tokio::test]
    async fn test_list_services_by_labels() {
        let (registry, _tmp) = create_test_registry().await;

        let mut labels1 = HashMap::new();
        labels1.insert("env".to_string(), "production".to_string());
        labels1.insert("region".to_string(), "us-east".to_string());

        let mut labels2 = HashMap::new();
        labels2.insert("env".to_string(), "production".to_string());
        labels2.insert("region".to_string(), "us-west".to_string());

        let mut labels3 = HashMap::new();
        labels3.insert("env".to_string(), "staging".to_string());

        registry.register(
            "source-1".to_string(),
            "my-source".to_string(),
            ServiceType::Source,
            "localhost:8080".to_string(),
            labels1,
            None,
        ).await.unwrap();

        registry.register(
            "source-2".to_string(),
            "my-source".to_string(),
            ServiceType::Source,
            "localhost:8081".to_string(),
            labels2,
            None,
        ).await.unwrap();

        registry.register(
            "source-3".to_string(),
            "my-source".to_string(),
            ServiceType::Source,
            "localhost:8082".to_string(),
            labels3,
            None,
        ).await.unwrap();

        let mut query = HashMap::new();
        query.insert("env".to_string(), "production".to_string());
        let production = registry.get_healthy_services_by_labels(&query).await;
        assert_eq!(production.len(), 2);

        let mut query2 = HashMap::new();
        query2.insert("env".to_string(), "production".to_string());
        query2.insert("region".to_string(), "us-east".to_string());
        let us_east = registry.get_healthy_services_by_labels(&query2).await;
        assert_eq!(us_east.len(), 1);
        assert_eq!(us_east[0].service_id, "source-1");
    }

    #[tokio::test]
    async fn test_watch_services_receives_events() {
        use crate::ServiceEvent;

        let (registry, _tmp) = create_test_registry().await;

        let mut receiver = registry.subscribe();

        registry.register(
            "source-1".to_string(),
            "my-source".to_string(),
            ServiceType::Source,
            "localhost:8080".to_string(),
            HashMap::new(),
            None,
        ).await.unwrap();

        let event = receiver.try_recv().unwrap();
        match event {
            ServiceEvent::Registered { service_id, service_name, endpoint, .. } => {
                assert_eq!(service_id, "source-1");
                assert_eq!(service_name, "my-source");
                assert_eq!(endpoint, "localhost:8080");
            }
            _ => panic!("Expected Registered event"),
        }

        registry.update_health("source-1", ServiceHealth::Unhealthy).await.unwrap();
        let event = receiver.try_recv().unwrap();
        match event {
            ServiceEvent::HealthChanged { service_id, old_health, new_health } => {
                assert_eq!(service_id, "source-1");
                assert_eq!(old_health, ServiceHealth::Healthy);
                assert_eq!(new_health, ServiceHealth::Unhealthy);
            }
            _ => panic!("Expected HealthChanged event"),
        }

        registry.update_endpoint("source-1", "localhost:9090".to_string()).await.unwrap();
        let event = receiver.try_recv().unwrap();
        match event {
            ServiceEvent::EndpointChanged { service_id, old_endpoint, new_endpoint } => {
                assert_eq!(service_id, "source-1");
                assert_eq!(old_endpoint, "localhost:8080");
                assert_eq!(new_endpoint, "localhost:9090");
            }
            _ => panic!("Expected EndpointChanged event"),
        }

        registry.add_label("source-1", "env".to_string(), "prod".to_string()).await.unwrap();
        let event = receiver.try_recv().unwrap();
        match event {
            ServiceEvent::LabelsUpdated { service_id } => {
                assert_eq!(service_id, "source-1");
            }
            _ => panic!("Expected LabelsUpdated event"),
        }

        registry.deregister("source-1").await.unwrap();
        let event = receiver.try_recv().unwrap();
        match event {
            ServiceEvent::Deregistered { service_id } => {
                assert_eq!(service_id, "source-1");
            }
            _ => panic!("Expected Deregistered event"),
        }
    }

    #[tokio::test]
    async fn test_service_health_transitions() {
        let (registry, _tmp) = create_test_registry().await;

        registry.register(
            "source-1".to_string(),
            "my-source".to_string(),
            ServiceType::Source,
            "localhost:8080".to_string(),
            HashMap::new(),
            None,
        ).await.unwrap();

        let service = registry.get_service("source-1").await.unwrap();
        assert_eq!(service.health, ServiceHealth::Healthy);

        registry.update_health("source-1", ServiceHealth::Unhealthy).await.unwrap();
        let service = registry.get_service("source-1").await.unwrap();
        assert_eq!(service.health, ServiceHealth::Unhealthy);

        registry.update_health("source-1", ServiceHealth::Healthy).await.unwrap();
        let service = registry.get_service("source-1").await.unwrap();
        assert_eq!(service.health, ServiceHealth::Healthy);
    }

    #[tokio::test]
    async fn test_service_metadata_update() {
        let (registry, _tmp) = create_test_registry().await;

        let mut initial_labels = HashMap::new();
        initial_labels.insert("env".to_string(), "staging".to_string());

        registry.register(
            "source-1".to_string(),
            "my-source".to_string(),
            ServiceType::Source,
            "localhost:8080".to_string(),
            initial_labels,
            None,
        ).await.unwrap();

        let service = registry.get_service("source-1").await.unwrap();
        assert_eq!(service.labels.get("env"), Some(&"staging".to_string()));
        assert_eq!(service.endpoint, "localhost:8080");

        let mut new_labels = HashMap::new();
        new_labels.insert("env".to_string(), "production".to_string());
        new_labels.insert("region".to_string(), "us-east".to_string());
        registry.update_labels("source-1", new_labels).await.unwrap();

        let service = registry.get_service("source-1").await.unwrap();
        assert_eq!(service.labels.get("env"), Some(&"production".to_string()));
        assert_eq!(service.labels.get("region"), Some(&"us-east".to_string()));

        registry.update_endpoint("source-1", "localhost:9090".to_string()).await.unwrap();
        let service = registry.get_service("source-1").await.unwrap();
        assert_eq!(service.endpoint, "localhost:9090");

        registry.add_label("source-1", "version".to_string(), "2.0".to_string()).await.unwrap();
        let service = registry.get_service("source-1").await.unwrap();
        assert_eq!(service.labels.get("version"), Some(&"2.0".to_string()));

        let removed = registry.remove_label("source-1", "region").await.unwrap();
        assert_eq!(removed, Some("us-east".to_string()));

        let service = registry.get_service("source-1").await.unwrap();
        assert!(service.labels.get("region").is_none());
    }
}

#[cfg(test)]
mod group_coordinator_tests {
    use crate::GroupCoordinator;

    #[tokio::test]
    async fn test_create_consumer_group() {
        let coordinator = GroupCoordinator::new();

        let result = coordinator.create_group(
            "group-1".to_string(),
            "stage-1".to_string(),
            4,
        ).await;

        assert!(result.is_ok());

        let group = coordinator.get_group("group-1").await;
        assert!(group.is_some());
        let g = group.unwrap();
        assert_eq!(g.group_id, "group-1");
        assert_eq!(g.stage_id, "stage-1");
        assert_eq!(g.total_partitions, 4);
        assert_eq!(g.generation, 0);
    }

    #[tokio::test]
    async fn test_join_group_single_member() {
        let coordinator = GroupCoordinator::new();

        coordinator.create_group(
            "group-1".to_string(),
            "stage-1".to_string(),
            4,
        ).await.unwrap();

        let events = coordinator.join_group("group-1", "service-a".to_string()).await.unwrap();

        assert!(!events.is_empty());

        let assignment = coordinator.get_assignment("group-1", "service-a").await;
        assert!(assignment.is_some());
        let partitions = assignment.unwrap();
        assert_eq!(partitions.len(), 4);
        assert!(partitions.contains(&0));
        assert!(partitions.contains(&1));
        assert!(partitions.contains(&2));
        assert!(partitions.contains(&3));
    }

    #[tokio::test]
    async fn test_join_group_triggers_rebalance() {
        let coordinator = GroupCoordinator::new();

        coordinator.create_group(
            "group-1".to_string(),
            "stage-1".to_string(),
            4,
        ).await.unwrap();

        coordinator.join_group("group-1", "service-a".to_string()).await.unwrap();

        let assignment_a = coordinator.get_assignment("group-1", "service-a").await.unwrap();
        assert_eq!(assignment_a.len(), 4);

        let events = coordinator.join_group("group-1", "service-b".to_string()).await.unwrap();
        assert!(!events.is_empty());

        let assignment_a = coordinator.get_assignment("group-1", "service-a").await.unwrap();
        let assignment_b = coordinator.get_assignment("group-1", "service-b").await.unwrap();

        assert_eq!(assignment_a.len(), 2);
        assert_eq!(assignment_b.len(), 2);

        let mut all_partitions: Vec<u32> = assignment_a.iter().chain(assignment_b.iter()).copied().collect();
        all_partitions.sort();
        assert_eq!(all_partitions, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn test_leave_group_triggers_rebalance() {
        let coordinator = GroupCoordinator::new();

        coordinator.create_group(
            "group-1".to_string(),
            "stage-1".to_string(),
            4,
        ).await.unwrap();

        coordinator.join_group("group-1", "service-a".to_string()).await.unwrap();
        coordinator.join_group("group-1", "service-b".to_string()).await.unwrap();

        let events = coordinator.leave_group("group-1", "service-b").await.unwrap();
        assert!(!events.is_empty());

        let assignment_a = coordinator.get_assignment("group-1", "service-a").await.unwrap();
        assert_eq!(assignment_a.len(), 4);

        let assignment_b = coordinator.get_assignment("group-1", "service-b").await;
        assert!(assignment_b.is_none());
    }

    #[tokio::test]
    async fn test_member_timeout_triggers_rebalance() {
        use std::time::Duration;

        let coordinator = GroupCoordinator::new();

        coordinator.create_group(
            "group-1".to_string(),
            "stage-1".to_string(),
            4,
        ).await.unwrap();

        coordinator.join_group("group-1", "service-a".to_string()).await.unwrap();
        coordinator.join_group("group-1", "service-b".to_string()).await.unwrap();

        let assignment_a = coordinator.get_assignment("group-1", "service-a").await.unwrap();
        let assignment_b = coordinator.get_assignment("group-1", "service-b").await.unwrap();
        assert_eq!(assignment_a.len(), 2);
        assert_eq!(assignment_b.len(), 2);

        tokio::time::sleep(Duration::from_millis(50)).await;

        coordinator.heartbeat("group-1", "service-a").await.unwrap();

        let events = coordinator.check_member_timeouts("group-1", Duration::from_millis(25)).await.unwrap();
        assert!(!events.is_empty());

        let assignment_a = coordinator.get_assignment("group-1", "service-a").await.unwrap();
        assert_eq!(assignment_a.len(), 4);

        let assignment_b = coordinator.get_assignment("group-1", "service-b").await;
        assert!(assignment_b.is_none());

        let stale_members = coordinator.get_members_needing_heartbeat("group-1", Duration::from_secs(60));
        assert!(stale_members.is_empty());
    }

    #[tokio::test]
    #[ignore = "Sticky assignment not yet implemented"]
    async fn test_sticky_partition_assignment() {
        todo!("Implement sticky assignment")
    }

    #[tokio::test]
    async fn test_range_assignment_strategy() {
        let coordinator = GroupCoordinator::new();

        coordinator.create_group(
            "group-1".to_string(),
            "stage-1".to_string(),
            10,
        ).await.unwrap();

        coordinator.join_group("group-1", "service-a".to_string()).await.unwrap();
        coordinator.join_group("group-1", "service-b".to_string()).await.unwrap();
        coordinator.join_group("group-1", "service-c".to_string()).await.unwrap();

        let assignment_a = coordinator.get_assignment("group-1", "service-a").await.unwrap();
        let assignment_b = coordinator.get_assignment("group-1", "service-b").await.unwrap();
        let assignment_c = coordinator.get_assignment("group-1", "service-c").await.unwrap();

        let mut all: Vec<u32> = assignment_a.iter()
            .chain(assignment_b.iter())
            .chain(assignment_c.iter())
            .copied()
            .collect();
        all.sort();
        assert_eq!(all, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        assert!(assignment_a.len() >= 3);
        assert!(assignment_b.len() >= 3);
        assert!(assignment_c.len() >= 3);
    }

    #[tokio::test]
    async fn test_round_robin_assignment_strategy() {
        let coordinator = GroupCoordinator::new();

        coordinator.create_group(
            "group-1".to_string(),
            "stage-1".to_string(),
            9,
        ).await.unwrap();

        coordinator.join_group("group-1", "service-a".to_string()).await.unwrap();
        coordinator.join_group("group-1", "service-b".to_string()).await.unwrap();
        coordinator.join_group("group-1", "service-c".to_string()).await.unwrap();

        let assignment_a = coordinator.get_assignment("group-1", "service-a").await.unwrap();
        let assignment_b = coordinator.get_assignment("group-1", "service-b").await.unwrap();
        let assignment_c = coordinator.get_assignment("group-1", "service-c").await.unwrap();

        assert_eq!(assignment_a.len(), 3);
        assert_eq!(assignment_b.len(), 3);
        assert_eq!(assignment_c.len(), 3);
    }

    #[tokio::test]
    async fn test_rebalance_callback_invoked() {
        use crate::group_coordinator::RebalanceEvent;

        let coordinator = GroupCoordinator::new();

        coordinator.create_group(
            "group-1".to_string(),
            "stage-1".to_string(),
            4,
        ).await.unwrap();

        let events = coordinator.join_group("group-1", "service-a".to_string()).await.unwrap();

        let has_assigned = events.iter().any(|e| matches!(e, RebalanceEvent::PartitionsAssigned { .. }));
        assert!(has_assigned);
    }

    #[tokio::test]
    async fn test_group_generation_increments() {
        let coordinator = GroupCoordinator::new();

        coordinator.create_group(
            "group-1".to_string(),
            "stage-1".to_string(),
            4,
        ).await.unwrap();

        let group = coordinator.get_group("group-1").await.unwrap();
        assert_eq!(group.generation, 0);

        coordinator.join_group("group-1", "service-a".to_string()).await.unwrap();
        let group = coordinator.get_group("group-1").await.unwrap();
        assert_eq!(group.generation, 1);

        coordinator.join_group("group-1", "service-b".to_string()).await.unwrap();
        let group = coordinator.get_group("group-1").await.unwrap();
        assert_eq!(group.generation, 2);
    }

    #[tokio::test]
    async fn test_stale_generation_rejected() {
        let coordinator = GroupCoordinator::new();

        coordinator.create_group(
            "group-1".to_string(),
            "stage-1".to_string(),
            4,
        ).await.unwrap();

        coordinator.join_group("group-1", "service-a".to_string()).await.unwrap();
        let gen1 = coordinator.get_current_generation("group-1").await.unwrap();
        assert_eq!(gen1, 1);

        coordinator.join_group("group-1", "service-b".to_string()).await.unwrap();
        let gen2 = coordinator.get_current_generation("group-1").await.unwrap();
        assert_eq!(gen2, 2);

        let stale_heartbeat = coordinator.heartbeat_with_generation("group-1", "service-a", gen1).await;
        assert!(stale_heartbeat.is_err());
        assert!(stale_heartbeat.unwrap_err().to_string().contains("Stale generation"));

        let valid_heartbeat = coordinator.heartbeat_with_generation("group-1", "service-a", gen2).await;
        assert!(valid_heartbeat.is_ok());

        let is_valid = coordinator.validate_generation("group-1", gen2).await.unwrap();
        assert!(is_valid);

        let is_stale = coordinator.validate_generation("group-1", gen1).await.unwrap();
        assert!(!is_stale);

        let stale_leave = coordinator.leave_group_with_generation("group-1", "service-a", gen1).await;
        assert!(stale_leave.is_err());

        let valid_leave = coordinator.leave_group_with_generation("group-1", "service-b", gen2).await;
        assert!(valid_leave.is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_joins_handled() {
        let coordinator = std::sync::Arc::new(GroupCoordinator::new());

        coordinator.create_group(
            "group-1".to_string(),
            "stage-1".to_string(),
            12,
        ).await.unwrap();

        let mut handles = vec![];
        for i in 0..4 {
            let coord = coordinator.clone();
            let handle = tokio::spawn(async move {
                coord.join_group("group-1", format!("service-{}", i)).await
            });
            handles.push(handle);
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        let group = coordinator.get_group("group-1").await.unwrap();
        assert_eq!(group.members.len(), 4);

        let mut all_partitions = std::collections::HashSet::new();
        for i in 0..4 {
            let assignment = coordinator.get_assignment("group-1", &format!("service-{}", i)).await.unwrap();
            for p in assignment {
                assert!(!all_partitions.contains(&p), "Partition {} assigned multiple times", p);
                all_partitions.insert(p);
            }
        }
        assert_eq!(all_partitions.len(), 12);
    }

    #[tokio::test]
    #[ignore = "Raft persistence requires full cluster setup"]
    async fn test_group_coordinator_persisted_in_raft() {
        todo!("Implement Raft persistence for groups")
    }
}

#[cfg(test)]
mod load_balancer_tests {
    use std::collections::HashMap;

    use crate::{LoadBalancer, RegisteredService, ServiceType, ServiceHealth};
    use crate::load_balancer::LoadBalanceStrategy;

    fn create_test_services(count: usize) -> Vec<RegisteredService> {
        (0..count).map(|i| RegisteredService {
            service_id: format!("service-{}", i),
            service_name: "test-service".to_string(),
            service_type: ServiceType::Transform,
            endpoint: format!("localhost:{}", 8080 + i),
            labels: HashMap::new(),
            health: ServiceHealth::Healthy,
            group_id: None,
            registered_at: None,
            last_heartbeat: None,
            lease_duration: std::time::Duration::from_secs(30),
        }).collect()
    }

    #[tokio::test]
    async fn test_round_robin_distribution() {
        let lb = LoadBalancer::new();
        let services = create_test_services(3);

        let mut selections = HashMap::new();
        for _ in 0..6 {
            let selected = lb.select(&services, LoadBalanceStrategy::RoundRobin, None).await.unwrap();
            *selections.entry(selected.service_id).or_insert(0) += 1;
        }

        assert_eq!(selections.get("service-0"), Some(&2));
        assert_eq!(selections.get("service-1"), Some(&2));
        assert_eq!(selections.get("service-2"), Some(&2));
    }

    #[tokio::test]
    async fn test_least_connections() {
        let lb = LoadBalancer::new();
        let services = create_test_services(3);

        lb.increment_connections("service-0");
        lb.increment_connections("service-0");
        lb.increment_connections("service-0");
        lb.increment_connections("service-0");
        lb.increment_connections("service-0");

        lb.increment_connections("service-1");
        lb.increment_connections("service-1");

        lb.increment_connections("service-2");
        lb.increment_connections("service-2");
        lb.increment_connections("service-2");
        lb.increment_connections("service-2");
        lb.increment_connections("service-2");
        lb.increment_connections("service-2");
        lb.increment_connections("service-2");
        lb.increment_connections("service-2");

        let selected = lb.select(&services, LoadBalanceStrategy::LeastConnections, None).await.unwrap();
        assert_eq!(selected.service_id, "service-1");
    }

    #[tokio::test]
    async fn test_weighted_random() {
        let lb = LoadBalancer::new();
        let services = create_test_services(2);

        lb.set_weight("service-0", 100);
        lb.set_weight("service-1", 1);

        let mut service_0_count = 0;
        let mut service_1_count = 0;

        for _ in 0..1000 {
            let selected = lb.select(&services, LoadBalanceStrategy::WeightedRandom, None).await.unwrap();
            if selected.service_id == "service-0" {
                service_0_count += 1;
            } else {
                service_1_count += 1;
            }
        }

        assert!(service_0_count > service_1_count * 5, "service-0 should be selected much more often");
    }

    #[tokio::test]
    async fn test_consistent_hash() {
        let lb = LoadBalancer::new();
        let services = create_test_services(5);

        let mut first_selection = None;
        for _ in 0..10 {
            let selected = lb.select(&services, LoadBalanceStrategy::ConsistentHash, Some("user-123")).await.unwrap();
            if first_selection.is_none() {
                first_selection = Some(selected.service_id.clone());
            }
            assert_eq!(selected.service_id, first_selection.as_ref().unwrap().as_str());
        }

        let different_key = lb.select(&services, LoadBalanceStrategy::ConsistentHash, Some("user-456")).await.unwrap();
        assert!(different_key.service_id.starts_with("service-"));
    }

    #[tokio::test]
    #[ignore = "Minimal redistribution test requires hash ring implementation"]
    async fn test_consistent_hash_minimal_redistribution() {
        todo!("Implement consistent hash redistribution test")
    }

    #[tokio::test]
    async fn test_unhealthy_service_excluded() {
        let lb = LoadBalancer::new();
        let mut services = create_test_services(3);

        services[1].health = ServiceHealth::Unhealthy;

        let healthy_services: Vec<_> = services.iter()
            .filter(|s| s.health == ServiceHealth::Healthy)
            .cloned()
            .collect();

        for _ in 0..10 {
            let selected = lb.select(&healthy_services, LoadBalanceStrategy::RoundRobin, None).await.unwrap();
            assert_ne!(selected.service_id, "service-1");
        }
    }

    #[tokio::test]
    async fn test_all_services_unhealthy() {
        let lb = LoadBalancer::new();
        let mut services = create_test_services(3);

        for s in &mut services {
            s.health = ServiceHealth::Unhealthy;
        }

        let healthy_services: Vec<_> = services.iter()
            .filter(|s| s.health == ServiceHealth::Healthy)
            .cloned()
            .collect();

        let result = lb.select(&healthy_services, LoadBalanceStrategy::RoundRobin, None).await;
        assert!(result.is_none());
    }
}
