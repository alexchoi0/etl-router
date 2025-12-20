#[cfg(test)]
mod raft_ha_tests {
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    use crate::core::{RaftCore, RaftConfig, RaftRole};
    use crate::storage::LogStorage;
    use crate::testing::TestCluster;

    async fn create_single_node_core() -> (Arc<RaftCore>, TempDir) {
        let tmp_dir = TempDir::new().unwrap();
        let log = Arc::new(LogStorage::new(tmp_dir.path()).unwrap());

        let config = RaftConfig {
            election_timeout_min: Duration::from_millis(50),
            election_timeout_max: Duration::from_millis(100),
            heartbeat_interval: Duration::from_millis(25),
            max_entries_per_append: 100,
        };

        let core = RaftCore::new(1, vec![], log, config).await.unwrap();
        (Arc::new(core), tmp_dir)
    }

    #[tokio::test]
    async fn test_leader_election_single_node() {
        let (core, _tmp) = create_single_node_core().await;

        assert_eq!(core.role().await, RaftRole::Follower);

        core.become_candidate().await;

        assert_eq!(core.role().await, RaftRole::Leader);
        assert!(core.is_leader().await);
        assert_eq!(core.current_term().await, 1);
        assert_eq!(core.leader_id().await, Some(1));
    }

    #[tokio::test]
    async fn test_single_node_quorum() {
        let (core, _tmp) = create_single_node_core().await;
        assert_eq!(core.quorum_size(), 1);
    }

    #[tokio::test]
    async fn test_term_increments_on_election() {
        let (core, _tmp) = create_single_node_core().await;

        assert_eq!(core.current_term().await, 0);

        core.become_candidate().await;
        assert_eq!(core.current_term().await, 1);

        core.become_follower(1, None).await;
        core.become_candidate().await;
        assert_eq!(core.current_term().await, 2);
    }

    #[tokio::test]
    async fn test_leader_step_down_on_higher_term() {
        let (core, _tmp) = create_single_node_core().await;

        core.become_candidate().await;
        assert!(core.is_leader().await);
        assert_eq!(core.current_term().await, 1);

        core.become_follower(5, Some(2)).await;

        assert_eq!(core.role().await, RaftRole::Follower);
        assert_eq!(core.current_term().await, 5);
        assert_eq!(core.leader_id().await, Some(2));
    }

    #[tokio::test]
    async fn test_persistent_state_survives_restart() {
        let tmp_dir = TempDir::new().unwrap();

        {
            let log = Arc::new(LogStorage::new(tmp_dir.path()).unwrap());
            let config = RaftConfig::default();
            let core = RaftCore::new(1, vec![], log, config).await.unwrap();

            core.become_candidate().await;
            assert_eq!(core.current_term().await, 1);
        }

        {
            let log = Arc::new(LogStorage::new(tmp_dir.path()).unwrap());
            let config = RaftConfig::default();
            let core = RaftCore::new(1, vec![], log, config).await.unwrap();

            assert_eq!(core.current_term().await, 1);
        }
    }

    #[tokio::test]
    async fn test_leader_election_three_nodes() {
        let cluster = TestCluster::new(3).await.unwrap();

        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await;
        assert!(leader_id.is_some(), "Should elect a leader");

        let leader_count = cluster.count_leaders().await;
        assert_eq!(leader_count, 1, "Should have exactly one leader");

        cluster.send_heartbeats().await.ok();

        let leader = cluster.leader().await.unwrap();
        assert_eq!(leader.leader_id().await, Some(leader.id), "Leader knows it's the leader");
    }

    #[tokio::test]
    async fn test_leader_election_five_nodes() {
        let cluster = TestCluster::new(5).await.unwrap();

        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await;
        assert!(leader_id.is_some(), "Should elect a leader");

        let leader_count = cluster.count_leaders().await;
        assert_eq!(leader_count, 1, "Should have exactly one leader");

        let leader = cluster.leader().await.unwrap();
        assert_eq!(leader.quorum_size(), 3, "Quorum should be 3 for 5-node cluster");
    }

    #[tokio::test]
    async fn test_propose_as_leader() {
        use crate::commands::RouterCommand;

        let (core, _tmp) = create_single_node_core().await;

        core.become_candidate().await;
        assert!(core.is_leader().await);

        let cmd = RouterCommand::CommitSourceOffset {
            source_id: "test-source".to_string(),
            partition: 0,
            offset: 100,
        };

        let index = core.propose(cmd).await.unwrap();
        assert_eq!(index, 1);

        assert_eq!(core.log.last_index(), 1);
        assert_eq!(core.log.last_term(), 1);
    }

    #[tokio::test]
    async fn test_propose_fails_as_follower() {
        use crate::commands::RouterCommand;

        let (core, _tmp) = create_single_node_core().await;

        assert_eq!(core.role().await, RaftRole::Follower);

        let cmd = RouterCommand::Noop;
        let result = core.propose(cmd).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Not the leader");
    }

    #[tokio::test]
    async fn test_state_machine_applies_on_commit() {
        use crate::commands::RouterCommand;

        let (core, _tmp) = create_single_node_core().await;
        core.become_candidate().await;

        let cmd = RouterCommand::CommitSourceOffset {
            source_id: "test-source".to_string(),
            partition: 0,
            offset: 42,
        };
        core.propose(cmd).await.unwrap();

        core.volatile.write().await.commit_index = 1;
        core.apply_committed_entries_public().await;

        let state = core.state_machine.read().await;
        let offsets = state.get_source_offsets("test-source");
        assert_eq!(offsets.get(&0), Some(&42));
    }

    #[tokio::test]
    #[ignore = "Replication tests need async background loops - infra WIP"]
    async fn test_log_replication_to_followers() {
        use crate::commands::RouterCommand;

        let cluster = TestCluster::new(3).await.unwrap();

        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        let leader = cluster.node(leader_id).unwrap();

        let cmd = RouterCommand::CommitSourceOffset {
            source_id: "test-source".to_string(),
            partition: 0,
            offset: 100,
        };
        leader.propose(cmd).await.unwrap();

        for _ in 0..20 {
            cluster.replicate().await.ok();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        for node in &cluster.nodes {
            let entry = node.log.get(1).await;
            assert!(entry.is_some(), "Node {} should have log entry", node.id);
            assert_eq!(entry.unwrap().term, leader.current_term().await);
        }
    }

    #[tokio::test]
    #[ignore = "Replication tests need async background loops - infra WIP"]
    async fn test_commit_requires_majority() {
        use crate::commands::RouterCommand;

        let cluster = TestCluster::new(3).await.unwrap();

        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        let leader = cluster.node(leader_id).unwrap();

        let followers: Vec<_> = cluster.nodes.iter()
            .filter(|n| n.id != leader_id)
            .collect();
        let isolated_follower = followers[0].id;

        cluster.transport.partition_node(isolated_follower, &[leader_id]).await;

        let cmd = RouterCommand::CommitSourceOffset {
            source_id: "test-source".to_string(),
            partition: 0,
            offset: 200,
        };
        leader.propose(cmd).await.unwrap();

        for _ in 0..20 {
            cluster.replicate().await.ok();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let commit_index = leader.volatile.read().await.commit_index;
        assert_eq!(commit_index, 1, "Should commit with 2/3 majority");

        let connected_follower = followers[1];
        let follower_commit = connected_follower.volatile.read().await.commit_index;
        assert_eq!(follower_commit, 1, "Connected follower should have committed");
    }

    #[tokio::test]
    async fn test_commit_fails_without_majority() {
        use crate::commands::RouterCommand;

        let cluster = TestCluster::new(3).await.unwrap();

        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        let leader = cluster.node(leader_id).unwrap();

        let follower_ids: Vec<_> = cluster.nodes.iter()
            .filter(|n| n.id != leader_id)
            .map(|n| n.id)
            .collect();

        for &follower in &follower_ids {
            cluster.transport.partition_node(follower, &[leader_id]).await;
        }

        let cmd = RouterCommand::CommitSourceOffset {
            source_id: "test-source".to_string(),
            partition: 0,
            offset: 300,
        };
        leader.propose(cmd).await.unwrap();

        for _ in 0..10 {
            let _ = cluster.replicate().await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let commit_index = leader.volatile.read().await.commit_index;
        assert_eq!(commit_index, 0, "Should not commit without majority");
    }

    #[tokio::test]
    #[ignore = "Replication tests need async background loops - infra WIP"]
    async fn test_follower_catches_up_after_partition_heals() {
        use crate::commands::RouterCommand;

        let cluster = TestCluster::new(3).await.unwrap();

        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        let leader = cluster.node(leader_id).unwrap();

        let isolated_follower_id = cluster.nodes.iter()
            .find(|n| n.id != leader_id)
            .unwrap().id;

        cluster.transport.partition_node(isolated_follower_id, &[leader_id]).await;

        for i in 1..=3 {
            let cmd = RouterCommand::CommitSourceOffset {
                source_id: "test-source".to_string(),
                partition: 0,
                offset: i * 100,
            };
            leader.propose(cmd).await.unwrap();
        }

        for _ in 0..20 {
            cluster.replicate().await.ok();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        cluster.transport.heal_all_partitions().await;

        for _ in 0..30 {
            cluster.replicate().await.ok();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let isolated_follower = cluster.node(isolated_follower_id).unwrap();
        let follower_last_index = isolated_follower.log.last_index();
        assert_eq!(follower_last_index, 3, "Follower should catch up to index 3");
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need log conflict resolution"]
    async fn test_log_conflict_resolution() {
        // Conflicting logs should be resolved in favor of leader
        // - Create scenario where follower has divergent log
        // - Follower reconnects to leader
        // - Verify follower's divergent entries are truncated
        // - Verify follower receives leader's entries
        todo!("Implement log conflict resolution")
    }

    #[tokio::test]
    async fn test_leader_crash_triggers_new_election() {
        let cluster = TestCluster::new(3).await.unwrap();

        let old_leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();

        cluster.transport.unregister_node(old_leader_id).await;
        let other_ids: Vec<_> = cluster.nodes.iter()
            .filter(|n| n.id != old_leader_id)
            .map(|n| n.id)
            .collect();
        for &id in &other_ids {
            cluster.transport.partition_node(old_leader_id, &[id]).await;
        }

        for node in &cluster.nodes {
            if node.id != old_leader_id {
                node.become_follower(node.current_term().await, None).await;
            }
        }

        let mut new_leader_id = None;
        for _ in 0..10 {
            for node in &cluster.nodes {
                if node.id != old_leader_id && !node.is_leader().await {
                    cluster.trigger_election(node.id).await.ok();
                    if node.is_leader().await {
                        new_leader_id = Some(node.id);
                        break;
                    }
                }
            }
            if new_leader_id.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(new_leader_id.is_some(), "Should elect new leader");
        assert_ne!(new_leader_id.unwrap(), old_leader_id, "New leader should be different");
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need crash recovery"]
    async fn test_node_recovers_state_from_disk() {
        // Node should recover state after restart
        // - Start node, commit entries
        // - Stop node
        // - Restart node
        // - Verify all committed entries are present
        // - Verify state machine has correct state
        todo!("Implement crash recovery")
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need crash recovery"]
    async fn test_uncommitted_entries_replayed_on_recovery() {
        // Uncommitted entries should be available for commit after recovery
        // - Start node, append entries but don't commit
        // - Restart node
        // - Verify entries still in log
        // - Complete commit process
        todo!("Implement uncommitted entry recovery")
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need network transport"]
    async fn test_append_entries_rpc() {
        // AppendEntries RPC should work correctly
        // - Send AppendEntries from leader to follower
        // - Verify follower appends entries
        // - Verify follower responds with success
        todo!("Implement AppendEntries RPC")
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need network transport"]
    async fn test_request_vote_rpc() {
        // RequestVote RPC should work correctly
        // - Send RequestVote from candidate
        // - Verify node grants vote if candidate's log is up-to-date
        // - Verify node denies vote if already voted this term
        todo!("Implement RequestVote RPC")
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need heartbeat mechanism"]
    async fn test_heartbeat_prevents_election() {
        // Regular heartbeats should prevent followers from starting election
        // - Start cluster with leader
        // - Verify no new elections while heartbeats arrive
        // - Stop heartbeats
        // - Verify election starts after timeout
        todo!("Implement heartbeat mechanism")
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need snapshot support"]
    async fn test_snapshot_creation() {
        // Snapshot should capture current state machine state
        // - Commit many entries
        // - Trigger snapshot
        // - Verify snapshot contains correct state
        // - Verify old log entries can be compacted
        todo!("Implement snapshot creation")
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need snapshot support"]
    async fn test_snapshot_installation_on_slow_follower() {
        // Very behind follower should receive snapshot instead of log entries
        // - Start cluster, commit many entries
        // - Add new node (or slow follower)
        // - Verify leader sends snapshot
        // - Verify follower installs snapshot and catches up
        todo!("Implement snapshot installation")
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need membership changes"]
    async fn test_add_node_to_cluster() {
        // Adding a node should be done safely
        // - Start 3-node cluster
        // - Add node 4
        // - Verify node 4 receives log and participates in consensus
        // - Verify quorum requirements updated
        todo!("Implement membership changes")
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need membership changes"]
    async fn test_remove_node_from_cluster() {
        // Removing a node should be done safely
        // - Start 5-node cluster
        // - Remove node 5
        // - Verify cluster continues with 4 nodes
        // - Verify quorum requirements updated
        todo!("Implement membership changes")
    }

    #[tokio::test]
    #[ignore = "Replication tests need async background loops - infra WIP"]
    async fn test_network_partition_minority_cannot_commit() {
        use crate::commands::RouterCommand;

        let cluster = TestCluster::new(5).await.unwrap();

        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        let leader = cluster.node(leader_id).unwrap();

        let other_ids: Vec<_> = cluster.nodes.iter()
            .filter(|n| n.id != leader_id)
            .map(|n| n.id)
            .collect();

        for &id in &other_ids[..3] {
            cluster.transport.partition_node(leader_id, &[id]).await;
        }

        let cmd = RouterCommand::CommitSourceOffset {
            source_id: "test".to_string(),
            partition: 0,
            offset: 100,
        };
        leader.propose(cmd).await.unwrap();

        for _ in 0..20 {
            let _ = cluster.replicate().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let commit_index = leader.volatile.read().await.commit_index;
        assert_eq!(commit_index, 0, "Minority (2 nodes) cannot commit");
    }

    #[tokio::test]
    #[ignore = "Replication tests need async background loops - infra WIP"]
    async fn test_network_partition_heals_state_converges() {
        use crate::commands::RouterCommand;

        let cluster = TestCluster::new(5).await.unwrap();

        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        let leader = cluster.node(leader_id).unwrap();

        let other_ids: Vec<_> = cluster.nodes.iter()
            .filter(|n| n.id != leader_id)
            .map(|n| n.id)
            .collect();

        let minority = &other_ids[..2];
        for &id in minority {
            cluster.transport.partition_node(id, &[leader_id]).await;
        }

        let cmd = RouterCommand::CommitSourceOffset {
            source_id: "test".to_string(),
            partition: 0,
            offset: 500,
        };
        leader.propose(cmd).await.unwrap();

        for _ in 0..20 {
            cluster.replicate().await.ok();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        cluster.transport.heal_all_partitions().await;

        for _ in 0..30 {
            cluster.replicate().await.ok();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        for &id in minority {
            let node = cluster.node(id).unwrap();
            let last_index = node.log.last_index();
            assert_eq!(last_index, 1, "Node {} should have caught up", id);
        }
    }

    #[tokio::test]
    async fn test_no_split_brain() {
        let cluster = TestCluster::new(5).await.unwrap();

        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        let leader_term = cluster.node(leader_id).unwrap().current_term().await;

        let leader_count = cluster.count_leaders().await;
        assert_eq!(leader_count, 1, "Should have exactly one leader");

        for node in &cluster.nodes {
            if node.is_leader().await {
                assert_eq!(node.current_term().await, leader_term,
                    "Leader should be in the expected term");
            }
        }

        assert!(cluster.verify_log_consistency().await, "Logs should be consistent");
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need read consistency"]
    async fn test_linearizable_reads() {
        // Reads should be linearizable
        // - Commit entry
        // - Read from leader
        // - Verify read reflects committed state
        // - Handle leader change during read
        todo!("Implement linearizable reads")
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need read consistency"]
    async fn test_stale_leader_read_rejected() {
        // Stale leader should not serve reads
        // - Partition leader from majority
        // - New leader elected
        // - Read from old leader should fail or redirect
        todo!("Implement stale read prevention")
    }
}
