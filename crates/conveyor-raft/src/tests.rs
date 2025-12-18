#[cfg(test)]
mod raft_ha_tests {
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    use crate::core::{RaftCore, RaftConfig, RaftRole};
    use crate::storage::LogStorage;

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
    #[ignore = "Requires multi-node setup with network"]
    async fn test_leader_election_three_nodes() {
        // Three nodes should elect exactly one leader
        // - Start nodes 1, 2, 3
        // - Wait for election
        // - Verify exactly one is leader
        // - Verify all agree on who the leader is
        todo!("Implement multi-node leader election test")
    }

    #[tokio::test]
    #[ignore = "Requires multi-node setup with network"]
    async fn test_leader_election_five_nodes() {
        // Five node cluster for stronger quorum guarantees
        // - Start nodes 1-5
        // - Verify one leader elected
        // - Verify leader has majority support
        todo!("Implement multi-node leader election test")
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
    #[ignore = "HA not implemented: need log replication"]
    async fn test_log_replication_to_followers() {
        // Leader should replicate entries to all followers
        // - Start 3-node cluster
        // - Propose command to leader
        // - Verify all nodes have the entry in their logs
        todo!("Implement log replication")
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need log replication"]
    async fn test_commit_requires_majority() {
        // Entry should only commit when majority acknowledges
        // - Start 3-node cluster
        // - Partition one follower
        // - Propose command
        // - Verify commit succeeds (2/3 = majority)
        // - Verify state machine applied on leader and one follower
        todo!("Implement majority commit")
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need log replication"]
    async fn test_commit_fails_without_majority() {
        // Entry should not commit without majority
        // - Start 3-node cluster
        // - Partition two followers
        // - Propose command
        // - Verify commit times out (only 1/3)
        todo!("Implement majority commit")
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need log replication"]
    async fn test_follower_catches_up_after_partition_heals() {
        // Follower should catch up after rejoining
        // - Start 3-node cluster
        // - Partition follower
        // - Commit several entries
        // - Heal partition
        // - Verify follower catches up with all entries
        todo!("Implement follower catch-up")
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
    #[ignore = "HA not implemented: need crash recovery"]
    async fn test_leader_crash_triggers_new_election() {
        // New leader should be elected when current leader crashes
        // - Start 3-node cluster, identify leader
        // - Kill leader
        // - Verify new leader elected from remaining nodes
        // - Verify cluster can still commit entries
        todo!("Implement leader failure handling")
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
    #[ignore = "HA not implemented: need network partition handling"]
    async fn test_network_partition_minority_cannot_commit() {
        // Minority partition should not be able to commit
        // - Start 5-node cluster
        // - Partition into 2 + 3
        // - Verify minority (2) cannot commit
        // - Verify majority (3) can commit
        todo!("Implement partition handling")
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need network partition handling"]
    async fn test_network_partition_heals_state_converges() {
        // State should converge after partition heals
        // - Start 5-node cluster
        // - Partition into 2 + 3
        // - Commit entries on majority side
        // - Heal partition
        // - Verify minority catches up
        // - Verify all nodes have same state
        todo!("Implement partition healing")
    }

    #[tokio::test]
    #[ignore = "HA not implemented: need split-brain prevention"]
    async fn test_no_split_brain() {
        // Two leaders should never exist in same term
        // - Create network partition scenario
        // - Verify at most one leader per term
        // - Verify no conflicting commits
        todo!("Implement split-brain prevention")
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
