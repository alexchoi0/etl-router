use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{Entry, EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError, StoredMembership};
use tokio::sync::RwLock;

use crate::config::{NodeId, RouterRequest, RouterResponse, TypeConfig};
use crate::router_state::RouterState;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<TypeConfig>,
    pub data: Vec<u8>,
}

pub struct StateMachine {
    state: Arc<RwLock<RouterState>>,
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<TypeConfig>,
    snapshot: Option<StoredSnapshot>,
}

impl StateMachine {
    pub fn new(state: Arc<RwLock<RouterState>>) -> Self {
        Self {
            state,
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            snapshot: None,
        }
    }

    pub fn state(&self) -> Arc<RwLock<RouterState>> {
        self.state.clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<TypeConfig>> {
        let state = self.state.read().await;
        let data = bincode::serialize(&*state).map_err(|e| {
            StorageError::read_state_machine(anyhow::anyhow!("Serialize error: {}", e))
        })?;
        drop(state);

        let snapshot_id = format!(
            "{}-{}",
            self.last_applied_log
                .map(|l| l.index)
                .unwrap_or(0),
            uuid::Uuid::new_v4()
        );

        let meta = SnapshotMeta {
            last_log_id: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        self.snapshot = Some(snapshot);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<TypeConfig>), StorageError<TypeConfig>>
    {
        Ok((self.last_applied_log, self.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<RouterResponse>, StorageError<TypeConfig>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut results = Vec::new();
        let mut state = self.state.write().await;

        for entry in entries {
            self.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    results.push(RouterResponse {
                        success: true,
                        error: None,
                    });
                }
                EntryPayload::Normal(req) => match state.apply_command(req.command) {
                    Ok(()) => results.push(RouterResponse {
                        success: true,
                        error: None,
                    }),
                    Err(e) => results.push(RouterResponse {
                        success: false,
                        error: Some(e.to_string()),
                    }),
                },
                EntryPayload::Membership(m) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), m);
                    results.push(RouterResponse {
                        success: true,
                        error: None,
                    });
                }
            }
        }

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        StateMachine {
            state: self.state.clone(),
            last_applied_log: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            snapshot: self.snapshot.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<TypeConfig>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<TypeConfig>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<TypeConfig>> {
        let data = snapshot.into_inner();

        let new_state: RouterState = bincode::deserialize(&data).map_err(|e| {
            StorageError::read_state_machine(anyhow::anyhow!("Deserialize error: {}", e))
        })?;

        *self.state.write().await = new_state;
        self.last_applied_log = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();

        self.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<TypeConfig>> {
        match &self.snapshot {
            Some(s) => Ok(Some(Snapshot {
                meta: s.meta.clone(),
                snapshot: Box::new(Cursor::new(s.data.clone())),
            })),
            None => Ok(None),
        }
    }
}
