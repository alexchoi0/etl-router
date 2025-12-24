use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, LogState, OptionalSend, StorageError, Vote};
use rocksdb::{checkpoint::Checkpoint, IteratorMode, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::config::{NodeId, TypeConfig};

const LOG_CF: &str = "log";
const META_CF: &str = "meta";

const LAST_INDEX_KEY: &[u8] = b"last_index";
const LAST_TERM_KEY: &[u8] = b"last_term";
const LAST_PURGED_INDEX_KEY: &[u8] = b"last_purged_index";
const LAST_PURGED_TERM_KEY: &[u8] = b"last_purged_term";
const VOTE_KEY: &[u8] = b"vote";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredVote {
    leader_id: Option<NodeId>,
    term: u64,
    committed: bool,
}

pub struct LogStorage {
    db: Arc<RwLock<DB>>,
    last_index: AtomicU64,
    last_term: AtomicU64,
    last_purged_index: AtomicU64,
    last_purged_term: AtomicU64,
}

impl LogStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = vec![LOG_CF, META_CF];
        let db = DB::open_cf(&opts, path, cfs).context("Failed to open RocksDB")?;

        let last_index = Self::read_u64_meta(&db, LAST_INDEX_KEY).unwrap_or(0);
        let last_term = Self::read_u64_meta(&db, LAST_TERM_KEY).unwrap_or(0);
        let last_purged_index = Self::read_u64_meta(&db, LAST_PURGED_INDEX_KEY).unwrap_or(0);
        let last_purged_term = Self::read_u64_meta(&db, LAST_PURGED_TERM_KEY).unwrap_or(0);

        Ok(Self {
            db: Arc::new(RwLock::new(db)),
            last_index: AtomicU64::new(last_index),
            last_term: AtomicU64::new(last_term),
            last_purged_index: AtomicU64::new(last_purged_index),
            last_purged_term: AtomicU64::new(last_purged_term),
        })
    }

    fn read_u64_meta(db: &DB, key: &[u8]) -> Option<u64> {
        let cf = db.cf_handle(META_CF)?;
        let bytes = db.get_cf(cf, key).ok()??;
        if bytes.len() == 8 {
            Some(u64::from_be_bytes(bytes.try_into().ok()?))
        } else {
            None
        }
    }

    fn index_to_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }

    fn key_to_index(key: &[u8]) -> Option<u64> {
        if key.len() == 8 {
            Some(u64::from_be_bytes(key.try_into().ok()?))
        } else {
            None
        }
    }

    pub fn last_index(&self) -> u64 {
        self.last_index.load(Ordering::SeqCst)
    }

    pub fn last_term(&self) -> u64 {
        self.last_term.load(Ordering::SeqCst)
    }

    pub async fn db_path(&self) -> PathBuf {
        self.db.read().await.path().to_path_buf()
    }

    pub async fn create_db_checkpoint<P: AsRef<Path>>(&self, path: P) -> Result<u64> {
        let db = self.db.read().await;
        let checkpoint = Checkpoint::new(&db).context("Failed to create checkpoint object")?;

        checkpoint
            .create_checkpoint(&path)
            .context("Failed to create checkpoint")?;

        let size = dir_size(path.as_ref())?;
        Ok(size)
    }

    fn last_purged_log_id(&self) -> Option<LogId<NodeId>> {
        let index = self.last_purged_index.load(Ordering::SeqCst);
        if index > 0 {
            let term = self.last_purged_term.load(Ordering::SeqCst);
            Some(LogId::new(term, index))
        } else {
            None
        }
    }
}

impl RaftLogReader<TypeConfig> for LogStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<TypeConfig>> {
        let db = self.db.read().await;
        let cf = match db.cf_handle(LOG_CF) {
            Some(cf) => cf,
            None => return Ok(Vec::new()),
        };

        let start = match range.start_bound() {
            std::ops::Bound::Included(&s) => s,
            std::ops::Bound::Excluded(&s) => s + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&e) => e + 1,
            std::ops::Bound::Excluded(&e) => e,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        let start_key = Self::index_to_key(start);
        let end_key = Self::index_to_key(end);

        let mut entries = Vec::new();
        let iter = db.iterator_cf(cf, IteratorMode::From(&start_key, rocksdb::Direction::Forward));

        for result in iter {
            match result {
                Ok((key, value)) => {
                    if key.as_ref() >= end_key.as_slice() {
                        break;
                    }
                    if let Ok(entry) = bincode::deserialize::<Entry<TypeConfig>>(&value) {
                        entries.push(entry);
                    }
                }
                Err(_) => break,
            }
        }

        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<TypeConfig>> {
        let db = self.db.read().await;
        let cf = match db.cf_handle(META_CF) {
            Some(cf) => cf,
            None => return Ok(None),
        };

        let bytes = match db.get_cf(cf, VOTE_KEY) {
            Ok(Some(b)) => b,
            _ => return Ok(None),
        };

        let stored: StoredVote = match bincode::deserialize(&bytes) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        let vote = Vote::new(stored.term, stored.leader_id.unwrap_or(0));
        if stored.committed {
            Ok(Some(vote.commit()))
        } else {
            Ok(Some(vote))
        }
    }
}

impl RaftLogStorage<TypeConfig> for LogStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<TypeConfig>> {
        let last_index = self.last_index.load(Ordering::SeqCst);
        let last_term = self.last_term.load(Ordering::SeqCst);

        let last_log_id = if last_index > 0 {
            Some(LogId::new(last_term, last_index))
        } else {
            None
        };

        Ok(LogState {
            last_purged_log_id: self.last_purged_log_id(),
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        LogStorage {
            db: self.db.clone(),
            last_index: AtomicU64::new(self.last_index.load(Ordering::SeqCst)),
            last_term: AtomicU64::new(self.last_term.load(Ordering::SeqCst)),
            last_purged_index: AtomicU64::new(self.last_purged_index.load(Ordering::SeqCst)),
            last_purged_term: AtomicU64::new(self.last_purged_term.load(Ordering::SeqCst)),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<TypeConfig>> {
        let db = self.db.write().await;
        let cf = db
            .cf_handle(META_CF)
            .ok_or_else(|| StorageError::write_vote(anyhow::anyhow!("Meta CF not found"), vote))?;

        let stored = StoredVote {
            leader_id: if *vote.leader_id() == 0 {
                None
            } else {
                Some(*vote.leader_id())
            },
            term: vote.leader_id().term,
            committed: vote.is_committed(),
        };

        let bytes = bincode::serialize(&stored)
            .map_err(|e| StorageError::write_vote(anyhow::anyhow!("Serialize error: {}", e), vote))?;

        db.put_cf(cf, VOTE_KEY, bytes)
            .map_err(|e| StorageError::write_vote(anyhow::anyhow!("Write error: {}", e), vote))?;

        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), StorageError<TypeConfig>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        if entries.is_empty() {
            callback.io_completed(Ok(()));
            return Ok(());
        }

        let db = self.db.write().await;
        let cf = db.cf_handle(LOG_CF).ok_or_else(|| {
            StorageError::write_logs(anyhow::anyhow!("Log CF not found"))
        })?;
        let meta_cf = db.cf_handle(META_CF).ok_or_else(|| {
            StorageError::write_logs(anyhow::anyhow!("Meta CF not found"))
        })?;

        let mut batch = WriteBatch::default();
        let mut new_last_index = self.last_index.load(Ordering::SeqCst);
        let mut new_last_term = self.last_term.load(Ordering::SeqCst);

        for entry in &entries {
            let index = entry.log_id.index;
            let term = entry.log_id.leader_id.term;

            new_last_index = index;
            new_last_term = term;

            let key = Self::index_to_key(index);
            let value = bincode::serialize(entry).map_err(|e| {
                StorageError::write_logs(anyhow::anyhow!("Serialize error: {}", e))
            })?;

            batch.put_cf(cf, key, value);
        }

        batch.put_cf(meta_cf, LAST_INDEX_KEY, new_last_index.to_be_bytes());
        batch.put_cf(meta_cf, LAST_TERM_KEY, new_last_term.to_be_bytes());

        db.write(batch)
            .map_err(|e| StorageError::write_logs(anyhow::anyhow!("Write error: {}", e)))?;

        self.last_index.store(new_last_index, Ordering::SeqCst);
        self.last_term.store(new_last_term, Ordering::SeqCst);

        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<TypeConfig>> {
        let index = log_id.index;
        let current_last = self.last_index.load(Ordering::SeqCst);

        if index >= current_last {
            return Ok(());
        }

        let db = self.db.write().await;
        let cf = db.cf_handle(LOG_CF).ok_or_else(|| {
            StorageError::write_logs(anyhow::anyhow!("Log CF not found"))
        })?;
        let meta_cf = db.cf_handle(META_CF).ok_or_else(|| {
            StorageError::write_logs(anyhow::anyhow!("Meta CF not found"))
        })?;

        let mut batch = WriteBatch::default();
        for i in (index + 1)..=current_last {
            let key = Self::index_to_key(i);
            batch.delete_cf(cf, key);
        }

        batch.put_cf(meta_cf, LAST_INDEX_KEY, index.to_be_bytes());
        batch.put_cf(meta_cf, LAST_TERM_KEY, log_id.leader_id.term.to_be_bytes());

        db.write(batch)
            .map_err(|e| StorageError::write_logs(anyhow::anyhow!("Truncate error: {}", e)))?;

        self.last_index.store(index, Ordering::SeqCst);
        self.last_term.store(log_id.leader_id.term, Ordering::SeqCst);

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<TypeConfig>> {
        let db = self.db.write().await;
        let cf = db.cf_handle(LOG_CF).ok_or_else(|| {
            StorageError::write_logs(anyhow::anyhow!("Log CF not found"))
        })?;
        let meta_cf = db.cf_handle(META_CF).ok_or_else(|| {
            StorageError::write_logs(anyhow::anyhow!("Meta CF not found"))
        })?;

        let iter = db.iterator_cf(cf, IteratorMode::Start);

        let mut batch = WriteBatch::default();
        for result in iter {
            match result {
                Ok((key, _)) => {
                    if let Some(entry_index) = Self::key_to_index(&key) {
                        if entry_index <= log_id.index {
                            batch.delete_cf(cf, &key);
                        } else {
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }

        batch.put_cf(meta_cf, LAST_PURGED_INDEX_KEY, log_id.index.to_be_bytes());
        batch.put_cf(meta_cf, LAST_PURGED_TERM_KEY, log_id.leader_id.term.to_be_bytes());

        db.write(batch)
            .map_err(|e| StorageError::write_logs(anyhow::anyhow!("Purge error: {}", e)))?;

        self.last_purged_index.store(log_id.index, Ordering::SeqCst);
        self.last_purged_term.store(log_id.leader_id.term, Ordering::SeqCst);

        Ok(())
    }
}

fn dir_size<P: AsRef<Path>>(path: P) -> Result<u64> {
    let mut size = 0u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            size += metadata.len();
        } else if metadata.is_dir() {
            size += dir_size(entry.path())?;
        }
    }
    Ok(size)
}

pub fn copy_dir_recursive<P: AsRef<Path>, Q: AsRef<Path>>(src: P, dst: Q) -> Result<()> {
    let src = src.as_ref();
    let dst = dst.as_ref();

    std::fs::create_dir_all(dst)?;

    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if entry.metadata()?.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}
