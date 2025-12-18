use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use anyhow::{Result, Context};
use rocksdb::{DB, Options, IteratorMode, WriteBatch, checkpoint::Checkpoint};
use serde::{Serialize, Deserialize};

use super::commands::RouterCommand;

const LOG_CF: &str = "log";
const META_CF: &str = "meta";

const LAST_INDEX_KEY: &[u8] = b"last_index";
const LAST_TERM_KEY: &[u8] = b"last_term";
const COMMIT_INDEX_KEY: &[u8] = b"commit_index";
const CURRENT_TERM_KEY: &[u8] = b"current_term";
const VOTED_FOR_KEY: &[u8] = b"voted_for";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub command: RouterCommand,
}

pub struct LogStorage {
    db: DB,
    last_index: AtomicU64,
    last_term: AtomicU64,
    commit_index: AtomicU64,
}

impl LogStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = vec![LOG_CF, META_CF];
        let db = DB::open_cf(&opts, path, cfs)
            .context("Failed to open RocksDB")?;

        let last_index = Self::read_u64_meta(&db, LAST_INDEX_KEY).unwrap_or(0);
        let last_term = Self::read_u64_meta(&db, LAST_TERM_KEY).unwrap_or(0);
        let commit_index = Self::read_u64_meta(&db, COMMIT_INDEX_KEY).unwrap_or(0);

        Ok(Self {
            db,
            last_index: AtomicU64::new(last_index),
            last_term: AtomicU64::new(last_term),
            commit_index: AtomicU64::new(commit_index),
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

    fn write_u64_meta(&self, key: &[u8], value: u64) -> Result<()> {
        let cf = self.db.cf_handle(META_CF)
            .context("Meta column family not found")?;
        self.db.put_cf(cf, key, value.to_be_bytes())
            .context("Failed to write meta")?;
        Ok(())
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

    pub async fn append(&self, term: u64, command: RouterCommand) -> Result<u64> {
        let new_index = self.last_index.fetch_add(1, Ordering::SeqCst) + 1;

        let entry = LogEntry {
            index: new_index,
            term,
            command,
        };

        let cf = self.db.cf_handle(LOG_CF)
            .context("Log column family not found")?;

        let key = Self::index_to_key(new_index);
        let value = bincode::serialize(&entry)
            .context("Failed to serialize log entry")?;

        let mut batch = WriteBatch::default();
        batch.put_cf(cf, key, value);

        let meta_cf = self.db.cf_handle(META_CF)
            .context("Meta column family not found")?;
        batch.put_cf(meta_cf, LAST_INDEX_KEY, new_index.to_be_bytes());
        batch.put_cf(meta_cf, LAST_TERM_KEY, term.to_be_bytes());

        self.db.write(batch)
            .context("Failed to write log entry batch")?;

        self.last_term.store(term, Ordering::SeqCst);

        Ok(new_index)
    }

    pub async fn append_entries(&self, entries: Vec<(u64, RouterCommand)>) -> Result<u64> {
        if entries.is_empty() {
            return Ok(self.last_index.load(Ordering::SeqCst));
        }

        let cf = self.db.cf_handle(LOG_CF)
            .context("Log column family not found")?;
        let meta_cf = self.db.cf_handle(META_CF)
            .context("Meta column family not found")?;

        let mut batch = WriteBatch::default();
        let mut new_last_index = self.last_index.load(Ordering::SeqCst);
        let mut new_last_term = self.last_term.load(Ordering::SeqCst);

        for (term, command) in entries {
            new_last_index += 1;
            new_last_term = term;

            let entry = LogEntry {
                index: new_last_index,
                term,
                command,
            };

            let key = Self::index_to_key(new_last_index);
            let value = bincode::serialize(&entry)
                .context("Failed to serialize log entry")?;

            batch.put_cf(cf, key, value);
        }

        batch.put_cf(meta_cf, LAST_INDEX_KEY, new_last_index.to_be_bytes());
        batch.put_cf(meta_cf, LAST_TERM_KEY, new_last_term.to_be_bytes());

        self.db.write(batch)
            .context("Failed to write log entries batch")?;

        self.last_index.store(new_last_index, Ordering::SeqCst);
        self.last_term.store(new_last_term, Ordering::SeqCst);

        Ok(new_last_index)
    }

    pub async fn get(&self, index: u64) -> Option<LogEntry> {
        let cf = self.db.cf_handle(LOG_CF)?;
        let key = Self::index_to_key(index);
        let bytes = self.db.get_cf(cf, key).ok()??;
        bincode::deserialize(&bytes).ok()
    }

    pub async fn get_range(&self, start: u64, end: u64) -> Vec<LogEntry> {
        let cf = match self.db.cf_handle(LOG_CF) {
            Some(cf) => cf,
            None => return Vec::new(),
        };

        let start_key = Self::index_to_key(start);
        let end_key = Self::index_to_key(end + 1);

        let mut entries = Vec::new();
        let iter = self.db.iterator_cf(cf, IteratorMode::From(&start_key, rocksdb::Direction::Forward));

        for result in iter {
            match result {
                Ok((key, value)) => {
                    if key.as_ref() >= end_key.as_slice() {
                        break;
                    }
                    if let Ok(entry) = bincode::deserialize::<LogEntry>(&value) {
                        entries.push(entry);
                    }
                }
                Err(_) => break,
            }
        }

        entries
    }

    pub async fn truncate_after(&self, index: u64) -> Result<()> {
        let current_last = self.last_index.load(Ordering::SeqCst);
        if index >= current_last {
            return Ok(());
        }

        // Get the entry term before taking cf handles
        let entry_term = self.get(index).await.map(|e| e.term);

        let cf = self.db.cf_handle(LOG_CF)
            .context("Log column family not found")?;
        let meta_cf = self.db.cf_handle(META_CF)
            .context("Meta column family not found")?;

        let mut batch = WriteBatch::default();
        for i in (index + 1)..=current_last {
            let key = Self::index_to_key(i);
            batch.delete_cf(cf, key);
        }

        batch.put_cf(meta_cf, LAST_INDEX_KEY, index.to_be_bytes());

        if let Some(term) = entry_term {
            batch.put_cf(meta_cf, LAST_TERM_KEY, term.to_be_bytes());
            self.last_term.store(term, Ordering::SeqCst);
        }

        self.db.write(batch)
            .context("Failed to truncate log")?;

        self.last_index.store(index, Ordering::SeqCst);

        Ok(())
    }

    pub async fn set_commit_index(&self, index: u64) -> Result<()> {
        self.write_u64_meta(COMMIT_INDEX_KEY, index)?;
        self.commit_index.store(index, Ordering::SeqCst);
        Ok(())
    }

    pub fn last_index(&self) -> u64 {
        self.last_index.load(Ordering::SeqCst)
    }

    pub fn last_term(&self) -> u64 {
        self.last_term.load(Ordering::SeqCst)
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index.load(Ordering::SeqCst)
    }

    pub async fn get_uncommitted_entries(&self) -> Vec<LogEntry> {
        let commit_idx = self.commit_index.load(Ordering::SeqCst);
        let last_idx = self.last_index.load(Ordering::SeqCst);

        if last_idx <= commit_idx {
            return Vec::new();
        }

        self.get_range(commit_idx + 1, last_idx).await
    }

    pub fn entry_count(&self) -> u64 {
        self.last_index.load(Ordering::SeqCst)
    }

    pub async fn compact_before(&self, index: u64) -> Result<u64> {
        let cf = self.db.cf_handle(LOG_CF)
            .context("Log column family not found")?;

        let mut deleted = 0u64;
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);

        let mut batch = WriteBatch::default();
        for result in iter {
            match result {
                Ok((key, _)) => {
                    if let Some(entry_index) = Self::key_to_index(&key) {
                        if entry_index < index {
                            batch.delete_cf(cf, &key);
                            deleted += 1;
                        } else {
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }

        if deleted > 0 {
            self.db.write(batch)
                .context("Failed to compact log")?;
        }

        Ok(deleted)
    }

    pub fn save_current_term(&self, term: u64) -> Result<()> {
        self.write_u64_meta(CURRENT_TERM_KEY, term)
    }

    pub fn load_current_term(&self) -> u64 {
        Self::read_u64_meta(&self.db, CURRENT_TERM_KEY).unwrap_or(0)
    }

    pub fn save_voted_for(&self, voted_for: Option<u64>) -> Result<()> {
        let cf = self.db.cf_handle(META_CF)
            .context("Meta column family not found")?;

        match voted_for {
            Some(node_id) => {
                self.db.put_cf(cf, VOTED_FOR_KEY, node_id.to_be_bytes())
                    .context("Failed to write voted_for")?;
            }
            None => {
                self.db.delete_cf(cf, VOTED_FOR_KEY)
                    .context("Failed to delete voted_for")?;
            }
        }
        Ok(())
    }

    pub fn load_voted_for(&self) -> Option<u64> {
        let cf = self.db.cf_handle(META_CF)?;
        let bytes = self.db.get_cf(cf, VOTED_FOR_KEY).ok()??;
        if bytes.len() == 8 {
            Some(u64::from_be_bytes(bytes.as_slice().try_into().ok()?))
        } else {
            None
        }
    }

    pub fn save_raft_state(&self, term: u64, voted_for: Option<u64>) -> Result<()> {
        let cf = self.db.cf_handle(META_CF)
            .context("Meta column family not found")?;

        let mut batch = WriteBatch::default();
        batch.put_cf(cf, CURRENT_TERM_KEY, term.to_be_bytes());

        match voted_for {
            Some(node_id) => {
                batch.put_cf(cf, VOTED_FOR_KEY, node_id.to_be_bytes());
            }
            None => {
                batch.delete_cf(cf, VOTED_FOR_KEY);
            }
        }

        self.db.write(batch)
            .context("Failed to save raft state")?;
        Ok(())
    }

    pub fn load_raft_state(&self) -> (u64, Option<u64>) {
        let term = self.load_current_term();
        let voted_for = self.load_voted_for();
        (term, voted_for)
    }

    pub fn create_db_checkpoint<P: AsRef<Path>>(&self, path: P) -> Result<u64> {
        let checkpoint = Checkpoint::new(&self.db)
            .context("Failed to create checkpoint object")?;

        checkpoint.create_checkpoint(&path)
            .context("Failed to create checkpoint")?;

        let size = dir_size(path.as_ref())?;
        Ok(size)
    }

    pub fn restore_from_checkpoint<P: AsRef<Path>>(&self, _checkpoint_path: P) -> Result<()> {
        anyhow::bail!("Restore requires reopening the database. Use LogStorage::restore_checkpoint() static method instead.")
    }

    pub fn restore_checkpoint<P: AsRef<Path>>(db_path: P, checkpoint_path: P) -> Result<Self> {
        let db_path = db_path.as_ref();
        let checkpoint_path = checkpoint_path.as_ref();

        if db_path.exists() {
            let backup_path = db_path.with_extension("backup");
            if backup_path.exists() {
                std::fs::remove_dir_all(&backup_path)
                    .context("Failed to remove old backup")?;
            }
            std::fs::rename(db_path, &backup_path)
                .context("Failed to backup current database")?;
        }

        copy_dir_recursive(checkpoint_path, db_path)
            .context("Failed to copy checkpoint to database path")?;

        Self::new(db_path)
    }

    pub fn db_path(&self) -> PathBuf {
        self.db.path().to_path_buf()
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

fn copy_dir_recursive<P: AsRef<Path>, Q: AsRef<Path>>(src: P, dst: Q) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_storage() -> (LogStorage, TempDir) {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LogStorage::new(tmp_dir.path()).unwrap();
        (storage, tmp_dir)
    }

    #[tokio::test]
    async fn test_append_and_get() {
        let (storage, _tmp) = create_test_storage().await;

        let cmd = RouterCommand::CommitSourceOffset {
            source_id: "test-source".to_string(),
            partition: 0,
            offset: 100,
        };

        let index = storage.append(1, cmd.clone()).await.unwrap();
        assert_eq!(index, 1);

        let entry = storage.get(1).await.unwrap();
        assert_eq!(entry.index, 1);
        assert_eq!(entry.term, 1);
    }

    #[tokio::test]
    async fn test_get_range() {
        let (storage, _tmp) = create_test_storage().await;

        for i in 0..5 {
            let cmd = RouterCommand::CommitSourceOffset {
                source_id: format!("source-{}", i),
                partition: 0,
                offset: i as u64 * 100,
            };
            storage.append(1, cmd).await.unwrap();
        }

        let entries = storage.get_range(2, 4).await;
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].index, 2);
        assert_eq!(entries[2].index, 4);
    }

    #[tokio::test]
    async fn test_truncate() {
        let (storage, _tmp) = create_test_storage().await;

        for i in 0..5 {
            let cmd = RouterCommand::CommitSourceOffset {
                source_id: format!("source-{}", i),
                partition: 0,
                offset: i as u64 * 100,
            };
            storage.append(1, cmd).await.unwrap();
        }

        storage.truncate_after(3).await.unwrap();

        assert_eq!(storage.last_index(), 3);
        assert!(storage.get(3).await.is_some());
        assert!(storage.get(4).await.is_none());
        assert!(storage.get(5).await.is_none());
    }

    #[tokio::test]
    async fn test_persistence() {
        let tmp_dir = TempDir::new().unwrap();

        {
            let storage = LogStorage::new(tmp_dir.path()).unwrap();
            let cmd = RouterCommand::CommitSourceOffset {
                source_id: "test".to_string(),
                partition: 0,
                offset: 42,
            };
            storage.append(1, cmd).await.unwrap();
            storage.set_commit_index(1).await.unwrap();
        }

        {
            let storage = LogStorage::new(tmp_dir.path()).unwrap();
            assert_eq!(storage.last_index(), 1);
            assert_eq!(storage.last_term(), 1);
            assert_eq!(storage.commit_index(), 1);

            let entry = storage.get(1).await.unwrap();
            assert_eq!(entry.index, 1);
        }
    }
}
